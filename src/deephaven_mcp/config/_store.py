"""Write-side storage for the configuration directory.

The read side of the configuration package (loaders, templating,
tree) is consumed by the server at startup; :class:`ConfigStore` is
the write side, consumed by the ``dhcli config`` authoring commands.
It upholds one invariant: **no caller can put a schema-invalid file
into the live configuration directory**. Every write validates staged
content against the location's Pydantic schema first and lands via
an atomic same-directory rename; a failed validation leaves the
directory byte-for-byte untouched.

:class:`ConfigStore` is bound to one ``config_dir`` and addresses
files via :class:`~deephaven_mcp.config._logical_paths.ConfigFieldLocation`,
mirroring :class:`~deephaven_mcp.config.tree.ConfigTreeLoader` on the
read side. All methods are synchronous: they run inside short-lived
CLI invocations on small local files.
"""

from __future__ import annotations

__all__ = [
    "ConfigStore",
    "RawConfigFile",
]

import json
import logging
import os
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import json5
from pydantic import ValidationError

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp._pydantic import format_error_details
from deephaven_mcp.config._logical_paths import ConfigFieldLocation
from deephaven_mcp.config._templating import JsonLoc, expand_tree_lenient

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RawConfigFile:
    """One configuration file's raw (unexpanded) parse."""

    data: dict[str, Any]
    """Parsed top-level JSON object, with every ``${env:...}`` /
    ``${file:...}`` placeholder left verbatim (no template
    expansion)."""

    strict_json: bool
    """Whether the file text also parses as strict JSON. ``False``
    means the file relies on JSON5 extensions (comments, trailing
    commas) that a programmatic rewrite would destroy; ``config
    set``/``unset`` refuse to rewrite such files."""


def _ensure_private_parents(config_dir: Path, file_path: Path) -> None:
    """Create ``file_path``'s parent directories under ``config_dir`` at 0o700.

    Every directory level from ``config_dir`` (inclusive) down to the
    file's parent is created when absent and, when newly created,
    tightened to owner-only mode (POSIX; on Windows the chmod is a
    no-op beyond the read-only bit, matching the platform audit's
    best-effort stance).

    Missing ancestors *above* ``config_dir`` (e.g. a not-yet-created
    data root like ``~/.deephaven/ai`` on a fresh machine) are created
    but not tightened — they may legitimately be shared. This mirrors
    the ancestor policy of
    :func:`deephaven_mcp._platform.dir_permissions.harden_private_dir`;
    keep the two sites aligned.
    """
    if not config_dir.exists():
        config_dir.parent.mkdir(parents=True, exist_ok=True)
    levels: list[Path] = [config_dir]
    relative_parent = file_path.parent.relative_to(config_dir)
    for part in relative_parent.parts:
        levels.append(levels[-1] / part)
    for level in levels:
        was_absent = not level.exists()
        # exist_ok=True: two first-time authoring processes can race to
        # create the same level on a fresh machine; the loser must
        # proceed (to the advisory lock and its write) rather than
        # surface FileExistsError. exist_ok ignores only an existing
        # directory, so a non-directory collision still raises.
        level.mkdir(mode=0o700, exist_ok=True)
        if was_absent:
            _LOGGER.debug(
                f"[_store:_ensure_private_parents] Created directory: {level}"
            )


def _error_at_unresolved_location(
    loc: JsonLoc,
    expanded: dict[str, Any],
    unresolved_locations: frozenset[JsonLoc],
) -> bool:
    """Return whether a schema error's ``loc`` lands on an unresolved ref.

    Walks ``expanded`` along ``loc``, accumulating the structural
    location of the segments that actually exist in the data. String
    segments absent from the current dict node are skipped: Pydantic
    inserts segments that are not data keys — a discriminated union's
    tag (e.g. the ``psk`` in ``('auth', 'credentials', 'psk',
    'token')``) or the loader-injected ``name`` — and those must not
    break the match.

    Args:
        loc (JsonLoc): The error's location as reported by
            :meth:`pydantic.ValidationError.errors`.
        expanded (dict[str, Any]): The leniently-expanded file
            contents the schema validated.
        unresolved_locations (frozenset[JsonLoc]): The locations left
            verbatim by the lenient expansion.

    Returns:
        bool: ``True`` when the walked location is one of
            ``unresolved_locations``.
    """
    node: Any = expanded
    walked: list[str | int] = []
    for segment in loc:
        if isinstance(segment, int):
            if not isinstance(node, list) or not 0 <= segment < len(node):
                return False
            node = node[segment]
            walked.append(segment)
        elif isinstance(node, dict) and segment in node:
            node = node[segment]
            walked.append(segment)
        # else: a non-data segment (union tag, injected field); skip.
    return JsonLoc(walked) in unresolved_locations


@dataclass(frozen=True, slots=True)
class _StagedFile:
    """One serialized file staged for a rollback-protected batch commit."""

    temp_path: Path
    """Same-directory temp file holding the new content."""

    final_path: Path
    """Destination the temp file is renamed onto at commit time."""


@dataclass(frozen=True, slots=True)
class _CommittedFile:
    """One file already renamed into place during a batch commit."""

    final_path: Path
    """The committed destination path."""

    backup_path: Path | None
    """Same-directory copy of the pre-existing content, or ``None``
    when :attr:`final_path` did not exist before this batch."""


def _serialize(data: dict[str, Any]) -> str:
    """Serialize a wire-format dict as pretty-printed strict JSON."""
    return json.dumps(data, indent=2) + "\n"


def _stage_file(final_path: Path, text: str) -> Path:
    """Write ``text`` to a temp file next to ``final_path``.

    :func:`tempfile.mkstemp` creates the file readable and writable
    only by the creating user (mode ``0o600`` on POSIX), which is
    exactly the mode the permission audit expects the committed file
    to carry.
    """
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{final_path.name}.", suffix=".tmp", dir=final_path.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
    except OSError:
        try:
            Path(temp_name).unlink(missing_ok=True)
        except OSError:
            _LOGGER.warning(
                f"[_store:_stage_file] Could not remove partial temp file "
                f"{temp_name}"
            )
        raise
    return Path(temp_name)


def _reserve_backup_path(final_path: Path) -> Path:
    """Reserve a unique same-directory name to back up ``final_path`` into.

    Used only for its collision-free naming; the reserved (empty) file
    is closed immediately and then overwritten by the caller's backup
    rename.
    """
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{final_path.name}.", suffix=".bak", dir=final_path.parent
    )
    os.close(fd)
    return Path(temp_name)


def _rollback_commits(committed: list[_CommittedFile]) -> list[_CommittedFile]:
    """Undo already-committed renames in ``committed``, most-recent-first.

    An entry with a backup held pre-existing content that was copied
    aside before the new content was renamed in (undo: rename the
    backup back over the destination); an entry without one did not
    exist before this batch (undo: delete it).

    Args:
        committed (list[_CommittedFile]): Entries to undo, in the
            order they were committed.

    Returns:
        list[_CommittedFile]: The subset of ``committed`` that could
            not be undone (rename/unlink itself raised ``OSError``),
            preserved in their original commit order. When such an
            entry has a backup, the backup file is left on disk at
            that path as the sole surviving copy of the pre-existing
            content.
    """
    stuck: list[_CommittedFile] = []
    for entry in reversed(committed):
        try:
            if entry.backup_path is not None:
                os.replace(entry.backup_path, entry.final_path)
            else:
                entry.final_path.unlink(missing_ok=True)
        except OSError:
            _LOGGER.error(
                f"[_store:_rollback_commits] Failed to restore "
                f"{entry.final_path} during rollback"
            )
            stuck.append(entry)
    return stuck


def _batch_failure_message(exc: OSError, stuck: list[_CommittedFile]) -> str:
    """Build the :class:`ConfigurationError` message for a failed batch write.

    Args:
        exc (OSError): The rename failure that aborted the commit.
        stuck (list[_CommittedFile]): Entries :func:`_rollback_commits`
            could not undo (empty when rollback fully succeeded).

    Returns:
        str: The failure message, naming each stuck file and how to
            recover it manually when rollback also failed.
    """
    message = f"Failed to write configuration file(s): {exc}"
    if not stuck:
        return message
    details = "; ".join(
        (
            f"{entry.final_path} could not be restored from backup "
            f"{entry.backup_path}: rename it back manually"
            if entry.backup_path is not None
            else f"{entry.final_path} was written but could not be removed "
            "during rollback"
        )
        for entry in stuck
    )
    return (
        f"{message}. Rollback also failed, leaving the configuration "
        f"directory inconsistent: {details}"
    )


def _commit_staged(staged: list[_StagedFile]) -> None:
    """Commit staged files, rolling back on failure.

    Each entry's pre-existing target (if any) is *copied* aside to a
    same-directory backup — leaving the target in place — and then the
    staged file is renamed directly over it with :func:`os.replace`.
    Because the target is never moved out of the way first, it is
    always present on disk: a concurrent reader sees either the old
    content or (atomically) the new, never a missing file. On failure,
    every already-committed entry is rolled back via
    :func:`_rollback_commits`; on full success, now-unneeded backups
    are removed.

    Args:
        staged (list[_StagedFile]): The files to commit.

    Raises:
        ConfigurationError: When any copy or rename fails. The message
            names any file(s) left inconsistent if rollback itself also
            failed.
    """
    committed: list[_CommittedFile] = []
    try:
        for item in staged:
            backup_path: Path | None = None
            if item.final_path.exists():
                backup_path = _reserve_backup_path(item.final_path)
                # Copy (not move) so the target stays in place; the
                # os.replace below then atomically overwrites it with no
                # window in which the file is absent. Recorded before the
                # replace: if that replace fails, rollback must already
                # know to restore this backup.
                shutil.copy2(item.final_path, backup_path)
                committed.append(
                    _CommittedFile(final_path=item.final_path, backup_path=backup_path)
                )
            os.replace(item.temp_path, item.final_path)
            if backup_path is None:
                committed.append(
                    _CommittedFile(final_path=item.final_path, backup_path=None)
                )
            _LOGGER.info(f"[_store:_commit_staged] Wrote {item.final_path}")
    except OSError as exc:
        stuck = _rollback_commits(committed)
        raise ConfigurationError(_batch_failure_message(exc, stuck)) from exc
    # The batch succeeded; these backups are no longer needed. A failure
    # here is cosmetic leftover clutter, not a write failure — log it
    # rather than raise, so it can never be mistaken for the former.
    for entry in committed:
        if entry.backup_path is not None:
            try:
                entry.backup_path.unlink(missing_ok=True)
            except OSError:
                _LOGGER.warning(
                    f"[_store:_commit_staged] Wrote successfully but could "
                    f"not remove leftover backup {entry.backup_path}"
                )


class ConfigStore:
    """All file I/O for one configuration directory.

    Bound to a single ``config_dir`` at construction, mirroring
    :class:`~deephaven_mcp.config.tree.ConfigTreeLoader` on the read
    side. Every method takes a
    :class:`~deephaven_mcp.config._logical_paths.ConfigFieldLocation`
    to address a file; :attr:`config_dir` is applied once, here,
    rather than threaded through every call.
    """

    def __init__(self, config_dir: Path) -> None:
        """Bind the store to ``config_dir``.

        Args:
            config_dir (Path): The configuration directory root. Not
                required to exist yet; :meth:`write` /
                :meth:`write_all` / :meth:`write_text` create it (and
                any missing parents) on first write.
        """
        self._config_dir = config_dir

    @property
    def config_dir(self) -> Path:
        """The configuration directory root this store is bound to."""
        return self._config_dir

    def path_of(self, location: ConfigFieldLocation) -> Path:
        """Return ``location``'s absolute file path under :attr:`config_dir`."""
        return self._config_dir / location.relative_file_path

    def read(self, location: ConfigFieldLocation) -> RawConfigFile:
        """Read and parse ``location``'s file without template expansion.

        Parses strict JSON first; a file that fails strict parsing is
        retried as JSON5 (comments, trailing commas). Most files parse
        strict on the first attempt, since every write through this
        store emits strict JSON (:meth:`write_text` is the exception,
        preserving whatever the operator wrote by hand).

        Args:
            location (ConfigFieldLocation): The file to read.

        Returns:
            RawConfigFile: The parse result plus the strict-JSON flag.

        Raises:
            ConfigurationError: When the file cannot be read, cannot
                be parsed as JSON or JSON5, or its top-level value is
                not a JSON object.
        """
        path = self.path_of(location)
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConfigurationError(f"Cannot read {path}: {exc}") from exc
        try:
            parsed = json.loads(text)
            strict = True
        except ValueError:
            strict = False
            try:
                parsed = json5.loads(text)
            except ValueError as exc:
                raise ConfigurationError(
                    f"Invalid JSON/JSON5 in configuration file {path}: {exc}"
                ) from exc
        if not isinstance(parsed, dict):
            raise ConfigurationError(
                f"Configuration file {path} must contain a JSON object at "
                f"the top level, got {type(parsed).__name__}."
            )
        return RawConfigFile(data=parsed, strict_json=strict)

    def validate(
        self, location: ConfigFieldLocation, data: dict[str, Any]
    ) -> list[str]:
        """Validate one file's wire-format dict against its schema.

        Template placeholders are expanded first (so a templated
        value is validated at its resolved type, matching what the
        server-side loader does); a placeholder that is syntactically
        valid but cannot be resolved in *this* environment is left
        verbatim and reported as a warning instead of failing — the
        daemon's environment may differ. A schema error whose
        location holds such an unresolved ref (e.g. a templated
        integer field) is likewise downgraded to a warning; only
        genuine schema violations raise. Named kinds are validated
        with the filename stem injected as ``name``, matching the
        loader contract; a file that supplies its own ``name`` is
        rejected, since the name is derived from the filename.

        Args:
            location (ConfigFieldLocation): Which file the dict is
                destined for; selects the schema and the stem
                injection.
            data (dict[str, Any]): The wire-format file contents.

        Returns:
            list[str]: Template-resolution warnings (possibly empty).

        Raises:
            ConfigurationError: When a placeholder is syntactically
                malformed or the (expanded) dict fails schema
                validation.
        """
        source = str(self.path_of(location))
        expansion = expand_tree_lenient(
            data, source=source, config_dir=self._config_dir
        )
        warnings = list(expansion.warnings)
        expanded = expansion.value
        if location.kind.named:
            if "name" in expanded:
                # 'name' is derived from the filename, not stored in the
                # file. Injecting location.name last (as before) would let
                # a raw 'name' override it, so foo.json could validate and
                # write as 'bar' and the loader would then register a
                # mismatched — and silently collision-prone — entity.
                raise ConfigurationError(
                    f"{source}: 'name' is derived from the filename and must "
                    f"not appear in the file contents."
                )
            payload = {"name": location.name, **expanded}
        else:
            payload = expanded
        try:
            location.kind.schema.model_validate(payload)
        except ValidationError as exc:
            # An error whose location holds a string left verbatim by the
            # lenient expansion is not a file defect — the value's type is
            # checked for real once the daemon's environment resolves the
            # ref. Downgrade those to warnings; anything else is a genuine
            # schema violation. ``extra_forbidden`` is exempt from the
            # downgrade: the field is illegal regardless of what its value
            # would resolve to.
            fatal: list[Mapping[str, Any]] = []
            for err in exc.errors():
                loc = JsonLoc(err["loc"])
                if err["type"] != "extra_forbidden" and _error_at_unresolved_location(
                    loc, expanded, expansion.unresolved_locations
                ):
                    warnings.append(
                        f"In {source} at {loc}: {err.get('input')!r} holds an "
                        "unresolved templating ref; its type is checked when "
                        "the server resolves it"
                    )
                else:
                    fatal.append(err)
            if fatal:
                # ``format_error_details`` (not ``as_configuration_error``)
                # keeps the store quiet: the caller surfaces the message in
                # its own payload or error, so an extra ERROR log line here
                # would duplicate it on the operator's terminal.
                raise ConfigurationError(format_error_details(source, fatal)) from exc
        return warnings

    def write(self, location: ConfigFieldLocation, data: dict[str, Any]) -> list[str]:
        """Validate ``data`` and atomically write it to ``location``'s file.

        Args:
            location (ConfigFieldLocation): The file to write.
            data (dict[str, Any]): Wire-format file contents.

        Returns:
            list[str]: Template-resolution warnings (possibly empty).

        Raises:
            ConfigurationError: When validation fails (nothing is
                written) or the file cannot be written.
        """
        return self.write_all([(location, data)])

    def write_all(
        self, entries: list[tuple[ConfigFieldLocation, dict[str, Any]]]
    ) -> list[str]:
        """Validate every entry, then write each file atomically with batch rollback.

        All entries are schema-validated before any file is touched;
        the serialized contents are then staged as same-directory temp
        files (mode ``0o600``). Each is committed by copying any
        pre-existing target aside to a same-directory backup — leaving
        the target in place — and then renaming the staged file
        directly over it with an atomic :func:`os.replace`. The target
        is therefore never absent: a concurrent loader observes either
        the old content or the new, never a missing file. If a later
        entry in the batch fails to commit, every already-committed
        entry is rolled back by renaming its backup back over the
        destination, restoring the configuration directory to its
        pre-call state. Rollback is pure same-directory renames of
        content already on disk — never a fresh write — so it shares
        essentially none of the failure modes (e.g. a full disk) that
        could cause the original commit to fail. In the extreme case
        where a rollback rename itself fails, the pre-existing content
        is preserved on disk at a reported backup path rather than
        lost, and the raised error names exactly which file(s) are
        affected. Leftover staging temp files are always removed.

        The guarantee is per-file atomic writes with all-or-nothing
        batch *rollback*, not cross-file atomic visibility: files
        commit one at a time, and readers (:class:`ConfigTreeLoader`)
        do not take the authoring advisory lock, so a load running
        concurrently with a multi-file commit can briefly observe some
        files already updated and others not. Authoring commands
        serialize against one another through the lock; concurrent
        readers do not participate.

        Args:
            entries (list[tuple[ConfigFieldLocation, dict[str, Any]]]):
                The files to write and their wire-format contents.

        Returns:
            list[str]: Accumulated template-resolution warnings across
                all entries (possibly empty).

        Raises:
            ConfigurationError: When any entry fails validation
                (nothing is written) or any file cannot be staged or
                committed. The message names any file(s) left
                inconsistent if rollback itself also failed.
        """
        warnings: list[str] = []
        for location, data in entries:
            warnings.extend(self.validate(location, data))

        staged: list[_StagedFile] = []
        try:
            for location, data in entries:
                final_path = self.path_of(location)
                _ensure_private_parents(self._config_dir, final_path)
                staged.append(
                    _StagedFile(
                        temp_path=_stage_file(final_path, _serialize(data)),
                        final_path=final_path,
                    )
                )
            _commit_staged(staged)
        except OSError as exc:
            raise ConfigurationError(
                f"Failed to write configuration file(s): {exc}"
            ) from exc
        finally:
            # A failure to remove a leftover staging temp file must never
            # mask the real outcome (success, or the ConfigurationError
            # just raised above) with an unrelated raw OSError.
            for item in staged:
                try:
                    item.temp_path.unlink(missing_ok=True)
                except OSError:
                    _LOGGER.warning(
                        f"[_store:ConfigStore.write_all] Could not remove "
                        f"leftover temp file {item.temp_path}"
                    )
        return warnings

    def write_text(self, location: ConfigFieldLocation, text: str) -> list[str]:
        """Validate and atomically write ``text`` to ``location``'s file, verbatim.

        Unlike :meth:`write`, which re-serializes a wire-format dict
        as pretty-printed strict JSON (destroying JSON5-only syntax),
        this writes ``text`` byte-for-byte after parsing and
        schema-validating it — the safe path for ``dhcli config
        edit``, which edits a file's literal text and must preserve
        whatever comments or formatting the operator wrote.

        Args:
            location (ConfigFieldLocation): The file to write.
            text (str): The literal file contents to write.

        Returns:
            list[str]: Template-resolution warnings (possibly empty).

        Raises:
            ConfigurationError: When ``text`` does not parse as a
                JSON5 object, fails schema validation (nothing is
                written), or the file cannot be written.
        """
        try:
            parsed = json5.loads(text)
        except ValueError as exc:
            raise ConfigurationError(f"Invalid JSON/JSON5: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ConfigurationError(
                f"Configuration file must contain a JSON object at the top "
                f"level, got {type(parsed).__name__}."
            )
        warnings = self.validate(location, parsed)
        final_path = self.path_of(location)
        # Directory creation and staging are inside the OSError->
        # ConfigurationError conversion (as in write_all): a permission
        # or disk failure there must surface the documented structured
        # config error, not leak a raw OSError that 'config edit' — which
        # catches only ConfigurationError — would report as internal_error.
        temp_path: Path | None = None
        try:
            _ensure_private_parents(self._config_dir, final_path)
            temp_path = _stage_file(final_path, text)
            os.replace(temp_path, final_path)
            _LOGGER.info(f"[_store:ConfigStore.write_text] Wrote {final_path}")
        except OSError as exc:
            raise ConfigurationError(
                f"Failed to write configuration file {final_path}: {exc}"
            ) from exc
        finally:
            # Only a successfully staged temp needs removing; _stage_file
            # cleans up after its own failure, and a pre-staging failure
            # leaves temp_path None.
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    _LOGGER.warning(
                        f"[_store:ConfigStore.write_text] Could not remove "
                        f"leftover temp file {temp_path}"
                    )
        return warnings

    def delete(self, location: ConfigFieldLocation) -> Path:
        """Delete ``location``'s file from the configuration directory.

        Args:
            location (ConfigFieldLocation): The file to delete.

        Returns:
            Path: The absolute path that was deleted.

        Raises:
            ConfigurationError: When the file does not exist or
                cannot be removed.
        """
        path = self.path_of(location)
        try:
            path.unlink()
        except FileNotFoundError as exc:
            raise ConfigurationError(f"{path} does not exist") from exc
        except OSError as exc:
            raise ConfigurationError(f"Cannot delete {path}: {exc}") from exc
        _LOGGER.info(f"[_store:ConfigStore.delete] Deleted {path}")
        return path
