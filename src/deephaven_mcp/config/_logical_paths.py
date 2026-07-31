"""Logical-path model of the configuration directory tree.

The ``dhcli`` configuration commands address the entire configuration
as **one logical JSON document** whose shape mirrors the on-disk
layout::

    {
        "cli": { ... },                                  # cli.json
        "server": { ... },                               # server.json
        "community": {
            "settings": { ... },                         # community/settings.json
            "sessions": {"<name>": { ... }, ...}         # community/sessions/<name>.json
        },
        "enterprise": {
            "settings": { ... },                         # enterprise/settings.json
            "systems": {"<name>": { ... }, ...}          # enterprise/systems/<name>.json
        }
    }

A **logical path** is a dot-separated address into that document, at
any depth (``cli.output.format``,
``community.settings.session_creation``,
``enterprise.systems.prod.auth.credentials``), represented by
:class:`~deephaven_mcp.config._field_path.FieldPath`. Paths address
the **wire format** (the JSON as written in the files, e.g.
``auth.credentials``), never the post-validation model shape.

Each of the six file kinds in :mod:`deephaven_mcp.config._file_kinds`
occupies one depth in that tree — its **file boundary**: ``cli.json``
sits at ``cli`` (depth 1); ``community/sessions/<name>.json`` sits at
``community.sessions.<name>`` (depth 3). Every valid logical path is
therefore one of exactly two things, and :func:`resolve_path`
classifies which:

- **At or below a file boundary** — the path names one file, and
  optionally a field within it: a :class:`ConfigFieldLocation`.
- **Above every file boundary** — the path names the root, a section
  (``community``), or a collection (``community.sessions``), scoping
  a set of files: a :class:`ConfigSection`, which owns enumeration of
  the file kinds (:meth:`ConfigSection.kinds`) and concrete files
  (:meth:`ConfigSection.files`) with that prefix.

Anything else — an unknown segment, a malformed or reserved entity
name — is not a location in the document at all and raises
:class:`~deephaven_mcp._exceptions.ConfigurationPathError`.
"""

from __future__ import annotations

__all__ = [
    "ConfigFieldLocation",
    "ConfigSection",
    "resolve_path",
]

import logging
from dataclasses import dataclass
from pathlib import Path

from deephaven_mcp._exceptions import ConfigurationPathError, InvalidSessionNameError
from deephaven_mcp._names import validate_resource_name
from deephaven_mcp._taxonomy import COMMUNITY_SYSTEM_NAME
from deephaven_mcp.config._field_path import FieldPath
from deephaven_mcp.config._file_kinds import ConfigFileKind

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ConfigFieldLocation:
    """A location in the logical document: one file, plus a field within it."""

    kind: ConfigFileKind
    """Which of the six file kinds the location names."""

    name: str | None
    """Entity name (filename stem) for named kinds; ``None`` for
    unnamed kinds."""

    field_path: FieldPath
    """Wire-format field path within the file; :attr:`FieldPath.ROOT`
    when the location is the whole file."""

    @property
    def relative_file_path(self) -> Path:
        """The physical file path, relative to the configuration directory."""
        return self.kind.relative_file_path(self.name)

    @property
    def logical_path(self) -> FieldPath:
        """The logical path to this file (excludes any field within it).

        The counterpart to :attr:`relative_file_path`: e.g.
        ``FieldPath(("community", "sessions", "local_dev"))`` for
        ``community/sessions/local_dev.json``, or
        ``FieldPath(("cli",))`` for ``cli.json``.
        """
        if self.kind.named:
            # ``relative_file_path`` already rejected a missing name,
            # so ``self.name`` is a non-empty string here.
            return self.kind.prefix + str(self.name)
        return self.kind.prefix


@dataclass(frozen=True, slots=True)
class ConfigSection:
    """A logical prefix strictly above every file boundary.

    Names the document root (:attr:`FieldPath.ROOT`), a section
    (``community``), or a collection (``community.sessions``) — never
    a file or a field within one. Construction validates that
    invariant, so holding a ``ConfigSection`` is proof that
    enumerating files with its prefix is meaningful.
    """

    prefix: FieldPath
    """The section's logical path; empty for the document root."""

    def __post_init__(self) -> None:
        """Reject a prefix at or below a file boundary, or invalid.

        Raises:
            ConfigurationPathError: When :attr:`prefix` names a file
                or a field within one (use :func:`resolve_path` /
                :class:`ConfigFieldLocation` for those), or does not
                address any known location.
        """
        if _resolve_field_location(self.prefix) is not None:
            raise ConfigurationPathError(
                f"{self.prefix} names a configuration file or a field "
                "within one, not a section."
            )

    def kinds(self) -> list[ConfigFileKind]:
        """Return the file kinds whose files sit below this section.

        Purely registry-derived — no filesystem access. Use
        :meth:`files` for the concrete files present on disk.

        Returns:
            list[ConfigFileKind]: The kinds whose prefix starts with
                this section's prefix, in registry order.
        """
        return [k for k in ConfigFileKind if k.prefix.has_prefix(self.prefix)]

    def files(self, config_dir: Path) -> list[ConfigFieldLocation]:
        """Enumerate the configuration files below this section.

        Unnamed kinds always contribute their single whole-file
        location (whether or not the file exists on disk); named
        kinds contribute one location per ``*.json`` file found in
        their directory. Callers check existence via
        ``config_dir / location.relative_file_path``.

        Args:
            config_dir (Path): The configuration directory root.

        Returns:
            list[ConfigFieldLocation]: Whole-file locations
                (``field_path`` empty), kinds in registry order and
                named entries sorted by name.
        """
        out: list[ConfigFieldLocation] = []
        for kind in self.kinds():
            if not kind.named:
                out.append(
                    ConfigFieldLocation(kind=kind, name=None, field_path=FieldPath.ROOT)
                )
                continue
            section_dir = config_dir / Path(*kind.prefix)
            if section_dir.is_dir():
                out.extend(
                    ConfigFieldLocation(
                        kind=kind, name=path.stem, field_path=FieldPath.ROOT
                    )
                    for path in sorted(section_dir.glob("*.json"))
                )
        return out


def resolve_path(path: FieldPath) -> ConfigFieldLocation | ConfigSection:
    """Classify a logical path against the file-kind registry.

    See the module docstring for the two-way split this performs.

    Args:
        path (FieldPath): The path to classify, possibly empty for
            the document root.

    Returns:
        ConfigFieldLocation | ConfigSection: A :class:`ConfigFieldLocation`
            when ``path`` is at or below a file boundary (naming one
            file, plus the field within it — empty for the whole
            file); a :class:`ConfigSection` when ``path`` sits above
            every boundary (the root, ``community``,
            ``community.sessions``, ...).

    Raises:
        ConfigurationPathError: When ``path`` does not address any
            location in the logical document:

            - an unknown top-level segment,
            - an unknown segment under a known section,
            - an entity name with an illegal character,
            - ``community`` used as an enterprise system name (it is
              reserved for the community umbrella system).
    """
    location = _resolve_field_location(path)
    if location is not None:
        return location
    return ConfigSection(prefix=path)


def _resolve_field_location(path: FieldPath) -> ConfigFieldLocation | None:
    """Resolve ``path`` to a :class:`ConfigFieldLocation`, or ``None`` for a section.

    The shared classifier behind :func:`resolve_path` and the
    :class:`ConfigSection` construction invariant. ``None`` here is
    strictly internal shorthand for "above every file boundary" —
    the public API surfaces that case as :class:`ConfigSection`.

    Args:
        path (FieldPath): The path to classify.

    Returns:
        ConfigFieldLocation | None: The location when ``path`` is at
            or below a file boundary; ``None`` when it sits above
            every boundary.

    Raises:
        ConfigurationPathError: See :func:`resolve_path`'s ``Raises``.
    """
    if not path:
        return None

    for kind in ConfigFileKind:
        prefix_len = len(kind.prefix)
        if len(path) < prefix_len:
            if kind.prefix.has_prefix(path):
                # Strict prefix of this kind's prefix (e.g.
                # ``community``): above the file boundary.
                return None
            continue
        if not path.has_prefix(kind.prefix):
            continue
        remainder = path.remove_prefix(kind.prefix)
        if not kind.named:
            return ConfigFieldLocation(kind=kind, name=None, field_path=remainder)
        if not remainder:
            # The collection itself (``community.sessions``): above
            # the file boundary.
            return None
        name = remainder.first
        try:
            validate_resource_name(name, field=f"{kind.prefix} name")
        except InvalidSessionNameError as exc:
            raise ConfigurationPathError(str(exc)) from exc
        if kind is ConfigFileKind.ENTERPRISE_SYSTEM and name == COMMUNITY_SYSTEM_NAME:
            raise ConfigurationPathError(
                f"'{COMMUNITY_SYSTEM_NAME}' is reserved for the community "
                "umbrella system and cannot be used as an enterprise "
                "system name."
            )
        return ConfigFieldLocation(
            kind=kind,
            name=name,
            field_path=remainder.remove_prefix(FieldPath((name,))),
        )

    # No registry prefix matched. Find the longest matched portion for
    # a targeted suggestion list.
    matched = (
        FieldPath((path.first,))
        if any(k.prefix.first == path.first for k in ConfigFileKind)
        else FieldPath.ROOT
    )
    suggestions = _example_paths_with_prefix(matched)
    _LOGGER.debug(
        f"[_logical_paths:_resolve_field_location] Rejecting: no file kind "
        f"matches path {str(path)!r}"
    )
    raise ConfigurationPathError(
        f"unknown configuration path {str(path)!r}. "
        f"Valid paths start with: {', '.join(suggestions)}. "
        "Run 'dhcli config files' to list every configuration file."
    )


def _example_paths_with_prefix(prefix: FieldPath) -> list[str]:
    """Return rendered example paths for every file kind with ``prefix``.

    Args:
        prefix (FieldPath): The matched portion of an unresolvable
            path, used to scope the suggestion list (``FieldPath.ROOT``
            suggests every file kind).

    Returns:
        list[str]: One example path per matching file kind, e.g.
            ``"community.sessions.<name>"`` for a named kind.
    """
    out: list[str] = []
    for kind in ConfigFileKind:
        if kind.prefix.has_prefix(prefix):
            example = str(kind.prefix) + (".<name>" if kind.named else "")
            out.append(example)
    return out
