"""``dhcli config`` noun group: inspect, validate, and author the configuration.

The group's verbs split along one line:

- **Runtime verbs** (``show``, ``validate``) read the fully-loaded,
  validated configuration tree and therefore require it to be valid.
- **Authoring verbs** (``files``, ``init``, ``edit``, ``get``, ``set``,
  ``unset``, ``keys``, and the ``session``/``system`` sub-groups) are
  declared ``needs_runtime=False`` and operate on the raw files, so
  they work on a broken or empty tree — exactly when they are needed.

The authoring verbs receive the cheap
:class:`~deephaven_mcp.cli._runtime.RuntimeSpec` on ``ctx.obj`` and
resolve what they need from it via the shared helpers below, rather
than reading a validated :class:`~deephaven_mcp.cli._runtime.Runtime`
(a fresh install cannot pass the full tree load).
"""

from __future__ import annotations

__all__ = ["config"]

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, assert_never, get_args

import click

from deephaven_mcp._exceptions import (
    ConfigurationError,
    ConfigurationFieldMissingError,
    ConfigurationPathError,
    FileLockTimeoutError,
    InternalError,
)
from deephaven_mcp._platform.fsutil import AdvisoryFileLock
from deephaven_mcp._pydantic import dump_redacted
from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._commands._wrapping import parse_key_value
from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode
from deephaven_mcp.cli._format import format_output
from deephaven_mcp.cli._help import (
    HelpEntry,
    HelpfulCommand,
    HelpfulGroup,
    HelpSpec,
    OutputField,
    OutputSpec,
    emit_payload,
)
from deephaven_mcp.cli._prompt import (
    can_prompt,
    confirm,
    prompt_optional,
    prompt_optional_int,
    prompt_text,
    require_choice,
    require_confirmation,
    require_value,
)
from deephaven_mcp.cli._runtime import Runtime, RuntimeSpec
from deephaven_mcp.config import resolve_config_dir
from deephaven_mcp.config._field_path import FieldPath
from deephaven_mcp.config._fields import get_field, has_field, set_field, unset_field
from deephaven_mcp.config._file_kinds import ConfigFileKind
from deephaven_mcp.config._logical_paths import (
    ConfigFieldLocation,
    ConfigSection,
    resolve_path,
)
from deephaven_mcp.config._settable_fields import settable_fields
from deephaven_mcp.config._store import ConfigStore

_LOGGER = logging.getLogger(__name__)

# Raw on-disk configuration is arbitrary JSON with no fixed shape, so the
# ``dict[str, Any]`` values throughout this module are genuinely open (per
# _python-coding-practices rule 6) rather than an under-specified type.


# ---------------------------------------------------------------------------
# shared helpers (used by 2+ verbs below)
# ---------------------------------------------------------------------------


_ENTITY_KIND_LABEL: dict[ConfigFileKind, str] = {
    ConfigFileKind.COMMUNITY_SESSION: "session",
    ConfigFileKind.ENTERPRISE_SYSTEM: "system",
}
"""Noun used in remediation messages for each named file kind."""


def _authoring_spec(ctx: click.Context) -> RuntimeSpec:
    """Return the :class:`RuntimeSpec` for a ``needs_runtime=False`` verb.

    Args:
        ctx (click.Context): The click context whose ``obj`` carries
            the spec stored by the root callback.

    Returns:
        RuntimeSpec: The per-invocation load recipe (directory
            overrides, output/timeout overrides, ``--no-input``).

    Raises:
        InternalError: When ``ctx.obj`` is not a :class:`RuntimeSpec`,
            which means the root callback did not run (a wiring bug).
    """
    obj = ctx.obj
    if isinstance(obj, RuntimeSpec):
        return obj
    raise InternalError(
        f"ctx.obj is {type(obj).__name__}, not RuntimeSpec; the root "
        "callback must run before an authoring verb resolves its spec."
    )


def _store_from_spec(spec: RuntimeSpec) -> ConfigStore:
    """Build the :class:`ConfigStore` for an authoring verb.

    Args:
        spec (RuntimeSpec): The per-invocation load recipe.

    Returns:
        ConfigStore: Bound to the explicit ``--config-dir`` override
            when given, otherwise the platform default
            (``$DH_AI_DATA_DIR/config`` or the user-data root's
            ``config`` subdirectory). The root is resolved to an
            absolute path so every emitted ``config_dir`` / ``file``
            payload honors its documented absolute-path contract even
            when ``--config-dir`` was given relative. Read
            :attr:`ConfigStore.config_dir` when a verb needs the raw
            directory (e.g. to enumerate a :class:`ConfigSection`).
    """
    return ConfigStore(resolve_config_dir(spec.config_dir_override).resolve())


_LOCK_FILENAME = ".dhcli.lock"
"""Name of the advisory write-lock file, held in the configuration
directory for the duration of every mutating ``config`` verb."""


@contextmanager
def _config_write_lock(ctx: click.Context) -> Iterator[None]:
    """Hold the configuration directory's advisory write lock.

    Every mutating ``config`` verb enters this on ``ctx`` (via
    :meth:`click.Context.with_resource`) before its first store access,
    so the lock spans the whole transaction — existence check, prompt,
    validation, and commit — closing the check-then-act race in which
    two concurrent invocations both observe an absent file and then
    both write it. The lock file lives at ``<config_dir>/.dhcli.lock``.

    When the configuration directory does not exist yet, no lock is
    taken: there is no on-disk state to serialize against (the first
    write creates the directory atomically), and creating the directory
    solely to place a lock file would leave an empty directory and lock
    file behind for a command that then fails before writing. Once the
    directory exists, every mutating verb locks normally.

    The lock file is created at owner-only mode (``0o600``) before
    :class:`AdvisoryFileLock` opens it: the configuration directory
    permission audit rejects any group/other-accessible file, and
    ``open('ab+')`` would otherwise create it at the umask default
    (commonly ``0o644``).

    Args:
        ctx (click.Context): The verb's click context, used to resolve
            the configuration directory from the per-invocation spec.

    Yields:
        None: Control returns to the command body, with the lock held
            when the configuration directory already exists.

    Raises:
        CliError: With :attr:`ErrorCode.CONFIG_LOCKED` when another
            process holds the lock past the acquisition timeout.
    """
    config_dir = _store_from_spec(_authoring_spec(ctx)).config_dir
    if not config_dir.exists():
        yield
        return
    lock_path = config_dir / _LOCK_FILENAME
    lock_path.touch(mode=0o600, exist_ok=True)
    lock_path.chmod(0o600)
    try:
        with AdvisoryFileLock(lock_path):
            yield
    except FileLockTimeoutError as exc:
        raise CliError(
            f"Another process holds the configuration write lock "
            f"({lock_path}); retry once it finishes.",
            code=ErrorCode.CONFIG_LOCKED,
        ) from exc


def _map_config_error(exc: ConfigurationError) -> CliError:
    """Translate a configuration-layer error to a structured CLI error.

    Args:
        exc (ConfigurationError): The error raised by the config
            path/read/write layer.

    Returns:
        CliError: With ``not_found`` for
            :class:`ConfigurationFieldMissingError` (the path is
            well-formed but nothing is set there), ``config_path_invalid``
            for every other :class:`ConfigurationPathError`, and
            ``config_invalid`` for every other :class:`ConfigurationError`.
    """
    match exc:
        case ConfigurationFieldMissingError():
            code = ErrorCode.NOT_FOUND
        case ConfigurationPathError():
            code = ErrorCode.CONFIG_PATH_INVALID
        case _:
            code = ErrorCode.CONFIG_INVALID
    return CliError(str(exc), code=code)


def _warn_template_resolution(warnings: list[str]) -> None:
    """Print template-resolution warnings to stderr.

    Args:
        warnings (list[str]): Messages from
            :meth:`deephaven_mcp.config._store.ConfigStore.validate`
            (unset env vars, missing secret files). Empty list prints
            nothing.
    """
    for message in warnings:
        click.echo(f"warning: {message}", err=True)


def _warn_restart_hint() -> None:
    """Remind the operator that a running daemon uses the old config."""
    click.echo(
        "note: a running daemon keeps its loaded configuration; run "
        "'dhcli daemon stop' so the next command picks up this change.",
        err=True,
    )


def _hint_literal_secret(value: str, flag: str) -> None:
    """Recommend a templating ref when a secret was supplied literally.

    Args:
        value (str): The secret value as supplied.
        flag (str): The flag it arrived on (for the hint text).
    """
    if "${" not in value:
        click.echo(
            f"hint: {flag} was given a literal value; consider a "
            "templating ref instead, e.g. '${env:MY_SECRET}' or "
            "'${file:/path/to/secret}', so the secret never sits in the "
            "config file.",
            err=True,
        )


def _reject_inapplicable(auth: str, **supplied: str | None) -> None:
    """Reject flags that do not apply to the chosen ``--auth`` type.

    Args:
        auth (str): The chosen auth type.
        **supplied: Mapping of flag spelling (with dashes as
            underscores) to the value the user supplied (``None`` when
            omitted). Any non-``None`` entry is rejected.

    Raises:
        CliError: With ``option_not_applicable`` naming the offending
            flags.
    """
    offending = [
        "--" + name.replace("_", "-")
        for name, value in supplied.items()
        if value is not None
    ]
    if offending:
        raise CliError(
            f"{', '.join(offending)} do{'es' if len(offending) == 1 else ''} "
            f"not apply to --auth {auth}.",
            code=ErrorCode.OPTION_NOT_APPLICABLE,
        )


def _resolve_entity(path: FieldPath) -> ConfigFieldLocation:
    """Resolve an entity's logical path to its file target.

    Args:
        path (FieldPath): The full logical path of the entity file
            (e.g. ``FieldPath(("community", "sessions", name))``).

    Returns:
        ConfigFieldLocation: The file-level target.

    Raises:
        CliError: With ``config_path_invalid`` when the name is
            malformed or reserved, or ``path`` names a section rather
            than a file.
    """
    try:
        resolved = resolve_path(path)
    except ConfigurationPathError as exc:
        raise _map_config_error(exc) from exc
    match resolved:
        case ConfigFieldLocation() as target:
            return target
        case ConfigSection():
            raise CliError(
                f"{path} does not name a configuration file.",
                code=ErrorCode.CONFIG_PATH_INVALID,
            )
        case _:
            assert_never(resolved)


def _resolve_field_target(path_text: str) -> ConfigFieldLocation:
    """Parse ``path_text`` and resolve it to a file-level target.

    Args:
        path_text (str): One logical path.

    Returns:
        ConfigFieldLocation: The resolved target (may have an empty
            ``field_path``, meaning the whole file).

    Raises:
        CliError: With ``config_path_invalid`` when the path is
            malformed or names a section/collection rather than a
            file.
    """
    try:
        resolved = resolve_path(FieldPath.parse(path_text))
    except ConfigurationPathError as exc:
        raise _map_config_error(exc) from exc
    match resolved:
        case ConfigFieldLocation() as target:
            return target
        case ConfigSection():
            raise CliError(
                f"{path_text} does not name a configuration file or field. Run "
                "'dhcli config keys' to list settable paths.",
                code=ErrorCode.CONFIG_PATH_INVALID,
            )
        case _:
            assert_never(resolved)


def _require_absent(
    store: ConfigStore, target: ConfigFieldLocation, *, kind: str
) -> Path:
    """Return the target's absolute path, failing when the file exists.

    Args:
        store (ConfigStore): The bound configuration store.
        target (ConfigFieldLocation): The entity file.
        kind (str): Entity kind for the error message (``"session"``
            or ``"system"``).

    Raises:
        CliError: With ``already_exists`` when the file is present.
    """
    path = store.path_of(target)
    if path.exists():
        raise CliError(
            f"{kind} '{target.name}' already exists at {path}. Remove it "
            f"first with 'dhcli config {kind} remove {target.name}'.",
            code=ErrorCode.ALREADY_EXISTS,
        )
    return path


def _require_present(
    store: ConfigStore, target: ConfigFieldLocation, *, kind: str
) -> Path:
    """Return the target's absolute path, failing when the file is absent.

    Args:
        store (ConfigStore): The bound configuration store.
        target (ConfigFieldLocation): The entity file.
        kind (str): Entity kind for the error message.

    Raises:
        CliError: With ``not_found`` when the file is missing.
    """
    path = store.path_of(target)
    if not path.is_file():
        raise CliError(
            f"{kind} '{target.name}' does not exist (no {path}). Run "
            f"'dhcli config {kind} list' to see the configured entries.",
            code=ErrorCode.NOT_FOUND,
        )
    return path


def _validity_fields(store: ConfigStore, target: ConfigFieldLocation) -> dict[str, Any]:
    """Read and validate one existing file, returning its status fields.

    Args:
        store (ConfigStore): The bound configuration store.
        target (ConfigFieldLocation): The file to read and validate.

    Returns:
        dict[str, Any]: ``valid`` (boolean), plus ``warnings`` when
            template refs could not be resolved locally and ``error``
            (first validation error) when invalid.
    """
    try:
        raw = store.read(target)
        warnings = store.validate(target, raw.data)
    except ConfigurationError as exc:
        return {"valid": False, "error": str(exc)}
    fields: dict[str, Any] = {"valid": True}
    if warnings:
        fields["warnings"] = warnings
    return fields


def _resolve_logical_path(path: str | None) -> ConfigFieldLocation | ConfigSection:
    """Parse and resolve a logical path (the whole tree when ``None``).

    Args:
        path (str | None): One logical path, or ``None`` for the root.

    Returns:
        ConfigFieldLocation | ConfigSection: The resolved file target or
            section.

    Raises:
        CliError: With ``config_path_invalid`` when the path is malformed
            or does not name a known location.
    """
    try:
        return resolve_path(FieldPath.parse(path) if path else FieldPath.ROOT)
    except ConfigurationPathError as exc:
        raise _map_config_error(exc) from exc


def _entity_status_entries(
    store: ConfigStore, prefix: FieldPath
) -> list[dict[str, Any]]:
    """Describe every entity file under ``prefix`` with validity status.

    Args:
        store (ConfigStore): The bound configuration store.
        prefix (FieldPath): The entity collection
            (``FieldPath(("community", "sessions"))`` or
            ``FieldPath(("enterprise", "systems"))``).

    Returns:
        list[dict[str, Any]]: One entry per file: ``name``, ``file``,
            ``valid``, plus ``error`` when invalid and ``warnings``
            when template refs could not be resolved locally.
    """
    entries: list[dict[str, Any]] = []
    for target in ConfigSection(prefix).files(store.config_dir):
        path = store.path_of(target)
        entry: dict[str, Any] = {"name": target.name, "file": str(path)}
        entry.update(_validity_fields(store, target))
        entries.append(entry)
    return entries


# ---------------------------------------------------------------------------
# the config group, show, validate
# ---------------------------------------------------------------------------


@click.group(cls=HelpfulGroup)
def config() -> None:
    """Inspect, validate, and author the configuration.

    The configuration is one logical JSON document addressed by
    dot-separated paths (e.g. 'cli.output.format',
    'community.settings.session_creation',
    'enterprise.systems.prod.auth.credentials'), stored across
    several files under the configuration directory. Use 'files' to
    see the file layout and per-file validity (works even when the
    configuration is broken), 'show' for the resolved runtime view
    (secrets redacted, requires a valid tree), and 'validate' as the
    explicit CI check. Authoring verbs never write an invalid file:
    every change is schema-validated before an atomic write. Every
    verb except 'show' and 'validate' operates on the raw files
    without loading the runtime, and so honors the root -o/--output
    flag and DHCLI_OUTPUT but not cli.json's output.format (the file
    may be the thing being inspected or repaired).
    """


_OUTPUT_SHOW = OutputSpec(
    "object",
    (
        OutputField(
            "config_dir", "string", "Directory the configuration was loaded from."
        ),
        OutputField("cli", "object", "dhcli CLI defaults (output, daemon, request)."),
        OutputField("server", "object", "Parsed server.json; omitted when absent."),
        OutputField("community", "object", "Community config; omitted when absent."),
        OutputField("enterprise", "object", "Enterprise config; omitted when absent."),
    ),
    note="Post-merge configuration; secret-bearing fields are redacted to ***.",
)
_OUTPUT_VALIDATE = OutputSpec(
    "object",
    (
        OutputField(
            "valid", "boolean", "Always true; a failure exits 2 before this prints."
        ),
        OutputField(
            "config_dir", "string", "Absolute path of the validated directory."
        ),
        OutputField("message", "string", "Human-readable confirmation."),
    ),
)


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------


@config.command(
    "show",
    help_spec=HelpSpec(
        summary="Print the resolved configuration with secrets redacted.",
        description=(
            "Shows the post-merge view used at runtime. Secret-bearing "
            "fields (passwords, API keys) are replaced with *** via "
            "the schema's redaction hooks. Requires a valid configuration "
            "tree; 'dhcli config get' works on a partial or invalid one "
            "and shows the raw on-disk values instead."
        ),
        arguments=(
            HelpEntry(
                "PATH",
                "Optional dot-separated logical path into the resolved "
                "tree (e.g. 'community.settings'). The resolved shape "
                "matches the wire format 'config get' uses, plus "
                "loader-injected fields such as each entity's 'name'.",
            ),
        ),
        output=_OUTPUT_SHOW,
        examples=(
            "$ dhcli config show",
            "$ dhcli config show community.settings",
            "$ dhcli -o json config show | jq .community",
        ),
        see_also=("dhcli config validate", "dhcli config get"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.CONFIG_INVALID,
            ErrorCode.NO_SYSTEMS_CONFIGURED,
            ErrorCode.CONFIG_PATH_INVALID,
            ErrorCode.NOT_FOUND,
        ),
    ),
)
@click.argument("path", required=False, default=None)
@click.pass_obj
@run_async
async def config_show(runtime: Runtime, path: str | None) -> None:
    """Print the resolved configuration with secrets redacted."""
    payload = dump_redacted(runtime.config, exclude_none=True)
    if path:
        try:
            payload = get_field(payload, FieldPath.parse(path))
        except ConfigurationPathError as exc:
            raise _map_config_error(exc) from exc
    click.echo(format_output(payload, output=runtime.config.cli.output.format))


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


@config.command(
    "validate",
    help_spec=HelpSpec(
        summary="Confirm the configuration is valid (exit 0 / 2).",
        description=(
            "Validation runs before every runtime-dependent command body, "
            "so a malformed file exits 2 with config_invalid — and a valid "
            "tree that declares no systems exits 2 with "
            "no_systems_configured — before this command prints. Paths that "
            "print without running a body (--help, --agents, the agents "
            "verbs) skip it, as do the offline config authoring/inspection "
            "verbs (needs_runtime=False, e.g. get/set/files), which operate "
            "on files directly and never trigger a full-tree load. This "
            "verb performs no extra work: when the load succeeds it emits a "
            "CI-friendly 'valid: true' payload. Use it as the explicit "
            "config check in CI pipelines."
        ),
        output=_OUTPUT_VALIDATE,
        examples=("$ dhcli config validate",),
        see_also=("dhcli config show",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(ErrorCode.CONFIG_INVALID, ErrorCode.NO_SYSTEMS_CONFIGURED),
    ),
)
@click.pass_obj
@run_async
async def config_validate(runtime: Runtime) -> None:
    """Confirm the configuration directory is valid.

    Always succeeds: a malformed tree exits with
    :attr:`ErrorCode.CONFIG_INVALID` (and a systems-less one with
    :attr:`ErrorCode.NO_SYSTEMS_CONFIGURED`) during runtime
    materialization, before this handler runs. Emits the
    ``valid: true`` payload in the active output mode.
    """
    payload = {
        "valid": True,
        "config_dir": str(runtime.config_dir),
        "message": "Configuration validated successfully.",
    }
    click.echo(format_output(payload, output=runtime.config.cli.output.format))


# ---------------------------------------------------------------------------
# get / set / unset / keys: the logical-path field editor
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------

_OUTPUT_GET = OutputSpec(
    "text",
    note=(
        "The raw value at PATH (or the whole tree when PATH is omitted): a "
        "JSON object for a subtree, or the bare scalar for a leaf. "
        "Templating refs ('${env:VAR}') are shown unexpanded — this is the "
        "on-disk view, not the resolved one ('dhcli config show')."
    ),
)


@click.command(
    "get",
    cls=HelpfulCommand,
    help_spec=HelpSpec(
        summary="Print the raw configuration at a logical path.",
        description=(
            "Reads directly from disk without expanding templating refs, so "
            "it works even on a partial or invalid tree ('dhcli config "
            "show' requires a valid tree and shows the resolved values "
            "instead). With no PATH, prints the whole logical "
            "configuration tree, assembled from every file. A PATH at a "
            "section (e.g. 'community') aggregates every file under it; a "
            "PATH at or below a file (e.g. 'cli.output.format') reads just "
            "that file."
        ),
        arguments=(
            HelpEntry(
                "PATH",
                "Optional dot-separated logical path (e.g. "
                "'community.sessions.local_dev.host'). A segment "
                "containing a literal dot is double-quoted "
                "('a.\"b.c\".d'). Run 'dhcli config keys' to discover "
                "valid paths.",
            ),
        ),
        output=_OUTPUT_GET,
        examples=(
            "$ dhcli config get",
            "$ dhcli config get community.settings",
            "$ dhcli -o human config get cli.output.format",
        ),
        see_also=(
            "dhcli config keys",
            "dhcli config set",
            "dhcli config show",
            "dhcli config files",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.CONFIG_PATH_INVALID,
            ErrorCode.NOT_FOUND,
            ErrorCode.CONFIG_INVALID,
        ),
    ),
    needs_runtime=False,
)
@click.argument("path", required=False, default=None)
@click.pass_context
@run_async
async def config_get(ctx: click.Context, path: str | None) -> None:
    """Print the raw configuration at a logical path."""
    spec = _authoring_spec(ctx)
    store = _store_from_spec(spec)
    resolved = _resolve_logical_path(path)
    match resolved:
        case ConfigFieldLocation() as target:
            emit_payload(ctx, _read_field(store, target))
        case ConfigSection() as section:
            emit_payload(ctx, _read_section_tree(store, section))
        case _:
            assert_never(resolved)


def _read_field(store: ConfigStore, target: ConfigFieldLocation) -> Any:
    """Return the raw value at ``target`` (whole file or one field).

    Args:
        store (ConfigStore): The bound configuration store.
        target (ConfigFieldLocation): The file, plus the field within it
            (:attr:`FieldPath.ROOT` for the whole file).

    Returns:
        Any: The raw on-disk value, templating refs unexpanded.

    Raises:
        CliError: With ``not_found`` when the file or field is absent,
            or ``config_invalid`` when the file cannot be parsed.
    """
    file_path = store.path_of(target)
    if not file_path.is_file():
        raise CliError(
            f"{target.logical_path} does not exist ({file_path}).",
            code=ErrorCode.NOT_FOUND,
        )
    try:
        raw = store.read(target)
    except ConfigurationError as exc:
        raise _map_config_error(exc) from exc
    try:
        return get_field(raw.data, target.field_path)
    except ConfigurationPathError as exc:
        raise _map_config_error(exc) from exc


def _read_section_tree(store: ConfigStore, section: ConfigSection) -> dict[str, Any]:
    """Aggregate every existing file below ``section`` into one tree.

    Args:
        store (ConfigStore): The bound configuration store.
        section (ConfigSection): The section to aggregate.

    Returns:
        dict[str, Any]: The raw on-disk data of each existing file,
            nested at its logical path relative to the section.

    Raises:
        CliError: With ``config_invalid`` when a file cannot be parsed.
    """
    tree: dict[str, Any] = {}
    for target in section.files(store.config_dir):
        file_path = store.path_of(target)
        if not file_path.is_file():
            continue
        try:
            raw = store.read(target)
        except ConfigurationError as exc:
            raise _map_config_error(exc) from exc
        tree = set_field(
            tree, target.logical_path.remove_prefix(section.prefix), raw.data
        )
    return tree


# ---------------------------------------------------------------------------
# shared: resolve one path token to its file target, with entity guards
# ---------------------------------------------------------------------------


def _load_for_rewrite(
    store: ConfigStore, target: ConfigFieldLocation
) -> dict[str, Any]:
    """Return the target file's raw data for a ``set``/``unset`` rewrite.

    An absent unnamed-kind file (``cli.json``, ``server.json``, the
    settings files) is treated as ``{}`` — those kinds have no
    required fields and are created on first write. An absent
    named-kind file is an error: ``set``/``unset`` edit existing
    entities, they do not create sessions or systems.

    Args:
        store (ConfigStore): The bound configuration store.
        target (ConfigFieldLocation): The file to load.

    Returns:
        dict[str, Any]: The file's current raw contents, or ``{}``.

    Raises:
        CliError: With ``not_found`` when a named-kind file is absent.
        CliError: With ``config_invalid`` when the file cannot be
            parsed.
        CliError: With ``config_not_rewritable`` when the file uses
            JSON5-only syntax (comments, trailing commas) that a
            programmatic rewrite would destroy.
    """
    file_path = store.path_of(target)
    if file_path.is_file():
        try:
            raw = store.read(target)
        except ConfigurationError as exc:
            raise _map_config_error(exc) from exc
        if not raw.strict_json:
            raise CliError(
                f"{file_path} uses JSON5-only syntax (comments, trailing "
                "commas) that a programmatic rewrite would destroy. Edit "
                "it directly, or with 'dhcli config edit', instead.",
                code=ErrorCode.CONFIG_NOT_REWRITABLE,
            )
        return raw.data
    if target.kind.named:
        label = _ENTITY_KIND_LABEL[target.kind]
        raise CliError(
            f"{label} {target.name!r} does not exist. Create it first with "
            f"'dhcli config {label} add {target.name}'.",
            code=ErrorCode.NOT_FOUND,
        )
    return {}


# ---------------------------------------------------------------------------
# set
# ---------------------------------------------------------------------------

_OUTPUT_SET = OutputSpec(
    "object",
    (
        OutputField("paths", "array", "The logical paths that were set."),
        OutputField("files", "array", "Absolute paths of the files written."),
    ),
    note="All assignments in one invocation land atomically across every file they touch.",
)


@click.command(
    "set",
    cls=HelpfulCommand,
    help_spec=HelpSpec(
        summary="Set one or more configuration fields.",
        description=(
            "Each ASSIGNMENT is PATH=VALUE; VALUE is parsed as JSON first "
            "(numbers, booleans, arrays, objects), falling back to a plain "
            "string. Intermediate objects are created as needed. A PATH "
            "naming a whole file takes a JSON object that replaces the "
            "file's contents outright (assignment, not a merge). "
            "Assignments may span multiple files; every touched file is "
            "schema-validated, then all are written atomically in one "
            "batch. 'set' edits an existing entity — it cannot create a "
            "new session or system (use 'config session/system add' for "
            "that); unnamed files (cli.json, server.json, "
            "community/settings.json, enterprise/settings.json) are "
            "created on first write."
        ),
        arguments=(
            HelpEntry(
                "ASSIGNMENT",
                "One or more PATH=VALUE tokens, e.g. "
                "'cli.output.format=human' or "
                "'community.sessions.local_dev.port=10001'.",
            ),
        ),
        output=_OUTPUT_SET,
        examples=(
            "$ dhcli config set cli.output.format=human",
            "$ dhcli config set community.settings.session_creation.max_concurrent_sessions=3",
            "$ dhcli config set enterprise.systems.prod.session_creation.defaults.heap_size_gb=8",
        ),
        see_also=(
            "dhcli config keys",
            "dhcli config get",
            "dhcli config unset",
            "dhcli config session add",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.ARG_PARSE_ERROR,
            ErrorCode.CONFIG_PATH_INVALID,
            ErrorCode.NOT_FOUND,
            ErrorCode.CONFIG_INVALID,
            ErrorCode.CONFIG_NOT_REWRITABLE,
            ErrorCode.CONFIG_LOCKED,
        ),
    ),
    needs_runtime=False,
)
@click.argument("assignment", nargs=-1, required=True)
@click.pass_context
@run_async
async def config_set(ctx: click.Context, assignment: tuple[str, ...]) -> None:
    """Set one or more configuration fields."""
    ctx.with_resource(_config_write_lock(ctx))
    spec = _authoring_spec(ctx)
    store = _store_from_spec(spec)
    groups: dict[
        tuple[ConfigFileKind, str | None], tuple[ConfigFieldLocation, dict[str, Any]]
    ] = {}
    rendered_paths: list[str] = []

    for token in assignment:
        path_text, value = parse_key_value(token, decode_json=True)
        target = _resolve_field_target(path_text)
        rendered_paths.append(str(target.logical_path + target.field_path))
        key = (target.kind, target.name)
        if key in groups:
            _, data = groups[key]
        else:
            data = _load_for_rewrite(store, target)
        try:
            data = set_field(data, target.field_path, value)
        except ConfigurationPathError as exc:
            raise _map_config_error(exc) from exc
        groups[key] = (target, data)

    entries = list(groups.values())
    try:
        warnings = store.write_all(entries)
    except ConfigurationError as exc:
        raise _map_config_error(exc) from exc
    _warn_template_resolution(warnings)
    _warn_restart_hint()
    emit_payload(
        ctx,
        {
            "paths": rendered_paths,
            "files": [str(store.path_of(t)) for t, _ in entries],
        },
    )


# ---------------------------------------------------------------------------
# unset
# ---------------------------------------------------------------------------

_OUTPUT_UNSET = OutputSpec(
    "object",
    (
        OutputField("paths", "array", "The logical paths that were unset."),
        OutputField("files", "array", "Absolute paths of the files written."),
    ),
    note="All removals in one invocation land atomically across every file they touch.",
)


@click.command(
    "unset",
    cls=HelpfulCommand,
    help_spec=HelpSpec(
        summary="Remove one or more configuration fields, reverting to default.",
        description=(
            "Removes each PATH from its file; the field then falls back to "
            "its schema default. Works only on fields below a file boundary "
            "— unsetting a whole file (e.g. a session or system) is not "
            "supported here; use 'config session/system remove' for that."
        ),
        arguments=(
            HelpEntry(
                "PATH",
                "One or more logical field paths to remove, e.g. "
                "'community.settings.session_creation.max_concurrent_sessions'.",
            ),
        ),
        output=_OUTPUT_UNSET,
        examples=("$ dhcli config unset cli.request.timeouts.default_seconds",),
        see_also=(
            "dhcli config keys",
            "dhcli config get",
            "dhcli config set",
            "dhcli config session remove",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.CONFIG_PATH_INVALID,
            ErrorCode.NOT_FOUND,
            ErrorCode.CONFIG_INVALID,
            ErrorCode.CONFIG_NOT_REWRITABLE,
            ErrorCode.CONFIG_LOCKED,
        ),
    ),
    needs_runtime=False,
)
@click.argument("path", nargs=-1, required=True)
@click.pass_context
@run_async
async def config_unset(ctx: click.Context, path: tuple[str, ...]) -> None:
    """Remove one or more configuration fields, reverting to default."""
    ctx.with_resource(_config_write_lock(ctx))
    spec = _authoring_spec(ctx)
    store = _store_from_spec(spec)
    groups: dict[
        tuple[ConfigFileKind, str | None], tuple[ConfigFieldLocation, dict[str, Any]]
    ] = {}
    rendered_paths: list[str] = []

    for path_text in path:
        target = _resolve_field_target(path_text)
        if not target.field_path:
            label = _ENTITY_KIND_LABEL.get(target.kind)
            hint = (
                f"Use 'dhcli config {label} remove {target.name}' instead."
                if label
                else "Edit the file with 'dhcli config set', or remove "
                "individual fields below it."
            )
            raise CliError(
                f"{path_text} names a whole file, not a field. {hint}",
                code=ErrorCode.CONFIG_PATH_INVALID,
            )
        rendered_paths.append(str(target.logical_path + target.field_path))
        key = (target.kind, target.name)
        if key in groups:
            _, data = groups[key]
        else:
            data = _load_for_rewrite(store, target)
        try:
            data = unset_field(data, target.field_path)
        except ConfigurationPathError as exc:
            raise _map_config_error(exc) from exc
        groups[key] = (target, data)

    entries = list(groups.values())
    try:
        warnings = store.write_all(entries)
    except ConfigurationError as exc:
        raise _map_config_error(exc) from exc
    _warn_template_resolution(warnings)
    _warn_restart_hint()
    emit_payload(
        ctx,
        {
            "paths": rendered_paths,
            "files": [str(store.path_of(t)) for t, _ in entries],
        },
    )


# ---------------------------------------------------------------------------
# keys
# ---------------------------------------------------------------------------

_OUTPUT_KEYS = OutputSpec(
    "object",
    (
        OutputField(
            "keys",
            "array",
            "One entry per settable path: 'path', 'type' (string, integer, "
            "number, boolean, array, or object), 'required' (boolean; "
            "only when true), 'secret' (boolean; only when true), "
            "'default' (only when the field has a plain scalar default), "
            "'description' (only when documented), 'present' (boolean; "
            "only when true, and only for a path resolved against a real "
            "entity, not a '<name>' template). Nested blocks appear as "
            "'object' entries alongside their children; either level is "
            "a valid 'config set' target.",
        ),
    ),
)


def _render_settable_fields(
    kind: ConfigFileKind,
    base: FieldPath,
    *,
    raw: dict[str, Any] | None,
    field_prefix: FieldPath = FieldPath.ROOT,
) -> list[dict[str, Any]]:
    """Render one kind's settable paths, optionally scoped to a sub-path.

    Args:
        kind (ConfigFileKind): The file kind to describe.
        base (FieldPath): Logical path to render full paths under
            (the file's own logical path, or that plus ``"<name>"``
            for a template listing).
        raw (dict[str, Any] | None): The file's raw contents, when
            known and readable, to populate ``present``; ``None``
            omits ``present`` entirely.
        field_prefix (FieldPath): Restrict to entries whose in-file
            field path starts with this prefix (used when the caller
            asked for a sub-path within the file).

    Returns:
        list[dict[str, Any]]: Rendered key entries.
    """
    out: list[dict[str, Any]] = []
    for entry in settable_fields(kind):
        if not entry.path.has_prefix(field_prefix):
            continue
        item: dict[str, Any] = {
            "path": str(base + entry.path),
            "type": entry.json_type,
        }
        if entry.required:
            item["required"] = True
        if entry.secret:
            item["secret"] = True
        if entry.default is not None:
            item["default"] = entry.default
        if entry.description:
            item["description"] = entry.description
        if raw is not None and has_field(raw, entry.path):
            item["present"] = True
        out.append(item)
    return out


def _read_raw_if_present(
    store: ConfigStore, target: ConfigFieldLocation
) -> dict[str, Any] | None:
    """Return a file's raw data, or ``None`` when absent or unreadable."""
    if not store.path_of(target).is_file():
        return None
    try:
        return store.read(target).data
    except ConfigurationError:
        return None


@click.command(
    "keys",
    cls=HelpfulCommand,
    help_spec=HelpSpec(
        summary="List every settable configuration path, schema-generated.",
        description=(
            "Generated from the Pydantic schemas, so it can never drift "
            "from what 'config set' actually accepts. With no PATH, lists "
            "every path across every file kind; named kinds (sessions, "
            "systems) are shown once as a '<name>' template. Nested "
            "blocks appear as 'object' entries (settable as whole JSON "
            "objects) alongside their leaf children. Two settable target "
            "families are not enumerated here: a whole file (its logical "
            "path; run 'config files') and free-form children below an "
            "open 'object' entry such as environment_vars. A PATH scopes "
            "the listing to one file kind or one field within it, and — "
            "when it names a real entity rather than a template — adds a "
            "'present' flag showing which paths are currently set."
        ),
        arguments=(
            HelpEntry(
                "PATH",
                "Optional logical path to scope the listing, e.g. "
                "'community.sessions.local_dev' or 'server'.",
            ),
        ),
        output=_OUTPUT_KEYS,
        examples=(
            "$ dhcli config keys",
            "$ dhcli config keys cli",
            "$ dhcli -o json config keys community.sessions.local_dev | jq -r '.keys[].path'",
        ),
        see_also=("dhcli config get", "dhcli config set", "dhcli config unset"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(ErrorCode.CONFIG_PATH_INVALID,),
    ),
    needs_runtime=False,
)
@click.argument("path", required=False, default=None)
@click.pass_context
@run_async
async def config_keys(ctx: click.Context, path: str | None) -> None:
    """List every settable configuration path, schema-generated."""
    spec = _authoring_spec(ctx)
    store = _store_from_spec(spec)
    resolved = _resolve_logical_path(path)
    keys: list[dict[str, Any]] = []
    match resolved:
        case ConfigFieldLocation() as target:
            raw = _read_raw_if_present(store, target)
            keys = _render_settable_fields(
                target.kind,
                target.logical_path,
                raw=raw,
                field_prefix=target.field_path,
            )
        case ConfigSection() as section:
            for file_kind in section.kinds():
                if file_kind.named:
                    keys.extend(
                        _render_settable_fields(
                            file_kind, file_kind.prefix + "<name>", raw=None
                        )
                    )
                else:
                    probe = ConfigFieldLocation(
                        kind=file_kind, name=None, field_path=FieldPath.ROOT
                    )
                    raw = _read_raw_if_present(store, probe)
                    keys.extend(
                        _render_settable_fields(file_kind, file_kind.prefix, raw=raw)
                    )
        case _:
            assert_never(resolved)
    emit_payload(ctx, {"keys": keys})


# ---------------------------------------------------------------------------
# edit: open one whole configuration file in $EDITOR
# ---------------------------------------------------------------------------


_OUTPUT_EDIT = OutputSpec(
    "object",
    (
        OutputField("file", "string", "Absolute path of the file that was edited."),
        OutputField(
            "changed",
            "boolean",
            "Whether the saved text differs from what was opened.",
        ),
    ),
    note=(
        "Closing the editor without saving, or saving identical content, "
        "exits 0 with changed: false and writes nothing."
    ),
)


def _open_editor(text: str) -> str | None:
    """Open ``text`` in ``$EDITOR``/``$VISUAL`` and return the saved result.

    Args:
        text (str): The initial file contents shown to the operator.

    Returns:
        str | None: The saved text, or ``None`` when the editor was
            unavailable, exited without saving, or the operator
            aborted the edit (``click.edit``'s convention).
    """
    return click.edit(text=text, extension=".json")


@click.command(
    "edit",
    cls=HelpfulCommand,
    help_spec=HelpSpec(
        summary="Edit one whole configuration file in $EDITOR.",
        description=(
            "Opens the file named by PATH in $EDITOR (or $VISUAL, falling "
            "back to a platform default) and writes back exactly what was "
            "saved — comments and formatting included — so a file relying "
            "on JSON5-only syntax that 'config set'/'unset' refuse to "
            "touch can still be edited here. The saved text is parsed and "
            "schema-validated before anything is written; a failure "
            "leaves the file untouched and reports the parse or "
            "validation error. Interactive only: requires a TTY and is "
            "unavailable with --no-input."
        ),
        arguments=(
            HelpEntry(
                "PATH",
                "The file to edit, e.g. 'cli', 'community.settings', or "
                "'community.sessions.local_dev'. Must name a whole file "
                "(run 'dhcli config files' to see the layout), not a "
                "field within one — use 'config set' for that.",
            ),
        ),
        output=_OUTPUT_EDIT,
        examples=(
            "$ dhcli config edit cli",
            "$ dhcli config edit community.sessions.local_dev",
        ),
        see_also=(
            "dhcli config files",
            "dhcli config get",
            "dhcli config set",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.CONFIG_PATH_INVALID,
            ErrorCode.NOT_FOUND,
            ErrorCode.CONFIG_INVALID,
            ErrorCode.NO_TTY,
            ErrorCode.CONFIG_LOCKED,
        ),
    ),
    needs_runtime=False,
)
@click.argument("path")
@click.pass_context
@run_async
async def config_edit(ctx: click.Context, path: str) -> None:
    """Edit one whole configuration file in $EDITOR."""
    spec = _authoring_spec(ctx)
    if not can_prompt(no_input=spec.no_input):
        raise CliError(
            "'config edit' is interactive-only: it requires a TTY and is "
            "unavailable with --no-input. Use 'config set'/'config get' "
            "for non-interactive field edits.",
            code=ErrorCode.NO_TTY,
        )
    ctx.with_resource(_config_write_lock(ctx))
    store = _store_from_spec(spec)
    target = _resolve_field_target(path)
    if target.field_path:
        raise CliError(
            f"{path} names a field within a file, not a whole file. "
            "'config edit' operates on whole files — use 'config set'/"
            "'config get' for individual fields.",
            code=ErrorCode.CONFIG_PATH_INVALID,
        )

    file_path = store.path_of(target)
    if file_path.is_file():
        try:
            original_text = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise CliError(
                f"Cannot read {file_path}: {exc}", code=ErrorCode.CONFIG_INVALID
            ) from exc
    elif target.kind.named:
        label = _ENTITY_KIND_LABEL[target.kind]
        raise CliError(
            f"{label} {target.name!r} does not exist. Create it first "
            f"with 'dhcli config {label} add {target.name}'.",
            code=ErrorCode.NOT_FOUND,
        )
    else:
        original_text = "{}\n"

    edited_text = _open_editor(original_text)
    if edited_text is None or edited_text == original_text:
        emit_payload(ctx, {"file": str(file_path), "changed": False})
        return

    try:
        warnings = store.write_text(target, edited_text)
    except ConfigurationError as exc:
        raise _map_config_error(exc) from exc
    _warn_template_resolution(warnings)
    _warn_restart_hint()
    emit_payload(ctx, {"file": str(file_path), "changed": True})


# ---------------------------------------------------------------------------
# files: map the logical configuration tree to disk
# ---------------------------------------------------------------------------


_OUTPUT_FILES = OutputSpec(
    "object",
    (
        OutputField("config_dir", "string", "The resolved configuration directory."),
        OutputField(
            "files",
            "array",
            "One entry per configuration file: 'path' (logical path, the "
            "address 'config get/set' use; omitted when the filename is "
            "not addressable as a path), 'file' (absolute on-disk path), "
            "'exists' (boolean), 'valid' (boolean; only when the file "
            "exists), 'error' (first validation error, or why the "
            "filename is not addressable; only when invalid), "
            "'warnings' (template-resolution notes such as an unset env "
            "var; only when present).",
        ),
    ),
    note=(
        "Files that hold one session or system appear once per existing "
        "file; new ones can be created at community.sessions.<name> / "
        "enterprise.systems.<name>. Absent keys mean false/empty."
    ),
)


@click.command(
    "files",
    cls=HelpfulCommand,
    needs_runtime=False,
    help_spec=HelpSpec(
        summary="List every configuration file with its logical path and status.",
        description=(
            "Shows where the logical configuration tree is stored on disk: "
            "each file's logical path (the address used by 'config get', "
            "'config set', and friends), its absolute file path, whether it "
            "exists, and whether its contents validate. Works even when the "
            "configuration is broken or empty, so it is the first command "
            "to run when diagnosing configuration problems."
        ),
        output=_OUTPUT_FILES,
        examples=(
            "$ dhcli config files",
            "$ dhcli -o json config files | jq '.files[] | select(.valid == false)'",
        ),
        see_also=(
            "dhcli config show",
            "dhcli config validate",
            "dhcli config get",
        ),
        exit_codes=(ExitCode.SUCCESS,),
    ),
)
@click.pass_context
@run_async
async def config_files(ctx: click.Context) -> None:
    """List every configuration file with its logical path and status."""
    spec = _authoring_spec(ctx)
    store = _store_from_spec(spec)
    entries: list[dict[str, Any]] = []
    for target in ConfigSection(FieldPath.ROOT).files(store.config_dir):
        path = store.path_of(target)
        try:
            # Rejects a filename stem that is not addressable as a
            # logical path (illegal characters, reserved names); such
            # a file is listed as invalid rather than crashing the
            # diagnostic listing.
            resolve_path(target.logical_path)
            logical = str(target.logical_path)
        except ConfigurationPathError as exc:
            entries.append(
                {
                    "file": str(path),
                    "exists": path.is_file(),
                    "valid": False,
                    "error": f"filename is not addressable as a "
                    f"configuration path (rename the file): {exc}",
                }
            )
            continue
        entry: dict[str, Any] = {
            "path": logical,
            "file": str(path),
            "exists": path.is_file(),
        }
        if entry["exists"]:
            entry.update(_validity_fields(store, target))
        entries.append(entry)
    emit_payload(ctx, {"config_dir": str(store.config_dir), "files": entries})


# ---------------------------------------------------------------------------
# init: guided first-time setup wizard
# ---------------------------------------------------------------------------


_OUTPUT_INIT = OutputSpec(
    "object",
    (
        OutputField(
            "community_session",
            "object",
            "Summary ('name', 'path', 'file') of the session created; "
            "omitted when the operator declined.",
        ),
        OutputField(
            "enterprise_system",
            "object",
            "Summary ('name', 'path', 'file') of the system created; "
            "omitted when the operator declined.",
        ),
    ),
    note=(
        "Both fields are omitted when the operator declines both prompts; "
        "'dhcli config files' still lists the empty configuration."
    ),
)


@click.command(
    "init",
    cls=HelpfulCommand,
    help_spec=HelpSpec(
        summary="Guided wizard for a first-time configuration.",
        description=(
            "Walks through creating one community session and/or one "
            "enterprise system, prompting for each on stderr — the same "
            "prompts as 'config session add'/'config system add' with no "
            "flags supplied. Skips either section on request. Every field "
            "it writes is schema-validated before an atomic write, so an "
            "interrupted or invalid answer never leaves a broken file. "
            "Interactive only: requires a TTY and is unavailable with "
            "--no-input — there is nothing to automate here; use 'config "
            "session add'/'config system add' with flags for scripted "
            "setup."
        ),
        output=_OUTPUT_INIT,
        examples=("$ dhcli config init",),
        see_also=(
            "dhcli config session add",
            "dhcli config system add",
            "dhcli config files",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.NO_TTY,
            ErrorCode.CONFIG_PATH_INVALID,
            ErrorCode.ALREADY_EXISTS,
            ErrorCode.OPTION_NOT_APPLICABLE,
            ErrorCode.CONFIG_INVALID,
            ErrorCode.CONFIG_LOCKED,
        ),
    ),
    needs_runtime=False,
)
@click.pass_context
@run_async
async def config_init(ctx: click.Context) -> None:
    """Guided wizard for a first-time configuration."""
    spec = _authoring_spec(ctx)
    if not can_prompt(no_input=spec.no_input):
        raise CliError(
            "'config init' is interactive-only: it requires a TTY and is "
            "unavailable with --no-input. Use 'config session add'/'config "
            "system add' with flags for scripted setup.",
            code=ErrorCode.NO_TTY,
        )

    ctx.with_resource(_config_write_lock(ctx))

    click.echo(
        "This wizard declares configuration files under the configuration "
        "directory; it does not contact any server.",
        err=True,
    )

    payload: dict[str, Any] = {}

    if confirm(
        "Configure a Community session now?", no_input=spec.no_input, default=True
    ):
        name = prompt_text("Session name", no_input=spec.no_input, default="local")
        payload["community_session"] = await _add_session_entity(ctx, name)

    if confirm(
        "Configure an Enterprise system now?", no_input=spec.no_input, default=False
    ):
        name = prompt_text("System name", no_input=spec.no_input)
        payload["enterprise_system"] = await _add_system_entity(ctx, name)

    emit_payload(ctx, payload)


# ---------------------------------------------------------------------------
# session sub-group: declare, remove, and list community session files
# ---------------------------------------------------------------------------


SessionAuthType = Literal["anonymous", "psk", "password", "custom"]
"""Community-session auth types, matching what CoreSession accepts."""

_SESSION_AUTH_CHOICES: tuple[SessionAuthType, ...] = get_args(SessionAuthType)
"""Runtime tuple derived from :data:`SessionAuthType` for click.Choice and prompts."""


@click.group("session", cls=HelpfulGroup)
def config_session() -> None:
    """Declare, remove, and list community session files.

    Each declared session is one file at
    community/sessions/<name>.json describing how to connect to an
    existing Deephaven Community server. These verbs edit those files;
    they do not talk to any server. For the live sessions the daemon
    can see, use 'dhcli session list' instead. Values may be
    templating refs like '${env:VAR}' or '${file:/path}', stored
    verbatim and resolved when the server loads the file.
    """


_OUTPUT_SESSION_ADD = OutputSpec(
    "object",
    (
        OutputField("name", "string", "The session name (filename stem)."),
        OutputField("path", "string", "Logical path (community.sessions.<name>)."),
        OutputField("file", "string", "Absolute path of the created file."),
    ),
)
_OUTPUT_SESSION_REMOVE = OutputSpec(
    "object",
    (
        OutputField("name", "string", "The removed session name."),
        OutputField("file", "string", "Absolute path of the deleted file."),
    ),
)
_OUTPUT_SESSION_LIST = OutputSpec(
    "object",
    (
        OutputField(
            "sessions",
            "array",
            "One entry per declared session file: 'name', 'file', 'valid' "
            "(boolean), 'error' (first validation error; only when invalid), "
            "'warnings' (template refs unresolvable in this shell; only "
            "when present).",
        ),
    ),
)


# ---------------------------------------------------------------------------
# add
# ---------------------------------------------------------------------------


def _build_session_credentials(
    *,
    auth: SessionAuthType,
    token: str | None,
    username: str | None,
    password: str | None,
    effective_user: str | None,
    auth_type: str | None,
    auth_token: str | None,
    no_input: bool,
) -> dict[str, Any]:
    """Assemble the wire-format credentials dict for the chosen auth type.

    Rejects flags that do not apply to ``auth``, prompts for missing
    required values on a TTY, and emits literal-secret hints.

    Args:
        auth (SessionAuthType): One of ``anonymous``/``psk``/``password``/``custom``.
        token (str | None): ``--token`` (psk only).
        username (str | None): ``--username`` (password only).
        password (str | None): ``--password`` (password only).
        effective_user (str | None): ``--effective-user`` (password only).
        auth_type (str | None): ``--auth-type`` (custom only).
        auth_token (str | None): ``--auth-token`` (custom only).
        no_input (bool): The root ``--no-input`` flag.

    Returns:
        dict[str, Any]: The ``auth.credentials`` block contents.

    Raises:
        CliError: On an inapplicable flag pairing or a missing value
            that cannot be prompted for.
    """
    match auth:
        case "anonymous":
            _reject_inapplicable(
                auth,
                token=token,
                username=username,
                password=password,
                effective_user=effective_user,
                auth_type=auth_type,
                auth_token=auth_token,
            )
            return {"type": "anonymous"}
        case "psk":
            _reject_inapplicable(
                auth,
                username=username,
                password=password,
                effective_user=effective_user,
                auth_type=auth_type,
                auth_token=auth_token,
            )
            token = require_value(
                token,
                flag="--token",
                label="Pre-shared key (or a ref like ${env:MY_PSK})",
                no_input=no_input,
                hide=True,
            )
            _hint_literal_secret(token, "--token")
            return {"type": "psk", "token": token}
        case "password":
            _reject_inapplicable(
                auth, token=token, auth_type=auth_type, auth_token=auth_token
            )
            username = require_value(
                username, flag="--username", label="Username", no_input=no_input
            )
            password = require_value(
                password,
                flag="--password",
                label="Password (or a ref like ${env:MY_PASSWORD})",
                no_input=no_input,
                hide=True,
            )
            _hint_literal_secret(password, "--password")
            credentials: dict[str, Any] = {
                "type": "password",
                "username": username,
                "password": password,
            }
            if effective_user is not None:
                credentials["effective_user"] = effective_user
            return credentials
        case "custom":
            _reject_inapplicable(
                auth,
                token=token,
                username=username,
                password=password,
                effective_user=effective_user,
            )
            auth_type = require_value(
                auth_type,
                flag="--auth-type",
                label="Java auth handler class (e.g. com.example.MyHandler)",
                no_input=no_input,
            )
            auth_token = require_value(
                auth_token,
                flag="--auth-token",
                label="Auth token (or a ref like ${env:MY_TOKEN})",
                no_input=no_input,
                hide=True,
            )
            _hint_literal_secret(auth_token, "--auth-token")
            return {
                "type": "custom",
                "auth_type": auth_type,
                "auth_token": auth_token,
            }
        case _ as unexpected:
            assert_never(unexpected)


async def _add_session_entity(
    ctx: click.Context,
    name: str,
    *,
    host: str | None = None,
    port: int | None = None,
    language: str | None = None,
    auth: str | None = None,
    token: str | None = None,
    username: str | None = None,
    password: str | None = None,
    effective_user: str | None = None,
    auth_type: str | None = None,
    auth_token: str | None = None,
) -> dict[str, Any]:
    """Create one community session file and return its summary payload.

    Args:
        ctx (click.Context): The active command context.
        name (str): Session name (filename stem).
        host (str | None): Deephaven server hostname, or ``None`` to
            prompt/default.
        port (int | None): Deephaven server port, or ``None`` to
            prompt/default.
        language (str | None): Worker scripting language, or ``None``
            to omit.
        auth (str | None): One of :data:`SessionAuthType`, or ``None`` to
            prompt.
        token (str | None): Pre-shared key for ``auth="psk"``.
        username (str | None): Username for ``auth="password"``.
        password (str | None): Password for ``auth="password"``.
        effective_user (str | None): Optional operate-as identity for
            ``auth="password"``.
        auth_type (str | None): Java auth-handler class for
            ``auth="custom"``.
        auth_token (str | None): Opaque token for ``auth="custom"``.

    Returns:
        dict[str, Any]: ``{"name", "path", "file"}``, the same shape
            emitted by 'config session add'.

    Raises:
        CliError: With ``already_exists``, ``config_path_invalid``,
            ``missing_required_option``, ``option_not_applicable``, or
            ``config_invalid`` — see 'config session add'.
    """
    spec = _authoring_spec(ctx)
    store = _store_from_spec(spec)
    target = _resolve_entity(FieldPath(("community", "sessions", name)))
    path = _require_absent(store, target, kind="session")

    # Interactive flow: connection first (host, port), then auth.
    host = prompt_optional(
        host, label="Host", no_input=spec.no_input, default="localhost"
    )
    port = prompt_optional_int(
        port, label="Port", no_input=spec.no_input, default=10000
    )
    auth_value = require_choice(
        auth,
        flag="--auth",
        label="Authentication type",
        no_input=spec.no_input,
        choices=_SESSION_AUTH_CHOICES,
    )

    credentials = _build_session_credentials(
        auth=auth_value,
        token=token,
        username=username,
        password=password,
        effective_user=effective_user,
        auth_type=auth_type,
        auth_token=auth_token,
        no_input=spec.no_input,
    )

    data: dict[str, Any] = {}
    if host is not None:
        data["host"] = host
    if port is not None:
        data["port"] = port
    if language is not None:
        data["programming_language"] = language
    data["auth"] = {"credentials": credentials}

    try:
        warnings = store.write(target, data)
    except ConfigurationError as exc:
        raise _map_config_error(exc) from exc
    _warn_template_resolution(warnings)
    _warn_restart_hint()
    return {
        "name": name,
        "path": str(target.logical_path),
        "file": str(path),
    }


@config_session.command(
    "add",
    help_spec=HelpSpec(
        summary="Declare a new community session file.",
        description=(
            "Creates community/sessions/<NAME>.json describing how to "
            "connect to an existing Deephaven Community server. Refuses to "
            "overwrite an existing session (remove it first). The file is "
            "schema-validated before an atomic write, so an invalid "
            "combination never lands on disk. On a terminal, missing "
            "required values are prompted for (stderr); otherwise they "
            "fail with missing_required_option naming the flag. Secret "
            "flags accept templating refs ('${env:VAR}', '${file:/path}') "
            "verbatim — recommended over literals."
        ),
        arguments=(
            HelpEntry(
                "NAME",
                "Session name; becomes the filename stem and the last "
                "segment of the logical path community.sessions.<NAME>. "
                "Letters, digits, '_' and '-' only (no dots), starting "
                "with a letter or digit.",
            ),
        ),
        output=_OUTPUT_SESSION_ADD,
        examples=(
            "$ dhcli config session add local_dev --auth anonymous",
            "$ dhcli config session add prod --host dh.example.com --port 10000 "
            "--auth psk --token '${env:DH_PROD_PSK}'",
            "$ dhcli -o json config session add ci --auth anonymous | jq -r .file",
        ),
        see_also=(
            "dhcli config session list",
            "dhcli config session remove NAME",
            "dhcli config set",
            "dhcli session list",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.ALREADY_EXISTS,
            ErrorCode.CONFIG_PATH_INVALID,
            ErrorCode.MISSING_REQUIRED_OPTION,
            ErrorCode.OPTION_NOT_APPLICABLE,
            ErrorCode.CONFIG_INVALID,
            ErrorCode.CONFIG_LOCKED,
        ),
    ),
    needs_runtime=False,
)
@click.argument("name")
@click.option(
    "--host",
    default=None,
    help=(
        "Deephaven server hostname. Omitted: the client default "
        "(typically localhost)."
    ),
)
@click.option(
    "--port",
    type=int,
    default=None,
    help="Deephaven server port. Omitted: the client default (typically 10000).",
)
@click.option(
    "--language",
    type=click.Choice(("Python", "Groovy")),
    default=None,
    help="Worker scripting language. Omitted: the client default.",
)
@click.option(
    "--auth",
    type=click.Choice(_SESSION_AUTH_CHOICES),
    default=None,
    help=(
        "Authentication type. Determines which credential flags apply: "
        "anonymous (none), psk (--token), password (--username, "
        "--password, optional --effective-user), custom (--auth-type, "
        "--auth-token). Prompted for on a terminal when omitted."
    ),
)
@click.option(
    "--token",
    default=None,
    help=(
        "Pre-shared key for --auth psk. Accepts a literal or a templating "
        "ref like '${env:MY_PSK}' (stored verbatim, resolved at server "
        "load time)."
    ),
)
@click.option(
    "--username",
    default=None,
    help="Authenticating username for --auth password.",
)
@click.option(
    "--password",
    default=None,
    help=(
        "Password for --auth password. Accepts a literal or a templating "
        "ref like '${env:MY_PASSWORD}'."
    ),
)
@click.option(
    "--effective-user",
    default=None,
    help=(
        "Optional operate-as identity for --auth password. Omitted: the "
        "authenticated user is also the effective user."
    ),
)
@click.option(
    "--auth-type",
    default=None,
    help=(
        "Fully-qualified Java auth-handler class name for --auth custom "
        "(e.g. com.example.MyHandler)."
    ),
)
@click.option(
    "--auth-token",
    default=None,
    help=(
        "Opaque token for --auth custom, in whatever format the custom "
        "handler expects. Accepts a literal or a templating ref."
    ),
)
@click.pass_context
@run_async
async def config_session_add(
    ctx: click.Context,
    name: str,
    host: str | None,
    port: int | None,
    language: str | None,
    auth: str | None,
    token: str | None,
    username: str | None,
    password: str | None,
    effective_user: str | None,
    auth_type: str | None,
    auth_token: str | None,
) -> None:
    """Declare a new community session file."""
    ctx.with_resource(_config_write_lock(ctx))
    payload = await _add_session_entity(
        ctx,
        name,
        host=host,
        port=port,
        language=language,
        auth=auth,
        token=token,
        username=username,
        password=password,
        effective_user=effective_user,
        auth_type=auth_type,
        auth_token=auth_token,
    )
    emit_payload(ctx, payload)


# ---------------------------------------------------------------------------
# remove
# ---------------------------------------------------------------------------


@config_session.command(
    "remove",
    help_spec=HelpSpec(
        summary="Delete a declared community session file.",
        description=(
            "Deletes community/sessions/<NAME>.json. Asks for confirmation "
            "on a terminal; otherwise requires --yes. Does not touch any "
            "running session — only the declaration file."
        ),
        arguments=(
            HelpEntry(
                "NAME",
                "Session name to remove. Run 'dhcli config session list' "
                "to see the declared names.",
            ),
        ),
        output=_OUTPUT_SESSION_REMOVE,
        examples=(
            "$ dhcli config session remove local_dev",
            "$ dhcli config session remove local_dev --yes",
        ),
        see_also=(
            "dhcli config session add NAME",
            "dhcli config session list",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.NOT_FOUND,
            ErrorCode.CONFIG_PATH_INVALID,
            ErrorCode.MISSING_REQUIRED_OPTION,
            ErrorCode.OPERATION_CANCELED,
            ErrorCode.CONFIG_INVALID,
            ErrorCode.CONFIG_LOCKED,
        ),
    ),
    needs_runtime=False,
)
@click.argument("name")
@click.option(
    "--yes",
    is_flag=True,
    default=False,
    help="Skip the confirmation prompt (required when stdin is not a TTY).",
)
@click.pass_context
@run_async
async def config_session_remove(ctx: click.Context, name: str, yes: bool) -> None:
    """Delete a declared community session file."""
    ctx.with_resource(_config_write_lock(ctx))
    spec = _authoring_spec(ctx)
    store = _store_from_spec(spec)
    target = _resolve_entity(FieldPath(("community", "sessions", name)))
    path = _require_present(store, target, kind="session")
    require_confirmation(
        f"Delete session '{name}' ({path})?", yes=yes, no_input=spec.no_input
    )
    try:
        store.delete(target)
    except ConfigurationError as exc:
        raise _map_config_error(exc) from exc
    _warn_restart_hint()
    emit_payload(ctx, {"name": name, "file": str(path)})


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@config_session.command(
    "list",
    help_spec=HelpSpec(
        summary="List the declared community session files.",
        description=(
            "Lists the session declaration files under community/sessions/ "
            "with per-file validity. This is the *declared* configuration; "
            "for the live sessions the daemon can see (including "
            "dynamically created ones), use 'dhcli session list'."
        ),
        output=_OUTPUT_SESSION_LIST,
        examples=(
            "$ dhcli config session list",
            "$ dhcli -o json config session list | jq -r '.sessions[].name'",
        ),
        see_also=(
            "dhcli config session add NAME",
            "dhcli session list",
            "dhcli config files",
        ),
        exit_codes=(ExitCode.SUCCESS,),
    ),
    needs_runtime=False,
)
@click.pass_context
@run_async
async def config_session_list(ctx: click.Context) -> None:
    """List the declared community session files."""
    spec = _authoring_spec(ctx)
    store = _store_from_spec(spec)
    entries = _entity_status_entries(store, FieldPath(("community", "sessions")))
    emit_payload(ctx, {"sessions": entries})


# ---------------------------------------------------------------------------
# system sub-group: declare, remove, and list enterprise system files
# ---------------------------------------------------------------------------


SystemAuthType = Literal["password", "private_key"]
"""Enterprise-system auth types, matching what CorePlusSessionFactory accepts."""

_SYSTEM_AUTH_CHOICES: tuple[SystemAuthType, ...] = get_args(SystemAuthType)
"""Runtime tuple derived from :data:`SystemAuthType` for click.Choice and prompts."""


@click.group("system", cls=HelpfulGroup)
def config_system() -> None:
    """Declare, remove, and list enterprise system files.

    Each declared system is one file at
    enterprise/systems/<name>.json describing how to connect to one
    Deephaven Enterprise (Core+) deployment. These verbs edit those
    files; they do not talk to any server. For the configured systems
    the daemon can see, use 'dhcli system list' instead. Values may be
    templating refs like '${env:VAR}' or '${file:/path}', stored
    verbatim and resolved when the server loads the file.
    """


_OUTPUT_SYSTEM_ADD = OutputSpec(
    "object",
    (
        OutputField("name", "string", "The system name (filename stem)."),
        OutputField("path", "string", "Logical path (enterprise.systems.<name>)."),
        OutputField("file", "string", "Absolute path of the created file."),
    ),
)
_OUTPUT_SYSTEM_REMOVE = OutputSpec(
    "object",
    (
        OutputField("name", "string", "The removed system name."),
        OutputField("file", "string", "Absolute path of the deleted file."),
    ),
)
_OUTPUT_SYSTEM_LIST = OutputSpec(
    "object",
    (
        OutputField(
            "systems",
            "array",
            "One entry per declared system file: 'name', 'file', 'valid' "
            "(boolean), 'error' (first validation error; only when invalid), "
            "'warnings' (template refs unresolvable in this shell; only "
            "when present).",
        ),
    ),
)


# ---------------------------------------------------------------------------
# add
# ---------------------------------------------------------------------------


def _build_system_credentials(
    *,
    auth: SystemAuthType,
    username: str | None,
    password: str | None,
    effective_user: str | None,
    key: str | None,
    no_input: bool,
) -> dict[str, Any]:
    """Assemble the wire-format credentials dict for the chosen auth type.

    Rejects flags that do not apply to ``auth``, prompts for missing
    required values on a TTY, and emits literal-secret hints.

    Args:
        auth (SystemAuthType): ``password`` or ``private_key``.
        username (str | None): ``--username`` (password only).
        password (str | None): ``--password`` (password only).
        effective_user (str | None): ``--effective-user`` (password only).
        key (str | None): ``--key`` (private_key only).
        no_input (bool): The root ``--no-input`` flag.

    Returns:
        dict[str, Any]: The ``auth.credentials`` block contents.

    Raises:
        CliError: On an inapplicable flag pairing or a missing value
            that cannot be prompted for.
    """
    match auth:
        case "password":
            _reject_inapplicable(auth, key=key)
            username = require_value(
                username, flag="--username", label="Username", no_input=no_input
            )
            password = require_value(
                password,
                flag="--password",
                label="Password (or a ref like ${env:MY_PASSWORD})",
                no_input=no_input,
                hide=True,
            )
            _hint_literal_secret(password, "--password")
            credentials: dict[str, Any] = {
                "type": "password",
                "username": username,
                "password": password,
            }
            if effective_user is not None:
                credentials["effective_user"] = effective_user
            return credentials
        case "private_key":
            _reject_inapplicable(
                auth,
                username=username,
                password=password,
                effective_user=effective_user,
            )
            key = require_value(
                key,
                flag="--key",
                label="Private key (use a ref like ${file:/path/to/key.pem})",
                no_input=no_input,
                hide=True,
            )
            _hint_literal_secret(key, "--key")
            return {"type": "private_key", "key_text": key}
        case _ as unexpected:
            assert_never(unexpected)


async def _add_system_entity(
    ctx: click.Context,
    name: str,
    *,
    url: str | None = None,
    auth: str | None = None,
    username: str | None = None,
    password: str | None = None,
    effective_user: str | None = None,
    key: str | None = None,
    max_sessions: int | None = None,
    heap_gb: float | None = None,
) -> dict[str, Any]:
    """Create one enterprise system file and return its summary payload.

    Args:
        ctx (click.Context): The active command context.
        name (str): System name (filename stem).
        url (str | None): connection.json URL, or ``None`` to prompt.
        auth (str | None): One of :data:`SystemAuthType`, or ``None`` to
            prompt.
        username (str | None): Username for ``auth="password"``.
        password (str | None): Password for ``auth="password"``.
        effective_user (str | None): Optional operate-as identity for
            ``auth="password"``.
        key (str | None): Private-key PEM material for
            ``auth="private_key"``.
        max_sessions (int | None): Optional concurrent-session cap.
        heap_gb (float | None): Optional default JVM heap size in GB.

    Returns:
        dict[str, Any]: ``{"name", "path", "file"}``, the same shape
            emitted by 'config system add'.

    Raises:
        CliError: With ``already_exists``, ``config_path_invalid``,
            ``missing_required_option``, ``option_not_applicable``, or
            ``config_invalid`` — see 'config system add'.
    """
    spec = _authoring_spec(ctx)
    store = _store_from_spec(spec)
    target = _resolve_entity(FieldPath(("enterprise", "systems", name)))
    path = _require_absent(store, target, kind="system")

    url = require_value(
        url,
        flag="--url",
        label="connection.json URL (https://host/iris/connection.json)",
        no_input=spec.no_input,
    )
    auth_value = require_choice(
        auth,
        flag="--auth",
        label="Authentication type",
        no_input=spec.no_input,
        choices=_SYSTEM_AUTH_CHOICES,
    )
    credentials = _build_system_credentials(
        auth=auth_value,
        username=username,
        password=password,
        effective_user=effective_user,
        key=key,
        no_input=spec.no_input,
    )

    data: dict[str, Any] = {
        "connection_json_url": url,
        "auth": {"credentials": credentials},
    }
    if max_sessions is not None or heap_gb is not None:
        session_creation: dict[str, Any] = {}
        if max_sessions is not None:
            session_creation["max_concurrent_sessions"] = max_sessions
        if heap_gb is not None:
            session_creation["defaults"] = {"heap_size_gb": heap_gb}
        data["session_creation"] = session_creation

    try:
        warnings = store.write(target, data)
    except ConfigurationError as exc:
        raise _map_config_error(exc) from exc
    _warn_template_resolution(warnings)
    _warn_restart_hint()
    return {
        "name": name,
        "path": str(target.logical_path),
        "file": str(path),
    }


@config_system.command(
    "add",
    help_spec=HelpSpec(
        summary="Declare a new enterprise system file.",
        description=(
            "Creates enterprise/systems/<NAME>.json describing how to "
            "connect to one Deephaven Enterprise (Core+) deployment. "
            "Refuses to overwrite an existing system (remove it first). "
            "The file is schema-validated before an atomic write, so an "
            "invalid combination never lands on disk. On a terminal, "
            "missing required values are prompted for (stderr); otherwise "
            "they fail with missing_required_option naming the flag. "
            "Secret flags accept templating refs ('${env:VAR}', "
            "'${file:/path}') verbatim — for --key a '${file:...}' ref is "
            "the practical form (PEM text is multi-line)."
        ),
        arguments=(
            HelpEntry(
                "NAME",
                "System name; becomes the filename stem and the last "
                "segment of the logical path enterprise.systems.<NAME>. "
                "Letters, digits, '_' and '-' only (no dots), starting "
                "with a letter or digit. 'community' is reserved.",
            ),
        ),
        output=_OUTPUT_SYSTEM_ADD,
        examples=(
            "$ dhcli config system add prod "
            "--url https://dhe.example.com/iris/connection.json "
            "--auth password --username alice --password '${env:DH_PROD_PASSWORD}'",
            "$ dhcli config system add staging --url https://stg/iris/connection.json "
            "--auth private_key --key '${file:/etc/deephaven/staging-key.pem}'",
        ),
        see_also=(
            "dhcli config system list",
            "dhcli config system remove NAME",
            "dhcli config set",
            "dhcli system list",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.ALREADY_EXISTS,
            ErrorCode.CONFIG_PATH_INVALID,
            ErrorCode.MISSING_REQUIRED_OPTION,
            ErrorCode.OPTION_NOT_APPLICABLE,
            ErrorCode.CONFIG_INVALID,
            ErrorCode.CONFIG_LOCKED,
        ),
    ),
    needs_runtime=False,
)
@click.argument("name")
@click.option(
    "--url",
    default=None,
    help=(
        "URL of the deployment's connection.json document, e.g. "
        "https://dhe.example.com/iris/connection.json. Prompted for on a "
        "terminal when omitted."
    ),
)
@click.option(
    "--auth",
    type=click.Choice(_SYSTEM_AUTH_CHOICES),
    default=None,
    help=(
        "Authentication type. Determines which credential flags apply: "
        "password (--username, --password, optional --effective-user) or "
        "private_key (--key). Prompted for on a terminal when omitted."
    ),
)
@click.option(
    "--username",
    default=None,
    help="Authenticating username for --auth password.",
)
@click.option(
    "--password",
    default=None,
    help=(
        "Password for --auth password. Accepts a literal or a templating "
        "ref like '${env:MY_PASSWORD}' (stored verbatim, resolved at "
        "server load time)."
    ),
)
@click.option(
    "--effective-user",
    default=None,
    help=(
        "Optional operate-as identity for --auth password. Omitted: the "
        "authenticated user is also the effective user."
    ),
)
@click.option(
    "--key",
    default=None,
    help=(
        "Private-key PEM material for --auth private_key. PEM text is "
        "multi-line, so a templating ref is the practical form: "
        "'${file:/path/to/key.pem}'."
    ),
)
@click.option(
    "--max-sessions",
    type=int,
    default=None,
    help=(
        "Cap on concurrent sessions MCP may create on this system "
        "(session_creation.max_concurrent_sessions). Omitted: no "
        "session_creation block is written unless --heap-gb is given."
    ),
)
@click.option(
    "--heap-gb",
    type=float,
    default=None,
    help=(
        "Default JVM heap in GB for workers MCP creates on this system "
        "(session_creation.defaults.heap_size_gb). Omitted: no "
        "session_creation block is written unless --max-sessions is given."
    ),
)
@click.pass_context
@run_async
async def config_system_add(
    ctx: click.Context,
    name: str,
    url: str | None,
    auth: str | None,
    username: str | None,
    password: str | None,
    effective_user: str | None,
    key: str | None,
    max_sessions: int | None,
    heap_gb: float | None,
) -> None:
    """Declare a new enterprise system file."""
    ctx.with_resource(_config_write_lock(ctx))
    payload = await _add_system_entity(
        ctx,
        name,
        url=url,
        auth=auth,
        username=username,
        password=password,
        effective_user=effective_user,
        key=key,
        max_sessions=max_sessions,
        heap_gb=heap_gb,
    )
    emit_payload(ctx, payload)


# ---------------------------------------------------------------------------
# remove
# ---------------------------------------------------------------------------


@config_system.command(
    "remove",
    help_spec=HelpSpec(
        summary="Delete a declared enterprise system file.",
        description=(
            "Deletes enterprise/systems/<NAME>.json. Asks for confirmation "
            "on a terminal; otherwise requires --yes. Does not touch the "
            "deployment itself — only the declaration file."
        ),
        arguments=(
            HelpEntry(
                "NAME",
                "System name to remove. Run 'dhcli config system list' to "
                "see the declared names.",
            ),
        ),
        output=_OUTPUT_SYSTEM_REMOVE,
        examples=(
            "$ dhcli config system remove staging",
            "$ dhcli config system remove staging --yes",
        ),
        see_also=(
            "dhcli config system add NAME",
            "dhcli config system list",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.NOT_FOUND,
            ErrorCode.CONFIG_PATH_INVALID,
            ErrorCode.MISSING_REQUIRED_OPTION,
            ErrorCode.OPERATION_CANCELED,
            ErrorCode.CONFIG_INVALID,
            ErrorCode.CONFIG_LOCKED,
        ),
    ),
    needs_runtime=False,
)
@click.argument("name")
@click.option(
    "--yes",
    is_flag=True,
    default=False,
    help="Skip the confirmation prompt (required when stdin is not a TTY).",
)
@click.pass_context
@run_async
async def config_system_remove(ctx: click.Context, name: str, yes: bool) -> None:
    """Delete a declared enterprise system file."""
    ctx.with_resource(_config_write_lock(ctx))
    spec = _authoring_spec(ctx)
    store = _store_from_spec(spec)
    target = _resolve_entity(FieldPath(("enterprise", "systems", name)))
    path = _require_present(store, target, kind="system")
    require_confirmation(
        f"Delete system '{name}' ({path})?", yes=yes, no_input=spec.no_input
    )
    try:
        store.delete(target)
    except ConfigurationError as exc:
        raise _map_config_error(exc) from exc
    _warn_restart_hint()
    emit_payload(ctx, {"name": name, "file": str(path)})


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@config_system.command(
    "list",
    help_spec=HelpSpec(
        summary="List the declared enterprise system files.",
        description=(
            "Lists the system declaration files under enterprise/systems/ "
            "with per-file validity. This is the *declared* configuration; "
            "for the systems the daemon can see (including the community "
            "umbrella), use 'dhcli system list'."
        ),
        output=_OUTPUT_SYSTEM_LIST,
        examples=(
            "$ dhcli config system list",
            "$ dhcli -o json config system list | jq -r '.systems[].name'",
        ),
        see_also=(
            "dhcli config system add NAME",
            "dhcli system list",
            "dhcli config files",
        ),
        exit_codes=(ExitCode.SUCCESS,),
    ),
    needs_runtime=False,
)
@click.pass_context
@run_async
async def config_system_list(ctx: click.Context) -> None:
    """List the declared enterprise system files."""
    spec = _authoring_spec(ctx)
    store = _store_from_spec(spec)
    entries = _entity_status_entries(store, FieldPath(("enterprise", "systems")))
    emit_payload(ctx, {"systems": entries})


# ---------------------------------------------------------------------------
# registration
# ---------------------------------------------------------------------------

# ``show`` and ``validate`` register themselves via ``@config.command``;
# the standalone verbs and the two sub-groups are registered here.
config.add_command(config_files)
config.add_command(config_init)
config.add_command(config_edit)
config.add_command(config_session)
config.add_command(config_system)
config.add_command(config_get)
config.add_command(config_set)
config.add_command(config_unset)
config.add_command(config_keys)
