"""``dhcli session`` noun group: manage, inspect, and operate Deephaven sessions.

Verbs: ``list``, ``show``, ``create``, ``delete``, ``exec``, ``pip-list``,
``credentials``, ``url``, ``open``.

Every session is addressed by a fully qualified id
``type:system:name`` (``type`` is ``community`` or ``enterprise``).
Verbs that take an existing id route to the right backend tool by the
id's prefix; ``create`` chooses the backend from ``--system``. Type is
never a command subgroup — see the ``ref-cli-tool-wrapping`` skill.
"""

from __future__ import annotations

__all__ = ["session"]

from collections.abc import Iterable
from pathlib import Path
from typing import Any, assert_never, cast

import click

from deephaven_mcp._taxonomy import SystemType
from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._browser import launch_browser
from deephaven_mcp.cli._command import HelpfulGroup
from deephaven_mcp.cli._commands._wrapping import (
    call_and_echo,
    call_and_echo_field,
    call_for_payload,
    parse_key_value,
    read_local_script,
    reveal_secrets_option,
    wrapper_error_codes,
    yes_option,
)
from deephaven_mcp.cli._context import (
    CONTEXT_HINT,
    CONTEXT_RISK_DESTRUCTIVE,
    CONTEXT_RISK_STATEFUL,
    TARGET_SELECTION_GUIDANCE,
    TARGET_SELECTION_HINT,
    ContextKey,
    ContextProvenance,
    clear_matching,
    require_context_target,
    require_context_value,
    resolve_for_runtime,
)
from deephaven_mcp.cli._echo import echo_payload
from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode
from deephaven_mcp.cli._help import (
    HelpEntry,
    HelpSpec,
    OutputField,
    OutputSpec,
)
from deephaven_mcp.cli._params import param_label
from deephaven_mcp.cli._runtime import Runtime

_COMMUNITY_SYSTEM = SystemType.COMMUNITY.value
"""The single fixed system name whose sessions are Community sessions."""

_CREDENTIALS_TOOL = "session_community_credentials"
"""MCP tool backing the credentials / url / open verbs (Community-only)."""

_CREDENTIAL_FIELDS = (
    "id",
    "auth_type",
    "auth_token",
    "connection_url",
    "connection_url_with_auth",
)
"""Payload keys rendered by ``credentials``, single-sourced from the tool."""

# Community-create options invalid for an Enterprise system, and vice versa.
# Names match the underlying tool parameters so the drift test joins them.
_COMMUNITY_ONLY_CREATE = (
    "launch_method",
    "auth_token",
    "docker_image",
    "docker_memory_limit_gb",
    "docker_cpu_limit",
    "docker_volumes",
    "python_venv_path",
)
_ENTERPRISE_ONLY_CREATE = (
    "server",
    "engine",
    "auto_delete_timeout",
    "admin_groups",
    "viewer_groups",
    "session_arguments",
)


def _create_flags(names: Iterable[str]) -> str:
    """Render ``session create`` parameter names as the flags a user types.

    The spelling comes from the command's own option declarations, so a
    repeatable option is named singular where its parameter is plural
    (``--admin-group`` for ``admin_groups``).

    Args:
        names (Iterable[str]): Parameter names of ``session create``.

    Returns:
        str: The flags, comma-joined in the order given.
    """
    labels = {param.name: param_label(param) for param in session_create.params}
    return ", ".join(labels[name] for name in names)


def _system_origin(system: str, provenance: ContextProvenance) -> str:
    """Describe which system was selected and where the choice came from.

    An "option does not apply to a *branch* session" message is only
    actionable if the user can see why that branch was chosen — and with
    the sticky context they may never have typed ``--system`` at all, so
    naming the flag alone would describe an argument they did not pass.

    Args:
        system (str): The resolved system name.
        provenance (ContextProvenance): Where the value came from.

    Returns:
        str: A parenthetical-ready phrase naming the system and, when it
            was not typed, the source that supplied it.
    """
    match provenance:
        case ContextProvenance.ARGUMENT:
            return f"--system {system!r}"
        case ContextProvenance.FILE:
            return (
                f"system {system!r}, from the sticky context; see "
                "'dhcli context show'"
            )
        case ContextProvenance.DISABLED | ContextProvenance.UNSET:
            return f"system {system!r}, the default"
        case _ as unexpected:
            assert_never(unexpected)


@click.group(cls=HelpfulGroup)
def session() -> None:
    """Manage, inspect, and operate Deephaven sessions hosted by the daemon.

    Sessions are addressed by a fully qualified id 'type:system:name'.
    'list' and 'show' inspect; 'create' provisions one (the backend is
    chosen by --system); 'delete' removes one; 'exec' runs a script in
    one; 'pip-list' reports its installed pip packages; 'credentials',
    'url', and 'open' surface a Community session's browser login. These
    commands auto-start the daemon unless --no-auto-start is set.

    Which session to work in: the one you were told to use, or one you
    make with 'session create' — 'session list' enumerates every user's
    sessions, production included, so it is a discovery tool rather than
    a menu to pick from. 'session create' records the new id as the
    sticky default, so the verbs that follow can omit it; confirm with
    'context show' before anything consequential.
    """


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _pairs(tokens: tuple[str, ...], *, decode_json: bool) -> dict[str, Any] | None:
    """Parse repeated ``KEY=VALUE`` option tokens into a dict.

    Args:
        tokens (tuple[str, ...]): The repeated option values, each
            ``"KEY=VALUE"``. An empty tuple means the option was omitted.
        decode_json (bool): Whether each value is JSON-decoded by
            :func:`parse_key_value`.

    Returns:
        dict[str, Any] | None: The parsed mapping, or ``None`` when
            ``tokens`` is empty.
    """
    if not tokens:
        return None
    return dict(parse_key_value(t, decode_json=decode_json) for t in tokens)


def _expand_local_path(path: str | None) -> str | None:
    """Expand a leading ``~`` in a local-machine path option.

    The community worker is launched by the local daemon, so these paths
    are on this machine; expanding client-side matches the
    ``--config-dir`` / ``--runtime-dir`` convention. A path without a
    leading ``~`` is returned verbatim (no normalization).

    Args:
        path (str | None): The option value, or ``None`` when omitted.

    Returns:
        str | None: The expanded path, or the input unchanged.
    """
    if path is not None and path.startswith("~"):
        return str(Path(path).expanduser())
    return path


def _expand_volume_host(spec: str) -> str:
    """Expand a leading ``~`` in the host half of a Docker volume spec.

    A spec is ``host:container[:mode]``; only the host path is on this
    machine, so only it is expanded. The container path names a location
    inside the container and is always forwarded verbatim.

    Args:
        spec (str): The ``--docker-volume`` option value.

    Returns:
        str: The spec with the host half ``~``-expanded.
    """
    host, sep, rest = spec.partition(":")
    return f"{_expand_local_path(host)}{sep}{rest}"


def _provided(value: Any) -> bool:
    """Return whether an option was supplied.

    Args:
        value (Any): A normalized option value from the ``create`` option
            map: a scalar (``None`` when omitted) or a list/dict (already
            ``None`` when empty).

    Returns:
        bool: ``True`` when ``value`` is non-``None`` and not an empty tuple.
    """
    return value is not None and value != ()


async def _fetch_credentials(
    runtime: Runtime, id: str, *, retry_command: str
) -> dict[str, Any]:
    """Fetch one Community session's credential payload.

    Args:
        runtime (Runtime): The active CLI runtime.
        id (str): The fully qualified session id.
        retry_command (str): Command rendered into the corrupt-registry hint.

    Returns:
        dict[str, Any]: The credential payload (``id``, ``auth_type``,
            ``auth_token``, ``connection_url``, ``connection_url_with_auth``).

    Raises:
        CliError: On daemon/transport failure, or exit 3 when retrieval is
            disabled or the session is missing / not a Community session.
    """
    return await call_for_payload(
        runtime,
        _CREDENTIALS_TOOL,
        retry_command=retry_command,
        arguments={"id": id},
    )


def _authenticated_url(payload: dict[str, Any]) -> str:
    """Extract the browser-ready URL from a credential payload.

    Args:
        payload (dict[str, Any]): A ``session_community_credentials`` payload.

    Returns:
        str: ``connection_url_with_auth`` when present, else ``connection_url``.

    Raises:
        CliError: When the payload carries neither URL field (exit 2,
            ``mcp_request_failed``).
    """
    url = payload.get("connection_url_with_auth") or payload.get("connection_url")
    if not url:
        raise CliError(
            "The session reported no connection URL.",
            code=ErrorCode.MCP_REQUEST_FAILED,
        )
    return cast(str, url)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

_OUTPUT_LIST = OutputSpec(
    "list",
    (
        OutputField(
            "id",
            "string",
            "Fully qualified id 'type:system:name' — pass this verbatim to "
            "the other verbs.",
        ),
        OutputField("type", "string", "'community' or 'enterprise'."),
        OutputField("system", "string", "Owning system name."),
        OutputField("session_name", "string", "Session name within the system."),
        OutputField(
            "origin",
            "string",
            "How the session became known: 'static' (declared in config), "
            "'dynamic' (created by an MCP tool, so possibly by you), or "
            "'discovered' (already existed on an Enterprise system).",
        ),
    ),
    note=(
        "Array of sessions. The list can be incomplete — Enterprise "
        "discovery may still be running or a system may be unreachable — in "
        "which case a warning naming the phase and the failing systems is "
        "written to stderr while stdout stays parseable. An empty array is "
        "not proof that no session exists."
    ),
)


@session.command(
    "list",
    wraps_tool="sessions_list",
    help_spec=HelpSpec(
        summary="List sessions with basic metadata.",
        description=(
            "Lightweight discovery across Community and Enterprise sessions; "
            "does not connect. Filter with --type, --system, and --origin. "
            "The list spans every user's sessions, production included. "
            + TARGET_SELECTION_GUIDANCE
            + " Filtering with --origin dynamic narrows it to "
            "tool-created sessions, which is the closest thing to 'sessions "
            "like the ones I create'."
        ),
        output=_OUTPUT_LIST,
        examples=(
            "$ dhcli session list",
            "$ dhcli session list --type community",
            "$ dhcli -o json session list | jq '.[].id'",
            "$ dhcli session list --origin dynamic | jq -r '.[].id'",
        ),
        see_also=(
            "dhcli session show ID",
            "dhcli session create SESSION_NAME",
            "dhcli system list",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=wrapper_error_codes(tool_error=False, no_systems=False),
    ),
)
@click.option(
    "--type",
    "type",
    type=click.Choice(["community", "enterprise"]),
    default=None,
    help="Filter by session type.",
)
@click.option(
    "--system",
    "system",
    default=None,
    help=(
        "Filter by system name: 'community' or a configured Enterprise system "
        "name (see 'dhcli system list')."
    ),
)
@click.option(
    "--origin",
    "origin",
    type=click.Choice(["static", "dynamic", "discovered"]),
    default=None,
    help=(
        "Filter sessions by how they came to be known to MCP: 'static' "
        "(declared in config), 'dynamic' (created by an MCP tool), or "
        "'discovered' (pre-existing on an enterprise system)."
    ),
)
@click.pass_obj
@run_async
async def session_list(
    runtime: Runtime, type: str | None, system: str | None, origin: str | None
) -> None:
    """List sessions with basic metadata."""
    arguments = {
        k: v
        for k, v in (("type", type), ("system", system), ("origin", origin))
        if v is not None
    }
    await call_and_echo_field(
        runtime,
        "sessions_list",
        retry_command="dhcli session list",
        arguments=arguments,
        field="sessions",
        default=[],
        empty_on_no_systems=True,
    )


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------

_OUTPUT_SHOW = OutputSpec(
    "object",
    (
        OutputField("id", "string", "Fully qualified session id."),
        OutputField("type", "string", "'community' or 'enterprise'."),
        OutputField("programming_language", "string", "Worker language, if known."),
        OutputField("liveness_status", "string", "ONLINE / OFFLINE / etc."),
    ),
    note=(
        "The session detail object (additional fields may be present). An "
        "enterprise session's id works verbatim with the 'pq' verbs. Without "
        "--connect, liveness_status reflects cached state and may be stale."
    ),
)


@session.command(
    "show",
    wraps_tool="session_details",
    help_spec=HelpSpec(
        summary="Show detailed information about one session.",
        description=(
            "Reports status and configuration for a session. By default this is "
            "a quick read of cached state; pass --connect to actively connect "
            "and verify liveness (slower, more accurate). An unknown or missing "
            "session exits 3."
        ),
        arguments=(
            HelpEntry(
                "ID",
                "Fully qualified id. Run 'session list'. Defaults to the "
                f"sticky context session if omitted. {CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_SHOW,
        examples=(
            "$ dhcli session show community:community:my-session",
            "$ dhcli session show community:community:my-session --connect",
            "$ dhcli session show community:community:my-session "
            "| jq -r .liveness_status",
        ),
        see_also=("dhcli session list", "dhcli context show"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.CONTEXT_NOT_SET, *wrapper_error_codes()),
    ),
)
@click.argument("id", required=False)
@click.option(
    "--connect",
    "attempt_to_connect",
    is_flag=True,
    default=False,
    help="Actively connect to verify liveness instead of reading cached state.",
)
@click.pass_obj
@run_async
async def session_show(
    runtime: Runtime, id: str | None, attempt_to_connect: bool
) -> None:
    """Show detailed information about one session."""
    id = require_context_value(runtime, ContextKey.SESSION, id)
    arguments: dict[str, Any] = {"id": id}
    if attempt_to_connect:
        arguments["attempt_to_connect"] = True
    await call_and_echo_field(
        runtime,
        "session_details",
        retry_command="dhcli session show",
        arguments=arguments,
        field="session",
        default={},
    )


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------

_OUTPUT_CREATE = OutputSpec(
    "object",
    (
        OutputField("id", "string", "Fully qualified id of the new session."),
        OutputField("session_name", "string", "The simple name."),
    ),
    note="Additional backend-specific fields may be present.",
)


@session.command(
    "create",
    wraps_tools=("session_community_create", "session_enterprise_create"),
    router_params=frozenset({"system"}),
    client_only_params=frozenset({"no_set_context"}),
    help_spec=HelpSpec(
        summary="Create a session on a system (Community or Enterprise).",
        description=(
            "The --system value selects the backend and its type: 'community' "
            "(the default) creates a local Community worker; any other name "
            "creates a worker on that Enterprise system. For an Enterprise "
            "system this is create-and-connect: it provisions a Persistent "
            "Query and connects immediately; use 'pq create' instead to "
            "define a durable PQ (scheduled, RunAndDone, or disabled) "
            "without connecting. SESSION_NAME is "
            "required for Community and optional (auto-generated) for "
            "Enterprise. Community options (--launch-method, --docker-*, "
            "--python-venv-path, --auth-token) and Enterprise options "
            "(--server, --engine, --auto-delete-timeout, --admin-group, "
            "--viewer-group, --session-arg) are mutually exclusive; supplying "
            "one for the wrong --system exits 2 with option_not_applicable. A "
            "backend that rejects the request exits 3. Prefer this over "
            "reusing a session from 'session list': a session you created is "
            "yours to run scripts in and to delete. " + CONTEXT_RISK_STATEFUL
        ),
        arguments=(
            HelpEntry("SESSION_NAME", "Session name (required for Community)."),
        ),
        output=OutputSpec(
            _OUTPUT_CREATE.mode,
            (
                *_OUTPUT_CREATE.fields,
                OutputField(
                    "context",
                    "object",
                    "Present when the sticky context was updated: the keys set "
                    "and their new values.",
                ),
            ),
            note=_OUTPUT_CREATE.note,
        ),
        examples=(
            "$ dhcli session create dev --launch-method python",
            "$ dhcli session create rpt --system prod --engine DeephavenEnterprise",
            "$ dhcli session create dev --env LOG_LEVEL=DEBUG --jvm-arg -Xmx2g",
            "$ dhcli session create dev --launch-method python | jq -r .id",
        ),
        see_also=(
            "dhcli session list",
            "dhcli session exec ID",
            "dhcli session delete ID",
            "dhcli system list",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(
            ErrorCode.OPTION_NOT_APPLICABLE,
            ErrorCode.ARG_PARSE_ERROR,
            *wrapper_error_codes(),
        ),
    ),
)
@click.argument("session_name", required=False)
@click.option(
    "--system",
    "system",
    default=None,
    show_default=False,
    help=(
        "Target system: 'community' for a local Community worker, or a configured "
        "Enterprise system name (run 'dhcli system list'). Defaults to the "
        f"sticky context system if set, else 'community'. {CONTEXT_HINT} "
        "The system's type "
        "selects which options apply."
    ),
)
@click.option(
    "--language",
    "programming_language",
    type=click.Choice(["Python", "Groovy"], case_sensitive=False),
    default=None,
    help=(
        "Language scripts in this worker are written in. Omitted: the "
        "backend's default."
    ),
)
@click.option(
    "--heap-size-gb",
    "heap_size_gb",
    type=float,
    default=None,
    help="JVM heap size in GB, e.g. 4 or 8.5. Omitted: the backend's default.",
)
@click.option(
    "--jvm-arg",
    "extra_jvm_args",
    multiple=True,
    help="One extra JVM argument, e.g. -Xmx2g (repeatable).",
)
@click.option(
    "--env",
    "environment_vars",
    multiple=True,
    metavar="KEY=VALUE",
    help=(
        "Worker environment variable as KEY=VALUE (repeatable). The value is "
        "sent verbatim, never JSON-decoded."
    ),
)
# Community-only
@click.option(
    "--launch-method",
    "launch_method",
    type=click.Choice(["docker", "python"], case_sensitive=False),
    default=None,
    help=(
        "[Community] How the local daemon launches the worker: 'docker' runs "
        "a container, 'python' runs an in-process server from a virtualenv."
    ),
)
@click.option(
    "--auth-token",
    "auth_token",
    default=None,
    help=(
        "[Community] Auth token to configure on the new worker. Omitted: the "
        "backend's default."
    ),
)
@click.option(
    "--docker-image",
    "docker_image",
    default=None,
    help=(
        "[Community] Image reference the container runs, e.g. "
        "ghcr.io/deephaven/server:latest (docker launch method). Omitted: the "
        "configured default."
    ),
)
@click.option(
    "--docker-memory-limit-gb",
    "docker_memory_limit_gb",
    type=float,
    default=None,
    help="[Community] Container memory limit in GB, e.g. 4.",
)
@click.option(
    "--docker-cpu-limit",
    "docker_cpu_limit",
    type=float,
    default=None,
    help="[Community] Container CPU limit in cores, e.g. 2 or 1.5.",
)
@click.option(
    "--docker-volume",
    "docker_volumes",
    multiple=True,
    metavar="HOST:CONTAINER[:MODE]",
    help=(
        "[Community] Bind mount as host:container[:mode], e.g. "
        "./data:/data:ro (repeatable). The host half is a path on this "
        "machine and '~' is expanded; the container half is forwarded "
        "verbatim."
    ),
)
@click.option(
    "--python-venv-path",
    "python_venv_path",
    default=None,
    help=(
        "[Community] Path to a virtualenv on this machine for the python "
        "launch method ('~' is expanded)."
    ),
)
# Enterprise-only
@click.option(
    "--server",
    "server",
    default=None,
    help=(
        "[Enterprise] Name of the server pool to run the worker on "
        "(deployment-specific). Omitted: the controller chooses."
    ),
)
@click.option(
    "--engine",
    "engine",
    default=None,
    help=(
        "[Enterprise] Engine name (deployment-specific; e.g. DeephavenCommunity, "
        "DeephavenEnterprise). Omitted: the controller's default."
    ),
)
@click.option(
    "--auto-delete-timeout",
    "auto_delete_timeout",
    type=int,
    default=None,
    help=(
        "[Enterprise] Seconds of idleness after which the session is deleted "
        "automatically. Omitted: the backend's default."
    ),
)
@click.option(
    "--admin-group",
    "admin_groups",
    multiple=True,
    help="[Enterprise] One group granted admin access to the session (repeatable).",
)
@click.option(
    "--viewer-group",
    "viewer_groups",
    multiple=True,
    help="[Enterprise] One group granted viewer access to the session (repeatable).",
)
@click.option(
    "--session-arg",
    "session_arguments",
    multiple=True,
    metavar="KEY=VALUE",
    help=(
        "[Enterprise] Extra controller session argument as KEY=VALUE "
        "(repeatable). Each value is JSON-decoded when possible, so n=42 "
        "sends the integer 42 and s=hi sends the string hi."
    ),
)
@click.option(
    "--no-set-context",
    "no_set_context",
    is_flag=True,
    default=False,
    help=(
        "Do not update the sticky context on success. Without this flag a "
        "successful create sets the 'session' key, and for an Enterprise "
        "session the 'system' and 'pq' keys too (an Enterprise session id "
        "is its PQ id). This governs only the write; --no-context governs "
        "whether an omitted id reads from the sticky context."
    ),
)
@click.pass_obj
@run_async
async def session_create(  # noqa: PLR0913 — a wrapper mirrors its tool's full surface
    runtime: Runtime,
    session_name: str | None,
    system: str | None,
    programming_language: str | None,
    heap_size_gb: float | None,
    extra_jvm_args: tuple[str, ...],
    environment_vars: tuple[str, ...],
    launch_method: str | None,
    auth_token: str | None,
    docker_image: str | None,
    docker_memory_limit_gb: float | None,
    docker_cpu_limit: float | None,
    docker_volumes: tuple[str, ...],
    python_venv_path: str | None,
    server: str | None,
    engine: str | None,
    auto_delete_timeout: int | None,
    admin_groups: tuple[str, ...],
    viewer_groups: tuple[str, ...],
    session_arguments: tuple[str, ...],
    no_set_context: bool,
) -> None:
    """Create a session on a Community or Enterprise system."""
    system_provenance = ContextProvenance.ARGUMENT
    if system is None:
        resolved = resolve_for_runtime(runtime, ContextKey.SYSTEM, None)
        system = resolved.value or _COMMUNITY_SYSTEM
        system_provenance = resolved.provenance
    community = system == _COMMUNITY_SYSTEM
    opts: dict[str, Any] = {
        "session_name": session_name,
        "programming_language": programming_language,
        "heap_size_gb": heap_size_gb,
        "extra_jvm_args": list(extra_jvm_args) or None,
        "environment_vars": _pairs(environment_vars, decode_json=False),
        "launch_method": launch_method,
        "auth_token": auth_token,
        "docker_image": docker_image,
        "docker_memory_limit_gb": docker_memory_limit_gb,
        "docker_cpu_limit": docker_cpu_limit,
        "docker_volumes": [_expand_volume_host(v) for v in docker_volumes] or None,
        "python_venv_path": _expand_local_path(python_venv_path),
        "server": server,
        "engine": engine,
        "auto_delete_timeout": auto_delete_timeout,
        "admin_groups": list(admin_groups) or None,
        "viewer_groups": list(viewer_groups) or None,
        "session_arguments": _pairs(session_arguments, decode_json=True),
    }

    wrong = _ENTERPRISE_ONLY_CREATE if community else _COMMUNITY_ONLY_CREATE
    misused = sorted(name for name in wrong if _provided(opts[name]))
    if misused:
        branch = "Community" if community else "Enterprise"
        flags = _create_flags(misused)
        raise CliError(
            f"{flags} do not apply to a {branch} session "
            f"({_system_origin(system, system_provenance)}).",
            code=ErrorCode.OPTION_NOT_APPLICABLE,
        )
    if community and not session_name:
        raise CliError(
            "A SESSION_NAME is required when creating a Community session.",
            code=ErrorCode.OPTION_NOT_APPLICABLE,
        )

    if community:
        tool = "session_community_create"
        relevant = {*_COMMUNITY_ONLY_CREATE, "session_name"}
    else:
        tool = "session_enterprise_create"
        relevant = {*_ENTERPRISE_ONLY_CREATE, "session_name"}
        opts["system"] = system
    shared = {
        "programming_language",
        "heap_size_gb",
        "extra_jvm_args",
        "environment_vars",
    }
    keep = relevant | shared | ({"system"} if not community else set())
    arguments = {k: v for k, v in opts.items() if k in keep and v is not None}

    payload = await call_for_payload(
        runtime, tool, retry_command="dhcli session create", arguments=arguments
    )
    new_id = payload.get("id")
    if not no_set_context and new_id:
        updates = {ContextKey.SESSION: new_id}
        if not community:
            updates[ContextKey.SYSTEM] = system
            updates[ContextKey.PQ] = new_id
        runtime.context_store.set_many(updates)
        payload = {**payload, "context": {k.value: v for k, v in updates.items()}}
    echo_payload(runtime, payload)


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------

_OUTPUT_DELETE = OutputSpec(
    "object",
    (OutputField("id", "string", "The deleted session's id."),),
    note="Additional backend-specific fields may be present.",
)


@session.command(
    "delete",
    wraps_tools=("session_community_delete", "session_enterprise_delete"),
    client_only_params=frozenset({"yes"}),
    help_spec=HelpSpec(
        summary="Delete a session by id.",
        description=(
            "Permanently deletes the session with ID; this cannot be undone. "
            "Deleting an enterprise session also deletes the Persistent Query "
            "backing it, so only a session created by 'session create' is "
            "eligible. A session that already existed — one from static "
            "community config, or an enterprise PQ found on the controller — "
            "is refused with exit 3; use 'pq delete' to remove such a PQ "
            "deliberately. On success the sticky session and pq keys are "
            "cleared if they pointed at the deleted id. " + CONTEXT_RISK_DESTRUCTIVE
        ),
        arguments=(
            HelpEntry(
                "ID",
                "Fully qualified id. Run 'session list'. Defaults to the "
                f"sticky context session if omitted. {TARGET_SELECTION_HINT} "
                f"{CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_DELETE,
        examples=(
            "$ dhcli session delete community:community:my-session",
            "$ dhcli session delete community:community:my-session | jq -r .id",
        ),
        see_also=(
            "dhcli session create",
            "dhcli session list",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(
            ErrorCode.CONTEXT_NOT_SET,
            ErrorCode.OPERATION_CANCELED,
            *wrapper_error_codes(),
        ),
    ),
)
@click.argument("id", required=False)
@yes_option
@click.pass_obj
@run_async
async def session_delete(runtime: Runtime, id: str | None, yes: bool) -> None:
    """Delete a session by id, routing on the id's type prefix."""
    id = require_context_target(
        runtime, ContextKey.SESSION, id, action="Delete", yes=yes
    )
    community = id.startswith(f"{_COMMUNITY_SYSTEM}:")
    tool = "session_community_delete" if community else "session_enterprise_delete"
    payload = await call_for_payload(
        runtime,
        tool,
        retry_command="dhcli session delete",
        arguments={"id": id},
    )
    clear_matching(
        runtime.context_store, frozenset({id}), (ContextKey.SESSION, ContextKey.PQ)
    )
    echo_payload(runtime, payload)


# ---------------------------------------------------------------------------
# exec
# ---------------------------------------------------------------------------

_OUTPUT_EXEC = OutputSpec(
    "object",
    (OutputField("id", "string", "The session id the script ran in."),),
    note=(
        "Confirmation only: the echoed id is the whole payload on success — "
        "no stdout, return value, or variable dump comes back. Read results "
        "with 'dhcli table list' / 'dhcli table data' on tables the script "
        "created, which persist in the session. A script that raises exits 3 "
        "with the error message."
    ),
)


@session.command(
    "exec",
    wraps_tool="session_script_run",
    client_only_params=frozenset({"yes"}),
    help_spec=HelpSpec(
        summary="Run a script in a session.",
        description=(
            "Executes a script in the session's worker. Provide the code inline "
            "with --script, from a local file with --script-path, or from "
            "standard input with '--script-path -'; supply exactly one source. "
            "The file is read by the CLI itself, so a relative path resolves "
            "against your working directory; an unreadable file exits 2. "
            "Supplying no source or several exits 2; a script error exits 3. "
            "The script runs with the session's full privileges and its "
            "effects persist there, so run scripts in a session you created "
            "or were pointed at. " + CONTEXT_RISK_DESTRUCTIVE
        ),
        arguments=(
            HelpEntry(
                "ID",
                "Fully qualified id. Run 'session list'. Defaults to the "
                f"sticky context session if omitted. {TARGET_SELECTION_HINT} "
                f"{CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_EXEC,
        examples=(
            "$ dhcli session exec community:community:dev --script 'print(1+1)'",
            "$ dhcli session exec community:community:dev --script-path /tmp/job.py",
            "$ cat job.py | dhcli session exec community:community:dev --script-path -",
            "$ dhcli session exec community:community:dev --script "
            "'t = empty_table(5)' && dhcli table list community:community:dev | jq .",
        ),
        see_also=(
            "dhcli table list ID",
            "dhcli session pip-list ID",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(
            ErrorCode.CONTEXT_NOT_SET,
            ErrorCode.OPERATION_CANCELED,
            ErrorCode.MISSING_ARGUMENT,
            ErrorCode.MUTUALLY_EXCLUSIVE_OPTIONS,
            ErrorCode.FILE_READ_FAILED,
            *wrapper_error_codes(),
        ),
    ),
)
@click.argument("id", required=False)
@click.option(
    "--script",
    "script",
    default=None,
    help=(
        "Script source inline, in the session's own language (run 'dhcli "
        "session show' to see which). Mutually exclusive with --script-path."
    ),
)
@click.option(
    "--script-path",
    "script_path",
    default=None,
    help=(
        "Path to a local script file, read by the CLI (a relative path "
        "resolves against your working directory, and '~' is expanded), or "
        "'-' to read the script from stdin. Mutually exclusive with --script."
    ),
)
@yes_option
@click.pass_obj
@run_async
async def session_exec(
    runtime: Runtime,
    id: str | None,
    script: str | None,
    script_path: str | None,
    yes: bool,
) -> None:
    """Run a script in a session."""
    # Both guards run before the target is resolved: confirming a run
    # against the sticky-context session and only then rejecting the flag
    # combination wastes the user's decision. Reading the file stays
    # after the confirmation, so a declined run does not touch the disk.
    if script is not None and script_path is not None:
        raise CliError(
            "--script and --script-path cannot be combined; supply exactly one.",
            code=ErrorCode.MUTUALLY_EXCLUSIVE_OPTIONS,
        )
    if script is None and script_path is None:
        raise CliError(
            "Provide a script source: --script, --script-path, or --script-path -.",
            code=ErrorCode.MISSING_ARGUMENT,
        )
    id = require_context_target(
        runtime, ContextKey.SESSION, id, action="Run script in", yes=yes
    )
    if script_path is not None:
        script = read_local_script(script_path)
    arguments: dict[str, Any] = {"id": id, "script": script}
    await call_and_echo(
        runtime,
        "session_script_run",
        retry_command="dhcli session exec",
        arguments=arguments,
    )


# ---------------------------------------------------------------------------
# pip-list
# ---------------------------------------------------------------------------

_OUTPUT_PIP_LIST = OutputSpec(
    "list",
    (
        OutputField("package", "string", "Package name."),
        OutputField("version", "string", "Installed version."),
    ),
    note="Array of installed pip packages.",
)


@session.command(
    "pip-list",
    wraps_tool="session_pip_list",
    help_spec=HelpSpec(
        summary="List a session's installed pip packages.",
        description=(
            "Returns the Python packages available in the session's environment "
            "as a list of {package, version}. Use it to confirm a library is "
            "present before running a script that imports it."
        ),
        arguments=(
            HelpEntry(
                "ID",
                "Fully qualified id. Run 'session list'. Defaults to the "
                f"sticky context session if omitted. {CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_PIP_LIST,
        examples=(
            "$ dhcli session pip-list community:community:dev",
            "$ dhcli -o json session pip-list community:community:dev | jq '.[].package'",
        ),
        see_also=("dhcli session exec ID", "dhcli context show"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.CONTEXT_NOT_SET, *wrapper_error_codes()),
    ),
)
@click.argument("id", required=False)
@click.pass_obj
@run_async
async def session_pip_list(runtime: Runtime, id: str | None) -> None:
    """List a session's installed pip packages."""
    id = require_context_value(runtime, ContextKey.SESSION, id)
    await call_and_echo_field(
        runtime,
        "session_pip_list",
        retry_command="dhcli session pip-list",
        arguments={"id": id},
        field="packages",
        default=[],
    )


# ---------------------------------------------------------------------------
# credentials
# ---------------------------------------------------------------------------

_OUTPUT_CREDENTIALS = OutputSpec(
    "object",
    (
        OutputField("id", "string", "The session id, echoed back."),
        OutputField("auth_type", "string", "Authentication type, uppercased."),
        OutputField("auth_token", "string", "Plaintext auth token (empty if anon)."),
        OutputField("connection_url", "string", "Base server URL without auth."),
        OutputField(
            "connection_url_with_auth", "string", "Browser-ready URL with the token."
        ),
    ),
)

_CREDENTIALS_DESCRIPTION = (
    "Wraps the session_community_credentials MCP tool (Community sessions "
    "only; a clear error is returned for an Enterprise id). The output "
    "contains a PLAINTEXT auth token by design so you can open the session "
    "in a browser. Retrieval is gated by the configured "
    "community.settings.security.credential_retrieval_mode (default "
    "'dynamic_only', which permits sessions created by 'session create' but "
    "withholds statically configured credentials); when refused, or the "
    "session is missing or not a Community session, the command exits 3."
)


@session.command(
    "credentials",
    wraps_tool=_CREDENTIALS_TOOL,
    help_spec=HelpSpec(
        summary="Print a Community session's browser-login credentials.",
        description=_CREDENTIALS_DESCRIPTION,
        arguments=(
            HelpEntry(
                "ID",
                "Community session id. Run 'session list'. Defaults to the "
                f"sticky context session if omitted. {TARGET_SELECTION_HINT} "
                f"{CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_CREDENTIALS,
        examples=(
            "$ dhcli session credentials community:community:my-session",
            "$ dhcli session credentials community:community:my-session "
            "| jq -r .connection_url_with_auth",
        ),
        see_also=(
            "dhcli session url ID",
            "dhcli session open ID",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.CONTEXT_NOT_SET, *wrapper_error_codes()),
    ),
)
@click.argument("id", required=False)
@click.pass_obj
@run_async
async def session_credentials(runtime: Runtime, id: str | None) -> None:
    """Fetch one Community session's browser-login credentials."""
    id = require_context_value(runtime, ContextKey.SESSION, id)
    payload = await _fetch_credentials(
        runtime, id, retry_command="dhcli session credentials"
    )
    credentials = {field: payload.get(field) for field in _CREDENTIAL_FIELDS}
    echo_payload(runtime, credentials)


# ---------------------------------------------------------------------------
# url
# ---------------------------------------------------------------------------

_OUTPUT_URL = OutputSpec("text", note="The authenticated browser URL, one line.")


@session.command(
    "url",
    wraps_tool=_CREDENTIALS_TOOL,
    help_spec=HelpSpec(
        summary="Print a Community session's authenticated browser URL.",
        description=(
            "Prints only connection_url_with_auth — the browser-ready URL "
            "including the auth token — so it can be piped or copied. Always "
            "prints the bare URL regardless of -o (so piping works); -o only "
            "affects the structured error on failure. Same Community-only scope "
            "and security gate as 'session credentials'."
        ),
        arguments=(
            HelpEntry(
                "ID",
                "Community session id. Run 'session list'. Defaults to the "
                f"sticky context session if omitted. {TARGET_SELECTION_HINT} "
                f"{CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_URL,
        examples=(
            "$ dhcli session url community:community:my-session",
            '$ open "$(dhcli session url community:community:my-session)"',
        ),
        see_also=(
            "dhcli session credentials ID",
            "dhcli session open ID",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.CONTEXT_NOT_SET, *wrapper_error_codes()),
    ),
)
@click.argument("id", required=False)
@click.pass_obj
@run_async
async def session_url(runtime: Runtime, id: str | None) -> None:
    """Print a Community session's authenticated browser URL."""
    id = require_context_value(runtime, ContextKey.SESSION, id)
    payload = await _fetch_credentials(runtime, id, retry_command="dhcli session url")
    click.echo(_authenticated_url(payload))


# ---------------------------------------------------------------------------
# open
# ---------------------------------------------------------------------------

_OUTPUT_OPEN = OutputSpec(
    "object",
    (
        OutputField(
            "opened",
            "string",
            "The session URL, without the auth token unless "
            "--reveal-secrets is passed.",
        ),
        OutputField("launched", "boolean", "True if a browser was launched."),
    ),
)


@session.command(
    "open",
    wraps_tool=_CREDENTIALS_TOOL,
    client_only_params=frozenset({"print_only", "reveal_secrets"}),
    help_spec=HelpSpec(
        summary="Open a Community session in the default web browser.",
        description=(
            "Fetches the session's authenticated URL and hands it to your "
            "default browser, which keeps the auth token out of stdout. Same "
            "Community-only scope and security gate as 'session credentials'. "
            "If the browser cannot be launched, exits 2 "
            "(browser_launch_failed) with a URL to open manually -- the "
            "token-free one unless --reveal-secrets was passed, so a failure "
            "does not disclose the credential either."
        ),
        arguments=(
            HelpEntry(
                "ID",
                "Community session id. Run 'session list'. Defaults to the "
                f"sticky context session if omitted. {TARGET_SELECTION_HINT} "
                f"{CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_OPEN,
        examples=(
            "$ dhcli session open community:community:my-session",
            "$ dhcli session open community:community:my-session --print",
            "$ dhcli session open community:community:my-session --print "
            "--reveal-secrets | jq -r .opened",
        ),
        see_also=(
            "dhcli session url ID",
            "dhcli session credentials ID",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(
            ErrorCode.CONTEXT_NOT_SET,
            ErrorCode.BROWSER_LAUNCH_FAILED,
            *wrapper_error_codes(),
        ),
    ),
)
@click.argument("id", required=False)
@click.option(
    "--print",
    "print_only",
    is_flag=True,
    default=False,
    help=(
        "Do not launch a browser; only report the URL (headless-safe). Add "
        "--reveal-secrets to get one you can actually log in with, or use "
        "'dhcli session url'."
    ),
)
@reveal_secrets_option
@click.pass_obj
@run_async
async def session_open(
    runtime: Runtime, id: str | None, print_only: bool, reveal_secrets: bool
) -> None:
    """Open a Community session in the default web browser."""
    id = require_context_value(runtime, ContextKey.SESSION, id)
    payload = await _fetch_credentials(runtime, id, retry_command="dhcli session open")
    url = _authenticated_url(payload)
    # Disclosure is orthogonal to launching: --print says "do not open a
    # browser", not "put the token on stdout". Only --reveal-secrets does
    # that, the same flag 'config get' uses.
    opened = url if reveal_secrets else payload.get("connection_url") or url
    launched = (
        False
        if print_only
        # The browser still receives the authenticated URL -- it has to,
        # or the page cannot log in. Only the *error message* falls back
        # to the token-free URL, so a launch failure does not write the
        # credential to stderr behind the opt-in's back.
        else launch_browser(
            url,
            manual_url=opened,
            # Earn the hint: an anonymous session (and any payload with
            # no separate authenticated URL) makes `opened` identical to
            # what we opened, so there is no withheld token to explain
            # and no better URL for 'session url' to hand back.
            hint=(
                "That URL omits the auth token; run 'dhcli session url' "
                "to get one you can log in with."
                if opened != url
                else None
            ),
        )
    )
    echo_payload(runtime, {"opened": opened, "launched": launched})
