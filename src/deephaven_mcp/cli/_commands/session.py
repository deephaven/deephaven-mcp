"""``dh-mcp session`` noun group: manage and inspect Deephaven sessions.

Verbs: ``list``, ``show``, ``create``, ``delete``, ``credentials``,
``url``, ``open``.

Every session is addressed by a fully qualified id
``type:system:name`` (``type`` is ``community`` or ``enterprise``).
Verbs that take an existing id route to the right backend tool by the
id's prefix; ``create`` chooses the backend from ``--system``. Type is
never a command subgroup — see the ``_cli-tool-wrapping`` skill.
"""

from __future__ import annotations

__all__ = ["session"]

from typing import Any, cast

import click

from deephaven_mcp._taxonomy import SystemType
from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._browser import launch_browser
from deephaven_mcp.cli._commands._wrapping import (
    call_and_echo,
    call_and_echo_field,
    call_for_payload,
    echo_payload,
    parse_key_value,
    wrapper_error_codes,
)
from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode
from deephaven_mcp.cli._help import (
    HelpEntry,
    HelpfulGroup,
    OutputField,
    OutputSpec,
    build_help,
)
from deephaven_mcp.cli._runtime import Runtime

_COMMUNITY_SYSTEM = SystemType.COMMUNITY.value
"""The single fixed system name whose sessions are Community sessions."""

_CREDENTIALS_TOOL = "session_community_credentials"
"""MCP tool backing the credentials / url / open verbs (Community-only)."""

_CREDENTIAL_FIELDS = (
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


@click.group(cls=HelpfulGroup)
def session() -> None:
    """Manage and inspect Deephaven sessions hosted by the daemon.

    Sessions are addressed by a fully qualified id 'type:system:name'.
    'list' and 'show' inspect; 'create' provisions one (the backend is
    chosen by --system); 'delete' removes one; 'credentials', 'url', and
    'open' surface a Community session's browser login. These commands
    auto-start the daemon unless --no-auto-start is set.
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
        dict[str, Any]: The credential payload (``auth_type``,
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
        OutputField("id", "string", "Fully qualified id 'type:system:name'."),
        OutputField("type", "string", "'community' or 'enterprise'."),
        OutputField("system", "string", "Owning system name."),
    ),
    note="Array of sessions (extra per-session fields may be present).",
)


@session.command(
    "list",
    output_spec=_OUTPUT_LIST,
    wraps_tool="sessions_list",
    help=build_help(
        summary="List sessions with basic metadata.",
        description=(
            "Lightweight discovery across Community and Enterprise sessions; "
            "does not connect. Filter with --type, --system, and --origin. Use "
            "a returned id verbatim with the other session verbs."
        ),
        output=_OUTPUT_LIST,
        examples=(
            "$ dh-mcp session list",
            "$ dh-mcp session list --type community",
            "$ dh-mcp -o json session list | jq '.[].id'",
        ),
        see_also=("dh-mcp session show ID", "dh-mcp system list"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=wrapper_error_codes(tool_error=False),
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
        "name (see 'dh-mcp system list')."
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
        retry_command="dh-mcp session list",
        arguments=arguments,
        field="sessions",
        default=[],
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
    note="The session detail object (additional fields may be present). An "
    "enterprise session's id works verbatim with the 'pq' verbs.",
)


@session.command(
    "show",
    output_spec=_OUTPUT_SHOW,
    wraps_tool="session_details",
    help=build_help(
        summary="Show detailed information about one session.",
        description=(
            "Reports status and configuration for a session. By default this is "
            "a quick read of cached state; pass --connect to actively connect "
            "and verify liveness (slower, more accurate). An unknown or missing "
            "session exits 3."
        ),
        arguments=(HelpEntry("ID", "Fully qualified id. Run 'session list'."),),
        output=_OUTPUT_SHOW,
        examples=(
            "$ dh-mcp session show community:community:my-session",
            "$ dh-mcp session show community:community:my-session --connect",
        ),
        see_also=("dh-mcp session list",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("id")
@click.option(
    "--connect",
    "attempt_to_connect",
    is_flag=True,
    default=False,
    help="Actively connect to verify liveness instead of reading cached state.",
)
@click.pass_obj
@run_async
async def session_show(runtime: Runtime, id: str, attempt_to_connect: bool) -> None:
    """Show detailed information about one session."""
    arguments: dict[str, Any] = {"id": id}
    if attempt_to_connect:
        arguments["attempt_to_connect"] = True
    await call_and_echo_field(
        runtime,
        "session_details",
        retry_command="dh-mcp session show",
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
    output_spec=_OUTPUT_CREATE,
    wraps_tools=("session_community_create", "session_enterprise_create"),
    router_params=frozenset({"system"}),
    help=build_help(
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
            "backend that rejects the request exits 3."
        ),
        arguments=(
            HelpEntry("SESSION_NAME", "Session name (required for Community)."),
        ),
        output=_OUTPUT_CREATE,
        examples=(
            "$ dh-mcp session create dev --launch-method python",
            "$ dh-mcp session create rpt --system prod --engine DeephavenEnterprise",
            "$ dh-mcp session create dev --env LOG_LEVEL=DEBUG --jvm-arg -Xmx2g",
        ),
        see_also=("dh-mcp session delete ID", "dh-mcp system list"),
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
    default=_COMMUNITY_SYSTEM,
    show_default=True,
    help=(
        "Target system: 'community' for a local Community worker, or a configured "
        "Enterprise system name (run 'dh-mcp system list'). The system's type "
        "selects which options apply."
    ),
)
@click.option(
    "--language",
    "programming_language",
    type=click.Choice(["Python", "Groovy"], case_sensitive=False),
    default=None,
    help="Worker language.",
)
@click.option(
    "--heap-size-gb",
    "heap_size_gb",
    type=float,
    default=None,
    help="JVM heap size (GB).",
)
@click.option(
    "--jvm-arg", "extra_jvm_args", multiple=True, help="Extra JVM arg (repeatable)."
)
@click.option(
    "--env",
    "environment_vars",
    multiple=True,
    metavar="KEY=VALUE",
    help="Worker environment variable (repeatable).",
)
# Community-only
@click.option(
    "--launch-method",
    "launch_method",
    type=click.Choice(["docker", "python"]),
    default=None,
    help="[Community] How to launch the worker.",
)
@click.option(
    "--auth-token", "auth_token", default=None, help="[Community] Bearer auth token."
)
@click.option(
    "--docker-image", "docker_image", default=None, help="[Community] Docker image."
)
@click.option(
    "--docker-memory-limit-gb",
    "docker_memory_limit_gb",
    type=float,
    default=None,
    help="[Community] Docker memory limit (GB).",
)
@click.option(
    "--docker-cpu-limit",
    "docker_cpu_limit",
    type=float,
    default=None,
    help="[Community] Docker CPU limit (cores).",
)
@click.option(
    "--docker-volume",
    "docker_volumes",
    multiple=True,
    help="[Community] Docker bind mount (repeatable).",
)
@click.option(
    "--python-venv-path",
    "python_venv_path",
    default=None,
    help="[Community] Host virtualenv path for the python launch method.",
)
# Enterprise-only
@click.option("--server", "server", default=None, help="[Enterprise] Server pool name.")
@click.option(
    "--engine",
    "engine",
    default=None,
    help=(
        "[Enterprise] Engine name (deployment-specific; e.g. DeephavenCommunity, "
        "DeephavenEnterprise)."
    ),
)
@click.option(
    "--auto-delete-timeout",
    "auto_delete_timeout",
    type=int,
    default=None,
    help="[Enterprise] Idle seconds before auto-delete.",
)
@click.option(
    "--admin-group",
    "admin_groups",
    multiple=True,
    help="[Enterprise] Admin group (repeatable).",
)
@click.option(
    "--viewer-group",
    "viewer_groups",
    multiple=True,
    help="[Enterprise] Viewer group (repeatable).",
)
@click.option(
    "--session-arg",
    "session_arguments",
    multiple=True,
    metavar="KEY=VALUE",
    help="[Enterprise] Controller session argument (repeatable; JSON values).",
)
@click.pass_obj
@run_async
async def session_create(  # noqa: PLR0913 — a wrapper mirrors its tool's full surface
    runtime: Runtime,
    session_name: str | None,
    system: str,
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
) -> None:
    """Create a session on a Community or Enterprise system."""
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
        "docker_volumes": list(docker_volumes) or None,
        "python_venv_path": python_venv_path,
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
        raise CliError(
            f"Options {misused} do not apply to a {branch} session "
            f"(--system {system!r}).",
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

    await call_and_echo(
        runtime, tool, retry_command="dh-mcp session create", arguments=arguments
    )


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
    output_spec=_OUTPUT_DELETE,
    wraps_tools=("session_community_delete", "session_enterprise_delete"),
    help=build_help(
        summary="Delete a session by id.",
        description=(
            "Permanently deletes the session with ID; this cannot be "
            "undone. For an enterprise session, this also deletes its "
            "underlying Persistent Query from the system (equivalent to "
            "'pq delete' with the same id). A community session "
            "can be deleted only if it was dynamically created; those defined "
            "in static config cannot."
        ),
        arguments=(HelpEntry("ID", "Fully qualified id. Run 'session list'."),),
        output=_OUTPUT_DELETE,
        examples=("$ dh-mcp session delete community:community:my-session",),
        see_also=("dh-mcp session create", "dh-mcp session list"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("id")
@click.pass_obj
@run_async
async def session_delete(runtime: Runtime, id: str) -> None:
    """Delete a session by id, routing on the id's type prefix."""
    community = id.startswith(f"{_COMMUNITY_SYSTEM}:")
    tool = "session_community_delete" if community else "session_enterprise_delete"
    await call_and_echo(
        runtime,
        tool,
        retry_command="dh-mcp session delete",
        arguments={"id": id},
    )


# ---------------------------------------------------------------------------
# credentials
# ---------------------------------------------------------------------------

_OUTPUT_CREDENTIALS = OutputSpec(
    "object",
    (
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
    "in a browser. Retrieval is gated by security.credential_retrieval_mode "
    "in community/settings.json (default 'none'); when disabled, or the "
    "session is missing or not a Community session, the command exits 3."
)


@session.command(
    "credentials",
    output_spec=_OUTPUT_CREDENTIALS,
    wraps_tool=_CREDENTIALS_TOOL,
    help=build_help(
        summary="Print a Community session's browser-login credentials.",
        description=_CREDENTIALS_DESCRIPTION,
        arguments=(HelpEntry("ID", "Community session id. Run 'session list'."),),
        output=_OUTPUT_CREDENTIALS,
        examples=("$ dh-mcp session credentials community:community:my-session",),
        see_also=("dh-mcp session url ID", "dh-mcp session open ID"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("id")
@click.pass_obj
@run_async
async def session_credentials(runtime: Runtime, id: str) -> None:
    """Fetch one Community session's browser-login credentials."""
    payload = await _fetch_credentials(
        runtime, id, retry_command="dh-mcp session credentials"
    )
    credentials = {field: payload.get(field) for field in _CREDENTIAL_FIELDS}
    echo_payload(runtime, credentials)


# ---------------------------------------------------------------------------
# url
# ---------------------------------------------------------------------------

_OUTPUT_URL = OutputSpec("text", note="The authenticated browser URL, one line.")


@session.command(
    "url",
    output_spec=_OUTPUT_URL,
    wraps_tool=_CREDENTIALS_TOOL,
    help=build_help(
        summary="Print a Community session's authenticated browser URL.",
        description=(
            "Prints only connection_url_with_auth — the browser-ready URL "
            "including the auth token — so it can be piped or copied. Always "
            "prints the bare URL regardless of -o (so piping works); -o only "
            "affects the structured error on failure. Same Community-only scope "
            "and security gate as 'session credentials'."
        ),
        arguments=(HelpEntry("ID", "Community session id. Run 'session list'."),),
        output=_OUTPUT_URL,
        examples=(
            "$ dh-mcp session url community:community:my-session",
            '$ open "$(dh-mcp session url community:community:my-session)"',
        ),
        see_also=("dh-mcp session credentials ID", "dh-mcp session open ID"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.argument("id")
@click.pass_obj
@run_async
async def session_url(runtime: Runtime, id: str) -> None:
    """Print a Community session's authenticated browser URL."""
    payload = await _fetch_credentials(runtime, id, retry_command="dh-mcp session url")
    click.echo(_authenticated_url(payload))


# ---------------------------------------------------------------------------
# open
# ---------------------------------------------------------------------------

_OUTPUT_OPEN = OutputSpec(
    "object",
    (
        OutputField("opened", "string", "The URL that was launched (or would be)."),
        OutputField("launched", "boolean", "True if a browser was launched."),
    ),
)


@session.command(
    "open",
    output_spec=_OUTPUT_OPEN,
    wraps_tool=_CREDENTIALS_TOOL,
    client_only_params=frozenset({"print_only"}),
    help=build_help(
        summary="Open a Community session in the default web browser.",
        description=(
            "Fetches the authenticated URL and launches your default browser. "
            "Pass --print to print the URL instead of launching (use this in "
            "headless / CI environments). Same Community-only scope and "
            "security gate as 'session credentials'. If the browser cannot be "
            "launched, exits 2 (browser_launch_failed) with the URL in the "
            "message so you can open it manually."
        ),
        arguments=(HelpEntry("ID", "Community session id. Run 'session list'."),),
        output=_OUTPUT_OPEN,
        examples=(
            "$ dh-mcp session open community:community:my-session",
            "$ dh-mcp session open community:community:my-session --print",
        ),
        see_also=("dh-mcp session url ID", "dh-mcp session credentials ID"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.BROWSER_LAUNCH_FAILED, *wrapper_error_codes()),
    ),
)
@click.argument("id")
@click.option(
    "--print",
    "print_only",
    is_flag=True,
    default=False,
    help="Print the URL instead of launching a browser (headless-safe).",
)
@click.pass_obj
@run_async
async def session_open(runtime: Runtime, id: str, print_only: bool) -> None:
    """Open a Community session in the default web browser."""
    payload = await _fetch_credentials(runtime, id, retry_command="dh-mcp session open")
    url = _authenticated_url(payload)
    launched = False if print_only else launch_browser(url)
    echo_payload(runtime, {"opened": url, "launched": launched})
