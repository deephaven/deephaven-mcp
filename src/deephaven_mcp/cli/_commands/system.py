"""``dhcli system`` noun group: inspect the configured Deephaven systems.

Verbs: ``list``, ``status``, ``url``, ``open``.
"""

from __future__ import annotations

__all__ = ["system"]

from urllib.parse import urlsplit, urlunsplit

import click

from deephaven_mcp._taxonomy import SystemType
from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._browser import launch_browser
from deephaven_mcp.cli._command import HelpfulGroup
from deephaven_mcp.cli._commands._wrapping import (
    call_and_echo_field,
    wrapper_error_codes,
)
from deephaven_mcp.cli._context import (
    CONTEXT_HINT,
    ContextKey,
    require_context_value,
)
from deephaven_mcp.cli._echo import echo_payload
from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode
from deephaven_mcp.cli._help import (
    HelpEntry,
    HelpSpec,
    OutputField,
    OutputSpec,
)
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.sessions import EnterpriseSystemConfig

_WEB_CONSOLE_PATH = "/iriside"
"""Path of the Deephaven Enterprise web console under a system's origin."""


@click.group(cls=HelpfulGroup)
def system() -> None:
    """Inspect the Deephaven systems the daemon is configured to serve.

    A 'system' is the source dimension of every fully qualified session
    id ('type:system:name'): the single Community umbrella (named
    'community') plus every configured Enterprise (Core+) system.
    'list' and 'status' speak MCP to the daemon (auto-starting it
    unless --no-auto-start is set): 'list' enumerates the configured
    systems; 'status' reports Enterprise system health. 'url' and
    'open' are computed locally from configuration and never contact
    the daemon: they surface an Enterprise system's web console.
    """


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

_OUTPUT_LIST = OutputSpec(
    "list",
    (
        OutputField("name", "string", "System name ('community' or a system id)."),
        OutputField("type", "string", "'community' or 'enterprise'."),
    ),
    note="Array of configured systems.",
)


@system.command(
    "list",
    wraps_tool="list_systems",
    help_spec=HelpSpec(
        summary="List the Deephaven systems the daemon is configured to serve.",
        description=(
            "Enumerates every configured system: the single Community umbrella "
            "(named 'community') and each Enterprise (Core+) system. Use the "
            "returned names with 'session create --system NAME' and as the "
            "'system' component of a fully qualified session id."
        ),
        output=_OUTPUT_LIST,
        examples=(
            "$ dhcli system list",
            "$ dhcli -o json system list | jq '.[].name'",
        ),
        see_also=("dhcli system status", "dhcli session list"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=wrapper_error_codes(tool_error=False, no_systems=False),
    ),
)
@click.pass_obj
@run_async
async def system_list(runtime: Runtime) -> None:
    """List the configured Deephaven systems."""
    await call_and_echo_field(
        runtime,
        "list_systems",
        retry_command="dhcli system list",
        arguments={},
        field="systems",
        default=[],
        empty_on_no_systems=True,
    )


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

_OUTPUT_STATUS = OutputSpec(
    "list",
    (
        OutputField("name", "string", "System name."),
        OutputField(
            "type",
            "string",
            "Always 'enterprise'; parallels 'system list' so the two outputs "
            "can be joined on (name, type).",
        ),
        OutputField(
            "liveness_status",
            "string",
            "ResourceLivenessStatus: 'ONLINE', 'OFFLINE', 'UNAUTHORIZED', "
            "'MISCONFIGURED', or 'UNKNOWN'.",
        ),
        OutputField("is_alive", "boolean", "True when the system is responsive."),
        OutputField(
            "liveness_detail",
            "string",
            "Optional short reason for the status. When --connect probed the "
            "system, this is the probe's own message; otherwise, when "
            "discovery recorded an error, it is a kubectl-style exception-type "
            "code (e.g. 'DeephavenConnectionError').",
        ),
    ),
    note=(
        "Array of per-system status records (Enterprise/Core+ only; community "
        "systems are not reported). Health only — use 'dhcli config show' for "
        "configuration. When discovery is still running or has failed, a "
        "phase-summary warning (with per-system details when available) is written to stderr."
    ),
)


@system.command(
    "status",
    wraps_tool="enterprise_systems_status",
    help_spec=HelpSpec(
        summary="Report Enterprise (Core+) system health.",
        description=(
            "Reports liveness for configured Enterprise systems. This is "
            "Enterprise-only: Community runs locally and has no remote health to "
            "report (an all-community deployment returns an empty list). Pass "
            "--system to scope to one system, and --connect to actively verify "
            "connectivity rather than reading cached status."
        ),
        output=_OUTPUT_STATUS,
        examples=(
            "$ dhcli system status",
            "$ dhcli system status --system prod --connect",
            "$ dhcli -o json system status | jq '.[].liveness_status'",
        ),
        see_also=("dhcli system list",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.option(
    "--system",
    "system",
    default=None,
    help=(
        "Enterprise system name to report on; see 'dhcli system list'. "
        "Omit to report all configured systems."
    ),
)
@click.option(
    "--connect",
    "attempt_to_connect",
    is_flag=True,
    default=False,
    help="Actively connect to verify status instead of reading cached state.",
)
@click.pass_obj
@run_async
async def system_status(
    runtime: Runtime, system: str | None, attempt_to_connect: bool
) -> None:
    """Report Enterprise system health."""
    arguments: dict[str, object] = {}
    if system is not None:
        arguments["system"] = system
    if attempt_to_connect:
        arguments["attempt_to_connect"] = True
    await call_and_echo_field(
        runtime,
        "enterprise_systems_status",
        retry_command="dhcli system status",
        arguments=arguments,
        field="systems",
        default=[],
        # Each row carries its per-system reason via `liveness_detail`, so a
        # COMPLETED partial_result is suppressed (the "had connection issues"
        # banner would only restate the table). LOADING/FAILED phases still
        # warn with the full `errors` map, since the row shows only the short
        # reason and the full message would otherwise be unreachable.
        reasons_in_rows=True,
    )


# ---------------------------------------------------------------------------
# url / open helpers
# ---------------------------------------------------------------------------


def _enterprise_system(runtime: Runtime, name: str) -> EnterpriseSystemConfig:
    """Look up a configured Enterprise system by name.

    Args:
        runtime (Runtime): The active CLI runtime.
        name (str): The system name to resolve.

    Returns:
        EnterpriseSystemConfig: The matching system declaration.

    Raises:
        CliError: When no Enterprise system named ``name`` is configured
            (exit 2, ``system_not_found``). The Community umbrella
            ('community') is reported with a pointer to ``session url``.
    """
    system_config = runtime.config.enterprise_systems.get(name)
    if system_config is not None:
        return system_config
    if name == SystemType.COMMUNITY.value:
        raise CliError(
            "'community' is the local Community umbrella, not an Enterprise "
            "system, and has no web console. Use 'dhcli session url ID' for a "
            "Community session.",
            code=ErrorCode.SYSTEM_NOT_FOUND,
        )
    raise CliError(
        f"No Enterprise system named '{name}' is configured. "
        f"Run 'dhcli system list' to see the configured systems.",
        code=ErrorCode.SYSTEM_NOT_FOUND,
    )


def _web_console_url(connection_json_url: str) -> str:
    """Derive an Enterprise web console URL from a ``connection.json`` URL.

    Args:
        connection_json_url (str): The system's ``connection_json_url``
            (e.g. ``https://dhe.example.com:8123/iris/connection.json``).

    Returns:
        str: The web console URL — the origin of ``connection_json_url``
            with the ``/iriside`` path (e.g.
            ``https://dhe.example.com:8123/iriside``).

    Raises:
        CliError: When ``connection_json_url`` has no scheme or host (exit
            2, ``config_invalid``).
    """
    parts = urlsplit(connection_json_url)
    if not parts.scheme or not parts.netloc:
        raise CliError(
            f"The system's connection_json_url is not an absolute URL: "
            f"{connection_json_url!r}.",
            code=ErrorCode.CONFIG_INVALID,
        )
    return urlunsplit((parts.scheme, parts.netloc, _WEB_CONSOLE_PATH, "", ""))


# ---------------------------------------------------------------------------
# url
# ---------------------------------------------------------------------------

_OUTPUT_URL = OutputSpec("text", note="The Enterprise web console URL, one line.")


@system.command(
    "url",
    help_spec=HelpSpec(
        summary="Print an Enterprise system's web console URL.",
        description=(
            "Prints the Deephaven Enterprise (Core+) web console URL for a "
            "configured system, derived from its connection_json_url (the "
            "origin plus the /iriside path). Always prints the bare URL "
            "regardless of -o (so piping works); -o only affects the "
            "structured error on failure. The URL is UNAUTHENTICATED: you log "
            "in interactively in the browser (unlike 'session url', which "
            "embeds a Community auth token). Computed from configuration only "
            "— this does not contact the daemon."
        ),
        arguments=(
            HelpEntry(
                "NAME",
                "Enterprise system name. Run 'dhcli system list'. Defaults "
                f"to the sticky context system if omitted. {CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_URL,
        examples=(
            "$ dhcli system url prod",
            '$ open "$(dhcli system url prod)"',
        ),
        see_also=(
            "dhcli system open NAME",
            "dhcli system list",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.CONTEXT_NOT_SET,
            ErrorCode.SYSTEM_NOT_FOUND,
            ErrorCode.CONFIG_INVALID,
        ),
    ),
)
@click.argument("name", required=False)
@click.pass_obj
@run_async
async def system_url(runtime: Runtime, name: str | None) -> None:
    """Print an Enterprise system's web console URL."""
    name = require_context_value(runtime, ContextKey.SYSTEM, name)
    system_config = _enterprise_system(runtime, name)
    url = _web_console_url(system_config.connection_json_url)
    click.echo(url)


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


@system.command(
    "open",
    help_spec=HelpSpec(
        summary="Open an Enterprise system's web console in the browser.",
        description=(
            "Derives the Deephaven Enterprise (Core+) web console URL for a "
            "configured system (its connection_json_url origin plus /iriside) "
            "and launches your default browser. Pass --print to print the URL "
            "instead of launching (use this in headless / CI environments). "
            "The web console is UNAUTHENTICATED: you log in interactively in "
            "the browser. Computed from configuration only — this does not "
            "contact the daemon. If the browser cannot be launched, exits 2 "
            "(browser_launch_failed) with the URL in the message so you can "
            "open it manually."
        ),
        arguments=(
            HelpEntry(
                "NAME",
                "Enterprise system name. Run 'dhcli system list'. Defaults "
                f"to the sticky context system if omitted. {CONTEXT_HINT}",
            ),
        ),
        output=_OUTPUT_OPEN,
        examples=(
            "$ dhcli system open prod",
            "$ dhcli system open prod --print",
            "$ dhcli system open prod --print | jq -r .opened",
        ),
        see_also=(
            "dhcli system url NAME",
            "dhcli system list",
            "dhcli context show",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.CONTEXT_NOT_SET,
            ErrorCode.SYSTEM_NOT_FOUND,
            ErrorCode.CONFIG_INVALID,
            ErrorCode.BROWSER_LAUNCH_FAILED,
        ),
    ),
)
@click.argument("name", required=False)
@click.option(
    "--print",
    "print_only",
    is_flag=True,
    default=False,
    help="Print the URL instead of launching a browser (headless-safe).",
)
@click.pass_obj
@run_async
async def system_open(runtime: Runtime, name: str | None, print_only: bool) -> None:
    """Open an Enterprise system's web console in the default web browser."""
    name = require_context_value(runtime, ContextKey.SYSTEM, name)
    system_config = _enterprise_system(runtime, name)
    url = _web_console_url(system_config.connection_json_url)
    launched = False if print_only else launch_browser(url)
    echo_payload(runtime, {"opened": url, "launched": launched})
