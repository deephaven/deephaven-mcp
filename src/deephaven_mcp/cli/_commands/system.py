"""``dh-mcp system`` noun group: inspect the configured Deephaven systems.

Verbs: ``list``, ``status``.
"""

from __future__ import annotations

__all__ = ["system"]

import click

from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._commands._wrapping import (
    call_and_echo,
    call_and_echo_field,
    wrapper_error_codes,
)
from deephaven_mcp.cli._errors import ExitCode
from deephaven_mcp.cli._help import (
    HelpfulGroup,
    OutputField,
    OutputSpec,
    build_help,
)
from deephaven_mcp.cli._runtime import Runtime


@click.group(cls=HelpfulGroup)
def system() -> None:
    """Inspect the Deephaven systems the daemon is configured to serve.

    A 'system' is the source dimension of every fully qualified session
    id ('type:system:name'): the single Community umbrella (named
    'community') plus every configured Enterprise (Core+) system. These
    commands connect to the daemon (auto-starting it unless
    --no-auto-start is set) and speak MCP: 'list' enumerates the
    configured systems; 'status' reports Enterprise system health.
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
    output_spec=_OUTPUT_LIST,
    wraps_tool="list_systems",
    help=build_help(
        summary="List the Deephaven systems the daemon is configured to serve.",
        description=(
            "Enumerates every configured system: the single Community umbrella "
            "(named 'community') and each Enterprise (Core+) system. Use the "
            "returned names with 'session create --system NAME' and as the "
            "'system' component of a fully qualified session id."
        ),
        output=_OUTPUT_LIST,
        examples=(
            "$ dh-mcp system list",
            "$ dh-mcp -o json system list | jq '.[].name'",
        ),
        see_also=("dh-mcp system status", "dh-mcp session list"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=wrapper_error_codes(tool_error=False),
    ),
)
@click.pass_obj
@run_async
async def system_list(runtime: Runtime) -> None:
    """List the configured Deephaven systems."""
    await call_and_echo_field(
        runtime,
        "list_systems",
        retry_command="dh-mcp system list",
        arguments={},
        field="systems",
        default=[],
    )


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

_OUTPUT_STATUS = OutputSpec(
    "object",
    (
        OutputField(
            "systems",
            "array",
            "Per-system status: name, liveness_status, is_alive, config.",
        ),
        OutputField(
            "partial_result",
            "object",
            "Present only when the report is incomplete (discovery in progress or "
            "a system failed): phase, detail, and optional per-system errors.",
        ),
    ),
    note="Enterprise (Core+) only; community systems are not reported.",
)


@system.command(
    "status",
    output_spec=_OUTPUT_STATUS,
    wraps_tool="enterprise_systems_status",
    help=build_help(
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
            "$ dh-mcp system status",
            "$ dh-mcp system status --system prod --connect",
            "$ dh-mcp -o json system status | jq '.systems[].liveness_status'",
        ),
        see_also=("dh-mcp system list",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=wrapper_error_codes(),
    ),
)
@click.option(
    "--system",
    "system",
    default=None,
    help=(
        "Enterprise system name to report on; see 'dh-mcp system list'. "
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
    await call_and_echo(
        runtime,
        "enterprise_systems_status",
        retry_command="dh-mcp system status",
        arguments=arguments,
    )
