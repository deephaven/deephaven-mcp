"""``dh-mcp session`` noun group: inspect running Deephaven sessions.

Verbs: ``credentials``.
"""

from __future__ import annotations

__all__ = ["session"]

import json
from typing import Any

import click
from mcp.types import CallToolResult, TextContent

from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._commands.shared import acquire_daemon, registry_corrupt_message
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._format import format_output
from deephaven_mcp.cli._help import (
    HelpfulGroup,
    OutputField,
    OutputSpec,
    build_help,
)
from deephaven_mcp.cli._mcp_client import McpClient, McpClientError
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.daemon_registry import DaemonRegistryEntry

_CREDENTIALS_TOOL = "session_community_credentials"
"""Name of the MCP tool the ``credentials`` verb wraps."""

_CREDENTIAL_FIELDS = (
    "auth_type",
    "auth_token",
    "connection_url",
    "connection_url_with_auth",
)
"""Payload keys rendered on success, single-sourced from the wrapped tool."""


@click.group(cls=HelpfulGroup)
def session() -> None:
    """Inspect Deephaven sessions hosted by the daemon.

    These commands connect to the daemon (auto-starting it unless
    --no-auto-start is set) and speak MCP: 'credentials' fetches the
    browser-login credentials for one Community session.
    """


_EXIT_CODES = (
    (0, "success"),
    (2, "user-facing failure (config, daemon, or MCP request)"),
)

# credentials exits 3 when the wrapped tool reports failure (retrieval
# disabled by configuration, session not found, or wrong session type).
_CREDENTIALS_EXIT_CODES = (
    *_EXIT_CODES,
    (3, "the credentials tool reported an error"),
)

# Error codes the shared _acquire path can raise (daemon lifecycle +
# MCP transport), single-sourced so the verb cannot drift from what
# _acquire actually raises.
_ACQUIRE_ERROR_CODES = (
    ("daemon_not_running", "No daemon and --no-auto-start was set."),
    ("daemon_startup_timeout", "Auto-started daemon did not register in time."),
    ("daemon_registry_corrupt", "daemon.json exists but cannot be parsed."),
    ("mcp_request_failed", "The MCP transport reported an error."),
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


async def _acquire(runtime: Runtime, *, retry_command: str) -> DaemonRegistryEntry:
    """Acquire a live daemon for a ``dh-mcp session`` subcommand.

    Args:
        runtime (Runtime): The active CLI runtime.
        retry_command (str): The command rendered into the
            corrupt-registry recovery hint for the operator to re-run.

    Returns:
        DaemonRegistryEntry: The validated registry entry for the
            running daemon.

    Raises:
        CliError: When the daemon cannot be acquired (startup timeout,
            client error, or corrupt registry).
    """
    return await acquire_daemon(
        runtime,
        auto_start=runtime.config.cli.daemon.auto_start,
        client_error_code=ErrorCode.DAEMON_NOT_RUNNING,
        on_registry_corrupt=lambda exc: CliError(
            registry_corrupt_message(exc, retry_command=retry_command),
            code=ErrorCode.DAEMON_REGISTRY_CORRUPT,
        ),
    )


def _payload(result: CallToolResult) -> dict[str, Any]:
    """Extract the JSON payload dict returned by the credentials tool.

    Prefers the structured result; falls back to parsing the first
    JSON text block when the server does not populate one.

    Args:
        result (CallToolResult): The raw MCP call result.

    Returns:
        dict[str, Any]: The decoded payload dict.

    Raises:
        CliError: When no structured dict payload can be recovered.
    """
    if isinstance(result.structuredContent, dict):
        return result.structuredContent
    for block in result.content:
        if isinstance(block, TextContent):
            try:
                parsed = json.loads(block.text)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                return parsed
    raise CliError(
        f"{_CREDENTIALS_TOOL!r} returned no structured result.",
        code=ErrorCode.MCP_REQUEST_FAILED,
    )


# ---------------------------------------------------------------------------
# credentials
# ---------------------------------------------------------------------------

_OUTPUT_CREDENTIALS = OutputSpec(
    "object",
    (
        OutputField("auth_type", "string", "Authentication type, uppercased."),
        OutputField(
            "auth_token",
            "string",
            "Plaintext auth token (empty for anonymous auth).",
        ),
        OutputField(
            "connection_url", "string", "Base server URL without auth parameters."
        ),
        OutputField(
            "connection_url_with_auth",
            "string",
            "Browser-ready URL including the auth token when applicable.",
        ),
    ),
)


@session.command(
    "credentials",
    output_spec=_OUTPUT_CREDENTIALS,
    help=build_help(
        summary="Print one Community session's browser-login credentials.",
        description=(
            "Wraps the session_community_credentials MCP tool. The output "
            "contains a PLAINTEXT auth token by design (unlike 'dh-mcp daemon', "
            "which redacts secrets) so you can open the session in a browser. "
            "Retrieval is gated by security.credential_retrieval_mode in "
            "community/settings.json, which defaults to 'none'; when retrieval "
            "is disabled, or the session is missing or not a Community session, "
            "the command exits 3 with the tool's explanation."
        ),
        arguments=(
            (
                "SESSION_ID",
                "Community session id, e.g. 'community:community:my-session'. "
                "Run 'dh-mcp tool call sessions_list' to discover ids.",
            ),
        ),
        output=_OUTPUT_CREDENTIALS,
        examples=(
            "$ dh-mcp session credentials community:community:my-session",
            "$ dh-mcp -o json session credentials community:community:my-session",
        ),
        see_also=("dh-mcp tool call sessions_list",),
        exit_codes=_CREDENTIALS_EXIT_CODES,
        error_codes=(
            (
                "tool_returned_error",
                "Credential retrieval is disabled or the session is "
                "missing/ineligible (exit 3).",
            ),
            *_ACQUIRE_ERROR_CODES,
        ),
    ),
)
@click.argument("session_id")
@click.pass_obj
@run_async
async def session_credentials(runtime: Runtime, session_id: str) -> None:
    """Fetch one Community session's browser-login credentials."""
    handle = await _acquire(runtime, retry_command="dh-mcp session credentials")

    try:
        async with McpClient(
            handle,
            request_timeout_seconds=runtime.config.cli.request.timeouts.default_seconds,
        ) as client:
            result = await client.call_tool(
                _CREDENTIALS_TOOL, {"session_id": session_id}
            )
    except McpClientError as exc:
        raise CliError(str(exc), code=ErrorCode.MCP_REQUEST_FAILED) from exc

    payload = _payload(result)
    if not payload.get("success", False):
        raise CliError(
            str(payload.get("error") or "Credential retrieval failed."),
            code=ErrorCode.TOOL_RETURNED_ERROR,
        )

    credentials = {field: payload.get(field) for field in _CREDENTIAL_FIELDS}
    click.echo(format_output(credentials, output=runtime.config.cli.output.format))
