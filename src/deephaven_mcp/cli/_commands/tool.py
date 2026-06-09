"""``dh-mcp tool`` noun group: inspect and invoke MCP tools.

Verbs: ``list``, ``show``, ``call``.
"""

from __future__ import annotations

__all__ = ["tool"]

import json
from typing import Any

import click

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


@click.group(cls=HelpfulGroup)
def tool() -> None:
    """Inspect and invoke MCP tools exposed by the daemon.

    These commands connect to the daemon (auto-starting it unless
    --no-auto-start is set) and speak MCP: 'list' enumerates the
    available tools, 'show' prints one tool's input schema, and
    'call' invokes a tool and prints its result.
    """


_EXIT_CODES = (
    (0, "success"),
    (2, "user-facing failure (config, daemon, or MCP request)"),
)

# call uniquely exits 3 when the invoked tool returns isError=True.
_CALL_EXIT_CODES = (
    *_EXIT_CODES,
    (3, "the invoked MCP tool returned isError=True"),
)

# Error codes every tool verb can raise: the shared _acquire path
# (daemon_not_running / daemon_startup_timeout / daemon_registry_corrupt)
# plus the MCP-client block (mcp_request_failed). Single-sourced so the
# three verbs can't drift from what _acquire actually raises.
_ACQUIRE_ERROR_CODES = (
    ("daemon_not_running", "No daemon and --no-auto-start was set."),
    ("daemon_startup_timeout", "Auto-started daemon did not register in time."),
    ("daemon_registry_corrupt", "daemon.json exists but cannot be parsed."),
    ("mcp_request_failed", "The MCP transport reported an error."),
)

_TOOL_FIELDS = (
    OutputField("name", "string", "Tool name."),
    OutputField("description", "string", "Human-readable description."),
    OutputField("inputSchema", "object", "JSON Schema of the tool's arguments."),
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _parse_arg_pair(raw: str) -> tuple[str, Any]:
    """Parse a single ``key=value`` argument string.

    JSON-decoding is attempted on the value; on failure the raw
    string is preserved so the operator can pass plain strings
    without quoting tricks. Keys must be non-empty.

    Args:
        raw (str): The raw ``key=value`` token.

    Returns:
        tuple[str, Any]: ``(key, decoded_value)``.

    Raises:
        ValueError: When the token does not contain ``=`` or the
            key portion is empty.
    """
    if "=" not in raw:
        raise ValueError(
            f"--arg expects key=value; got {raw!r}. Use a literal '=' to "
            f"separate the key and the value."
        )
    key, _, value = raw.partition("=")
    if not key:
        raise ValueError(f"--arg has an empty key in {raw!r}.")
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        decoded = value
    return key, decoded


async def _acquire(runtime: Runtime, *, retry_command: str) -> DaemonRegistryEntry:
    """Acquire a live daemon for a ``dh-mcp tool`` subcommand.

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


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

_OUTPUT_LIST = OutputSpec("list", _TOOL_FIELDS, note="Array of registered tools.")


@tool.command(
    "list",
    output_spec=_OUTPUT_LIST,
    help=build_help(
        summary="List the MCP tools the daemon exposes.",
        description=(
            "Enumerates the tools registered on the daemon. Internal tools "
            "(names beginning with '_') are hidden by default; pass --all to "
            "include them. Use this to discover names for 'tool show' and "
            "'tool call'."
        ),
        output=_OUTPUT_LIST,
        examples=(
            "$ dh-mcp tool list",
            "$ dh-mcp -o json tool list | jq '.[].name'",
        ),
        see_also=("dh-mcp tool show NAME", "dh-mcp tool call NAME"),
        exit_codes=_EXIT_CODES,
        error_codes=_ACQUIRE_ERROR_CODES,
    ),
)
@click.option(
    "--all",
    "show_all",
    is_flag=True,
    default=False,
    help="Include internal tools whose names begin with '_'.",
)
@click.pass_obj
@run_async
async def tool_list(runtime: Runtime, show_all: bool) -> None:
    """List MCP tools registered on the daemon."""
    handle = await _acquire(runtime, retry_command="dh-mcp tool list")
    try:
        async with McpClient(
            handle,
            request_timeout_seconds=runtime.config.cli.request.timeouts.default_seconds,
        ) as client:
            tools = await client.list_tools()
    except McpClientError as exc:
        raise CliError(str(exc), code=ErrorCode.MCP_REQUEST_FAILED) from exc
    visible = tools if show_all else [t for t in tools if not t.name.startswith("_")]
    click.echo(format_output(visible, output=runtime.config.cli.output.format))


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------

_OUTPUT_SHOW = OutputSpec("object", _TOOL_FIELDS)


@tool.command(
    "show",
    output_spec=_OUTPUT_SHOW,
    help=build_help(
        summary="Print one tool's name, description, and input schema.",
        description=(
            "Prints a single tool's metadata, including its full JSON input "
            "schema. In human mode the schema is pretty-printed below the "
            "summary; in json/yaml mode the whole tool object is emitted."
        ),
        arguments=(("NAME", "Tool name. Run 'dh-mcp tool list' to discover names."),),
        output=_OUTPUT_SHOW,
        examples=(
            "$ dh-mcp tool show sessions_list",
            "$ dh-mcp -o json tool show sessions_list | jq .inputSchema",
        ),
        see_also=("dh-mcp tool list", "dh-mcp tool call NAME"),
        exit_codes=_EXIT_CODES,
        error_codes=(
            ("tool_not_found", "NAME is not a registered tool."),
            *_ACQUIRE_ERROR_CODES,
        ),
    ),
)
@click.argument("name")
@click.pass_obj
@run_async
async def tool_show(runtime: Runtime, name: str) -> None:
    """Show one MCP tool's metadata."""
    handle = await _acquire(runtime, retry_command="dh-mcp tool show")
    try:
        async with McpClient(
            handle,
            request_timeout_seconds=runtime.config.cli.request.timeouts.default_seconds,
        ) as client:
            tools = await client.list_tools()
    except McpClientError as exc:
        raise CliError(str(exc), code=ErrorCode.MCP_REQUEST_FAILED) from exc
    match = next((t for t in tools if t.name == name), None)
    if match is None:
        # The "Available:" suggestion mirrors ``tool list``'s default
        # filtering (no internal ``_``-prefixed tools) unless the
        # operator explicitly asked for one by name, in which case
        # showing the full set helps them confirm the spelling.
        show_internal = name.startswith("_")
        names = sorted(
            t.name for t in tools if show_internal or not t.name.startswith("_")
        )
        suggestion = f" Available: {', '.join(names)}." if names else ""
        raise CliError(
            f"Tool {name!r} is not registered.{suggestion}",
            code=ErrorCode.TOOL_NOT_FOUND,
        )
    output = runtime.config.cli.output.format
    if output == "human":
        # Show the tool list rendering plus a JSON dump of the schema.
        header = format_output([match], output="human")
        schema = json.dumps(match.inputSchema or {}, indent=2, sort_keys=True)
        click.echo(header)
        click.echo("")
        click.echo("Input schema:")
        click.echo(schema)
        return
    click.echo(format_output(match, output=output))


# ---------------------------------------------------------------------------
# call
# ---------------------------------------------------------------------------

_OUTPUT_CALL = OutputSpec(
    "object",
    (
        OutputField("content", "array", "Result blocks (text, image, or resource)."),
        OutputField(
            "isError", "boolean", "True when the tool reported an error (exit 3)."
        ),
        OutputField(
            "structuredContent",
            "object",
            "Structured result, when the tool returns one.",
        ),
    ),
)


@tool.command(
    "call",
    output_spec=_OUTPUT_CALL,
    help=build_help(
        summary="Invoke a single MCP tool and print its result.",
        description=(
            "Each --arg is a key=value pair; the value is JSON-decoded when "
            "possible (so --arg n=42 sends the integer 42, while --arg s=hi "
            "sends the string hi). Repeat --arg for multiple arguments. "
            "Exit code 3 if the tool returns isError=True."
        ),
        arguments=(("NAME", "Tool name. Run 'dh-mcp tool list' to discover names."),),
        output=_OUTPUT_CALL,
        examples=(
            "$ dh-mcp tool call sessions_list",
            "$ dh-mcp tool call sessions_list --arg type=community",
            "$ dh-mcp -o json tool call session_community_create "
            "--arg session_name=demo",
        ),
        see_also=("dh-mcp tool list", "dh-mcp tool show NAME"),
        exit_codes=_CALL_EXIT_CODES,
        error_codes=(
            ("arg_parse_error", "A --arg key=value token was malformed."),
            ("tool_returned_error", "The tool returned isError=true (exit 3)."),
            *_ACQUIRE_ERROR_CODES,
        ),
    ),
)
@click.argument("name")
@click.option(
    "--arg",
    "args",
    multiple=True,
    metavar="KEY=VALUE",
    help="Tool argument (repeatable). Values are JSON-decoded when possible.",
)
@click.pass_obj
@run_async
async def tool_call(runtime: Runtime, name: str, args: tuple[str, ...]) -> None:
    """Invoke a single MCP tool and print its result."""
    try:
        arguments = dict(_parse_arg_pair(p) for p in args)
    except ValueError as exc:
        raise CliError(str(exc), code=ErrorCode.ARG_PARSE_ERROR) from exc

    handle = await _acquire(runtime, retry_command="dh-mcp tool call")

    try:
        async with McpClient(
            handle,
            request_timeout_seconds=runtime.config.cli.request.timeouts.default_seconds,
        ) as client:
            result = await client.call_tool(name, arguments)
    except McpClientError as exc:
        raise CliError(str(exc), code=ErrorCode.MCP_REQUEST_FAILED) from exc

    click.echo(format_output(result, output=runtime.config.cli.output.format))
    if result.isError:
        raise CliError(
            f"Tool {name!r} returned isError=True.",
            code=ErrorCode.TOOL_RETURNED_ERROR,
        )
