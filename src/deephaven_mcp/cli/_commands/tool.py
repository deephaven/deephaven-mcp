"""``dhcli tool`` noun group: inspect and invoke MCP tools.

Verbs: ``list``, ``show``, ``call``.
"""

from __future__ import annotations

__all__ = ["tool"]

import json

import click

from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._command import HelpfulGroup
from deephaven_mcp.cli._commands._wrapping import (
    acquire,
    call_tool,
    parse_key_value,
    wrapper_error_codes,
)
from deephaven_mcp.cli._echo import echo_payload
from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode
from deephaven_mcp.cli._format import format_output
from deephaven_mcp.cli._help import (
    HelpEntry,
    HelpSpec,
    OutputField,
    OutputSpec,
)
from deephaven_mcp.cli._mcp_client import McpClient, McpClientError
from deephaven_mcp.cli._runtime import Runtime


@click.group(cls=HelpfulGroup)
def tool() -> None:
    """Inspect and invoke MCP tools exposed by the daemon.

    These commands connect to the daemon (auto-starting it unless
    --no-auto-start is set) and speak MCP: 'list' enumerates the
    available tools, 'show' prints one tool's input schema, and
    'call' invokes a tool and prints its result.

    This group is the escape hatch, not the main road: it reaches every
    tool, including ones no verb fronts, but 'tool call' passes your
    arguments straight through. Prefer the first-class verbs
    ('session', 'table', 'catalog', 'pq', 'docs') wherever one exists —
    they resolve the target, confirm a destructive action, and document
    the output shape.
    """


_TOOL_FIELDS = (
    OutputField("name", "string", "Tool name."),
    OutputField("description", "string", "Human-readable description."),
    OutputField("inputSchema", "object", "JSON Schema of the tool's arguments."),
)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

_OUTPUT_LIST = OutputSpec("list", _TOOL_FIELDS, note="Array of registered tools.")


@tool.command(
    "list",
    help_spec=HelpSpec(
        summary="List the MCP tools the daemon exposes.",
        description=(
            "Enumerates the tools registered on the daemon. Internal tools "
            "(names beginning with '_') are hidden by default; pass --all to "
            "include them. Use this to discover names for 'tool show' and "
            "'tool call'."
        ),
        output=_OUTPUT_LIST,
        examples=(
            "$ dhcli tool list",
            "$ dhcli -o json tool list | jq '.[].name'",
        ),
        see_also=("dhcli tool show NAME", "dhcli tool call NAME"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=wrapper_error_codes(tool_error=False, request_timeout=False),
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
    handle = await acquire(runtime, retry_command="dhcli tool list")
    try:
        async with McpClient.for_daemon(
            handle,
            request_timeout_seconds=runtime.config.cli.request.timeouts.default_seconds,
        ) as client:
            tools = await client.list_tools()
    except McpClientError as exc:
        raise CliError(str(exc), code=ErrorCode.MCP_REQUEST_FAILED) from exc
    visible = tools if show_all else [t for t in tools if not t.name.startswith("_")]
    echo_payload(runtime, visible, empty_message="(no tools registered)")


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------

_OUTPUT_SHOW = OutputSpec("object", _TOOL_FIELDS)


@tool.command(
    "show",
    help_spec=HelpSpec(
        summary="Print one tool's name, description, and input schema.",
        description=(
            "Prints a single tool's metadata, including its full JSON input "
            "schema. In human mode the schema is pretty-printed below the "
            "summary; in json/yaml mode the whole tool object is emitted."
        ),
        arguments=(
            HelpEntry("NAME", "Tool name. Run 'dhcli tool list' to discover names."),
        ),
        output=_OUTPUT_SHOW,
        examples=(
            "$ dhcli tool show sessions_list",
            "$ dhcli -o json tool show sessions_list | jq .inputSchema",
        ),
        see_also=("dhcli tool list", "dhcli tool call NAME"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.TOOL_NOT_FOUND,
            *wrapper_error_codes(tool_error=False, request_timeout=False),
        ),
    ),
)
@click.argument("name")
@click.pass_obj
@run_async
async def tool_show(runtime: Runtime, name: str) -> None:
    """Show one MCP tool's metadata."""
    handle = await acquire(runtime, retry_command="dhcli tool show")
    try:
        async with McpClient.for_daemon(
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
    echo_payload(runtime, match)


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
    help_spec=HelpSpec(
        summary="Invoke a single MCP tool and print its result.",
        description=(
            "Each --arg is a key=value pair; the value is JSON-decoded when "
            "possible (so --arg n=42 sends the integer 42, while --arg s=hi "
            "sends the string hi). Repeat --arg for multiple arguments. "
            "Exit code 3 if the tool returns isError=True. "
            "Use this for a tool no verb fronts, or to pass a parameter a "
            "verb does not expose. It is a raw passthrough: none of the "
            "safety and convenience a first-class verb adds applies — no "
            "sticky-context resolution, no confirmation before a destructive "
            "call, no --yes, no documented output schema, and the result is "
            "the unwrapped MCP envelope rather than the verb's shaped "
            "payload. Calling a delete or script tool this way acts "
            "immediately on exactly the id you pass, so name the target "
            "deliberately."
        ),
        arguments=(
            HelpEntry(
                "NAME",
                "Tool name. Run 'dhcli tool list' to discover names and "
                "'dhcli tool show NAME' for its input schema.",
            ),
        ),
        output=_OUTPUT_CALL,
        examples=(
            "$ dhcli tool call sessions_list",
            "$ dhcli tool call sessions_list --arg type=community",
            "$ dhcli -o json tool call session_community_create "
            "--arg session_name=demo",
            "$ dhcli tool call sessions_list | jq '.structuredContent.sessions'",
        ),
        see_also=(
            "dhcli tool list",
            "dhcli tool show NAME",
            "dhcli agents tree",
        ),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(ErrorCode.ARG_PARSE_ERROR, *wrapper_error_codes()),
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
    arguments = dict(parse_key_value(p, decode_json=True) for p in args)
    handle = await acquire(runtime, retry_command="dhcli tool call")
    result = await call_tool(handle, runtime, name, arguments)
    echo_payload(runtime, result)
    if result.isError:
        raise CliError(
            f"Tool {name!r} returned isError=True.",
            code=ErrorCode.TOOL_RETURNED_ERROR,
        )
