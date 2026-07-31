"""``dhcli docs`` noun group: query the Deephaven docs MCP server.

Verbs: ``ask``, ``status``.
"""

from __future__ import annotations

__all__ = ["docs"]

import asyncio
import json
import time

import click
from mcp.types import CallToolResult

from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._command import HelpfulGroup
from deephaven_mcp.cli._commands._wrapping import (
    require_success,
    tool_payload,
)
from deephaven_mcp.cli._echo import echo_payload
from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode
from deephaven_mcp.cli._help import (
    HelpEntry,
    HelpSpec,
    OutputField,
    OutputSpec,
)
from deephaven_mcp.cli._mcp_client import (
    McpClient,
    McpClientError,
    McpRequestTimeoutError,
)
from deephaven_mcp.cli._runtime import Runtime

_DOCS_TIMEOUT_SETTING = "cli.docs.timeouts.request_seconds"
"""Logical config path for the docs request timeout, named in error hints."""


@click.group(cls=HelpfulGroup)
def docs() -> None:
    """Query the Deephaven documentation MCP server.

    These commands talk directly to the docs MCP server named by the
    configured cli.docs.url (default: the Deephaven-hosted production
    docs server) — the local daemon is not involved and is never
    started. 'ask' sends a one-shot question to the documentation
    assistant; 'status' checks that the configured server is reachable.
    """


def _docs_client(runtime: Runtime) -> McpClient:
    """Build an unopened client for the configured docs MCP server.

    Args:
        runtime (Runtime): The active CLI runtime, for the docs URL and
            request timeout.

    Returns:
        McpClient: An unopened client pointed at ``docs.url``.
    """
    return McpClient(
        runtime.config.cli.docs.url,
        request_timeout_seconds=runtime.config.cli.docs.timeouts.request_seconds,
        timeout_setting=_DOCS_TIMEOUT_SETTING,
    )


async def _call_docs_tool(
    runtime: Runtime, name: str, arguments: dict[str, object]
) -> CallToolResult:
    """Invoke one docs-server MCP tool over a fresh client connection.

    The configured request timeout bounds the complete operation —
    connect, MCP initialize, and the tool call — so a server that
    accepts the connection but stalls during initialization still
    fails within the budget, as in ``docs status``.

    Args:
        runtime (Runtime): The active CLI runtime.
        name (str): The MCP tool name to invoke.
        arguments (dict[str, object]): The tool arguments.

    Returns:
        CallToolResult: The raw MCP call result.

    Raises:
        CliError: When the MCP transport reports an error —
            ``mcp_request_timeout`` when the operation does not
            complete within the request timeout,
            ``mcp_request_failed`` for everything else.
    """
    timeout_seconds = runtime.config.cli.docs.timeouts.request_seconds
    try:
        # call_tool applies the request timeout only to the tool call
        # itself; bound the whole operation (connect + initialize +
        # call) so a stall before the call also maps to a timeout.
        async with asyncio.timeout(timeout_seconds):
            async with _docs_client(runtime) as client:
                return await client.call_tool(name, arguments)
    except McpRequestTimeoutError as exc:
        raise CliError(str(exc), code=ErrorCode.MCP_REQUEST_TIMEOUT) from exc
    except TimeoutError as exc:
        raise CliError(
            f"docs tool call {name!r} did not complete within "
            f"{timeout_seconds} seconds (including connect and "
            f"initialize). The server may still finish processing the "
            f"request — if the operation changes state, verify the "
            f"result before retrying. To allow more time, pass --timeout "
            f"or raise {_DOCS_TIMEOUT_SETTING}.",
            code=ErrorCode.MCP_REQUEST_TIMEOUT,
        ) from exc
    except McpClientError as exc:
        raise CliError(
            f"Could not reach the docs server at "
            f"{runtime.config.cli.docs.url}: {exc}",
            code=ErrorCode.MCP_REQUEST_FAILED,
        ) from exc


def _parse_history(raw: str) -> list[dict[str, str]]:
    """Parse the ``--history`` JSON payload into the tool argument value.

    Validates the declared wire type of the ``docs_chat`` tool's
    ``history`` parameter — ``list[dict[str, str]]``, a JSON array of
    objects with string values. Entry semantics (required keys, role
    vocabulary) are owned by the docs server, which rejects invalid
    messages with a structured error.

    Args:
        raw (str): The raw ``--history`` option value; a JSON array of
            ``{"role", "content"}`` objects.

    Returns:
        list[dict[str, str]]: The decoded message list.

    Raises:
        CliError: With :attr:`ErrorCode.ARG_PARSE_ERROR` when ``raw`` is
            not valid JSON, not a JSON array, or an entry is not a JSON
            object with string values.
    """
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CliError(
            f"--history is not valid JSON: {exc}", code=ErrorCode.ARG_PARSE_ERROR
        ) from exc
    if not isinstance(parsed, list):
        raise CliError(
            "--history must be a JSON array of {role, content} objects.",
            code=ErrorCode.ARG_PARSE_ERROR,
        )
    for index, entry in enumerate(parsed):
        if not isinstance(entry, dict) or not all(
            isinstance(value, str) for value in entry.values()
        ):
            raise CliError(
                f"--history entry {index} must be a JSON object with string "
                f"values, got {json.dumps(entry)}.",
                code=ErrorCode.ARG_PARSE_ERROR,
            )
    # Only the wire type (the tool parameter's declared
    # list[dict[str, str]]) is checked here. Entry semantics — which
    # keys are required, which roles are meaningful — are deliberately
    # single-homed at the docs server's terminal validator
    # (OpenAIClient._validate_history); a client-side copy of those
    # rules drifts (e.g. it would wrongly reject 'system' roles the
    # terminal level accepts).
    return parsed


# ---------------------------------------------------------------------------
# ask
# ---------------------------------------------------------------------------

_OUTPUT_ASK = OutputSpec(
    "object",
    (
        OutputField(
            "response",
            "string",
            "The documentation assistant's answer (may include code examples "
            "and links).",
        ),
    ),
)


@docs.command(
    "ask",
    wraps_tool="docs_chat",
    help_spec=HelpSpec(
        summary="Ask the Deephaven documentation assistant a question.",
        description=(
            "Sends a one-shot question to the docs MCP server named by the "
            "configured cli.docs.url (default: the Deephaven-hosted "
            "production docs server). The assistant is LLM-backed, so responses "
            "typically take several seconds; raise --timeout for complex "
            "questions. Multi-turn follow-ups stay one-shot: pass the prior "
            "exchange via --history. The local daemon is not involved."
        ),
        arguments=(
            HelpEntry(
                "PROMPT",
                "The question to ask, as one shell-quoted string. Specific "
                "questions get better answers.",
            ),
        ),
        output=_OUTPUT_ASK,
        examples=(
            '$ dhcli docs ask "How do I join two tables?"',
            '$ dhcli docs ask "Show me a ring table example" --language python',
            '$ dhcli -o json docs ask "What is a liveness scope?" | jq -r .response',
            '$ dhcli docs ask "How do I filter it?" --history '
            '\'[{"role":"user","content":"How do I make a time table?"},'
            '{"role":"assistant","content":"Use time_table()..."}]\'',
        ),
        see_also=("dhcli docs status",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
        error_codes=(
            ErrorCode.ARG_PARSE_ERROR,
            ErrorCode.TOOL_RETURNED_ERROR,
            ErrorCode.MCP_REQUEST_FAILED,
            ErrorCode.MCP_REQUEST_TIMEOUT,
        ),
    ),
)
@click.argument("prompt")
@click.option(
    "--language",
    "programming_language",
    type=click.Choice(["python", "groovy"]),
    default=None,
    help=(
        "Programming language for code examples and syntax in the answer. "
        "Omit for general conceptual questions."
    ),
)
@click.option(
    "--core-version",
    "deephaven_core_version",
    default=None,
    metavar="VERSION",
    help="Deephaven Community (Core) version to tailor the answer to, e.g. 0.39.0.",
)
@click.option(
    "--enterprise-version",
    "deephaven_enterprise_version",
    default=None,
    metavar="VERSION",
    help=(
        "Deephaven Enterprise (Core+) version to tailor the answer to, "
        "e.g. 1.20240517.344."
    ),
)
@click.option(
    "--history",
    "history",
    default=None,
    metavar="JSON",
    help=(
        "Prior conversation turns for a follow-up question: a JSON array of "
        "message objects — each with a string 'role' (e.g. 'user' or "
        "'assistant') and a string 'content' — oldest first. The docs "
        "server validates entry semantics. Omit for a fresh question."
    ),
)
@click.pass_obj
@run_async
async def docs_ask(
    runtime: Runtime,
    prompt: str,
    programming_language: str | None,
    deephaven_core_version: str | None,
    deephaven_enterprise_version: str | None,
    history: str | None,
) -> None:
    """Ask the documentation assistant a one-shot question."""
    arguments: dict[str, object] = {"prompt": prompt}
    if programming_language is not None:
        arguments["programming_language"] = programming_language
    if deephaven_core_version is not None:
        arguments["deephaven_core_version"] = deephaven_core_version
    if deephaven_enterprise_version is not None:
        arguments["deephaven_enterprise_version"] = deephaven_enterprise_version
    if history is not None:
        arguments["history"] = _parse_history(history)
    result = await _call_docs_tool(runtime, "docs_chat", arguments)
    payload = require_success(tool_payload(result), tool="docs_chat")
    echo_payload(runtime, payload)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

_OUTPUT_STATUS = OutputSpec(
    "object",
    (
        OutputField("url", "string", "The effective docs server endpoint URL."),
        OutputField(
            "reachable",
            "boolean",
            "Always true on success; an unreachable server exits 2 instead.",
        ),
        OutputField(
            "tools", "array", "Names of the MCP tools the docs server exposes."
        ),
        OutputField(
            "latency_ms",
            "integer",
            "Round-trip time of the connect + tool-list probe, in milliseconds.",
        ),
    ),
)


@docs.command(
    "status",
    help_spec=HelpSpec(
        summary="Check that the configured docs MCP server is reachable.",
        description=(
            "Connects to the docs MCP server named by the configured "
            "cli.docs.url, initializes an MCP session, and lists its tools. "
            "Use this to distinguish an unreachable or misconfigured docs "
            "server from a slow assistant answer when 'docs ask' fails. "
            "Exits 2 with mcp_request_failed when the server cannot be "
            "reached, or with mcp_request_timeout when the whole probe does "
            "not complete within the request timeout (--timeout or "
            "cli.docs.timeouts.request_seconds). The local daemon is not "
            "involved."
        ),
        output=_OUTPUT_STATUS,
        examples=(
            "$ dhcli docs status",
            "$ dhcli -o json docs status | jq .latency_ms",
        ),
        see_also=("dhcli docs ask PROMPT",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.MCP_REQUEST_FAILED,
            ErrorCode.MCP_REQUEST_TIMEOUT,
        ),
    ),
)
@click.pass_obj
@run_async
async def docs_status(runtime: Runtime) -> None:
    """Check connectivity to the configured docs MCP server."""
    url = runtime.config.cli.docs.url
    timeout_seconds = runtime.config.cli.docs.timeouts.request_seconds
    started = time.monotonic()
    try:
        # list_tools applies no per-request read timeout, so bound the
        # whole probe (connect + initialize + tool list) here; a server
        # that connects but stalls must not exceed the request budget.
        async with asyncio.timeout(timeout_seconds):
            async with _docs_client(runtime) as client:
                tools = await client.list_tools()
    except TimeoutError as exc:
        raise CliError(
            f"docs status probe of {url} timed out after {timeout_seconds} "
            f"seconds. To allow more time, pass --timeout or raise "
            f"{_DOCS_TIMEOUT_SETTING}.",
            code=ErrorCode.MCP_REQUEST_TIMEOUT,
        ) from exc
    except McpClientError as exc:
        raise CliError(
            f"Could not reach the docs server at {url}: {exc}",
            code=ErrorCode.MCP_REQUEST_FAILED,
        ) from exc
    latency_ms = int((time.monotonic() - started) * 1000)
    echo_payload(
        runtime,
        {
            "url": url,
            "reachable": True,
            "tools": sorted(t.name for t in tools),
            "latency_ms": latency_ms,
        },
        sort_keys=False,
    )
