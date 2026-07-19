"""Smoke-test client for the Deephaven MCP systems server.

Targets the unified ``dh-mcp-systems-server`` binary that hosts both
community and enterprise capabilities in one process.

Features
--------
- Connects to a running systems server via streamable-http or stdio.
- Lists every registered tool.
- Calls a representative read-only subset of community-side and
  enterprise-side tools with sample arguments. The one minor
  side-effect is ``session_script_run``, which executes a hello-world
  script on the targeted community session, in the session's own
  language (Python or Groovy, read from ``session_details``).
- Exits with code ``0`` when every tool call returned without
  raising, or ``1`` when at least one call failed — suitable as a CI
  smoke check or post-deploy ping.

Exit codes
----------
- ``0`` — every called tool returned without raising.
- ``1`` — at least one tool call raised an exception.
- ``2`` — fatal error before tools could be exercised (bad CLI args,
  connection failure, transport setup error, etc.).
- ``130`` — interrupted by user (Ctrl-C); follows the conventional
  ``128 + SIGINT`` convention.

Usage examples
--------------
    # Streamable-HTTP transport (the systems server's HTTP mode)
    $ python mcp_systems_test_client.py \\
        --transport streamable-http \\
        --url http://127.0.0.1:8000/mcp \\
        --psk supersecretpsk

    # stdio transport (launch the server as a child process)
    $ python mcp_systems_test_client.py \\
        --transport stdio \\
        --stdio-cmd "uv run dh-mcp-systems-server --transport stdio" \\
        --env DH_MCP_DATA_DIR=/path/to/data-root

    # Fail on the first tool error (useful in CI):
    $ python mcp_systems_test_client.py ... --strict

Authentication
--------------
Over HTTP, the systems server is gated by :class:`PSKMiddleware` which
requires the ``X-Deephaven-PSK`` header. Pass the PSK via ``--psk``;
the script sends it in the right header. (``Authorization: Bearer``
is **not** accepted by this server.)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import shlex
import sys
from contextlib import AsyncExitStack

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_LOGGER = logging.getLogger(__name__)

# The systems server's :class:`PSKMiddleware` requires this exact header
# on every authenticated request; the value is the operator's PSK.
_PSK_HEADER = "X-Deephaven-PSK"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the systems-server smoke test.

    Returns:
        argparse.Namespace: Parsed arguments with fields:
            - transport: Transport type (``streamable-http`` or ``stdio``).
            - url: HTTP server URL (default ``http://127.0.0.1:8000/mcp``).
            - stdio_cmd: Command to launch the stdio server.
            - env: List of ``KEY=VALUE`` strings to inject into the stdio subprocess.
            - psk: Optional pre-shared key for HTTP transport (sent as ``X-Deephaven-PSK``).
            - session_id: Community session id to exercise with demo tools.
            - strict: When set, abort on the first failed tool call.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Smoke-test client for dh-mcp-systems-server. Exits 0 if every "
            "tool call returned, 1 if any failed."
        )
    )
    parser.add_argument(
        "--transport",
        choices=["streamable-http", "stdio"],
        default="streamable-http",
        help="Transport type (streamable-http or stdio).",
    )
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8000/mcp",
        help="HTTP server URL (default: http://127.0.0.1:8000/mcp).",
    )
    parser.add_argument(
        "--stdio-cmd",
        default="uv run dh-mcp-systems-server --transport stdio",
        help=(
            "Stdio server command (pass as a shell string, e.g. "
            "'uv run dh-mcp-systems-server --transport stdio')."
        ),
    )
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        help=(
            "Environment variable for stdio transport, format KEY=VALUE. "
            "Can be specified multiple times."
        ),
    )
    parser.add_argument(
        "--psk",
        default=None,
        help=(
            f"Pre-shared key for HTTP transport. Sent in the "
            f"{_PSK_HEADER!r} header. Required for HTTP unless the server "
            f"is unconfigured (it normally rejects HTTP without a PSK)."
        ),
    )
    parser.add_argument(
        "--session-id",
        default="community:community:default",
        help=(
            "Fully qualified community session id to exercise with the demo "
            "tools. The single-tenant grammar is "
            "'community:community:<session_name>' (e.g. "
            "'community:community:default')."
        ),
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Abort on the first failed tool call instead of running through them all.",
    )
    return parser.parse_args()


class _ToolFailure(Exception):
    """Raised internally to abort a strict run on the first tool failure."""


_DEMO_SCRIPTS = {
    "python": (
        "from deephaven import empty_table\n"
        "demo_table = empty_table(3).update('X = i')\n"
        "print('hello world')"
    ),
    "groovy": ('demo_table = emptyTable(3).update("X = i")\n' 'println "hello world"'),
}
"""Language-appropriate demo scripts; each creates ``demo_table`` for the
schema check that follows and prints a hello-world line."""


def _extract_programming_language(session_details_text: str) -> str:
    """Extract the session's programming language from ``session_details`` output.

    Args:
        session_details_text: Raw text returned by the ``session_details``
            tool call (JSON on success, an error string otherwise).

    Returns:
        str: The lowercased language (``"python"`` or ``"groovy"``),
        defaulting to ``"python"`` when the payload cannot be parsed or
        omits the field.
    """
    try:
        details = json.loads(session_details_text)
        language = details["session"]["programming_language"]
        return str(language).lower()
    except (json.JSONDecodeError, KeyError, TypeError):
        _LOGGER.warning(
            "Could not determine programming language from session_details; "
            "assuming 'python'"
        )
        return "python"


async def call_tool(
    session: ClientSession,
    tool_name: str,
    arguments: dict,
    *,
    strict: bool,
    failures: list[str],
) -> str:
    """Call an MCP tool and render the result as text.

    Args:
        session: An initialized :class:`mcp.ClientSession`.
        tool_name: Name of the tool to call (must be registered on the server).
        arguments: Dictionary of arguments to pass to the tool.
        strict: When ``True``, raise :class:`_ToolFailure` on any exception so the
            caller can short-circuit the run.
        failures: Mutable list to which each failed ``tool_name`` is appended,
            used to drive the script's exit code.

    Returns:
        str: The first text block of the tool result when present, otherwise the
        ``str()`` of the full result. On exception the error is logged, recorded
        in ``failures``, and re-raised as :class:`_ToolFailure` if ``strict`` is
        set (otherwise returned as ``"Error: <str(exc)>"``).
    """
    try:
        result = await session.call_tool(tool_name, arguments=arguments)
    except Exception as e:
        _LOGGER.error(f"Error calling tool {tool_name}: {e!r}", exc_info=True)
        failures.append(tool_name)
        if strict:
            raise _ToolFailure(tool_name) from e
        return f"Error: {e}"

    if result.content:
        first = result.content[0]
        text = getattr(first, "text", None)
        if text is not None:
            return text
    return str(result)


async def _run_session(
    session: ClientSession,
    demo_session_id: str,
    *,
    strict: bool,
) -> list[str]:
    """Initialize an MCP session and exercise a representative tool set.

    Args:
        session: The :class:`mcp.ClientSession` to drive (not yet initialized).
        demo_session_id: Fully qualified community session id to use as the
            ``id`` argument for demo tool calls.
        strict: When ``True``, abort on the first failed tool call.

    Returns:
        list[str]: Names of tools whose calls raised. Empty on a fully
            successful run.
    """
    init_result = await session.initialize()
    _LOGGER.info(f"Connected to MCP server: {init_result!r}")

    tools_result = await session.list_tools()
    tool_names = [t.name for t in tools_result.tools]
    _LOGGER.info(f"Available tools: {tool_names}")
    print("Available tools:", tool_names)

    failures: list[str] = []
    try:
        await test_tools(session, demo_session_id, strict=strict, failures=failures)
    except _ToolFailure:
        # Strict mode short-circuit; ``failures`` already records the cause.
        pass
    return failures


async def main() -> int:
    """Run the smoke test and return the process exit code.

    Establishes a connection over the selected transport
    (``streamable-http`` or ``stdio``), lists registered tools, calls a
    representative subset, and tallies failures.

    Returns:
        int: ``0`` when every tool call returned without raising,
            ``1`` when at least one call failed.

    Raises:
        ValueError: If a ``--env`` entry is malformed or ``--stdio-cmd`` is empty.
    """
    args = parse_args()

    _LOGGER.info(f"Connecting to MCP Systems server via {args.transport} transport")

    if args.transport == "streamable-http":
        _LOGGER.info(f"Server URL: {args.url}")
        async with AsyncExitStack() as stack:
            # Only pre-build a client when we need to inject headers (PSK).
            # When no PSK is configured, let the library create its default
            # client (with recommended MCP timeouts).
            http_client = None
            if args.psk:
                http_client = await stack.enter_async_context(
                    create_mcp_http_client(headers={_PSK_HEADER: args.psk})
                )
            read, write, _get_session_id = await stack.enter_async_context(
                streamable_http_client(args.url, http_client=http_client)
            )
            session = await stack.enter_async_context(ClientSession(read, write))
            failures = await _run_session(session, args.session_id, strict=args.strict)
    else:
        env_dict: dict[str, str] = {}
        for item in args.env:
            if "=" not in item:
                raise ValueError(f"Invalid --env entry: {item}. Must be KEY=VALUE.")
            k, v = item.split("=", 1)
            env_dict[k] = v

        stdio_tokens = shlex.split(args.stdio_cmd)
        if not stdio_tokens:
            raise ValueError("--stdio-cmd must not be empty")

        server_params = StdioServerParameters(
            command=stdio_tokens[0],
            args=stdio_tokens[1:],
            env=env_dict or None,
        )

        async with AsyncExitStack() as stack:
            read, write = await stack.enter_async_context(stdio_client(server_params))
            session = await stack.enter_async_context(ClientSession(read, write))
            failures = await _run_session(session, args.session_id, strict=args.strict)

    if failures:
        _LOGGER.error(f"Smoke test FAILED: {len(failures)} tool(s) errored: {failures}")
        print(f"\nFAILED: {len(failures)} tool(s) errored: {failures}", file=sys.stderr)
        return 1
    _LOGGER.info("Smoke test PASSED: every tool call returned without raising")
    print("\nPASSED: every tool call returned without raising")
    return 0


async def test_tools(
    session: ClientSession,
    demo_session_id: str,
    *,
    strict: bool,
    failures: list[str],
) -> None:
    """Exercise a representative cross-section of systems-server tools.

    The selected tools mirror the registry in
    ``src/deephaven_mcp/mcp_systems_server/_tools/`` and cover the
    discovery, session-introspection, table, script, and
    enterprise-status surfaces. Failures are logged and recorded in
    ``failures``; in strict mode the first failure raises
    :class:`_ToolFailure` to short-circuit the run.

    Args:
        session: An initialized :class:`mcp.ClientSession`.
        demo_session_id: Fully qualified community session id passed to
            tools that require an ``id`` argument. Expected
            grammar: ``community:community:<session_name>``.
        strict: When ``True``, raise :class:`_ToolFailure` on the first
            tool error so the caller can short-circuit.
        failures: Mutable list to which each failed tool name is appended.
    """
    print("\n--- Discovery (cross-cutting) ---")
    _LOGGER.info("Testing tool: list_systems")
    print("\nCalling tool: list_systems")
    result = await call_tool(
        session, "list_systems", {}, strict=strict, failures=failures
    )
    print(f"Result for list_systems: {result}")

    _LOGGER.info("Testing tool: sessions_list")
    print("\nCalling tool: sessions_list")
    result = await call_tool(
        session, "sessions_list", {}, strict=strict, failures=failures
    )
    print(f"Result for sessions_list: {result}")

    print("\n--- Community-side session tools ---")
    _LOGGER.info("Testing tool: session_details")
    print(f"\nCalling tool: session_details (id={demo_session_id})")
    result = await call_tool(
        session,
        "session_details",
        {"id": demo_session_id},
        strict=strict,
        failures=failures,
    )
    print(f"Result for session_details: {result}")

    # Later calls are language-sensitive: the demo script must use the
    # session's own syntax, and pip introspection is Python-only.
    programming_language = _extract_programming_language(result)
    _LOGGER.info(f"Demo session programming language: {programming_language}")

    _LOGGER.info("Testing tool: session_tables_list")
    print(f"\nCalling tool: session_tables_list (id={demo_session_id})")
    result = await call_tool(
        session,
        "session_tables_list",
        {"id": demo_session_id},
        strict=strict,
        failures=failures,
    )
    print(f"Result for session_tables_list: {result}")

    _LOGGER.info("Testing tool: session_script_run")
    print(f"\nCalling tool: session_script_run (id={demo_session_id})")
    demo_script = _DEMO_SCRIPTS.get(programming_language, _DEMO_SCRIPTS["python"])
    result = await call_tool(
        session,
        "session_script_run",
        {"id": demo_session_id, "script": demo_script},
        strict=strict,
        failures=failures,
    )
    print(f"Result for session_script_run: {result}")

    _LOGGER.info("Testing tool: session_table_schema")
    print(f"\nCalling tool: session_table_schema (id={demo_session_id})")
    result = await call_tool(
        session,
        "session_table_schema",
        {"id": demo_session_id, "table_name": "demo_table"},
        strict=strict,
        failures=failures,
    )
    print(f"Result for session_table_schema: {result}")

    if programming_language == "python":
        _LOGGER.info("Testing tool: session_pip_list")
        print(f"\nCalling tool: session_pip_list (id={demo_session_id})")
        result = await call_tool(
            session,
            "session_pip_list",
            {"id": demo_session_id},
            strict=strict,
            failures=failures,
        )
        print(f"Result for session_pip_list: {result}")
    else:
        _LOGGER.info(
            f"Skipping session_pip_list: Python-only tool, session is "
            f"{programming_language}"
        )
        print(
            f"\nSkipping tool: session_pip_list (Python-only; session is "
            f"{programming_language})"
        )

    print("\n--- Enterprise-side status ---")
    _LOGGER.info("Testing tool: enterprise_systems_status")
    print("\nCalling tool: enterprise_systems_status")
    result = await call_tool(
        session,
        "enterprise_systems_status",
        {},
        strict=strict,
        failures=failures,
    )
    print(f"Result for enterprise_systems_status: {result}")


if __name__ == "__main__":
    try:
        _LOGGER.info("Starting MCP systems-server smoke test")
        exit_code = asyncio.run(main())
        _LOGGER.info(f"Smoke test exiting with code {exit_code}")
        sys.exit(exit_code)
    except KeyboardInterrupt:
        _LOGGER.info("Interrupted by user")
        print("\nInterrupted by user.", file=sys.stderr)
        sys.exit(130)  # conventional code for SIGINT
    except Exception as e:
        _LOGGER.error(f"Fatal error in main: {e!r}", exc_info=True)
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(2)
