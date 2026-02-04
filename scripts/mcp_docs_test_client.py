#!/usr/bin/env python3
"""
Async test client for the Deephaven MCP Docs server (agentic, MCP-compatible).

- Connects to the docs server via streamable-http (default), SSE, or stdio.
- Lists available tools (should be 'docs_chat').
- Demonstrates calling docs_chat with a sample prompt.
- Prints the result for verification.

Requires: mcp package (native client)

Usage examples:
    python scripts/mcp_docs_test_client.py --transport streamable-http --url http://localhost:8001/mcp --prompt "What is Deephaven?"
    python scripts/mcp_docs_test_client.py --transport sse --url http://localhost:8001/sse --prompt "What is Deephaven?"
    python scripts/mcp_docs_test_client.py --transport stdio --stdio-cmd "uv run dh-mcp-docs-server --transport stdio"

See --help for all options.
"""

import argparse
import asyncio
import json
import logging
import shlex
import sys

import httpx
from mcp import StdioServerParameters
from mcp.client.sse import sse_client
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_LOGGER = logging.getLogger(__name__)


def parse_args():
    """
    Parse command-line arguments for the MCP Docs test client.

    Returns:
        argparse.Namespace: Parsed arguments with fields:
            - transport: Transport type ('streamable-http', 'sse', or 'stdio')
            - url: HTTP server URL (auto-detected if not specified)
            - stdio_cmd: Command to launch stdio server
            - env: List of environment variable strings (KEY=VALUE)
            - prompt: Question/prompt to send to docs_chat tool
            - history: Optional chat history as JSON string
            - token: Optional authorization token for HTTP transports
    """
    parser = argparse.ArgumentParser(
        description="Async MCP Docs test client (streamable-http, SSE, or stdio)"
    )
    parser.add_argument(
        "--transport",
        choices=["streamable-http", "sse", "stdio"],
        default="streamable-http",
        help="Transport type (streamable-http, sse, or stdio)",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="HTTP server URL (auto-detected based on transport if not specified)",
    )
    parser.add_argument(
        "--stdio-cmd",
        default="uv run dh-mcp-docs-server --transport stdio",
        help="Stdio server command (default: uv run dh-mcp-docs-server --transport stdio)",
    )
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        help="Environment variable for stdio transport, format KEY=VALUE. Can be specified multiple times.",
    )
    parser.add_argument(
        "--prompt",
        default="How do I use Deephaven tables?",
        help="Prompt/question to send to docs_chat tool.",
    )
    parser.add_argument(
        "--history",
        default=None,
        help="Optional chat history as JSON string (list of {role, content}).",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Optional authorization token for HTTP transports (Bearer token)",
    )
    return parser.parse_args()


async def main():
    """
    Main async function that connects to MCP Docs server and tests docs_chat tool.

    Establishes connection using the specified transport (streamable-http, SSE, or stdio),
    discovers available tools, and demonstrates calling the docs_chat tool with a prompt.
    Handles URL auto-detection, environment variable parsing, and error reporting.

    Raises:
        ValueError: If invalid environment variables or stdio command provided.
        SystemExit: On various error conditions (tool not found, parsing errors, etc.).
    """
    args = parse_args()

    # Auto-detect URL based on transport if not specified
    if args.url is None:
        if args.transport == "sse":
            args.url = "http://localhost:8001/sse"
        elif args.transport == "streamable-http":
            args.url = "http://localhost:8001/mcp"

    # Prepare arguments for docs_chat
    history = None
    if args.history:
        try:
            history = json.loads(args.history)
            _LOGGER.info(f"Parsed chat history with {len(history)} messages")
        except Exception as e:
            _LOGGER.error(f"Failed to parse --history: {e}")
            print(f"Failed to parse --history: {e}", file=sys.stderr)
            sys.exit(2)

    _LOGGER.info(f"Connecting to MCP Docs server via {args.transport} transport")

    if args.transport in ["sse", "streamable-http"]:
        # Prepare HTTP client with optional auth token
        headers = {}
        if args.token:
            headers["Authorization"] = f"Bearer {args.token}"
        http_client = httpx.AsyncClient(headers=headers) if headers else None

        if args.transport == "streamable-http":
            client_func = lambda url: streamable_http_client(
                url, http_client=http_client
            )
        else:
            client_func = lambda url: sse_client(url, http_client=http_client)

        _LOGGER.info(f"Server URL: {args.url}")
        async with client_func(args.url) as (read, write):
            async with read, write:
                await write.send_initialize()
                result = await read.recv_initialize()
                _LOGGER.info(f"Connected to MCP server: {result}")

                session = await write.get_result(read)

                # List tools
                tools_result = await session.list_tools()
                tools = tools_result.tools
                tool_names = [t.name for t in tools]
                _LOGGER.info(f"Available tools: {tool_names}")

                if "docs_chat" not in tool_names:
                    _LOGGER.error("docs_chat tool not found on server!")
                    print("docs_chat tool not found on server!", file=sys.stderr)
                    sys.exit(1)

                # Call docs_chat
                _LOGGER.info(f"Calling docs_chat with prompt: {args.prompt!r}")
                if history:
                    _LOGGER.info(f"Using chat history with {len(history)} messages")

                try:
                    call_args = {"prompt": args.prompt}
                    if history:
                        call_args["history"] = history
                    result = await session.call_tool("docs_chat", arguments=call_args)
                    _LOGGER.info("docs_chat call completed successfully")
                    print("\ndocs_chat result:")
                    print(result.content[0].text if result.content else str(result))
                except Exception as e:
                    _LOGGER.error(f"Error calling docs_chat: {e}")
                    print(f"Error calling docs_chat: {e}", file=sys.stderr)
                    sys.exit(3)
    else:  # stdio
        # Parse env vars from --env KEY=VALUE
        env_dict = {}
        for item in args.env:
            if "=" in item:
                k, v = item.split("=", 1)
                env_dict[k] = v
            else:
                raise ValueError(f"Invalid --env entry: {item}. Must be KEY=VALUE.")

        stdio_tokens = shlex.split(args.stdio_cmd)
        if not stdio_tokens:
            raise ValueError("--stdio-cmd must not be empty")

        server_params = StdioServerParameters(
            command=stdio_tokens[0],
            args=stdio_tokens[1:],
            env=env_dict if env_dict else None,
        )

        async with stdio_client(server_params) as (read, write):
            async with read, write:
                await write.send_initialize()
                result = await read.recv_initialize()
                _LOGGER.info(f"Connected to MCP server: {result}")

                session = await write.get_result(read)

                # List tools
                tools_result = await session.list_tools()
                tools = tools_result.tools
                tool_names = [t.name for t in tools]
                _LOGGER.info(f"Available tools: {tool_names}")

                if "docs_chat" not in tool_names:
                    _LOGGER.error("docs_chat tool not found on server!")
                    print("docs_chat tool not found on server!", file=sys.stderr)
                    sys.exit(1)

                # Call docs_chat
                _LOGGER.info(f"Calling docs_chat with prompt: {args.prompt!r}")
                if history:
                    _LOGGER.info(f"Using chat history with {len(history)} messages")

                try:
                    call_args = {"prompt": args.prompt}
                    if history:
                        call_args["history"] = history
                    result = await session.call_tool("docs_chat", arguments=call_args)
                    _LOGGER.info("docs_chat call completed successfully")
                    print("\ndocs_chat result:")
                    print(result.content[0].text if result.content else str(result))
                except Exception as e:
                    _LOGGER.error(f"Error calling docs_chat: {e}")
                    print(f"Error calling docs_chat: {e}", file=sys.stderr)
                    sys.exit(3)


if __name__ == "__main__":
    try:
        asyncio.run(
            asyncio.wait_for(main(), timeout=40)
        )  # 40 seconds for the whole script
    except asyncio.TimeoutError:
        _LOGGER.error("Timed out waiting for main() to complete")
        print("Timed out waiting for main() to complete.", file=sys.stderr)
        sys.exit(5)
    except asyncio.CancelledError:
        _LOGGER.error("Async operation was cancelled")
        print("Async operation was cancelled.", file=sys.stderr)
        sys.exit(6)
    except KeyboardInterrupt:
        _LOGGER.info("Interrupted by user")
        print("Interrupted by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        _LOGGER.error(f"Fatal error in main: {e}")
        print(f"Fatal error in main: {e}", file=sys.stderr)
        sys.exit(10)
