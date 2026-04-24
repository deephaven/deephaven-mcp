#!/usr/bin/env python3
"""
Async test client for the Deephaven MCP Docs server (agentic, MCP-compatible).

- Connects to the docs server via streamable-http.
- Lists available tools (should be 'docs_chat').
- Demonstrates calling docs_chat with a sample prompt.
- Prints the result for verification.

Requires: mcp package (native client)

Usage examples:
    python scripts/mcp_docs_test_client.py --url http://localhost:8001/mcp --prompt "What is Deephaven?"
    python scripts/mcp_docs_test_client.py --url https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/mcp --prompt "What is Deephaven?"

See --help for all options.
"""

import argparse
import asyncio
import json
import logging
import sys

import httpx
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
            - url: streamable-http server URL (default: http://localhost:8001/mcp)
            - prompt: Question/prompt to send to docs_chat tool
            - history: Optional chat history as JSON string
            - token: Optional authorization token (Bearer token)
    """
    parser = argparse.ArgumentParser(
        description="Async MCP Docs test client (streamable-http)"
    )
    parser.add_argument(
        "--url",
        default="http://localhost:8001/mcp",
        help="streamable-http server URL (default: http://localhost:8001/mcp)",
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
        help="Optional authorization token (Bearer token)",
    )
    return parser.parse_args()


async def main():
    """
    Main async function that connects to MCP Docs server and tests docs_chat tool.

    Connects via streamable-http, discovers available tools, and demonstrates
    calling the docs_chat tool with a prompt.

    Raises:
        SystemExit: On various error conditions (tool not found, parsing errors, etc.).
    """
    args = parse_args()

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

    _LOGGER.info(f"Connecting to MCP Docs server via streamable-http")
    _LOGGER.info(f"Server URL: {args.url}")

    headers = {}
    if args.token:
        headers["Authorization"] = f"Bearer {args.token}"

    http_client = httpx.AsyncClient(headers=headers) if headers else None

    try:
        async with streamable_http_client(args.url, http_client=http_client) as (
            read,
            write,
        ):
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
    finally:
        if http_client:
            await http_client.aclose()


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
