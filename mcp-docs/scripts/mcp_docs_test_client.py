#!/usr/bin/env python3
"""
Async test client for the Deephaven MCP Docs server (agentic, MCP-compatible).

- Connects to the docs server via SSE (default) or stdio.
- Lists available tools (should be 'docs_chat').
- Demonstrates calling docs_chat with a sample prompt.
- Prints the result for verification.

Requires: autogen-ext[mcp]

Usage examples:
    python scripts/mcp_docs_test_client.py --transport sse --url http://localhost:8000/sse --prompt "What is Deephaven?"
    python scripts/mcp_docs_test_client.py --transport stdio --stdio-cmd "uv run dh-mcp-docs --transport stdio"

See --help for all options.
"""
import argparse
import asyncio
import shlex
import json
import sys

from autogen_core import CancellationToken
from autogen_ext.tools.mcp import SseServerParams, StdioServerParams, mcp_server_tools

def parse_args():
    parser = argparse.ArgumentParser(description="Async MCP Docs test client (SSE or stdio)")
    parser.add_argument(
        "--transport",
        choices=["sse", "stdio"],
        default="sse",
        help="Transport type (sse or stdio)",
    )
    parser.add_argument(
        "--url", default="http://localhost:8000/sse", help="SSE server URL (default: http://localhost:8000/sse)"
    )
    parser.add_argument(
        "--stdio-cmd",
        default="uv run dh-mcp-docs --transport stdio",
        help="Stdio server command (default: uv run dh-mcp-docs --transport stdio)",
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
    return parser.parse_args()

async def main():
    args = parse_args()

    if args.transport == "sse":
        server_params = SseServerParams(url=args.url)
    else:
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
        stdio_command = stdio_tokens[0]
        stdio_args = stdio_tokens[1:]
        server_params = StdioServerParams(command=stdio_command, args=stdio_args, env=env_dict if env_dict else None)

    tools = await mcp_server_tools(server_params)
    print("Available tools:", [t.name for t in tools])
    tool_map = {t.name: t for t in tools}

    if "docs_chat" not in tool_map:
        print("docs_chat tool not found on server!", file=sys.stderr)
        sys.exit(1)

    # Prepare arguments for docs_chat
    prompt = args.prompt
    history = None
    if args.history:
        try:
            history = json.loads(args.history)
        except Exception as e:
            print(f"Failed to parse --history: {e}", file=sys.stderr)
            sys.exit(2)

    print(f"\nCalling docs_chat with prompt: {prompt!r}")
    if history:
        print(f"With history: {history}")
    try:
        result = await tool_map["docs_chat"].run_json(
            {"prompt": prompt, "history": history} if history else {"prompt": prompt},
            cancellation_token=CancellationToken(),
        )
        print("docs_chat result:")
        print(result)
    except Exception as e:
        print(f"Error calling docs_chat: {e}", file=sys.stderr)
        sys.exit(3)

if __name__ == "__main__":
    asyncio.run(main())
