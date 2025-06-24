#!/usr/bin/env python3
"""
Async stress test client for the Deephaven MCP Docs server (agentic, MCP-compatible) using user queries and cancellation.

Features:
- Connects to the docs server via SSE (Server-Sent Events) only.
- Lists available tools from the server (should include 'docs_chat').
- Iterates over a set of predefined prompts and sends each to the docs_chat tool.
- Runs the set of user queries sequentially for a configurable number of runs.
- For each prompt, starts the docs_chat call and cancels it after a configurable sleep period.
- Optionally includes chat history from the command line as a JSON string.
- Collects and prints timing statistics (min, max, avg) for each prompt after completion.
- Handles connection errors, timeouts, cancellations, and other exceptions robustly using Python 3.11+ ExceptionGroup handling.

Requirements:
- autogen-ext[mcp]
- Python 3.11 or newer (for ExceptionGroup/except* support)

Usage:
    uv run scripts/mcp_docs_stress_sse_cancel_queries.py --url http://localhost:8000/sse --runs 10 --sleep 1.5
    uv run scripts/mcp_docs_stress_sse_cancel_queries.py --url dev --history '[{"role": "user", "content": "Hi"}]' --runs 5 --sleep 2.0

See --help for all options.
"""

import argparse
import asyncio
import json
import sys
import time
import traceback
from collections import defaultdict

import httpx
from autogen_core import CancellationToken
from autogen_ext.tools.mcp import SseServerParams, mcp_server_tools

prompts = [
    "What is Deephaven?",
    "What is the schema or column structure of the DbInternal AuditEventLog table? Can you provide details about all columns and their data types?",
    "Write a Legacy style query looking at the DbInternal AuditEventLog",
    "Ask the doc server if it has any access to that audit log schema",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Async MCP Docs test client (SSE only)"
    )
    parser.add_argument(
        "--url",
        default="prod",
        help=(
            "SSE server URL or shortcut: "
            "'local' for http://localhost:8000/sse, "
            "'prod' for http://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/sse, "
            "'dev' for http://deephaven-mcp-docs-dev.dhc-demo.deephaven.io/sse. "
            "You may also provide a full URL."
        ),
    )
    parser.add_argument(
        "--history",
        default=None,
        help="Optional chat history as JSON string (list of {role, content}).",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Number of times to run the set of user queries (default: 1).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=2.0,
        help="Seconds to sleep before cancelling each docs_chat call (default: 1.0).",
    )
    return parser.parse_args()


def resolve_url(url_option):
    if url_option == "local":
        return "http://localhost:8000/sse"
    elif url_option == "prod":
        return "https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/sse"
    elif url_option == "dev":
        return "https://deephaven-mcp-docs-dev.dhc-demo.deephaven.io/sse"
    return url_option


# Store timing statistics: {prompt: [list of durations]}
prompt_timings = defaultdict(list)
prompt_timings_lock = asyncio.Lock()


async def call_docs_chat(tool, prompt, history, sleep_time) -> str:
    start = time.perf_counter()
    cancellation_token = CancellationToken()
    # Start the docs_chat call as a task
    task = asyncio.create_task(
        tool.run_json(
            {"prompt": prompt, "history": history} if history else {"prompt": prompt},
            cancellation_token=cancellation_token,
        )
    )
    # Sleep before canceling the call
    await asyncio.sleep(sleep_time)
    cancellation_token.cancel()
    try:
        result = await task
    except Exception as e:
        result = f"Cancelled: {e}"
    duration = time.perf_counter() - start
    async with prompt_timings_lock:
        prompt_timings[prompt].append(duration)
    return result


def print_prompt_stats():
    print("\nPrompt timing statistics (seconds):")
    for prompt in prompts:
        print(f"Prompt: {prompt!r}")
        times = prompt_timings[prompt]
        if times:
            min_time = min(times)
            max_time = max(times)
            avg_time = sum(times) / len(times)
            print(
                f"  Min: {min_time:.2f}  Max: {max_time:.2f}  Avg: {avg_time:.2f}  (n={len(times)})"
            )
        else:
            print(f"  No data")


async def main():
    try:
        args = parse_args()
        url = resolve_url(args.url)
        print(f"Connecting to MCP Docs server at {url}")

        server_params = SseServerParams(url=url)

        tools = await mcp_server_tools(server_params)
        print("Available tools:", [t.name for t in tools])
        tool_map = {t.name: t for t in tools}

        if "docs_chat" not in tool_map:
            print("docs_chat tool not found on server!", file=sys.stderr)
            sys.exit(1)

        # Prepare arguments for docs_chat
        history = None
        if args.history:
            try:
                history = json.loads(args.history)
            except Exception as e:
                print(f"Failed to parse --history: {e}", file=sys.stderr)
                sys.exit(2)

        # Run the set of user queries sequentially for the specified number of runs
        for run in range(args.runs):
            for prompt in prompts:
                print(f"[Run {run+1} of {args.runs}] Sending prompt: {prompt!r}")
                try:
                    result = await call_docs_chat(
                        tool_map["docs_chat"], prompt, history, args.sleep
                    )
                    print(f"[Run {run+1} of {args.runs}] Result: {result!r}")
                except* asyncio.CancelledError as eg:
                    print("Async operation was cancelled.", file=sys.stderr)

        print("\n" * 3)
        print_prompt_stats()
    except* asyncio.TimeoutError as eg:
        print("Timed out waiting for main() to complete.", file=sys.stderr)
        sys.exit(5)
    except* KeyboardInterrupt as eg:
        print("Interrupted by user.", file=sys.stderr)
        sys.exit(130)
    except* httpx.ConnectError as eg:
        print(f"Connection error(s) for {url}:", file=sys.stderr)
        for e in eg.exceptions:
            print(" ", e, file=sys.stderr)
        sys.exit(7)
    except* Exception as eg:
        print(f"Fatal error in main: {eg}", file=sys.stderr)
        for e in eg.exceptions:
            traceback.print_exception(type(e), e, e.__traceback__)
        sys.exit(10)


if __name__ == "__main__":
    asyncio.run(
        asyncio.wait_for(main(), timeout=400)
    )  # 20 seconds for the whole script
