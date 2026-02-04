#!/usr/bin/env python3
"""
Async stress test client for the Deephaven MCP Docs server (agentic, MCP-compatible) using user queries.

Features:
- Connects to the docs server via SSE (Server-Sent Events) only.
- Lists available tools from the server (should include 'docs_chat').
- Iterates over a set of predefined prompts and sends each to the docs_chat tool.
- Supports concurrent execution with configurable number of threads (async tasks).
- Each thread can run the set of user queries multiple times (configurable).
- Optionally includes chat history from the command line as a JSON string.
- Collects and prints timing statistics (min, max, avg) for each prompt after completion.
- Handles connection errors, timeouts, cancellations, and other exceptions robustly using Python 3.11+ ExceptionGroup handling.

Requirements:
- mcp package (native client)
- Python 3.11 or newer (for ExceptionGroup/except* support)

Usage:
    uv run scripts/mcp_docs_stress_sse_user_queries.py --url http://localhost:8000/sse --threads 4 --runs 10
    uv run scripts/mcp_docs_stress_sse_user_queries.py --url dev --history '[{"role": "user", "content": "Hi"}]' --threads 2 --runs 5

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
from mcp.client.sse import sse_client

prompts = [
    "What is Deephaven?",
    "What is the schema or column structure of the DbInternal AuditEventLog table? Can you provide details about all columns and their data types?",
    "Write a Legacy style query looking at the DbInternal AuditEventLog",
    "Ask the doc server if it has any access to that audit log schema",
]


def parse_args():
    """
    Parse command-line arguments for the stress test client.
    
    Returns:
        argparse.Namespace: Parsed arguments with fields:
            - url: SSE server URL or shortcut (prod, dev, local)
            - history: Optional chat history as JSON string
            - threads: Number of concurrent threads (async tasks) to use
            - runs: Number of times each thread runs the set of user queries
    """
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
        "--threads",
        type=int,
        default=1,
        help="Number of concurrent threads to use for calling docs_chat (default: 1).",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Number of times to run the set of user queries on each thread (default: 1).",
    )
    return parser.parse_args()


def resolve_url(url_option):
    """
    Resolve URL shortcut to full SSE endpoint URL.
    
    Args:
        url_option: URL string or shortcut ('local', 'prod', 'dev')
    
    Returns:
        str: Full SSE endpoint URL
    """
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


async def call_docs_chat(session, prompt, history) -> str:
    """
    Call docs_chat tool and record timing statistics.
    
    Args:
        session: Active MCP session object
        prompt: Question/prompt to send to docs_chat
        history: Optional chat history (list of message dicts)
    
    Returns:
        str: Tool result text
    """
    start = time.perf_counter()
    call_args = {"prompt": prompt}
    if history:
        call_args["history"] = history
    result = await session.call_tool("docs_chat", arguments=call_args)
    duration = time.perf_counter() - start
    async with prompt_timings_lock:
        prompt_timings[prompt].append(duration)
    return result.content[0].text if result.content else str(result)


async def thread_worker(session, history, runs, thread_id):
    """
    Worker task that executes prompts concurrently.
    
    Each worker runs through all prompts for the specified number of runs,
    calling docs_chat and displaying results.
    
    Args:
        session: Active MCP session object
        history: Optional chat history (list of message dicts)
        runs: Number of times to iterate through all prompts
        thread_id: Identifier for this worker thread (for display purposes)
    """
    for run in range(runs):
        for prompt in prompts:
            print(
                f"[Thread {thread_id} Run {run+1} of {runs}] Sending prompt: {prompt!r}"
            )
            result = await call_docs_chat(session, prompt, history)
            print(f"[Thread {thread_id} Run {run+1} of {runs}] Result: {result!r}")


def print_prompt_stats():
    """
    Print timing statistics (min, max, avg) for each prompt.
    
    Displays statistics collected during the stress test run, including
    the number of samples for each prompt.
    """
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
    """
    Main entry point for the concurrent stress test.
    
    Connects to the MCP Docs server via SSE, verifies docs_chat tool availability,
    launches concurrent worker tasks, and prints timing statistics.
    Handles various exception types using Python 3.11+ ExceptionGroup syntax.
    """
    try:
        args = parse_args()
        url = resolve_url(args.url)
        print(f"Connecting to MCP Docs server at {url}")

        # Prepare arguments for docs_chat
        history = None
        if args.history:
            try:
                history = json.loads(args.history)
            except Exception as e:
                print(f"Failed to parse --history: {e}", file=sys.stderr)
                sys.exit(2)

        async with sse_client(url) as (read, write):
            async with read, write:
                await write.send_initialize()
                result = await read.recv_initialize()
                print(f"Connected to MCP server: {result}")
                
                session = await write.get_result(read)
                
                # List tools
                tools_result = await session.list_tools()
                tools = tools_result.tools
                tool_names = [t.name for t in tools]
                print("Available tools:", tool_names)
                
                if "docs_chat" not in tool_names:
                    print("docs_chat tool not found on server!", file=sys.stderr)
                    sys.exit(1)

                # Launch threads (tasks) concurrently
                tasks = [
                    thread_worker(session, history, args.runs, i + 1)
                    for i in range(args.threads)
                ]
                await asyncio.gather(*tasks)
                print("\n" * 3)
                print_prompt_stats()
    except* asyncio.TimeoutError as eg:
        print("Timed out waiting for main() to complete.", file=sys.stderr)
        sys.exit(5)
    except* asyncio.CancelledError as eg:
        print("Async operation was cancelled.", file=sys.stderr)
        sys.exit(6)
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
    )  # 400 seconds timeout for the whole script
