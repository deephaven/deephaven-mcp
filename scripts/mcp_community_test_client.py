"""
mcp_community_test_client.py

Async Python client for discovering and calling all tools on an MCP (Model Context Protocol) server using streamable-http, SSE, or stdio transport.

Features:
- Connects to a running MCP server via SSE endpoint or spawns a stdio server process.
- Lists all available tools on the server.
- Demonstrates how to call each tool registered on the server, using appropriate or sample arguments for each tool.
- Supports passing environment variables to stdio subprocesses.
- Requires `autogen-ext[mcp]` to be installed.

Usage examples:
    # Connect via streamable-http (default)
    $ python mcp_community_test_client.py --transport streamable-http --url http://localhost:8000/mcp

    # Connect via SSE
    $ python mcp_community_test_client.py --transport sse --url http://localhost:8000/sse

    # Connect via stdio
    $ python mcp_community_test_client.py --transport stdio --stdio-cmd "uv run dh-mcp-systems --transport stdio" --env DH_MCP_CONFIG_FILE=/path/to/file.json

Arguments:
    --transport   Transport type: 'streamable-http' (default), 'sse', or 'stdio'.
    --url         HTTP server URL (auto-detected: http://localhost:8000/mcp for streamable-http, http://localhost:8000/sse for SSE).
    --stdio-cmd   Command to launch stdio server (default: uv run dh-mcp-systems --transport stdio).
    --env         Environment variable for stdio, format KEY=VALUE. Can be specified multiple times.

See the project README for further details.
"""

# TODO: *** is this needed with the "mcp dev" command?

import argparse
import asyncio
import logging
import shlex
import sys

from autogen_core import CancellationToken
from autogen_ext.tools.mcp import SseServerParams, StdioServerParams, mcp_server_tools

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_LOGGER = logging.getLogger(__name__)


def parse_args():
    """
    Parse command-line arguments for the MCP test client.

    Returns:
        argparse.Namespace: Parsed arguments with fields:
            - transport: Transport type ('streamable-http', 'sse', or 'stdio')
            - url: HTTP server URL (auto-detected if not specified)
            - stdio_cmd: Command to launch stdio server
            - env: List of environment variable strings (KEY=VALUE)
            - token: Optional authorization token for HTTP transports
    """
    parser = argparse.ArgumentParser(
        description="MCP test client for SSE or stdio server"
    )
    parser.add_argument(
        "--transport",
        choices=["sse", "stdio", "streamable-http"],
        default="streamable-http",
        help="Transport type (sse, stdio, or streamable-http)",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="HTTP server URL (auto-detected based on transport if not specified)",
    )
    parser.add_argument(
        "--stdio-cmd",
        default="uv run dh-mcp-systems --transport stdio",
        help="Stdio server command (pass as a shell string, e.g. 'uv run dh-mcp-systems --transport stdio')",
    )
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        help="Environment variable for stdio transport, format KEY=VALUE. Can be specified multiple times.",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Optional authorization token for HTTP transports (Bearer token)",
    )
    return parser.parse_args()


async def main():
    """
    Connects to the MCP server (SSE or stdio), lists available tools, and demonstrates invocation of all tools.

    - Establishes a connection to the MCP server using the selected transport.
    - Lists all registered tools on the server.
    - Calls each tool with correct or sample arguments based on its definition in _mcp.py.
    - Prints the results or errors for each tool invocation.
    - Modify the tool arguments as needed for your server setup or data.
    """
    args = parse_args()

    # Auto-detect URL based on transport if not specified
    if args.url is None:
        if args.transport == "sse":
            args.url = "http://localhost:8000/sse"
        elif args.transport == "streamable-http":
            args.url = "http://localhost:8000/mcp"
        # stdio doesn't need a URL

    if args.transport in ["sse", "streamable-http"]:
        headers = {}
        if args.token:
            headers["Authorization"] = f"Bearer {args.token}"
        server_params = SseServerParams(
            url=args.url, headers=headers if headers else None
        )
    else:  # stdio
        # Parse env vars from --env KEY=VALUE
        env_dict = {}
        for item in args.env:
            if "=" in item:
                k, v = item.split("=", 1)
                env_dict[k] = v
            else:
                raise ValueError(f"Invalid --env entry: {item}. Must be KEY=VALUE.")
        # StdioServerParams expects 'command' as the executable, and 'args' as the list of arguments.
        stdio_tokens = shlex.split(args.stdio_cmd)
        if not stdio_tokens:
            raise ValueError("--stdio-cmd must not be empty")
        stdio_command = stdio_tokens[0]
        stdio_args = stdio_tokens[1:]
        server_params = StdioServerParams(
            command=stdio_command, args=stdio_args, env=env_dict if env_dict else None
        )

    _LOGGER.info(f"Connecting to MCP Systems server via {args.transport} transport")
    if args.transport in ["sse", "streamable-http"]:
        _LOGGER.info(f"Server URL: {args.url}")

    tools = await mcp_server_tools(server_params)

    # List all tools
    _LOGGER.info(f"Available tools: {[t.name for t in tools]}")
    print("Available tools:", [t.name for t in tools])

    # Build a map for tool lookup by name
    tool_map = {t.name: t for t in tools}

    # 1. refresh
    _LOGGER.info("Testing tool: refresh")
    print("\nCalling tool: refresh")
    if "refresh" in tool_map:
        try:
            result = await tool_map["refresh"].run_json(
                {}, cancellation_token=CancellationToken()
            )
            _LOGGER.info("refresh tool call completed successfully")
            print(f"Result for refresh: {result}")
        except Exception as e:
            _LOGGER.error(f"Error calling refresh: {e}")
            print(f"Error calling refresh: {e}")

    # 2. default_worker
    _LOGGER.info("Testing tool: default_worker")
    print("\nCalling tool: default_worker")
    if "default_worker" in tool_map:
        try:
            result = await tool_map["default_worker"].run_json(
                {}, cancellation_token=CancellationToken()
            )
            _LOGGER.info("default_worker tool call completed successfully")
            print(f"Result for default_worker: {result}")
        except Exception as e:
            _LOGGER.error(f"Error calling default_worker: {e}")
            print(f"Error calling default_worker: {e}")

    # 3. worker_names
    print("\nCalling tool: worker_names")
    if "worker_names" in tool_map:
        try:
            result = await tool_map["worker_names"].run_json(
                {}, cancellation_token=CancellationToken()
            )
            print(f"Result for worker_names: {result}")
        except Exception as e:
            print(f"Error calling worker_names: {e}")

    # 4. table_schemas (call with no args, then with sample args)
    print("\nCalling tool: table_schemas (1 arg)")
    if "table_schemas" in tool_map:
        try:
            result = await tool_map["table_schemas"].run_json(
                {"worker_name": "worker1"}, cancellation_token=CancellationToken()
            )
            print(f"Result for table_schemas (1 arg): {result}")
        except Exception as e:
            print(f"Error calling table_schemas (1 arg): {e}")
        # Try with sample args
        print("\nCalling tool: table_schemas (sample args)")
        try:
            result = await tool_map["table_schemas"].run_json(
                {"worker_name": "worker1", "table_names": ["t1"]},
                cancellation_token=CancellationToken(),
            )
            print(f"Result for table_schemas (sample args): {result}")
        except Exception as e:
            print(f"Error calling table_schemas (sample args): {e}")

    # 5. run_script (must provide script or script_path)
    print("\nCalling tool: run_script (with script)")
    if "run_script" in tool_map:
        try:
            result = await tool_map["run_script"].run_json(
                {"worker_name": "worker1", "script": "print('hello world')"},
                cancellation_token=CancellationToken(),
            )
            print(f"Result for run_script: {result}")
        except Exception as e:
            print(f"Error calling run_script: {e}")

    # 6. describe_workers
    print("\nCalling tool: describe_workers")
    if "describe_workers" in tool_map:
        try:
            result = await tool_map["describe_workers"].run_json(
                {},
                cancellation_token=CancellationToken(),
            )
            print(f"Result for describe_workers: {result}")
        except Exception as e:
            print(f"Error calling describe_workers: {e}")

    # 7. pip_packages (requires worker_name)
    print("\nCalling tool: pip_packages (sample args)")
    if "pip_packages" in tool_map:
        try:
            result = await tool_map["pip_packages"].run_json(
                {"worker_name": "worker1"},
                cancellation_token=CancellationToken(),
            )
            print(f"Result for pip_packages: {result}")
        except Exception as e:
            print(f"Error calling pip_packages: {e}")


if __name__ == "__main__":
    try:
        _LOGGER.info("Starting MCP Community test client")
        asyncio.run(main())
        _LOGGER.info("Test client completed successfully")
    except KeyboardInterrupt:
        _LOGGER.info("Interrupted by user")
        print("\nInterrupted by user.", file=sys.stderr)
    except Exception as e:
        _LOGGER.error(f"Fatal error in main: {e}")
        print(f"Fatal error in main: {e}", file=sys.stderr)
        raise
