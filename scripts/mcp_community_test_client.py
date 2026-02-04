"""
mcp_community_test_client.py

Async Python client for discovering and calling all tools on an MCP (Model Context Protocol) server using streamable-http, SSE, or stdio transport.

Features:
- Connects to a running MCP server via streamable-http, SSE, or stdio transport.
- Lists all available tools on the server.
- Demonstrates how to call each tool registered on the server, using appropriate or sample arguments for each tool.
- Supports passing environment variables to stdio subprocesses.
- Uses native MCP client (no external dependencies beyond mcp package).

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
    --token       Optional authorization token for HTTP transports (Bearer token).

See the project README for further details.
"""

import argparse
import asyncio
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
        description="MCP test client for streamable-http, SSE, or stdio server"
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


async def call_tool(session, tool_name, arguments):
    """
    Call an MCP tool and handle errors gracefully.

    Args:
        session: Active MCP session object
        tool_name: Name of the tool to call
        arguments: Dictionary of arguments to pass to the tool

    Returns:
        str: Tool result as text, or error message if the call fails
    """
    try:
        result = await session.call_tool(tool_name, arguments=arguments)
        return result.content[0].text if result.content else str(result)
    except Exception as e:
        _LOGGER.error(f"Error calling tool {tool_name}: {e}")
        return f"Error: {e}"


async def main():
    """
    Connect to MCP server and demonstrate tool invocation.

    - Establishes a connection to the MCP server using the selected transport (streamable-http, SSE, or stdio).
    - Lists all registered tools on the server.
    - Calls test_tools() to demonstrate tool invocation with sample arguments.
    - Prints the results or errors for each tool invocation.

    Raises:
        ValueError: If --env entries are malformed or --stdio-cmd is empty
    """
    args = parse_args()

    # Auto-detect URL based on transport if not specified
    if args.url is None:
        if args.transport == "sse":
            args.url = "http://localhost:8000/sse"
        elif args.transport == "streamable-http":
            args.url = "http://localhost:8000/mcp"

    _LOGGER.info(f"Connecting to MCP Systems server via {args.transport} transport")

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
                print("Available tools:", tool_names)

                # Test tools
                await test_tools(session)
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
                print("Available tools:", tool_names)

                # Test tools
                await test_tools(session)


async def test_tools(session):
    """
    Demonstrate calling various MCP tools with example arguments.

    This function shows how to call each tool type. Modify the session_id
    and other arguments as needed for your actual server setup.

    Args:
        session: Active MCP session object
    """

    # 1. refresh
    _LOGGER.info("Testing tool: refresh")
    print("\nCalling tool: refresh")
    result = await call_tool(session, "refresh", {})
    print(f"Result for refresh: {result}")

    # 2. list_sessions
    _LOGGER.info("Testing tool: list_sessions")
    print("\nCalling tool: list_sessions")
    result = await call_tool(session, "list_sessions", {})
    print(f"Result for list_sessions: {result}")

    # 3. get_session_details (example - requires session_id)
    _LOGGER.info("Testing tool: get_session_details")
    print("\nCalling tool: get_session_details (example)")
    result = await call_tool(
        session, "get_session_details", {"session_id": "community:local:example"}
    )
    print(f"Result for get_session_details: {result}")

    # 4. table_schemas (example - requires session_id)
    _LOGGER.info("Testing tool: table_schemas")
    print("\nCalling tool: table_schemas (example)")
    result = await call_tool(
        session, "table_schemas", {"session_id": "community:local:example"}
    )
    print(f"Result for table_schemas: {result}")

    # 5. run_script (example - requires session_id and script)
    _LOGGER.info("Testing tool: run_script")
    print("\nCalling tool: run_script (example)")
    result = await call_tool(
        session,
        "run_script",
        {"session_id": "community:local:example", "script": "print('hello world')"},
    )
    print(f"Result for run_script: {result}")

    # 6. enterprise_systems_status
    _LOGGER.info("Testing tool: enterprise_systems_status")
    print("\nCalling tool: enterprise_systems_status")
    result = await call_tool(session, "enterprise_systems_status", {})
    print(f"Result for enterprise_systems_status: {result}")

    # 7. pip_packages (example - requires session_id)
    _LOGGER.info("Testing tool: pip_packages")
    print("\nCalling tool: pip_packages (example)")
    result = await call_tool(
        session, "pip_packages", {"session_id": "community:local:example"}
    )
    print(f"Result for pip_packages: {result}")


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
