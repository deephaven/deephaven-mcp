"""
deephaven_mcp.community package

This module serves as the entrypoint for the Deephaven MCP Community server package. It provides access to the MCP server instance (`mcp_server`) and the `run_server` entrypoint for starting the server.

All MCP tool definitions are implemented in the internal module `_mcp.py`.

Exports:
    - mcp_server: The FastMCP server instance with all registered tools.
    - run_server: Function to start the MCP server with the specified transport.

Usage:
    from deephaven_mcp.community import mcp_server, run_server
    run_server("stdio")

See the project README for configuration details, available tools, and usage examples.
"""

import asyncio  # noqa: F401
import logging
import os
import sys
from typing import Literal

from ._mcp import mcp_server

__all__ = ["mcp_server", "run_server"]

_LOGGER = logging.getLogger(__name__)


def run_server(transport: Literal["stdio", "sse", "streamable-http"] = "stdio") -> None:
    """
    Start the MCP server with the specified transport.

    Args:
        transport (str, optional): The transport type ('stdio' or 'sse' or 'streamable-http'). Defaults to 'stdio'.
    """
    # Set stream based on transport
    # stdio MCP servers log to stderr so that they don't pollute the communication channel
    stream = sys.stderr if transport == "stdio" else sys.stdout

    # Configure logging with the PYTHONLOGLEVEL environment variable
    logging.basicConfig(
        level=os.getenv("PYTHONLOGLEVEL", "INFO"),
        format="[%(asctime)s] %(levelname)s: %(message)s",
        stream=stream,
        force=True,  # Ensure we override any existing logging configuration
    )

    try:
        # Start the server
        _LOGGER.info(
            f"Starting MCP server '{mcp_server.name}' with transport={transport}"
        )
        mcp_server.run(transport=transport)
    finally:
        _LOGGER.info(f"MCP server '{mcp_server.name}' stopped.")


def main() -> None:
    """
    Command-line entry point for the Deephaven MCP Community server.

    Parses CLI arguments using argparse and starts the MCP server with the specified transport.

    Arguments:
        -t, --transport: Transport type for the MCP server ('stdio', 'sse', or 'streamable-http'). Default: 'stdio'.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Start the Deephaven MCP Community server."
    )
    parser.add_argument(
        "-t",
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default="stdio",
        help="Transport type for the MCP server (stdio, sse, or streamable-http). Default: stdio",
    )
    args = parser.parse_args()
    _LOGGER.info(f"CLI args: {args}")
    run_server(args.transport)


if __name__ == "__main__":
    main()
