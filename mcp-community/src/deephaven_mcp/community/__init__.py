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

import logging
import asyncio
import sys
import os
from deephaven_mcp import config
from ._mcp import mcp_server

__all__ = ["mcp_server", "run_server"]

_CONFIG_MANAGER = config.DEFAULT_CONFIG_MANAGER
"""
_CONFIG_MANAGER is a private, module-level reference to the default configuration manager.

This allows for easier patching and testability in unit tests or other scenarios where the
configuration manager needs to be swapped or mocked. Production code should not modify this value.
"""


def run_server(transport: str = "stdio") -> None:
    """
    Start the MCP server with the specified transport.

    Args:
        transport (str, optional): The transport type ('stdio' or 'sse'). Defaults to 'stdio'.
    """
    # Set stream based on transport
    # stdio MCP servers log to stderr so that they don't pollute the communication channel
    stream = sys.stderr if transport == "stdio" else sys.stdout
    
    # Configure logging with the PYTHONLOGLEVEL environment variable
    logging.basicConfig(
        level=os.getenv('PYTHONLOGLEVEL', 'INFO'),
        format="[%(asctime)s] %(levelname)s: %(message)s",
        stream=stream,
        force=True  # Ensure we override any existing logging configuration
    )

    logging.info(f"Starting MCP server '{mcp_server.name}' with transport={transport}")

    async def run():
        # Make sure config can be loaded before starting
        await _CONFIG_MANAGER.get_config()

        try:
            await mcp_server.run(transport=transport)
        finally:
            logging.info(f"MCP server '{mcp_server.name}' stopped.")

    asyncio.run(run())
