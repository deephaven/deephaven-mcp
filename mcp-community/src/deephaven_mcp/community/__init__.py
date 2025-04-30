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
from deephaven_mcp import config
from ._mcp import mcp_server

__all__ = ["mcp_server", "run_server"]


def run_server(transport: str = "stdio") -> None:
    """
    Start the MCP server with the specified transport.

    Args:
        transport (str, optional): The transport type ('stdio' or 'sse'). Defaults to 'stdio'.
    """
    # TODO: can the log_level just be set via env?
    # Set log level based on transport
    log_level = logging.ERROR if transport == "stdio" else logging.DEBUG
    logging.basicConfig(
        level=log_level, format="[%(asctime)s] %(levelname)s: %(message)s"
    )

    logging.info(f"Starting MCP server '{mcp_server.name}' with transport={transport}")

    async def run():
        # Make sure config can be loaded before starting
        await config.get_config()

        try:
            await mcp_server.run(transport=transport)
        finally:
            logging.info(f"MCP server '{mcp_server.name}' stopped.")

    asyncio.run(run())
