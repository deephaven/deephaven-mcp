"""
CLI entrypoint for the Deephaven MCP Systems server.

This module sets up logging before starting the Systems MCP server.
It provides a command-line interface to launch the server with a specified transport (stdio, sse, or streamable-http).

See the project README for configuration details, available tools, and usage examples.
"""

from .._logging import setup_global_exception_logging, setup_logging, setup_signal_handler_logging  # noqa: E402

# Ensure logging is set up before any other imports
setup_logging()
# Ensure global exception logging is set up before any server code runs
setup_global_exception_logging()

# Register signal handlers for improved debugging of termination signals
setup_signal_handler_logging()

import logging  # noqa: E402
from typing import Literal  # noqa: E402

from ._mcp import mcp_server  # noqa: E402

_LOGGER = logging.getLogger(__name__)


def run_server(transport: Literal["stdio", "sse", "streamable-http"] = "stdio") -> None:
    """
    Start the MCP server with the specified transport.

    Args:
        transport (str, optional): The transport type ('stdio' or 'sse' or 'streamable-http'). Defaults to 'stdio'.
    """
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
    Command-line entry point for the Deephaven MCP Systems server.

    Parses CLI arguments using argparse and starts the MCP server with the specified transport.

    Arguments:
        -t, --transport: Transport type for the MCP server ('stdio', 'sse', or 'streamable-http'). Default: 'stdio'.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Start the Deephaven MCP Systems server."
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


if __name__ == "__main__":  # pragma: no cover
    main()
