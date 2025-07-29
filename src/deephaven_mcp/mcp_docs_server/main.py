"""
CLI entrypoint for the Deephaven MCP Docs server.

This module sets up logging, global exception handling, and Uvicorn exception patching before starting the Docs MCP server.
It provides a command-line interface to launch the server with a specified transport (stdio, sse, or streamable-http).

See the project README for configuration details, available tools, and usage examples.
"""

from .._logging import (  # noqa: E402
    setup_global_exception_logging,
    setup_logging,
    setup_signal_handler_logging,
)

# Ensure logging is set up before any other imports
setup_logging()
# Ensure global exception logging is set up before any server code runs
setup_global_exception_logging()

from .._monkeypatch import monkeypatch_uvicorn_exception_handling  # noqa: E402

# Ensure Uvicorn's exception handling is patched before any server code runs
monkeypatch_uvicorn_exception_handling()

# Register signal handlers for improved debugging of termination signals
setup_signal_handler_logging()

import argparse  # noqa: E402
import logging  # noqa: E402
from typing import Literal  # noqa: E402

from ._mcp import mcp_docs_host, mcp_docs_port, mcp_server  # noqa: E402

_LOGGER = logging.getLogger(__name__)


def run_server(
    transport: Literal["stdio", "sse", "streamable-http"],
) -> None:
    """
    Start the MCP server with the specified transport.

    Args:
        transport: The transport type ('stdio', 'sse', or 'streamable-http').
            Must be one of the supported transport methods.

    Returns:
        None
    """
    try:
        # Start the server
        _LOGGER.warning(
            f"Starting MCP server '{mcp_server.name}' with transport={transport} (host={mcp_docs_host}, port={mcp_docs_port})"
        )
        mcp_server.run(transport=transport)
    finally:
        _LOGGER.info(f"MCP server '{mcp_server.name}' stopped.")


def main() -> None:
    """
    Command-line entry point for the Deephaven MCP Docs server.

    Parses CLI arguments using argparse and starts the MCP server with the specified transport.

    Args:
        -t, --transport: Transport type for the MCP server ('stdio', 'sse', or 'streamable-http').
            Default: 'streamable-http'.

    Returns:
        None
    """
    parser = argparse.ArgumentParser(description="Start the Deephaven MCP Docs server.")
    parser.add_argument(
        "-t",
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default="streamable-http",
        help="Transport type for the MCP server (stdio, sse, or streamable-http). Default: streamable-http",
    )
    args = parser.parse_args()
    _LOGGER.info(f"CLI args: {vars(args)}")
    run_server(args.transport)


if __name__ == "__main__":
    main()
