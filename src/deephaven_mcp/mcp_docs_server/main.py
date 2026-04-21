"""CLI entrypoint for the Deephaven MCP Docs server.

This module sets up logging, signal handler logging, global exception handling, and Uvicorn exception patching before starting the Docs MCP server.
It provides a command-line interface to launch the server using streamable-http transport (HTTP-only, like the community and enterprise servers).

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

import logging  # noqa: E402

from ._mcp import mcp_docs_host, mcp_docs_port, mcp_server  # noqa: E402

_LOGGER = logging.getLogger(__name__)


def run_server() -> None:
    """Start the MCP docs server using streamable-http transport."""
    _LOGGER.info(
        f"[run_server] Starting MCP docs server '{mcp_server.name}' with transport=streamable-http (host={mcp_docs_host}, port={mcp_docs_port})"
    )
    try:
        mcp_server.run(transport="streamable-http")
    finally:
        _LOGGER.info(f"[run_server] MCP docs server '{mcp_server.name}' stopped.")


def main() -> None:
    """Command-line entry point for the Deephaven MCP Docs server."""
    run_server()


if __name__ == "__main__":  # pragma: no cover
    main()
