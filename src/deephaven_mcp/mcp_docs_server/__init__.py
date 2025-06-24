"""
deephaven_mcp.mcp_docs_server package.

This module serves as the entrypoint for the Deephaven MCP Docs server package. It provides access to the MCP server instance (`mcp_server`) and the `run_server` entrypoint for starting the server.

All MCP tool definitions are implemented in the internal module `_mcp.py`.

Exports:
    - mcp_server: The FastMCP server instance with all registered tools.
    - run_server: Function to start the MCP server with the specified transport.

Usage:
    from deephaven_mcp.mcp_docs_server import mcp_server, run_server
    run_server("stdio")

See the project README for configuration details, available tools, and usage examples.
"""

import asyncio
import logging
import os
import sys
import traceback
from types import TracebackType
from typing import Any, Literal

from uvicorn.protocols.http.httptools_impl import RequestResponseCycle

from ._mcp import mcp_docs_host, mcp_docs_port, mcp_server

__all__ = ["mcp_server", "run_server"]

_LOGGER = logging.getLogger(__name__)

# Idempotency guard for global exception logging setup
_EXC_LOGGING_INSTALLED = False


# TODO: remove monkey patching
def monkeypatch_uvicorn_exception_handling() -> None:  # pragma: no cover
    """
    Monkey-patch Uvicorn's RequestResponseCycle to ensure exceptions in ASGI applications are logged.
    This is necessary because some versions of Uvicorn do not log exceptions in ASGI applications properly in some cases.
    This patch wraps the ASGI app execution in a try-except block to catch and log exceptions,
    ensuring that unhandled exceptions in the ASGI application are logged properly.
    This is useful for debugging and monitoring, especially in production environments where silent failures
    can lead to difficult-to-diagnose issues.
    This function should be called once at process startup to ensure that the patch is applied globally.
    """
    _LOGGER.warning(
        "Monkey-patching Uvicorn's RequestResponseCycle to log unhandled ASGI exceptions."
    )
    orig_run_asgi = RequestResponseCycle.run_asgi

    async def my_run_asgi(self: RequestResponseCycle, app: Any) -> None:
        async def wrapped_app(*args: Any) -> Any:
            try:
                return await app(*args)
            except Exception as e:
                print(
                    f"Unhandled exception in ASGI application: {type(e)} {e}",
                    file=sys.stderr,
                )
                traceback.print_exc()
                traceback.print_exception(type(e), e, e.__traceback__)
                _LOGGER.error(
                    "Unhandled exception in ASGI application",
                    exc_info=(type(e), e, e.__traceback__),
                )
                raise

        await orig_run_asgi(self, wrapped_app)

    RequestResponseCycle.run_asgi = my_run_asgi  # type: ignore[method-assign]


# TODO: make this generic for all MCP servers
def setup_global_exception_logging() -> None:
    """
    Set up global logging for all unhandled exceptions (synchronous and asynchronous) in the process.

    This function ensures that:
        - All uncaught exceptions in synchronous code (main thread and others) are logged using the root logger.
        - All uncaught exceptions in asynchronous code (asyncio event loops) are logged, regardless of which event loop is used or where it is created.
        - The function monkey-patches `asyncio.new_event_loop` so that every new event loop created in the process will have the async exception handler set automatically.
        - The handler is also set on the current event loop, if one exists.

    Why use this:
        - Guarantees that no unhandled error (sync or async) will go unnoticed in logs, even if event loops are created by libraries or frameworks outside your control.
        - Useful for debugging, production monitoring, and ensuring robust error visibility in long-running server processes.

    Usage:
        Call this function once at process startup (e.g., at the top of your main() entrypoint) before any event loops are created or server code is run.
    """
    global _EXC_LOGGING_INSTALLED
    if _EXC_LOGGING_INSTALLED:
        return
    _EXC_LOGGING_INSTALLED = True

    def _log_unhandled_exception(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: TracebackType | None,
    ) -> None:
        if issubclass(exc_type, KeyboardInterrupt):
            return
        # Logger.error expects exc_info to be a tuple of (type, value, traceback) or True
        _LOGGER.error(
            "UNHANDLED EXCEPTION", exc_info=(exc_type, exc_value, exc_traceback)
        )

    # sys.excepthook expects a function with this signature
    sys.excepthook = _log_unhandled_exception

    def _asyncio_exception_handler(
        loop: asyncio.AbstractEventLoop, context: dict[str, Any]
    ) -> None:
        exception = context.get("exception")
        _LOGGER.error(
            f"UNHANDLED ASYNC EXCEPTION: {context.get('message')}",
            exc_info=(
                (type(exception), exception, exception.__traceback__)
                if exception
                else None
            ),
        )

    # Patch new_event_loop to always set the handler
    _orig_new_event_loop = asyncio.new_event_loop

    def _patched_new_event_loop(*args: Any, **kwargs: Any) -> asyncio.AbstractEventLoop:
        loop = _orig_new_event_loop(*args, **kwargs)
        loop.set_exception_handler(_asyncio_exception_handler)
        return loop

    asyncio.new_event_loop = _patched_new_event_loop

    # Also set on the current loop if possible
    try:
        asyncio.get_event_loop().set_exception_handler(_asyncio_exception_handler)
    except RuntimeError:
        # If no event loop is running, this is fine; it will be set when the loop is created
        pass


def run_server(
    transport: Literal["stdio", "sse", "streamable-http"],
) -> None:
    """
    Start the MCP server with the specified transport.

    Args:
        transport (str, optional): The transport type ('stdio', 'sse', or 'streamable-http')

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

    Arguments:
        -t, --transport: Transport type for the MCP server ('stdio', 'sse', or 'streamable-http'). Default: 'sse'.
    """
    import argparse

    setup_global_exception_logging()
    parser = argparse.ArgumentParser(description="Start the Deephaven MCP Docs server.")
    parser.add_argument(
        "-t",
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default="sse",
        help="Transport type for the MCP server (stdio, sse, or streamable-http). Default: stdio",
    )
    args = parser.parse_args()
    _LOGGER.info(f"CLI args: {args}")
    monkeypatch_uvicorn_exception_handling()
    run_server(args.transport)


if __name__ == "__main__":
    main()
