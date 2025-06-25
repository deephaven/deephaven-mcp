"""
Logging and global exception handling utilities for Deephaven MCP servers.

This module provides functions to:
- Set up root logger configuration early in process startup (`setup_logging`).
- Ensure all unhandled synchronous and asynchronous exceptions are logged (`setup_global_exception_logging`).

Call `setup_logging()` before any other imports in your main entrypoint to ensure all loggers are configured correctly.
Call `setup_global_exception_logging()` once at process startup to guarantee robust error visibility.
"""

import asyncio
import logging
import os
import sys
from types import TracebackType
from typing import Any


def setup_logging() -> None:
    """
    Set up logging configuration for the application.

    This function configures the root logger using the PYTHONLOGLEVEL environment variable to set the log level.
    It should be called before any other imports in your main entrypoint to ensure that all loggers are set up correctly
    and that no other modules configure logging before this setup takes effect.
    """
    logging.basicConfig(
        level=os.getenv("PYTHONLOGLEVEL", "INFO"),
        format="[%(asctime)s] %(levelname)s: %(message)s",
        # stream=stream,
        stream=sys.stderr,
        force=True,  # Ensure we override any existing logging configuration
    )


# Idempotency guard for global exception logging setup
_EXC_LOGGING_INSTALLED = False


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
        logging.error(
            "UNHANDLED EXCEPTION", exc_info=(exc_type, exc_value, exc_traceback)
        )

    # sys.excepthook expects a function with this signature
    sys.excepthook = _log_unhandled_exception

    def _asyncio_exception_handler(
        loop: asyncio.AbstractEventLoop, context: dict[str, Any]
    ) -> None:
        exception = context.get("exception")
        logging.error(
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
