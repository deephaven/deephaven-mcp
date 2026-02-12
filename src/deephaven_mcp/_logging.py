"""
Logging and global exception handling utilities for Deephaven MCP servers.

This module provides functions to:
- Set up root logger configuration early in process startup (`setup_logging`).
- Ensure all unhandled synchronous and asynchronous exceptions are logged (`setup_global_exception_logging`).
- Set up logging for all catchable termination signals (`setup_signal_handler_logging`).
- Log process resource state for diagnostic purposes (`log_process_state`).

Call `setup_logging()` before any other imports in your main entrypoint to ensure all loggers are configured correctly.
Call `setup_global_exception_logging()` once at process startup to guarantee robust error visibility.
Call `setup_signal_handler_logging()` to register handlers for all catchable signals (SIGTERM, SIGINT, SIGHUP, etc.).
"""

import asyncio
import logging
import os
import signal
import sys
import types
from typing import Any

import psutil


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
        stream=sys.stderr,
        force=True,  # Ensure we override any existing logging configuration
    )


# Idempotency guard for global exception logging setup
_EXC_LOGGING_INSTALLED = False

# Idempotency guard for signal handler registration
_SIGNAL_HANDLERS_INSTALLED = False


def _signal_handler(signum: int, frame: types.FrameType | None) -> None:
    """Signal handler to log received signals that might cause server shutdown.

    This handler is registered for all catchable termination signals to provide
    diagnostic information when the server process is being terminated. It logs
    the signal number, name, and frame information to help with debugging shutdown-
    related issues, particularly in containerized environments where signals may
    be sent by orchestration systems.

    The handler is defensive and catches its own exceptions to prevent the signal
    handler from crashing during logging.

    Args:
        signum (int): The signal number that was received.
        frame (types.FrameType | None): The current stack frame when the signal was received.
    """
    try:
        signal_name = signal.Signals(signum).name
    except (ValueError, AttributeError):
        signal_name = f"UNKNOWN({signum})"

    try:
        logging.warning(f"[signal_handler] Received signal {signum} ({signal_name})")
        logging.warning(f"[signal_handler] Signal frame: {frame}")
        logging.warning("[signal_handler] Process will likely terminate soon")
        # Flush all handlers to ensure logs are written before potential termination
        for handler in logging.root.handlers:
            handler.flush()
    except Exception as e:
        # Fallback to stderr if logging fails
        try:
            sys.stderr.write(
                f"[signal_handler] CRITICAL: Received signal {signum} ({signal_name}) "
                f"but logging failed: {e}\n"
            )
            sys.stderr.flush()
        except Exception:  # noqa: S110
            pass  # Nothing more we can do - last resort fallback


def _register_signal(signal_name: str, is_critical: bool) -> tuple[bool, str | None]:
    """Register a signal handler for the given signal name.

    Args:
        signal_name: Name of the signal to register (e.g., 'SIGTERM')
        is_critical: Whether to generate an error message if the signal is not available on this platform

    Returns:
        Tuple of (success, error_message). If successful, error_message is None.
    """
    try:
        if hasattr(signal, signal_name):
            sig = getattr(signal, signal_name)
            signal.signal(sig, _signal_handler)
            return (True, None)
        if is_critical:
            return (False, f"{signal_name} (not available on this platform)")
        return (False, None)
    except (OSError, RuntimeError, ValueError) as e:
        # OSError: Signal not supported on this platform
        # RuntimeError: Signal already registered by another handler
        # ValueError: Invalid signal
        return (False, f"{signal_name} ({e})")


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
        exc_traceback: types.TracebackType | None,
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


def log_process_state(log_tag: str, context: str) -> None:
    """Log current process resource state for debugging.

    Records key metrics about the current process to help diagnose resource issues:
      - Memory usage in MB (via RSS - Resident Set Size)
      - CPU utilization percentage
      - Number of open file descriptors
      - Process ID (only during startup)

    This function is particularly useful for monitoring resource consumption during
    server lifecycle events, identifying memory leaks, resource exhaustion, or
    performance issues during stress testing and long-running operations.

    Args:
        log_tag (str): Tag to use in log message prefix (e.g., "app_lifespan" becomes "[app_lifespan]").
            This helps identify the source of the metric logs.
        context (str): Context string describing when metrics are being collected (e.g., "startup",
            "shutdown", "periodic_check"). Special handling is applied for "shutdown" context.

    Example usage:
        # At server startup
        log_process_state("mcp_docs_server:app_lifespan", "startup")

        # During server shutdown
        log_process_state("mcp_docs_server:app_lifespan", "shutdown")

        # During periodic health check
        log_process_state("health_monitor", "hourly_check")

    Note:
        Process ID is only logged during startup to avoid log spam during shutdown.
        All exceptions are caught and logged to prevent diagnostic failures from
        affecting server operation.
    """
    try:
        process = psutil.Process()
        prefix = "Final " if context == "shutdown" else ""
        logging.info(
            f"[{log_tag}] {prefix}memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB"
        )
        logging.info(f"[{log_tag}] {prefix}CPU percent: {process.cpu_percent()}%")
        logging.info(f"[{log_tag}] {prefix}open file descriptors: {process.num_fds()}")

        # Only log PID during startup
        if context != "shutdown":
            logging.info(f"[{log_tag}] Process PID: {process.pid}")
    except Exception as e:
        logging.error(f"[{log_tag}] Error getting {context} process state: {e}")


def setup_signal_handler_logging() -> None:
    r"""
    Set up logging for all catchable termination signals.

    This function registers handlers for all catchable signals that might terminate the process:

    **Unix/Linux/macOS signals:**
      - SIGTERM: Standard termination signal (e.g., from container orchestrators or service managers)
      - SIGINT: Keyboard interrupt signal (e.g., from Ctrl+C in terminal)
      - SIGHUP: Hangup signal (terminal disconnect)
      - SIGQUIT: Quit signal (e.g., from Ctrl+\\ in terminal)
      - SIGABRT: Abort signal (from abort() calls or assertion failures)
      - SIGUSR1: User-defined signal 1
      - SIGUSR2: User-defined signal 2
      - SIGALRM: Alarm clock signal
      - SIGPIPE: Broken pipe signal

    **Windows signals:**
      - SIGTERM: Termination signal (limited use on Windows)
      - SIGINT: Keyboard interrupt (Ctrl+C)
      - SIGBREAK: Break signal (Ctrl+Break on Windows)
      - SIGABRT: Abort signal

    **NOT catchable (handled by OS directly):**
      - SIGKILL: Immediate termination (cannot be caught)
      - SIGSTOP: Stop process (cannot be caught)

    The handlers log the signal number, name, and frame information before the process terminates.
    This is particularly useful for debugging unexpected shutdowns in containerized environments,
    Cloud Run instances, or any environment where signals may be sent by orchestration systems.

    The function is idempotent and can be safely called multiple times. Signal handler registration
    failures are logged but do not raise exceptions.

    Example usage:
        # In your main application entry point
        from deephaven_mcp._logging import setup_logging, setup_signal_handler_logging

        # Set up logging before any other imports
        setup_logging()

        # Set up signal handlers for improved shutdown diagnostics
        setup_signal_handler_logging()

    Returns:
        None
    """
    global _SIGNAL_HANDLERS_INSTALLED
    if _SIGNAL_HANDLERS_INSTALLED:
        return
    _SIGNAL_HANDLERS_INSTALLED = True

    # Register signal handlers for all catchable termination signals
    registered_signals = []
    failed_signals = []

    # List of signals to register (Unix/Linux/macOS + Windows)
    # Format: (signal_name, is_critical)
    # is_critical = True means registration failures produce an error message (logged at debug),
    #                 False means unavailable signals are ignored without logging.
    signals_to_register = [
        # Critical signals present on all platforms
        ("SIGTERM", True),  # Standard termination
        ("SIGINT", True),  # Keyboard interrupt
        ("SIGABRT", True),  # Abort signal
        # Unix/Linux/macOS signals
        ("SIGHUP", False),  # Hangup
        ("SIGQUIT", False),  # Quit
        ("SIGUSR1", False),  # User-defined 1
        ("SIGUSR2", False),  # User-defined 2
        ("SIGALRM", False),  # Alarm clock
        ("SIGPIPE", False),  # Broken pipe
        # Windows-specific
        ("SIGBREAK", False),  # Ctrl+Break on Windows
    ]

    for signal_name, is_critical in signals_to_register:
        success, error_msg = _register_signal(signal_name, is_critical)
        if success:
            registered_signals.append(signal_name)
        elif error_msg:
            failed_signals.append(error_msg)

    # Log registration results
    if registered_signals:
        logging.info(
            f"[signal_handler] Signal handlers registered for: {', '.join(registered_signals)}"
        )

    if failed_signals:
        logging.debug(
            f"[signal_handler] Failed to register handlers for: {', '.join(failed_signals)}"
        )
