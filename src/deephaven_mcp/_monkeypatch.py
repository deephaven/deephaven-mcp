"""Monkeypatch utilities for Deephaven MCP servers.

This module provides comprehensive structured logging for unhandled ASGI exceptions
by monkey-patching Uvicorn's RequestResponseCycle. It implements Google Cloud
Platform (GCP) Cloud Logging optimized for production environments.

Key Features:
- Client disconnect detection and graceful handling
- Google Cloud Logging integration for native GCP log aggregation
- Structured exception metadata for filtering and alerting
- Defensive error handling to prevent logging failures from masking exceptions
- DEBUG-level logging for expected client disconnects vs ERROR-level for server errors

Usage:
    Call `monkeypatch_uvicorn_exception_handling()` once at process startup to
    ensure robust error visibility for ASGI server exceptions.

Logging Strategy:
    Google Cloud Logging: Native GCP integration with structured metadata,
    automatic log aggregation, and appropriate severity levels for different
    exception types (client disconnects vs server errors).
"""

import logging
import traceback
from typing import Any

import anyio
from google.cloud import logging as gcp_logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler
from uvicorn.protocols.http.httptools_impl import RequestResponseCycle


def _setup_gcp_logging() -> logging.Logger:
    """
    Configure Google Cloud Logging for ASGI exception handling.

    Creates a logger named 'gcp_asgi_errors' that sends structured log entries
    directly to Google Cloud Logging service using CloudLoggingHandler. This
    provides native GCP integration with automatic log aggregation, filtering,
    and alerting capabilities.

    Returns:
        logging.Logger: Configured logger with CloudLoggingHandler attached,
            with propagation disabled.

    Note:
        The type ignore comment is required due to untyped call in the
        Google Cloud Logging library. Only adds handler if none exists to
        prevent duplicate log entries.
    """
    client = gcp_logging.Client()  # type: ignore[no-untyped-call]  # Suppression required because google-cloud-logging Client lacks type hints
    handler = CloudLoggingHandler(client)
    gcp_logger = logging.getLogger("gcp_asgi_errors")

    # Only add handler if none exists to prevent duplicate log entries
    if not gcp_logger.handlers:
        gcp_logger.addHandler(handler)

    # Disable propagation to prevent duplicate log entries from parent loggers
    gcp_logger.propagate = False
    return gcp_logger


# Lazy initialization - loggers created only when needed
_gcp_logger: logging.Logger | None = None


def _get_gcp_logger() -> logging.Logger:
    """
    Get or create the GCP logger using lazy initialization.

    This prevents early initialization issues by only creating the GCP logger
    when it's actually needed, rather than at module import time.

    Returns:
        logging.Logger: The GCP logger instance.
    """
    global _gcp_logger
    if _gcp_logger is None:
        _gcp_logger = _setup_gcp_logging()
    return _gcp_logger


def _is_client_disconnect_error(exc: BaseException) -> bool:
    """Check if exception indicates client disconnect rather than server error.

    This function recursively examines exceptions to detect anyio.ClosedResourceError,
    which typically indicates that a client has disconnected during request processing.
    It handles direct exceptions, ExceptionGroups, and nested exceptions via __cause__
    and __context__ attributes.

    Args:
        exc: The exception to examine for client disconnect indicators.

    Returns:
        bool: True if the exception indicates a client disconnect, False otherwise.

    Note:
        Client disconnects are expected behavior and should be logged at DEBUG level
        rather than ERROR level to reduce noise in production logs.
    """
    # Direct ClosedResourceError
    if isinstance(exc, anyio.ClosedResourceError):
        return True

    # ExceptionGroup containing ClosedResourceError
    if hasattr(exc, "exceptions"):  # ExceptionGroup
        for sub_exc in exc.exceptions:
            if _is_client_disconnect_error(sub_exc):
                return True

    # Check nested __cause__ and __context__
    if exc.__cause__ and _is_client_disconnect_error(exc.__cause__):
        return True
    if exc.__context__ and _is_client_disconnect_error(exc.__context__):
        return True

    return False


def monkeypatch_uvicorn_exception_handling() -> None:
    """
    Monkey-patch Uvicorn's RequestResponseCycle for comprehensive ASGI exception logging.

    This function addresses limitations in Uvicorn's default exception handling by
    wrapping ASGI application execution with structured logging optimized for
    Google Cloud Platform (GCP) Cloud Run environments. It distinguishes between
    client disconnects and actual server errors for appropriate log severity.

    Exception Handling:
        - Client disconnects (anyio.ClosedResourceError): Logged at DEBUG level
        - Server errors: Logged at ERROR level with full structured metadata
        - Recursive detection of nested exceptions and ExceptionGroups
        - Defensive error handling to prevent logging failures

    Logging Strategy:
        Google Cloud Logging with structured metadata including:
        - Exception type, module, and message details
        - Complete stack trace for debugging
        - Event type classification (client_disconnect vs server_error)
        - Structured metadata for filtering and alerting

    This patch is essential for:
        - Production monitoring and alerting with reduced noise
        - Debugging silent failures in ASGI applications
        - Ensuring log visibility in containerized environments
        - Meeting observability requirements for cloud deployments

    Note:
        This function should be called exactly once at process startup to ensure
        the patch is applied globally without interference.
    """
    logging.getLogger(__name__).warning(
        "[_monkeypatch:monkeypatch_uvicorn_exception_handling] Monkey-patching Uvicorn's RequestResponseCycle to log unhandled ASGI exceptions."
    )
    orig_run_asgi = RequestResponseCycle.run_asgi

    async def my_run_asgi(self: RequestResponseCycle, app: Any) -> None:
        async def wrapped_app(*args: Any) -> Any:
            try:
                return await app(*args)
            except Exception as e:
                exc_type = type(e)
                exc_value = e
                exc_traceback = e.__traceback__

                # Extract exception details for structured logging
                full_traceback = "".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)
                )

                # Check if this is a client disconnect (not a server error)
                if _is_client_disconnect_error(exc_value):
                    # Log client disconnect at DEBUG level with full details for debugging
                    try:
                        _get_gcp_logger().debug(
                            f"Client disconnect detected in ASGI application: {exc_type.__name__}: {str(exc_value)}",
                            extra={
                                "event_type": "client_disconnect",
                                "exception_type": exc_type.__name__,
                                "exception_module": exc_type.__module__,
                                "exception_message": str(exc_value),
                                "stack_trace": full_traceback,
                            },
                            exc_info=(exc_type, exc_value, exc_traceback),
                        )
                    except Exception as disconnect_log_err:
                        # Defensive handling: Log disconnect logging failures to stderr
                        logging.getLogger(__name__).error(
                            f"[_monkeypatch:wrapped_app] Client disconnect logging failed: {disconnect_log_err}"
                        )

                    # Return gracefully without re-raising - let connection close naturally
                    return

                # Google Cloud Logging: The primary logging strategy for native GCP integration
                # Provides structured metadata and automatic log aggregation
                # (This is the best log message)
                try:
                    _get_gcp_logger().error(
                        f"Unhandled exception in ASGI application (GCP Cloud Logging): {exc_type.__name__}: {str(exc_value)}",
                        extra={
                            "exception_type": exc_type.__name__,
                            "exception_module": exc_type.__module__,
                            "exception_message": str(exc_value),
                            "stack_trace": full_traceback,
                        },
                        exc_info=(exc_type, exc_value, exc_traceback),
                    )
                except Exception as gcp_err:
                    # Defensive handling: Log GCP logging failures to stderr
                    logging.getLogger(__name__).error(
                        f"[_monkeypatch:wrapped_app] GCP Logging failed: {gcp_err}"
                    )

                # Re-raise the original exception to maintain normal ASGI error flow
                raise

        await orig_run_asgi(self, wrapped_app)

    # Apply the monkey patch to Uvicorn's RequestResponseCycle
    RequestResponseCycle.run_asgi = my_run_asgi  # type: ignore[method-assign]
