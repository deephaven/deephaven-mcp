"""
Monkeypatch utilities for Deephaven MCP servers.

This module provides comprehensive structured logging for unhandled ASGI exceptions
by monkey-patching Uvicorn's RequestResponseCycle. It implements multiple logging
strategies optimized for Google Cloud Platform (GCP) Cloud Run environments.

Key Features:
- Multiple structured logging approaches for GCP Cloud Run compatibility
- Direct stderr JSON logging for reliable error capture
- Google Cloud Logging integration for native GCP log aggregation
- Python JSON Logger for standardized JSON formatting
- Defensive error handling to prevent logging failures from masking exceptions

Usage:
    Call `monkeypatch_uvicorn_exception_handling()` once at process startup to
    ensure robust error visibility for ASGI server exceptions.

Logging Strategies:
    1. Direct stderr JSON: Bypasses Python logging for maximum reliability
    2. Google Cloud Logging: Native GCP integration with structured metadata
    3. Python JSON Logger: Standardized JSON formatting with GCP fields
"""

import json
import logging
import sys
import traceback
from datetime import datetime, timezone
from typing import Any

from google.cloud import logging as gcp_logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler
from pythonjsonlogger import json as jsonlogger
from uvicorn.protocols.http.httptools_impl import RequestResponseCycle

_LOGGER = logging.getLogger(__name__)


def _setup_gcp_logging() -> logging.Logger:
    """
    Configure Google Cloud Logging for ASGI exception handling.

    Creates a logger named 'gcp_asgi_errors' that sends structured log entries
    directly to Google Cloud Logging service using CloudLoggingHandler. This
    provides native GCP integration with automatic log aggregation, filtering,
    and alerting capabilities.

    Returns:
        logging.Logger: Configured logger with CloudLoggingHandler attached,
            set to ERROR level with propagation disabled.

    Note:
        The type ignore comment is required due to missing type stubs in the
        Google Cloud Logging library. Only adds handler if none exists to
        prevent duplicate log entries.
    """
    client = gcp_logging.Client()  # type: ignore[no-untyped-call]
    handler = CloudLoggingHandler(client)
    gcp_logger = logging.getLogger("gcp_asgi_errors")

    # Only add handler if none exists to prevent duplicate log entries
    if not gcp_logger.handlers:
        gcp_logger.addHandler(handler)
        gcp_logger.setLevel(logging.ERROR)

    # Disable propagation to prevent duplicate log entries from parent loggers
    gcp_logger.propagate = False
    return gcp_logger


def _setup_json_logging() -> logging.Logger:
    """
    Configure Python JSON Logger for structured ASGI exception logging.

    Creates a logger named 'json_asgi_errors' that outputs structured JSON log
    entries to stderr using pythonjsonlogger.JsonFormatter. The output is
    formatted for compatibility with GCP Cloud Run log parsing.

    Returns:
        logging.Logger: Configured logger with StreamHandler(sys.stderr) attached,
            using JsonFormatter with ISO 8601 timestamps, set to ERROR level
            with propagation disabled to prevent duplicate log entries.

    Note:
        Uses ISO 8601 timestamp format (%Y-%m-%dT%H:%M:%S.%fZ) with microsecond
        precision for optimal GCP Cloud Run log correlation and filtering.
        Only adds handler if none exists to prevent duplicate log entries.
    """
    json_logger = logging.getLogger("json_asgi_errors")

    # Only add handler if none exists to prevent duplicate log entries
    if not json_logger.handlers:
        json_handler = logging.StreamHandler(sys.stderr)

        # Configure JsonFormatter with GCP-compatible timestamp and field formatting
        json_formatter = jsonlogger.JsonFormatter(
            fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S.%fZ",  # ISO 8601 with microseconds
        )
        json_handler.setFormatter(json_formatter)
        json_logger.addHandler(json_handler)
        json_logger.setLevel(logging.ERROR)

        # Disable propagation to prevent duplicate log entries from parent loggers
        json_logger.propagate = False
    return json_logger


# Lazy initialization - loggers created only when needed
_gcp_logger: logging.Logger | None = None
_json_logger: logging.Logger | None = None


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


def _get_json_logger() -> logging.Logger:
    """
    Get or create the JSON logger using lazy initialization.

    This prevents early initialization issues by only creating the JSON logger
    when it's actually needed, rather than at module import time.

    Returns:
        logging.Logger: The JSON logger instance.
    """
    global _json_logger
    if _json_logger is None:
        _json_logger = _setup_json_logging()
    return _json_logger


def monkeypatch_uvicorn_exception_handling() -> None:
    """
    Monkey-patch Uvicorn's RequestResponseCycle for comprehensive ASGI exception logging.

    This function addresses limitations in Uvicorn's default exception handling by
    wrapping ASGI application execution with multiple structured logging strategies.
    It ensures that unhandled exceptions are captured and logged using formats
    optimized for Google Cloud Platform (GCP) Cloud Run environments.

    Logging Strategies Implemented:
        1. Direct stderr JSON logging: Bypasses Python logging for maximum reliability
        2. Google Cloud Logging: Native GCP integration with structured metadata
        3. Python JSON Logger: Standardized JSON formatting with GCP-compatible fields

    Each logging strategy includes:
        - Exception type, module, and message details
        - Complete stack trace for debugging
        - Structured metadata for filtering and alerting
        - Defensive error handling to prevent logging failures

    This patch is essential for:
        - Production monitoring and alerting
        - Debugging silent failures in ASGI applications
        - Ensuring log visibility in containerized environments
        - Meeting observability requirements for cloud deployments

    Note:
        This function should be called exactly once at process startup to ensure
        the patch is applied globally without interference.
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
                exc_type = type(e)
                exc_value = e
                exc_traceback = e.__traceback__

                # Extract exception details for structured logging
                full_traceback = "".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)
                )

                # Logging is performed multiple times because these errors are rare, and different logging strategies have been more or less reliable in recording important details.

                # Strategy #1: Direct stderr JSON logging for maximum reliability
                # Bypasses Python logging infrastructure to ensure log delivery
                stderr_log = {
                    "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
                    "severity": "ERROR",
                    "message": f"Unhandled exception in ASGI application (Direct stderr JSON): {exc_type.__name__}: {str(exc_value)}",
                    "exception": {
                        "type": exc_type.__name__,
                        "module": exc_type.__module__,
                        "args": str(getattr(exc_value, "args", None)),
                        "traceback": full_traceback,
                    },
                }
                print(json.dumps(stderr_log), file=sys.stderr, flush=True)

                # Strategy #2: Google Cloud Logging for native GCP integration
                # Provides structured metadata and automatic log aggregation
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
                    print(f"GCP Logging failed: {gcp_err}", file=sys.stderr)

                # Strategy #3: Python JSON Logger for standardized JSON formatting
                # Provides consistent JSON structure with GCP-compatible fields
                try:
                    # Log with human-readable message and structured metadata
                    _get_json_logger().error(
                        f"Unhandled exception in ASGI application (Python JSON Logger): {exc_type.__name__}: {str(exc_value)}",
                        extra={
                            "severity": "ERROR",
                            "exception_type": exc_type.__name__,
                            "exception_module": exc_type.__module__,
                            "exception_message": str(exc_value),
                            "exception_args": getattr(exc_value, "args", None),
                            "stack_trace": full_traceback,
                            "gcp_timestamp": datetime.now(timezone.utc).isoformat()
                            + "Z",
                        },
                        exc_info=(exc_type, exc_value, exc_traceback),
                    )
                except Exception as json_err:
                    # Defensive handling: Log JSON logging failures to stderr
                    print(f"Python JSON Logger failed: {json_err}", file=sys.stderr)

                # Re-raise the original exception to maintain normal ASGI error flow
                raise

        await orig_run_asgi(self, wrapped_app)

    # Apply the monkey patch to Uvicorn's RequestResponseCycle
    RequestResponseCycle.run_asgi = my_run_asgi  # type: ignore[method-assign]
