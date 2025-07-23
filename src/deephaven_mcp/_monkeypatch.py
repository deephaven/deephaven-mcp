"""
Monkeypatch utilities for Deephaven MCP servers.

This module provides a function to monkey-patch Uvicorn's RequestResponseCycle so that unhandled exceptions in ASGI applications are properly logged.

Call `monkeypatch_uvicorn_exception_handling()` once at process startup to ensure robust error visibility for ASGI server exceptions.
"""

import json
import logging
import sys
import traceback
from datetime import datetime, timezone
from typing import Any

# import structlog
from google.cloud import logging as gcp_logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler
from pythonjsonlogger import json as jsonlogger
from uvicorn.protocols.http.httptools_impl import RequestResponseCycle

_LOGGER = logging.getLogger(__name__)

# # Configure structlog for JSON output at module level
# structlog.configure(
#     processors=[
#         structlog.stdlib.filter_by_level,
#         structlog.stdlib.add_logger_name,
#         structlog.stdlib.add_log_level,
#         structlog.stdlib.PositionalArgumentsFormatter(),
#         structlog.processors.TimeStamper(fmt="iso"),
#         structlog.processors.StackInfoRenderer(),
#         structlog.processors.format_exc_info,
#         structlog.processors.JSONRenderer(),
#     ],
#     wrapper_class=structlog.stdlib.BoundLogger,
#     logger_factory=structlog.stdlib.LoggerFactory(),
#     cache_logger_on_first_use=True,
# )

# struct_logger = structlog.get_logger()


def monkeypatch_uvicorn_exception_handling() -> None:
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
                exc_type = type(e)
                exc_value = e
                exc_traceback = e.__traceback__

                # Format the full stack trace as a string
                full_traceback = "".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)
                )

                # # Original logging methods (0-3)
                # print(
                #     f"Unhandled exception in ASGI application (0): {type(e)} {e}",
                #     file=sys.stderr,
                # )
                # traceback.print_exc()
                # traceback.print_exception(type(e), e, e.__traceback__)
                # _LOGGER.exception(
                #     f"Unhandled exception in ASGI application (1): {type(e)} {e}",
                #     exc_info=(type(e), e, e.__traceback__),
                # )
                # _LOGGER.exception(
                #     f"Unhandled exception in ASGI application (2): {type(e)} {e}"
                # )
                # _LOGGER.error(
                #     "Unhandled exception in ASGI application (3)",
                #     exc_info=(type(e), e, e.__traceback__),
                # )

                # # New comprehensive logging (4-5)

                # # Log comprehensive error information at ERROR level
                # error_msg = (
                #     f"UNHANDLED ASGI EXCEPTION (4):\n"
                #     f"Exception Type: {exc_type.__name__}\n"
                #     f"Exception Module: {exc_type.__module__}\n"
                #     f"Exception Message: {str(exc_value)}\n"
                #     f"Exception Args: {getattr(exc_value, 'args', 'N/A')}\n"
                #     f"Full Stack Trace:\n{full_traceback}"
                # )

                # _LOGGER.error(error_msg)

                # # Log to stderr for immediate visibility in Cloud Run
                # print(
                #     f"\n{'='*80}\n{error_msg}\n{'='*80}\n", file=sys.stderr, flush=True
                # )

                # # Log using the logger at ERROR level with exc_info
                # _LOGGER.error(
                #     "Unhandled exception in ASGI application (5) - comprehensive logging",
                #     exc_info=(exc_type, exc_value, exc_traceback),
                #     extra={
                #         "exception_type": exc_type.__name__,
                #         "exception_module": exc_type.__module__,
                #         "exception_message": str(exc_value),
                #         "exception_args": getattr(exc_value, "args", None),
                #         "full_traceback": full_traceback,
                #     },
                # )

                # # Option 1: JSON structured logging for GCP Cloud Run (6)
                # log_data = {
                #     "severity": "ERROR",
                #     "message": "Unhandled exception in ASGI application (6) - JSON structured",
                #     "timestamp": datetime.now(timezone.utc).isoformat(),
                #     "exception": {
                #         "type": exc_type.__name__,
                #         "module": exc_type.__module__,
                #         "message": str(exc_value),
                #         "args": getattr(exc_value, "args", None),
                #     },
                #     "stack_trace": full_traceback,  # Preserves original formatting
                # }
                # _LOGGER.error(json.dumps(log_data))

                # # Option 2: Structured logging with extra parameters (7)
                # _LOGGER.error(
                #     "Unhandled exception in ASGI application (7) - structured extra",
                #     extra={
                #         "severity": "ERROR",
                #         "exception_type": exc_type.__name__,
                #         "exception_module": exc_type.__module__,
                #         "exception_message": str(exc_value),
                #         "exception_args": getattr(exc_value, "args", None),
                #         "stack_trace": full_traceback,
                #         "timestamp": datetime.now(timezone.utc).isoformat(),
                #     },
                # )

                # # Option 3: GCP-specific structured logging format (8)
                # gcp_log_entry = {
                #     "severity": "ERROR",
                #     "message": f"Unhandled exception in ASGI application (8) - GCP format: {exc_type.__name__}: {str(exc_value)}",
                #     "labels": {
                #         "exception_type": exc_type.__name__,
                #         "exception_module": exc_type.__module__,
                #     },
                #     "jsonPayload": {
                #         "exception_args": getattr(exc_value, "args", None),
                #         "stack_trace": full_traceback,
                #     },
                #     "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
                # }
                # _LOGGER.error(json.dumps(gcp_log_entry))

                # Option 6: Direct stderr with GCP JSON format (9)
                stderr_log = {
                    "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
                    "severity": "ERROR",
                    "message": f"Unhandled exception in ASGI application (9) - stderr GCP: {exc_type.__name__}: {str(exc_value)}",
                    "exception": {
                        "type": exc_type.__name__,
                        "module": exc_type.__module__,
                        "args": str(getattr(exc_value, "args", None)),
                        # "traceback": full_traceback.replace("\n", "\\n"),
                        "traceback": full_traceback,
                    },
                }
                print(json.dumps(stderr_log), file=sys.stderr, flush=True)

                # Option 7: Google Cloud Logging (10)
                try:
                    client = gcp_logging.Client()  # type: ignore[no-untyped-call]
                    handler = CloudLoggingHandler(client)
                    gcp_logger = logging.getLogger("gcp_asgi_errors")
                    if not gcp_logger.handlers:
                        gcp_logger.addHandler(handler)
                        gcp_logger.setLevel(logging.ERROR)
                    gcp_logger.error(
                        f"Unhandled exception in ASGI application (10) - GCP Cloud Logging: {exc_type.__name__}: {str(exc_value)}",
                        extra={
                            "exception_type": exc_type.__name__,
                            "exception_module": exc_type.__module__,
                            "exception_message": str(exc_value),
                            "stack_trace": full_traceback,
                        },
                        exc_info=(exc_type, exc_value, exc_traceback),
                    )
                except Exception as gcp_err:
                    print(f"GCP Logging failed: {gcp_err}", file=sys.stderr)

                # # Option 8: Structlog (11)
                # try:
                #     struct_logger.error(
                #         f"Unhandled exception in ASGI application (11) - Structlog: {exc_type.__name__}: {str(exc_value)}",
                #         exception_type=exc_type.__name__,
                #         exception_module=exc_type.__module__,
                #         exception_message=str(exc_value),
                #         exception_args=getattr(exc_value, "args", None),
                #         stack_trace=full_traceback,
                #     )
                # except Exception as struct_err:
                #     print(f"Structlog failed: {struct_err}", file=sys.stderr)

                # Option 9: Python JSON Logger (12)
                try:
                    json_logger = logging.getLogger("json_asgi_errors")
                    if not json_logger.handlers:
                        json_handler = logging.StreamHandler(sys.stderr)
                        # Configure JsonFormatter with timestamp and proper field formatting
                        json_formatter = jsonlogger.JsonFormatter(
                            fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
                            datefmt="%Y-%m-%dT%H:%M:%S.%fZ",
                        )
                        json_handler.setFormatter(json_formatter)
                        json_logger.addHandler(json_handler)
                        json_logger.setLevel(logging.ERROR)
                        # Ensure propagation is enabled for GCP
                        json_logger.propagate = True

                    # Log with both message and structured extra data
                    json_logger.error(
                        f"Unhandled exception in ASGI application (12) - Python JSON Logger: {exc_type.__name__}: {str(exc_value)}",
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
                    print(f"Python JSON Logger failed: {json_err}", file=sys.stderr)

                raise

        await orig_run_asgi(self, wrapped_app)

    RequestResponseCycle.run_asgi = my_run_asgi  # type: ignore[method-assign]
