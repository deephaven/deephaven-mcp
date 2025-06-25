"""
Monkeypatch utilities for Deephaven MCP servers.

This module provides a function to monkey-patch Uvicorn's RequestResponseCycle so that unhandled exceptions in ASGI applications are properly logged.

Call `monkeypatch_uvicorn_exception_handling()` once at process startup to ensure robust error visibility for ASGI server exceptions.
"""

import logging
import sys
import traceback
from typing import Any

from uvicorn.protocols.http.httptools_impl import RequestResponseCycle

_LOGGER = logging.getLogger(__name__)


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
                print(
                    f"Unhandled exception in ASGI application (0): {type(e)} {e}",
                    file=sys.stderr,
                )
                traceback.print_exc()
                traceback.print_exception(type(e), e, e.__traceback__)
                _LOGGER.exception(
                    f"Unhandled exception in ASGI application (1): {type(e)} {e}",
                    exc_info=(type(e), e, e.__traceback__),
                )
                _LOGGER.exception(
                    f"Unhandled exception in ASGI application (2): {type(e)} {e}"
                )
                _LOGGER.error(
                    "Unhandled exception in ASGI application (3)",
                    exc_info=(type(e), e, e.__traceback__),
                )
                raise

        await orig_run_asgi(self, wrapped_app)

    RequestResponseCycle.run_asgi = my_run_asgi  # type: ignore[method-assign]
