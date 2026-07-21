"""Async-to-sync adapter for ``click`` command callbacks (Pattern B).

The ``dhcli`` CLI is built on ``click``, whose command callbacks are
synchronous. The :func:`run_async` decorator wraps an ``async def``
callback in a synchronous shim that runs the coroutine via
:func:`asyncio.run`, preserving ``__doc__`` / ``__name__`` /
``__wrapped__`` via :func:`functools.wraps` so click's ``--help``
output and :func:`inspect.signature` introspection see the original
async function unchanged.

Usage::

    @cli.command("status")
    @click.pass_obj
    @run_async
    async def status(runtime: Runtime) -> None:
        '''Report daemon status.'''
        ...

The decorator order is fixed: ``@click.pass_obj`` (or
``@click.pass_context``) must come **above** ``@run_async`` so that
click hands the bound objects through the wrapper to the underlying
coroutine.
"""

from __future__ import annotations

__all__ = ["run_async"]

import asyncio
import functools
import logging
from collections.abc import Callable, Coroutine
from typing import Any

_LOGGER = logging.getLogger(__name__)


def _cli_loop_exception_handler(
    _loop: asyncio.AbstractEventLoop, context: dict[str, Any]
) -> None:
    """Log loop-level asyncio exceptions at DEBUG instead of ERROR.

    Exceptions raised inside the awaited command coroutine propagate out of
    :func:`asyncio.run` and are rendered by the CLI's top-level handler. This
    handler only fires for exceptions the loop surfaces on its own — orphaned
    background tasks and teardown callbacks (e.g. anyio cancellation races
    during mcp client shutdown) — which asyncio would otherwise dump as a raw
    ``ERROR`` traceback to stderr. They are logged at ``DEBUG`` so they stay
    out of normal output but remain visible under ``-vv``.
    """
    message = context.get("message") or "Unhandled exception in event loop"
    _LOGGER.debug(
        f"[_async:_cli_loop_exception_handler] {message}",
        exc_info=context.get("exception"),
    )


def run_async[**P, R](f: Callable[P, Coroutine[Any, Any, R]]) -> Callable[P, R]:
    """Adapt an ``async def`` click callback to a synchronous callable.

    The returned wrapper drives the wrapped coroutine with
    :func:`asyncio.run`, which creates a fresh event loop per call.
    This is appropriate for the CLI's one-shot invocation model
    (each ``dhcli`` subcommand runs to completion in a single
    process) but means the decorator cannot be used inside an
    already-running event loop.

    Args:
        f (Callable[P, Coroutine[Any, Any, R]]): The asynchronous
            click command callback.

    Returns:
        Callable[P, R]: A synchronous wrapper that runs ``f`` via
            :func:`asyncio.run` and forwards the result.

    Raises:
        RuntimeError: When the returned wrapper is invoked while an
            event loop is already running in the current thread.
            Propagated verbatim from :func:`asyncio.run`.
    """

    @functools.wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        async def _run() -> R:
            asyncio.get_running_loop().set_exception_handler(
                _cli_loop_exception_handler
            )
            return await f(*args, **kwargs)

        return asyncio.run(_run())

    return wrapper
