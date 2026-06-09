"""Async-to-sync adapter for ``click`` command callbacks (Pattern B).

The ``dh-mcp`` CLI is built on ``click``, whose command callbacks are
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
from collections.abc import Callable, Coroutine
from typing import Any


def run_async[**P, R](f: Callable[P, Coroutine[Any, Any, R]]) -> Callable[P, R]:
    """Adapt an ``async def`` click callback to a synchronous callable.

    The returned wrapper drives the wrapped coroutine with
    :func:`asyncio.run`, which creates a fresh event loop per call.
    This is appropriate for the CLI's one-shot invocation model
    (each ``dh-mcp`` subcommand runs to completion in a single
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
        return asyncio.run(f(*args, **kwargs))

    return wrapper
