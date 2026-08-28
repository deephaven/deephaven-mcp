"""Deadline-bounded use of a blocking resource on a worker thread.

Synchronous vendor clients often expose reads that cannot be interrupted;
closing the underlying resource from another thread is the only way to end
one. ``asyncio.wait_for`` cancels the awaiting coroutine but does not stop the
worker thread, so a resource opened inside that thread can outlive the caller
that asked for it. This module settles that ownership question in one place so
call sites do not each reinvent it.
"""

from __future__ import annotations

__all__ = ["BlockingResource"]

import asyncio
import logging
import threading
from collections.abc import Callable

_LOGGER = logging.getLogger(__name__)


def _resolve(finished: asyncio.Future[None]) -> None:
    """Complete ``finished`` unless the loop already canceled it.

    Args:
        finished (asyncio.Future[None]): The future awaiting a cleanup thread.
    """
    if not finished.done():
        finished.set_result(None)


class _AbandonedError(Exception):
    """Raised in the worker when the caller gave up before the open finished.

    Never reaches a caller: the only code that refuses a claim is the
    ``finally`` in :meth:`BlockingResource.run`, which runs after ``wait_for``
    has already raised, and the abandoned worker's exception is discarded.
    """


class BlockingResource[R]:
    """Opens, uses, and closes a blocking resource on one worker thread.

    The resource is closed exactly once, including when the deadline expires
    while it is still being opened. Ownership is settled under a lock and the
    side that arrives second performs the close, because the worker can finish
    opening after the caller has stopped waiting.

    Single use: each :meth:`run` call needs a fresh instance.
    """

    def __init__(self, open_: Callable[[], R], close: Callable[[R], None]) -> None:
        """Initialize the resource with its open and close operations.

        Args:
            open_ (Callable[[], R]): Opens the resource. Runs on the worker
                thread and may block.
            close (Callable[[R], None]): Closes the resource. Runs on a worker
                thread and may block; failures are logged, not raised.
        """
        self._open = open_
        self._close = close
        self._lock = threading.Lock()
        self._resource: R | None = None
        self._released = False

    async def run[T](self, use: Callable[[R], T], *, timeout_seconds: float) -> T:
        """Open the resource, pass it to ``use``, and close it.

        Args:
            use (Callable[[R], T]): Consumes the open resource. Runs on the
                same worker thread as the open and may block.
            timeout_seconds (float): Budget covering the open and the use
                together.

        Returns:
            T: Whatever ``use`` returned.

        Raises:
            TimeoutError: If the open and use together exceed
                ``timeout_seconds``.
            Exception: Anything ``open_`` or ``use`` raises propagates
                unchanged.
        """
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._open_and_use, use),
                timeout=timeout_seconds,
            )
        finally:
            await self._release_off_pool(timeout_seconds)

    async def _release_off_pool(self, timeout_seconds: float) -> None:
        """Close the resource on a thread of its own, then return.

        Never the default executor: once every worker there is parked in a
        blocking ``use``, a close queued behind them could never run to
        release them, and the deadline would stop being enforceable. The
        thread is a daemon so a close that never returns cannot hold up
        interpreter exit.

        Args:
            timeout_seconds (float): How long to wait for the close before
                detaching it and returning, so a stalled close cannot outlast
                the deadline the caller asked for.
        """
        loop = asyncio.get_running_loop()
        finished: asyncio.Future[None] = loop.create_future()

        def _cleanup() -> None:
            try:
                self._release()
            finally:
                loop.call_soon_threadsafe(_resolve, finished)

        threading.Thread(
            target=_cleanup, name="dh-mcp-blocking-cleanup", daemon=True
        ).start()
        try:
            await asyncio.wait_for(finished, timeout=timeout_seconds)
        except TimeoutError:
            _LOGGER.warning(
                f"[BlockingResource:_release_off_pool] Close did not finish within "
                f"{timeout_seconds}s; leaving it to the cleanup thread"
            )

    def _open_and_use[T](self, use: Callable[[R], T]) -> T:
        """Open the resource, claim ownership of it, then use it.

        Args:
            use (Callable[[R], T]): Consumes the open resource.

        Returns:
            T: Whatever ``use`` returned.

        Raises:
            _AbandonedError: If the caller stopped waiting while the resource
                was being opened.
        """
        resource = self._open()
        if not self._claim(resource):
            self._close_quietly(resource)
            raise _AbandonedError
        return use(resource)

    def _claim(self, resource: R) -> bool:
        """Register ``resource`` for the caller to close.

        Args:
            resource (R): The freshly opened resource.

        Returns:
            bool: False if the caller already gave up, leaving the close to
                the worker.
        """
        with self._lock:
            if self._released:
                return False
            self._resource = resource
            return True

    def _release(self) -> None:
        """Close the claimed resource, if any, and refuse all later claims."""
        with self._lock:
            self._released = True
            resource, self._resource = self._resource, None
        if resource is not None:
            self._close_quietly(resource)

    def _close_quietly(self, resource: R) -> None:
        """Close ``resource``, logging rather than raising on failure.

        Args:
            resource (R): The resource to close.
        """
        try:
            self._close(resource)
        except Exception as e:
            _LOGGER.warning(
                f"[BlockingResource:_close_quietly] Failed to close resource: {e!r}"
            )
