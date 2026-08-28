"""Deadline-bounded use of a blocking resource on a private thread.

Synchronous vendor clients often expose reads that cannot be interrupted;
closing the underlying resource from another thread is the only way to end
one. ``asyncio.wait_for`` cancels the awaiting coroutine but does not stop the
thread it was waiting on, so a resource opened there can outlive the caller
that asked for it, and a call that never returns keeps its thread forever.
This module settles both problems in one place so call sites do not each
reinvent them.
"""

from __future__ import annotations

__all__ = ["BlockingResource"]

import asyncio
import logging
import threading
from collections.abc import Callable

_LOGGER = logging.getLogger(__name__)


async def _run_on_own_thread[T](
    fn: Callable[[], T], name: str, timeout_seconds: float
) -> T:
    """Run ``fn`` on a dedicated daemon thread, bounded by ``timeout_seconds``.

    Deliberately not ``asyncio.to_thread``: that shares one process-wide
    executor, so a call that never returns permanently consumes a worker every
    other part of the process competes for. A thread of its own keeps that
    damage local, and being a daemon it cannot hold up interpreter exit. The
    wait itself runs entirely on the event loop, so the deadline holds however
    busy the machine is.

    Args:
        fn (Callable[[], T]): The blocking work to run.
        name (str): Thread name, for diagnosis in stack dumps.
        timeout_seconds (float): How long to wait before abandoning the thread.

    Returns:
        T: Whatever ``fn`` returned.

    Raises:
        TimeoutError: If ``fn`` does not finish within ``timeout_seconds``.
        BaseException: Anything ``fn`` raised propagates unchanged.
    """
    loop = asyncio.get_running_loop()
    done: asyncio.Future[T] = loop.create_future()

    # An abandoned thread still reports back, hence the done() checks.
    def _deliver_result(value: T) -> None:
        if not done.done():
            done.set_result(value)

    def _deliver_error(error: BaseException) -> None:
        if not done.done():
            done.set_exception(error)

    def _worker() -> None:
        try:
            result = fn()
        except BaseException as e:  # noqa: BLE001 - reported through the future
            loop.call_soon_threadsafe(_deliver_error, e)
        else:
            loop.call_soon_threadsafe(_deliver_result, result)

    threading.Thread(target=_worker, name=name, daemon=True).start()
    return await asyncio.wait_for(done, timeout=timeout_seconds)


class _AbandonedError(Exception):
    """The caller gave up while the resource was still opening.

    Never observed: it can only be raised after ``wait_for`` has already
    raised, so the future that would carry it is gone.
    """


class BlockingResource[R]:
    """Opens, uses, and closes a blocking resource on a private thread.

    The resource is closed exactly once, including when the deadline expires
    while it is still being opened. Ownership is settled under a lock and the
    side that arrives second performs the close, because the worker can finish
    opening after the caller has stopped waiting.

    Nothing here touches the default executor, so a vendor call that never
    returns cannot starve unrelated work.

    Single use: each :meth:`run` call needs a fresh instance.
    """

    def __init__(self, open_: Callable[[], R], close: Callable[[R], None]) -> None:
        """Initialize the resource with its open and close operations.

        Args:
            open_ (Callable[[], R]): Opens the resource. Runs on a private
                thread and may block.
            close (Callable[[R], None]): Closes the resource. Runs on a private
                thread and may block; failures are logged, not raised.
        """
        self._open = open_
        self._close = close
        self._lock = threading.Lock()
        self._resource: R | None = None
        self._released = False

    async def run[T](self, use: Callable[[R], T], *, timeout_seconds: float) -> T:
        """Open the resource, pass it to ``use``, and close it.

        ``timeout_seconds`` bounds the call as a whole: whatever the operation
        leaves unspent is all the cleanup gets, so a stalled close cannot
        stretch the wait past the deadline the caller asked for.

        Args:
            use (Callable[[R], T]): Consumes the open resource. Runs on the
                same private thread as the open and may block.
            timeout_seconds (float): Budget for the open, the use, and the
                close together.

        Returns:
            T: Whatever ``use`` returned.

        Raises:
            TimeoutError: If the open and use together exceed
                ``timeout_seconds``.
            Exception: Anything ``open_`` or ``use`` raises propagates
                unchanged.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        try:
            return await _run_on_own_thread(
                lambda: self._open_and_use(use),
                "dh-mcp-blocking-use",
                timeout_seconds,
            )
        finally:
            # Only what the operation left over, so a stalled close cannot
            # stretch the call to twice the advertised budget.
            remaining = max(0.0, deadline - loop.time())
            try:
                await _run_on_own_thread(
                    self._release, "dh-mcp-blocking-cleanup", remaining
                )
            except TimeoutError:
                _LOGGER.warning(
                    f"[BlockingResource:run] Close did not finish within the "
                    f"remaining {remaining:.3f}s; leaving it to its own thread"
                )

    def _open_and_use[T](self, use: Callable[[R], T]) -> T:
        """Open the resource, claim ownership of it, then use it.

        Args:
            use (Callable[[R], T]): Consumes the open resource.

        Returns:
            T: Whatever ``use`` returned.

        Raises:
            _AbandonedError: If the caller stopped waiting while the resource
                was opening, leaving this thread to close it.
        """
        resource = self._open()
        with self._lock:
            claimed = not self._released
            if claimed:
                self._resource = resource
        if not claimed:
            self._close_quietly(resource)
            raise _AbandonedError
        return use(resource)

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
