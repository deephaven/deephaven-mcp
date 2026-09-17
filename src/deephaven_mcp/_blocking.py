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

__all__ = ["BlockingResource", "run_blocking"]

import asyncio
import logging
import threading
from collections.abc import Callable
from typing import Any

from deephaven_mcp._exceptions import BlockingDeadlineExceeded

_LOGGER = logging.getLogger(__name__)

_ABANDONED_LOCK = threading.Lock()
_abandoned_threads = 0
"""Live count of threads whose caller stopped waiting, for the log messages below."""


def _note_abandoned(name: str, timeout_seconds: float) -> None:
    """Report a thread the caller has stopped waiting for.

    Python cannot kill a thread, so an uninterruptible call that overruns its
    deadline keeps running and keeps everything it captured alive. Nothing here
    can prevent that; the running total exists so a wedged dependency shows up
    as a trend in the logs instead of silently growing RSS.

    Args:
        name (str): The abandoned thread's name.
        timeout_seconds (float): The deadline that expired.
    """
    global _abandoned_threads
    with _ABANDONED_LOCK:
        _abandoned_threads += 1
        live = _abandoned_threads
    _LOGGER.warning(
        f"[run_blocking] {name!r} overran {timeout_seconds}s and was abandoned; "
        f"it keeps running and retains what it captured. "
        f"Abandoned threads now live: {live}."
    )


def _note_late_arrival(name: str, abandoned: bool) -> None:
    """Report a thread that finished after nobody was waiting for it.

    Args:
        name (str): The thread's name.
        abandoned (bool): Whether this call's own deadline abandoned it. False
            when the caller was canceled from outside, which never incremented
            the total and so must not decrement it.
    """
    global _abandoned_threads
    if not abandoned:
        return
    with _ABANDONED_LOCK:
        _abandoned_threads -= 1
        live = _abandoned_threads
    _LOGGER.info(
        f"[run_blocking] Abandoned {name!r} finished and released what it held. "
        f"Abandoned threads still live: {live}."
    )


def _report_to_loop(
    loop: asyncio.AbstractEventLoop,
    deliver: Callable[[Any], None],
    value: Any,
    name: str,
    dispose: Callable[[Any], None] | None = None,
) -> None:
    """Hand ``value`` to ``deliver`` on ``loop``, tolerating a closed loop.

    Args:
        loop (asyncio.AbstractEventLoop): The loop the caller waited on.
        deliver (Callable[[Any], None]): Loop-thread callback recording the
            outcome.
        value (Any): The result or exception to report.
        name (str): Thread name, for the discard log.
        dispose (Callable[[Any], None] | None): Releases ``value`` when the
            loop is gone, since no loop-thread callback can run to do it.
    """
    try:
        loop.call_soon_threadsafe(deliver, value)
    except RuntimeError:
        # An abandoned thread can outlive the loop it was started from, and
        # nobody is waiting on it by then.
        _LOGGER.debug(
            f"[run_blocking] {name!r} finished after its loop closed; "
            f"result discarded"
        )
        # Already off the loop here, so dispose inline rather than spawning.
        if dispose is not None:
            _dispose_abandoned(dispose, value, name)


def _dispose_abandoned(dispose: Callable[[Any], None], value: Any, name: str) -> None:
    """Run ``dispose`` on ``value``, logging rather than raising on failure.

    Args:
        dispose (Callable[[Any], None]): Releases the abandoned result.
        value (Any): The result nobody was waiting for.
        name (str): Thread name, for the log.
    """
    try:
        dispose(value)
    except Exception as e:
        _LOGGER.warning(
            f"[run_blocking] Failed to dispose of {name!r}'s abandoned "
            f"result: {e!r}"
        )
    else:
        _LOGGER.info(f"[run_blocking] Disposed of {name!r}'s abandoned result")


def _release_abandoned(
    dispose: Callable[[Any], None] | None, value: Any, name: str
) -> None:
    """Hand ``value`` to ``dispose`` on a thread of its own, if there is one.

    Args:
        dispose (Callable[[Any], None] | None): Releases the result, or
            ``None`` to leave it to the garbage collector.
        value (Any): The result nobody was waiting for.
        name (str): Thread name, for the log and the disposal thread's name.
    """
    if dispose is None:
        return
    # Disposal blocks, and the caller of this is the loop thread.
    threading.Thread(
        target=_dispose_abandoned,
        args=(dispose, value, name),
        name=f"{name}-dispose",
        daemon=True,
    ).start()


async def run_blocking[T](
    fn: Callable[[], T],
    name: str,
    timeout_seconds: float,
    *,
    on_abandoned_result: Callable[[T], None] | None = None,
) -> T:
    """Run ``fn`` on a dedicated daemon thread, bounded by ``timeout_seconds``.

    Deliberately not ``asyncio.to_thread``: that shares one process-wide
    executor, so a call that never returns permanently consumes a worker every
    other part of the process competes for. A thread of its own keeps that
    damage local, and being a daemon it cannot hold up interpreter exit. The
    wait itself runs entirely on the event loop, so the deadline holds however
    busy the machine is.

    The deadline bounds the *wait*, never the work: on expiry the thread is
    abandoned, not stopped, and is logged by :func:`_note_abandoned`. Capping
    how many may accumulate would only reintroduce the starvation this avoids,
    since the cleanup that frees them runs through here too.

    Args:
        fn (Callable[[], T]): The blocking work to run.
        name (str): Thread name, for diagnosis in stack dumps.
        timeout_seconds (float): How long to wait before abandoning the thread.
        on_abandoned_result (Callable[[T], None] | None): Releases a result
            that arrives after the deadline, which nobody receives and which
            would otherwise be retained for the process's lifetime by anything
            the vendor registers it with. Runs on its own daemon thread, so it
            may block; failures are logged, not raised.

    Returns:
        T: Whatever ``fn`` returned.

    Raises:
        BlockingDeadlineExceeded: If ``fn`` does not finish within
            ``timeout_seconds``.
        BaseException: Anything ``fn`` raised propagates unchanged, including a
            ``TimeoutError`` of its own.
    """
    loop = asyncio.get_running_loop()
    done: asyncio.Future[T] = loop.create_future()
    abandoned = False
    delivered = False

    # An abandoned thread still reports back, hence the done() checks. Both
    # these and the assignment below run on the loop thread, so the flags need
    # no lock.
    def _deliver_result(value: T) -> None:
        nonlocal delivered
        delivered = True
        if done.done():
            _note_late_arrival(name, abandoned)
            _release_abandoned(on_abandoned_result, value, name)
        else:
            done.set_result(value)

    def _deliver_error(error: BaseException) -> None:
        nonlocal delivered
        delivered = True
        if done.done():
            _note_late_arrival(name, abandoned)
        else:
            done.set_exception(error)

    def _worker() -> None:
        try:
            result = fn()
        except BaseException as e:  # noqa: BLE001 - reported through the future
            _report_to_loop(loop, _deliver_error, e, name)
        else:
            _report_to_loop(loop, _deliver_result, result, name, on_abandoned_result)

    threading.Thread(target=_worker, name=name, daemon=True).start()
    try:
        return await asyncio.wait_for(done, timeout=timeout_seconds)
    except TimeoutError:
        # A TimeoutError from ``fn`` itself arrives through the future and
        # leaves no thread behind, so it keeps its own meaning.
        if delivered:
            raise
        abandoned = True
        _note_abandoned(name, timeout_seconds)
        raise BlockingDeadlineExceeded(
            f"{name!r} did not finish within {timeout_seconds}s"
        ) from None


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
            return await run_blocking(
                lambda: self._open_and_use(use),
                "dh-mcp-blocking-use",
                timeout_seconds,
            )
        finally:
            # Committed here, not on the cleanup thread: that thread may never
            # be scheduled before this returns, and until _released is set a
            # late opener could still claim the resource and run ``use``.
            with self._lock:
                self._released = True
            # Only what the operation left over, so a stalled close cannot
            # stretch the call to twice the advertised budget.
            remaining = max(0.0, deadline - loop.time())
            try:
                await run_blocking(self._release, "dh-mcp-blocking-cleanup", remaining)
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
