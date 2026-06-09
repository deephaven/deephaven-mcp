"""Idle-shutdown machinery for the systems server.

A generic mechanism that any HTTP-mode server invocation can opt into
by handing an unstarted :class:`IdleWatcher` to the lifespan factory.
Daemon mode always supplies one (the CLI configures it via
``server.daemon.idle_shutdown_seconds``); foreground HTTP mode could
set one but does not today.

Three pieces, each with one job:

- :class:`IdleTimer` — pure data + monotonic clock. No I/O, no tasks,
  no ASGI.
- :class:`ActivityMiddleware` — Starlette middleware. Calls
  :meth:`IdleTimer.touch` after each successful response.
- :func:`idle_watcher` — long-running coroutine intended to be
  registered on a Starlette/FastMCP lifespan. Sleeps until the timer
  shows zero remaining, then invokes the supplied ``exit_fn`` (which
  in production sets ``uvicorn.Server.should_exit = True``).

The "decide to die" concern lives in :func:`idle_watcher` at the
lifespan layer, not in the middleware. The middleware's only job is
to observe activity.

``exit_fn`` is a strict contract: callbacks **must not raise**. The
watcher does not defensively catch exceptions; if ``exit_fn`` raises,
the task ends abnormally and the lifespan that owns it surfaces the
failure via ``task.exception()``.
"""

from __future__ import annotations

__all__ = [
    "ActivityMiddleware",
    "IdleTimer",
    "IdleWatcher",
    "idle_watcher",
]

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

_LOGGER = logging.getLogger(__name__)


class IdleTimer:
    """Monotonic-clock activity tracker.

    Holds a single :func:`time.monotonic` timestamp, advanced via
    :meth:`touch`. Methods are non-async and non-blocking; the
    timer carries no event-loop state and can be created before
    one exists.

    Attributes are private; observers must use :meth:`elapsed` and
    :meth:`remaining`.
    """

    def __init__(self, idle_seconds: int) -> None:
        """Construct a fresh timer.

        Args:
            idle_seconds (int): Number of seconds without activity
                after which :meth:`remaining` reports ``0``. Must
                be non-negative; ``0`` is treated by
                :func:`idle_watcher` as "disabled" (the watcher
                should not be started).
        """
        if idle_seconds < 0:
            raise ValueError("idle_seconds must be non-negative")
        self._idle_seconds = idle_seconds
        self._last_activity = time.monotonic()

    @property
    def idle_seconds(self) -> int:
        """Configured idle window in seconds."""
        return self._idle_seconds

    @property
    def last_activity(self) -> float:
        """Monotonic timestamp of the most recently observed activity."""
        return self._last_activity

    def touch(self) -> None:
        """Record activity *now*."""
        self._last_activity = time.monotonic()

    def elapsed(self) -> float:
        """Return seconds since the last :meth:`touch`."""
        return time.monotonic() - self._last_activity

    def remaining(self) -> int:
        """Return seconds left in the current idle window.

        Returns:
            int: ``idle_seconds - elapsed``, floored at ``0``. Equal
                to :attr:`idle_seconds` when the watcher window has
                just been reset.
        """
        return max(self._idle_seconds - int(self.elapsed()), 0)


class ActivityMiddleware(BaseHTTPMiddleware):
    """Starlette middleware: bump an :class:`IdleTimer` per successful response.

    Failed downstream responses still count as activity (a 4xx or
    5xx body is a response); only an upstream *exception* — i.e.
    something that prevents the app from producing any response —
    leaves the timer untouched.

    This middleware should be inserted *after* any authentication
    gate (e.g. PSK middleware) so rejected/anonymous traffic does
    not reset the idle timer.
    """

    def __init__(self, app: ASGIApp, *, timer: IdleTimer) -> None:
        """Capture the downstream app and the timer to bump.

        Args:
            app (ASGIApp): The downstream ASGI app this middleware
                wraps.
            timer (IdleTimer): Shared timer instance. The same
                instance must be observed by :func:`idle_watcher`.
        """
        super().__init__(app)
        self._timer = timer

    @property
    def timer(self) -> IdleTimer:
        """The :class:`IdleTimer` this middleware bumps."""
        return self._timer

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        """Forward the request, then bump the timer on the response.

        Args:
            request (Request): The incoming HTTP request.
            call_next: Starlette callable that forwards to the
                downstream application.

        Returns:
            Response: The downstream response, unmodified.
        """
        response = await call_next(request)
        self._timer.touch()
        return response


async def idle_watcher(
    timer: IdleTimer,
    exit_fn: Callable[[], None],
) -> None:
    """Watch ``timer`` and call ``exit_fn`` once the window expires.

    Long-running coroutine. Intended to be created as an
    :func:`asyncio.create_task` during lifespan startup and
    cancelled during lifespan shutdown.

    The sleep duration is :meth:`IdleTimer.remaining` at each loop
    iteration: when the window is reset by intervening activity,
    the watcher naturally waits the remainder of the new window
    rather than polling at a fixed interval. ``remaining()`` is
    floored at ``0``; we therefore sleep at least one second to
    avoid a busy loop in the boundary case where ``elapsed`` ==
    ``idle_seconds``.

    Cancellation (the clean-shutdown path) propagates out as
    :exc:`asyncio.CancelledError`. ``exit_fn`` is contractually
    forbidden from raising; if it does, the exception propagates
    out of the coroutine and the owning lifespan is responsible for
    logging it via :meth:`asyncio.Task.exception`.

    Args:
        timer (IdleTimer): The shared timer to observe. Its
            :attr:`idle_seconds` must be positive; callers must
            not start the watcher when the timer is disabled.
        exit_fn (Callable[[], None]): Zero-arg callable invoked
            when the idle window expires. The production wiring
            sets ``uvicorn.Server.should_exit = True``. **Must not
            raise.**

    Raises:
        ValueError: When ``timer.idle_seconds`` is zero — callers
            must check first and skip starting the watcher.
    """
    if timer.idle_seconds == 0:
        raise ValueError(
            "idle_watcher invoked with timer.idle_seconds=0; "
            "callers must skip starting the watcher when the timer "
            "is disabled"
        )
    _LOGGER.info("[_idle:idle_watcher] Started; idle_seconds=%d", timer.idle_seconds)
    while True:
        remaining = timer.remaining()
        if remaining <= 0:
            _LOGGER.info(
                "[_idle:idle_watcher] Idle window exceeded "
                "(elapsed=%ds >= %ds); invoking exit_fn",
                int(timer.elapsed()),
                timer.idle_seconds,
            )
            exit_fn()
            return
        await asyncio.sleep(max(remaining, 1))


class IdleWatcher:
    """Explicit-lifecycle owner of the :func:`idle_watcher` task.

    Construct with the timer + ``exit_fn`` to supervise; the watcher
    is not started until :meth:`start` is called. Pair every
    :meth:`start` with a :meth:`stop` (the lifespan does this via
    :class:`contextlib.AsyncExitStack`'s
    :meth:`~contextlib.AsyncExitStack.push_async_callback`).

    - :meth:`start` creates an :class:`asyncio.Task` running
      :func:`idle_watcher`, *unless* the timer is disabled
      (``idle_seconds == 0``) — in that case start is a no-op and
      :meth:`stop` is also a no-op. The "disabled" branch keeps
      callers from having to special-case zero before constructing
      the watcher.
    - :meth:`stop` cancels the task and awaits its result.
      :exc:`asyncio.CancelledError` is the expected clean-shutdown
      path and is swallowed silently. Any other exception (a
      contract violation by ``exit_fn``) is logged at ERROR but not
      re-raised — shutdown must continue regardless.
    """

    def __init__(
        self,
        *,
        timer: IdleTimer,
        exit_fn: Callable[[], None],
    ) -> None:
        """Capture the timer and exit callback.

        Args:
            timer (IdleTimer): Shared monotonic-clock activity
                tracker. The same instance must be observed by the
                HTTP transport's :class:`ActivityMiddleware` so
                request traffic resets the same window the watcher
                sleeps against.
            exit_fn (Callable[[], None]): Zero-arg shutdown callback
                invoked once the idle window expires. Production
                wiring sets ``uvicorn.Server.should_exit = True``.
                **Must not raise**; see :func:`idle_watcher`.
        """
        self._timer = timer
        self._exit_fn = exit_fn
        self._task: asyncio.Task[None] | None = None

    @property
    def timer(self) -> IdleTimer:
        """The shared activity tracker passed to the constructor."""
        return self._timer

    @property
    def exit_fn(self) -> Callable[[], None]:
        """The zero-arg shutdown callback passed to the constructor."""
        return self._exit_fn

    @property
    def task(self) -> asyncio.Task[None] | None:
        """The underlying watcher task, or ``None`` when disabled or not yet started."""
        return self._task

    async def start(self) -> None:
        """Start the watcher task (or no-op when the timer is disabled).

        Idempotent only in the disabled-timer case; calling
        :meth:`start` twice on an enabled watcher leaks the prior
        task and is a caller bug.
        """
        if self._timer.idle_seconds == 0:
            return
        self._task = asyncio.create_task(
            idle_watcher(self._timer, self._exit_fn),
            name="idle-watcher",
        )
        _LOGGER.info(
            f"[_idle:IdleWatcher] Started (idle_seconds={self._timer.idle_seconds})"
        )

    async def stop(self) -> None:
        """Cancel the watcher task and log any non-cancellation exception.

        ``CancelledError`` is the expected clean-shutdown path and
        is swallowed silently. The contract for ``idle_watcher``
        forbids ``exit_fn`` from raising; if it does, the awaited
        cancelled task re-raises that exception here and we log it
        once before letting the surrounding shutdown sequence
        continue. Calling :meth:`stop` when the watcher was never
        started (or its timer was disabled) is a no-op.
        """
        task = self._task
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return
        except Exception:
            _LOGGER.exception(
                "[_idle:IdleWatcher] Idle watcher exited with an "
                "unexpected exception; daemon may not shut down cleanly."
            )
