"""Tests for ``deephaven_mcp.mcp_systems_server._idle``."""

from __future__ import annotations

import asyncio
import logging
import time

import pytest
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from deephaven_mcp.mcp_systems_server._idle import (
    ActivityMiddleware,
    IdleTimer,
    IdleWatcher,
    idle_watcher,
)

# ---------------------------------------------------------------------------
# IdleTimer
# ---------------------------------------------------------------------------


def test_timer_init_rejects_negative() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        IdleTimer(-1)


def test_timer_disabled_has_zero_idle_seconds() -> None:
    timer = IdleTimer(0)
    assert timer.idle_seconds == 0


def test_timer_remaining_starts_near_window() -> None:
    timer = IdleTimer(60)
    # Some tiny elapsed time between construction and the call.
    assert 58 <= timer.remaining() <= 60


def test_timer_touch_resets_remaining() -> None:
    timer = IdleTimer(60)
    # Force the last-activity timestamp into the past.
    timer._last_activity = time.monotonic() - 30  # noqa: SLF001
    assert timer.remaining() <= 30
    timer.touch()
    assert timer.remaining() >= 59


def test_timer_remaining_floored_at_zero() -> None:
    timer = IdleTimer(1)
    timer._last_activity = time.monotonic() - 100  # noqa: SLF001
    assert timer.remaining() == 0


def test_timer_last_activity_property_advances_on_touch() -> None:
    timer = IdleTimer(60)
    before = timer.last_activity
    time.sleep(0.01)
    timer.touch()
    assert timer.last_activity > before


# ---------------------------------------------------------------------------
# ActivityMiddleware
# ---------------------------------------------------------------------------


def test_activity_middleware_bumps_timer_per_request() -> None:
    timer = IdleTimer(60)
    timer._last_activity = time.monotonic() - 30  # noqa: SLF001
    before = timer.last_activity

    async def ok(_request: object) -> PlainTextResponse:
        return PlainTextResponse("ok")

    app = Starlette(
        routes=[Route("/", ok)],
        middleware=[Middleware(ActivityMiddleware, timer=timer)],
    )
    with TestClient(app) as client:
        client.get("/")
    assert timer.last_activity > before


def test_activity_middleware_timer_property_returns_input() -> None:
    timer = IdleTimer(60)
    mw = ActivityMiddleware(app=lambda *_: None, timer=timer)  # type: ignore[arg-type]
    assert mw.timer is timer


@pytest.mark.asyncio
async def test_activity_middleware_does_not_bump_timer_on_exception() -> None:
    """An upstream exception leaves the idle timer untouched.

    ``dispatch`` bumps the timer only *after* ``call_next`` returns a
    response; when ``call_next`` raises, the touch is never reached and
    the exception propagates unchanged.
    """
    timer = IdleTimer(60)
    timer._last_activity = time.monotonic() - 30  # noqa: SLF001
    before = timer.last_activity
    mw = ActivityMiddleware(app=lambda *_: None, timer=timer)  # type: ignore[arg-type]

    async def boom(_request: object) -> object:
        raise RuntimeError("downstream-boom")

    with pytest.raises(RuntimeError, match="downstream-boom"):
        await mw.dispatch(object(), boom)  # type: ignore[arg-type]
    assert timer.last_activity == before


# ---------------------------------------------------------------------------
# idle_watcher
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_idle_watcher_rejects_disabled_timer() -> None:
    """Callers must not start the watcher when idle_seconds=0."""
    timer = IdleTimer(0)
    with pytest.raises(ValueError, match="idle_seconds=0"):
        await idle_watcher(timer, lambda: None)


@pytest.mark.asyncio
async def test_idle_watcher_fires_when_idle() -> None:
    """A short idle window must fire ``exit_fn`` and return."""
    flag: dict[str, bool] = {"fired": False}

    def exit_fn() -> None:
        flag["fired"] = True

    timer = IdleTimer(1)
    timer._last_activity = time.monotonic() - 60  # noqa: SLF001
    await asyncio.wait_for(idle_watcher(timer, exit_fn), timeout=5)
    assert flag["fired"] is True


@pytest.mark.asyncio
async def test_idle_watcher_propagates_exit_fn_exception() -> None:
    """A raising ``exit_fn`` is contractually forbidden but must propagate.

    The watcher does not defensively swallow the failure; the owning
    lifespan is responsible for inspecting ``task.exception()`` and
    logging it. This test pins the contract: if ``exit_fn`` raises,
    the coroutine raises the same exception.
    """

    def exit_fn() -> None:
        raise RuntimeError("boom")

    timer = IdleTimer(1)
    timer._last_activity = time.monotonic() - 60  # noqa: SLF001
    with pytest.raises(RuntimeError, match="boom"):
        await asyncio.wait_for(idle_watcher(timer, exit_fn), timeout=5)


@pytest.mark.asyncio
async def test_idle_watcher_waits_for_window_then_fires() -> None:
    """When the window has not elapsed, the watcher sleeps and re-checks."""
    flag: dict[str, bool] = {"fired": False}

    def exit_fn() -> None:
        flag["fired"] = True

    timer = IdleTimer(1)
    # Watcher should sleep ~1 second, then fire.
    start = time.monotonic()
    await asyncio.wait_for(idle_watcher(timer, exit_fn), timeout=5)
    elapsed = time.monotonic() - start
    assert flag["fired"] is True
    assert elapsed >= 0.9


@pytest.mark.asyncio
async def test_idle_watcher_resumes_window_after_touch() -> None:
    """Touching the timer mid-sleep extends the watcher's wait."""
    flag: dict[str, bool] = {"fired": False}

    def exit_fn() -> None:
        flag["fired"] = True

    timer = IdleTimer(2)
    task = asyncio.create_task(idle_watcher(timer, exit_fn))
    # Bump just before the original window would expire.
    await asyncio.sleep(0.5)
    timer.touch()
    # Wait long enough for the original window but short enough that
    # the extended window has not yet elapsed.
    await asyncio.sleep(1.6)
    assert flag["fired"] is False
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_idle_watcher_propagates_cancellation() -> None:
    """An in-flight watcher must re-raise ``CancelledError`` on cancel."""
    timer = IdleTimer(60)
    task = asyncio.create_task(idle_watcher(timer, lambda: None))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# IdleWatcher (explicit start/stop lifecycle)
# ---------------------------------------------------------------------------


def test_idle_watcher_exposes_timer_and_exit_fn_properties():
    """``timer`` and ``exit_fn`` properties return the constructor inputs."""
    timer = IdleTimer(60)

    def exit_fn() -> None: ...

    watcher = IdleWatcher(timer=timer, exit_fn=exit_fn)
    assert watcher.timer is timer
    assert watcher.exit_fn is exit_fn
    # Pre-start, no task is created.
    assert watcher.task is None


@pytest.mark.asyncio
async def test_idle_watcher_start_creates_task_and_stop_cancels_it():
    """``start()`` creates the watcher task; ``stop()`` cancels and awaits it."""
    timer = IdleTimer(60)
    exit_calls: list[None] = []

    def exit_fn() -> None:
        exit_calls.append(None)

    watcher = IdleWatcher(timer=timer, exit_fn=exit_fn)
    await watcher.start()
    try:
        # The task is alive and named.
        tasks = [t for t in asyncio.all_tasks() if t.get_name() == "idle-watcher"]
        assert len(tasks) == 1
        assert not tasks[0].done()
        assert watcher.task is tasks[0]
    finally:
        await watcher.stop()
    # After stop(), the task is gone (cancelled and awaited).
    tasks = [t for t in asyncio.all_tasks() if t.get_name() == "idle-watcher"]
    assert tasks == []
    # exit_fn was never called: the watcher was cancelled, not fired.
    assert exit_calls == []


@pytest.mark.asyncio
async def test_idle_watcher_start_is_noop_when_timer_disabled():
    """``IdleTimer(0)`` is the documented "disabled" pretense; no task is created."""
    watcher = IdleWatcher(timer=IdleTimer(0), exit_fn=lambda: None)
    await watcher.start()
    assert watcher.task is None
    tasks = [t for t in asyncio.all_tasks() if t.get_name() == "idle-watcher"]
    assert tasks == []
    # ``stop`` on a never-started watcher is also a no-op.
    await watcher.stop()
    assert watcher.task is None


@pytest.mark.asyncio
async def test_idle_watcher_stop_logs_exit_fn_exception(caplog) -> None:
    """A raising ``exit_fn`` is a contract violation; logged at ERROR but never re-raised."""
    timer = IdleTimer(1)
    # Force the timer past its window so the watcher fires on first iteration.
    timer._last_activity = time.monotonic() - 100  # noqa: SLF001

    def boom_exit_fn() -> None:
        raise RuntimeError("exit_fn boom")

    watcher = IdleWatcher(timer=timer, exit_fn=boom_exit_fn)
    caplog.set_level(logging.ERROR, logger="deephaven_mcp.mcp_systems_server._idle")
    await watcher.start()
    # Give the watcher a chance to fire its exit_fn before stopping.
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await watcher.stop()
    # stop() completed cleanly despite the contract violation.
    assert watcher.task is not None
    assert watcher.task.done()
    # The non-cancellation exception was logged at ERROR.
    assert any(
        "Idle watcher exited" in rec.message
        for rec in caplog.records
        if rec.levelno >= logging.ERROR
    )
