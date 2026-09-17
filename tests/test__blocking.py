"""Tests for deephaven_mcp._blocking."""

import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from deephaven_mcp import _blocking
from deephaven_mcp._blocking import BlockingResource, run_blocking
from deephaven_mcp._exceptions import BlockingDeadlineExceeded


def _live_abandoned():
    """Current count of threads whose caller stopped waiting."""
    return _blocking._abandoned_threads


async def _drain(before):
    """Wait for the abandoned-thread total to fall back to ``before``."""
    for _ in range(500):
        if _live_abandoned() == before:
            return
        await asyncio.sleep(0.01)


async def _settles(event, timeout=10.0):
    """Wait for ``event`` without blocking the loop the callbacks run on."""
    for _ in range(int(timeout / 0.01)):
        if event.is_set():
            return True
        await asyncio.sleep(0.01)
    return event.is_set()


@pytest.mark.asyncio
async def test_a_result_nobody_waited_for_is_disposed_of():
    """The vendor may retain it forever, so an abandoned result must be released."""
    release = threading.Event()
    disposed = []
    closed = threading.Event()

    def dispose(value):
        disposed.append(value)
        closed.set()

    def slow():
        release.wait(30)
        return "the session"

    with pytest.raises(BlockingDeadlineExceeded):
        await run_blocking(slow, "probe-dispose", 0.01, on_abandoned_result=dispose)

    release.set()
    assert await _settles(closed), "the abandoned result was never disposed of"
    assert disposed == ["the session"]


def test_a_result_arriving_after_the_loop_closed_is_still_disposed_of():
    """No loop-thread callback can run at shutdown, so the thread must dispose."""
    release = threading.Event()
    disposed = threading.Event()

    def slow():
        release.wait(30)
        return "the session"

    async def abandon():
        with pytest.raises(BlockingDeadlineExceeded):
            await run_blocking(
                slow,
                "probe-closedloop",
                0.01,
                on_abandoned_result=lambda _value: disposed.set(),
            )

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(abandon())
    finally:
        loop.close()

    # Not on a running loop here, so a blocking wait is safe.
    release.set()
    assert disposed.wait(10), "the result was stranded when the loop went away"


@pytest.mark.asyncio
async def test_disposal_runs_off_the_event_loop():
    """Disposal blocks, so running it on the loop thread would stall the server."""
    release = threading.Event()
    ran_on = []
    done = threading.Event()
    loop_thread = threading.current_thread()

    def dispose(_value):
        ran_on.append(threading.current_thread())
        done.set()

    with pytest.raises(BlockingDeadlineExceeded):
        await run_blocking(
            lambda: release.wait(30), "probe-thread", 0.01, on_abandoned_result=dispose
        )

    release.set()
    assert await _settles(done)
    assert ran_on[0] is not loop_thread


@pytest.mark.asyncio
async def test_a_failed_disposal_is_logged_not_raised(caplog):
    """A close that fails must not take down the thread that attempted it."""
    release = threading.Event()
    attempted = threading.Event()

    def dispose(_value):
        attempted.set()
        raise RuntimeError("close refused")

    with caplog.at_level(logging.WARNING, logger="deephaven_mcp._blocking"):
        with pytest.raises(BlockingDeadlineExceeded):
            await run_blocking(
                lambda: release.wait(30),
                "probe-badclose",
                0.01,
                on_abandoned_result=dispose,
            )
        release.set()
        assert await _settles(attempted)
        for _ in range(500):
            if any("Failed to dispose" in r.message for r in caplog.records):
                break
            await asyncio.sleep(0.01)

    assert any("Failed to dispose" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_abandoning_a_thread_warns_with_a_running_total(caplog):
    """A wedge must leave a trace; silent retention is the worst failure mode."""
    release = threading.Event()
    before = _live_abandoned()

    with caplog.at_level(logging.WARNING, logger="deephaven_mcp._blocking"):
        with pytest.raises(TimeoutError):
            await run_blocking(lambda: release.wait(30), "probe-a", 0.01)

    assert _live_abandoned() == before + 1
    warning = "".join(r.message for r in caplog.records if r.levelno == logging.WARNING)
    assert "'probe-a'" in warning and "abandoned" in warning.lower()

    release.set()
    await _drain(before)
    assert (
        _live_abandoned() == before
    ), "the total never dropped when the thread returned"


@pytest.mark.asyncio
async def test_abandoned_total_drains_when_the_late_thread_raises():
    """The total must drain whether the late thread returns or raises."""
    release = threading.Event()
    before = _live_abandoned()

    def boom():
        release.wait(30)
        raise RuntimeError("late failure")

    with pytest.raises(TimeoutError):
        await run_blocking(boom, "probe-b", 0.01)
    assert _live_abandoned() == before + 1

    release.set()
    await _drain(before)
    assert _live_abandoned() == before


@pytest.mark.asyncio
async def test_timeout_error_from_fn_is_not_counted_as_abandonment():
    """A TimeoutError means the deadline expired unless the work raised it."""
    before = _live_abandoned()

    def boom():
        raise TimeoutError("the vendor call reported its own timeout")

    with pytest.raises(TimeoutError, match="vendor call"):
        await run_blocking(boom, "probe-d", 30.0)

    # Nothing is left running, so nothing will ever arrive to drain a count.
    assert _live_abandoned() == before, "a finished thread was counted as abandoned"


@pytest.mark.asyncio
async def test_outer_cancellation_is_not_counted_as_abandonment():
    """Only our own deadline abandons; an outer cancel must not skew the total."""
    release = threading.Event()
    before = _live_abandoned()

    task = asyncio.create_task(run_blocking(lambda: release.wait(30), "probe-c", 30.0))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    release.set()
    await asyncio.sleep(0.3)
    assert _live_abandoned() == before, "cancellation must not drive the total negative"


class DummyResource:
    """Records whether it was closed, and signals when."""

    def __init__(self, close_error: Exception | None = None):
        self.closed = False
        self.close_error = close_error
        self.close_called = threading.Event()

    def close(self):
        self.closed = True
        self.close_called.set()
        if self.close_error is not None:
            raise self.close_error


def _resource_for(resource):
    """Build a BlockingResource that opens ``resource`` and closes it."""
    return BlockingResource(lambda: resource, lambda r: r.close())


@pytest.mark.asyncio
async def test_run_returns_the_use_result_and_closes():
    resource = DummyResource()
    used = []

    result = await _resource_for(resource).run(
        lambda r: used.append(r) or "done", timeout_seconds=30.0
    )

    assert result == "done"
    assert used == [resource]
    assert resource.closed


@pytest.mark.asyncio
async def test_run_closes_after_use_raises():
    resource = DummyResource()

    with pytest.raises(RuntimeError, match="boom"):
        await _resource_for(resource).run(
            lambda _r: (_ for _ in ()).throw(RuntimeError("boom")),
            timeout_seconds=30.0,
        )

    assert resource.closed


@pytest.mark.asyncio
async def test_run_propagates_an_open_failure_with_nothing_to_close():
    def failing_open():
        raise RuntimeError("cannot open")

    closed = []
    resource = BlockingResource(failing_open, lambda r: closed.append(r))

    with pytest.raises(RuntimeError, match="cannot open"):
        await resource.run(lambda _r: None, timeout_seconds=30.0)

    assert closed == []


@pytest.mark.asyncio
async def test_run_times_out_and_closes_to_end_the_blocked_use():
    """The close is what releases a worker parked in an uninterruptible use."""
    resource = DummyResource()
    worker_returned = threading.Event()

    def blocking_use(r):
        assert r.close_called.wait(timeout=10)
        worker_returned.set()

    with pytest.raises(TimeoutError):
        await _resource_for(resource).run(blocking_use, timeout_seconds=0.05)

    assert resource.close_called.wait(timeout=10), "the resource was never closed"
    assert worker_returned.wait(timeout=10), "worker thread was left stranded"


@pytest.mark.asyncio
async def test_a_resource_opened_after_the_caller_gave_up_is_still_closed():
    """The abandoned worker owns the close once the caller has stopped waiting."""
    resource = DummyResource()
    release = threading.Event()
    used = []

    def stalled_open():
        assert release.wait(timeout=10)
        return resource

    blocking = BlockingResource(stalled_open, lambda r: r.close())

    with pytest.raises(TimeoutError):
        await blocking.run(used.append, timeout_seconds=0.05)

    release.set()
    assert resource.close_called.wait(timeout=10), "late resource was leaked"
    assert used == [], "an abandoned resource must not be used"


@pytest.mark.asyncio
async def test_abandonment_is_committed_before_cleanup_is_detached():
    """run() must mark the resource abandoned itself, not leave it to cleanup.

    The cleanup thread is gated here so it cannot set the flag, proving the
    commit is synchronous. Otherwise a stalled opener could claim the resource
    and run ``use`` after the caller had already timed out.
    """
    gate = threading.Event()
    open_gate = threading.Event()
    resource = DummyResource()
    used = []

    def stalled_open():
        assert open_gate.wait(timeout=10)
        return resource

    blocking = BlockingResource(stalled_open, lambda r: r.close())
    release_on_thread = blocking._release

    def gated_release():
        assert gate.wait(timeout=10)
        release_on_thread()

    blocking._release = gated_release

    try:
        with pytest.raises(TimeoutError):
            await blocking.run(used.append, timeout_seconds=0.05)

        assert blocking._released, "run() returned before committing abandonment"

        open_gate.set()
        assert resource.close_called.wait(timeout=10), "late resource was leaked"
        assert used == [], "an abandoned resource must not be used"
    finally:
        gate.set()
        open_gate.set()


@pytest.mark.asyncio
async def test_a_close_failure_does_not_mask_the_result():
    resource = DummyResource(close_error=RuntimeError("already gone"))

    result = await _resource_for(resource).run(lambda _r: "done", timeout_seconds=30.0)

    assert result == "done"
    assert resource.closed


@pytest.mark.asyncio
async def test_a_close_failure_does_not_mask_an_error():
    resource = DummyResource(close_error=RuntimeError("already gone"))

    with pytest.raises(ValueError, match="original"):
        await _resource_for(resource).run(
            lambda _r: (_ for _ in ()).throw(ValueError("original")),
            timeout_seconds=30.0,
        )

    assert resource.closed


@pytest.mark.asyncio
async def test_release_is_idempotent_when_nothing_was_opened():
    closed = []
    blocking = BlockingResource(lambda: None, lambda r: closed.append(r))

    await asyncio.to_thread(blocking._release)
    await asyncio.to_thread(blocking._release)

    assert closed == []


@pytest.mark.asyncio
async def test_a_stalled_close_does_not_hold_up_the_caller():
    """Cleanup is bounded: a close that stalls is detached, not awaited.

    The constructor allows close to block, so an unbounded wait here would let
    a request that already timed out never return.
    """
    stuck = threading.Event()
    closing = threading.Event()

    def stalled_close(_r):
        closing.set()
        stuck.wait(timeout=30)

    blocking = BlockingResource(DummyResource, stalled_close)

    try:
        result = await asyncio.wait_for(
            blocking.run(lambda _r: "done", timeout_seconds=0.05), timeout=5
        )
    finally:
        stuck.set()

    assert result == "done"
    assert closing.is_set(), "the close never started"


@pytest.mark.asyncio
async def test_timeouts_still_fire_when_the_default_executor_is_saturated():
    """A saturated default executor must not affect this class at all.

    Nothing here runs on that executor, so both the deadline and the close
    still hold while every shared worker is parked.
    """
    workers = 2
    asyncio.get_running_loop().set_default_executor(
        ThreadPoolExecutor(max_workers=workers)
    )
    hog = threading.Event()
    hogs = [
        asyncio.ensure_future(asyncio.to_thread(hog.wait, 30)) for _ in range(workers)
    ]
    await asyncio.sleep(0.05)

    resources = [DummyResource() for _ in range(workers)]

    def blocking_use(r):
        r.close_called.wait(timeout=30)

    try:
        results = await asyncio.wait_for(
            asyncio.gather(
                *(
                    _resource_for(r).run(blocking_use, timeout_seconds=0.05)
                    for r in resources
                ),
                return_exceptions=True,
            ),
            timeout=5,
        )
    finally:
        hog.set()
        await asyncio.gather(*hogs, return_exceptions=True)

    assert all(isinstance(r, TimeoutError) for r in results)
    for r in resources:
        assert r.close_called.wait(timeout=10)


@pytest.mark.asyncio
async def test_a_stalled_open_does_not_consume_a_shared_executor_worker():
    """An open that never returns must not block unrelated pool work."""
    asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=1))
    never = threading.Event()

    def stalled_open():
        never.wait(timeout=30)
        return DummyResource()

    blocking = BlockingResource(stalled_open, lambda r: r.close())

    try:
        with pytest.raises(TimeoutError):
            await blocking.run(lambda _r: None, timeout_seconds=0.05)

        # The one shared worker is still free.
        assert await asyncio.wait_for(asyncio.to_thread(lambda: "free"), timeout=5)
    finally:
        never.set()


@pytest.mark.asyncio
async def test_run_gives_cleanup_only_what_the_operation_left(monkeypatch):
    """One deadline covers the operation and the close, not one budget each."""
    budget = 0.2
    spent = 0.12
    granted: list[float] = []
    loop = asyncio.get_running_loop()
    now = loop.time()
    monkeypatch.setattr(loop, "time", lambda: now)

    async def fake_run_blocking(fn, name, timeout_seconds):
        nonlocal now
        granted.append(timeout_seconds)
        if len(granted) == 1:
            now += spent
            raise TimeoutError
        return None

    monkeypatch.setattr(_blocking, "run_blocking", fake_run_blocking)

    with pytest.raises(TimeoutError):
        await BlockingResource(DummyResource, lambda _r: None).run(
            lambda _r: None, timeout_seconds=budget
        )

    assert granted == [
        budget,
        pytest.approx(budget - spent),
    ], "the close must get the remainder of the budget, not a fresh one"
