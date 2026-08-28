"""Tests for deephaven_mcp._blocking."""

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from deephaven_mcp._blocking import BlockingResource


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
async def test_run_is_bounded_by_one_deadline_not_two():
    """A stalled use followed by a stalled close stays within the budget."""
    stuck = threading.Event()

    def stalled_close(_r):
        stuck.wait(timeout=30)

    blocking = BlockingResource(DummyResource, stalled_close)
    loop = asyncio.get_running_loop()
    started = loop.time()

    try:
        with pytest.raises(TimeoutError):
            await blocking.run(lambda _r: stuck.wait(timeout=30), timeout_seconds=0.2)
        elapsed = loop.time() - started
    finally:
        stuck.set()

    assert elapsed < 0.35, f"cleanup extended the deadline: {elapsed:.3f}s"
