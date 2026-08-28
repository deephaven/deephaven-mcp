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


def _resource_for(resource, close_error=None):
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

    assert resource.closed
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
async def test_timeouts_still_fire_when_the_default_executor_is_saturated():
    """Cleanup must not queue behind the workers it has to release.

    Every worker is parked in a blocking use, so a close scheduled on the
    default executor could never run and no run() would finish. The generous
    fallback inside the use exists only to stop a regression from hanging the
    suite forever; the deadline below is what actually fails.
    """
    workers = 2
    asyncio.get_running_loop().set_default_executor(
        ThreadPoolExecutor(max_workers=workers)
    )
    resources = [DummyResource() for _ in range(workers)]

    def blocking_use(r):
        r.close_called.wait(timeout=30)

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

    assert all(isinstance(r, TimeoutError) for r in results)
    assert all(r.closed for r in resources)
