"""Tests for deephaven_mcp.mcp_systems_server._session_registry_manager."""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deephaven_mcp.config import DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS
from deephaven_mcp.mcp_systems_server._session_registry_manager import (
    SessionClosedDuringInitError,
    SessionRegistryManager,
)
from deephaven_mcp.resource_manager import BaseRegistry


def _make_manager(
    idle_timeout: float = DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS,
    sweep_interval: float = 60.0,
    registry_class=None,
) -> SessionRegistryManager:
    if registry_class is None:
        registry_class = MagicMock(spec=type)
    return SessionRegistryManager(
        registry_class=registry_class,
        idle_timeout_seconds=idle_timeout,
        sweep_interval_seconds=sweep_interval,
    )


def _make_registry_class():
    """Return (factory, registry) where factory() returns the same AsyncMock registry."""
    registry = AsyncMock(spec=BaseRegistry)
    registry.initialize = AsyncMock()
    registry.close = AsyncMock()
    factory = MagicMock()
    factory.return_value = registry
    return factory, registry


# =============================================================================
# __init__
# =============================================================================


def test_init_stores_params():
    factory, _ = _make_registry_class()
    mgr = SessionRegistryManager(
        registry_class=factory,
        idle_timeout_seconds=300.0,
        sweep_interval_seconds=30.0,
    )
    assert mgr._idle_timeout == 300.0
    assert mgr._sweep_interval == 30.0
    assert mgr._registry_class is factory
    assert mgr._entries == {}
    assert mgr._sweeper_task is None


# =============================================================================
# get_or_create_registry
# =============================================================================


@pytest.mark.asyncio
async def test_first_call_creates_and_initializes():
    factory, registry = _make_registry_class()
    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    result = await mgr.get_or_create_registry("session-1", config_manager)

    assert result is registry
    factory.assert_called_once_with()
    registry.initialize.assert_called_once_with(config_manager)
    assert "session-1" in mgr._entries
    entry = mgr._entries["session-1"]
    assert entry.registry is registry
    assert not entry.closed.is_set()


@pytest.mark.asyncio
async def test_second_call_returns_same_instance_no_reinit():
    factory, registry = _make_registry_class()
    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    r1 = await mgr.get_or_create_registry("session-1", config_manager)
    r2 = await mgr.get_or_create_registry("session-1", config_manager)

    assert r1 is r2
    factory.assert_called_once_with()
    registry.initialize.assert_called_once_with(config_manager)


@pytest.mark.asyncio
async def test_updates_last_used_each_call():
    factory, _ = _make_registry_class()
    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    with patch(
        "deephaven_mcp.mcp_systems_server._session_registry_manager.time"
    ) as mock_time:
        mock_time.monotonic.return_value = 100.0
        await mgr.get_or_create_registry("session-1", config_manager)
        assert mgr._entries["session-1"].last_used == 100.0

        mock_time.monotonic.return_value = 200.0
        await mgr.get_or_create_registry("session-1", config_manager)
        assert mgr._entries["session-1"].last_used == 200.0


@pytest.mark.asyncio
async def test_different_sessions_isolated():
    factory = MagicMock()
    reg_a = AsyncMock(spec=BaseRegistry)
    reg_a.initialize = AsyncMock()
    reg_b = AsyncMock(spec=BaseRegistry)
    reg_b.initialize = AsyncMock()
    factory.side_effect = [reg_a, reg_b]

    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    ra = await mgr.get_or_create_registry("session-a", config_manager)
    rb = await mgr.get_or_create_registry("session-b", config_manager)

    assert ra is reg_a
    assert rb is reg_b
    assert factory.call_count == 2


@pytest.mark.asyncio
async def test_init_failure_propagates_and_closes_registry():
    factory, registry = _make_registry_class()
    registry.initialize.side_effect = RuntimeError("init failed")
    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    with pytest.raises(RuntimeError, match="init failed"):
        await mgr.get_or_create_registry("session-1", config_manager)

    registry.close.assert_called_once()
    assert "session-1" not in mgr._entries


@pytest.mark.asyncio
async def test_init_cancelled_closes_and_propagates():
    factory, registry = _make_registry_class()
    registry.initialize.side_effect = asyncio.CancelledError()
    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    with pytest.raises(asyncio.CancelledError):
        await mgr.get_or_create_registry("session-1", config_manager)

    registry.close.assert_called_once()
    assert "session-1" not in mgr._entries


@pytest.mark.asyncio
async def test_init_failure_close_failure_is_logged_not_raised():
    """If initialize fails AND close also fails, the close error is logged."""
    factory, registry = _make_registry_class()
    registry.initialize.side_effect = RuntimeError("init failed")
    registry.close.side_effect = RuntimeError("close also failed")
    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    # Original initialize() error must be the one that propagates.
    with pytest.raises(RuntimeError, match="init failed"):
        await mgr.get_or_create_registry("session-1", config_manager)

    registry.close.assert_called_once()
    assert "session-1" not in mgr._entries


@pytest.mark.asyncio
async def test_init_failure_allows_subsequent_retry():
    """A fresh entry is created on the next call after a prior init failure."""
    factory = MagicMock()
    reg_first = AsyncMock(spec=BaseRegistry)
    reg_first.initialize = AsyncMock(side_effect=RuntimeError("first init failed"))
    reg_first.close = AsyncMock()
    reg_second = AsyncMock(spec=BaseRegistry)
    reg_second.initialize = AsyncMock()
    reg_second.close = AsyncMock()
    factory.side_effect = [reg_first, reg_second]

    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    with pytest.raises(RuntimeError, match="first init failed"):
        await mgr.get_or_create_registry("session-1", config_manager)

    result = await mgr.get_or_create_registry("session-1", config_manager)
    assert result is reg_second


# =============================================================================
# Single-flight initialization
# =============================================================================


@pytest.mark.asyncio
async def test_concurrent_first_access_single_flight():
    """Two concurrent first-access calls share one initialize()."""
    factory, registry = _make_registry_class()

    init_started = asyncio.Event()
    init_gate = asyncio.Event()

    async def blocking_init(*_args, **_kwargs):
        init_started.set()
        await init_gate.wait()

    registry.initialize = AsyncMock(side_effect=blocking_init)
    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    task_a = asyncio.create_task(
        mgr.get_or_create_registry("session-1", config_manager)
    )
    await init_started.wait()

    task_b = asyncio.create_task(
        mgr.get_or_create_registry("session-1", config_manager)
    )
    await asyncio.sleep(0)  # let waiter reach the per-entry lock

    init_gate.set()
    result_a = await task_a
    result_b = await task_b

    assert result_a is registry
    assert result_b is registry
    factory.assert_called_once()
    registry.initialize.assert_called_once()
    registry.close.assert_not_called()


@pytest.mark.asyncio
async def test_concurrent_first_access_init_failure_propagates_to_waiter():
    """If owner's initialize() fails, the waiter retries with a fresh entry."""
    factory = MagicMock()
    reg_first = AsyncMock(spec=BaseRegistry)
    reg_first.close = AsyncMock()
    reg_second = AsyncMock(spec=BaseRegistry)
    reg_second.initialize = AsyncMock()
    reg_second.close = AsyncMock()

    init_started = asyncio.Event()
    init_gate = asyncio.Event()

    async def failing_init(*_args, **_kwargs):
        init_started.set()
        await init_gate.wait()
        raise RuntimeError("owner init failed")

    reg_first.initialize = AsyncMock(side_effect=failing_init)
    factory.side_effect = [reg_first, reg_second]

    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    task_a = asyncio.create_task(
        mgr.get_or_create_registry("session-1", config_manager)
    )
    await init_started.wait()

    task_b = asyncio.create_task(
        mgr.get_or_create_registry("session-1", config_manager)
    )
    await asyncio.sleep(0)

    init_gate.set()

    with pytest.raises(RuntimeError, match="owner init failed"):
        await task_a
    # Waiter retries on a fresh entry since the failed owner marked the entry dead.
    result_b = await task_b
    assert result_b is reg_second
    reg_first.close.assert_called_once()


@pytest.mark.asyncio
async def test_lock_not_held_across_initialize_for_other_sessions():
    """One session's slow initialize() must not block other sessions."""
    factory = MagicMock()
    reg_a = AsyncMock(spec=BaseRegistry)
    reg_b = AsyncMock(spec=BaseRegistry)

    init_started = asyncio.Event()
    other_completed = asyncio.Event()

    async def blocking_init(*_args, **_kwargs):
        init_started.set()
        await other_completed.wait()

    reg_a.initialize = AsyncMock(side_effect=blocking_init)
    reg_b.initialize = AsyncMock()
    factory.side_effect = [reg_a, reg_b]

    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    task_a = asyncio.create_task(
        mgr.get_or_create_registry("session-1", config_manager)
    )
    await init_started.wait()

    result_b = await mgr.get_or_create_registry("session-2", config_manager)
    other_completed.set()
    result_a = await task_a

    assert result_a is reg_a
    assert result_b is reg_b


# =============================================================================
# close_session
# =============================================================================


@pytest.mark.asyncio
async def test_close_session_closes_and_removes():
    factory, registry = _make_registry_class()
    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()
    await mgr.get_or_create_registry("session-1", config_manager)

    await mgr.close_session("session-1")

    registry.close.assert_called_once()
    assert "session-1" not in mgr._entries


@pytest.mark.asyncio
async def test_close_session_idempotent_on_unknown_id():
    mgr = _make_manager()
    await mgr.close_session("nonexistent-session")  # must not raise


@pytest.mark.asyncio
async def test_close_session_exception_is_logged_not_raised():
    factory, registry = _make_registry_class()
    registry.close.side_effect = RuntimeError("close failed")
    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()
    await mgr.get_or_create_registry("session-1", config_manager)

    await mgr.close_session("session-1")

    registry.close.assert_called_once()
    assert "session-1" not in mgr._entries


@pytest.mark.asyncio
async def test_close_session_during_init_discards_new_registry():
    """close_session() while initialize() is in flight must discard the new registry."""
    factory, registry = _make_registry_class()

    init_started = asyncio.Event()
    init_gate = asyncio.Event()

    async def blocking_init(*_args, **_kwargs):
        init_started.set()
        await init_gate.wait()

    registry.initialize = AsyncMock(side_effect=blocking_init)
    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    task_owner = asyncio.create_task(
        mgr.get_or_create_registry("session-1", config_manager)
    )
    await init_started.wait()

    # close_session needs the entry's lock, which the owner holds; run it in
    # the background so we can release the gate.
    close_task = asyncio.create_task(mgr.close_session("session-1"))
    await asyncio.sleep(0)
    assert "session-1" not in mgr._entries  # already removed from map

    init_gate.set()
    with pytest.raises(SessionClosedDuringInitError):
        await task_owner
    await close_task

    registry.close.assert_called_once()
    assert mgr._entries == {}


@pytest.mark.asyncio
async def test_close_session_during_init_close_failure_logged_not_raised(caplog):
    """If registry.close() also fails after close-during-init, the close error is logged."""
    factory, registry = _make_registry_class()

    init_started = asyncio.Event()
    init_gate = asyncio.Event()

    async def blocking_init(*_args, **_kwargs):
        init_started.set()
        await init_gate.wait()

    registry.initialize = AsyncMock(side_effect=blocking_init)
    registry.close = AsyncMock(side_effect=RuntimeError("close failed"))
    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    task_owner = asyncio.create_task(
        mgr.get_or_create_registry("session-1", config_manager)
    )
    await init_started.wait()

    close_task = asyncio.create_task(mgr.close_session("session-1"))
    await asyncio.sleep(0)

    init_gate.set()

    with caplog.at_level(logging.ERROR):
        # The owner must surface SessionClosedDuringInitError, NOT the close failure.
        with pytest.raises(SessionClosedDuringInitError):
            await task_owner
        await close_task

    registry.close.assert_called()
    assert any(
        "Error closing registry discarded after close-during-init" in rec.message
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_close_session_during_init_propagates_to_waiters():
    """A waiter that joined before close_session retries on a fresh entry."""
    factory = MagicMock()
    reg_first = AsyncMock(spec=BaseRegistry)
    reg_first.close = AsyncMock()
    reg_second = AsyncMock(spec=BaseRegistry)
    reg_second.initialize = AsyncMock()
    reg_second.close = AsyncMock()

    init_started = asyncio.Event()
    init_gate = asyncio.Event()

    async def blocking_init(*_args, **_kwargs):
        init_started.set()
        await init_gate.wait()

    reg_first.initialize = AsyncMock(side_effect=blocking_init)
    factory.side_effect = [reg_first, reg_second]

    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    task_owner = asyncio.create_task(
        mgr.get_or_create_registry("session-1", config_manager)
    )
    await init_started.wait()

    task_waiter = asyncio.create_task(
        mgr.get_or_create_registry("session-1", config_manager)
    )
    await asyncio.sleep(0)

    close_task = asyncio.create_task(mgr.close_session("session-1"))
    await asyncio.sleep(0)

    init_gate.set()
    with pytest.raises(SessionClosedDuringInitError):
        await task_owner
    # Waiter retries on a fresh entry created after the first one was marked dead.
    result_b = await task_waiter
    await close_task

    assert result_b is reg_second
    reg_first.close.assert_called_once()


# =============================================================================
# start / stop
# =============================================================================


@pytest.mark.asyncio
async def test_start_creates_sweeper_task():
    mgr = _make_manager()
    assert mgr._sweeper_task is None
    await mgr.start()
    assert mgr._sweeper_task is not None
    assert not mgr._sweeper_task.done()
    mgr._sweeper_task.cancel()
    try:
        await mgr._sweeper_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_start_twice_does_not_orphan_first_task():
    mgr = _make_manager(sweep_interval=60.0)
    await mgr.start()
    original_task = mgr._sweeper_task

    await mgr.start()  # idempotent

    assert mgr._sweeper_task is original_task
    assert not original_task.done()

    original_task.cancel()
    try:
        await original_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_start_after_done_creates_new_task():
    """If the previous sweeper task ended (e.g. cancelled), start() can run again."""
    mgr = _make_manager()
    await mgr.start()
    first = mgr._sweeper_task
    first.cancel()
    try:
        await first
    except asyncio.CancelledError:
        pass
    assert first.done()

    await mgr.start()
    assert mgr._sweeper_task is not first
    assert not mgr._sweeper_task.done()

    mgr._sweeper_task.cancel()
    try:
        await mgr._sweeper_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_stop_cancels_sweeper_and_closes_all():
    factory, registry = _make_registry_class()
    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()
    await mgr.get_or_create_registry("session-1", config_manager)

    await mgr.start()
    assert mgr._sweeper_task is not None

    await mgr.stop()

    assert mgr._sweeper_task is None
    registry.close.assert_called_once()
    assert mgr._entries == {}


@pytest.mark.asyncio
async def test_stop_without_start_is_safe():
    mgr = _make_manager()
    await mgr.stop()


@pytest.mark.asyncio
async def test_stop_absorbs_cancelled_error():
    mgr = _make_manager()
    await mgr.start()
    task = mgr._sweeper_task
    task.cancel()
    await mgr.stop()
    assert mgr._sweeper_task is None


@pytest.mark.asyncio
async def test_stop_logs_non_cancelled_sweeper_task_exception(caplog):
    """If awaiting the cancelled sweeper task raises a non-CancelledError, stop() logs it and continues."""
    mgr = _make_manager()

    async def failing_task():
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            raise RuntimeError("sweeper post-cancel failure")

    mgr._sweeper_task = asyncio.create_task(failing_task())
    await asyncio.sleep(0)  # let the task start

    with caplog.at_level(logging.ERROR):
        await mgr.stop()

    assert mgr._sweeper_task is None
    assert any(
        "Error awaiting cancelled sweeper task" in rec.message for rec in caplog.records
    )


# =============================================================================
# _close_all
# =============================================================================


@pytest.mark.asyncio
async def test_close_all_closes_every_registry():
    factory = MagicMock()
    reg_a = AsyncMock(spec=BaseRegistry)
    reg_a.initialize = AsyncMock()
    reg_a.close = AsyncMock()
    reg_b = AsyncMock(spec=BaseRegistry)
    reg_b.initialize = AsyncMock()
    reg_b.close = AsyncMock()
    factory.side_effect = [reg_a, reg_b]

    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()
    await mgr.get_or_create_registry("session-a", config_manager)
    await mgr.get_or_create_registry("session-b", config_manager)

    await mgr._close_all()

    reg_a.close.assert_called_once()
    reg_b.close.assert_called_once()
    assert mgr._entries == {}


@pytest.mark.asyncio
async def test_close_all_continues_after_individual_close_failure():
    factory = MagicMock()
    reg_a = AsyncMock(spec=BaseRegistry)
    reg_a.initialize = AsyncMock()
    reg_a.close = AsyncMock(side_effect=RuntimeError("fail a"))
    reg_b = AsyncMock(spec=BaseRegistry)
    reg_b.initialize = AsyncMock()
    reg_b.close = AsyncMock()
    factory.side_effect = [reg_a, reg_b]

    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()
    await mgr.get_or_create_registry("session-a", config_manager)
    await mgr.get_or_create_registry("session-b", config_manager)

    await mgr._close_all()

    reg_a.close.assert_called_once()
    reg_b.close.assert_called_once()
    assert mgr._entries == {}


@pytest.mark.asyncio
async def test_close_all_with_no_entries_is_noop():
    mgr = _make_manager()
    await mgr._close_all()
    assert mgr._entries == {}


@pytest.mark.asyncio
async def test_close_all_waits_for_in_flight_init_and_discards():
    """_close_all() must wait for in-flight initializations and discard them."""
    factory, registry = _make_registry_class()

    init_started = asyncio.Event()
    init_gate = asyncio.Event()

    async def blocking_init(*_args, **_kwargs):
        init_started.set()
        await init_gate.wait()

    registry.initialize = AsyncMock(side_effect=blocking_init)
    mgr = _make_manager(registry_class=factory)
    config_manager = MagicMock()

    task_owner = asyncio.create_task(
        mgr.get_or_create_registry("session-1", config_manager)
    )
    await init_started.wait()

    close_all_task = asyncio.create_task(mgr._close_all())
    await asyncio.sleep(0.01)
    assert not close_all_task.done()  # blocked on owner's entry lock

    init_gate.set()
    with pytest.raises(SessionClosedDuringInitError):
        await task_owner
    await close_all_task

    registry.close.assert_called_once()
    assert mgr._entries == {}


# =============================================================================
# _sweep_expired / TTL logic
# =============================================================================


@pytest.mark.asyncio
async def test_sweep_closes_idle_session():
    factory, registry = _make_registry_class()
    mgr = _make_manager(idle_timeout=100.0, registry_class=factory)
    config_manager = MagicMock()

    with patch(
        "deephaven_mcp.mcp_systems_server._session_registry_manager.time"
    ) as mock_time:
        mock_time.monotonic.return_value = 1000.0
        await mgr.get_or_create_registry("session-1", config_manager)

        mock_time.monotonic.return_value = 1200.0
        await mgr._sweep_expired()

    registry.close.assert_called_once()
    assert "session-1" not in mgr._entries


@pytest.mark.asyncio
async def test_sweep_keeps_active_session():
    factory, registry = _make_registry_class()
    mgr = _make_manager(idle_timeout=100.0, registry_class=factory)
    config_manager = MagicMock()

    with patch(
        "deephaven_mcp.mcp_systems_server._session_registry_manager.time"
    ) as mock_time:
        mock_time.monotonic.return_value = 1000.0
        await mgr.get_or_create_registry("session-1", config_manager)

        mock_time.monotonic.return_value = 1050.0
        await mgr._sweep_expired()

    registry.close.assert_not_called()
    assert "session-1" in mgr._entries


@pytest.mark.asyncio
async def test_sweep_toctou_recheck_skips_recently_used():
    """A session used between candidate snapshot and per-entry-lock recheck is not evicted.

    Simulates the TOCTOU window by hooking the per-entry lock so that
    ``last_used`` is bumped to "now" the moment the sweeper acquires the
    entry's lock — modeling another coroutine using the session in that
    window.
    """
    factory, registry = _make_registry_class()
    mgr = _make_manager(idle_timeout=100.0, registry_class=factory)
    config_manager = MagicMock()

    with patch(
        "deephaven_mcp.mcp_systems_server._session_registry_manager.time"
    ) as mock_time:
        mock_time.monotonic.return_value = 1000.0
        await mgr.get_or_create_registry("session-1", config_manager)

        entry = mgr._entries["session-1"]
        # Hook the entry's lock so that the act of acquiring it bumps last_used,
        # simulating a concurrent get_or_create_registry between snapshot and recheck.
        real_lock = entry.lock

        class _BumpingLock:
            async def __aenter__(self):
                entry.last_used = 1201.0
                return await real_lock.__aenter__()

            async def __aexit__(self, *exc):
                return await real_lock.__aexit__(*exc)

        entry.lock = _BumpingLock()

        # Outer "now" = 1200 (looks idle, since last_used=1000 in snapshot);
        # recheck "now" = 1201.5 (last_used was bumped to 1201, fresh -> skip).
        mock_time.monotonic.side_effect = [1200.0, 1201.5]
        await mgr._sweep_expired()

    registry.close.assert_not_called()
    assert "session-1" in mgr._entries
    assert not entry.closed.is_set()


@pytest.mark.asyncio
async def test_sweep_skips_dead_entry():
    """If an entry was marked dead between snapshot and re-check, sweep skips it."""
    factory, registry = _make_registry_class()
    mgr = _make_manager(idle_timeout=100.0, registry_class=factory)
    config_manager = MagicMock()

    with patch(
        "deephaven_mcp.mcp_systems_server._session_registry_manager.time"
    ) as mock_time:
        mock_time.monotonic.return_value = 1000.0
        await mgr.get_or_create_registry("session-1", config_manager)
        mgr._entries["session-1"].closed.set()
        mock_time.monotonic.return_value = 1200.0
        await mgr._sweep_expired()

    registry.close.assert_not_called()


@pytest.mark.asyncio
async def test_sweep_close_failure_logged_not_raised():
    factory, registry = _make_registry_class()
    registry.close.side_effect = RuntimeError("sweep close failed")
    mgr = _make_manager(idle_timeout=100.0, registry_class=factory)
    config_manager = MagicMock()

    with patch(
        "deephaven_mcp.mcp_systems_server._session_registry_manager.time"
    ) as mock_time:
        mock_time.monotonic.return_value = 1000.0
        await mgr.get_or_create_registry("session-1", config_manager)
        mock_time.monotonic.return_value = 1200.0
        await mgr._sweep_expired()

    registry.close.assert_called_once()
    assert "session-1" not in mgr._entries


@pytest.mark.asyncio
async def test_sweep_with_no_entries_is_noop():
    mgr = _make_manager(idle_timeout=100.0)
    await mgr._sweep_expired()  # must not raise
    assert mgr._entries == {}


# =============================================================================
# _sweep_loop
# =============================================================================


@pytest.mark.asyncio
async def test_sweep_loop_runs_repeatedly():
    mgr = _make_manager(sweep_interval=0.01)
    sweep_calls = []

    async def fake_sweep():
        sweep_calls.append(1)

    mgr._sweep_expired = fake_sweep

    task = asyncio.create_task(mgr._sweep_loop())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(sweep_calls) >= 2


@pytest.mark.asyncio
async def test_sweep_loop_continues_after_unexpected_exception():
    mgr = _make_manager(sweep_interval=0.01)
    sweep_calls = []

    async def flaky_sweep():
        sweep_calls.append(1)
        if len(sweep_calls) == 1:
            raise RuntimeError("transient")

    mgr._sweep_expired = flaky_sweep

    task = asyncio.create_task(mgr._sweep_loop())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(sweep_calls) >= 2
