"""Tests for the per-registry :class:`Evictor` sweep coordinator."""

import asyncio
import logging
import time
from typing import ClassVar, override
from unittest.mock import AsyncMock

import pytest

from deephaven_mcp._exceptions import InternalError
from deephaven_mcp.resource_manager import EvictionTimeouts, Evictor
from deephaven_mcp.resource_manager._manager import (
    BaseItemManager,
    ResourceLivenessStatus,
    SystemType,
)
from deephaven_mcp.resource_manager._registry import (
    BaseRegistry,
    MutableSessionRegistry,
)
from deephaven_mcp.resource_manager._session_id import SessionId


class _StubItem:
    def __init__(self):
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _session_id_for(name: str) -> SessionId:
    """Return a stable :class:`SessionId` derived from ``name``.

    Community convention: the SessionId is just the session name, so
    ``qualified_session_id`` is stable per ``name`` and distinct across distinct
    names — exactly what these tests need.
    """
    return SessionId(name)


class _StubStaticManager(BaseItemManager[_StubItem]):
    """``evicts_on_idle = False``."""

    def __init__(self, name: str):
        super().__init__(
            SystemType.COMMUNITY,
            "test",
            _session_id_for(name),
            name,
        )
        self.create_count = 0

    @override
    async def _create_item(self) -> _StubItem:
        self.create_count += 1
        return _StubItem()

    @override
    async def _check_liveness(self, item: _StubItem):
        return (ResourceLivenessStatus.ONLINE, None)


class _StubDynamicManager(_StubStaticManager):
    """``evicts_on_idle = True``."""

    evicts_on_idle: ClassVar[bool] = True


class _StubRegistry(BaseRegistry[_StubItem]):
    """BaseRegistry subclass that lets tests pre-populate ``_items``."""

    @override
    async def _load_items(self) -> None:
        return None


class _StubMutableRegistry(MutableSessionRegistry):
    @override
    async def _load_items(self) -> None:
        return None


# ---------------------------------------------------------------------------
# _sweep_once
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sweep_once_skips_fresh_items():
    reg = _StubRegistry()
    await reg.initialize()
    try:
        mgr = _StubStaticManager("fresh")
        reg._items[mgr.qualified_session_id] = mgr
        await mgr.get()

        evictor = Evictor(reg, EvictionTimeouts(session_idle_timeout_seconds=3600.0))
        await evictor._sweep_once()
        assert mgr._item_cache is not None
        assert mgr.qualified_session_id in reg._items
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_closes_idle_keep_in_registry():
    reg = _StubRegistry()
    await reg.initialize()
    try:
        mgr = _StubStaticManager("idle")
        reg._items[mgr.qualified_session_id] = mgr
        await mgr.get()
        mgr._last_accessed = time.monotonic() - 1000.0

        evictor = Evictor(reg, EvictionTimeouts(session_idle_timeout_seconds=0.01))
        await evictor._sweep_once()
        assert mgr._item_cache is None
        # Non-evicting items stay in the registry for lazy reconnect.
        assert mgr.qualified_session_id in reg._items
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_removes_evicts_on_idle():
    reg = _StubMutableRegistry()
    await reg.initialize()
    try:
        mgr = _StubDynamicManager("dyn")
        reg._items[mgr.qualified_session_id] = mgr
        reg._added_session_ids.add(mgr.qualified_session_id)
        await mgr.get()
        mgr._last_accessed = time.monotonic() - 1000.0

        evictor = Evictor(reg, EvictionTimeouts(session_idle_timeout_seconds=0.01))
        await evictor._sweep_once()
        assert mgr.qualified_session_id not in reg._items
        # Added-session tracking is freed too (via _on_removed hook).
        assert mgr.qualified_session_id not in reg._added_session_ids
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_identity_checked_drop_protects_same_key_re_add():
    """A new manager added with the same key during the sweep survives."""
    reg = _StubMutableRegistry()
    await reg.initialize()
    try:
        old = _StubDynamicManager("dyn")
        reg._items[old.qualified_session_id] = old
        reg._added_session_ids.add(old.qualified_session_id)
        await old.get()
        old._last_accessed = time.monotonic() - 1000.0

        # Simulate a same-key re-add between the snapshot and the drop:
        # we close `old` outside the registry (via maybe_close_if_idle),
        # then swap in a fresh manager under the same key, then call
        # remove(name, expected=old). The identity check should leave the
        # fresh manager in place.
        snapshot = await reg.get_all()
        closed_any = False
        for key, mgr in snapshot.items.items():
            closed = await mgr.maybe_close_if_idle(0.01, time.monotonic())
            if closed and mgr.evicts_on_idle:
                closed_any = True
        assert closed_any

        # Re-add under same key with a fresh identity.
        fresh = _StubDynamicManager("dyn")
        reg._items[fresh.qualified_session_id] = fresh
        reg._added_session_ids.add(fresh.qualified_session_id)

        # Now drop using the OLD identity — should NOT remove the fresh one.
        removed = await reg.remove(old.qualified_session_id, expected=old)
        assert removed is None  # identity check failed, nothing removed
        assert reg._items.get(fresh.qualified_session_id) is fresh
        assert fresh.qualified_session_id in reg._added_session_ids
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_logs_and_continues_on_per_item_error(caplog):
    reg = _StubRegistry()
    await reg.initialize()
    try:
        raising = _StubStaticManager("raises")
        ok = _StubStaticManager("ok")
        reg._items[raising.qualified_session_id] = raising
        reg._items[ok.qualified_session_id] = ok
        await raising.get()
        await ok.get()
        raising._last_accessed = time.monotonic() - 1000.0
        ok._last_accessed = time.monotonic() - 1000.0

        async def _boom(*args, **kwargs):
            raise RuntimeError("boom")

        raising.maybe_close_if_idle = _boom  # type: ignore[method-assign]

        evictor = Evictor(reg, EvictionTimeouts(session_idle_timeout_seconds=0.01))
        with caplog.at_level(
            logging.ERROR, logger="deephaven_mcp.resource_manager._evictor"
        ):
            await evictor._sweep_once()
        # Error in one item doesn't block the next.
        assert ok._item_cache is None
        # And the failure was logged at ERROR via logger.exception(...).
        assert any(
            "eviction failed" in r.message and r.levelno == logging.ERROR
            for r in caplog.records
        )
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_skips_non_base_item_manager(caplog):
    """Items in the registry that aren't BaseItemManager are skipped with a WARNING."""

    class _BareClosable:
        """An ``AsyncClosable`` that isn't a ``BaseItemManager``."""

        def __init__(self):
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    reg = _StubRegistry()
    await reg.initialize()
    try:
        bare = _BareClosable()
        # Bypass typing — registry's generic only requires AsyncClosable.
        reg._items["bare:item"] = bare  # type: ignore[assignment]

        evictor = Evictor(reg, EvictionTimeouts(session_idle_timeout_seconds=0.01))
        with caplog.at_level(
            logging.WARNING, logger="deephaven_mcp.resource_manager._evictor"
        ):
            await evictor._sweep_once()
        # Item must not be touched.
        assert bare.closed is False
        assert "bare:item" in reg._items
        # And the skip was logged.
        assert any(
            "not a BaseItemManager" in r.message and r.levelno == logging.WARNING
            for r in caplog.records
        )
    finally:
        # Remove the non-closable item before reg.close() to avoid type errors.
        reg._items.pop("bare:item", None)
        await reg.close()


# ---------------------------------------------------------------------------
# Evictor.start / Evictor.stop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_launches_task_when_timeout_set():
    reg = _StubRegistry()
    await reg.initialize()
    try:
        evictor = Evictor(
            reg,
            EvictionTimeouts(
                session_idle_timeout_seconds=3600.0, sweep_interval_seconds=60.0
            ),
        )
        await evictor.start()
        assert evictor._sweeper_task is not None
        assert not evictor._sweeper_task.done()
        await evictor.stop()
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_start_skips_task_when_timeout_none():
    reg = _StubRegistry()
    await reg.initialize()
    try:
        evictor = Evictor(reg, None)
        await evictor.start()
        assert evictor._sweeper_task is None
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_start_uses_default_sweep_interval_when_only_idle_specified():
    """Providing ``EvictionTimeouts`` always enables the sweeper.

    Under the new typed API there is no "one knob set, the other not"
    state — ``EvictionTimeouts`` validates both fields. The disabled
    state is encoded as ``Evictor(reg, None)`` only (covered above).
    """
    reg = _StubRegistry()
    await reg.initialize()
    try:
        evictor = Evictor(reg, EvictionTimeouts(session_idle_timeout_seconds=3600.0))
        await evictor.start()
        assert evictor._sweeper_task is not None
    finally:
        await evictor.stop()
        await reg.close()


@pytest.mark.asyncio
async def test_stop_cancels_task():
    reg = _StubRegistry()
    await reg.initialize()
    try:
        evictor = Evictor(
            reg,
            EvictionTimeouts(
                session_idle_timeout_seconds=3600.0, sweep_interval_seconds=60.0
            ),
        )
        await evictor.start()
        task = evictor._sweeper_task
        assert task is not None
        await evictor.stop()
        # asyncio's API spells it Task.cancelled().  # codespell:ignore cancelled
        assert task.cancelled() or task.done()  # codespell:ignore cancelled
        assert evictor._sweeper_task is None
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_stop_is_idempotent_without_prior_start():
    reg = _StubRegistry()
    await reg.initialize()
    try:
        evictor = Evictor(reg, EvictionTimeouts(session_idle_timeout_seconds=3600.0))
        # No start; stop should be a no-op.
        await evictor.stop()
        await evictor.stop()  # second call also fine
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_raises_internal_error_when_timeouts_none():
    """Direct invocation of _sweep_once with timeouts=None must raise.

    Invariant: start() refuses to launch the sweep loop unless both timing
    params are non-None.  If a caller bypasses start() and invokes the
    private sweep method directly on a disabled Evictor, that's an internal
    bug — raise InternalError rather than silently no-op'ing or asserting.
    """
    reg = _StubRegistry()
    await reg.initialize()
    try:
        evictor = Evictor(reg, None)
        with pytest.raises(InternalError, match="_timeouts is None"):
            await evictor._sweep_once()
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_loop_raises_internal_error_when_timeouts_none():
    """Direct invocation of _sweep_loop with timeouts=None must raise.

    Same rationale as the _sweep_once test: bypassing start() to invoke the
    sweep loop on a disabled Evictor is an internal bug.
    """
    reg = _StubRegistry()
    await reg.initialize()
    try:
        evictor = Evictor(reg, None)
        with pytest.raises(InternalError, match="_timeouts is None"):
            await evictor._sweep_loop()
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_start_then_stop_then_start_again():
    """A sweeper can be restarted after a stop."""
    reg = _StubRegistry()
    await reg.initialize()
    try:
        evictor = Evictor(
            reg,
            EvictionTimeouts(
                session_idle_timeout_seconds=3600.0, sweep_interval_seconds=60.0
            ),
        )
        await evictor.start()
        await evictor.stop()
        assert evictor._sweeper_task is None
        await evictor.start()
        assert evictor._sweeper_task is not None
        assert not evictor._sweeper_task.done()
        await evictor.stop()
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_start_is_idempotent_while_running():
    """Calling ``start()`` again while a sweeper task is alive is a no-op."""
    reg = _StubRegistry()
    await reg.initialize()
    try:
        evictor = Evictor(
            reg,
            EvictionTimeouts(
                session_idle_timeout_seconds=3600.0, sweep_interval_seconds=60.0
            ),
        )
        await evictor.start()
        task = evictor._sweeper_task
        assert task is not None and not task.done()
        # Second start() must not replace the live task.
        await evictor.start()
        assert evictor._sweeper_task is task
        await evictor.stop()
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_stop_with_already_finished_task():
    """``stop()`` is a no-op when the sweeper task has already finished."""
    reg = _StubRegistry()
    await reg.initialize()
    try:
        evictor = Evictor(
            reg,
            EvictionTimeouts(
                session_idle_timeout_seconds=3600.0, sweep_interval_seconds=60.0
            ),
        )
        # Install a pre-finished task directly so we hit the ``task.done()``
        # branch in stop() without racing the real sweep loop.

        async def _noop() -> None:
            return None

        evictor._sweeper_task = asyncio.create_task(_noop())
        # Let the task complete.
        await evictor._sweeper_task
        assert evictor._sweeper_task.done()
        await evictor.stop()  # must not raise
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_loop_continues_after_sweep_once_raises(caplog):
    """When ``_sweep_once`` raises, the loop swallows it and keeps sweeping."""
    reg = _StubRegistry()
    await reg.initialize()
    try:
        evictor = Evictor(
            reg,
            EvictionTimeouts(
                session_idle_timeout_seconds=3600.0, sweep_interval_seconds=0.01
            ),
        )

        call_count = 0
        success_event = asyncio.Event()

        async def _fake_sweep_once():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("first sweep boom")
            # Subsequent iterations succeed; signal we got past the failure.
            success_event.set()

        evictor._sweep_once = _fake_sweep_once  # type: ignore[method-assign]

        with caplog.at_level(
            logging.ERROR, logger="deephaven_mcp.resource_manager._evictor"
        ):
            await evictor.start()
            # The first sweep raises; the loop must continue and run again.
            await asyncio.wait_for(success_event.wait(), timeout=2.0)
            await evictor.stop()

        assert call_count >= 2
        assert any(
            "sweep failed; continuing" in r.message and r.levelno == logging.ERROR
            for r in caplog.records
        )
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_stop_logs_when_task_raises_non_cancelled_error(caplog):
    """If the sweeper task raises a non-CancelledError on cancel, ``stop`` logs it.

    Covers the defensive ``except Exception`` branch in :meth:`Evictor.stop`
    that exists to surface a buggy sweep loop (one that swallows the
    cancellation and re-raises something else).
    """
    reg = _StubRegistry()
    await reg.initialize()
    try:
        evictor = Evictor(
            reg,
            EvictionTimeouts(
                session_idle_timeout_seconds=3600.0, sweep_interval_seconds=60.0
            ),
        )

        async def _bad_sweeper() -> None:
            try:
                await asyncio.sleep(60.0)
            except asyncio.CancelledError:
                # Misbehaving loop: convert cancellation to a different error.
                raise RuntimeError("sweep loop bug") from None

        evictor._sweeper_task = asyncio.create_task(_bad_sweeper())
        # Yield once so the task actually starts before we cancel it.
        await asyncio.sleep(0)

        with caplog.at_level(
            logging.ERROR, logger="deephaven_mcp.resource_manager._evictor"
        ):
            await evictor.stop()

        assert any(
            "non-CancelledError" in r.message and r.levelno == logging.ERROR
            for r in caplog.records
        )
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_logs_and_continues_on_registry_remove_error(caplog):
    """When ``registry.remove`` raises, the sweep logs and continues."""
    reg = _StubMutableRegistry()
    await reg.initialize()
    try:
        mgr = _StubDynamicManager("dyn-remove-raises")
        reg._items[mgr.qualified_session_id] = mgr
        reg._added_session_ids.add(mgr.qualified_session_id)
        await mgr.get()
        mgr._last_accessed = time.monotonic() - 1000.0

        async def _boom(*_args, **_kwargs):
            raise RuntimeError("remove boom")

        reg.remove = _boom  # type: ignore[method-assign]

        evictor = Evictor(reg, EvictionTimeouts(session_idle_timeout_seconds=0.01))
        with caplog.at_level(
            logging.ERROR, logger="deephaven_mcp.resource_manager._evictor"
        ):
            await evictor._sweep_once()

        assert any(
            "removal failed" in r.message and r.levelno == logging.ERROR
            for r in caplog.records
        )
    finally:
        # Restore for clean teardown.
        reg._items.pop(mgr.qualified_session_id, None)
        reg._added_session_ids.discard(mgr.qualified_session_id)
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_logs_at_debug_when_registry_remove_returns_none(caplog):
    """When ``registry.remove`` returns ``None`` (concurrent replace), DEBUG logs the race."""
    reg = _StubMutableRegistry()
    await reg.initialize()
    try:
        mgr = _StubDynamicManager("dyn-remove-none")
        reg._items[mgr.qualified_session_id] = mgr
        reg._added_session_ids.add(mgr.qualified_session_id)
        await mgr.get()
        mgr._last_accessed = time.monotonic() - 1000.0

        async def _returns_none(*_args, **_kwargs):
            return None

        reg.remove = _returns_none  # type: ignore[method-assign]

        evictor = Evictor(reg, EvictionTimeouts(session_idle_timeout_seconds=0.01))
        with caplog.at_level(
            logging.DEBUG, logger="deephaven_mcp.resource_manager._evictor"
        ):
            await evictor._sweep_once()

        assert any(
            "no longer the evicted manager" in r.message and r.levelno == logging.DEBUG
            for r in caplog.records
        )
    finally:
        reg._items.pop(mgr.qualified_session_id, None)
        reg._added_session_ids.discard(mgr.qualified_session_id)
        await reg.close()


@pytest.mark.asyncio
async def test_base_registry_on_removed_default_is_no_op():
    """Removing an item from a base ``BaseRegistry`` invokes the default no-op hook.

    Subclasses that maintain extra tracking (e.g.
    :class:`MutableSessionRegistry`) override ``_on_removed``. Plain
    ``BaseRegistry`` callers must hit the default body, which simply
    returns ``None``.
    """
    reg = _StubRegistry()
    await reg.initialize()
    try:
        mgr = _StubStaticManager("plain")
        reg._items[mgr.qualified_session_id] = mgr
        removed = await reg.remove(mgr.qualified_session_id)
        assert removed is mgr
        assert mgr.qualified_session_id not in reg._items
    finally:
        await reg.close()


# ---------------------------------------------------------------------------
# H2 regression: Evictor uses snapshot_items (cheap path), never get_all
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sweep_once_uses_snapshot_items_not_get_all():
    """The sweep loop must use :meth:`BaseRegistry.snapshot_items` so that
    enterprise registries are not forced into a controller refresh on every
    eviction cadence.
    """
    reg = _StubRegistry()
    await reg.initialize()
    try:
        mgr = _StubStaticManager("idle-snap")
        reg._items[mgr.qualified_session_id] = mgr
        await mgr.get()
        mgr._last_accessed = time.monotonic() - 1000.0

        snapshot_calls = 0
        get_all_calls = 0
        original_snapshot = reg.snapshot_items
        original_get_all = reg.get_all

        async def _counting_snapshot():
            nonlocal snapshot_calls
            snapshot_calls += 1
            return await original_snapshot()

        async def _counting_get_all():
            nonlocal get_all_calls
            get_all_calls += 1
            return await original_get_all()

        reg.snapshot_items = _counting_snapshot  # type: ignore[method-assign]
        reg.get_all = _counting_get_all  # type: ignore[method-assign]

        evictor = Evictor(reg, EvictionTimeouts(session_idle_timeout_seconds=0.01))
        await evictor._sweep_once()

        assert snapshot_calls == 1
        assert get_all_calls == 0
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_does_not_invoke_enterprise_refresh_path():
    """A stub registry whose ``get_all`` triggers a side effect must not have
    that side effect invoked during eviction sweeps.  Mimics the enterprise
    behavior where ``get_all`` performs a network refresh.
    """
    reg = _StubRegistry()
    await reg.initialize()
    try:
        mgr = _StubStaticManager("idle-sync")
        reg._items[mgr.qualified_session_id] = mgr
        await mgr.get()
        mgr._last_accessed = time.monotonic() - 1000.0

        side_effect_count = 0

        async def _get_all_with_side_effect():
            nonlocal side_effect_count
            side_effect_count += 1
            # Simulate the enterprise refresh side effect.
            return await BaseRegistry.get_all(reg)

        reg.get_all = _get_all_with_side_effect  # type: ignore[method-assign]

        evictor = Evictor(reg, EvictionTimeouts(session_idle_timeout_seconds=0.01))
        await evictor._sweep_once()

        # The sweep must rely on snapshot_items, not get_all.
        assert side_effect_count == 0
    finally:
        await reg.close()
