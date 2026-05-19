"""Tests for the per-registry :class:`Evictor` sweep coordinator."""

import asyncio
import logging
import time
from typing import ClassVar, override
from unittest.mock import AsyncMock

import pytest

from deephaven_mcp._exceptions import InternalError
from deephaven_mcp.resource_manager import Evictor
from deephaven_mcp.resource_manager._manager import (
    BaseItemManager,
    ResourceLivenessStatus,
    SystemType,
)
from deephaven_mcp.resource_manager._registry import (
    BaseRegistry,
    MutableSessionRegistry,
)


class _StubItem:
    def __init__(self):
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _StubStaticManager(BaseItemManager[_StubItem]):
    """``evicts_on_idle = False``."""

    def __init__(self, name: str):
        super().__init__(SystemType.COMMUNITY, "test", name)
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
    async def _load_items(self, config_manager) -> None:
        return None


class _StubMutableRegistry(MutableSessionRegistry):
    @override
    async def _load_items(self, config_manager) -> None:
        return None


# ---------------------------------------------------------------------------
# _sweep_once
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sweep_once_skips_fresh_items():
    reg = _StubRegistry()
    await reg.initialize(config_manager=AsyncMock())
    try:
        mgr = _StubStaticManager("fresh")
        reg._items[mgr.full_name] = mgr
        await mgr.get()

        evictor = Evictor(reg, idle_timeout_seconds=3600.0)
        await evictor._sweep_once()
        assert mgr._item_cache is not None
        assert mgr.full_name in reg._items
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_closes_idle_keep_in_registry():
    reg = _StubRegistry()
    await reg.initialize(config_manager=AsyncMock())
    try:
        mgr = _StubStaticManager("idle")
        reg._items[mgr.full_name] = mgr
        await mgr.get()
        mgr._last_accessed = time.monotonic() - 1000.0

        evictor = Evictor(reg, idle_timeout_seconds=0.01)
        await evictor._sweep_once()
        assert mgr._item_cache is None
        # Non-evicting items stay in the registry for lazy reconnect.
        assert mgr.full_name in reg._items
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_removes_evicts_on_idle():
    reg = _StubMutableRegistry()
    await reg.initialize(config_manager=AsyncMock())
    try:
        mgr = _StubDynamicManager("dyn")
        reg._items[mgr.full_name] = mgr
        reg._added_session_ids.add(mgr.full_name)
        await mgr.get()
        mgr._last_accessed = time.monotonic() - 1000.0

        evictor = Evictor(reg, idle_timeout_seconds=0.01)
        await evictor._sweep_once()
        assert mgr.full_name not in reg._items
        # Added-session tracking is freed too (via _on_removed hook).
        assert mgr.full_name not in reg._added_session_ids
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_identity_checked_drop_protects_same_key_re_add():
    """A new manager added with the same key during the sweep survives."""
    reg = _StubMutableRegistry()
    await reg.initialize(config_manager=AsyncMock())
    try:
        old = _StubDynamicManager("dyn")
        reg._items[old.full_name] = old
        reg._added_session_ids.add(old.full_name)
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
        reg._items[fresh.full_name] = fresh
        reg._added_session_ids.add(fresh.full_name)

        # Now drop using the OLD identity — should NOT remove the fresh one.
        removed = await reg.remove(old.full_name, expected=old)
        assert removed is None  # identity check failed, nothing removed
        assert reg._items.get(fresh.full_name) is fresh
        assert fresh.full_name in reg._added_session_ids
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_logs_and_continues_on_per_item_error(caplog):
    reg = _StubRegistry()
    await reg.initialize(config_manager=AsyncMock())
    try:
        raising = _StubStaticManager("raises")
        ok = _StubStaticManager("ok")
        reg._items[raising.full_name] = raising
        reg._items[ok.full_name] = ok
        await raising.get()
        await ok.get()
        raising._last_accessed = time.monotonic() - 1000.0
        ok._last_accessed = time.monotonic() - 1000.0

        async def _boom(*args, **kwargs):
            raise RuntimeError("boom")

        raising.maybe_close_if_idle = _boom  # type: ignore[method-assign]

        evictor = Evictor(reg, idle_timeout_seconds=0.01)
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
    await reg.initialize(config_manager=AsyncMock())
    try:
        bare = _BareClosable()
        # Bypass typing — registry's generic only requires AsyncClosable.
        reg._items["bare:item"] = bare  # type: ignore[assignment]

        evictor = Evictor(reg, idle_timeout_seconds=0.01)
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
    await reg.initialize(config_manager=AsyncMock())
    try:
        evictor = Evictor(reg, idle_timeout_seconds=3600.0, sweep_interval_seconds=60.0)
        await evictor.start()
        assert evictor._sweeper_task is not None
        assert not evictor._sweeper_task.done()
        await evictor.stop()
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_start_skips_task_when_timeout_none():
    reg = _StubRegistry()
    await reg.initialize(config_manager=AsyncMock())
    try:
        evictor = Evictor(reg, idle_timeout_seconds=None)
        await evictor.start()
        assert evictor._sweeper_task is None
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_start_skips_task_when_sweep_interval_none():
    """``sweep_interval_seconds=None`` disables the sweeper, same as the timeout."""
    reg = _StubRegistry()
    await reg.initialize(config_manager=AsyncMock())
    try:
        evictor = Evictor(reg, idle_timeout_seconds=3600.0)
        # sweep_interval_seconds defaults to None — start() must return early.
        await evictor.start()
        assert evictor._sweeper_task is None
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_stop_cancels_task():
    reg = _StubRegistry()
    await reg.initialize(config_manager=AsyncMock())
    try:
        evictor = Evictor(reg, idle_timeout_seconds=3600.0, sweep_interval_seconds=60.0)
        await evictor.start()
        task = evictor._sweeper_task
        assert task is not None
        await evictor.stop()
        assert task.cancelled() or task.done()
        assert evictor._sweeper_task is None
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_stop_is_idempotent_without_prior_start():
    reg = _StubRegistry()
    await reg.initialize(config_manager=AsyncMock())
    try:
        evictor = Evictor(reg, idle_timeout_seconds=3600.0)
        # No start; stop should be a no-op.
        await evictor.stop()
        await evictor.stop()  # second call also fine
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_raises_internal_error_when_idle_timeout_none():
    """Direct invocation of _sweep_once with idle_timeout=None must raise.

    Invariant: start() refuses to launch the sweep loop unless both timing
    params are non-None.  If a caller bypasses start() and invokes the
    private sweep method directly on a disabled Evictor, that's an internal
    bug — raise InternalError rather than silently no-op'ing or asserting.
    """
    reg = _StubRegistry()
    await reg.initialize(config_manager=AsyncMock())
    try:
        evictor = Evictor(reg, idle_timeout_seconds=None)
        with pytest.raises(InternalError, match="_idle_timeout is None"):
            await evictor._sweep_once()
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_loop_raises_internal_error_when_sweep_interval_none():
    """Direct invocation of _sweep_loop with sweep_interval=None must raise.

    Same rationale as the _sweep_once test: bypassing start() to invoke the
    sweep loop on a disabled Evictor is an internal bug.
    """
    reg = _StubRegistry()
    await reg.initialize(config_manager=AsyncMock())
    try:
        evictor = Evictor(reg, idle_timeout_seconds=3600.0)
        # sweep_interval_seconds defaults to None.
        with pytest.raises(InternalError, match="_sweep_interval is None"):
            await evictor._sweep_loop()
    finally:
        await reg.close()


@pytest.mark.asyncio
async def test_start_then_stop_then_start_again():
    """A sweeper can be restarted after a stop (used by mcp_reload)."""
    reg = _StubRegistry()
    await reg.initialize(config_manager=AsyncMock())
    try:
        evictor = Evictor(reg, idle_timeout_seconds=3600.0, sweep_interval_seconds=60.0)
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
    await reg.initialize(config_manager=AsyncMock())
    try:
        evictor = Evictor(reg, idle_timeout_seconds=3600.0, sweep_interval_seconds=60.0)
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
    await reg.initialize(config_manager=AsyncMock())
    try:
        evictor = Evictor(reg, idle_timeout_seconds=3600.0, sweep_interval_seconds=60.0)
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
    await reg.initialize(config_manager=AsyncMock())
    try:
        evictor = Evictor(reg, idle_timeout_seconds=3600.0, sweep_interval_seconds=0.01)

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
    await reg.initialize(config_manager=AsyncMock())
    try:
        evictor = Evictor(reg, idle_timeout_seconds=3600.0, sweep_interval_seconds=60.0)

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
    await reg.initialize(config_manager=AsyncMock())
    try:
        mgr = _StubDynamicManager("dyn-remove-raises")
        reg._items[mgr.full_name] = mgr
        reg._added_session_ids.add(mgr.full_name)
        await mgr.get()
        mgr._last_accessed = time.monotonic() - 1000.0

        async def _boom(*_args, **_kwargs):
            raise RuntimeError("remove boom")

        reg.remove = _boom  # type: ignore[method-assign]

        evictor = Evictor(reg, idle_timeout_seconds=0.01)
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
        reg._items.pop("community:test:dyn-remove-raises", None)
        reg._added_session_ids.discard("community:test:dyn-remove-raises")
        await reg.close()


@pytest.mark.asyncio
async def test_sweep_once_logs_at_debug_when_registry_remove_returns_none(caplog):
    """When ``registry.remove`` returns ``None`` (concurrent replace), DEBUG logs the race."""
    reg = _StubMutableRegistry()
    await reg.initialize(config_manager=AsyncMock())
    try:
        mgr = _StubDynamicManager("dyn-remove-none")
        reg._items[mgr.full_name] = mgr
        reg._added_session_ids.add(mgr.full_name)
        await mgr.get()
        mgr._last_accessed = time.monotonic() - 1000.0

        async def _returns_none(*_args, **_kwargs):
            return None

        reg.remove = _returns_none  # type: ignore[method-assign]

        evictor = Evictor(reg, idle_timeout_seconds=0.01)
        with caplog.at_level(
            logging.DEBUG, logger="deephaven_mcp.resource_manager._evictor"
        ):
            await evictor._sweep_once()

        assert any(
            "no longer the evicted manager" in r.message and r.levelno == logging.DEBUG
            for r in caplog.records
        )
    finally:
        reg._items.pop("community:test:dyn-remove-none", None)
        reg._added_session_ids.discard("community:test:dyn-remove-none")
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
    await reg.initialize(config_manager=AsyncMock())
    try:
        mgr = _StubStaticManager("plain")
        reg._items[mgr.full_name] = mgr
        removed = await reg.remove(mgr.full_name)
        assert removed is mgr
        assert mgr.full_name not in reg._items
    finally:
        await reg.close()
