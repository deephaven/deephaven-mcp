"""Tests for ``deephaven_mcp.mcp_systems_server._evictors``.

The :class:`EvictorPool` owns the per-child evictor lifecycle: atomic
startup with rollback, concurrent shutdown with per-evictor error
isolation. Tests exercise the explicit ``start()`` / ``stop()``
methods directly; the ``_running_pool`` helper below pairs them in
the success path so individual tests stay focused on the behavior
they assert on.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deephaven_mcp.mcp_systems_server import _evictors as evictors_module
from deephaven_mcp.mcp_systems_server._evictors import EvictorPool

# ---------------------------------------------------------------------------
# Test helper: pair start() with stop() on the success path.
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _running_pool(*args: object, **kwargs: object) -> AsyncIterator[EvictorPool]:
    """Construct a pool, ``start()`` it, yield it, ``stop()`` on exit.

    Mirrors the lifespan's start/push_async_callback(stop) pairing in
    a form convenient for tests. The failure-path tests bypass this
    helper and call ``start()`` directly inside ``pytest.raises(...)``.
    """
    pool = EvictorPool(*args, **kwargs)  # type: ignore[arg-type]
    await pool.start()
    try:
        yield pool
    finally:
        await pool.stop()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_multi_config(
    *,
    community_idle: float | None = None,
    community_sweep: float | None = None,
    enterprise_systems: list[str] | None = None,
    enterprise_idle: float | None = None,
    enterprise_sweep: float | None = None,
) -> MagicMock:
    """Build a ``ConfigTree`` mock with the given timer values.

    Idle/sweep timers live on the umbrella ``settings`` block (community
    or enterprise) and apply uniformly to every child system within
    that side.
    """
    mc = MagicMock()
    if community_idle is None and community_sweep is None:
        mc.community = None
    else:
        community = MagicMock()
        community.settings = MagicMock()
        community.settings.timeouts.eviction.session_idle_timeout_seconds = (
            community_idle if community_idle is not None else 3600.0
        )
        community.settings.timeouts.eviction.sweep_interval_seconds = (
            community_sweep if community_sweep is not None else 60.0
        )
        mc.community = community
    if enterprise_systems:
        enterprise = MagicMock()
        enterprise.settings = MagicMock()
        enterprise.settings.timeouts.eviction.session_idle_timeout_seconds = (
            enterprise_idle if enterprise_idle is not None else 3600.0
        )
        enterprise.settings.timeouts.eviction.sweep_interval_seconds = (
            enterprise_sweep if enterprise_sweep is not None else 60.0
        )
        enterprise.systems = {name: MagicMock() for name in enterprise_systems}
        mc.enterprise = enterprise
    else:
        mc.enterprise = None
    return mc


def _make_registry(
    *,
    community: AsyncMock | None,
    enterprise: dict[str, AsyncMock] | None = None,
) -> MagicMock:
    reg = MagicMock()
    reg.community = community
    reg.enterprise_systems = dict(enterprise or {})
    return reg


# ---------------------------------------------------------------------------
# Startup behavior
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_empty_when_no_children():
    """No community + no enterprise = no evictors built or started."""
    mc = _make_multi_config()
    registry = _make_registry(community=None)
    started: list[tuple[float, float]] = []

    def _factory(child, timeouts):
        started.append(
            (timeouts.session_idle_timeout_seconds, timeouts.sweep_interval_seconds)
        )
        ev = AsyncMock()
        ev.start = AsyncMock()
        return ev

    with patch.object(evictors_module, "Evictor", side_effect=_factory):
        async with _running_pool(registry, mc) as pool:
            assert pool.evictors == []
    assert started == []


@pytest.mark.asyncio
async def test_pool_skips_community_when_config_absent_but_registry_present():
    """A community child without matching config yields no community evictor.

    ``_eviction_targets`` requires *both* the registry child and the
    config section to be present; a registry/config mismatch is
    silently skipped rather than building an evictor with no timers.
    """
    mc = _make_multi_config()  # mc.community is None
    community_child = AsyncMock()
    registry = _make_registry(community=community_child)
    started: list[object] = []

    def _factory(child, timeouts):
        started.append(child)
        ev = AsyncMock()
        ev.start = AsyncMock()
        return ev

    with patch.object(evictors_module, "Evictor", side_effect=_factory):
        async with _running_pool(registry, mc) as pool:
            assert pool.evictors == []
    assert started == []


@pytest.mark.asyncio
async def test_pool_uses_per_section_timers():
    """Each child receives an Evictor parameterized by its umbrella settings."""
    mc = _make_multi_config(
        community_idle=100,
        community_sweep=10,
        enterprise_systems=["prod", "dev"],
        enterprise_idle=300,
        enterprise_sweep=5,
    )
    community_child = AsyncMock()
    prod_child = AsyncMock()
    dev_child = AsyncMock()
    registry = _make_registry(
        community=community_child,
        enterprise={"prod": prod_child, "dev": dev_child},
    )

    captured: list[tuple[object, float, float]] = []

    def _factory(child, timeouts):
        captured.append(
            (
                child,
                timeouts.session_idle_timeout_seconds,
                timeouts.sweep_interval_seconds,
            )
        )
        ev = AsyncMock()
        ev.start = AsyncMock()
        ev.stop = AsyncMock()
        return ev

    with patch.object(evictors_module, "Evictor", side_effect=_factory):
        async with _running_pool(registry, mc) as pool:
            assert len(pool.evictors) == 3
            for ev in pool.evictors:
                ev.start.assert_awaited_once()

    # Community evictor is built first; its timers come from community settings.
    assert captured[0] == (community_child, 100, 10)
    # Enterprise evictors follow, sharing the system-wide (idle, sweep).
    enterprise_calls = {child: (idle, sweep) for child, idle, sweep in captured[1:]}
    assert enterprise_calls[prod_child] == (300, 5)
    assert enterprise_calls[dev_child] == (300, 5)


@pytest.mark.asyncio
async def test_pool_community_only():
    mc = _make_multi_config(community_idle=42, community_sweep=7)
    community_child = AsyncMock()
    registry = _make_registry(community=community_child)
    captured: list[tuple[float, float]] = []

    def _factory(child, timeouts):
        captured.append(
            (timeouts.session_idle_timeout_seconds, timeouts.sweep_interval_seconds)
        )
        ev = AsyncMock()
        ev.start = AsyncMock()
        ev.stop = AsyncMock()
        return ev

    with patch.object(evictors_module, "Evictor", side_effect=_factory):
        async with _running_pool(registry, mc) as pool:
            assert len(pool.evictors) == 1
    assert captured == [(42, 7)]


@pytest.mark.asyncio
async def test_pool_enterprise_only():
    mc = _make_multi_config(
        enterprise_systems=["only"], enterprise_idle=15, enterprise_sweep=3
    )
    only_child = AsyncMock()
    registry = _make_registry(community=None, enterprise={"only": only_child})
    captured: list[tuple[float, float]] = []

    def _factory(child, timeouts):
        captured.append(
            (timeouts.session_idle_timeout_seconds, timeouts.sweep_interval_seconds)
        )
        ev = AsyncMock()
        ev.start = AsyncMock()
        ev.stop = AsyncMock()
        return ev

    with patch.object(evictors_module, "Evictor", side_effect=_factory):
        async with _running_pool(registry, mc) as pool:
            assert len(pool.evictors) == 1
    assert captured == [(15, 3)]


@pytest.mark.asyncio
async def test_pool_per_section_eviction_reaches_every_child():
    """Each child evictor reads its section's ``timeouts.eviction`` block.

    Pins the contract: there is no parallel scalar override path.
    Per-section values flow through, and every enterprise system
    receives the same enterprise eviction block.
    """
    mc = _make_multi_config(
        community_idle=100,
        community_sweep=10,
        enterprise_systems=["prod", "dev"],
        enterprise_idle=300,
        enterprise_sweep=5,
    )
    registry = _make_registry(
        community=AsyncMock(),
        enterprise={"prod": AsyncMock(), "dev": AsyncMock()},
    )
    captured: list[tuple[float, float]] = []

    def _factory(child, timeouts):
        captured.append(
            (timeouts.session_idle_timeout_seconds, timeouts.sweep_interval_seconds)
        )
        ev = AsyncMock()
        ev.start = AsyncMock()
        ev.stop = AsyncMock()
        return ev

    with patch.object(evictors_module, "Evictor", side_effect=_factory):
        async with _running_pool(registry, mc):
            pass

    assert captured == [(100, 10), (300, 5), (300, 5)]


# ---------------------------------------------------------------------------
# Atomic startup (rollback on partial failure)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_partial_start_failure_stops_already_started_and_reraises():
    """A mid-loop ``start()`` failure stops every previously-started evictor.

    Regression for the original lifespan partial-startup leak: the pool
    must observe an all-or-nothing startup contract. A raise inside
    ``start()`` must roll back already-started evictors before
    re-raising the original exception.
    """
    mc = _make_multi_config(
        community_idle=10,
        community_sweep=2,
        enterprise_systems=["a", "b", "c"],
        enterprise_idle=300,
        enterprise_sweep=5,
    )
    registry = _make_registry(
        community=AsyncMock(),
        enterprise={"a": AsyncMock(), "b": AsyncMock(), "c": AsyncMock()},
    )

    call_count = 0
    built_evictors: list[AsyncMock] = []

    def _factory(child, timeouts):
        nonlocal call_count
        call_count += 1
        ev = AsyncMock()
        if call_count == 3:
            ev.start = AsyncMock(side_effect=RuntimeError("evictor #3 boom"))
        else:
            ev.start = AsyncMock()
        ev.stop = AsyncMock()
        built_evictors.append(ev)
        return ev

    with patch.object(evictors_module, "Evictor", side_effect=_factory):
        with pytest.raises(RuntimeError, match="evictor #3 boom"):
            await EvictorPool(registry, mc).start()

    # All three were constructed: #1 (community) + #2 (enterprise "a")
    # + #3 (enterprise "b" — the one whose start() raised).
    assert len(built_evictors) == 3
    # #1 and #2 started successfully and were stopped on cleanup.
    built_evictors[0].stop.assert_awaited_once()
    built_evictors[1].stop.assert_awaited_once()
    # #3 raised at start; its stop() is not called (was never started).
    built_evictors[2].stop.assert_not_called()


@pytest.mark.asyncio
async def test_pool_partial_start_failure_swallows_stop_errors():
    """Cleanup is best-effort: a stop() failure during partial-startup is logged, not raised."""
    mc = _make_multi_config(
        community_idle=10,
        community_sweep=2,
        enterprise_systems=["a", "b"],
        enterprise_idle=300,
        enterprise_sweep=5,
    )
    registry = _make_registry(
        community=AsyncMock(),
        enterprise={"a": AsyncMock(), "b": AsyncMock()},
    )

    call_count = 0

    def _factory(child, timeouts):
        nonlocal call_count
        call_count += 1
        ev = AsyncMock()
        if call_count == 3:
            ev.start = AsyncMock(side_effect=RuntimeError("evictor #3 boom"))
            ev.stop = AsyncMock()
        else:
            ev.start = AsyncMock()
            # Force cleanup of the earlier evictors' stop() to itself raise.
            ev.stop = AsyncMock(side_effect=RuntimeError(f"stop boom {call_count}"))
        return ev

    with patch.object(evictors_module, "Evictor", side_effect=_factory):
        # The original RuntimeError from start() propagates; the cleanup
        # stop() failures are swallowed (logged via _LOGGER.exception).
        with pytest.raises(RuntimeError, match="evictor #3 boom"):
            await EvictorPool(registry, mc).start()


# ---------------------------------------------------------------------------
# Shutdown (concurrent stop, error isolation)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_stops_all_in_parallel_even_when_one_raises(caplog):
    """Every evictor receives ``stop()`` even when an earlier ``stop()`` raises.

    The pool calls ``asyncio.gather(..., return_exceptions=True)`` so a
    single failure does not block the rest from being canceled.
    """
    mc = _make_multi_config(
        community_idle=10,
        community_sweep=2,
        enterprise_systems=["a", "b"],
        enterprise_idle=300,
        enterprise_sweep=5,
    )
    registry = _make_registry(
        community=AsyncMock(),
        enterprise={"a": AsyncMock(), "b": AsyncMock()},
    )

    evictors = [
        AsyncMock(start=AsyncMock(), stop=AsyncMock()),
        AsyncMock(
            start=AsyncMock(),
            stop=AsyncMock(side_effect=RuntimeError("middle stop boom")),
        ),
        AsyncMock(start=AsyncMock(), stop=AsyncMock()),
    ]
    evictor_iter = iter(evictors)

    def _factory(_child, _timeouts):
        return next(evictor_iter)

    with patch.object(evictors_module, "Evictor", side_effect=_factory):
        caplog.set_level(
            logging.ERROR, logger="deephaven_mcp.mcp_systems_server._evictors"
        )
        async with _running_pool(registry, mc):
            pass

    # All three evictors were stopped, regardless of the middle one raising.
    for ev in evictors:
        ev.stop.assert_awaited_once()
    # The failure was logged at ERROR (not re-raised).
    assert any(
        "Error stopping evictor" in rec.message and "middle stop boom" in rec.message
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_pool_stop_is_noop_when_empty():
    """An empty pool is a clean no-op on exit (no asyncio.gather call needed)."""
    mc = _make_multi_config()
    registry = _make_registry(community=None)
    async with _running_pool(registry, mc) as pool:
        assert pool.evictors == []
    # Nothing to assert beyond "the with-block exited cleanly."


# ---------------------------------------------------------------------------
# evictors property
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_evictors_property_returns_a_copy():
    """The ``evictors`` property hands out a copy so callers can't mutate state."""
    mc = _make_multi_config(community_idle=10, community_sweep=2)
    community_child = AsyncMock()
    registry = _make_registry(community=community_child)

    def _factory(child, timeouts):
        ev = AsyncMock()
        ev.start = AsyncMock()
        ev.stop = AsyncMock()
        return ev

    with patch.object(evictors_module, "Evictor", side_effect=_factory):
        async with _running_pool(registry, mc) as pool:
            snapshot = pool.evictors
            snapshot.clear()
            # Mutating the snapshot must not affect the pool's internal state.
            assert len(pool.evictors) == 1
