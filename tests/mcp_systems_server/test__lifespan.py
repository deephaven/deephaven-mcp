"""Tests for ``deephaven_mcp.mcp_systems_server._lifespan``.

The lifespan factory has a single entry point — ``make_lifespan`` —
that takes a pre-loaded :class:`MultiSystemConfig` and wires up an
InstanceTracker, MultiSystemRegistry, and one ``Evictor`` per child
registry. There is no longer a community/enterprise split, and the
lifespan no longer parses configuration itself.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deephaven_mcp._exceptions import InternalError
from deephaven_mcp._taxonomy import SystemRef, SystemType
from deephaven_mcp.mcp_systems_server import _lifespan as lifespan_module
from deephaven_mcp.mcp_systems_server._lifespan import (
    LifespanContext,
    _build_and_start_per_child_evictors,
    make_lifespan,
)

# ---------------------------------------------------------------------------
# _build_and_start_per_child_evictors — pure unit tests
# ---------------------------------------------------------------------------


def _make_multi_config(
    *,
    community_idle: float | None = None,
    community_sweep: float | None = None,
    enterprise_systems: list[str] | None = None,
    enterprise_idle: float | None = None,
    enterprise_sweep: float | None = None,
) -> MagicMock:
    """Build a ``MultiSystemConfig`` mock with the given timer values.

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


@pytest.mark.asyncio
async def test_build_evictors_returns_empty_when_no_children():
    """No community and no enterprise systems means no evictors."""
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

    with patch.object(lifespan_module, "Evictor", side_effect=_factory):
        evictors = await _build_and_start_per_child_evictors(registry, mc)
    assert evictors == []
    assert started == []


@pytest.mark.asyncio
async def test_build_evictors_uses_settings_timers():
    """Each child registry receives an Evictor parameterized by its umbrella settings."""
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
        return ev

    with patch.object(lifespan_module, "Evictor", side_effect=_factory):
        evictors = await _build_and_start_per_child_evictors(registry, mc)

    # Three children -> three evictors, all started.
    assert len(evictors) == 3
    for ev in evictors:
        ev.start.assert_awaited_once()

    # Community evictor goes first; its timers come from community settings.
    assert captured[0] == (community_child, 100, 10)
    # Enterprise evictors follow; both share the system-wide (idle, sweep).
    enterprise_calls = {child: (idle, sweep) for child, idle, sweep in captured[1:]}
    assert enterprise_calls[prod_child] == (300, 5)
    assert enterprise_calls[dev_child] == (300, 5)


@pytest.mark.asyncio
async def test_build_evictors_community_only():
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
        return ev

    with patch.object(lifespan_module, "Evictor", side_effect=_factory):
        evictors = await _build_and_start_per_child_evictors(registry, mc)
    assert len(evictors) == 1
    assert captured == [(42, 7)]


@pytest.mark.asyncio
async def test_build_evictors_enterprise_only():
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
        return ev

    with patch.object(lifespan_module, "Evictor", side_effect=_factory):
        evictors = await _build_and_start_per_child_evictors(registry, mc)
    assert len(evictors) == 1
    assert captured == [(15, 3)]


@pytest.mark.asyncio
async def test_build_evictors_per_section_eviction_reaches_every_child():
    """Each child evictor reads its section's ``timeouts.eviction`` block.

    There is no parallel scalar override path on the lifespan or the
    evictor builder — callers that want different timers must build a
    ``MultiSystemConfig`` whose ``settings.timeouts.eviction`` carries
    the values they want. This test pins that contract: per-section
    values flow through, and every enterprise system receives the same
    enterprise eviction block.
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
        return ev

    with patch.object(lifespan_module, "Evictor", side_effect=_factory):
        await _build_and_start_per_child_evictors(registry, mc)

    # Community section: its own eviction block. Enterprise section:
    # one shared block applied to every enterprise child.
    assert captured == [(100, 10), (300, 5), (300, 5)]


@pytest.mark.asyncio
async def test_build_evictors_partial_start_failure_stops_already_started_and_reraises():
    """A mid-loop ``start()`` failure stops every previously-started evictor and re-raises.

    Regression for the original lifespan partial-startup leak. Old
    behavior: the helper returned a list; a raise inside ``start()``
    prevented the caller's assignment from running, orphaning already-
    started evictors. New contract: startup is atomic — the helper
    stops every started evictor (best-effort) before re-raising, so the
    caller never observes a partial list.
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

    with patch.object(lifespan_module, "Evictor", side_effect=_factory):
        with pytest.raises(RuntimeError, match="evictor #3 boom"):
            await _build_and_start_per_child_evictors(registry, mc)

    # All three were constructed: #1 (community) + #2 (enterprise "a")
    # + #3 (enterprise "b" — the one whose start() raised).
    assert len(built_evictors) == 3
    # #1 and #2 started successfully and were stopped on cleanup.
    built_evictors[0].stop.assert_awaited_once()
    built_evictors[1].stop.assert_awaited_once()
    # #3 raised at start; its stop() is not called (was never started).
    built_evictors[2].stop.assert_not_called()


@pytest.mark.asyncio
async def test_build_evictors_partial_start_failure_swallows_stop_errors():
    """Cleanup is best-effort: a stop() failure during partial-startup cleanup is logged, not raised."""
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

    with patch.object(lifespan_module, "Evictor", side_effect=_factory):
        # The original RuntimeError from start() propagates; the cleanup
        # stop() failures are swallowed (logged via _LOGGER.exception).
        with pytest.raises(RuntimeError, match="evictor #3 boom"):
            await _build_and_start_per_child_evictors(registry, mc)


@pytest.mark.asyncio
async def test_build_evictors_orphan_enterprise_child_raises_internal_error_and_rolls_back():
    """An enterprise child registry with no matching config entry triggers the defensive ``InternalError``.

    Constructs a registry whose ``enterprise_systems`` contains a name
    (``"ghost"``) that does NOT appear in ``multi_config.enterprise.systems``
    (which only declares ``"prod"``). The community evictor starts
    successfully on iteration one, then the enterprise loop raises
    :class:`InternalError` when it hits the orphan name. The atomic-
    startup contract requires the already-started community evictor to
    be ``stop()``-rolled back before the original exception re-raises.
    """
    mc = _make_multi_config(
        community_idle=10,
        community_sweep=2,
        enterprise_systems=["prod"],
        enterprise_idle=300,
        enterprise_sweep=5,
    )
    registry = _make_registry(
        community=AsyncMock(),
        enterprise={"ghost": AsyncMock()},
    )

    built_evictors: list[AsyncMock] = []

    def _factory(child, timeouts):
        ev = AsyncMock()
        ev.start = AsyncMock()
        ev.stop = AsyncMock()
        built_evictors.append(ev)
        return ev

    with patch.object(lifespan_module, "Evictor", side_effect=_factory):
        with pytest.raises(
            InternalError,
            match=r"'ghost' has a child registry but no matching config entry",
        ):
            await _build_and_start_per_child_evictors(registry, mc)

    # Only the community evictor was ever constructed/started; the
    # orphan-name check fires before any enterprise Evictor is built.
    assert len(built_evictors) == 1
    built_evictors[0].start.assert_awaited_once()
    # Atomic-startup roll-back: the community evictor was stopped before
    # the defensive InternalError propagated.
    built_evictors[0].stop.assert_awaited_once()


# ---------------------------------------------------------------------------
# make_lifespan — startup / yield / shutdown
# ---------------------------------------------------------------------------


def _patch_lifespan_deps(
    *,
    multi_config: MagicMock,
    registry: MagicMock,
    evictor: AsyncMock,
    instance_tracker: MagicMock,
):
    """Patch every ``_lifespan`` external dependency in one place."""
    return (
        patch.object(
            lifespan_module.InstanceTracker,
            "create_and_register",
            AsyncMock(return_value=instance_tracker),
        ),
        patch.object(
            lifespan_module,
            "cleanup_orphaned_resources",
            AsyncMock(),
        ),
        patch.object(
            lifespan_module,
            "MultiSystemRegistry",
            MagicMock(return_value=registry),
        ),
        # Single Evictor mock used for every child; per-child topology
        # is exercised by the unit tests above.
        patch.object(
            lifespan_module,
            "Evictor",
            MagicMock(return_value=evictor),
        ),
    )


def _build_registry_mock(*, community: bool, enterprise: list[str]) -> MagicMock:
    reg = MagicMock()
    reg.initialize = AsyncMock()
    reg.close = AsyncMock()
    reg.community = AsyncMock() if community else None
    reg.enterprise_systems = {name: AsyncMock() for name in enterprise}
    return reg


@pytest.mark.asyncio
async def test_make_lifespan_yields_context_and_shuts_down_cleanly(caplog):
    multi_config = _make_multi_config(community_idle=10, community_sweep=2)
    multi_config.list_systems = MagicMock(
        return_value=[SystemRef(name="default", type=SystemType.COMMUNITY)]
    )

    registry = _build_registry_mock(community=True, enterprise=[])

    evictor = AsyncMock()
    evictor.start = AsyncMock()
    evictor.stop = AsyncMock()

    instance_tracker = MagicMock()
    instance_tracker.instance_id = "inst-1"
    instance_tracker.unregister = AsyncMock()

    server = MagicMock()
    server.name = "test-server"

    patches = _patch_lifespan_deps(
        multi_config=multi_config,
        registry=registry,
        evictor=evictor,
        instance_tracker=instance_tracker,
    )
    for p in patches:
        p.start()
    try:
        caplog.set_level(
            logging.INFO, logger="deephaven_mcp.mcp_systems_server._lifespan"
        )
        cm = make_lifespan(multi_config)(server)
        async with cm as ctx:
            assert isinstance(ctx, dict)
            assert ctx["multi_config"] is multi_config
            assert ctx["registry"] is registry
            # One community child -> exactly one evictor.
            assert ctx["evictors"] == [evictor]
            assert ctx["instance_tracker"] is instance_tracker

        registry.initialize.assert_awaited_once()
        evictor.start.assert_awaited_once()
        evictor.stop.assert_awaited_once()
        registry.close.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()
    finally:
        for p in patches:
            p.stop()


@pytest.mark.asyncio
async def test_make_lifespan_forwards_multi_config_to_registry():
    """The lifespan unpacks per-section ingredients into ``MultiSystemRegistry``."""
    multi_config = _make_multi_config(
        community_idle=10,
        community_sweep=2,
        enterprise_systems=["prod"],
    )
    multi_config.list_systems = MagicMock(return_value=[])

    registry = _build_registry_mock(community=False, enterprise=[])
    evictor = AsyncMock(start=AsyncMock(), stop=AsyncMock())
    tracker = MagicMock(instance_id="i", unregister=AsyncMock())

    with (
        patch.object(
            lifespan_module.InstanceTracker,
            "create_and_register",
            AsyncMock(return_value=tracker),
        ),
        patch.object(
            lifespan_module,
            "cleanup_orphaned_resources",
            AsyncMock(),
        ),
        patch.object(
            lifespan_module,
            "MultiSystemRegistry",
            MagicMock(return_value=registry),
        ) as mock_registry_cls,
        patch.object(
            lifespan_module,
            "Evictor",
            MagicMock(return_value=evictor),
        ),
    ):
        async with make_lifespan(multi_config)(MagicMock(name="srv")):
            pass
        mock_registry_cls.assert_called_once_with(
            community_sessions=multi_config.community.sessions,
            community_client_timeouts=multi_config.community.settings.timeouts.client,
            enterprise_systems=multi_config.enterprise.systems,
            enterprise_client_timeouts=multi_config.enterprise.settings.timeouts.client,
        )


@pytest.mark.asyncio
async def test_make_lifespan_failure_during_startup_still_cleans_up():
    """If registry.initialize raises, the partial resources are still cleaned up."""
    multi_config = _make_multi_config()
    multi_config.list_systems = MagicMock(return_value=[])

    registry = _build_registry_mock(community=False, enterprise=[])
    registry.initialize = AsyncMock(side_effect=RuntimeError("boom"))

    evictor = AsyncMock(start=AsyncMock(), stop=AsyncMock())
    tracker = MagicMock(instance_id="i", unregister=AsyncMock())

    with (
        patch.object(
            lifespan_module.InstanceTracker,
            "create_and_register",
            AsyncMock(return_value=tracker),
        ),
        patch.object(
            lifespan_module,
            "cleanup_orphaned_resources",
            AsyncMock(),
        ),
        patch.object(
            lifespan_module,
            "MultiSystemRegistry",
            MagicMock(return_value=registry),
        ),
        patch.object(
            lifespan_module,
            "Evictor",
            MagicMock(return_value=evictor),
        ),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            async with make_lifespan(multi_config)(MagicMock(name="srv")):
                pass

    # Evictor never started, so it was never stopped. But the tracker
    # was registered, so it must be unregistered.
    evictor.start.assert_not_called()
    evictor.stop.assert_not_called()
    registry.close.assert_awaited_once()
    tracker.unregister.assert_awaited_once()


@pytest.mark.asyncio
async def test_make_lifespan_swallows_shutdown_errors(caplog):
    """Errors during shutdown are logged, never re-raised."""
    multi_config = _make_multi_config(community_idle=10, community_sweep=2)
    multi_config.list_systems = MagicMock(return_value=[])

    registry = _build_registry_mock(community=True, enterprise=[])
    registry.close = AsyncMock(side_effect=RuntimeError("close-fail"))
    evictor = AsyncMock(
        start=AsyncMock(),
        stop=AsyncMock(side_effect=RuntimeError("stop-fail")),
    )
    tracker = MagicMock(
        instance_id="i",
        unregister=AsyncMock(side_effect=RuntimeError("unreg-fail")),
    )

    with (
        patch.object(
            lifespan_module.InstanceTracker,
            "create_and_register",
            AsyncMock(return_value=tracker),
        ),
        patch.object(
            lifespan_module,
            "cleanup_orphaned_resources",
            AsyncMock(),
        ),
        patch.object(
            lifespan_module,
            "MultiSystemRegistry",
            MagicMock(return_value=registry),
        ),
        patch.object(
            lifespan_module,
            "Evictor",
            MagicMock(return_value=evictor),
        ),
    ):
        caplog.set_level(logging.ERROR)
        async with make_lifespan(multi_config)(MagicMock(name="srv")):
            pass

    msgs = [rec.message for rec in caplog.records]
    assert any("Error stopping evictor" in m for m in msgs)
    assert any("Error closing registry" in m for m in msgs)
    assert any("Error unregistering" in m for m in msgs)


def test_lifespan_context_is_typed_dict():
    """LifespanContext is a TypedDict with the documented keys."""
    keys = set(LifespanContext.__annotations__)
    assert keys == {"multi_config", "registry", "evictors", "instance_tracker"}


@pytest.mark.asyncio
async def test_make_lifespan_stops_all_evictors_in_parallel_even_when_one_raises(
    caplog,
):
    """Every evictor receives ``stop()`` even when an earlier ``stop()`` raises.

    The lifespan shutdown calls ``asyncio.gather(..., return_exceptions=True)``
    on every evictor's ``stop()``, so a single failure does not block the
    rest from being cancelled. This regression test pins that contract by
    constructing three evictors where the middle one raises from ``stop()``
    and asserting all three were awaited (not just the first two) and that
    the failure was logged but did not propagate.
    """
    multi_config = _make_multi_config(
        community_idle=10,
        community_sweep=2,
        enterprise_systems=["a", "b"],
        enterprise_idle=300,
        enterprise_sweep=5,
    )
    multi_config.list_systems = MagicMock(return_value=[])

    registry = _build_registry_mock(community=True, enterprise=["a", "b"])

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

    tracker = MagicMock(instance_id="i", unregister=AsyncMock())

    with (
        patch.object(
            lifespan_module.InstanceTracker,
            "create_and_register",
            AsyncMock(return_value=tracker),
        ),
        patch.object(
            lifespan_module,
            "cleanup_orphaned_resources",
            AsyncMock(),
        ),
        patch.object(
            lifespan_module,
            "MultiSystemRegistry",
            MagicMock(return_value=registry),
        ),
        patch.object(lifespan_module, "Evictor", side_effect=_factory),
    ):
        caplog.set_level(
            logging.ERROR, logger="deephaven_mcp.mcp_systems_server._lifespan"
        )
        async with make_lifespan(multi_config)(MagicMock(name="srv")):
            pass

    # All three evictors were stopped, regardless of the middle one raising.
    for ev in evictors:
        ev.stop.assert_awaited_once()
    # The failure was logged at ERROR (not re-raised).
    assert any(
        "Error stopping evictor" in rec.message and "middle stop boom" in rec.message
        for rec in caplog.records
    )
    # The registry and tracker were still cleaned up after the evictor sweep.
    registry.close.assert_awaited_once()
    tracker.unregister.assert_awaited_once()
