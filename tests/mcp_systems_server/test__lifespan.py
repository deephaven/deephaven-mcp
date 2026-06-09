"""Tests for ``deephaven_mcp.mcp_systems_server._lifespan``.

The lifespan factory is now a thin orchestrator: it builds an
``InstanceTracker`` + ``MultiSystemRegistry``, hands the per-child
evictor lifecycle off to :class:`EvictorPool`, and (when supplied)
the idle-watcher lifecycle off to :class:`IdleWatcher`. These tests
cover the orchestrator wiring and its teardown discipline; the
subsystem behaviours are covered in ``test__evictors.py`` and
``test__idle.py``.
"""

from __future__ import annotations

import logging
from dataclasses import is_dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deephaven_mcp._taxonomy import SystemRef, SystemType
from deephaven_mcp.mcp_systems_server import _lifespan as lifespan_module
from deephaven_mcp.mcp_systems_server._lifespan import (
    LifespanContext,
    make_lifespan,
)

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
    """Build a ``ConfigTree`` mock with the given timer values."""
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


def _build_registry_mock(*, community: bool, enterprise: list[str]) -> MagicMock:
    reg = MagicMock()
    reg.initialize = AsyncMock()
    reg.close = AsyncMock()
    reg.community = AsyncMock() if community else None
    reg.enterprise_systems = {name: AsyncMock() for name in enterprise}
    return reg


def _patch_subsystems(
    *,
    multi_config: MagicMock,
    registry: MagicMock,
    instance_tracker: MagicMock,
    evictor_pool: MagicMock | None = None,
    idle_watcher: MagicMock | None = None,
):
    """Patch every external dependency the orchestrator drives.

    ``EvictorPool`` and ``IdleWatcher`` are patched as *classes* whose
    instances expose explicit ``start`` / ``stop`` AsyncMocks. Tests
    configure those mocks (or supply their own) if they want to assert
    per-call behaviour.
    """
    if evictor_pool is None:
        evictor_pool = MagicMock()
        evictor_pool.start = AsyncMock()
        evictor_pool.stop = AsyncMock()
    if idle_watcher is None:
        idle_watcher = MagicMock()
        idle_watcher.start = AsyncMock()
        idle_watcher.stop = AsyncMock()
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
        patch.object(
            lifespan_module,
            "EvictorPool",
            MagicMock(return_value=evictor_pool),
        ),
        patch.object(
            lifespan_module,
            "IdleWatcher",
            MagicMock(return_value=idle_watcher),
        ),
    )


# ---------------------------------------------------------------------------
# LifespanContext
# ---------------------------------------------------------------------------


def test_lifespan_context_is_frozen_dataclass():
    """LifespanContext is a frozen dataclass with three fields."""
    assert is_dataclass(LifespanContext)
    fields = set(LifespanContext.__dataclass_fields__)
    assert fields == {"multi_config", "registry", "instance_tracker"}
    # Frozen: assignment raises.
    ctx = LifespanContext(
        multi_config=MagicMock(),
        registry=MagicMock(),
        instance_tracker=MagicMock(),
    )
    with pytest.raises((AttributeError, Exception)):
        ctx.registry = MagicMock()  # type: ignore[misc]


# ---------------------------------------------------------------------------
# make_lifespan: startup wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_lifespan_yields_context_and_shuts_down_cleanly(caplog):
    """Happy-path: every subsystem is started, the context is yielded, and
    every subsystem is torn down in reverse order."""
    multi_config = _make_multi_config(community_idle=10, community_sweep=2)
    multi_config.list_systems = MagicMock(
        return_value=[SystemRef(name="default", type=SystemType.COMMUNITY)]
    )
    registry = _build_registry_mock(community=True, enterprise=[])
    instance_tracker = MagicMock(instance_id="inst-1", unregister=AsyncMock())

    evictor_pool = MagicMock()
    evictor_pool.start = AsyncMock()
    evictor_pool.stop = AsyncMock()

    server = MagicMock()
    server.name = "test-server"

    patches = _patch_subsystems(
        multi_config=multi_config,
        registry=registry,
        instance_tracker=instance_tracker,
        evictor_pool=evictor_pool,
    )
    for p in patches:
        p.start()
    try:
        caplog.set_level(
            logging.INFO, logger="deephaven_mcp.mcp_systems_server._lifespan"
        )
        cm = make_lifespan(multi_config, idle=None)(server)
        async with cm as ctx:
            assert isinstance(ctx, LifespanContext)
            assert ctx.multi_config is multi_config
            assert ctx.registry is registry
            assert ctx.instance_tracker is instance_tracker

        registry.initialize.assert_awaited_once()
        evictor_pool.start.assert_awaited_once()
        evictor_pool.stop.assert_awaited_once()
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
    tracker = MagicMock(instance_id="i", unregister=AsyncMock())

    with (
        patch.object(
            lifespan_module.InstanceTracker,
            "create_and_register",
            AsyncMock(return_value=tracker),
        ),
        patch.object(lifespan_module, "cleanup_orphaned_resources", AsyncMock()),
        patch.object(
            lifespan_module,
            "MultiSystemRegistry",
            MagicMock(return_value=registry),
        ) as mock_registry_cls,
        patch.object(
            lifespan_module,
            "EvictorPool",
            MagicMock(
                return_value=MagicMock(
                    start=AsyncMock(),
                    stop=AsyncMock(),
                )
            ),
        ),
    ):
        async with make_lifespan(multi_config, idle=None)(MagicMock(name="srv")):
            pass
        mock_registry_cls.assert_called_once_with(
            community_sessions=multi_config.community.sessions,
            community_client_timeouts=multi_config.community.settings.timeouts.client,
            enterprise_systems=multi_config.enterprise.systems,
            enterprise_client_timeouts=multi_config.enterprise.settings.timeouts.client,
        )


# ---------------------------------------------------------------------------
# make_lifespan: failure-path teardown discipline
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_lifespan_failure_during_startup_still_cleans_up():
    """If registry.initialize raises, the partial resources are still cleaned up."""
    multi_config = _make_multi_config()
    multi_config.list_systems = MagicMock(return_value=[])

    registry = _build_registry_mock(community=False, enterprise=[])
    registry.initialize = AsyncMock(side_effect=RuntimeError("boom"))

    tracker = MagicMock(instance_id="i", unregister=AsyncMock())

    evictor_pool = MagicMock()
    evictor_pool.start = AsyncMock()
    evictor_pool.stop = AsyncMock()

    patches = _patch_subsystems(
        multi_config=multi_config,
        registry=registry,
        instance_tracker=tracker,
        evictor_pool=evictor_pool,
    )
    for p in patches:
        p.start()
    try:
        with pytest.raises(RuntimeError, match="boom"):
            async with make_lifespan(multi_config, idle=None)(MagicMock(name="srv")):
                pass

        # registry.initialize raised before EvictorPool was started, so
        # the pool was never started — but the tracker was registered
        # and the registry-close fallback runs unconditionally.
        evictor_pool.start.assert_not_called()
        evictor_pool.stop.assert_not_called()
        registry.close.assert_awaited_once()
        tracker.unregister.assert_awaited_once()
    finally:
        for p in patches:
            p.stop()


@pytest.mark.asyncio
async def test_make_lifespan_swallows_shutdown_errors(caplog):
    """Errors during shutdown are logged, never re-raised."""
    multi_config = _make_multi_config(community_idle=10, community_sweep=2)
    multi_config.list_systems = MagicMock(return_value=[])

    registry = _build_registry_mock(community=True, enterprise=[])
    registry.close = AsyncMock(side_effect=RuntimeError("close-fail"))
    tracker = MagicMock(
        instance_id="i",
        unregister=AsyncMock(side_effect=RuntimeError("unreg-fail")),
    )

    # ``EvictorPool.stop`` is contractually responsible for
    # logging-and-swallowing its own errors; here we simulate that by
    # making stop return cleanly.
    evictor_pool = MagicMock()
    evictor_pool.start = AsyncMock()
    evictor_pool.stop = AsyncMock()

    patches = _patch_subsystems(
        multi_config=multi_config,
        registry=registry,
        instance_tracker=tracker,
        evictor_pool=evictor_pool,
    )
    for p in patches:
        p.start()
    try:
        caplog.set_level(logging.ERROR)
        async with make_lifespan(multi_config, idle=None)(MagicMock(name="srv")):
            pass
    finally:
        for p in patches:
            p.stop()

    msgs = [rec.message for rec in caplog.records]
    assert any("Error during close registry" in m for m in msgs)
    assert any("Error during unregister tracker" in m for m in msgs)


# ---------------------------------------------------------------------------
# Idle-watcher integration (orchestrator wiring only)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_lifespan_uses_idle_watcher_when_idle_supplied():
    """When ``idle`` is supplied, the orchestrator drives its lifecycle."""
    multi_config = _make_multi_config(community_idle=10, community_sweep=2)
    multi_config.list_systems = MagicMock(return_value=[])

    registry = _build_registry_mock(community=True, enterprise=[])
    tracker = MagicMock(instance_id="i", unregister=AsyncMock())

    # The caller now constructs the watcher and hands it to the
    # lifespan; the orchestrator just calls start() and registers
    # stop(). Pass a mock with explicit start/stop AsyncMocks.
    idle_mock = MagicMock()
    idle_mock.start = AsyncMock()
    idle_mock.stop = AsyncMock()

    patches = _patch_subsystems(
        multi_config=multi_config,
        registry=registry,
        instance_tracker=tracker,
    )
    for p in patches:
        p.start()
    try:
        async with make_lifespan(multi_config, idle=idle_mock)(MagicMock(name="srv")):
            pass
        idle_mock.start.assert_awaited_once()
        idle_mock.stop.assert_awaited_once()
    finally:
        for p in patches:
            p.stop()


@pytest.mark.asyncio
async def test_make_lifespan_does_not_construct_idle_watcher_when_idle_none():
    """When ``idle`` is ``None``, ``IdleWatcher`` is never constructed or driven."""
    multi_config = _make_multi_config(community_idle=10, community_sweep=2)
    multi_config.list_systems = MagicMock(return_value=[])

    registry = _build_registry_mock(community=True, enterprise=[])
    tracker = MagicMock(instance_id="i", unregister=AsyncMock())

    with (
        patch.object(
            lifespan_module.InstanceTracker,
            "create_and_register",
            AsyncMock(return_value=tracker),
        ),
        patch.object(lifespan_module, "cleanup_orphaned_resources", AsyncMock()),
        patch.object(
            lifespan_module,
            "MultiSystemRegistry",
            MagicMock(return_value=registry),
        ),
        patch.object(
            lifespan_module,
            "EvictorPool",
            MagicMock(
                return_value=MagicMock(
                    start=AsyncMock(),
                    stop=AsyncMock(),
                )
            ),
        ),
        patch.object(lifespan_module, "IdleWatcher") as mock_idle_watcher_cls,
    ):
        async with make_lifespan(multi_config, idle=None)(MagicMock(name="srv")):
            pass
        mock_idle_watcher_cls.assert_not_called()


# ---------------------------------------------------------------------------
# _build_registry helper
# ---------------------------------------------------------------------------


def test_build_registry_unpacks_per_section_ingredients():
    """``_build_registry`` forwards per-section ingredients to ``MultiSystemRegistry``."""
    multi_config = _make_multi_config(
        community_idle=10,
        community_sweep=2,
        enterprise_systems=["prod"],
    )
    with patch.object(lifespan_module, "MultiSystemRegistry") as mock_cls:
        lifespan_module._build_registry(multi_config)
    mock_cls.assert_called_once_with(
        community_sessions=multi_config.community.sessions,
        community_client_timeouts=multi_config.community.settings.timeouts.client,
        enterprise_systems=multi_config.enterprise.systems,
        enterprise_client_timeouts=multi_config.enterprise.settings.timeouts.client,
    )


def test_build_registry_handles_missing_sections():
    """When community/enterprise are absent, their kwargs are ``None``."""
    multi_config = _make_multi_config()  # no community, no enterprise
    with patch.object(lifespan_module, "MultiSystemRegistry") as mock_cls:
        lifespan_module._build_registry(multi_config)
    mock_cls.assert_called_once_with(
        community_sessions=None,
        community_client_timeouts=None,
        enterprise_systems=None,
        enterprise_client_timeouts=None,
    )


# ---------------------------------------------------------------------------
# Teardown wrappers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_log_teardown_failure_logs_and_swallows(caplog):
    """``_log_teardown_failure`` logs the labelled failure but never raises."""

    async def _boom() -> None:
        raise RuntimeError("teardown boom")

    caplog.set_level(logging.ERROR, logger="deephaven_mcp.mcp_systems_server._lifespan")
    # Must not raise.
    await lifespan_module._log_teardown_failure(_boom(), label="boom step")
    assert any(
        "Error during boom step" in rec.message
        and "teardown boom" in (rec.exc_text or "")
        for rec in caplog.records
        if rec.levelno >= logging.ERROR
    )


@pytest.mark.asyncio
async def test_log_teardown_failure_passes_through_on_success():
    """A teardown coroutine that returns cleanly is awaited without logging."""

    seen: list[str] = []

    async def _ok() -> None:
        seen.append("ran")

    await lifespan_module._log_teardown_failure(_ok(), label="ok step")
    assert seen == ["ran"]
