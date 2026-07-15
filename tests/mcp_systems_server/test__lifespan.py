"""Tests for ``deephaven_mcp.mcp_systems_server._lifespan``.

The process-scoped context manager :func:`process_lifespan` is a thin
orchestrator: it builds an ``InstanceTracker`` + ``MultiSystemRegistry``,
hands the per-child evictor lifecycle off to :class:`EvictorPool`, and
(when supplied) the idle-watcher lifecycle off to :class:`IdleWatcher`,
storing the resulting :class:`LifespanContext` on a :class:`ProcessResources`
holder. The per-MCP-session lifespan from :func:`make_lifespan` only reads
that holder. These tests cover the orchestrator wiring + teardown
discipline and the per-session shim; the subsystem behaviors are covered
in ``test__evictors.py`` and ``test__idle.py``.
"""

from __future__ import annotations

import logging
from dataclasses import is_dataclass
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deephaven_mcp._exceptions import InternalError
from deephaven_mcp._taxonomy import SystemRef, SystemType
from deephaven_mcp.mcp_systems_server import _lifespan as lifespan_module
from deephaven_mcp.mcp_systems_server._lifespan import (
    LifespanContext,
    ProcessResources,
    make_lifespan,
    process_lifespan,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# The instance tracker is patched in these tests, so this value is only threaded
# through ``instances_dir`` (a pure path op); no directory is created on disk.
_RUNTIME_DIR = Path("/test/runtime")


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
    per-call behavior.
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
        patch.object(lifespan_module, "harden_private_dir", MagicMock()),
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
# ProcessResources holder
# ---------------------------------------------------------------------------


def test_process_resources_defaults_to_empty():
    """A fresh holder carries no context."""
    assert ProcessResources().context is None


# ---------------------------------------------------------------------------
# process_lifespan: startup wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_lifespan_yields_context_and_shuts_down_cleanly(caplog):
    """Happy-path: every subsystem is started, the context is yielded and
    stored on the holder, and every subsystem is torn down in reverse order
    with the holder cleared."""
    multi_config = _make_multi_config(community_idle=10, community_sweep=2)
    multi_config.list_systems = MagicMock(
        return_value=[SystemRef(name="default", type=SystemType.COMMUNITY)]
    )
    registry = _build_registry_mock(community=True, enterprise=[])
    instance_tracker = MagicMock(instance_id="inst-1", unregister=AsyncMock())

    evictor_pool = MagicMock()
    evictor_pool.start = AsyncMock()
    evictor_pool.stop = AsyncMock()

    holder = ProcessResources()

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
        async with process_lifespan(
            multi_config, idle=None, holder=holder, runtime_dir=_RUNTIME_DIR
        ) as ctx:
            assert isinstance(ctx, LifespanContext)
            assert ctx.multi_config is multi_config
            assert ctx.registry is registry
            assert ctx.instance_tracker is instance_tracker
            assert holder.context is ctx

        assert holder.context is None
        registry.initialize.assert_awaited_once()
        evictor_pool.start.assert_awaited_once()
        evictor_pool.stop.assert_awaited_once()
        registry.close.assert_awaited_once()
        instance_tracker.unregister.assert_awaited_once()
    finally:
        for p in patches:
            p.stop()


@pytest.mark.asyncio
async def test_process_lifespan_hardens_runtime_dir():
    """The runtime directory is hardened to user-private mode at startup.

    Also pins that the instance tracker and orphan cleanup operate on the
    ``instances`` subdirectory of the runtime root (not the root itself), so a
    regression that dropped the :func:`instances_dir` composition is caught.
    """
    multi_config = _make_multi_config(community_idle=10, community_sweep=2)
    multi_config.list_systems = MagicMock(return_value=[])
    registry = _build_registry_mock(community=True, enterprise=[])
    tracker = MagicMock(instance_id="i", unregister=AsyncMock())
    evictor_pool = MagicMock(start=AsyncMock(), stop=AsyncMock())
    harden = MagicMock()
    create_and_register = AsyncMock(return_value=tracker)
    cleanup = AsyncMock()

    with (
        patch.object(lifespan_module, "harden_private_dir", harden),
        patch.object(
            lifespan_module.InstanceTracker,
            "create_and_register",
            create_and_register,
        ),
        patch.object(lifespan_module, "cleanup_orphaned_resources", cleanup),
        patch.object(
            lifespan_module, "MultiSystemRegistry", MagicMock(return_value=registry)
        ),
        patch.object(
            lifespan_module, "EvictorPool", MagicMock(return_value=evictor_pool)
        ),
    ):
        async with process_lifespan(
            multi_config,
            idle=None,
            holder=ProcessResources(),
            runtime_dir=_RUNTIME_DIR,
        ):
            pass

    harden.assert_called_once_with(_RUNTIME_DIR)
    create_and_register.assert_awaited_once_with(_RUNTIME_DIR / "instances")
    cleanup.assert_awaited_once_with(_RUNTIME_DIR / "instances")


@pytest.mark.asyncio
async def test_process_lifespan_forwards_multi_config_to_registry():
    """``process_lifespan`` unpacks per-section ingredients into ``MultiSystemRegistry``."""
    multi_config = _make_multi_config(
        community_idle=10,
        community_sweep=2,
        enterprise_systems=["prod"],
    )
    multi_config.list_systems = MagicMock(return_value=[])

    registry = _build_registry_mock(community=False, enterprise=[])
    tracker = MagicMock(instance_id="i", unregister=AsyncMock())

    with (
        patch.object(lifespan_module, "harden_private_dir", MagicMock()),
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
        async with process_lifespan(
            multi_config,
            idle=None,
            holder=ProcessResources(),
            runtime_dir=_RUNTIME_DIR,
        ):
            pass
        mock_registry_cls.assert_called_once_with(
            community_sessions=multi_config.community.sessions,
            community_client_timeouts=multi_config.community.settings.timeouts.client,
            enterprise_systems=multi_config.enterprise.systems,
            enterprise_client_timeouts=multi_config.enterprise.settings.timeouts.client,
        )


# ---------------------------------------------------------------------------
# process_lifespan: failure-path teardown discipline
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_lifespan_failure_during_startup_still_cleans_up():
    """If registry.initialize raises, the partial resources are still cleaned up."""
    multi_config = _make_multi_config()
    multi_config.list_systems = MagicMock(return_value=[])

    registry = _build_registry_mock(community=False, enterprise=[])
    registry.initialize = AsyncMock(side_effect=RuntimeError("boom"))

    tracker = MagicMock(instance_id="i", unregister=AsyncMock())

    evictor_pool = MagicMock()
    evictor_pool.start = AsyncMock()
    evictor_pool.stop = AsyncMock()

    holder = ProcessResources()

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
            async with process_lifespan(
                multi_config, idle=None, holder=holder, runtime_dir=_RUNTIME_DIR
            ):
                pass

        # registry.initialize raised before EvictorPool was started, so
        # the pool was never started — but the tracker was registered
        # and the registry-close fallback runs unconditionally. The holder
        # is never populated because the failure precedes the yield.
        assert holder.context is None
        evictor_pool.start.assert_not_called()
        evictor_pool.stop.assert_not_called()
        registry.close.assert_awaited_once()
        tracker.unregister.assert_awaited_once()
    finally:
        for p in patches:
            p.stop()


@pytest.mark.asyncio
async def test_process_lifespan_pool_start_failure_still_cleans_up():
    """If EvictorPool.start raises, the tracker and registry are still torn down.

    The ``pool.stop`` teardown callback is pushed only *after*
    ``await pool.start()`` succeeds, so a start failure leaves stop
    unregistered — but the tracker-unregister and registry-close
    callbacks (registered earlier) must still run via the
    :class:`AsyncExitStack`.
    """
    multi_config = _make_multi_config()
    multi_config.list_systems = MagicMock(return_value=[])

    registry = _build_registry_mock(community=False, enterprise=[])

    tracker = MagicMock(instance_id="i", unregister=AsyncMock())

    evictor_pool = MagicMock()
    evictor_pool.start = AsyncMock(side_effect=RuntimeError("pool-boom"))
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
        with pytest.raises(RuntimeError, match="pool-boom"):
            async with process_lifespan(
                multi_config,
                idle=None,
                holder=ProcessResources(),
                runtime_dir=_RUNTIME_DIR,
            ):
                pass

        evictor_pool.start.assert_awaited_once()
        evictor_pool.stop.assert_not_called()
        registry.close.assert_awaited_once()
        tracker.unregister.assert_awaited_once()
    finally:
        for p in patches:
            p.stop()


@pytest.mark.asyncio
async def test_process_lifespan_swallows_shutdown_errors(caplog):
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
        async with process_lifespan(
            multi_config,
            idle=None,
            holder=ProcessResources(),
            runtime_dir=_RUNTIME_DIR,
        ):
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
async def test_process_lifespan_uses_idle_watcher_when_idle_supplied():
    """When ``idle`` is supplied, the orchestrator drives its lifecycle."""
    multi_config = _make_multi_config(community_idle=10, community_sweep=2)
    multi_config.list_systems = MagicMock(return_value=[])

    registry = _build_registry_mock(community=True, enterprise=[])
    tracker = MagicMock(instance_id="i", unregister=AsyncMock())

    # The caller constructs the watcher and hands it to process_lifespan;
    # the orchestrator just calls start() and registers stop(). Pass a
    # mock with explicit start/stop AsyncMocks.
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
        async with process_lifespan(
            multi_config,
            idle=idle_mock,
            holder=ProcessResources(),
            runtime_dir=_RUNTIME_DIR,
        ):
            pass
        idle_mock.start.assert_awaited_once()
        idle_mock.stop.assert_awaited_once()
    finally:
        for p in patches:
            p.stop()


@pytest.mark.asyncio
async def test_process_lifespan_does_not_drive_idle_watcher_when_idle_none():
    """When ``idle`` is ``None``, no idle watcher is started or constructed."""
    multi_config = _make_multi_config(community_idle=10, community_sweep=2)
    multi_config.list_systems = MagicMock(return_value=[])

    registry = _build_registry_mock(community=True, enterprise=[])
    tracker = MagicMock(instance_id="i", unregister=AsyncMock())

    with (
        patch.object(lifespan_module, "harden_private_dir", MagicMock()),
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
        async with process_lifespan(
            multi_config,
            idle=None,
            holder=ProcessResources(),
            runtime_dir=_RUNTIME_DIR,
        ):
            pass
        mock_idle_watcher_cls.assert_not_called()


# ---------------------------------------------------------------------------
# make_lifespan: per-MCP-session shim
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_lifespan_yields_holder_context_without_building():
    """The per-session lifespan yields the holder's context and builds nothing."""
    ctx = LifespanContext(
        multi_config=MagicMock(),
        registry=MagicMock(),
        instance_tracker=MagicMock(),
    )
    holder = ProcessResources(context=ctx)

    # No subsystem patches: a build attempt would call the real
    # InstanceTracker/registry and fail, proving the shim builds nothing.
    async with make_lifespan(holder)(MagicMock(name="srv")) as yielded:
        assert yielded is ctx


@pytest.mark.asyncio
async def test_make_lifespan_raises_when_holder_unpopulated():
    """Entering the per-session lifespan before process resources exist raises."""
    holder = ProcessResources()  # context is None
    with pytest.raises(InternalError, match="process-scoped resources"):
        async with make_lifespan(holder)(MagicMock(name="srv")):
            pass


@pytest.mark.asyncio
async def test_concurrent_sessions_share_one_process_scoped_registry():
    """Overlapping MCP sessions read the single process-scoped registry, so a
    dynamic session added through one session is visible to another.

    This is the property that makes a dynamically-created session persist
    across ``dh-mcp`` invocations: every per-session lifespan yields the same
    registry built once by ``process_lifespan``.
    """

    class _StubRegistry:
        """Minimal stateful stand-in for the shared registry."""

        def __init__(self) -> None:
            self._items: dict[str, object] = {}

        async def initialize(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def add(self, name: str, value: object) -> None:
            self._items[name] = value

        async def list_names(self) -> set[str]:
            return set(self._items)

    multi_config = _make_multi_config(community_idle=10, community_sweep=2)
    multi_config.list_systems = MagicMock(return_value=[])
    registry = _StubRegistry()
    tracker = MagicMock(instance_id="i", unregister=AsyncMock())
    holder = ProcessResources()

    patches = _patch_subsystems(
        multi_config=multi_config,
        registry=registry,
        instance_tracker=tracker,
    )
    for p in patches:
        p.start()
    try:
        async with process_lifespan(
            multi_config, idle=None, holder=holder, runtime_dir=_RUNTIME_DIR
        ):
            session_lifespan = make_lifespan(holder)
            async with (
                session_lifespan(MagicMock(name="srv-a")) as ctx_a,
                session_lifespan(MagicMock(name="srv-b")) as ctx_b,
            ):
                # Both sessions resolve to the one registry instance.
                assert ctx_a.registry is registry
                assert ctx_b.registry is registry
                # A dynamic session added via session A is visible to B.
                await ctx_a.registry.add("community:community:dyn", object())
                assert "community:community:dyn" in await ctx_b.registry.list_names()
    finally:
        for p in patches:
            p.stop()


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
    """``_log_teardown_failure`` logs the labeled failure but never raises."""

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
