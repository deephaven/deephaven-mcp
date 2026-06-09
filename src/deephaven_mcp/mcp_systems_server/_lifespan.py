"""MCP systems-server lifespan factory.

The multiplexed systems server is built from an already-loaded
:class:`~deephaven_mcp.config.tree.ConfigTree`
(produced by the entry-point in :mod:`deephaven_mcp.mcp_systems_server.server`)
and constructs a single
:class:`~deephaven_mcp.resource_manager.MultiSystemRegistry` over the
community + enterprise systems it describes, plus an
:class:`~deephaven_mcp.mcp_systems_server._evictors.EvictorPool` that
attaches one :class:`~deephaven_mcp.resource_manager.Evictor` per child
registry so each system runs idle eviction with its own configured
timers.

The lifespan deliberately does **not** parse the on-disk configuration
tree itself — that is the entry-point's job, and threading the result
in here avoids a redundant second parse + permission-audit pass.

Public surface:

- :class:`LifespanContext`: frozen dataclass yielded to FastMCP tools.
- :func:`make_lifespan`: factory returning the FastMCP lifespan async
  context manager.

Subsystem lifecycles (idle watcher, evictor pool, instance tracker,
multi-system registry) are owned by their respective modules; this
module is purely the orchestrator that threads them together via
:class:`contextlib.AsyncExitStack`.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import (
    AbstractAsyncContextManager,
    AsyncExitStack,
    asynccontextmanager,
)
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP

from deephaven_mcp.config.tree import ConfigTree
from deephaven_mcp.resource_manager import (
    InstanceTracker,
    MultiSystemRegistry,
    cleanup_orphaned_resources,
)

from ._evictors import EvictorPool
from ._idle import IdleWatcher

_LOGGER = logging.getLogger(__name__)

__all__ = ["LifespanContext", "make_lifespan"]


@dataclass(frozen=True, slots=True)
class LifespanContext:
    """Frozen value yielded by :func:`make_lifespan` to FastMCP tools.

    Tools read fields by attribute (``ctx.registry``); the helpers in
    :mod:`deephaven_mcp.mcp_systems_server._tools.shared` are the
    canonical accessors. The dataclass is frozen so tool code cannot
    accidentally mutate startup-owned references.
    """

    multi_config: ConfigTree
    """The validated multi-system configuration loaded at startup.

    Tools that need to enumerate available systems read it from here."""

    registry: MultiSystemRegistry
    """Composite registry that routes session-id reads to one community
    child registry plus one enterprise child registry per configured
    enterprise system."""

    instance_tracker: InstanceTracker
    """Per-process tracker used by the orphan-resource cleanup helper
    at startup and shutdown."""


def _build_registry(multi_config: ConfigTree) -> MultiSystemRegistry:
    """Construct the composite registry from the validated config.

    :class:`MultiSystemRegistry` takes its per-section ingredients
    directly (sessions + client timeouts), not the whole
    :class:`ConfigTree`, so it has no dependency on this
    module's data type. This helper unpacks the config once.

    Args:
        multi_config (ConfigTree): The validated multi-system
            configuration. Either, both, or neither of ``community``
            and ``enterprise`` may be present.

    Returns:
        MultiSystemRegistry: A fresh, **uninitialised** registry.
            Callers are responsible for awaiting
            :meth:`MultiSystemRegistry.initialize` before use.
    """
    community = multi_config.community
    enterprise = multi_config.enterprise
    return MultiSystemRegistry(
        community_sessions=community.sessions if community else None,
        community_client_timeouts=(
            community.settings.timeouts.client if community else None
        ),
        enterprise_systems=enterprise.systems if enterprise else None,
        enterprise_client_timeouts=(
            enterprise.settings.timeouts.client if enterprise else None
        ),
    )


def make_lifespan(
    multi_config: ConfigTree,
    *,
    idle: IdleWatcher | None,
) -> Callable[[FastMCP[LifespanContext]], AbstractAsyncContextManager[LifespanContext]]:
    """Build the FastMCP lifespan context manager for the systems server.

    The orchestrator is a thin :class:`~contextlib.AsyncExitStack`-driven
    sequence; each subsystem owns its own start/stop discipline:

    1. **Instance tracker** — register a per-process tracker, then
       reclaim resources left behind by previously crashed instances
       via
       :func:`~deephaven_mcp.resource_manager.cleanup_orphaned_resources`.
    2. **Multi-system registry** — built from ``multi_config`` via
       :func:`_build_registry` and ``initialize()``-d.
    3. **Evictor pool** — :class:`._evictors.EvictorPool` attaches one
       :class:`~deephaven_mcp.resource_manager.Evictor` per child
       registry. Atomic startup with rollback; concurrent shutdown
       with per-evictor error isolation. Owned by the pool, not by
       this module.
    4. **Idle watcher** — when ``idle`` is supplied,
       :class:`._idle.IdleWatcher` runs the watcher coroutine for the
       lifetime of the lifespan. Disabled timers are a no-op inside
       the watcher; callers do not need to special-case zero.

    Shutdown is automatic and runs in reverse order on both clean
    exit and startup failure: idle watcher, evictor pool, registry,
    instance tracker. Each subsystem's ``__aexit__`` is responsible
    for logging and swallowing its own teardown errors so the
    sequence cannot be derailed by a single subsystem.

    Args:
        multi_config (ConfigTree): Pre-validated multi-system
            configuration (already produced by
            :class:`~deephaven_mcp.config.tree.ConfigTreeLoader`
            in the server entry-point). The lifespan does **not**
            re-parse the on-disk tree, and there are no parallel
            scalar override kwargs — every duration knob the lifespan
            consumes lives on this object.
        idle (IdleWatcher | None): Unstarted watcher controlling
            idle shutdown. Pass ``None`` to disable supervision (the
            stdio and operator-HTTP entry points always do; only
            daemon mode supplies a watcher). Supplying an
            :class:`IdleWatcher` opts in — the lifespan owns its
            lifecycle (calls :meth:`IdleWatcher.start` on entry and
            registers :meth:`IdleWatcher.stop` for teardown).

    Returns:
        Callable[[FastMCP[LifespanContext]], AbstractAsyncContextManager[LifespanContext]]:
        An async context manager suitable for passing to
        ``FastMCP(..., lifespan=...)``.
    """

    @asynccontextmanager
    async def _lifespan(
        server: FastMCP[LifespanContext],
    ) -> AsyncIterator[LifespanContext]:
        _LOGGER.info(f"[_lifespan] Starting MCP systems server '{server.name}'")
        async with AsyncExitStack() as stack:
            # 1. Instance tracker. ``unregister`` is best-effort on
            #    teardown; the log-and-swallow callback ensures a
            #    tracker failure cannot derail the rest of shutdown.
            tracker = await InstanceTracker.create_and_register()
            stack.push_async_callback(
                lambda: _log_teardown_failure(
                    tracker.unregister(), label="unregister tracker"
                )
            )
            _LOGGER.info(f"[_lifespan] Server instance: {tracker.instance_id}")
            await cleanup_orphaned_resources()

            _LOGGER.info(
                "[_lifespan] Using pre-loaded configuration; "
                f"systems={multi_config.list_systems()}"
            )

            # 2. Multi-system registry. Register the close callback
            #    *before* ``initialize()`` so a half-initialized
            #    registry is still closed (preserves the previous
            #    finally-block behaviour). ``close`` is best-effort.
            registry = _build_registry(multi_config)
            stack.push_async_callback(
                lambda: _log_teardown_failure(registry.close(), label="close registry")
            )
            await registry.initialize()

            # 3. Evictor pool — atomic startup with rollback baked in.
            pool = EvictorPool(registry, multi_config)
            await pool.start()
            stack.push_async_callback(pool.stop)

            # 4. Idle watcher — caller hands in an unstarted
            #    :class:`IdleWatcher`; the lifespan owns its lifecycle.
            #    ``start`` is a no-op when the timer is disabled.
            if idle is not None:
                await idle.start()
                stack.push_async_callback(idle.stop)

            _LOGGER.info(f"[_lifespan] MCP systems server '{server.name}' ready.")
            try:
                yield LifespanContext(
                    multi_config=multi_config,
                    registry=registry,
                    instance_tracker=tracker,
                )
            finally:
                _LOGGER.info(
                    f"[_lifespan] Shutting down MCP systems server '{server.name}'"
                )
        _LOGGER.info(f"[_lifespan] MCP systems server '{server.name}' shut down.")

    return _lifespan


# ---------------------------------------------------------------------------
# Best-effort teardown helper
# ---------------------------------------------------------------------------


async def _log_teardown_failure(awaitable: Awaitable[None], *, label: str) -> None:
    """Await a teardown coroutine; log any failure instead of raising.

    Used by the :class:`AsyncExitStack` callbacks that own best-effort
    shutdown steps (tracker unregister, registry close, ...). Failures
    must be logged at ERROR but never propagate — otherwise a single
    subsystem's teardown error would block every later callback in the
    LIFO sequence.

    Args:
        awaitable (Awaitable[None]): The teardown coroutine, e.g.
            ``tracker.unregister()`` or ``registry.close()``.
        label (str): Short human-readable label for the operation,
            included in the error log so operators can correlate the
            failure with the resource that produced it.
    """
    try:
        await awaitable
    except Exception:
        _LOGGER.exception(f"[_lifespan:_log_teardown_failure] Error during {label}")
