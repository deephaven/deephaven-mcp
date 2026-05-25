"""MCP systems-server lifespan factory.

The multiplexed systems server is built from an already-loaded
:class:`~deephaven_mcp.mcp_systems_server.config.MultiSystemConfig`
(produced by the entry-point in :mod:`deephaven_mcp.mcp_systems_server.server`)
and constructs a single
:class:`~deephaven_mcp.resource_manager.MultiSystemRegistry` over the
community + enterprise systems it describes, plus one
:class:`~deephaven_mcp.resource_manager.Evictor` *per child registry*
so each system runs idle eviction with its own configured timers.

The lifespan deliberately does **not** parse the on-disk configuration
tree itself — that is the entry-point's job, and threading the result
in here avoids a redundant second parse + permission-audit pass.

Public surface:

- :class:`LifespanContext`: ``TypedDict`` describing the dictionary the
  lifespan yields to FastMCP tools.
- :func:`make_lifespan`: factory returning the FastMCP lifespan async
  context manager. Single function — there is no community/enterprise
  split.

The previous ``refresh_lock`` field is gone because the ``mcp_reload``
tool was removed: configuration changes require a server restart.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TypedDict

from mcp.server.fastmcp import FastMCP

from deephaven_mcp._exceptions import InternalError
from deephaven_mcp.mcp_systems_server.config import MultiSystemConfig
from deephaven_mcp.resource_manager import (
    Evictor,
    InstanceTracker,
    MultiSystemRegistry,
    cleanup_orphaned_resources,
)

_LOGGER = logging.getLogger(__name__)

__all__ = ["LifespanContext", "make_lifespan"]


class LifespanContext(TypedDict):
    """Typed dictionary yielded by :func:`make_lifespan`.

    Attributes:
        multi_config (MultiSystemConfig): The validated multi-system
            configuration loaded at startup. Tools that need to
            enumerate available systems read it from here.
        registry (MultiSystemRegistry): Composite registry that routes
            session-id reads to one community child registry plus one
            enterprise child registry per configured enterprise system.
        evictors (list[Evictor]): One :class:`Evictor` per child
            registry, each parameterized by that child's own idle
            timeout and sweep interval. The list is in startup order:
            community first (when present), then enterprise systems in
            declaration order.
        instance_tracker (InstanceTracker): Per-process tracker used by
            the orphan-resource cleanup helper at startup and shutdown.
    """

    multi_config: MultiSystemConfig
    registry: MultiSystemRegistry
    evictors: list[Evictor]
    instance_tracker: InstanceTracker


def make_lifespan(
    multi_config: MultiSystemConfig,
) -> Callable[[FastMCP[LifespanContext]], AbstractAsyncContextManager[LifespanContext]]:
    """Build the FastMCP lifespan context manager for the systems server.

    Lifecycle:

    1. **Startup** (before ``yield``):

       a. Register an :class:`InstanceTracker` and reclaim resources
          left behind by previously crashed instances via
          :func:`cleanup_orphaned_resources`.
       b. Build a :class:`MultiSystemRegistry` from the supplied
          ``multi_config`` and call :meth:`MultiSystemRegistry.initialize`,
          which fans out to every child registry concurrently.
       c. Build one :class:`Evictor` per child registry, parameterized
          by the ``timeouts.eviction`` block on the owning section's
          settings (community or enterprise). Enterprise evictors
          share one :class:`EvictionTimeouts` across every system.
          Callers that want different timers for tests or short-lived
          tooling construct a ``MultiSystemConfig`` whose
          ``settings.timeouts.eviction`` carries the values they want;
          there is no parallel scalar override path.
       d. Yield a :class:`LifespanContext` whose ``evictors`` field is
          a list of every started evictor.

    2. **Shutdown** (in ``finally``, runs on clean shutdown and on
       startup failure):

       a. Stop every evictor concurrently via :func:`asyncio.gather`
          with ``return_exceptions=True`` (best effort: one failure is
          logged but does not block the rest). Each :class:`Evictor`
          owns its own lock and task, so concurrent ``stop()`` calls
          do not race.
       b. Close the registry, which closes every child registry
          concurrently.
       c. Unregister the instance tracker.

    Args:
        multi_config (MultiSystemConfig): Pre-validated multi-system
            configuration (already produced by
            :class:`~deephaven_mcp.mcp_systems_server.config.MultiSystemConfigManager`
            in the server entry-point). The lifespan does **not**
            re-parse the on-disk tree, and there are no parallel
            scalar override kwargs — every duration knob the lifespan
            consumes lives on this object.

    Returns:
        Callable[[FastMCP[LifespanContext]], AbstractAsyncContextManager[LifespanContext]]:
        An async context manager suitable for passing to
        ``FastMCP(..., lifespan=...)``.
    """

    @asynccontextmanager
    async def _lifespan(
        server: FastMCP[LifespanContext],
    ) -> AsyncIterator[LifespanContext]:
        _LOGGER.info(f"[systems_lifespan] Starting MCP systems server '{server.name}'")
        instance_tracker: InstanceTracker | None = None
        registry: MultiSystemRegistry | None = None
        evictors: list[Evictor] = []
        try:
            instance_tracker = await InstanceTracker.create_and_register()
            _LOGGER.info(
                f"[systems_lifespan] Server instance: {instance_tracker.instance_id}"
            )
            await cleanup_orphaned_resources()

            _LOGGER.info(
                "[systems_lifespan] Using pre-loaded configuration; "
                f"systems={multi_config.list_systems()}"
            )

            # ``MultiSystemRegistry`` takes its per-section ingredients
            # directly so it has no dependency on ``MultiSystemConfig``;
            # there is no process-global timeout state. ``pq_tools`` is
            # likewise read from the lifespan context by PQ tool
            # functions via
            # :func:`deephaven_mcp.mcp_systems_server._tools.shared.get_enterprise_settings`.
            community = multi_config.community
            enterprise = multi_config.enterprise
            registry = MultiSystemRegistry(
                community_sessions=community.sessions if community else None,
                community_client_timeouts=(
                    community.settings.timeouts.client if community else None
                ),
                enterprise_systems=enterprise.systems if enterprise else None,
                enterprise_client_timeouts=(
                    enterprise.settings.timeouts.client if enterprise else None
                ),
            )
            await registry.initialize()

            evictors = await _build_and_start_per_child_evictors(
                registry,
                multi_config,
            )

            _LOGGER.info(
                f"[systems_lifespan] MCP systems server '{server.name}' ready."
            )
            yield LifespanContext(
                multi_config=multi_config,
                registry=registry,
                evictors=evictors,
                instance_tracker=instance_tracker,
            )
        finally:
            _LOGGER.info(
                f"[systems_lifespan] Shutting down MCP systems server '{server.name}'"
            )
            # Stop all evictors in parallel (best-effort). Each Evictor
            # has independent state (its own ``_lock`` and ``_sweeper_task``),
            # so concurrent ``stop()`` calls do not race with each other.
            # We log any exceptions returned, but do not re-raise: shutdown
            # must continue regardless.
            if evictors:
                stop_results = await asyncio.gather(
                    *(ev.stop() for ev in evictors),
                    return_exceptions=True,
                )
                for ev, result in zip(evictors, stop_results, strict=True):
                    if isinstance(result, BaseException):
                        _LOGGER.error(
                            "[systems_lifespan] Error stopping evictor %r: %r",
                            ev,
                            result,
                            exc_info=result,
                        )
            if registry is not None:
                try:
                    await registry.close()
                except Exception:
                    _LOGGER.exception("[systems_lifespan] Error closing registry")
            if instance_tracker is not None:
                try:
                    await instance_tracker.unregister()
                except Exception:
                    _LOGGER.exception(
                        "[systems_lifespan] Error unregistering instance_tracker"
                    )
            _LOGGER.info(
                f"[systems_lifespan] MCP systems server '{server.name}' shut down."
            )

    return _lifespan


async def _build_and_start_per_child_evictors(
    registry: MultiSystemRegistry,
    multi_config: MultiSystemConfig,
) -> list[Evictor]:
    """Build and start one :class:`Evictor` per child registry.

    Startup is atomic: on a mid-loop failure, every already-started
    evictor is stopped (best-effort) before the original exception is
    re-raised. Callers therefore see either a fully-started list or no
    surviving state to clean up.

    Args:
        registry (MultiSystemRegistry): The composite registry whose
            child registries will each receive their own evictor.
        multi_config (MultiSystemConfig): The validated configuration
            tree the timers are read from.

    Returns:
        list[Evictor]: All started evictors, in startup order
            (community first when present, then enterprise systems in
            declaration order). The lifespan stops them in reverse on
            shutdown.

    Raises:
        Exception: Any exception raised by :meth:`Evictor.start` is
            re-raised after partial cleanup. Other failures (e.g.
            :class:`InternalError` from the enterprise consistency
            check) follow the same path.
    """
    evictors: list[Evictor] = []
    try:
        if registry.community is not None and multi_config.community is not None:
            timeouts = multi_config.community.settings.timeouts.eviction
            ev = Evictor(registry.community, timeouts)
            await ev.start()
            evictors.append(ev)
            _LOGGER.info(
                f"[systems_lifespan:_build_and_start_per_child_evictors] "
                f"Started evictor for community "
                f"(idle={timeouts.session_idle_timeout_seconds}, "
                f"sweep={timeouts.sweep_interval_seconds})"
            )

        enterprise_children = registry.enterprise_systems
        enterprise_cfg = multi_config.enterprise
        if enterprise_cfg is not None:
            timeouts = enterprise_cfg.settings.timeouts.eviction
            for name, child in enterprise_children.items():
                if name not in enterprise_cfg.systems:
                    raise InternalError(
                        f"[systems_lifespan:_build_and_start_per_child_evictors] "
                        f"Enterprise system {name!r} has a "
                        f"child registry but no matching config entry"
                    )
                ev = Evictor(child, timeouts)
                await ev.start()
                evictors.append(ev)
                _LOGGER.info(
                    f"[systems_lifespan:_build_and_start_per_child_evictors] "
                    f"Started evictor for enterprise system "
                    f"{name!r} (idle={timeouts.session_idle_timeout_seconds}, "
                    f"sweep={timeouts.sweep_interval_seconds})"
                )
        return evictors
    except BaseException as exc:
        # Atomic startup: stop every already-started evictor before
        # re-raising so callers cannot observe partial state.
        _LOGGER.error(
            f"[systems_lifespan:_build_and_start_per_child_evictors] "
            f"Partial-startup failure: {exc!r}; rolling back "
            f"{len(evictors)} already-started evictor(s)",
            exc_info=True,
        )
        rollback_failures = 0
        for ev in reversed(evictors):
            try:
                await ev.stop()
            except Exception as stop_exc:
                rollback_failures += 1
                _LOGGER.exception(
                    f"[systems_lifespan:_build_and_start_per_child_evictors] "
                    f"Error stopping evictor {ev!r} during "
                    f"partial-startup cleanup: {stop_exc!r}"
                )
        _LOGGER.info(
            f"[systems_lifespan:_build_and_start_per_child_evictors] "
            f"Partial-startup rollback complete: "
            f"{len(evictors) - rollback_failures}/{len(evictors)} evictors "
            f"stopped cleanly; re-raising original failure"
        )
        raise
