"""MCP systems-server lifespan orchestration.

Builds the process-scoped subsystems for the multiplexed systems server from
an already-loaded :class:`~deephaven_mcp.config.tree.ConfigTree`: a single
:class:`~deephaven_mcp.resource_manager.MultiSystemRegistry` over the
community + enterprise systems, plus an
:class:`~deephaven_mcp.mcp_systems_server._evictors.EvictorPool` (one
:class:`~deephaven_mcp.resource_manager.Evictor` per child registry).

Under stateful streamable-HTTP the FastMCP per-session lifespan runs once per
MCP session, so the subsystems are built once per process by
:func:`process_lifespan` (wrapping the transport run) and shared with every
session through a :class:`ProcessResources` holder; :func:`make_lifespan`
returns the per-session lifespan that only reads that holder.

Public surface:

- :class:`LifespanContext`: frozen dataclass yielded to FastMCP tools.
- :class:`ProcessResources`: mutable holder carrying the process-scoped
  context from :func:`process_lifespan` to :func:`make_lifespan`.
- :func:`process_lifespan`: builds the subsystems once and stores the context
  on the holder.
- :func:`make_lifespan`: returns the per-session lifespan that yields the
  holder's context.
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

from deephaven_mcp._exceptions import InternalError
from deephaven_mcp.config.tree import ConfigTree
from deephaven_mcp.resource_manager import (
    InstanceTracker,
    MultiSystemRegistry,
    cleanup_orphaned_resources,
)

from ._evictors import EvictorPool
from ._idle import IdleWatcher

_LOGGER = logging.getLogger(__name__)

__all__ = ["LifespanContext", "ProcessResources", "make_lifespan", "process_lifespan"]


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


@dataclass(slots=True)
class ProcessResources:
    """Mutable holder for the process-scoped :class:`LifespanContext`.

    :func:`process_lifespan` sets :attr:`context` once the process-scoped
    subsystems are built and clears it on shutdown; the per-MCP-session
    lifespan from :func:`make_lifespan` reads it.
    """

    context: LifespanContext | None = None
    """The process-scoped context, or ``None`` before
    :func:`process_lifespan` has built the subsystems (and after it tears
    them down)."""


def _build_registry(multi_config: ConfigTree) -> MultiSystemRegistry:
    """Construct the composite registry from the validated config.

    Args:
        multi_config (ConfigTree): The validated multi-system configuration.
            Either, both, or neither of ``community`` and ``enterprise`` may
            be present.

    Returns:
        MultiSystemRegistry: A fresh, uninitialized registry; callers must
            await :meth:`MultiSystemRegistry.initialize` before use.
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


@asynccontextmanager
async def process_lifespan(
    multi_config: ConfigTree,
    *,
    idle: IdleWatcher | None,
    holder: ProcessResources,
) -> AsyncIterator[LifespanContext]:
    """Build and own the process-scoped systems-server subsystems.

    Wraps the transport run (the streamable-HTTP app lifespan, or the stdio
    ``run_stdio_async`` call) so the subsystems — instance tracker,
    multi-system registry, evictor pool, and optional idle watcher — are built
    once per process and shared across every MCP session. The built
    :class:`LifespanContext` is stored on ``holder`` for :func:`make_lifespan`
    to read, and cleared on exit. Shutdown runs in reverse order on both clean
    exit and startup failure; each subsystem logs and swallows its own
    teardown errors.

    Args:
        multi_config (ConfigTree): Pre-validated multi-system configuration.
        idle (IdleWatcher | None): Unstarted watcher controlling idle
            shutdown. Pass ``None`` to disable supervision (stdio and
            default-HTTP do; only daemon mode supplies one). When supplied,
            this context owns its lifecycle (``start`` on entry, ``stop`` on
            teardown).
        holder (ProcessResources): Holder whose ``context`` is set to the
            built :class:`LifespanContext` while active and reset to ``None``
            on exit.

    Yields:
        LifespanContext: The process-scoped context, also stored on ``holder``.
    """
    _LOGGER.info("[process_lifespan] Starting process-scoped systems-server resources")
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
        _LOGGER.info(f"[process_lifespan] Server instance: {tracker.instance_id}")
        await cleanup_orphaned_resources()

        _LOGGER.info(
            "[process_lifespan] Using pre-loaded configuration; "
            f"systems={multi_config.list_systems()}"
        )

        # 2. Multi-system registry. Register the close callback *before*
        #    initialize() so a half-initialized registry is still closed.
        #    close is best-effort.
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
        #    :class:`IdleWatcher`; this context owns its lifecycle.
        #    ``start`` is a no-op when the timer is disabled.
        if idle is not None:
            await idle.start()
            stack.push_async_callback(idle.stop)

        context = LifespanContext(
            multi_config=multi_config,
            registry=registry,
            instance_tracker=tracker,
        )
        holder.context = context
        stack.callback(lambda: setattr(holder, "context", None))
        _LOGGER.info(
            "[process_lifespan] Process-scoped systems-server resources ready."
        )
        try:
            yield context
        finally:
            _LOGGER.info(
                "[process_lifespan] Shutting down process-scoped systems-server resources"
            )
    _LOGGER.info(
        "[process_lifespan] Process-scoped systems-server resources shut down."
    )


def make_lifespan(
    holder: ProcessResources,
) -> Callable[[FastMCP[LifespanContext]], AbstractAsyncContextManager[LifespanContext]]:
    """Build the per-MCP-session FastMCP lifespan for the systems server.

    The returned context manager yields the process-scoped
    :class:`LifespanContext` that :func:`process_lifespan` stored on
    ``holder``; it builds and tears down nothing of its own, so every MCP
    session served by the process shares the one registry and subsystems.

    Args:
        holder (ProcessResources): Holder populated by
            :func:`process_lifespan` before the transport begins serving.

    Returns:
        Callable[[FastMCP[LifespanContext]], AbstractAsyncContextManager[LifespanContext]]:
        An async context manager suitable for passing to
        ``FastMCP(..., lifespan=...)``.
    """

    @asynccontextmanager
    async def _session_lifespan(
        _server: FastMCP[LifespanContext],
    ) -> AsyncIterator[LifespanContext]:
        if holder.context is None:
            raise InternalError(
                "Per-session lifespan entered before process-scoped resources "
                "were built; process_lifespan must wrap the transport run."
            )
        yield holder.context

    return _session_lifespan


# ---------------------------------------------------------------------------
# Best-effort teardown helper
# ---------------------------------------------------------------------------


async def _log_teardown_failure(awaitable: Awaitable[None], *, label: str) -> None:
    """Await a teardown coroutine; log any failure instead of raising.

    Used by the :class:`AsyncExitStack` callbacks for best-effort shutdown
    steps (tracker unregister, registry close, ...); failures are logged at
    ERROR and never propagate.

    Args:
        awaitable (Awaitable[None]): The teardown coroutine, e.g.
            ``tracker.unregister()`` or ``registry.close()``.
        label (str): Short label for the operation, included in the error log.
    """
    try:
        await awaitable
    except Exception:
        _LOGGER.exception(f"[_lifespan:_log_teardown_failure] Error during {label}")
