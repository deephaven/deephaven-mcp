"""MCP Server Lifespan Factories.

Provides lifespan context managers for the DHE and DHC FastMCP servers:

- :func:`make_enterprise_lifespan`: Lifespan factory for the DHE MCP server.
- :func:`make_community_lifespan`: Lifespan factory for the DHC MCP server.
- :class:`LifespanContext`: ``TypedDict`` describing the context object
  yielded by both lifespans.

Both factories yield the same context keys
(``config_manager``, ``registry``, ``evictor``, ``refresh_lock``,
``instance_tracker``) so all shared tools work without modification in
either server context.  The ``registry`` is a single process-wide
instance shared across all MCP clients; the ``evictor`` is its idle
eviction coordinator.
"""

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TypedDict

from mcp.server.fastmcp import FastMCP

from deephaven_mcp.config import (
    CommunityServerConfigManager,
    ConfigManager,
    EnterpriseServerConfigManager,
)
from deephaven_mcp.resource_manager import (
    BaseRegistry,
    CommunitySessionRegistry,
    EnterpriseSessionRegistry,
    Evictor,
    InstanceTracker,
    cleanup_orphaned_resources,
)

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "LifespanContext",
    "make_enterprise_lifespan",
    "make_community_lifespan",
]


class LifespanContext(TypedDict):
    """Typed dictionary yielded by the MCP server lifespan context managers.

    Attributes:
        config_manager (ConfigManager): The server-wide configuration manager
            (``CommunityServerConfigManager`` for DHC, ``EnterpriseServerConfigManager``
            for DHE).
        registry (BaseRegistry): The single process-wide Deephaven registry
            shared across all MCP clients.  Tools narrow this to
            ``CommunitySessionRegistry`` or ``EnterpriseSessionRegistry``
            with ``isinstance``.
        evictor (Evictor): The idle-eviction coordinator for ``registry``.
            Internal to the systems server — only :mod:`mcp_reload` uses
            it (to stop/start the sweep loop around a config reload).
        refresh_lock (asyncio.Lock): Lock acquired by the ``mcp_reload`` tools
            to serialize concurrent config-reload requests against each other.
            Other tools do not acquire this lock and run concurrently with
            reloads.
        instance_tracker (InstanceTracker): Tracks this server instance for
            orphan-resource cleanup at startup and shutdown.
    """

    config_manager: ConfigManager
    registry: BaseRegistry
    evictor: Evictor
    refresh_lock: asyncio.Lock
    instance_tracker: InstanceTracker


def _make_lifespan(
    config_manager_class: type[ConfigManager],
    registry_class: type[BaseRegistry],
    idle_timeout_seconds: float | None,
    sweep_interval_seconds: float | None,
    label: str,
    config_path: str | None,
) -> Callable[[FastMCP[LifespanContext]], AbstractAsyncContextManager[LifespanContext]]:
    """Create a lifespan context manager shared by community and enterprise servers.

    Lifecycle of the returned context manager:

    1. **Startup** (before ``yield``):

       a. Create an :class:`InstanceTracker` and register this server instance.
       b. Run :func:`cleanup_orphaned_resources` to reclaim resources left
          behind by previously crashed instances.
       c. Instantiate ``config_manager_class(config_path=config_path)`` and
          eagerly load the configuration (so config errors surface during
          startup rather than on first tool call).
       d. Construct the single process-wide registry of type
          ``registry_class`` and call ``initialize()`` on it.
       e. Construct an :class:`Evictor` for the registry with the
          configured ``idle_timeout_seconds`` and ``sweep_interval_seconds``
          and start its sweeper loop.
       f. Create a fresh ``refresh_lock`` for the ``mcp_reload`` tools.
       g. Yield a :class:`LifespanContext` containing the five objects
          above.

    2. **Shutdown** (in ``finally``, runs on both clean shutdown and
       startup failure):

       a. ``await evictor.stop()`` if the evictor was constructed;
          errors are logged and swallowed so the next step runs.
       b. ``await registry.close()`` if the registry was initialized;
          errors are logged and swallowed so step (c) still runs.
       c. ``await instance_tracker.unregister()`` if the tracker was
          successfully created in step 1a; errors are logged and
          swallowed.

    Args:
        config_manager_class (type[ConfigManager]): The config manager class
            to instantiate during startup step 1c.
        registry_class (type[BaseRegistry]): The concrete ``BaseRegistry``
            subclass to instantiate as the single process-wide registry.
        idle_timeout_seconds (float | None): Per-Deephaven-session idle
            timeout passed into the :class:`Evictor`.  ``None`` disables
            the sweeper entirely.
        sweep_interval_seconds (float | None): Sweeper cadence passed
            into the :class:`Evictor`.  ``None`` disables the sweeper
            entirely (same effect as ``idle_timeout_seconds=None``).
        label (str): Server label used in log messages (``"community"`` or
            ``"enterprise"``).
        config_path (str | None): Explicit path to the config file, or
            ``None`` to fall back to the ``DH_MCP_CONFIG_FILE`` environment
            variable (resolution is performed by ``config_manager_class``).

    Returns:
        Callable[[FastMCP[LifespanContext]], AbstractAsyncContextManager[LifespanContext]]:
        An async context manager suitable for passing to
        ``FastMCP(..., lifespan=...)``.
    """

    @asynccontextmanager
    async def _lifespan(
        server: FastMCP[LifespanContext],
    ) -> AsyncIterator[LifespanContext]:
        _LOGGER.info(
            f"[{label}_lifespan] Starting {label.upper()} MCP server '{server.name}'"
        )
        instance_tracker = None
        registry: BaseRegistry | None = None
        evictor: Evictor | None = None
        try:
            instance_tracker = await InstanceTracker.create_and_register()
            _LOGGER.info(
                f"[{label}_lifespan] Server instance: {instance_tracker.instance_id}"
            )
            await cleanup_orphaned_resources()

            config_manager = config_manager_class(config_path=config_path)
            _LOGGER.info(f"[{label}_lifespan] Loading {label} configuration...")
            await config_manager.get_config()
            _LOGGER.info(
                f"[{label}_lifespan] {label.capitalize()} configuration loaded."
            )

            registry = registry_class()
            await registry.initialize(config_manager)

            evictor = Evictor(
                registry,
                idle_timeout_seconds=idle_timeout_seconds,
                sweep_interval_seconds=sweep_interval_seconds,
            )
            await evictor.start()

            refresh_lock = asyncio.Lock()

            _LOGGER.info(
                f"[{label}_lifespan] {label.upper()} MCP server '{server.name}' ready."
            )
            yield LifespanContext(
                config_manager=config_manager,
                registry=registry,
                evictor=evictor,
                refresh_lock=refresh_lock,
                instance_tracker=instance_tracker,
            )
        finally:
            _LOGGER.info(
                f"[{label}_lifespan] Shutting down {label.upper()} MCP server '{server.name}'"
            )
            if evictor is not None:
                try:
                    await evictor.stop()
                except Exception:
                    _LOGGER.exception(f"[{label}_lifespan] Error stopping evictor")
            if registry is not None:
                try:
                    await registry.close()
                except Exception:
                    _LOGGER.exception(f"[{label}_lifespan] Error closing registry")
            if instance_tracker is not None:
                try:
                    await instance_tracker.unregister()
                except Exception:
                    _LOGGER.exception(
                        f"[{label}_lifespan] Error unregistering instance_tracker"
                    )
            _LOGGER.info(
                f"[{label}_lifespan] {label.upper()} MCP server '{server.name}' shut down."
            )

    return _lifespan


def make_enterprise_lifespan(
    idle_timeout_seconds: float | None = None,
    sweep_interval_seconds: float | None = None,
    config_path: str | None = None,
) -> Callable[[FastMCP[LifespanContext]], AbstractAsyncContextManager[LifespanContext]]:
    """Create a FastMCP lifespan for the DHE MCP server.

    The returned lifespan initializes an :class:`EnterpriseServerConfigManager`
    that reads the DHE config file, constructs a single shared
    :class:`EnterpriseSessionRegistry` for the server process, and an
    :class:`Evictor` that runs idle eviction for it.

    Args:
        idle_timeout_seconds (float | None): Per-Deephaven-session idle
            timeout for the Evictor's sweeper.  ``None`` disables the
            sweeper entirely.
        sweep_interval_seconds (float | None): Sweeper cadence for the
            Evictor.  ``None`` disables the sweeper entirely.
        config_path (str | None): Explicit path to the DHE config file.
            If ``None``, the ``DH_MCP_CONFIG_FILE`` environment variable is
            used.

    Returns:
        Callable[[FastMCP[LifespanContext]], AbstractAsyncContextManager[LifespanContext]]: An async
        context manager suitable for passing to ``FastMCP(..., lifespan=...)``.
    """
    return _make_lifespan(
        EnterpriseServerConfigManager,
        EnterpriseSessionRegistry,
        idle_timeout_seconds,
        sweep_interval_seconds,
        "enterprise",
        config_path,
    )


def make_community_lifespan(
    idle_timeout_seconds: float | None = None,
    sweep_interval_seconds: float | None = None,
    config_path: str | None = None,
) -> Callable[[FastMCP[LifespanContext]], AbstractAsyncContextManager[LifespanContext]]:
    """Create a FastMCP lifespan for the DHC MCP server.

    The returned lifespan initializes a :class:`CommunityServerConfigManager`
    that reads the DHC config file, constructs a single shared
    :class:`CommunitySessionRegistry` for the server process, and an
    :class:`Evictor` that runs idle eviction for it.

    Args:
        idle_timeout_seconds (float | None): Per-Deephaven-session idle
            timeout for the Evictor's sweeper.  ``None`` disables the
            sweeper entirely.
        sweep_interval_seconds (float | None): Sweeper cadence for the
            Evictor.  ``None`` disables the sweeper entirely.
        config_path (str | None): Explicit path to the DHC config file.
            If ``None``, the ``DH_MCP_CONFIG_FILE`` environment variable is
            used.

    Returns:
        Callable[[FastMCP[LifespanContext]], AbstractAsyncContextManager[LifespanContext]]: An async
        context manager suitable for passing to ``FastMCP(..., lifespan=...)``.
    """
    return _make_lifespan(
        CommunityServerConfigManager,
        CommunitySessionRegistry,
        idle_timeout_seconds,
        sweep_interval_seconds,
        "community",
        config_path,
    )
