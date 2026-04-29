"""MCP Server Lifespan Factories.

Provides lifespan context managers for the DHE and DHC FastMCP servers:

- :func:`make_enterprise_lifespan`: Lifespan factory for the DHE MCP server.
- :func:`make_community_lifespan`: Lifespan factory for the DHC MCP server.
- :class:`LifespanContext`: ``TypedDict`` describing the context object
  yielded by both lifespans.

Both factories yield the same context keys
(``config_manager``, ``session_registry_manager``, ``refresh_lock``,
``instance_tracker``) so all shared tools work without modification in
either server context.
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
from deephaven_mcp.mcp_systems_server._session_registry_manager import SessionRegistryManager
from deephaven_mcp.resource_manager import (
    BaseRegistry,
    InstanceTracker,
    cleanup_orphaned_resources,
)

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "LifespanContext",
    "make_enterprise_lifespan",
    "make_community_lifespan",
]


class LifespanContext[R: BaseRegistry](TypedDict):
    """Typed dictionary yielded by the MCP server lifespan context managers.

    Parameterized by the registry type ``R`` so that consumers know whether
    ``session_registry_manager`` produces
    :class:`~deephaven_mcp.resource_manager.CommunitySessionRegistry` or
    :class:`~deephaven_mcp.resource_manager.EnterpriseSessionRegistry`
    instances per MCP session.

    Attributes:
        config_manager (ConfigManager): The server-wide configuration manager
            (``CommunityServerConfigManager`` for DHC, ``EnterpriseServerConfigManager``
            for DHE).
        session_registry_manager (SessionRegistryManager[R]): Manages per-MCP-session
            registries of type ``R``.
        refresh_lock (asyncio.Lock): Lock acquired by the ``mcp_reload`` tools
            to serialize concurrent config-reload requests against each other.
            Other tools do not acquire this lock and run concurrently with
            reloads.
        instance_tracker (InstanceTracker): Tracks this server instance for
            orphan-resource cleanup at startup and shutdown.
    """

    config_manager: ConfigManager
    session_registry_manager: SessionRegistryManager[R]
    refresh_lock: asyncio.Lock
    instance_tracker: InstanceTracker


def _make_lifespan[R: BaseRegistry](
    config_manager_class: type[ConfigManager],
    session_registry_manager: SessionRegistryManager[R],
    label: str,
    config_path: str | None,
) -> Callable[[FastMCP[LifespanContext[R]]], AbstractAsyncContextManager[LifespanContext[R]]]:
    """Create a lifespan context manager shared by community and enterprise servers.

    Lifecycle of the returned context manager:

    1. **Startup** (before ``yield``):

       a. Create an :class:`InstanceTracker` and register this server instance.
       b. Run :func:`cleanup_orphaned_resources` to reclaim resources left
          behind by previously crashed instances.
       c. Instantiate ``config_manager_class(config_path=config_path)`` and
          eagerly load the configuration (so config errors surface during
          startup rather than on first tool call).
       d. Start ``session_registry_manager`` (launches its TTL sweeper).
       e. Create a fresh ``refresh_lock`` for the ``mcp_reload`` tools.
       f. Yield a :class:`LifespanContext` containing the four objects
          above.

    2. **Shutdown** (in ``finally``, runs on both clean shutdown and
       startup failure):

       a. ``await session_registry_manager.stop()``; errors are logged
          and swallowed so that step (b) still runs.
       b. ``await instance_tracker.unregister()`` if the tracker was
          successfully created in step 1a; errors are logged and
          swallowed.

    Args:
        config_manager_class (type[ConfigManager]): The config manager class
            to instantiate during startup step 1c.
        session_registry_manager (SessionRegistryManager[R]): The per-session
            registry manager to start (1d) and stop (2a) with the server
            lifespan.
        label (str): Server label used in log messages (``"community"`` or
            ``"enterprise"``).
        config_path (str | None): Explicit path to the config file, or
            ``None`` to fall back to the ``DH_MCP_CONFIG_FILE`` environment
            variable (resolution is performed by ``config_manager_class``).

    Returns:
        Callable[[FastMCP[LifespanContext[R]]], AbstractAsyncContextManager[LifespanContext[R]]]:
        An async context manager suitable for passing to
        ``FastMCP(..., lifespan=...)``.
    """

    @asynccontextmanager
    async def _lifespan(server: FastMCP[LifespanContext[R]]) -> AsyncIterator[LifespanContext[R]]:
        _LOGGER.info(
            f"[{label}_lifespan] Starting {label.upper()} MCP server '{server.name}'"
        )
        instance_tracker = None
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

            await session_registry_manager.start()

            refresh_lock = asyncio.Lock()

            _LOGGER.info(
                f"[{label}_lifespan] {label.upper()} MCP server '{server.name}' ready."
            )
            yield LifespanContext(
                config_manager=config_manager,
                session_registry_manager=session_registry_manager,
                refresh_lock=refresh_lock,
                instance_tracker=instance_tracker,
            )
        finally:
            _LOGGER.info(
                f"[{label}_lifespan] Shutting down {label.upper()} MCP server '{server.name}'"
            )
            try:
                await session_registry_manager.stop()
            except Exception:
                _LOGGER.exception(
                    f"[{label}_lifespan] Error stopping session_registry_manager"
                )
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


def make_enterprise_lifespan[R: BaseRegistry](
    session_registry_manager: SessionRegistryManager[R],
    config_path: str | None = None,
) -> Callable[[FastMCP[LifespanContext[R]]], AbstractAsyncContextManager[LifespanContext[R]]]:
    """Create a FastMCP lifespan for the DHE MCP server.

    The returned lifespan initializes an :class:`EnterpriseServerConfigManager` that reads
    the DHE config file and starts the given :class:`SessionRegistryManager` for per-session
    registry management.

    Args:
        session_registry_manager (SessionRegistryManager[R]): The per-session registry manager to start
            and stop with the server lifespan.
        config_path (str | None): Explicit path to the DHE config file.
            If ``None``, the ``DH_MCP_CONFIG_FILE`` environment variable is used.

    Returns:
        Callable[[FastMCP[LifespanContext[R]]], AbstractAsyncContextManager[LifespanContext[R]]]: An async
        context manager suitable for passing to ``FastMCP(..., lifespan=...)``.
    """
    return _make_lifespan(
        EnterpriseServerConfigManager,
        session_registry_manager,
        "enterprise",
        config_path,
    )


def make_community_lifespan[R: BaseRegistry](
    session_registry_manager: SessionRegistryManager[R],
    config_path: str | None = None,
) -> Callable[[FastMCP[LifespanContext[R]]], AbstractAsyncContextManager[LifespanContext[R]]]:
    """Create a FastMCP lifespan for the DHC MCP server.

    The returned lifespan initializes a :class:`CommunityServerConfigManager` that reads
    the DHC config file and starts the given :class:`SessionRegistryManager` for per-session
    registry management.

    Args:
        session_registry_manager (SessionRegistryManager[R]): The per-session registry manager to start
            and stop with the server lifespan.
        config_path (str | None): Explicit path to the DHC config file.
            If ``None``, the ``DH_MCP_CONFIG_FILE`` environment variable is used.

    Returns:
        Callable[[FastMCP[LifespanContext[R]]], AbstractAsyncContextManager[LifespanContext[R]]]: An async
        context manager suitable for passing to ``FastMCP(..., lifespan=...)``.
    """
    return _make_lifespan(
        CommunityServerConfigManager, session_registry_manager, "community", config_path
    )
