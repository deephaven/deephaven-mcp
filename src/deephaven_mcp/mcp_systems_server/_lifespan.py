"""MCP Server Lifespan Factories.

Provides lifespan context managers for the DHE and DHC FastMCP servers:

- ``make_enterprise_lifespan(config_path)``: Lifespan factory for the DHE MCP server.
- ``make_community_lifespan(config_path)``: Lifespan factory for the DHC MCP server.

Both factories yield the same context dict keys
(``config_manager``, ``session_registry``, ``refresh_lock``, ``instance_tracker``)
so all shared tools work without modification in either server context.
"""

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

from mcp.server.fastmcp import FastMCP

from deephaven_mcp.config import (
    CommunityServerConfigManager,
    EnterpriseServerConfigManager,
)
from deephaven_mcp.resource_manager import (
    CommunitySessionRegistry,
    EnterpriseSessionRegistry,
)
from deephaven_mcp.resource_manager._instance_tracker import (
    InstanceTracker,
    cleanup_orphaned_resources,
)

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "make_enterprise_lifespan",
    "make_community_lifespan",
]


def _make_lifespan(
    config_manager_class: type,
    registry_class: type,
    label: str,
    config_path: str | None,
) -> Callable[[FastMCP], AbstractAsyncContextManager[dict[str, Any]]]:
    """Generic lifespan factory shared by community and enterprise servers.

    Args:
        config_manager_class (type): The config manager class to instantiate.
        registry_class (type): The session registry class to instantiate.
        label (str): Server label used in log messages (``"community"`` or ``"enterprise"``).
        config_path (str | None): Explicit path to the config file, or ``None`` to fall back
            to the ``DH_MCP_CONFIG_FILE`` environment variable.

    Returns:
        Callable[[FastMCP], AbstractAsyncContextManager[dict[str, Any]]]: An async context
        manager suitable for passing to ``FastMCP(..., lifespan=...)``.
    """

    @asynccontextmanager
    async def _lifespan(server: FastMCP) -> AsyncIterator[dict[str, Any]]:
        _LOGGER.info(
            f"[{label}_lifespan] Starting {label.upper()} MCP server '{server.name}'"
        )
        session_registry = None
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

            session_registry = registry_class()
            await session_registry.initialize(config_manager)

            refresh_lock = asyncio.Lock()

            yield {
                "config_manager": config_manager,
                "session_registry": session_registry,
                "refresh_lock": refresh_lock,
                "instance_tracker": instance_tracker,
            }
        finally:
            _LOGGER.info(
                f"[{label}_lifespan] Shutting down {label.upper()} MCP server '{server.name}'"
            )
            if session_registry is not None:
                try:
                    await session_registry.close()
                except Exception:
                    _LOGGER.exception(
                        f"[{label}_lifespan] Error closing session_registry"
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


def make_enterprise_lifespan(
    config_path: str | None = None,
) -> Callable[[FastMCP], AbstractAsyncContextManager[dict[str, Any]]]:
    """Create a FastMCP lifespan for the DHE MCP server.

    The returned lifespan initializes an :class:`EnterpriseServerConfigManager` that reads
    the DHE config file and an :class:`EnterpriseSessionRegistry`.

    Args:
        config_path (str | None): Explicit path to the DHE config file.
            If ``None``, the ``DH_MCP_CONFIG_FILE`` environment variable is used.

    Returns:
        Callable[[FastMCP], AbstractAsyncContextManager[dict[str, Any]]]: An async context
        manager suitable for passing to ``FastMCP(..., lifespan=...)``.
    """
    return _make_lifespan(
        EnterpriseServerConfigManager,
        EnterpriseSessionRegistry,
        "enterprise",
        config_path,
    )


def make_community_lifespan(
    config_path: str | None = None,
) -> Callable[[FastMCP], AbstractAsyncContextManager[dict[str, Any]]]:
    """Create a FastMCP lifespan for the DHC MCP server.

    The returned lifespan initializes a :class:`CommunityServerConfigManager` that reads
    the DHC config file and a :class:`CommunitySessionRegistry`.

    Args:
        config_path (str | None): Explicit path to the DHC config file.
            If ``None``, the ``DH_MCP_CONFIG_FILE`` environment variable is used.

    Returns:
        Callable[[FastMCP], AbstractAsyncContextManager[dict[str, Any]]]: An async context
        manager suitable for passing to ``FastMCP(..., lifespan=...)``.
    """
    return _make_lifespan(
        CommunityServerConfigManager, CommunitySessionRegistry, "community", config_path
    )
