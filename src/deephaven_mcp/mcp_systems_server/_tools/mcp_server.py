"""
MCP Server Infrastructure - FastMCP Server Instance and Configuration Management.

Provides core MCP server infrastructure:
- mcp_server: The FastMCP server instance with registered tools
- app_lifespan: Application lifecycle manager for resource cleanup
- mcp_reload: Tool to reload server configuration without restart

This module initializes the MCP server and registers all Deephaven tools.
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from mcp.server.fastmcp import Context, FastMCP

from deephaven_mcp.config import ConfigManager
from deephaven_mcp.resource_manager import CombinedSessionRegistry
from deephaven_mcp.resource_manager._instance_tracker import (
    InstanceTracker,
    cleanup_orphaned_resources,
)

_LOGGER = logging.getLogger(__name__)



@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[dict[str, object]]:
    """
    Async context manager for the FastMCP server application lifespan.

    This function manages the startup and shutdown lifecycle of the MCP server. It is responsible for:
      - Instantiating a ConfigManager and CombinedSessionRegistry for Deephaven session configuration and session management.
      - Creating a coroutine-safe asyncio.Lock (refresh_lock) for atomic configuration/session refreshes.
      - Loading and validating the Deephaven session configuration before the server accepts requests.
      - Yielding a context dictionary containing config_manager, session_registry, and refresh_lock for use by all tool functions via dependency injection.
      - Ensuring all session resources are properly cleaned up on shutdown.

    Startup Process:
      - Logs server startup initiation.
      - Creates and initializes a ConfigManager instance.
      - Loads and validates the Deephaven session configuration.
      - Creates a CombinedSessionRegistry for managing both community and enterprise sessions.
      - Creates an asyncio.Lock for coordinating refresh operations.
      - Yields the context dictionary for use by MCP tools.

    Shutdown Process:
      - Logs server shutdown initiation.
      - Closes all active Deephaven sessions via the session registry.
      - For dynamically created community sessions, stops Docker containers or python processes.
      - Logs completion of server shutdown.
      - This cleanup runs on normal shutdown, SIGTERM, or SIGINT signals.

    Args:
        server (FastMCP): The FastMCP server instance (required by the FastMCP lifespan API).

    Yields:
        dict[str, object]: A context dictionary with the following keys for dependency injection into MCP tool requests:
            - 'config_manager' (ConfigManager): Instance for accessing session configuration.
            - 'session_registry' (CombinedSessionRegistry): Instance for managing all session types.
            - 'refresh_lock' (asyncio.Lock): Lock for atomic refresh operations across tools.
            - 'instance_tracker' (InstanceTracker): Instance tracker for managing server instance lifecycle.
    """
    _LOGGER.info(
        f"[mcp_systems_server:app_lifespan] Starting MCP server '{server.name}'"
    )
    session_registry = None
    instance_tracker = None

    try:
        # Register this server instance for tracking
        instance_tracker = await InstanceTracker.create_and_register()
        _LOGGER.info(
            f"[mcp_systems_server:app_lifespan] Server instance: {instance_tracker.instance_id}"
        )

        # Clean up orphaned resources from previous crashed/killed instances
        await cleanup_orphaned_resources()

        config_manager = ConfigManager()

        # Make sure config can be loaded before starting
        _LOGGER.info("[mcp_systems_server:app_lifespan] Loading configuration...")
        await config_manager.get_config()
        _LOGGER.info("[mcp_systems_server:app_lifespan] Configuration loaded.")

        session_registry = CombinedSessionRegistry()
        await session_registry.initialize(config_manager)

        # lock for refresh to prevent concurrent refresh operations.
        refresh_lock = asyncio.Lock()

        yield {
            "config_manager": config_manager,
            "session_registry": session_registry,
            "refresh_lock": refresh_lock,
            "instance_tracker": instance_tracker,
        }
    finally:
        _LOGGER.info(
            f"[mcp_systems_server:app_lifespan] Shutting down MCP server '{server.name}'"
        )
        if session_registry is not None:
            await session_registry.close()
        if instance_tracker is not None:
            await instance_tracker.unregister()
        _LOGGER.info(
            f"[mcp_systems_server:app_lifespan] MCP server '{server.name}' shut down."
        )


mcp_server = FastMCP("deephaven-mcp-systems", lifespan=app_lifespan)
"""
FastMCP Server Instance for Deephaven MCP Systems Tools.

This is the singleton FastMCP server for the Deephaven MCP systems toolset. It manages
registration and exposure of MCP tool functions to the MCP runtime environment.

All functions decorated with @mcp_server.tool() across the _tools package are automatically
registered as MCP tools and made available for remote invocation.

Key Details:
    - Server name: 'deephaven-mcp-systems' (unique identifier in the MCP ecosystem)
    - Lifespan manager: app_lifespan (handles startup/shutdown and resource cleanup)
    - Tools are registered via @mcp_server.tool() decorator across multiple modules
    - Singleton instance - should not be instantiated more than once per process

Usage:
    - Use @mcp_server.tool() decorator to register new tools
    - MCP runtime discovers and invokes registered tools automatically
    - Do not call methods on mcp_server directly
"""



# TODO: remove mcp_reload?
@mcp_server.tool()
async def mcp_reload(context: Context) -> dict:
    """
    MCP Tool: Reload configuration and clear all active sessions.

    Reloads the Deephaven session configuration from disk and clears all active session objects.
    Configuration changes (adding, removing, or updating systems) are applied immediately.
    All sessions will be reopened with the new configuration on next access.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Use this tool after making configuration file changes
    - Check 'success' field to verify reload completed
    - Sessions will be automatically recreated with new configuration on next use
    - Operation is atomic and thread-safe
    - WARNING: All active sessions will be cleared, including those created with session_enterprise_create and session_community_create
    - Use carefully - any work in active sessions will be lost

    Args:
        context (Context): The MCP context object.

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): True if the refresh completed successfully, False otherwise.
            - 'error' (str, optional): Error message if the refresh failed. Omitted on success.
            - 'isError' (bool, optional): Present and True if this is an error response (i.e., success is False).

    Example Successful Response:
        {'success': True}

    Example Error Response:
        {'success': False, 'error': 'Failed to reload configuration: ...', 'isError': True}

    Error Scenarios:
        - Context access errors: Returns error if required context objects (refresh_lock, config_manager, session_registry) are not available
        - Configuration reload errors: Returns error if config_manager.clear_config_cache() fails
        - Session registry errors: Returns error if session_registry operations (close, initialize) fail
    """
    _LOGGER.info(
        "[mcp_systems_server:mcp_reload] Invoked: refreshing session configuration and session cache."
    )
    # Acquire the refresh lock to prevent concurrent refreshes. This does not
    # guarantee atomicity with respect to other config/session operations, but
    # it does ensure that only one refresh runs at a time and reduces race risk.
    try:
        refresh_lock: asyncio.Lock = context.request_context.lifespan_context[
            "refresh_lock"
        ]
        config_manager: ConfigManager = context.request_context.lifespan_context[
            "config_manager"
        ]
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )

        async with refresh_lock:
            await config_manager.clear_config_cache()
            await session_registry.close()
            await session_registry.initialize(config_manager)
        _LOGGER.info(
            "[mcp_systems_server:mcp_reload] Success: Session configuration and session cache have been reloaded."
        )
        return {"success": True}
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:mcp_reload] Failed to refresh session configuration/session cache: {e!r}",
            exc_info=True,
        )
        return {"success": False, "error": str(e), "isError": True}


