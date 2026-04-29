"""MCP Reload Tool - Reload Configuration and Session Cache.

Provides the ``mcp_reload`` tool in two server-specific variants:
- ``mcp_reload_community``: registered on the DHC server; dynamic sessions are permanently destroyed.
- ``mcp_reload_enterprise``: registered on the DHE server; sessions are rebuilt from the controller.

Both variants share the same implementation but carry backend-accurate docstrings so that an AI
agent sees exactly one ``mcp_reload`` tool whose description is correct for its backend.
"""

import asyncio
import logging

from mcp.server.fastmcp import Context, FastMCP

from deephaven_mcp.mcp_systems_server._session_registry_manager import SessionRegistryManager
from deephaven_mcp.mcp_systems_server._tools.shared import get_config_manager, get_mcp_session_id

_LOGGER = logging.getLogger(__name__)


async def _do_reload(context: Context) -> dict:
    _LOGGER.info(
        "[mcp_systems_server:mcp_reload] Invoked: refreshing session configuration and session cache."
    )
    try:
        mcp_session_id = get_mcp_session_id(context)
        refresh_lock: asyncio.Lock = context.request_context.lifespan_context[
            "refresh_lock"
        ]
        session_registry_manager: SessionRegistryManager = context.request_context.lifespan_context[
            "session_registry_manager"
        ]

        async with refresh_lock:
            await get_config_manager(context).clear_config_cache()
            await session_registry_manager.close_session(mcp_session_id)
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


async def mcp_reload_community(context: Context) -> dict:
    """MCP Tool: Reload configuration and reset all Community sessions.

    Reloads the Deephaven Community session configuration from disk and resets the session
    registry for the current MCP session. Configuration changes (adding, removing, or
    updating systems) are applied immediately.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')

    Session Behavior (Community):
    - Dynamic sessions created with session_community_create are MCP-managed subprocesses.
      These processes are PERMANENTLY TERMINATED on reload and do not survive.
    - After reload, only sessions defined in the configuration file are available.
    - Any work in progress in dynamic sessions will be lost.
    - Config-defined static sessions will be lazily reconnected on next use.
    - Other MCP client sessions are unaffected by this reload.

    AI Agent Usage:
    - Use this tool after making configuration file changes
    - Check 'success' field to verify reload completed
    - Operation is atomic and thread-safe
    - WARNING: All dynamic sessions created with session_community_create are permanently
      destroyed. Any in-progress work in those sessions will be lost.
    - Use carefully - only config-defined sessions will be available after reload

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
        {'success': False, 'error': '<exception message>', 'isError': True}

    Error Scenarios:
        - Context access errors: Returns error if required context objects (refresh_lock, config_manager, session_registry_manager) are not available
        - Configuration reload errors: Returns error if config_manager.clear_config_cache() fails
        - Session registry errors: Returns error if session_registry_manager.close_session() fails
    """
    return await _do_reload(context)


async def mcp_reload_enterprise(context: Context) -> dict:
    """MCP Tool: Reload configuration and refresh the Enterprise session list from the controller.

    Reloads the Deephaven Enterprise configuration from disk and closes the current MCP
    session's registry. On the next tool call, the registry is lazily rebuilt by re-querying
    the DHE controller. Configuration changes (updating connection details, credentials, etc.)
    are applied immediately. Other MCP client sessions are unaffected.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')
    - A 'Persistent Query' (PQ) is a long-running DHE worker that the controller manages

    Session Behavior (Enterprise):
    - Enterprise sessions are views into controller-owned Persistent Queries (PQs). The DHE
      controller owns the PQ lifecycle — MCP does not create or destroy PQs during reload.
    - On reload, local session handles are closed and the session list is rebuilt by querying
      the controller. All currently running PQs reappear automatically after re-discovery.
    - Sessions created with session_enterprise_create correspond to PQs that keep running on
      the controller; they will reappear in the session list after reload completes.
    - This tool provides a config refresh and controller re-sync, NOT a clean-slate wipe of
      sessions. To delete a specific session, use session_enterprise_delete instead.

    AI Agent Usage:
    - Use this tool after making configuration file changes
    - Check 'success' field to verify reload completed
    - Operation is atomic and thread-safe
    - After reload, background re-discovery runs asynchronously; the full session list may not
      be immediately available — call sessions_list and check the 'initialization' field to monitor discovery progress
    - NOTE: This does NOT destroy enterprise PQs. All controller-owned PQs will reappear
      after re-discovery. To get a clean slate, delete individual sessions with
      session_enterprise_delete before reloading.

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
        {'success': False, 'error': '<exception message>', 'isError': True}

    Error Scenarios:
        - Context access errors: Returns error if required context objects (refresh_lock, config_manager, session_registry_manager) are not available
        - Configuration reload errors: Returns error if config_manager.clear_config_cache() fails
        - Session registry errors: Returns error if session_registry_manager.close_session() fails
    """
    return await _do_reload(context)


def register_community_tools(server: FastMCP) -> None:
    """Register the Community reload tool with the given FastMCP server.

    Args:
        server (FastMCP): The server to register tools with.
    """
    server.tool(name="mcp_reload")(mcp_reload_community)


def register_enterprise_tools(server: FastMCP) -> None:
    """Register the Enterprise reload tool with the given FastMCP server.

    Args:
        server (FastMCP): The server to register tools with.
    """
    server.tool(name="mcp_reload")(mcp_reload_enterprise)
