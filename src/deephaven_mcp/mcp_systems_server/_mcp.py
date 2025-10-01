"""
Deephaven MCP Systems Tools Module.

This module defines the set of MCP (Multi-Cluster Platform) tool functions for managing and interacting with Deephaven sessions in a multi-server environment. All functions are designed for use as MCP tools and are decorated with @mcp_server.tool().

Key Features:
    - Structured, protocol-compliant error handling: all tools return consistent dict structures with 'success' and 'error' keys as appropriate.
    - Async, coroutine-safe operations for configuration and session management.
    - Detailed logging for all tool invocations, results, and errors.
    - All docstrings are optimized for agentic and programmatic consumption and describe both user-facing and technical details.

Tools Provided:
    - refresh: Reload configuration and clear all sessions atomically.
    - enterprise_systems_status: List all enterprise (CorePlus) systems with their status and configuration details.
    - list_sessions: List all sessions (community and enterprise) with basic metadata.
    - get_session_details: Get detailed information about a specific session.
    - table_schemas: Retrieve schemas for one or more tables from a session (requires session_id).
    - run_script: Execute a script on a specified Deephaven session (requires session_id).
    - pip_packages: Retrieve all installed pip packages (name and version) from a specified Deephaven session using importlib.metadata, returned as a list of dicts.
    - get_table_data: Retrieve table data with flexible formatting (json-row, json-column, csv) and optional row limiting for safe access to large tables.
    - get_table_meta: Retrieve table metadata/schema information as structured data describing column types and properties.
    - create_enterprise_session: Create a new enterprise session with configurable parameters and resource limits.
    - delete_enterprise_session: Delete an existing enterprise session and remove it from the session registry.

Return Types:
    - All tools return structured dict objects, never raise exceptions to the MCP layer.
    - On success, 'success': True. On error, 'success': False and 'error': str.
    - Tools that return multiple items use nested structures (e.g., 'systems', 'sessions', 'schemas' arrays within the main dict).

See individual tool docstrings for full argument, return, and error details.
"""

import asyncio
import io
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, TypeVar

import aiofiles
import pyarrow as pa
import pyarrow.csv as csv
from mcp.server.fastmcp import Context, FastMCP

from deephaven_mcp import queries
from deephaven_mcp.client import BaseSession, CorePlusSession
from deephaven_mcp.config import (
    ConfigManager,
    get_config_section,
    redact_enterprise_system_config,
)
from deephaven_mcp.resource_manager import (
    BaseItemManager,
    CombinedSessionRegistry,
    EnterpriseSessionManager,
    SystemType,
)

T = TypeVar("T")

# Enterprise session management constants
DEFAULT_MAX_CONCURRENT_SESSIONS = 5
"""
Default maximum number of concurrent sessions per enterprise system.

This default is used when session_creation.max_concurrent_sessions is not specified
in the enterprise system configuration. Can be overridden per system in the config.
"""

# Response size estimation constants
# Conservative estimate: ~20 chars + 8 bytes numeric + JSON overhead + safety margin
ESTIMATED_BYTES_PER_CELL = 50
"""
Estimated bytes per table cell for response size calculation.

This rough estimate is used to prevent memory issues when retrieving large tables.
The estimation assumes:
- Average string length: ~20 characters (20 bytes)
- Numeric values: ~8 bytes (int64/double)
- Null values and metadata: ~5 bytes overhead
- JSON formatting overhead: ~15-20 bytes per cell
- Safety margin: 50 bytes total per cell

This conservative estimate helps catch potentially problematic responses before
expensive formatting operations. Can be tuned based on actual data patterns.
"""

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
      - Logs completion of server shutdown.

    Args:
        server (FastMCP): The FastMCP server instance (required by the FastMCP lifespan API).

    Yields:
        dict[str, object]: A context dictionary with the following keys for dependency injection into MCP tool requests:
            - 'config_manager' (ConfigManager): Instance for accessing session configuration.
            - 'session_registry' (CombinedSessionRegistry): Instance for managing all session types.
            - 'refresh_lock' (asyncio.Lock): Lock for atomic refresh operations across tools.
    """
    _LOGGER.info(
        "[mcp_systems_server:app_lifespan] Starting MCP server '%s'", server.name
    )
    session_registry = None

    try:
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
        }
    finally:
        _LOGGER.info(
            "[mcp_systems_server:app_lifespan] Shutting down MCP server '%s'",
            server.name,
        )
        if session_registry is not None:
            await session_registry.close()
        _LOGGER.info(
            "[mcp_systems_server:app_lifespan] MCP server '%s' shut down.", server.name
        )


mcp_server = FastMCP("deephaven-mcp-systems", lifespan=app_lifespan)
"""
FastMCP Server Instance for Deephaven MCP Systems Tools

This object is the singleton FastMCP server for the Deephaven MCP systems toolset. It is responsible for registering and exposing all MCP tool functions defined in this module (such as refresh, enterprise_systems_status, list_sessions, get_session_details, table_schemas, run_script, and pip_packages) to the MCP runtime environment.

Key Details:
    - The server is instantiated with the name 'deephaven-mcp-systems', which uniquely identifies this toolset in the MCP ecosystem.
    - All functions decorated with @mcp_server.tool() are automatically registered as MCP tools and made available for remote invocation.
    - The server manages protocol compliance, tool metadata, and integration with the broader MCP infrastructure.
    - This object should not be instantiated more than once per process/module.

Usage:
    - Do not call methods on mcp_server directly; instead, use the @mcp_server.tool() decorator to register new tools.
    - The MCP runtime will discover and invoke registered tools as needed.

See the module-level docstring for an overview of the available tools and error handling conventions.
"""


# TODO: remove refresh?
@mcp_server.tool()
async def refresh(context: Context) -> dict:
    """
    MCP Tool: Reload configuration and clear all active sessions.

    Reloads the Deephaven session configuration from disk and clears all active session objects.
    Configuration changes (adding, removing, or updating systems) are applied immediately.
    All sessions will be reopened with the new configuration on next access.

    AI Agent Usage:
    - Use this tool after making configuration file changes
    - Check 'success' field to verify reload completed
    - Sessions will be automatically recreated with new configuration on next use
    - Operation is atomic and thread-safe
    - WARNING: All active sessions will be cleared, including those created with create_enterprise_session
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
        "[mcp_systems_server:refresh] Invoked: refreshing session configuration and session cache."
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
            "[mcp_systems_server:refresh] Success: Session configuration and session cache have been reloaded."
        )
        return {"success": True}
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:refresh] Failed to refresh session configuration/session cache: {e!r}",
            exc_info=True,
        )
        return {"success": False, "error": str(e), "isError": True}


@mcp_server.tool()
async def enterprise_systems_status(
    context: Context, attempt_to_connect: bool = False
) -> dict:
    """
    MCP Tool: List all enterprise systems with their status and configuration details.

    This tool provides comprehensive status information about all configured enterprise systems in the MCP
    environment. It returns detailed health status using the ResourceLivenessStatus classification system,
    along with explanatory details and configuration information (with sensitive fields redacted for security).

    The tool supports two operational modes:
    1. Default mode (attempt_to_connect=False): Quick status check of existing connections
       - Fast response time, minimal resource usage
       - Suitable for dashboards, monitoring, and non-critical status checks
       - Will report systems as OFFLINE if no connection exists

    2. Connection verification mode (attempt_to_connect=True): Active connection attempt
       - Attempts to establish connections to verify actual availability
       - Higher latency but more accurate status reporting
       - Suitable for troubleshooting and pre-flight checks before critical operations
       - May create new connections if none exist

    Status Classification:
      - "ONLINE": System is healthy and ready for operational use
      - "OFFLINE": System is unresponsive, failed health checks, or not connected
      - "UNAUTHORIZED": Authentication or authorization failures prevent access
      - "MISCONFIGURED": Configuration errors prevent proper system operation
      - "UNKNOWN": Unexpected errors occurred during status determination

    AI Agent Usage:
    - Use attempt_to_connect=False (default) for quick status checks
    - Use attempt_to_connect=True to actively verify system connectivity
    - Check 'systems' array in response for individual system status
    - Use 'detail' field for troubleshooting connection issues
    - Configuration details are included but sensitive fields are redacted

    Args:
        context (Context): The MCP context object.
        attempt_to_connect (bool, optional): If True, actively attempts to connect to each system
            to verify its status. This provides more accurate results but increases latency.
            Default is False (only checks existing connections for faster response).

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if retrieval succeeded, False otherwise.
            - 'systems' (list[dict]): List of system info dicts. Each contains:
                - 'name' (str): System name identifier
                - 'liveness_status' (str): ResourceLivenessStatus ("ONLINE", "OFFLINE", "UNAUTHORIZED", "MISCONFIGURED", "UNKNOWN")
                - 'liveness_detail' (str, optional): Explanation message for the status, useful for troubleshooting
                - 'is_alive' (bool): Simple boolean indicating if the system is responsive
                - 'config' (dict): System configuration with sensitive fields redacted
            - 'error' (str, optional): Error message if retrieval failed.
            - 'isError' (bool, optional): Present and True if this is an error response.

    Example Successful Response:
        {
            'success': True,
            'systems': [
                {
                    'name': 'prod-system',
                    'liveness_status': 'ONLINE',
                    'liveness_detail': 'Connection established successfully',
                    'is_alive': True,
                    'config': {'host': 'prod.example.com', 'port': 10000, 'auth_type': 'anonymous'}
                }
            ]
        }

    Example Error Response:
        {'success': False, 'error': 'Failed to retrieve systems status', 'isError': True}

    Performance Considerations:
        - With attempt_to_connect=False: Typically completes in milliseconds
        - With attempt_to_connect=True: May take seconds due to connection operations
    """
    _LOGGER.info("[mcp_systems_server:enterprise_systems_status] Invoked.")
    try:
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )
        config_manager: ConfigManager = context.request_context.lifespan_context[
            "config_manager"
        ]
        # Get all factories (enterprise systems)
        enterprise_registry = await session_registry.enterprise_registry()
        factories = await enterprise_registry.get_all()
        config = await config_manager.get_config()

        try:
            systems_config = get_config_section(config, ["enterprise", "systems"])
        except KeyError:
            systems_config = {}

        systems = []
        for name, factory in factories.items():
            # Use liveness_status() for detailed health information
            status_enum, liveness_detail = await factory.liveness_status(
                ensure_item=attempt_to_connect
            )
            liveness_status = status_enum.name

            # Also get simple is_alive boolean
            is_alive = await factory.is_alive()

            # Redact config for output
            raw_config = systems_config.get(name, {})
            redacted_config = redact_enterprise_system_config(raw_config)

            system_info = {
                "name": name,
                "liveness_status": liveness_status,
                "is_alive": is_alive,
                "config": redacted_config,
            }

            # Include detail if available
            if liveness_detail is not None:
                system_info["liveness_detail"] = liveness_detail

            systems.append(system_info)
        return {"success": True, "systems": systems}
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:enterprise_systems_status] Failed: {e!r}",
            exc_info=True,
        )
        return {"success": False, "error": str(e), "isError": True}


@mcp_server.tool()
async def list_sessions(context: Context) -> dict:
    """
    MCP Tool: List all sessions with basic metadata.

    Returns basic information about all available sessions (community and enterprise).
    This is a lightweight operation that doesn't connect to sessions or check their status.

    AI Agent Usage:
    - Use this to discover available sessions before calling other session-based tools
    - Use returned 'session_id' values with other tools like run_script, get_table_data
    - Check 'type' field to understand session capabilities (community vs enterprise)
    - For detailed session information, use get_session_details with a specific session_id

    Args:
        context (Context): The MCP context object.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if retrieval succeeded, False otherwise.
            - 'sessions' (list[dict]): List of session info dicts. Each contains:
                - 'session_id' (str): Fully qualified session identifier for use with other tools
                - 'type' (str): Session type ("COMMUNITY" or "ENTERPRISE")
                - 'source' (str): Source system name
                - 'session_name' (str): Session name within the source
            - 'error' (str, optional): Error message if retrieval failed.
            - 'isError' (bool, optional): Present and True if this is an error response.

    Example Successful Response:
        {
            'success': True,
            'sessions': [
                {
                    'session_id': 'enterprise:prod-system:my-session',
                    'type': 'ENTERPRISE',
                    'source': 'prod-system',
                    'session_name': 'my-session'
                },
                {
                    'session_id': 'community:local-community:default',
                    'type': 'COMMUNITY',
                    'source': 'local-community',
                    'session_name': 'default'
                }
            ]
        }

    Example Error Response:
        {'success': False, 'error': 'Failed to retrieve sessions', 'isError': True}

    Error Scenarios:
        - Context access errors: Returns error if session_registry cannot be accessed from context
        - Registry operation errors: Returns error if session_registry.get_all() fails
        - Session processing errors: Returns error if individual session metadata cannot be extracted
    """
    _LOGGER.info("[mcp_systems_server:list_sessions] Invoked.")
    try:
        _LOGGER.debug(
            "[mcp_systems_server:list_sessions] Accessing session registry from context"
        )
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )
        _LOGGER.debug(
            "[mcp_systems_server:list_sessions] Retrieving all sessions from registry"
        )
        sessions = await session_registry.get_all()

        _LOGGER.info(
            "[mcp_systems_server:list_sessions] Found %d sessions.", len(sessions)
        )

        results = []
        for fq_name, mgr in sessions.items():
            _LOGGER.debug(
                "[mcp_systems_server:list_sessions] Processing session '%s'", fq_name
            )

            try:
                system_type = mgr.system_type
                system_type_str = system_type.name
                source = mgr.source
                session_name = mgr.name

                results.append(
                    {
                        "session_id": fq_name,
                        "type": system_type_str,
                        "source": source,
                        "session_name": session_name,
                    }
                )
            except Exception as e:
                _LOGGER.warning(
                    f"[mcp_systems_server:list_sessions] Could not process session '{fq_name}': {e!r}"
                )
                results.append({"session_id": fq_name, "error": str(e)})
        return {"success": True, "sessions": results}
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:list_sessions] Failed: {e!r}", exc_info=True
        )
        return {"success": False, "error": str(e), "isError": True}


async def _get_session_liveness_info(
    mgr: BaseItemManager, session_id: str, attempt_to_connect: bool
) -> tuple[bool, str, str | None]:
    """
    Get session liveness status and availability.

    This function checks the liveness status of a session using the provided manager.
    It can optionally attempt to connect to the session to verify its actual status.

    Args:
        mgr: Session manager for the target session
        session_id: Session identifier for logging purposes
        attempt_to_connect: Whether to attempt connecting to verify status

    Returns:
        tuple: A 3-tuple containing:
            - available (bool): Whether the session is available and responsive
            - liveness_status (str): Status classification ("ONLINE", "OFFLINE", etc.)
            - liveness_detail (str): Detailed explanation of the status
    """
    try:
        status, detail = await mgr.liveness_status(ensure_item=attempt_to_connect)
        liveness_status = status.name
        liveness_detail = detail
        available = await mgr.is_alive()
        _LOGGER.debug(
            f"[mcp_systems_server:get_session_details] Session '{session_id}' liveness: {liveness_status}, detail: {liveness_detail}"
        )
        return available, liveness_status, liveness_detail
    except Exception as e:
        _LOGGER.warning(
            f"[mcp_systems_server:get_session_details] Could not check liveness for '{session_id}': {e!r}"
        )
        return False, "OFFLINE", str(e)


async def _get_session_property(
    mgr: BaseItemManager,
    session_id: str,
    available: bool,
    property_name: str,
    getter_func: Callable[[BaseSession], Awaitable[T]],
) -> T | None:
    """
    Safely get a session property.

    Args:
        mgr (BaseItemManager): Session manager
        session_id (str): Session identifier
        available (bool): Whether the session is available
        property_name (str): Name of the property for logging
        getter_func (Callable[[BaseSession], Awaitable[T]]): Async function to get the property from the session

    Returns:
        T | None: The property value or None if unavailable/failed
    """
    if not available:
        return None

    try:
        session = await mgr.get()
        result = await getter_func(session)
        _LOGGER.debug(
            f"[mcp_systems_server:get_session_details] Session '{session_id}' {property_name}: {result}"
        )
        return result
    except Exception as e:
        _LOGGER.warning(
            f"[mcp_systems_server:get_session_details] Could not get {property_name} for '{session_id}': {e!r}"
        )
        return None


async def _get_session_programming_language(
    mgr: BaseItemManager, session_id: str, available: bool
) -> str | None:
    """
    Get the programming language of a session.

    This function retrieves the programming language (e.g., "python", "groovy")
    associated with the session. If the session is not available, it returns None
    immediately without attempting to connect.

    Args:
        mgr: Session manager for the target session
        session_id: Session identifier for logging purposes
        available: Whether the session is available (pre-checked)

    Returns:
        str | None: The programming language name (e.g., "python") or None if
                   unavailable/failed to retrieve
    """
    if not available:
        return None

    try:
        session: BaseSession = await mgr.get()
        programming_language = str(session.programming_language)
        _LOGGER.debug(
            f"[mcp_systems_server:get_session_details] Session '{session_id}' programming_language: {programming_language}"
        )
        return programming_language
    except Exception as e:
        _LOGGER.warning(
            f"[mcp_systems_server:get_session_details] Could not get programming_language for '{session_id}': {e!r}"
        )
        return None


async def _get_session_versions(
    mgr: BaseItemManager, session_id: str, available: bool
) -> tuple[str | None, str | None]:
    """
    Get Deephaven version information.

    This function retrieves both community (Core) and enterprise (Core+/CorePlus)
    version information from the session. If the session is not available, it returns
    (None, None) immediately without attempting to connect.

    Args:
        mgr: Session manager for the target session
        session_id: Session identifier for logging purposes
        available: Whether the session is available (pre-checked)

    Returns:
        tuple: A 2-tuple containing:
            - community_version (str | None): Deephaven Community/Core version (e.g., "0.24.0")
            - enterprise_version (str | None): Deephaven Enterprise/Core+/CorePlus version
                                              (e.g., "0.24.0") or None if not enterprise
    """
    if not available:
        return None, None

    try:
        session = await mgr.get()
        community_version, enterprise_version = await queries.get_dh_versions(session)
        _LOGGER.debug(
            f"[mcp_systems_server:get_session_details] Session '{session_id}' versions: community={community_version}, enterprise={enterprise_version}"
        )
        return community_version, enterprise_version
    except Exception as e:
        _LOGGER.warning(
            f"[mcp_systems_server:get_session_details] Could not get Deephaven versions for '{session_id}': {e!r}"
        )
        return None, None


@mcp_server.tool()
async def get_session_details(
    context: Context, session_id: str, attempt_to_connect: bool = False
) -> dict:
    """
    MCP Tool: Get detailed information about a specific session.

    Returns comprehensive status and configuration information for a specific session,
    including availability status, programming language, and version information.

    AI Agent Usage:
    - Use attempt_to_connect=False (default) for quick status checks
    - Use attempt_to_connect=True to actively verify session connectivity
    - Check 'available' field to determine if session can be used
    - Use 'liveness_status' for detailed status classification
    - Use list_sessions first to discover available session_id values
    - IMPORTANT: attempt_to_connect=True creates resource overhead (open sessions consume MCP server resources and each session maintains connections)
    - Only use attempt_to_connect=True for sessions you actually intend to use, not for general discovery or monitoring

    Args:
        context (Context): The MCP context object.
        session_id (str): The session identifier (fully qualified name) to get details for.
        attempt_to_connect (bool, optional): Whether to attempt connecting to the session
            to verify its status. Defaults to False for faster response.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if retrieval succeeded, False otherwise.
            - 'session' (dict): Session details including:
                - session_id (fully qualified session name)
                - type ("community" or "enterprise")
                - source (community source or enterprise factory)
                - session_name (session name)
                - available (bool): Whether the session is available
                - liveness_status (str): Status classification ("ONLINE", "OFFLINE", etc.)
                - liveness_detail (str): Detailed explanation of the status
                - programming_language (str, optional): The programming language of the session (e.g., "python", "groovy")
                - programming_language_version (str, optional): Version of the programming language (e.g., "3.9.7")
                - deephaven_community_version (str, optional): Version of Deephaven Community/Core (e.g., "0.24.0")
                - deephaven_enterprise_version (str, optional): Version of Deephaven Enterprise/Core+/CorePlus (e.g., "0.24.0")
                  if the session is an enterprise installation
            - 'error' (str, optional): Error message if retrieval failed.
            - 'isError' (bool, optional): Present and True if this is an error response.

        Note: The version fields (programming_language_version, deephaven_community_version,
        deephaven_enterprise_version) will only be present if the session is available and
        the information could be retrieved successfully. Fields with null values are excluded
        from the response.
    """
    _LOGGER.info(
        f"[mcp_systems_server:get_session_details] Invoked for session_id: {session_id}"
    )
    try:
        _LOGGER.debug(
            "[mcp_systems_server:get_session_details] Accessing session registry from context"
        )
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )

        # Get the specific session manager directly
        _LOGGER.debug(
            f"[mcp_systems_server:get_session_details] Retrieving session manager for '{session_id}'"
        )
        try:
            mgr = await session_registry.get(session_id)
            _LOGGER.debug(
                f"[mcp_systems_server:get_session_details] Successfully retrieved session manager for '{session_id}'"
            )
        except Exception as e:
            return {
                "success": False,
                "error": f"Session with ID '{session_id}' not found: {str(e)}",
                "isError": True,
            }

        try:
            # Get basic metadata
            _LOGGER.debug(
                f"[mcp_systems_server:get_session_details] Extracting metadata for session '{session_id}'"
            )
            system_type_str = mgr.system_type.name
            source = mgr.source
            session_name = mgr.name
            _LOGGER.debug(
                f"[mcp_systems_server:get_session_details] Session '{session_id}' metadata: type={system_type_str}, source={source}, name={session_name}"
            )

            # Get liveness status and availability
            _LOGGER.debug(
                f"[mcp_systems_server:get_session_details] Checking liveness for session '{session_id}' (attempt_to_connect={attempt_to_connect})"
            )
            available, liveness_status, liveness_detail = (
                await _get_session_liveness_info(mgr, session_id, attempt_to_connect)
            )

            # Get session properties using helper functions
            _LOGGER.debug(
                f"[mcp_systems_server:get_session_details] Retrieving session properties for '{session_id}' (available={available})"
            )
            programming_language = await _get_session_programming_language(
                mgr, session_id, available
            )

            # TODO: should the versions be cached?
            programming_language_version = await _get_session_property(
                mgr,
                session_id,
                available,
                "programming_language_version",
                queries.get_programming_language_version,
            )

            community_version, enterprise_version = await _get_session_versions(
                mgr, session_id, available
            )
            _LOGGER.debug(
                f"[mcp_systems_server:get_session_details] Completed property retrieval for session '{session_id}'"
            )

            # Build session info dictionary with all potential fields
            session_info_with_nones = {
                "session_id": session_id,
                "type": system_type_str,
                "source": source,
                "session_name": session_name,
                "available": available,
                "liveness_status": liveness_status,
                "liveness_detail": liveness_detail,
                "programming_language": programming_language,
                "programming_language_version": programming_language_version,
                "deephaven_community_version": community_version,
                "deephaven_enterprise_version": enterprise_version,
            }

            # Filter out None values
            session_info = {
                k: v for k, v in session_info_with_nones.items() if v is not None
            }
            _LOGGER.debug(
                f"[mcp_systems_server:get_session_details] Built session info for '{session_id}' with {len(session_info)} fields"
            )

            return {"success": True, "session": session_info}

        except Exception as e:
            _LOGGER.warning(
                f"[mcp_systems_server:get_session_details] Could not process session '{session_id}': {e!r}"
            )
            return {
                "success": False,
                "error": f"Error processing session '{session_id}': {str(e)}",
                "isError": True,
            }

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:get_session_details] Failed: {e!r}", exc_info=True
        )
        return {"success": False, "error": str(e), "isError": True}


@mcp_server.tool()
async def table_schemas(
    context: Context, session_id: str, table_names: list[str] | None = None
) -> dict:
    """
    MCP Tool: Retrieve column schemas for one or more tables from a Deephaven session.

    Returns column names and data types for the specified tables. If no table_names are provided,
    returns schemas for all available tables in the session. Each table's schema is returned as
    a list of dictionaries with 'name' and 'type' fields.

    AI Agent Usage:
    - Call with no table_names to discover all available tables and their schemas
    - Call with specific table_names list when you know which tables you need
    - Always check the 'success' field in each schema result before using the schema data
    - Use the returned column names and types to construct valid queries and operations
    - The 'type' field contains Deephaven data type strings (e.g., 'int', 'double', 'java.lang.String')
    - Essential before calling get_table_data or run_script to understand table structure
    - Individual table failures don't stop processing of other tables

    Args:
        context (Context): The MCP context object.
        session_id (str): ID of the Deephaven session to query. This argument is required.
        table_names (list[str], optional): List of table names to retrieve schemas for.
            If None, all available tables will be queried. Defaults to None.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if the operation completed, False if it failed entirely.
            - 'schemas' (list[dict], optional): List of per-table results if operation completed. Each contains:
                - 'success' (bool): True if this table's schema was retrieved successfully
                - 'table' (str): Table name
                - 'schema' (list[dict], optional): List of column definitions (name/type pairs) if successful
                - 'error' (str, optional): Error message if this table's schema retrieval failed
                - 'isError' (bool, optional): Present and True if this table had an error
            - 'error' (str, optional): Error message if the entire operation failed.
            - 'isError' (bool, optional): Present and True if this is an error response.

    Example Successful Response (mixed results):
        {
            'success': True,
            'schemas': [
                {'success': True, 'table': 'MyTable', 'schema': [{'name': 'Col1', 'type': 'int'}, ...]},
                {'success': False, 'table': 'MissingTable', 'error': 'Table not found', 'isError': True}
            ]
        }

    Example Error Response (total failure):
        {'success': False, 'error': 'Failed to connect to session: ...', 'isError': True}

    Logging:
        - Logs tool invocation, per-table results, and error details at INFO/ERROR levels.
    """
    _LOGGER.info(
        f"[mcp_systems_server:table_schemas] Invoked: session_id={session_id!r}, table_names={table_names!r}"
    )
    schemas = []
    try:
        _LOGGER.debug(
            "[mcp_systems_server:table_schemas] Accessing session registry from context"
        )
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )
        _LOGGER.debug(
            f"[mcp_systems_server:table_schemas] Retrieving session manager for '{session_id}'"
        )
        session_manager = await session_registry.get(session_id)
        _LOGGER.debug(
            f"[mcp_systems_server:table_schemas] Establishing session connection for '{session_id}'"
        )
        session = await session_manager.get()
        _LOGGER.info(
            f"[mcp_systems_server:table_schemas] Session established for session: '{session_id}'"
        )

        if table_names is not None:
            selected_table_names = table_names
            _LOGGER.info(
                f"[mcp_systems_server:table_schemas] Fetching schemas for specified tables: {selected_table_names!r}"
            )
        else:
            _LOGGER.debug(
                f"[mcp_systems_server:table_schemas] Discovering available tables in session '{session_id}'"
            )
            selected_table_names = await session.tables()
            _LOGGER.info(
                f"[mcp_systems_server:table_schemas] Fetching schemas for all tables in session: {selected_table_names!r}"
            )

        for table_name in selected_table_names:
            _LOGGER.debug(
                f"[mcp_systems_server:table_schemas] Processing table '{table_name}' in session '{session_id}'"
            )
            try:
                meta_table = await queries.get_meta_table(session, table_name)
                # meta_table is a pyarrow.Table with columns: 'Name', 'DataType', etc.
                schema = [
                    {"name": row["Name"], "type": row["DataType"]}
                    for row in meta_table.to_pylist()
                ]
                schemas.append({"success": True, "table": table_name, "schema": schema})
                _LOGGER.info(
                    f"[mcp_systems_server:table_schemas] Success: Retrieved schema for table '{table_name}'"
                )
            except Exception as table_exc:
                _LOGGER.error(
                    f"[mcp_systems_server:table_schemas] Failed to get schema for table '{table_name}': {table_exc!r}",
                    exc_info=True,
                )
                schemas.append(
                    {
                        "success": False,
                        "table": table_name,
                        "error": str(table_exc),
                        "isError": True,
                    }
                )

        _LOGGER.info(
            f"[mcp_systems_server:table_schemas] Returning {len(schemas)} table results"
        )
        return {"success": True, "schemas": schemas}
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:table_schemas] Failed for session: '{session_id}', error: {e!r}",
            exc_info=True,
        )
        return {"success": False, "error": str(e), "isError": True}


@mcp_server.tool()
async def run_script(
    context: Context,
    session_id: str,
    script: str | None = None,
    script_path: str | None = None,
) -> dict:
    """
    MCP Tool: Execute a script on a specified Deephaven session.

    Executes a script on the specified Deephaven session and returns execution status. The script
    can be provided either as a string in the 'script' parameter or as a file path in the 'script_path'
    parameter. Exactly one of these parameters must be provided.

    AI Agent Usage:
    - Use 'script' parameter for inline script execution
    - Use 'script_path' parameter to execute scripts from files
    - Check 'success' field in response to verify execution completed without errors
    - Script executes in the session's environment with access to session state
    - Any tables or variables created will persist in the session for future use
    - Script language depends on the session's configured programming language

    Args:
        context (Context): The MCP context object.
        session_id (str): ID of the Deephaven session on which to execute the script. This argument is required.
        script (str, optional): The script to execute. Defaults to None.
        script_path (str, optional): Path to a script file to execute. Defaults to None.

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): True if the script executed successfully, False otherwise.
            - 'error' (str, optional): Error message if execution failed. Omitted on success.
            - 'isError' (bool, optional): Present and True if this is an error response (i.e., success is False).

    Example Successful Response:
        {'success': True}

    Example Error Responses:
        {'success': False, 'error': 'Must provide either script or script_path.', 'isError': True}
        {'success': False, 'error': 'Script execution failed: ...', 'isError': True}

    Logging:
        - Logs tool invocation, script source/path, execution status, and error details at INFO/WARNING/ERROR levels.
    """
    _LOGGER.info(
        f"[mcp_systems_server:run_script] Invoked: session_id={session_id!r}, script={'<provided>' if script else None}, script_path={script_path!r}"
    )
    result: dict[str, object] = {"success": False}
    try:
        _LOGGER.debug(
            f"[mcp_systems_server:run_script] Validating script parameters for session '{session_id}'"
        )
        if script is None and script_path is None:
            _LOGGER.warning(
                "[mcp_systems_server:run_script] No script or script_path provided. Returning error."
            )
            result["error"] = "Must provide either script or script_path."
            result["isError"] = True
            return result

        if script is None:
            _LOGGER.info(
                f"[mcp_systems_server:run_script] Reading script from file: {script_path!r}"
            )
            if script_path is None:
                raise RuntimeError(
                    "Internal error: script_path is None after prior guard"
                )  # pragma: no cover
            _LOGGER.debug(
                f"[mcp_systems_server:run_script] Opening script file '{script_path}' for reading"
            )
            async with aiofiles.open(script_path) as f:
                script = await f.read()
            _LOGGER.debug(
                f"[mcp_systems_server:run_script] Successfully read {len(script)} characters from script file"
            )

        _LOGGER.debug(
            "[mcp_systems_server:run_script] Accessing session registry from context"
        )
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )
        _LOGGER.debug(
            f"[mcp_systems_server:run_script] Retrieving session manager for '{session_id}'"
        )
        session_manager = await session_registry.get(session_id)
        _LOGGER.debug(
            f"[mcp_systems_server:run_script] Establishing session connection for '{session_id}'"
        )
        session = await session_manager.get()
        _LOGGER.info(
            f"[mcp_systems_server:run_script] Session established for session: '{session_id}'"
        )

        _LOGGER.info(
            f"[mcp_systems_server:run_script] Executing script on session: '{session_id}'"
        )
        _LOGGER.debug(
            f"[mcp_systems_server:run_script] Script length: {len(script)} characters"
        )

        await session.run_script(script)

        _LOGGER.info(
            f"[mcp_systems_server:run_script] Script executed successfully on session: '{session_id}'"
        )
        result["success"] = True
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:run_script] Failed for session: '{session_id}', error: {e!r}",
            exc_info=True,
        )
        result["error"] = str(e)
        result["isError"] = True
    return result


@mcp_server.tool()
async def pip_packages(context: Context, session_id: str) -> dict:
    """
    MCP Tool: Retrieve installed pip packages from a specified Deephaven session.

    Queries the specified Deephaven session for installed pip packages using importlib.metadata.
    Returns package names and versions for all Python packages available in the session's environment.

    AI Agent Usage:
    - Use this to understand what libraries are available in a session before running scripts
    - Check 'result' array for list of installed packages with names and versions
    - Useful for determining if specific libraries need to be installed before script execution
    - Essential for generating code that uses available libraries and avoiding import errors
    - Helps inform decisions about which libraries to use when multiple options are available

    Args:
        context (Context): The MCP context object.
        session_id (str): ID of the Deephaven session to query.

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): True if the packages were retrieved successfully, False otherwise.
            - 'result' (list[dict], optional): List of pip package dicts if successful. Each contains:
                - 'package' (str): Package name
                - 'version' (str): Package version
            - 'error' (str, optional): Error message if retrieval failed.
            - 'isError' (bool, optional): Present and True if this is an error response (i.e., success is False).

    Example Successful Response:
        {'success': True, 'result': [{"package": "numpy", "version": "1.25.0"}, ...]}

    Example Error Response:
        {'success': False, 'error': 'Failed to get pip packages: ...', 'isError': True}
    """
    _LOGGER.info(
        f"[mcp_systems_server:pip_packages] Invoked for session_id: {session_id!r}"
    )
    result: dict = {"success": False}
    try:
        _LOGGER.debug(
            "[mcp_systems_server:pip_packages] Accessing session registry from context"
        )
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )
        _LOGGER.debug(
            f"[mcp_systems_server:pip_packages] Retrieving session manager for '{session_id}'"
        )
        session_manager = await session_registry.get(session_id)
        _LOGGER.debug(
            f"[mcp_systems_server:pip_packages] Establishing session connection for '{session_id}'"
        )
        session = await session_manager.get()
        _LOGGER.info(
            f"[mcp_systems_server:pip_packages] Session established for session: '{session_id}'"
        )

        _LOGGER.debug(
            f"[mcp_systems_server:pip_packages] Querying pip packages for session '{session_id}'"
        )
        arrow_table = await queries.get_pip_packages_table(session)
        _LOGGER.debug(
            f"[mcp_systems_server:pip_packages] Retrieved pip packages table for session '{session_id}'"
        )
        _LOGGER.info(
            f"[mcp_systems_server:pip_packages] Pip packages table retrieved successfully for session: '{session_id}'"
        )

        # Convert the Arrow table to a list of dicts
        packages: list[dict[str, str]] = []
        if arrow_table is not None:
            # Convert to pandas DataFrame for easy dict conversion
            df = arrow_table.to_pandas()
            raw_packages = df.to_dict(orient="records")
            # Validate and convert keys to lowercase
            packages = []
            for pkg in raw_packages:
                if (
                    not isinstance(pkg, dict)
                    or "Package" not in pkg
                    or "Version" not in pkg
                ):
                    raise ValueError(
                        "Malformed package data: missing 'Package' or 'Version' key"
                    )
                # Results should have lower case names.  The query had to use Upper case names to avoid invalid column names
                packages.append({"package": pkg["Package"], "version": pkg["Version"]})

        result["success"] = True
        result["result"] = packages
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:pip_packages] Failed for session: '{session_id}', error: {e!r}",
            exc_info=True,
        )
        result["error"] = str(e)
        result["isError"] = True
    return result


# Size limits for table data responses
MAX_RESPONSE_SIZE = 50_000_000  # 50MB hard limit
WARNING_SIZE = 5_000_000  # 5MB warning threshold


def _check_response_size(table_name: str, estimated_size: int) -> dict | None:
    """
    Check if estimated response size is within acceptable limits.

    Evaluates the estimated response size against predefined limits to prevent memory
    issues and excessive network traffic. Logs warnings for large responses and
    returns structured error responses for oversized requests.

    Args:
        table_name (str): Name of the table being processed, used for logging context.
        estimated_size (int): Estimated response size in bytes.

    Returns:
        dict | None: Returns None if size is acceptable, or a structured error dict
                     with 'success': False, 'error': str, 'isError': True if the
                     response would exceed MAX_RESPONSE_SIZE (50MB).

    Side Effects:
        - Logs warning message if size exceeds WARNING_SIZE (5MB).
        - No side effects if size is within acceptable limits.
    """
    if estimated_size > WARNING_SIZE:
        _LOGGER.warning(
            f"Large response (~{estimated_size/1_000_000:.1f}MB) for table '{table_name}'. "
            f"Consider reducing max_rows for better performance."
        )

    if estimated_size > MAX_RESPONSE_SIZE:
        return {
            "success": False,
            "error": f"Response would be ~{estimated_size/1_000_000:.1f}MB (max 50MB). Please reduce max_rows.",
            "isError": True,
        }

    return None  # Size is acceptable


def _format_table_data(
    arrow_table: pa.Table, format_type: str, row_count: int
) -> tuple[str, object]:
    """
    Convert Arrow table to specified output format with automatic format selection.

    Transforms PyArrow table data into one of several supported output formats. Supports
    automatic format selection based on row count to balance performance and usability.
    Handles memory-efficient CSV conversion and proper JSON serialization.

    Args:
        arrow_table: PyArrow Table object containing the source data.
        format_type (str): Desired output format. Must be one of:
                          - "auto": Automatically selects "json-column" for ≤100 rows, "csv" for >100 rows
                          - "json-row": Array of objects, each representing a row
                          - "json-column": Object with column names as keys, arrays as values
                          - "csv": Comma-separated values as a string
        row_count (int): Number of rows in the table, used for auto format selection.

    Returns:
        tuple[str, object]: A 2-tuple containing:
                           - str: The actual format used ("json-row", "json-column", or "csv")
                           - object: The formatted data (list, dict, or str depending on format)

    Raises:
        ValueError: If format_type is not one of the supported formats.

    Performance Notes:
        - CSV format uses direct Arrow→CSV conversion for memory efficiency
        - JSON formats create Python object copies, unavoidable for JSON serialization
        - Auto selection optimizes for small tables (JSON) vs large tables (CSV)
    """
    if format_type == "auto":
        actual_format = "json-column" if row_count <= 100 else "csv"
    else:
        actual_format = format_type

    if actual_format == "json-row":
        return actual_format, arrow_table.to_pylist()
    elif actual_format == "json-column":
        return actual_format, arrow_table.to_pydict()
    elif actual_format == "csv":
        # Direct Arrow → CSV conversion (most memory efficient)
        output = io.BytesIO()
        csv.write_csv(arrow_table, output)
        # Note: decode() creates a copy, but necessary for JSON serialization
        return actual_format, output.getvalue().decode("utf-8")
    else:
        raise ValueError(f"Unsupported format: {actual_format}")


@mcp_server.tool()
async def get_table_data(
    context: Context,
    session_id: str,
    table_name: str,
    max_rows: int | None = 1000,
    head: bool = True,
    format: str = "auto",
) -> dict:
    """
    MCP Tool: Retrieve table data from a specified Deephaven session with flexible formatting options.

    This tool queries the specified Deephaven session for table data and returns it in the requested format
    with optional row limiting. Supports multiple output formats and provides completion status to indicate
    if the entire table was retrieved. Includes safety limits (50MB max response size) to prevent memory issues.

    Args:
        context (Context): The MCP context object, required by MCP protocol but not actively used.
        session_id (str): ID of the Deephaven session to query. Must match an existing active session.
        table_name (str): Name of the table to retrieve data from. Must exist in the specified session.
        max_rows (int | None, optional): Maximum number of rows to retrieve. Defaults to 1000 for safety.
                                        Set to None to retrieve entire table (use with caution for large tables).
        head (bool, optional): Direction of row retrieval. If True (default), retrieve from beginning.
                              If False, retrieve from end (most recent rows for time-series data).
        format (str, optional): Output format selection. Defaults to "auto". Options:
                               - "auto": Selects json-column for ≤100 rows, csv for >100 rows (recommended)
                               - "json-row": Array of objects [{col1: val1, col2: val2}, ...] (best for iteration)
                               - "json-column": Object {col1: [val1, val2], col2: [val3, val4]} (best for analysis)
                               - "csv": Comma-separated string (most memory efficient for large datasets)

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): Always present. True if table data was retrieved successfully, False on any error.
            - 'table_name' (str, optional): Name of the retrieved table if successful. Echoes input for confirmation.
            - 'format' (str, optional): Actual format used for the data if successful. May differ from request when "auto".
            - 'schema' (list[dict], optional): Array of column definitions if successful. Each dict contains:
                                              {'name': str, 'type': str} describing column name and Arrow type.
            - 'row_count' (int, optional): Number of rows in the returned data if successful. May be less than max_rows.
            - 'is_complete' (bool, optional): True if entire table was retrieved if successful. False if truncated by max_rows.
            - 'data' (list | dict | str, optional): The actual table data if successful. Type depends on format:
                                                   list for json-row, dict for json-column, str for csv.
            - 'error' (str, optional): Human-readable error message if retrieval failed. Omitted on success.
            - 'isError' (bool, optional): Present and True only when success=False. Explicit error flag for frameworks.

    Error Scenarios:
        - Invalid session_id: Returns error if session doesn't exist or is not accessible
        - Invalid table_name: Returns error if table doesn't exist in the session
        - Invalid format: Returns error if format is not one of the supported options
        - Response too large: Returns error if estimated response would exceed 50MB limit
        - Session connection issues: Returns error if unable to communicate with Deephaven server
        - Query execution errors: Returns error if table query fails (permissions, syntax, etc.)

    Performance Considerations:
        - Large tables: Use csv format or limit max_rows to avoid memory issues
        - Column analysis: Use json-column format for efficient column-wise operations
        - Row processing: Use json-row format for record-by-record iteration
        - Auto format: Recommended for general use, optimizes based on data size
        - Response size limit: 50MB maximum to prevent memory issues

    AI Agent Usage:
        - Always check 'success' field before accessing data fields
        - Use 'is_complete' to determine if more data exists beyond max_rows limit
        - Parse 'schema' array to understand column types before processing 'data'
        - Handle variable data types when using auto format (list/dict/string)
        - Use head=True (default) to get rows from table start, head=False to get from table end
        - Start with small max_rows values for large tables to avoid memory issues
        - Use csv format for memory efficiency with large datasets
        - Check 'format' field in response to know actual format used (may differ from auto request)
    """
    _LOGGER.info(
        f"[mcp_systems_server:get_table_data] Invoked: session_id={session_id!r}, "
        f"table_name={table_name!r}, max_rows={max_rows}, head={head}, format={format!r}"
    )

    result: dict[str, object] = {"success": False}

    try:
        # Validate format parameter
        valid_formats = {"auto", "json-row", "json-column", "csv"}
        if format not in valid_formats:
            result["error"] = (
                f"Invalid format '{format}'. Valid options: {', '.join(valid_formats)}"
            )
            result["isError"] = True
            return result

        # Get session registry and session
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )

        _LOGGER.debug(
            f"[mcp_systems_server:get_table_data] Retrieving session '{session_id}'"
        )
        session_manager = await session_registry.get(session_id)
        session = await session_manager.get()

        # Get table data using queries module
        _LOGGER.debug(
            f"[mcp_systems_server:get_table_data] Retrieving table data for '{table_name}'"
        )
        arrow_table, is_complete = await queries.get_table(
            session, table_name, max_rows=max_rows, head=head
        )

        # Check response size before formatting (rough estimation to avoid memory overhead)
        row_count = len(arrow_table)
        col_count = len(arrow_table.schema)
        estimated_size = row_count * col_count * ESTIMATED_BYTES_PER_CELL
        size_error = _check_response_size(table_name, estimated_size)
        if size_error:
            return size_error

        # Format the data
        actual_format, formatted_data = _format_table_data(
            arrow_table, format, row_count
        )

        # Extract schema information
        schema = [
            {"name": field.name, "type": str(field.type)}
            for field in arrow_table.schema
        ]

        result.update(
            {
                "success": True,
                "table_name": table_name,
                "format": actual_format,
                "schema": schema,
                "row_count": len(arrow_table),
                "is_complete": is_complete,
                "data": formatted_data,
            }
        )

        _LOGGER.info(
            f"[mcp_systems_server:get_table_data] Success: Retrieved {len(arrow_table)} rows "
            f"from table '{table_name}' in format '{actual_format}' (complete: {is_complete})"
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:get_table_data] Failed for session '{session_id}', "
            f"table '{table_name}': {e!r}",
            exc_info=True,
        )
        result["error"] = str(e)
        result["isError"] = True

    return result


@mcp_server.tool()
async def get_table_meta(context: Context, session_id: str, table_name: str) -> dict:
    """
    MCP Tool: Retrieve metadata information for a specified table.

    Returns detailed schema information for a table including column names, data types, and properties.
    Focuses on table structure rather than actual data.

    AI Agent Usage:
    - Use this to understand table structure before data operations
    - Check 'data' array for column definitions with names and types
    - Always check 'success' field before accessing metadata fields
    - Similar to table_schemas but returns more detailed metadata properties
    - Essential for generating Deephaven scripts that reference specific columns and use appropriate data types
    - Helps ensure generated code uses correct column names and compatible operations

    Args:
        context (Context): The MCP context object.
        session_id (str): ID of the Deephaven session to query.
        table_name (str): Name of the table to retrieve metadata for.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if metadata was retrieved successfully, False on error.
            - 'table_name' (str, optional): Name of the table if successful.
            - 'format' (str, optional): Always "json-row" for meta tables if successful.
            - 'meta_columns' (list[dict], optional): Schema of the meta table itself if successful. Each dict contains:
                {'name': str, 'type': str} describing the metadata table structure.
            - 'row_count' (int, optional): Number of metadata rows (columns in original table) if successful.
            - 'is_complete' (bool, optional): Always True for meta tables if successful. Metadata is never truncated.
            - 'data' (list[dict], optional): Array of metadata objects if successful. Each dict describes one column with:
                - 'Name' (str): Column name in the original table
                - 'DataType' (str): Deephaven data type (e.g., 'int', 'double', 'java.lang.String')
                - 'IsPartitioning' (bool, optional): Whether this column is used for partitioning
                - 'ComponentType' (str, optional): Component type for array/vector columns
                - Additional metadata properties depending on column type and table configuration
            - 'error' (str, optional): Human-readable error message if metadata retrieval failed. Omitted on success.
            - 'isError' (bool, optional): Present and True only when success=False. Explicit error flag for frameworks.

    Error Scenarios:
        - Invalid session_id: Returns error if session doesn't exist or is not accessible
        - Invalid table_name: Returns error if table doesn't exist in the session
        - Session connection issues: Returns error if unable to communicate with Deephaven server
        - Permission errors: Returns error if session lacks permission to access table metadata
        - Server errors: Returns error if Deephaven server fails to generate metadata

    Performance Notes:
        - Metadata retrieval is typically fast regardless of table size
        - No size limits apply to metadata (unlike table data)
        - Safe to call repeatedly as metadata is cached by Deephaven
        - Minimal memory usage compared to actual data retrieval
    """
    _LOGGER.info(
        f"[mcp_systems_server:get_table_meta] Invoked: session_id={session_id!r}, table_name={table_name!r}"
    )

    result: dict[str, object] = {"success": False}

    try:
        # Get session registry and session
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )

        _LOGGER.debug(
            f"[mcp_systems_server:get_table_meta] Retrieving session '{session_id}'"
        )
        session_manager = await session_registry.get(session_id)
        session = await session_manager.get()

        # Get table metadata using queries module
        _LOGGER.debug(
            f"[mcp_systems_server:get_table_meta] Retrieving metadata for table '{table_name}'"
        )
        meta_arrow_table = await queries.get_meta_table(session, table_name)

        # Convert to row-oriented JSON (meta tables are small)
        meta_data = meta_arrow_table.to_pylist()

        # Extract schema of the meta table itself
        meta_schema = [
            {"name": field.name, "type": str(field.type)}
            for field in meta_arrow_table.schema
        ]

        result.update(
            {
                "success": True,
                "table_name": table_name,
                "format": "json-row",
                "meta_columns": meta_schema,
                "row_count": len(meta_arrow_table),
                "is_complete": True,  # Meta tables are always complete
                "data": meta_data,
            }
        )

        _LOGGER.info(
            f"[mcp_systems_server:get_table_meta] Success: Retrieved metadata for table '{table_name}' "
            f"({len(meta_arrow_table)} columns)"
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:get_table_meta] Failed for session '{session_id}', "
            f"table '{table_name}': {e!r}",
            exc_info=True,
        )
        result["error"] = str(e)
        result["isError"] = True

    return result


async def _check_session_limits(
    session_registry: CombinedSessionRegistry, system_name: str, max_sessions: int
) -> dict | None:
    """Check if session creation is allowed and within limits.

    Args:
        session_registry: The session registry
        system_name: Name of the enterprise system
        max_sessions: Maximum concurrent sessions allowed

    Returns:
        dict: Error response dict if not allowed, None if allowed
    """
    # Check if session creation is disabled
    if max_sessions == 0:
        error_msg = f"Session creation is disabled for system '{system_name}' (max_concurrent_sessions = 0)"
        _LOGGER.error(f"[mcp_systems_server:_check_session_limits] {error_msg}")
        return {"error": error_msg, "isError": True}

    # Check if current session count would exceed the limit
    current_session_count = await session_registry.count_added_sessions(
        SystemType.ENTERPRISE, system_name
    )
    if current_session_count >= max_sessions:
        error_msg = f"Max concurrent sessions ({max_sessions}) reached for system '{system_name}'"
        _LOGGER.error(f"[mcp_systems_server:_check_session_limits] {error_msg}")
        return {"error": error_msg, "isError": True}

    return None


def _generate_session_name_if_none(
    system_config: dict, session_name: str | None
) -> str:
    """Generate a session name if none provided.

    Args:
        system_config: Enterprise system configuration dict
        session_name: Provided session name or None

    Returns:
        str: Either the provided session_name or auto-generated name
    """
    if session_name is not None:
        return session_name

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    username = system_config.get("username")
    if username:
        generated = f"mcp-{username}-{timestamp}"
    else:
        generated = f"mcp-session-{timestamp}"

    _LOGGER.debug(
        f"[mcp_systems_server:_generate_session_name_if_none] Auto-generated session name: {generated}"
    )
    return generated


async def _check_session_id_available(
    session_registry: CombinedSessionRegistry, session_id: str
) -> dict | None:
    """Check if session ID is available (not already in use).

    Called during session creation to prevent duplicate session IDs.
    This ensures each session has a unique identifier in the registry.

    Args:
        session_registry: The session registry to check
        session_id: The session ID to check for availability

    Returns:
        dict | None: Error response dict if session exists, None if available
    """
    try:
        await session_registry.get(session_id)
        # If we got here, session already exists
        error_msg = f"Session '{session_id}' already exists"
        _LOGGER.error(f"[mcp_systems_server:_check_session_id_available] {error_msg}")
        return {"error": error_msg, "isError": True}
    except KeyError:
        return None  # Good - session doesn't exist yet


def _get_system_config(
    function_name: str, config_manager: ConfigManager, system_name: str
) -> tuple[dict, dict | None]:
    """Get system config from configuration and validate system exists.

    Retrieves the configuration for the specified enterprise system. Returns both
    the system configuration and any error that occurred during retrieval.

    Args:
        function_name: Name of the calling function for logging
        config_manager: ConfigManager instance
        system_name: Name of the enterprise system

    Returns:
        tuple[dict, dict | None]: (system_config, error_dict)
            - system_config: The enterprise system configuration dict
            - error_dict: Error response if system not found, None if successful
    """
    enterprise_systems_config = get_config_section(config_manager, "enterprise_sessions")  # type: ignore[arg-type]

    if not enterprise_systems_config or system_name not in enterprise_systems_config:
        error_msg = f"Enterprise system '{system_name}' not found in configuration"
        _LOGGER.error(f"[mcp_systems_server:{function_name}] {error_msg}")
        return {}, {"error": error_msg, "isError": True}

    return enterprise_systems_config[system_name], None


@mcp_server.tool()
async def create_enterprise_session(
    context: Context,
    system_name: str,
    session_name: str | None = None,
    heap_size_gb: float | None = None,
    programming_language: str | None = None,
    auto_delete_timeout: int | None = None,
    server: str | None = None,
    engine: str | None = None,
    extra_jvm_args: list[str] | None = None,
    extra_environment_vars: list[str] | None = None,
    admin_groups: list[str] | None = None,
    viewer_groups: list[str] | None = None,
    timeout_seconds: float | None = None,
    session_arguments: dict[str, Any] | None = None,
) -> dict:
    """
    MCP Tool: Create a new enterprise session with configurable parameters.

    Creates a new enterprise session on the specified enterprise system and registers it in the
    session registry for future use. The session is configured using provided parameters or defaults
    from the enterprise system configuration.

    Parameter Resolution Priority (highest to lowest):
    1. Tool parameters provided in this function call
    2. Enterprise system session_creation defaults from configuration
    3. Deephaven server built-in defaults

    AI Agent Usage:
    - Use this tool only when you need to create a new session
    - Check 'success' field and use returned 'session_id' for subsequent operations
    - Sessions have resource limits and may auto-delete after timeout periods
    - Use delete_enterprise_session tool to clean up when done

    Args:
        context (Context): The MCP context object.
        system_name (str): Name of the enterprise system to create the session on.
            Must match a configured enterprise system name.
        session_name (str | None): Name for the new session. If None, auto-generates
            a timestamp-based name like "mcp-{username}-20241126-1130".
        heap_size_gb (float | None): JVM heap size in gigabytes. If None, uses
            config default or Deephaven default.
        programming_language (str | None): Programming language for the session.
            Supported values: "Python" (default) or "Groovy". If None, uses config default or "Python".
        auto_delete_timeout (int | None): Seconds of inactivity before automatic session deletion.
            If None, uses config default or API default (600 seconds).
        server (str | None): Specific server to run session on.
            If None, uses config default or lets Deephaven auto-select.
        engine (str | None): Engine type for the session.
            If None, uses config default or "DeephavenCommunity".
        extra_jvm_args (list[str] | None): Additional JVM arguments for the session.
            If None, uses config default or standard JVM settings.
        extra_environment_vars (list[str] | None): Environment variables for the session in format
            ["NAME=value", ...]. If None, uses config default environment.
        admin_groups (list[str] | None): User groups with administrative permissions on the session.
            If None, uses config default or creator-only access.
        viewer_groups (list[str] | None): User groups with read-only access to session.
            If None, uses config default or creator-only access.
        timeout_seconds (float | None): Maximum time in seconds to wait for session startup.
            If None, uses config default or 60 seconds.
        session_arguments (dict[str, Any] | None): Additional arguments for pydeephaven.Session constructor.
            If None, uses config default or standard session settings.

    Returns:
        dict: Structured response with session creation details.

        Success response:
        {
            "success": True,
            "session_id": "enterprise:prod-system:analytics-session-001",
            "system_name": "prod-system",
            "session_name": "analytics-session-001",
            "configuration": {
                "heap_size_gb": 8.0,
                "auto_delete_timeout_minutes": 60,
                "server": "server-east-1",
                "engine": "DeephavenCommunity"
            }
        }

        Error response:
        {
            "success": False,
            "error": "Max concurrent sessions (5) reached for system 'prod-system'",
            "isError": True
        }

    Validation and Safety:
        - Verifies enterprise system exists and is accessible
        - Checks max_concurrent_sessions limit from configuration
        - Ensures no session ID conflicts in registry
        - Authenticates with enterprise system before creation
        - Provides detailed error messages for troubleshooting

    Common Error Scenarios:
        - System not found: "Enterprise system 'xyz' not found"
        - Session limit reached: "Max concurrent sessions (N) reached"
        - Name conflict: "Session 'enterprise:sys:name' already exists"
        - Authentication failure: "Failed to authenticate with enterprise system"
        - Resource exhaustion: "Insufficient resources to create session"
        - Network issues: "Failed to connect to enterprise system"

    Logging:
        - Info: Tool invocation, successful creation, parameter resolution
        - Debug: Configuration loading, session registry operations
        - Error: All failure scenarios with full context and stack traces
    """
    _LOGGER.info(
        f"[mcp_systems_server:create_enterprise_session] Invoked: "
        f"system_name={system_name!r}, session_name={session_name!r}, "
        f"heap_size_gb={heap_size_gb}, auto_delete_timeout={auto_delete_timeout}, "
        f"server={server!r}, engine={engine!r}, "
        f"extra_jvm_args={extra_jvm_args}, extra_environment_vars={extra_environment_vars}, "
        f"admin_groups={admin_groups}, viewer_groups={viewer_groups}, "
        f"timeout_seconds={timeout_seconds}, session_arguments={session_arguments}, "
        f"programming_language={programming_language}"
    )

    result: dict[str, object] = {"success": False}

    try:
        # Get config and session registry
        config_manager: ConfigManager = context.request_context.lifespan_context[
            "config_manager"
        ]
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )

        # Get enterprise system configuration
        system_config, error_response = _get_system_config(
            "create_enterprise_session", config_manager, system_name
        )
        if error_response:
            result.update(error_response)
            return result
        session_creation_config = system_config.get("session_creation", {})
        max_sessions = session_creation_config.get(
            "max_concurrent_sessions", DEFAULT_MAX_CONCURRENT_SESSIONS
        )

        # Check session limits (both enabled and count)
        error_response = await _check_session_limits(
            session_registry, system_name, max_sessions
        )
        if error_response:
            result.update(error_response)
            return result

        # Generate session name if not provided
        session_name = _generate_session_name_if_none(system_config, session_name)

        # Create session ID and check for conflicts
        session_id = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, system_name, session_name
        )
        error_response = await _check_session_id_available(session_registry, session_id)
        if error_response:
            result.update(error_response)
            return result

        # Resolve configuration parameters
        defaults = session_creation_config.get("defaults", {})
        resolved_config = _resolve_session_parameters(
            heap_size_gb,
            auto_delete_timeout,
            server,
            engine,
            extra_jvm_args,
            extra_environment_vars,
            admin_groups,
            viewer_groups,
            timeout_seconds,
            session_arguments,
            programming_language,
            defaults,
        )

        _LOGGER.debug(
            f"[mcp_systems_server:create_enterprise_session] Resolved configuration: {resolved_config}"
        )

        # Get enterprise factory and create session
        enterprise_registry = await session_registry.enterprise_registry()
        factory_manager = await enterprise_registry.get(system_name)
        factory = await factory_manager.get()

        # Create configuration transformer based on programming language
        configuration_transformer = None
        programming_lang = resolved_config["programming_language"]
        if programming_lang and programming_lang.lower() != "python":

            def language_transformer(config: Any) -> Any:
                config.scriptLanguage = programming_lang
                return config

            configuration_transformer = language_transformer

        _LOGGER.debug(
            f"[mcp_systems_server:create_enterprise_session] Creating session with parameters: "
            f"name={session_name}, heap_size_gb={resolved_config['heap_size_gb']}, "
            f"auto_delete_timeout={resolved_config['auto_delete_timeout']}, "
            f"server={resolved_config['server']}, engine={resolved_config['engine']}, "
            f"programming_language={programming_lang}"
        )

        # Create the session
        session = await factory.connect_to_new_worker(
            name=session_name,
            heap_size_gb=resolved_config["heap_size_gb"],
            auto_delete_timeout=resolved_config["auto_delete_timeout"],
            server=resolved_config["server"],
            engine=resolved_config["engine"],
            extra_jvm_args=resolved_config["extra_jvm_args"],
            extra_environment_vars=resolved_config["extra_environment_vars"],
            admin_groups=resolved_config["admin_groups"],
            viewer_groups=resolved_config["viewer_groups"],
            timeout_seconds=resolved_config["timeout_seconds"],
            configuration_transformer=configuration_transformer,
            session_arguments=resolved_config["session_arguments"],
        )

        # Create an EnterpriseSessionManager and add to registry
        async def creation_function(source: str, name: str) -> CorePlusSession:
            return session

        enterprise_session_manager = EnterpriseSessionManager(
            source=system_name,
            name=session_name,
            creation_function=creation_function,
        )
        session_id = enterprise_session_manager.full_name

        # Add to session registry  
        await session_registry.add_session(enterprise_session_manager)

        _LOGGER.info(
            f"[mcp_systems_server:create_enterprise_session] Successfully created session "
            f"'{session_name}' on system '{system_name}' with session ID '{session_id}'"
        )

        result.update(
            {
                "success": True,
                "session_id": session_id,
                "system_name": system_name,
                "session_name": session_name,
                "configuration": {
                    "heap_size_gb": resolved_config["heap_size_gb"],
                    "auto_delete_timeout": resolved_config["auto_delete_timeout"],
                    "server": resolved_config["server"],
                    "engine": resolved_config["engine"],
                },
            }
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:create_enterprise_session] Failed to create session "
            f"'{session_name}' on system '{system_name}': {e!r}",
            exc_info=True,
        )
        result["error"] = str(e)
        result["isError"] = True

    return result


def _resolve_session_parameters(
    heap_size_gb: float | None,
    auto_delete_timeout: int | None,
    server: str | None,
    engine: str | None,
    extra_jvm_args: list[str] | None,
    extra_environment_vars: list[str] | None,
    admin_groups: list[str] | None,
    viewer_groups: list[str] | None,
    timeout_seconds: float | None,
    session_arguments: dict[str, Any] | None,
    programming_language: str | None,
    defaults: dict,
) -> dict:
    """Resolve session parameters with priority: tool param -> config default -> API default.

    Args:
        heap_size_gb (float | None): Tool parameter value for JVM heap size in GB.
        auto_delete_timeout (int | None): Tool parameter value for session timeout in seconds.
        server (str | None): Tool parameter value for target server.
        engine (str | None): Tool parameter value for engine type.
        extra_jvm_args (list[str] | None): Tool parameter value for additional JVM arguments.
        extra_environment_vars (list[str] | None): Tool parameter value for environment variables.
        admin_groups (list[str] | None): Tool parameter value for admin user groups.
        viewer_groups (list[str] | None): Tool parameter value for viewer user groups.
        timeout_seconds (float | None): Tool parameter value for session startup timeout.
        session_arguments (dict[str, Any] | None): Tool parameter value for pydeephaven.Session constructor.
        programming_language (str | None): Tool parameter value for session language ("Python" or "Groovy").
        defaults (dict): Configuration defaults dictionary from session_creation config.

    Returns:
        dict: Resolved configuration with all parameters using priority order.
    """
    return {
        "heap_size_gb": heap_size_gb or defaults.get("heap_size_gb"),
        "auto_delete_timeout": (
            auto_delete_timeout
            if auto_delete_timeout is not None
            else defaults.get("auto_delete_timeout")
        ),
        "server": server or defaults.get("server"),
        "engine": engine or defaults.get("engine", "DeephavenCommunity"),
        "extra_jvm_args": extra_jvm_args or defaults.get("extra_jvm_args"),
        "extra_environment_vars": extra_environment_vars
        or defaults.get("extra_environment_vars"),
        "admin_groups": admin_groups or defaults.get("admin_groups"),
        "viewer_groups": viewer_groups or defaults.get("viewer_groups"),
        "timeout_seconds": (
            timeout_seconds
            if timeout_seconds is not None
            else defaults.get("timeout_seconds", 60)
        ),
        "session_arguments": session_arguments or defaults.get("session_arguments"),
        "programming_language": programming_language
        or defaults.get("programming_language", "Python"),
    }


@mcp_server.tool()
async def delete_enterprise_session(
    context: Context,
    system_name: str,
    session_name: str,
) -> dict:
    """
    MCP Tool: Delete an existing enterprise session.

    Removes an enterprise session from the specified enterprise system and removes it from the
    session registry. The session becomes inaccessible for future operations.

    AI Agent Usage:
    - Use this tool to clean up sessions when no longer needed
    - Check 'success' field to verify deletion completed
    - This operation is irreversible - deleted sessions cannot be recovered
    - Session will no longer be accessible via other MCP tools after deletion

    Args:
        context (Context): The MCP context object.
        system_name (str): Name of the enterprise system containing the session.
            Must match a configured enterprise system name.
        session_name (str): Name of the session to delete. Must be an existing session.

    Returns:
        dict: Structured response with deletion details.

        Success response:
        {
            "success": True,
            "session_id": "enterprise:prod-system:analytics-session-001",
            "system_name": "prod-system",
            "session_name": "analytics-session-001"
        }

        Error response:
        {
            "success": False,
            "error": "Session 'enterprise:prod-system:nonexistent-session' not found",
            "isError": True
        }

    Validation and Safety:
        - Verifies enterprise system exists in configuration
        - Checks that the specified session exists in registry
        - Properly closes the session before removal
        - Removes session from registry to prevent future access
        - Provides detailed error messages for troubleshooting

    Common Error Scenarios:
        - System not found: "Enterprise system 'xyz' not found"
        - Session not found: "Session 'enterprise:sys:session' not found"
        - Already deleted: "Session 'enterprise:sys:session' not found"
        - Close failure: "Failed to close session"
        - Registry error: "Failed to remove session from registry"

    Logging:
        - Info: Tool invocation, successful deletion, session cleanup
        - Debug: Session registry operations, session identification
        - Error: All failure scenarios with full context and stack traces

    Note:
        - This operation is irreversible - deleted sessions cannot be recovered
        - Any running queries or tables in the session will be lost
        - Other connections to the same session will lose access
        - Use with caution in production environments
    """
    _LOGGER.info(
        f"[mcp_systems_server:delete_enterprise_session] Invoked: "
        f"system_name={system_name!r}, session_name={session_name!r}"
    )

    result: dict[str, object] = {"success": False}

    try:
        # Get config and session registry
        config_manager: ConfigManager = context.request_context.lifespan_context[
            "config_manager"
        ]
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )

        # Verify enterprise system exists in configuration
        _, error_response = _get_system_config(
            "delete_enterprise_session", config_manager, system_name
        )
        if error_response:
            result.update(error_response)
            return result

        # Create expected session ID
        session_id = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, system_name, session_name
        )

        _LOGGER.debug(
            f"[mcp_systems_server:delete_enterprise_session] Looking for session '{session_id}'"
        )

        # Check if session exists in registry
        try:
            session_manager = await session_registry.get(session_id)
        except KeyError:
            error_msg = f"Session '{session_id}' not found"
            _LOGGER.error(f"[mcp_systems_server:delete_enterprise_session] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        # Verify it's an EnterpriseSessionManager (safety check)
        if not isinstance(session_manager, EnterpriseSessionManager):
            error_msg = f"Session '{session_id}' is not an enterprise session"
            _LOGGER.error(f"[mcp_systems_server:delete_enterprise_session] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        _LOGGER.debug(
            f"[mcp_systems_server:delete_enterprise_session] Found enterprise session manager for '{session_id}'"
        )

        # Close the session if it's active
        try:
            _LOGGER.debug(
                f"[mcp_systems_server:delete_enterprise_session] Closing session '{session_id}'"
            )
            await session_manager.close()
            _LOGGER.debug(
                f"[mcp_systems_server:delete_enterprise_session] Successfully closed session '{session_id}'"
            )
        except Exception as e:
            _LOGGER.warning(
                f"[mcp_systems_server:delete_enterprise_session] Failed to close session '{session_id}': {e}"
            )
            # Continue with removal even if close failed

        # Remove from session registry
        try:
            removed_manager = await session_registry.remove_session(session_id)
            if removed_manager is None:
                error_msg = (
                    f"Session '{session_id}' was not found in registry during removal"
                )
                _LOGGER.warning(
                    f"[mcp_systems_server:delete_enterprise_session] {error_msg}"
                )
            else:
                _LOGGER.debug(
                    f"[mcp_systems_server:delete_enterprise_session] Removed session '{session_id}' from registry"
                )

        except Exception as e:
            error_msg = f"Failed to remove session '{session_id}' from registry: {e}"
            _LOGGER.error(f"[mcp_systems_server:delete_enterprise_session] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        _LOGGER.info(
            f"[mcp_systems_server:delete_enterprise_session] Successfully deleted session "
            f"'{session_name}' from system '{system_name}' (session ID: '{session_id}')"
        )

        result.update(
            {
                "success": True,
                "session_id": session_id,
                "system_name": system_name,
                "session_name": session_name,
            }
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:delete_enterprise_session] Failed to delete session "
            f"'{session_name}' from system '{system_name}': {e!r}",
            exc_info=True,
        )
        result["error"] = str(e)
        result["isError"] = True

    return result
