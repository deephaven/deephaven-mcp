"""
Session Management MCP Tools - List and Query Sessions.

Provides MCP tools for viewing and managing Deephaven sessions:
- sessions_list: List all active sessions (Community and Enterprise)
- session_details: Get detailed information about a specific session

These tools work with both Community and Enterprise sessions.
"""

import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar

from mcp.server.fastmcp import Context

from deephaven_mcp import queries
from deephaven_mcp.client import BaseSession
from deephaven_mcp.mcp_systems_server._tools.mcp_server import mcp_server
from deephaven_mcp.resource_manager import (
    BaseItemManager,
    CombinedSessionRegistry,
    DynamicCommunitySessionManager,
)

T = TypeVar("T")

_LOGGER = logging.getLogger(__name__)


# Session management constants
DEFAULT_MAX_CONCURRENT_SESSIONS = 5
"""
Default maximum number of concurrent sessions per enterprise system.

This default is used when session_creation.max_concurrent_sessions is not specified
in the enterprise system configuration. Can be overridden per system in the config.
"""


DEFAULT_PROGRAMMING_LANGUAGE = "Python"
"""Default programming language for community and enterprise sessions when not specified in config."""


@mcp_server.tool()
async def sessions_list(context: Context) -> dict:
    """
    MCP Tool: List all sessions with basic metadata.

    Returns basic information about all available sessions (community and enterprise).
    This is a lightweight operation that doesn't connect to sessions or check their status.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

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
    _LOGGER.info("[mcp_systems_server:sessions_list] Invoked")
    try:
        _LOGGER.debug(
            "[mcp_systems_server:sessions_list] Accessing session registry from context"
        )
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )
        _LOGGER.debug(
            "[mcp_systems_server:sessions_list] Retrieving all sessions from registry"
        )
        sessions = await session_registry.get_all()

        _LOGGER.info(
            f"[mcp_systems_server:sessions_list] Found {len(sessions)} sessions."
        )

        results = []
        for fq_name, mgr in sessions.items():
            _LOGGER.debug(
                f"[mcp_systems_server:sessions_list] Processing session '{fq_name}'"
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
                    f"[mcp_systems_server:sessions_list] Could not process session '{fq_name}': {e!r}"
                )
                results.append({"session_id": fq_name, "error": str(e)})
        return {"success": True, "sessions": results}
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:sessions_list] Failed: {e!r}", exc_info=True
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
        mgr (BaseItemManager): Session manager for the target session
        session_id (str): Session identifier for logging purposes
        attempt_to_connect (bool): Whether to attempt connecting to verify status

    Returns:
        tuple[bool, str, str | None]: A 3-tuple containing:
            - available (bool): Whether the session is available and responsive
            - liveness_status (str): Status classification ("ONLINE", "OFFLINE", etc.)
            - liveness_detail (str | None): Detailed explanation of the status
    """
    try:
        status, detail = await mgr.liveness_status(ensure_item=attempt_to_connect)
        liveness_status = status.name
        liveness_detail = detail
        available = await mgr.is_alive()
        _LOGGER.debug(
            f"[mcp_systems_server:session_details] Session '{session_id}' liveness: {liveness_status}, detail: {liveness_detail}"
        )
        return available, liveness_status, liveness_detail
    except Exception as e:
        _LOGGER.warning(
            f"[mcp_systems_server:session_details] Could not check liveness for '{session_id}': {e!r}"
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
            f"[mcp_systems_server:session_details] Session '{session_id}' {property_name}: {result}"
        )
        return result
    except Exception as e:
        _LOGGER.warning(
            f"[mcp_systems_server:session_details] Could not get {property_name} for '{session_id}': {e!r}"
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
        mgr (BaseItemManager): Session manager for the target session
        session_id (str): Session identifier for logging purposes
        available (bool): Whether the session is available (pre-checked)

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
            f"[mcp_systems_server:session_details] Session '{session_id}' programming_language: {programming_language}"
        )
        return programming_language
    except Exception as e:
        _LOGGER.warning(
            f"[mcp_systems_server:session_details] Could not get programming_language for '{session_id}': {e!r}"
        )
        return None


async def _get_session_versions(
    mgr: BaseItemManager, session_id: str, available: bool
) -> tuple[str | None, str | None]:
    """
    Get Deephaven version information from a session.

    Retrieves both community (Core) and enterprise (Core+) version information.
    Returns (None, None) immediately without connecting if the session is unavailable.

    Args:
        mgr (BaseItemManager): Session manager for the target session
        session_id (str): Session identifier for logging purposes
        available (bool): Whether the session is available (pre-checked)

    Returns:
        tuple[str | None, str | None]: A 2-tuple containing:
            - community_version (str | None): Deephaven Community/Core version (e.g., "0.24.0")
            - enterprise_version (str | None): Deephaven Enterprise/Core+ version
                                              (e.g., "0.24.0") or None if not enterprise
    """
    if not available:
        return None, None

    try:
        session = await mgr.get()
        community_version, enterprise_version = await queries.get_dh_versions(session)
        _LOGGER.debug(
            f"[mcp_systems_server:session_details] Session '{session_id}' versions: community={community_version}, enterprise={enterprise_version}"
        )
        return community_version, enterprise_version
    except Exception as e:
        _LOGGER.warning(
            f"[mcp_systems_server:session_details] Could not get Deephaven versions for '{session_id}': {e!r}"
        )
        return None, None


@mcp_server.tool()
async def session_details(
    context: Context, session_id: str, attempt_to_connect: bool = False
) -> dict:
    """
    MCP Tool: Get detailed information about a specific session.

    Returns comprehensive status and configuration information for a specific session,
    including availability status, programming language, and version information.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

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
                - connection_url (str, optional): Base connection URL for dynamically created sessions (e.g., "http://localhost:45123")
                - connection_url_with_auth (str, optional): Connection URL with auth token for dynamically created sessions
                - auth_type (str, optional): Authentication type for dynamically created sessions ("PSK" or "Anonymous")
                - launch_method (str, optional): Launch method for dynamically created sessions ("docker" or "python")
                - port (int, optional): Port number for dynamically created sessions
                - container_id (str, optional): Docker container ID for Docker-launched sessions
                - process_id (int, optional): Process ID for python-launched sessions
            - 'error' (str, optional): Error message if retrieval failed.
            - 'isError' (bool, optional): Present and True if this is an error response.

        Note: The version fields (programming_language_version, deephaven_community_version,
        deephaven_enterprise_version) will only be present if the session is available and
        the information could be retrieved successfully. Fields with null values are excluded
        from the response.
    """
    _LOGGER.info(
        f"[mcp_systems_server:session_details] Invoked for session_id: {session_id}"
    )
    try:
        _LOGGER.debug(
            "[mcp_systems_server:session_details] Accessing session registry from context"
        )
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )

        # Get the specific session manager directly
        _LOGGER.debug(
            f"[mcp_systems_server:session_details] Retrieving session manager for '{session_id}'"
        )
        try:
            mgr = await session_registry.get(session_id)
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Successfully retrieved session manager for '{session_id}'"
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
                f"[mcp_systems_server:session_details] Extracting metadata for session '{session_id}'"
            )
            system_type_str = mgr.system_type.name
            source = mgr.source
            session_name = mgr.name
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Session '{session_id}' metadata: type={system_type_str}, source={source}, name={session_name}"
            )

            # Get liveness status and availability
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Checking liveness for session '{session_id}' (attempt_to_connect={attempt_to_connect})"
            )
            available, liveness_status, liveness_detail = (
                await _get_session_liveness_info(mgr, session_id, attempt_to_connect)
            )

            # Get session properties using helper functions
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Retrieving session properties for '{session_id}' (available={available})"
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
                f"[mcp_systems_server:session_details] Completed property retrieval for session '{session_id}'"
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

            # Add dynamic session information if applicable
            # Check if this is a manager type that provides additional session details
            if isinstance(mgr, DynamicCommunitySessionManager):
                try:
                    dynamic_info = mgr.to_dict()
                    # Merge all fields from to_dict() into session_info
                    # This automatically includes any new fields added to to_dict() in the future
                    session_info_with_nones.update(dynamic_info)
                    _LOGGER.debug(
                        f"[mcp_systems_server:session_details] Added dynamic session info for '{session_id}'"
                    )
                except Exception as e:
                    _LOGGER.warning(
                        f"[mcp_systems_server:session_details] Could not retrieve dynamic session info for '{session_id}': {e}"
                    )

            # Filter out None values
            session_info = {
                k: v for k, v in session_info_with_nones.items() if v is not None
            }
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Built session info for '{session_id}' with {len(session_info)} fields"
            )

            return {"success": True, "session": session_info}

        except Exception as e:
            _LOGGER.warning(
                f"[mcp_systems_server:session_details] Could not process session '{session_id}': {e!r}"
            )
            return {
                "success": False,
                "error": f"Error processing session '{session_id}': {str(e)}",
                "isError": True,
            }

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_details] Failed: {e!r}", exc_info=True
        )
        return {"success": False, "error": str(e), "isError": True}
