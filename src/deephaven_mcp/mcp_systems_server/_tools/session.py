"""Session Management MCP Tools - List and Query Sessions.

Provides MCP tools for viewing and managing Deephaven sessions:
- sessions_list: List all active sessions (Community and Enterprise)
- session_details: Get detailed information about a specific session

These tools work with both Community and Enterprise sessions.
"""

import logging
import time
from collections.abc import Awaitable, Callable

from mcp.server.fastmcp import Context, FastMCP

from deephaven_mcp import queries
from deephaven_mcp.client import BaseSession
from deephaven_mcp.mcp_systems_server._tools.shared import (
    error_response,
    format_initialization_status,
    get_multi_config,
    get_registry,
)
from deephaven_mcp.resource_manager import (
    BaseItemManager,
    CommunitySessionManager,
    CorePlusSessionFactoryManager,
    DynamicCommunitySessionManager,
    SessionOrigin,
    SystemType,
)

_LOGGER = logging.getLogger(__name__)


def _validate_sessions_list_filters(
    context: Context,
    *,
    type: str | None,
    system: str | None,
    origin: str | None,
) -> dict | None:
    """Validate ``sessions_list`` filter arguments.

    Returns ``None`` when all filters are acceptable, or a standard
    error-response dict when any filter is invalid or self-inconsistent.
    """
    if type is not None and type not in (
        SystemType.COMMUNITY.value,
        SystemType.ENTERPRISE.value,
    ):
        return error_response(
            f"Invalid type {type!r}; expected one of "
            f"{[SystemType.COMMUNITY.value, SystemType.ENTERPRISE.value]}."
        )
    if origin is not None and origin not in (
        SessionOrigin.STATIC.value,
        SessionOrigin.DYNAMIC.value,
    ):
        return error_response(
            f"Invalid origin {origin!r}; expected one of "
            f"{[SessionOrigin.STATIC.value, SessionOrigin.DYNAMIC.value]}."
        )
    if origin is not None and type == SystemType.ENTERPRISE.value:
        return error_response(
            "origin filter is meaningful only for community sessions; "
            "remove origin or set type='community'."
        )
    if system is None:
        return None
    multi_config = get_multi_config(context)
    known_systems = {name for name, _ in multi_config.list_systems()}
    if system not in known_systems:
        return error_response(
            f"Unknown system {system!r}. Known systems: "
            f"{sorted(known_systems)}. Call list_systems to enumerate "
            f"valid values."
        )
    if type is not None:
        expected_type = (
            SystemType.COMMUNITY.value
            if system == "community"
            else SystemType.ENTERPRISE.value
        )
        if type != expected_type:
            return error_response(
                f"system={system!r} implies type={expected_type!r}, "
                f"but type={type!r} was supplied."
            )
    return None


def _build_sessions_list_row(
    fq_name: str,
    mgr: BaseItemManager,
    *,
    type: str | None,
    system: str | None,
    origin: str | None,
) -> dict[str, object] | None:
    """Project one registry entry into a ``sessions_list`` result row.

    Returns ``None`` to drop the entry (factory-kind manager or filter
    miss). Returns a row with an ``error`` key when metadata extraction
    fails for an individual session, so callers can surface partial
    failures alongside successful rows.
    """
    try:
        # Factories are internal; never list them as sessions. The class
        # hierarchy itself encodes session-vs-factory.
        if isinstance(mgr, CorePlusSessionFactoryManager):
            return None
        row_type = mgr.system_type.value
        row_system = mgr.system
        # ``origin`` is community-session-only; enterprise sessions report
        # ``None`` in the listing row.
        row_origin = (
            mgr.origin.value if isinstance(mgr, CommunitySessionManager) else None
        )

        if type is not None and row_type != type:
            return None
        if system is not None and row_system != system:
            return None
        if origin is not None and row_origin != origin:
            return None

        return {
            "session_id": fq_name,
            "type": row_type,
            "system": row_system,
            "origin": row_origin,
            "session_name": mgr.name,
        }
    except Exception as e:
        _LOGGER.warning(
            f"[mcp_systems_server:sessions_list] Could not process session "
            f"'{fq_name}': {e!r}"
        )
        return {"session_id": fq_name, "error": str(e)}


async def sessions_list(
    context: Context,
    *,
    type: str | None = None,
    system: str | None = None,
    origin: str | None = None,
) -> dict:
    """MCP Tool: List sessions with basic metadata.

    Returns basic information about available sessions (community and
    enterprise). This is a lightweight operation that doesn't connect to
    sessions or check their status. Three orthogonal optional filters
    (``type``, ``system``, ``origin``) scope the result; passing none of
    them returns every session.

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
    - Check 'type' / 'system' / 'origin' fields on returned rows to scope subsequent calls
    - For detailed session information, use session_details with a specific session_id

    Args:
        context (Context): The MCP context object.
        type (str | None): Optional filter on session type. One of
            ``"community"`` or ``"enterprise"``. ``None`` (default) keeps
            both. Any other value yields an error response.
        system (str | None): Optional filter on the system this session
            belongs to (matches the ``name`` field returned by
            ``list_systems``): ``"community"`` for the community
            umbrella, or any configured enterprise ``system_name``. Exact
            and case-sensitive. ``None`` (default) keeps every system.
            A value not in ``list_systems`` yields an error response.
        origin (str | None): Optional filter on how a community session
            was created. One of ``"static"`` (declared in
            ``community/sessions/*.json``) or ``"dynamic"`` (created at
            runtime via ``session_community_create``). Only meaningful
            for community sessions; combining ``origin`` with
            ``type="enterprise"`` yields an error response. ``None``
            (default) keeps every origin.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if retrieval succeeded, False otherwise.
            - 'sessions' (list[dict]): List of session info dicts. Each contains:
                - 'session_id' (str): Fully qualified session identifier
                  in the format ``"{type}:{system}:{session_name}"``. Use
                  this verbatim when calling other tools.
                - 'type' (str): ``"community"`` or ``"enterprise"``.
                - 'system' (str): The system identifier (matches
                  ``list_systems``).
                - 'origin' (str | None): ``"static"`` or ``"dynamic"`` for
                  community sessions; ``None`` for enterprise sessions.
                - 'session_name' (str): Session name within the system.
            - 'initialization' (dict, optional): Present when enterprise session discovery is
                still in progress or completed with errors. Contains:
                - 'status' (str): Human-readable description of the initialization state.
                - 'errors' (dict[str, str], optional): Present when one or more enterprise systems
                    had connection errors during initial discovery. Keys are factory names, values
                    are error descriptions.
            - 'error' (str, optional): Error message if retrieval failed.
            - 'isError' (bool, optional): Present and True if this is an error response.

    Example Successful Response:
        {
            'success': True,
            'sessions': [
                {
                    'session_id': 'enterprise:prod-system:my-session',
                    'type': 'enterprise',
                    'system': 'prod-system',
                    'origin': None,
                    'session_name': 'my-session',
                },
                {
                    'session_id': 'community:community:default',
                    'type': 'community',
                    'system': 'community',
                    'origin': 'static',
                    'session_name': 'default',
                },
            ],
        }

    Example Error Response:
        {'success': False, 'error': 'Failed to retrieve sessions', 'isError': True}

    Error Scenarios:
        - Invalid filter value: returns error response naming the bad argument and the allowed values.
        - Context access errors: Returns error if session_registry cannot be accessed from context.
        - Registry operation errors: Returns error if session_registry.get_all() fails.
        - Session processing errors: A row with ``error`` key replaces a session whose metadata could not be extracted.
    """
    _LOGGER.info(
        f"[mcp_systems_server:sessions_list] Invoked: type={type!r} "
        f"system={system!r} origin={origin!r}"
    )
    try:
        validation_error = _validate_sessions_list_filters(
            context, type=type, system=system, origin=origin
        )
        if validation_error is not None:
            return validation_error

        # ---- Fetch and filter ----------------------------------------------------
        _LOGGER.debug(
            "[mcp_systems_server:sessions_list] Accessing session registry from context"
        )
        session_registry = get_registry(context)
        snapshot = await session_registry.get_all()

        _LOGGER.info(
            f"[mcp_systems_server:sessions_list] Found {len(snapshot.items)} sessions, "
            f"init_phase={snapshot.initialization_phase.value}, "
            f"init_errors={len(snapshot.initialization_errors)}"
        )
        if snapshot.initialization_errors:
            _LOGGER.warning(
                f"[mcp_systems_server:sessions_list] Initialization errors: "
                f"{snapshot.initialization_errors}"
            )

        results: list[dict[str, object]] = []
        for fq_name, mgr in snapshot.items.items():
            row = _build_sessions_list_row(
                fq_name, mgr, type=type, system=system, origin=origin
            )
            if row is not None:
                results.append(row)

        response: dict[str, object] = {"success": True, "sessions": results}

        # Surface initialization status from the same atomic snapshot
        init_info = format_initialization_status(
            snapshot.initialization_phase, snapshot.initialization_errors
        )
        if init_info:
            response["initialization"] = init_info

        return response
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:sessions_list] Failed: {e!r}", exc_info=True
        )
        return error_response(str(e))


async def _get_session_liveness_info(
    mgr: BaseItemManager, session_id: str, attempt_to_connect: bool
) -> tuple[bool, str, str | None]:
    """Get session liveness status and availability.

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


async def _get_session_property[T](
    mgr: BaseItemManager,
    session_id: str,
    available: bool,
    property_name: str,
    getter_func: Callable[[BaseSession], Awaitable[T]],
) -> T | None:
    """Safely get a session property.

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
    """Get the programming language of a session.

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
    """Get Deephaven version information from a session.

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


async def session_details(
    context: Context, session_id: str, attempt_to_connect: bool = False
) -> dict:
    """MCP Tool: Get detailed information about a specific session.

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
    - Use sessions_list first to discover available session_id values
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
                - system (matches list_systems names)
                - origin ("static" / "dynamic" for community; null for enterprise)
                - session_name (session name)
                - available (bool): Whether the session is available
                - liveness_status (str): Status classification ("ONLINE", "OFFLINE", etc.)
                - liveness_detail (str, optional): Detailed explanation of the status, omitted if unavailable
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
        session_registry = get_registry(context)

        # Get the specific session manager directly
        _LOGGER.debug(
            f"[mcp_systems_server:session_details] Retrieving session manager for '{session_id}'"
        )
        try:
            _t0 = time.monotonic()
            mgr = await session_registry.get(session_id)
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Retrieved session manager for '{session_id}' in {time.monotonic() - _t0:.2f}s"
            )
        except Exception as e:
            return {
                "success": False,
                "error": f"Session with ID '{session_id}' not found: {e}",
                "isError": True,
            }

        try:
            # Get basic metadata
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Extracting metadata for session '{session_id}'"
            )
            system_type_str = mgr.system_type.value
            mgr_system = mgr.system
            mgr_origin = (
                mgr.origin.value if isinstance(mgr, CommunitySessionManager) else None
            )
            session_name = mgr.name
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Session '{session_id}' metadata: "
                f"type={system_type_str}, system={mgr_system}, origin={mgr_origin}, name={session_name}"
            )

            # Get liveness status and availability
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Checking liveness for session '{session_id}' (attempt_to_connect={attempt_to_connect})"
            )
            _t1 = time.monotonic()
            available, liveness_status, liveness_detail = (
                await _get_session_liveness_info(mgr, session_id, attempt_to_connect)
            )
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Liveness check for '{session_id}' took {time.monotonic() - _t1:.2f}s"
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
                "system": mgr_system,
                "origin": mgr_origin,
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


async def list_systems(context: Context) -> dict:
    """MCP Tool: List every Deephaven system the server is configured to serve.

    Returns a discovery list of every Community session and Enterprise
    system loaded from the on-disk configuration tree at startup. Use
    this tool to find the ``system`` argument required by the
    enterprise tools (``enterprise_systems_status``,
    ``session_enterprise_create``, ``pq_list``, ``pq_create``,
    ``pq_name_to_id``).

    Terminology Note:
        - 'System' here means either a static Community session or an
          Enterprise (Core+) system; it is the source dimension of
          every fully qualified session id.
        - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
        - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
        - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
        - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
        - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
        - 'DHC' is shorthand for Deephaven Community (also called 'Core')
        - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    Args:
        context (Context): The MCP context object.

    Returns:
        dict: ``{"success": True, "systems": [{"name": ..., "type":
            "community"|"enterprise"}, ...]}``.

            The list is built from
            :class:`~deephaven_mcp.config.MultiSystemConfig` and
            therefore reflects exactly what the server validated at
            startup; restart the server to pick up changes.
    """
    multi_config = get_multi_config(context)
    # SystemType is a StrEnum, so JSON encodes ref.type as its lowercase value
    # ("community" / "enterprise"); the MCP-facing JSON shape is unchanged.
    systems = [
        {"name": ref.name, "type": ref.type} for ref in multi_config.list_systems()
    ]
    _LOGGER.info(
        f"[mcp_systems_server:list_systems] Returning {len(systems)} configured systems"
    )
    return {"success": True, "systems": systems}


def register_tools(server: FastMCP) -> None:
    """Register all session listing/details tools with the given FastMCP server.

    Tools registered:
        - ``list_systems``: discovery — every configured system.
        - ``sessions_list``: every session known to the registry.
        - ``session_details``: details for a single session id.

    Args:
        server (FastMCP): The server to register tools with.
    """
    server.tool()(list_systems)
    server.tool()(sessions_list)
    server.tool()(session_details)
