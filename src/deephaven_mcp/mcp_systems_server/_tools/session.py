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
    format_partial_result,
    get_multi_config,
    get_registry,
)
from deephaven_mcp.resource_manager import (
    BaseItemManager,
    QualifiedSessionId,
    SessionManager,
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
    if type is not None and type not in SystemType:
        return error_response(
            f"Invalid type {type!r}; expected one of "
            f"{[member.value for member in SystemType]}."
        )
    if origin is not None and origin not in SessionOrigin:
        return error_response(
            f"Invalid origin {origin!r}; expected one of "
            f"{[member.value for member in SessionOrigin]}."
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


def _session_matches_filters(
    mgr: SessionManager,
    *,
    type: str | None,
    system: str | None,
    origin: str | None,
) -> bool:
    """Whether a session manager satisfies the active ``sessions_list`` filters.

    Each ``None`` filter matches everything; otherwise the manager's
    corresponding identity attribute must equal the requested value.
    Filtering reads the manager's typed attributes directly rather than
    its serialized row, keeping filtering independent of serialization.
    """
    return (
        (type is None or mgr.system_type.value == type)
        and (system is None or mgr.system == system)
        and (origin is None or mgr.origin.value == origin)
    )


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
    - Use returned 'id' values with other tools like run_script, get_table_data
    - Check 'type' / 'system' / 'origin' fields on returned rows to scope subsequent calls
    - For detailed session information, use session_details with a specific id
    - If 'partial_result' is present, this list may be incomplete: check its 'phase'
      ('loading'/'partial' → discovery still running, retry later; 'failed' → report)
      and 'errors' for which enterprise systems could not be reached

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
        origin (str | None): Optional filter on how the session came
            to be known to MCP. One of:
              - ``"static"`` — declared in configuration at startup
                (community sessions from ``community/sessions/*.json``).
              - ``"dynamic"`` — created at runtime by an MCP tool
                (``session_community_create`` for community,
                ``session_enterprise_create`` for enterprise).
              - ``"discovered"`` — pre-existing on the source system
                and surfaced to MCP (enterprise persistent queries
                read from the DHE controller).
            ``None`` (default) keeps every origin.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if retrieval succeeded, False otherwise.
            - 'sessions' (list[dict]): List of session info dicts. Each contains:
                - 'id' (str): Fully qualified session identifier
                  in the format ``"{type}:{system}:{session_name}"``. Use
                  this verbatim when calling other tools.
                - 'type' (str): ``"community"`` or ``"enterprise"``.
                - 'system' (str): The system identifier (matches
                  ``list_systems``).
                - 'origin' (str | None): ``"static"``, ``"dynamic"``, or
                  ``"discovered"`` describing how the session came to be
                  known to MCP (see the ``origin`` argument for the full
                  definition). ``None`` only for a future manager kind
                  that has not yet been classified.
                - 'session_name' (str): Session name within the system.
            - 'partial_result' (dict, optional): Present only when this list may be
                incomplete — enterprise session discovery is still in progress or some
                systems failed. Contains:
                - 'phase' (str): Machine-readable discovery phase — one of 'not_started',
                    'partial', 'loading', 'completed', 'failed'. Use it to decide whether
                    to retry (still loading) or report (failed).
                - 'detail' (str): Human-readable description of why the result is partial.
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
                    'id': 'enterprise:prod-system:my-session',
                    'type': 'enterprise',
                    'system': 'prod-system',
                    'origin': 'discovered',
                    'session_name': 'my-session',
                },
                {
                    'id': 'community:community:default',
                    'type': 'community',
                    'system': 'community',
                    'origin': 'static',
                    'session_name': 'default',
                },
            ],
        }

    Example Partial Result (an enterprise system failed discovery):
        {
            'success': True,
            'sessions': [
                {
                    'id': 'community:community:default',
                    'type': 'community',
                    'system': 'community',
                    'origin': 'static',
                    'session_name': 'default',
                },
            ],
            'partial_result': {
                'phase': 'completed',
                'detail': 'Some enterprise systems had connection issues during discovery.',
                'errors': {'prod-system': 'connection refused'},
            },
        }

    Example Error Response:
        {'success': False, 'error': 'Failed to retrieve sessions', 'isError': True}

    Error Scenarios:
        - Invalid filter value: returns error response naming the bad argument and the allowed values.
        - Context access errors: Returns error if session_registry cannot be accessed from context.
        - Registry operation errors: Returns error if session_registry.get_all() fails.
        - Session serialization errors: a failure projecting any one session aborts the
          whole call with an error response (rows are uniform; there is no per-row error sentinel).
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

        # The registry holds only session managers; each serializes its
        # own uniform identity row, and the optional filters match against
        # the manager's typed attributes.
        results: list[dict[str, object]] = [
            mgr.to_dict()
            for mgr in snapshot.items.values()
            if _session_matches_filters(mgr, type=type, system=system, origin=origin)
        ]

        response: dict[str, object] = {"success": True, "sessions": results}

        # Flag a partial result (incomplete discovery) from the same atomic snapshot
        partial = format_partial_result(
            snapshot.initialization_phase, snapshot.initialization_errors
        )
        if partial:
            response["partial_result"] = partial

        return response
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:sessions_list] Failed: {e!r}", exc_info=True
        )
        return error_response(str(e))


async def _get_session_liveness_info(
    mgr: BaseItemManager, id: str, attempt_to_connect: bool
) -> tuple[bool, str, str | None]:
    """Get session liveness status and availability.

    This function checks the liveness status of a session using the provided manager.
    It can optionally attempt to connect to the session to verify its actual status.

    Args:
        mgr (BaseItemManager): Session manager for the target session
        id (str): Fully qualified id, used for logging
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
            f"[mcp_systems_server:session_details] Session '{id}' liveness: {liveness_status}, detail: {liveness_detail}"
        )
        return available, liveness_status, liveness_detail
    except Exception as e:
        _LOGGER.warning(
            f"[mcp_systems_server:session_details] Could not check liveness for '{id}': {e!r}"
        )
        return False, "OFFLINE", str(e)


async def _get_session_property[T](
    mgr: BaseItemManager,
    id: str,
    available: bool,
    property_name: str,
    getter_func: Callable[[BaseSession], Awaitable[T]],
) -> T | None:
    """Safely get a session property.

    Args:
        mgr (BaseItemManager): Session manager
        id (str): Fully qualified id.
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
            f"[mcp_systems_server:session_details] Session '{id}' {property_name}: {result}"
        )
        return result
    except Exception as e:
        _LOGGER.warning(
            f"[mcp_systems_server:session_details] Could not get {property_name} for '{id}': {e!r}"
        )
        return None


async def _get_session_programming_language(
    mgr: BaseItemManager, id: str, available: bool
) -> str | None:
    """Get the programming language of a session.

    This function retrieves the programming language (e.g., "python", "groovy")
    associated with the session. If the session is not available, it returns None
    immediately without attempting to connect.

    Args:
        mgr (BaseItemManager): Session manager for the target session
        id (str): Fully qualified id, used for logging
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
            f"[mcp_systems_server:session_details] Session '{id}' programming_language: {programming_language}"
        )
        return programming_language
    except Exception as e:
        _LOGGER.warning(
            f"[mcp_systems_server:session_details] Could not get programming_language for '{id}': {e!r}"
        )
        return None


async def _get_session_versions(
    mgr: BaseItemManager, id: str, available: bool
) -> tuple[str | None, str | None]:
    """Get Deephaven version information from a session.

    Retrieves both community (Core) and enterprise (Core+) version information.
    Returns (None, None) immediately without connecting if the session is unavailable.

    Args:
        mgr (BaseItemManager): Session manager for the target session
        id (str): Fully qualified id, used for logging
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
            f"[mcp_systems_server:session_details] Session '{id}' versions: community={community_version}, enterprise={enterprise_version}"
        )
        return community_version, enterprise_version
    except Exception as e:
        _LOGGER.warning(
            f"[mcp_systems_server:session_details] Could not get Deephaven versions for '{id}': {e!r}"
        )
        return None, None


async def session_details(
    context: Context, id: str, attempt_to_connect: bool = False
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
    - Use sessions_list first to discover available id values
    - IMPORTANT: attempt_to_connect=True creates resource overhead (open sessions consume MCP server resources and each session maintains connections)
    - Only use attempt_to_connect=True for sessions you actually intend to use, not for general discovery or monitoring

    Args:
        context (Context): The MCP context object.
        id (str): Fully qualified id in the form 'type:system:name'
            (e.g. 'community:local:my_worker', 'enterprise:prod:12345'),
            as returned by sessions_list.
        attempt_to_connect (bool, optional): Whether to attempt connecting to the session
            to verify its status. Defaults to False for faster response.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if retrieval succeeded, False otherwise.
            - 'session' (dict): Session details including:
                - id (fully qualified session name)
                - type ("community" or "enterprise")
                - system (matches list_systems names)
                - origin ("static" / "dynamic" / "discovered"; null only for a manager kind not yet classified)
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
    _LOGGER.info(f"[mcp_systems_server:session_details] Invoked for id: {id}")
    try:
        _LOGGER.debug(
            "[mcp_systems_server:session_details] Accessing session registry from context"
        )
        session_registry = get_registry(context)

        # Get the specific session manager directly
        _LOGGER.debug(
            f"[mcp_systems_server:session_details] Retrieving session manager for '{id}'"
        )
        try:
            _t0 = time.monotonic()
            mgr = await session_registry.get(QualifiedSessionId.from_str(id))
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Retrieved session manager for '{id}' in {time.monotonic() - _t0:.2f}s"
            )
        except Exception as e:
            return {
                "success": False,
                "error": f"Session with ID '{id}' not found: {e}",
                "isError": True,
            }

        try:
            # Get basic metadata
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Extracting metadata for session '{id}'"
            )
            # ``session_registry.get(...)`` returns a ``SessionManager``,
            # so the manager serializes itself — common identity plus any
            # detail-level extras (e.g. dynamic connection details). The
            # caller layers on the runtime facts it queries below.
            session_info: dict[str, object] = mgr.to_dict(verbose=True)
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Session '{id}' metadata: "
                f"type={session_info['type']}, system={session_info['system']}, "
                f"origin={session_info['origin']}, name={session_info['session_name']}"
            )

            # Get liveness status and availability
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Checking liveness for session '{id}' (attempt_to_connect={attempt_to_connect})"
            )
            _t1 = time.monotonic()
            available, liveness_status, liveness_detail = (
                await _get_session_liveness_info(mgr, id, attempt_to_connect)
            )
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Liveness check for '{id}' took {time.monotonic() - _t1:.2f}s"
            )

            # Get session properties using helper functions
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Retrieving session properties for '{id}' (available={available})"
            )
            programming_language = await _get_session_programming_language(
                mgr, id, available
            )

            # TODO: should the versions be cached?
            programming_language_version = await _get_session_property(
                mgr,
                id,
                available,
                "programming_language_version",
                queries.get_programming_language_version,
            )

            community_version, enterprise_version = await _get_session_versions(
                mgr, id, available
            )
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Completed property retrieval for session '{id}'"
            )

            # The runtime facts are queried from the live session, not
            # read from manager state, so they are the caller's
            # responsibility. They are the only nullable source here —
            # the manager serialization above never yields None — so
            # None-filtering is scoped to just these.
            runtime_facts = {
                "available": available,
                "liveness_status": liveness_status,
                "liveness_detail": liveness_detail,
                "programming_language": programming_language,
                "programming_language_version": programming_language_version,
                "deephaven_community_version": community_version,
                "deephaven_enterprise_version": enterprise_version,
            }
            session_info.update(
                {k: v for k, v in runtime_facts.items() if v is not None}
            )
            _LOGGER.debug(
                f"[mcp_systems_server:session_details] Built session info for '{id}' with {len(session_info)} fields"
            )

            return {"success": True, "session": session_info}

        except Exception as e:
            _LOGGER.warning(
                f"[mcp_systems_server:session_details] Could not process session '{id}': {e!r}"
            )
            return {
                "success": False,
                "error": f"Error processing session '{id}': {str(e)}",
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
            :class:`~deephaven_mcp.config.ConfigTree` and
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
