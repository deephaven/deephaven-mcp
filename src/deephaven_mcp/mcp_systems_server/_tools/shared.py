"""Shared Utilities - Internal Helper Functions.

Provides internal helper functions used across multiple MCP tool modules:
- Response size checking and validation
- Common error handling patterns
- Shared data formatting utilities

This module contains private helper functions not exposed as MCP tools.
"""

import json
import logging

import pyarrow
from mcp.server.fastmcp import Context

from deephaven_mcp._exceptions import InternalError
from deephaven_mcp._redaction import REDACTED
from deephaven_mcp.auth.credentials import Credentials
from deephaven_mcp.auth.middleware import SCOPE_KEY_CREDENTIALS
from deephaven_mcp.client import BaseSession, CorePlusSession
from deephaven_mcp.config import ConfigManager
from deephaven_mcp.formatters import format_table_data
from deephaven_mcp.mcp_systems_server._lifespan import LifespanContext
from deephaven_mcp.resource_manager import (
    BaseRegistry,
    CommunitySessionRegistry,
    EnterpriseSessionRegistry,
    InitializationPhase,
)

_LOGGER = logging.getLogger(__name__)


def error_response(msg: str) -> dict[str, object]:
    """Return a standard MCP tool error response dict."""
    return {"success": False, "error": msg, "isError": True}


def format_initialization_status(
    phase: InitializationPhase,
    init_errors: dict[str, str],
) -> dict[str, object] | None:
    """Format initialization phase and errors into a response-ready dict.

    Pure formatting function — does not query any registry.  Callers are
    responsible for obtaining *phase* and *init_errors* from the same
    atomic snapshot (e.g. via ``BaseRegistry.get_all()`` on the shared
    registry instance).

    Returns ``None`` when there is nothing to report (completed without
    errors), so callers can simply do::

        init_info = format_initialization_status(phase, errors)
        if init_info:
            response["initialization"] = init_info

    Args:
        phase (InitializationPhase): The current initialization phase.
        init_errors (dict[str, str]): Dict mapping factory names to error descriptions.

    Returns:
        A dict with ``status`` (str, always) and ``errors``
        (dict[str, str], only when present), or ``None`` if initialization
        completed cleanly.
    """
    init_info: dict[str, object] = {}
    if phase == InitializationPhase.FAILED:
        init_info["status"] = (
            "Enterprise session discovery failed critically (e.g. cancelled during shutdown). "
            "The registry may have partial or no data."
        )
    elif phase in (InitializationPhase.NOT_STARTED, InitializationPhase.PARTIAL):
        init_info["status"] = (
            "Enterprise session discovery has not yet started. "
            "Some sessions or systems may not yet be visible."
        )
    elif phase == InitializationPhase.LOADING:
        init_info["status"] = (
            "Enterprise session discovery is actively running. "
            "Some sessions or systems may not yet be visible."
        )
    elif init_errors:
        # SIMPLE or COMPLETED — only report if there were errors
        init_info["status"] = (
            "Some enterprise systems had connection issues during discovery."
        )
    if init_errors:
        init_info["errors"] = init_errors
    return init_info or None


def get_config_manager(context: Context) -> ConfigManager:
    """Extract the ConfigManager from the MCP lifespan context.

    Args:
        context (Context): The MCP context object.

    Returns:
        ConfigManager: The server's config manager instance.
    """
    lifespan_context: LifespanContext[BaseRegistry] = (
        context.request_context.lifespan_context
    )
    return lifespan_context["config_manager"]


def get_mcp_session_id(ctx: Context) -> str:
    """Extract the MCP session ID from the request headers.

    Raises:
        InternalError: If the mcp-session-id header is absent. Every MCP request over
            streamable-HTTP carries an mcp-session-id; absence indicates a misconfigured
            transport or unauthorized caller. There is no fallback — a default would
            collapse per-session isolation and is a security risk.
    """
    request = ctx.request_context.request
    if request is not None:
        session_id = request.headers.get("mcp-session-id")
        if session_id:
            return str(session_id)
    raise InternalError(
        "No mcp-session-id found in request headers. "
        "All MCP tool calls must originate from an authenticated session."
    )


async def get_registry_from_context(context: Context) -> BaseRegistry:
    """Get the per-MCP-session Deephaven registry, creating it on first access.

    Extracts the mcp-session-id from the request headers, then returns the
    per-session registry from the lifespan context, creating one if this is the
    session's first tool call.

    Args:
        context (Context): The MCP context object.

    Returns:
        BaseRegistry: The per-session registry for the current MCP session.

    Raises:
        InternalError: If the mcp-session-id header is absent.
    """
    mcp_session_id = get_mcp_session_id(context)
    lifespan_context: LifespanContext[BaseRegistry] = (
        context.request_context.lifespan_context
    )
    session_registry_manager = lifespan_context["session_registry_manager"]
    return await session_registry_manager.get_or_create_registry(
        mcp_session_id, get_config_manager(context)
    )


async def get_community_registry(context: Context) -> CommunitySessionRegistry:
    """Get the per-MCP-session community registry, creating it on first access.

    Delegates to :func:`get_registry_from_context` and validates that the result is a
    :class:`~deephaven_mcp.resource_manager.CommunitySessionRegistry`.

    Args:
        context (Context): The MCP context object.

    Returns:
        CommunitySessionRegistry: The per-session community registry.

    Raises:
        InternalError: If the mcp-session-id header is absent or the registry is not a
            CommunitySessionRegistry (indicates a server misconfiguration).
    """
    registry = await get_registry_from_context(context)
    if not isinstance(registry, CommunitySessionRegistry):
        raise InternalError(
            f"Expected CommunitySessionRegistry, got {type(registry).__name__}."
        )
    return registry


def _get_request_credentials(context: Context) -> Credentials:
    """Return the per-request :data:`Credentials` attached by the auth middleware.

    The enterprise auth middleware writes the resolved credentials into the
    ASGI ``scope`` under :data:`SCOPE_KEY_CREDENTIALS` on every authenticated
    request. This helper reads that value from
    ``context.request_context.request.scope``.

    Args:
        context (Context): The MCP context object.

    Returns:
        Credentials: The credentials produced by whichever
            :class:`~deephaven_mcp.auth.backends.AuthBackend` matched the request.

    Raises:
        InternalError: If the MCP context has no associated HTTP request,
            or if the middleware did not attach credentials to the scope
            (which would only happen if the enterprise server were
            mounted without :class:`AuthenticationMiddleware`).
    """
    request = context.request_context.request
    if request is None:
        raise InternalError(
            "MCP context has no associated HTTP request; the enterprise "
            "server requires per-request authentication."
        )
    creds: Credentials | None = request.scope.get(SCOPE_KEY_CREDENTIALS)
    if creds is None:
        raise InternalError(
            "Authenticated credentials are missing from the request scope. "
            "AuthenticationMiddleware must run before any enterprise tool "
            "handler."
        )
    return creds


async def get_enterprise_registry(context: Context) -> EnterpriseSessionRegistry:
    """Get the per-MCP-session enterprise registry, ready for use by tools.

    On first access for an MCP session, the registry is created from the
    server config (no credentials yet). Every call to this helper then
    binds the per-request credentials to the registry via
    :meth:`~deephaven_mcp.resource_manager.EnterpriseSessionRegistry.bind_credentials`,
    which is idempotent for the lifetime of the MCP session: the first
    bind creates the
    :class:`~deephaven_mcp.resource_manager._manager.CorePlusSessionFactoryManager`
    and starts background discovery; subsequent calls with the same
    credentials are no-ops; calls with different credentials raise.

    Args:
        context (Context): The MCP context object.

    Returns:
        EnterpriseSessionRegistry: The per-session enterprise registry,
            with credentials bound.

    Raises:
        InternalError: If the mcp-session-id header is absent, the
            registry is not an :class:`EnterpriseSessionRegistry` (server
            misconfiguration), the request has no scope-attached
            credentials, or the request carries a credential type that
            is not valid for enterprise (e.g.
            :class:`~deephaven_mcp.auth.credentials.PSKCredentials`).
    """
    registry = await get_registry_from_context(context)
    if not isinstance(registry, EnterpriseSessionRegistry):
        raise InternalError(
            f"Expected EnterpriseSessionRegistry, got {type(registry).__name__}."
        )
    creds = _get_request_credentials(context)
    # bind_credentials is the single rejection point for unsupported
    # credential subclasses (e.g. PSKCredentials, which only the
    # community server accepts); it raises InternalError on mismatch.
    await registry.bind_credentials(creds)
    return registry


async def get_session_from_context(
    function_name: str, context: Context, session_id: str
) -> BaseSession:
    """Get an active session from the MCP context.

    This helper eliminates duplication of the common pattern for accessing
    sessions from the MCP context. It handles the standard flow of:
    1. Extracting the per-MCP-session registry via session_registry_manager
    2. Getting the session_manager for the session_id
    3. Establishing the session connection

    Args:
        function_name (str): Name of calling function for logging purposes
        context (Context): The MCP context object containing lifespan context
        session_id (str): ID of the session to retrieve

    Returns:
        BaseSession: The active session connection

    Raises:
        InternalError: If the mcp-session-id header is absent
        RegistryItemNotFoundError: If session_id not found in registry
        Exception: If session cannot be established or context is invalid
    """
    _LOGGER.debug(
        f"[mcp_systems_server:{function_name}] Accessing session registry from context"
    )
    session_registry = await get_registry_from_context(context)

    _LOGGER.debug(
        f"[mcp_systems_server:{function_name}] Retrieving session manager for '{session_id}'"
    )
    session_manager = await session_registry.get(session_id)

    _LOGGER.debug(
        f"[mcp_systems_server:{function_name}] Establishing session connection for '{session_id}'"
    )
    session: BaseSession = await session_manager.get()

    _LOGGER.info(
        f"[mcp_systems_server:{function_name}] Session established for '{session_id}'"
    )

    return session


async def get_enterprise_session(
    function_name: str, context: Context, session_id: str
) -> tuple[CorePlusSession | None, dict[str, object] | None]:
    """Get and validate an enterprise (Core+) session from context.

    This helper combines session retrieval and validation into a single clean operation,
    consolidating the common pattern of getting a session and verifying it's an enterprise
    (Core+) session. This eliminates code duplication across catalog-related tools.

    Args:
        function_name (str): Name of calling function for logging and error messages.
        context (Context): The MCP context object containing lifespan context with session_registry.
        session_id (str): ID of the session to retrieve (e.g., "enterprise:prod:analytics").

    Returns:
        tuple[CorePlusSession | None, dict[str, object] | None]: A 2-tuple (session, error) where:
            - session (CorePlusSession | None): The validated enterprise session on success, None on failure.
            - error (dict[str, object] | None): None on success, structured error dict on failure with keys:
                - 'success': False
                - 'error': str (human-readable error message)
                - 'isError': True

    Error Conditions:
        - Session not found in registry
        - Session is not a CorePlusSession (community session provided)
        - Any exception during session retrieval

    Example:
        >>> session, error = await get_enterprise_session("catalog_tables_schema", context, "enterprise:prod:analytics")
        >>> if error:
        ...     return error
        >>> session = cast(CorePlusSession, session)  # Type narrowing for mypy
    """
    try:
        # Get session from context
        session = await get_session_from_context(function_name, context, session_id)

        # Validate it's an enterprise session
        if not isinstance(session, CorePlusSession):
            error_msg = (
                f"{function_name} only works with enterprise (Core+) sessions, "
                f"but session '{session_id}' is {type(session).__name__}"
            )
            _LOGGER.error(f"[mcp_systems_server:{function_name}] {error_msg}")
            return None, error_response(error_msg)

        return session, None
    except Exception as e:
        error_msg = f"Failed to get session '{session_id}': {e}"
        _LOGGER.error(f"[mcp_systems_server:{function_name}] {error_msg}")
        return None, error_response(error_msg)


# Size limits for table data responses
MAX_RESPONSE_SIZE = 50_000_000  # 50MB hard limit
WARNING_SIZE = 5_000_000  # 5MB warning threshold


def check_response_size(table_name: str, estimated_size: int) -> dict | None:
    """Check if estimated response size is within acceptable limits.

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
        return error_response(
            f"Response would be ~{estimated_size/1_000_000:.1f}MB (max 50MB). Please reduce max_rows."
        )

    return None  # Size is acceptable


def format_meta_table_result(
    arrow_meta_table: pyarrow.Table,
    table_name: str,
    namespace: str | None = None,
) -> dict:
    """Format a PyArrow meta table into a standardized result dictionary.

    This helper eliminates code duplication between session_tables_schema and
    catalog_tables_schema by providing a single place to format metadata results.

    A "meta table" in Deephaven is a table that describes another table's structure.
    Each row in a meta table represents one column from the original table, with
    properties like Name, DataType, IsPartitioning, ComponentType, etc.

    Args:
        arrow_meta_table (pyarrow.Table): The PyArrow meta table containing column metadata.
            Each row describes one column of the original table.
        table_name (str): Name of the table being described.
        namespace (str | None): Optional namespace for catalog tables. If provided (not None),
            it will be included in the result. Session tables should pass None since they
            don't have namespaces. Defaults to None.

    Returns:
        dict: Formatted result with success status and metadata fields. The structure is:
            {
                "success": True,  # Always True for successful formatting
                "table": str,  # Name of the table
                "format": "json-row",  # Data format (always "json-row" = list of dicts)
                "data": list[dict],  # Full metadata rows with all column properties
                "meta_columns": list[dict],  # Schema of the meta table itself (describes "data" structure)
                "row_count": int,  # Number of rows in meta table = number of columns in original table
                "namespace": str  # Only present if namespace parameter was not None (catalog tables)
            }

            Note: The "namespace" field is conditionally included only when the namespace
            parameter is not None. This keeps session table results clean (no namespace field)
            while catalog table results include the namespace for context.

    Example:
        >>> # For a table with 2 columns (Date and Price)
        >>> result = format_meta_table_result(meta_table, "daily_prices", "market_data")
        >>> result
        {
            "success": True,
            "table": "daily_prices",
            "namespace": "market_data",
            "format": "json-row",
            "data": [
                {"Name": "Date", "DataType": "LocalDate", "IsPartitioning": False},
                {"Name": "Price", "DataType": "double", "IsPartitioning": False}
            ],
            "meta_columns": [
                {"name": "Name", "type": "string"},
                {"name": "DataType", "type": "string"},
                {"name": "IsPartitioning", "type": "bool"}
            ],
            "row_count": 2
        }
    """
    # Convert to full metadata using to_pylist() for complete information
    # to_pylist() returns native Python types (dict, list, str, int, bool, None)
    # which are JSON-serializable for MCP protocol
    meta_data = arrow_meta_table.to_pylist()

    # Extract schema of the meta table itself
    meta_schema = [
        {"name": field.name, "type": str(field.type)}
        for field in arrow_meta_table.schema
    ]

    result = {
        "success": True,
        "table": table_name,
        "format": "json-row",  # Explicit format for AI agent clarity
        "data": meta_data,
        "meta_columns": meta_schema,
        "row_count": len(arrow_meta_table),
    }

    # Only include namespace for catalog tables (where it's meaningful)
    if namespace is not None:
        result["namespace"] = namespace

    return result


# Response size estimation constants
# Conservative estimate: ~20 chars + 8 bytes numeric + JSON overhead + safety margin
ESTIMATED_BYTES_PER_CELL = 50
"""Estimated bytes per table cell for response size calculation.

This rough estimate is used to prevent memory issues when retrieving large tables.
The estimation assumes:
- Average string length: ~20 characters (20 bytes)
- Numeric values: ~8 bytes (int64/double)
- Null values and metadata: ~5 bytes overhead
- JSON formatting overhead: ~15-20 bytes per cell
- Safety margin: 50 bytes total per cell

This conservative estimate helps catch potentially problematic responses before
expensive formatting operations. Can be tuned based on actual data patterns.

Typically multiplied by ``rows * cols`` and passed to :func:`check_response_size`
before invoking :func:`build_table_data_response` to format the data.
"""


def build_table_data_response(
    arrow_table: pyarrow.Table,
    is_complete: bool,
    format: str,
    table_name: str | None = None,
    namespace: str | None = None,
) -> dict:
    """Build a standardized table data response with schema, formatting, and metadata.

    This helper consolidates the common pattern of:
    1. Extracting schema from Arrow table
    2. Formatting data with format_table_data
    3. Building response dict with standard fields

    Used by both session table tools and catalog table tools to ensure consistent
    response structure across all table data retrieval operations.

    Args:
        arrow_table (pyarrow.Table): The Arrow table containing the data.
        is_complete (bool): Whether the entire table was retrieved (False if truncated by max_rows).
        format (str): Desired output format (may be optimization strategy or specific format like "csv", "json-row", etc.).
        table_name (str | None): Optional table name to include in response. Recommended for clarity.
        namespace (str | None): Optional namespace to include in response. Use for catalog tables only.

    Returns:
        dict: Standardized response with success=True and fields:
            - success (bool): Always True for this helper (errors handled by callers).
            - format (str): Actual format used (resolved from optimization strategies to specific format).
            - schema (list[dict]): Column definitions with name and type.
            - row_count (int): Number of rows in the response.
            - is_complete (bool): Whether entire table was retrieved.
            - data (varies): Formatted table data (type depends on format).
            - table_name (str, optional): Included if table_name parameter provided.
            - namespace (str, optional): Included if namespace parameter provided (catalog tables).
    """
    # Extract schema
    schema = [
        {"name": field.name, "type": str(field.type)} for field in arrow_table.schema
    ]

    # Format data
    actual_format, formatted_data = format_table_data(arrow_table, format_type=format)

    # Build response
    response = {
        "success": True,
        "format": actual_format,
        "schema": schema,
        "row_count": len(arrow_table),
        "is_complete": is_complete,
        "data": formatted_data,
    }

    # Add optional fields
    if namespace is not None:
        response["namespace"] = namespace
    if table_name is not None:
        response["table_name"] = table_name

    return response


# =============================================================================
# Credential redaction utilities
# =============================================================================

_SENSITIVE_JSON_KEYS: frozenset[str] = frozenset(
    {"password", "passwd", "token", "secret", "api_key", "apikey", "api_secret"}
)
"""JSON object keys whose values are redacted in type_specific_fields_json / type_specific_state_json output."""


def _redact_recursive(obj: object) -> object:
    """Recursively redact values of sensitive keys in a parsed JSON structure.

    Walks dicts, lists, and nested combinations thereof, replacing the value of
    any key whose lowercase form appears in _SENSITIVE_JSON_KEYS with "[REDACTED]".
    Scalar values (str, int, float, bool, None) are returned unchanged.
    """
    if isinstance(obj, dict):
        return {
            k: (REDACTED if k.lower() in _SENSITIVE_JSON_KEYS else _redact_recursive(v))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_redact_recursive(item) for item in obj]
    return obj


def redact_json_sensitive_fields(json_str: str | None) -> str | None:
    """Parse a JSON string and redact values whose keys match known-sensitive names.

    Returns None for empty/None input. Returns "[UNPARSEABLE]" if the string cannot
    be parsed as JSON (with a warning log). Otherwise returns a re-serialized JSON
    string with sensitive values replaced by "[REDACTED]".
    """
    if not json_str:
        return None
    try:
        parsed = json.loads(json_str)
    except (json.JSONDecodeError, ValueError):
        _LOGGER.warning(
            "type_specific JSON field is not valid JSON; content suppressed"
        )
        return "[UNPARSEABLE]"
    return json.dumps(_redact_recursive(parsed))
