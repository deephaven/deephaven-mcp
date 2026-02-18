"""
Shared Utilities - Internal Helper Functions.

Provides internal helper functions used across multiple MCP tool modules:
- Response size checking and validation
- Common error handling patterns
- Shared data formatting utilities

This module contains private helper functions not exposed as MCP tools.
"""

import logging

import pyarrow
from mcp.server.fastmcp import Context

from deephaven_mcp.client import BaseSession, CorePlusSession
from deephaven_mcp.config import ConfigManager, get_config_section
from deephaven_mcp.resource_manager import (
    CombinedSessionRegistry,
    InitializationPhase,
)

_LOGGER = logging.getLogger(__name__)


def _format_initialization_status(
    phase: InitializationPhase,
    init_errors: dict[str, str],
) -> dict[str, object] | None:
    """Format initialization phase and errors into a response-ready dict.

    Pure formatting function — does not query any registry.  Callers are
    responsible for obtaining *phase* and *init_errors* from the same
    atomic snapshot (e.g. via ``get_all()`` or
    ``initialization_status()``).

    Returns ``None`` when there is nothing to report (completed without
    errors), so callers can simply do::

        init_info = _format_initialization_status(phase, errors)
        if init_info:
            response["initialization"] = init_info

    Args:
        phase: The current initialization phase.
        init_errors: Dict mapping factory names to error descriptions.

    Returns:
        A dict with ``status`` (str, always) and ``errors``
        (dict[str, str], only when present), or ``None`` if initialization
        completed cleanly.
    """
    init_info: dict[str, object] = {}
    if phase not in (InitializationPhase.SIMPLE, InitializationPhase.COMPLETED):
        init_info["status"] = (
            "Enterprise session discovery is still in progress. "
            "Some sessions or systems may not yet be visible."
        )
    elif init_errors:
        init_info["status"] = (
            "Some enterprise systems had connection issues during discovery."
        )
    if init_errors:
        init_info["errors"] = init_errors
    return init_info or None


async def _get_session_from_context(
    function_name: str, context: Context, session_id: str
) -> BaseSession:
    """
    Get an active session from the MCP context.

    This helper eliminates duplication of the common pattern for accessing
    sessions from the MCP context. It handles the standard flow of:
    1. Extracting session_registry from context
    2. Getting the session_manager for the session_id
    3. Establishing the session connection

    Args:
        function_name (str): Name of calling function for logging purposes
        context (Context): The MCP context object containing lifespan context
        session_id (str): ID of the session to retrieve

    Returns:
        BaseSession: The active session connection

    Raises:
        KeyError: If session_id not found in registry
        Exception: If session cannot be established or context is invalid
    """
    _LOGGER.debug(
        f"[mcp_systems_server:{function_name}] Accessing session registry from context"
    )
    session_registry: CombinedSessionRegistry = (
        context.request_context.lifespan_context["session_registry"]
    )

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


async def _get_enterprise_session(
    function_name: str, context: Context, session_id: str
) -> tuple[CorePlusSession | None, dict[str, object] | None]:
    """
    Get and validate an enterprise (Core+) session from context.

    This helper combines session retrieval and validation into a single clean operation,
    consolidating the common pattern of getting a session and verifying it's an enterprise
    (Core+) session. This eliminates code duplication across catalog-related tools.

    Args:
        function_name (str): Name of calling function for logging and error messages.
        context (Context): The MCP context object containing lifespan context with session_registry.
        session_id (str): ID of the session to retrieve (e.g., "enterprise:prod:analytics").

    Returns:
        tuple: A 2-tuple (session, error) where:
            - session (CorePlusSession | None): The validated enterprise session on success, None on failure.
            - error (dict | None): None on success, structured error dict on failure with keys:
                - 'success': False
                - 'error': str (human-readable error message)
                - 'isError': True

    Error Conditions:
        - Session not found in registry
        - Session is not a CorePlusSession (community session provided)
        - Any exception during session retrieval

    Example:
        >>> session, error = await _get_enterprise_session("catalog_tables_schema", context, "enterprise:prod:analytics")
        >>> if error:
        >>>     return error
        >>> session = cast(CorePlusSession, session)  # Type narrowing for mypy
    """
    try:
        # Get session from context
        session = await _get_session_from_context(function_name, context, session_id)

        # Validate it's an enterprise session
        if not isinstance(session, CorePlusSession):
            error_msg = (
                f"{function_name} only works with enterprise (Core+) sessions, "
                f"but session '{session_id}' is {type(session).__name__}"
            )
            _LOGGER.error(f"[mcp_systems_server:{function_name}] {error_msg}")
            return None, {"success": False, "error": error_msg, "isError": True}

        return session, None
    except Exception as e:
        error_msg = f"Failed to get session '{session_id}': {e}"
        _LOGGER.error(f"[mcp_systems_server:{function_name}] {error_msg}")
        return None, {"success": False, "error": error_msg, "isError": True}


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


def _format_meta_table_result(
    arrow_meta_table: pyarrow.Table,
    table_name: str,
    namespace: str | None = None,
) -> dict:
    """
    Format a PyArrow meta table into a standardized result dictionary.

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
        >>> result = _format_meta_table_result(meta_table, "daily_prices", "market_data")
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


async def _get_system_config(
    function_name: str, config_manager: ConfigManager, system_name: str
) -> tuple[dict, dict | None]:
    """Get enterprise system configuration and validate it exists.

    Retrieves the configuration for the specified enterprise system from the config manager.
    This is a common validation step used by enterprise session management functions.

    Args:
        function_name (str): Name of the calling function for logging purposes.
        config_manager (ConfigManager): ConfigManager instance to retrieve configuration.
        system_name (str): Name of the enterprise system to look up.

    Returns:
        tuple[dict, dict | None]: A tuple containing (system_config, error_dict):
            - system_config (dict): The enterprise system configuration if found, or empty dict {} if not found.
            - error_dict (dict | None): Error response with 'error' and 'isError' keys if system not found, or None on success.

            Success: ({"url": "...", "username": "...", ...}, None)
            Error: ({}, {"error": "Enterprise system 'X' not found...", "isError": True})

    Example:
        >>> config_mgr = ConfigManager()
        >>> system_config, error = await _get_system_config("session_enterprise_create", config_mgr, "prod")
        >>> if error:
        ...     return error  # System not found
        >>> # Use system_config for session creation
    """
    config = await config_manager.get_config()

    try:
        enterprise_systems_config = get_config_section(
            config, ["enterprise", "systems"]
        )
    except KeyError:
        enterprise_systems_config = {}

    if not enterprise_systems_config or system_name not in enterprise_systems_config:
        error_msg = f"Enterprise system '{system_name}' not found in configuration"
        _LOGGER.error(f"[mcp_systems_server:{function_name}] {error_msg}")
        return {}, {"error": error_msg, "isError": True}

    return enterprise_systems_config[system_name], None
