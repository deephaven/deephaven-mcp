"""
Table Operations MCP Tools - Query and Export Table Data.

Provides MCP tools for working with tables in Deephaven sessions:
- session_tables_list: List all available tables in a session
- session_tables_schema: Get schema information for tables
- session_table_data: Export and retrieve table data in various formats

These tools work with both Community and Enterprise sessions.
"""

import logging

import pyarrow
from mcp.server.fastmcp import Context

from deephaven_mcp import queries
from deephaven_mcp.formatters import format_table_data
from deephaven_mcp.mcp_systems_server._tools.mcp_server import (
    mcp_server,
)
from deephaven_mcp.mcp_systems_server._tools.shared import (
    _check_response_size,
    _format_meta_table_result,
    _get_session_from_context,
)

_LOGGER = logging.getLogger(__name__)


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


def _build_table_data_response(
    arrow_table: pyarrow.Table,
    is_complete: bool,
    format: str,
    table_name: str | None = None,
    namespace: str | None = None,
) -> dict:
    """
    Build a standardized table data response with schema, formatting, and metadata.

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


@mcp_server.tool()
async def session_tables_schema(
    context: Context, session_id: str, table_names: list[str] | None = None
) -> dict:
    """
    MCP Tool: Retrieve table schemas as TABULAR METADATA from a Deephaven session.

    **Returns**: Schema information formatted as TABULAR DATA where each row represents a column
    in the source table. This tabular metadata should be displayed as a table to users for easy
    comprehension of table structure.

    Returns complete metadata information for the specified tables including column names, data types,
    and all metadata properties. If no table_names are provided, returns schemas for all available
    tables in the session. This provides the FULL schema with all metadata properties, not just
    simplified name/type pairs.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    Table Rendering:
    - **This tool returns TABULAR METADATA that MUST be displayed as a table to users**
    - Each row in the result represents one column from the source table
    - The table shows column properties: Name, DataType, IsPartitioning, ComponentType, etc.
    - Present schema data in tabular format (table or grid) for easy comprehension
    - Do NOT present schema data as plain text or unstructured lists

    AI Agent Usage:
    - Call with no table_names to discover all available tables and their full schemas
    - Call with specific table_names list when you know which tables you need
    - Always check the 'success' field in each schema result before using the schema data
    - The 'data' field contains full metadata with properties like Name, DataType, IsPartitioning, etc.
    - Use the returned metadata to construct valid queries and understand table structure
    - Essential before calling session_table_data or session_script_run to understand table structure
    - Individual table failures don't stop processing of other tables
    - This returns FULL metadata, not simplified schema - use for complete table understanding

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
                - 'data' (list[dict], optional): Full metadata rows if successful. Each dict contains:
                    - 'Name' (str): Column name
                    - 'DataType' (str): Deephaven data type
                    - 'IsPartitioning' (bool, optional): Whether column is used for partitioning
                    - 'ComponentType' (str, optional): Component type for array/vector columns
                    - Additional metadata properties depending on column type
                - 'meta_columns' (list[dict], optional): Schema of the metadata table itself
                - 'row_count' (int, optional): Number of columns in the table
                - 'error' (str, optional): Error message if this table's schema retrieval failed
                - 'isError' (bool, optional): Present and True if this table had an error
            - 'count' (int, optional): Total number of table results in schemas list if operation completed.
            - 'error' (str, optional): Error message if the entire operation failed.
            - 'isError' (bool, optional): Present and True if this is an error response.

    Example Successful Response (mixed results):
        {
            'success': True,
            'schemas': [
                {
                    'success': True,
                    'table': 'MyTable',
                    'data': [{'Name': 'Col1', 'DataType': 'int', ...}, ...],
                    'row_count': 3
                },
                {'success': False, 'table': 'MissingTable', 'error': 'Table not found', 'isError': True}
            ]
        }

    Example Error Response (total failure):
        {'success': False, 'error': 'Failed to connect to session: ...', 'isError': True}

    Example Usage:
        # Get full schemas for all tables in the session
        Tool: session_tables_schema
        Parameters: {
            "session_id": "community:localhost:10000"
        }

        # Get full schemas for specific tables
        Tool: session_tables_schema
        Parameters: {
            "session_id": "community:localhost:10000",
            "table_names": ["trades", "quotes", "orders"]
        }

        # Get full schema for a single table
        Tool: session_tables_schema
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "table_names": ["market_data"]
        }
    """
    _LOGGER.info(
        f"[mcp_systems_server:session_tables_schema] Invoked: session_id={session_id!r}, table_names={table_names!r}"
    )
    schemas = []
    try:
        # Use helper to get session from context
        session = await _get_session_from_context(
            "session_tables_schema", context, session_id
        )

        if table_names is not None:
            selected_table_names = table_names
            _LOGGER.info(
                f"[mcp_systems_server:session_tables_schema] Fetching schemas for specified tables: {selected_table_names!r}"
            )
        else:
            _LOGGER.debug(
                f"[mcp_systems_server:session_tables_schema] Discovering available tables in session '{session_id}'"
            )
            selected_table_names = await session.tables()
            _LOGGER.info(
                f"[mcp_systems_server:session_tables_schema] Fetching schemas for all tables in session: {selected_table_names!r}"
            )

        for table_name in selected_table_names:
            _LOGGER.debug(
                f"[mcp_systems_server:session_tables_schema] Processing table '{table_name}' in session '{session_id}'"
            )
            try:
                meta_arrow_table = await queries.get_session_meta_table(
                    session, table_name
                )

                # Use helper to format result (no namespace for session tables)
                result = _format_meta_table_result(
                    meta_arrow_table, table_name, namespace=None
                )
                schemas.append(result)

                _LOGGER.info(
                    f"[mcp_systems_server:session_tables_schema] Success: Retrieved full schema for table '{table_name}' ({result['row_count']} columns)"
                )
            except Exception as table_exc:
                _LOGGER.error(
                    f"[mcp_systems_server:session_tables_schema] Failed to get schema for table '{table_name}': {table_exc!r}",
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
            f"[mcp_systems_server:session_tables_schema] Returning {len(schemas)} table results"
        )
        return {"success": True, "schemas": schemas, "count": len(schemas)}
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_tables_schema] Failed for session: '{session_id}', error: {e!r}",
            exc_info=True,
        )
        return {"success": False, "error": str(e), "isError": True}


@mcp_server.tool()
async def session_tables_list(context: Context, session_id: str) -> dict:
    """
    MCP Tool: Retrieve the names of all tables in a Deephaven session.

    Returns a simple list of table names without schemas or metadata. This is a lightweight
    alternative to table_schemas when you only need to discover what tables exist in a session.
    Much faster than table_schemas since it doesn't fetch schema information for each table.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Use this for quick table discovery when you don't need schema details
    - Much faster than table_schemas for large sessions with many tables
    - Follow up with table_schemas or get_table_meta for specific tables you're interested in
    - Works with both Community and Enterprise sessions
    - Check 'count' field to see how many tables exist
    - Always check 'success' field before accessing 'table_names'

    Args:
        context (Context): The MCP context object, required by MCP protocol but not actively used.
        session_id (str): ID of the Deephaven session to query. Must match an existing active session.

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): Always present. True if table names were retrieved successfully, False on any error.
            - 'session_id' (str, optional): The session ID if successful. Useful for confirming which session was queried.
            - 'table_names' (list[str], optional): List of table names if successful. Empty list if session has no tables.
            - 'count' (int, optional): Number of tables found if successful. Convenient for quick checks.
            - 'error' (str, optional): Human-readable error message if retrieval failed. Omitted on success.
            - 'isError' (bool, optional): Present and True only when success=False. Explicit error flag for frameworks.

    Error Scenarios:
        - Invalid session_id: Returns error if session doesn't exist or is not accessible
        - Session connection issues: Returns error if unable to communicate with Deephaven server
        - Session not available: Returns error if session is closed or unavailable

    Example Successful Response:
        {
            'success': True,
            'session_id': 'community:localhost:10000',
            'table_names': ['trades', 'quotes', 'orders'],
            'count': 3
        }

    Example Error Response:
        {
            'success': False,
            'error': 'Session not found: community:localhost:10000',
            'isError': True
        }

    Performance Notes:
        - Very fast operation, typically completes in milliseconds
        - No network data transfer (just metadata query)
        - Safe to call frequently for session monitoring
        - Scales well even with hundreds of tables
    """
    _LOGGER.info(
        f"[mcp_systems_server:session_tables_list] Invoked: session_id={session_id!r}"
    )

    try:
        # Use helper to get session from context
        session = await _get_session_from_context(
            "session_tables_list", context, session_id
        )

        _LOGGER.debug(
            f"[mcp_systems_server:session_tables_list] Retrieving table names from session '{session_id}'"
        )
        table_names = await session.tables()

        _LOGGER.info(
            f"[mcp_systems_server:session_tables_list] Success: Retrieved {len(table_names)} table(s) from session '{session_id}'"
        )

        return {
            "success": True,
            "session_id": session_id,
            "table_names": table_names,
            "count": len(table_names),
        }

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_tables_list] Failed for session: '{session_id}', error: {e!r}",
            exc_info=True,
        )
        return {"success": False, "error": str(e), "isError": True}


@mcp_server.tool()
async def session_table_data(
    context: Context,
    session_id: str,
    table_name: str,
    max_rows: int | None = 1000,
    head: bool = True,
    format: str = "optimize-rendering",
) -> dict:
    r"""
    MCP Tool: Retrieve TABULAR DATA from a specified Deephaven session table.

    **Returns**: Structured table data formatted for optimal AI agent comprehension and rendering.
    The response contains TABULAR DATA that should be displayed as a table to users.

    This tool queries the specified Deephaven session for table data and returns it in the requested format
    with optional row limiting. Supports multiple output formats optimized for AI agent consumption.

    **Format Accuracy for AI Agents** (based on empirical research):
    - markdown-kv: 61% accuracy (highest comprehension, more tokens)
    - markdown-table: 55% accuracy (good balance)
    - json-row/json-column: 50% accuracy
    - yaml: 50% accuracy
    - xml: 45% accuracy
    - csv: 44% accuracy (lowest comprehension, fewest tokens)

    Includes safety limits (50MB max response size) to prevent memory issues.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    Args:
        context (Context): The MCP context object, required by MCP protocol but not actively used.
        session_id (str): ID of the Deephaven session to query. Must match an existing active session.
        table_name (str): Name of the table to retrieve data from. Must exist in the specified session.
        max_rows (int | None, optional): Maximum number of rows to retrieve. Defaults to 1000 for safety.
                                        Set to None to retrieve entire table (use with caution for large tables).
        head (bool, optional): Direction of row retrieval. If True (default), retrieve from beginning.
                              If False, retrieve from end (most recent rows for time-series data).
        format (str, optional): Output format selection. Defaults to "optimize-rendering" for best table display.
                               Options:
                               - "optimize-rendering": (DEFAULT) Always use markdown-table (best for AI agent table display)
                               - "optimize-accuracy": Always use markdown-kv (best comprehension, more tokens)
                               - "optimize-cost": Always use csv (fewer tokens, may be harder to parse)
                               - "optimize-speed": Always use json-column (fastest conversion)
                               - "markdown-table": String with pipe-delimited table (| col1 | col2 |\n| --- | --- |\n| val1 | val2 |)
                               - "markdown-kv": String with record headers and key-value pairs (## Record 1\ncol1: val1\ncol2: val2)
                               - "json-row": List of dicts, one per row: [{col1: val1, col2: val2}, ...]
                               - "json-column": Dict with column names as keys, value arrays: {col1: [val1, val2], col2: [val3, val4]}
                               - "csv": String with comma-separated values, includes header row
                               - "yaml": String with YAML-formatted records list
                               - "xml": String with XML records structure

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): Always present. True if table data was retrieved successfully, False on any error.
            - 'table_name' (str, optional): Name of the retrieved table if successful.
            - 'format' (str, optional): Actual format used for the data if successful. May differ from request when using optimization strategies.
            - 'schema' (list[dict], optional): Array of column definitions if successful. Each dict contains:
                                              {'name': str, 'type': str} describing column name and PyArrow data type
                                              (e.g., 'int64', 'string', 'double', 'timestamp[ns]').
            - 'row_count' (int, optional): Number of rows in the returned data if successful. May be less than max_rows.
            - 'is_complete' (bool, optional): True if entire table was retrieved if successful. False if truncated by max_rows.
            - 'data' (list | dict | str, optional): The actual table data if successful. Type depends on format.
            - 'error' (str, optional): Human-readable error message if retrieval failed. Omitted on success.
            - 'isError' (bool, optional): Present and True only when success=False. Explicit error flag for frameworks.

    Error Scenarios:
        - Invalid session_id: Returns error if session doesn't exist or is not accessible
        - Invalid table_name: Returns error if table doesn't exist in the session
        - Invalid format: Returns error if format is not one of the supported options listed above
        - Response too large: Returns error if estimated response would exceed 50MB limit
        - Session connection issues: Returns error if unable to communicate with Deephaven server
        - Query execution errors: Returns error if table query fails (permissions, syntax, etc.)

    Table Rendering:
        - **This tool returns TABULAR DATA that should be displayed as a table to users**
        - The 'data' field contains formatted table data ready for display
        - Default format (markdown-table) renders well as tables in AI interfaces
        - Always present the returned data in tabular format (table, grid, or structured rows)

    Performance Considerations:
        - Large tables: Use csv format or limit max_rows to avoid memory issues
        - Column analysis: Use json-column format for efficient column-wise operations
        - Row processing: Use json-row format for record-by-record iteration
        - Response size limit: 50MB maximum to prevent memory issues

    AI Agent Usage:
        - Always check 'success' field before accessing data fields
        - Use 'is_complete' to determine if more data exists beyond max_rows limit
        - Parse 'schema' array to understand column types before processing 'data'
        - Use head=True (default) to get rows from table start, head=False to get from table end
        - Start with small max_rows values for large tables to avoid memory issues
        - Use 'optimize-rendering' (default) for best table display in AI interfaces
        - Use 'optimize-accuracy' for highest comprehension (markdown-kv format, more tokens)
        - Use 'optimize-cost' for fewest tokens (csv format, may be harder to parse)
        - Check 'format' field in response to know actual format used

    Example Usage:
        # Get first 1000 rows with default format
        Tool: session_table_data
        Parameters: {
            "session_id": "community:localhost:10000",
            "table_name": "my_table"
        }

        # Get last 500 rows (most recent for time-series)
        Tool: session_table_data
        Parameters: {
            "session_id": "community:localhost:10000",
            "table_name": "trades",
            "max_rows": 500,
            "head": false
        }

        # Get data in CSV format for efficient parsing
        Tool: session_table_data
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "table_name": "market_data",
            "max_rows": 10000,
            "format": "csv"
        }

        # Get data optimized for AI comprehension
        Tool: session_table_data
        Parameters: {
            "session_id": "community:localhost:10000",
            "table_name": "customer_records",
            "max_rows": 100,
            "format": "optimize-accuracy"
        }

        # Get entire small table in JSON row format
        Tool: session_table_data
        Parameters: {
            "session_id": "community:localhost:10000",
            "table_name": "config_settings",
            "max_rows": null,
            "format": "json-row"
        }

        # Get data in markdown table format
        Tool: session_table_data
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "table_name": "summary_stats",
            "max_rows": 50,
            "format": "markdown-table"
        }
    """
    _LOGGER.info(
        f"[mcp_systems_server:session_table_data] Invoked: session_id={session_id!r}, "
        f"table_name={table_name!r}, max_rows={max_rows}, head={head}, format={format!r}"
    )

    result: dict[str, object] = {"success": False}

    try:
        # Use helper to get session from context
        session = await _get_session_from_context(
            "session_table_data", context, session_id
        )

        # Get table data using queries module
        _LOGGER.debug(
            f"[mcp_systems_server:session_table_data] Retrieving table data for '{table_name}'"
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

        # Build response using helper
        _LOGGER.debug(
            f"[mcp_systems_server:session_table_data] Formatting data with format='{format}'"
        )
        response = _build_table_data_response(
            arrow_table, is_complete, format, table_name=table_name
        )
        result.update(response)

        _LOGGER.info(
            f"[mcp_systems_server:session_table_data] Successfully retrieved {row_count} rows "
            f"from '{table_name}' in '{response['format']}' format"
        )

    except ValueError as e:
        # Format validation error from formatters package
        _LOGGER.error(
            f"[mcp_systems_server:session_table_data] Invalid format parameter: {e!r}"
        )
        result["error"] = (
            f"Invalid format parameter for table '{table_name}': {type(e).__name__}: {e}"
        )
        result["isError"] = True

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_table_data] Failed for session '{session_id}', "
            f"table '{table_name}': {e!r}",
            exc_info=True,
        )
        result["error"] = (
            f"Failed to get data from table '{table_name}' in session '{session_id}': {type(e).__name__}: {e}"
        )
        result["isError"] = True

    return result
