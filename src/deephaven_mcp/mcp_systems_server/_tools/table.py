"""Table Operations MCP Tools - Query and Export Table Data.

Provides MCP tools for working with tables in Deephaven sessions:
- session_tables_list: List all available tables in a session
- session_table_schema: Get schema information for one table
- session_table_data: Export and retrieve table data in various formats

These tools work with both Community and Enterprise sessions.
"""

import logging

from mcp.server.fastmcp import Context, FastMCP

from deephaven_mcp import queries
from deephaven_mcp._exception_utils import exception_summary
from deephaven_mcp.formatters import TableFormat
from deephaven_mcp.mcp_systems_server._tools.shared import (
    build_table_data_response,
    check_response_size,
    error_response,
    format_schema_result,
    get_response_limits,
    get_session_from_context,
)

_LOGGER = logging.getLogger(__name__)


async def session_table_schema(context: Context, id: str, table_name: str) -> dict:
    """MCP Tool: Retrieve the schema of one table in a Deephaven session.

    Returns the column definitions for a single named table. Deliberately
    single-table: discover table names with session_tables_list first, then
    fetch schemas one table per call (calls can run in parallel).

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Use session_tables_list first to discover table names, then call this per table
    - Essential before session_table_data or session_script_run to understand table structure
    - 'type' values are Deephaven type names (e.g. "java.lang.String", "int"), not
      PyArrow names - session_table_data's schema field uses PyArrow names instead
    - 'column_type' is omitted from ordinary columns; its absence means a
      Normal column

    Args:
        context (Context): The MCP context object.
        id (str): Fully qualified id of the session to query ('type:system:name',
            as returned by sessions_list).
        table_name (str): Name of the table whose schema to retrieve.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if the schema was retrieved, False on error.
            - 'id' (str, optional): The session id echoed back if successful.
            - 'table_name' (str, optional): The table name if successful.
            - 'schema' (list[dict], optional): One entry per column if successful.
              Each contains 'name' (str) and 'type' (str, Deephaven type name),
              plus one sparse key: 'column_type' (str, e.g. 'Partitioning' or
              'Grouping'; omitted for Normal columns).
            - 'column_count' (int, optional): Number of columns if successful.
            - 'error' (str, optional): Error message if retrieval failed.
            - 'isError' (bool, optional): Present and True if this is an error response.

    Example Successful Response:
        {
            'success': True,
            'id': 'community:community:main-worker',
            'table_name': 'trades',
            'schema': [
                {'name': 'Date', 'type': 'java.lang.String', 'column_type': 'Partitioning'},
                {'name': 'Prices', 'type': 'double[]'},
                {'name': 'Symbol', 'type': 'java.lang.String'}
            ],
            'column_count': 3
        }

    Example Error Response:
        {'success': False, 'error': "Failed to get schema for table 'missing' ...", 'isError': True}
    """
    _LOGGER.info(
        f"[mcp_systems_server:session_table_schema] Invoked: id={id!r}, "
        f"table_name={table_name!r}"
    )
    try:
        session = await get_session_from_context("session_table_schema", context, id)

        meta_arrow_table = await queries.get_session_meta_table(session, table_name)
        result = format_schema_result(meta_arrow_table, id, table_name, namespace=None)

        _LOGGER.info(
            f"[mcp_systems_server:session_table_schema] Success: Retrieved schema for "
            f"table '{table_name}' ({result['column_count']} columns)"
        )
        return result
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_table_schema] Failed for session '{id}', "
            f"table '{table_name}': {e!r}",
            exc_info=True,
        )
        return error_response(
            f"Failed to get schema for table '{table_name}' in session '{id}': {exception_summary(e)}"
        )


async def session_tables_list(context: Context, id: str) -> dict:
    """MCP Tool: Retrieve the names of all tables in a Deephaven session.

    Returns a simple list of table names without schemas or metadata. This is a
    lightweight discovery operation that doesn't fetch schema information.

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
    - Follow up with session_table_schema for each table you're interested in
    - Works with both Community and Enterprise sessions
    - Check 'count' field to see how many tables exist
    - Always check 'success' field before accessing 'table_names'

    Args:
        context (Context): The MCP context object used to access the session registry.
        id (str): Fully qualified id of the session to query ('type:system:name',
            as returned by sessions_list). Must match an existing active session.

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): Always present. True if table names were retrieved successfully, False on any error.
            - 'id' (str, optional): The session ID if successful. Useful for confirming which session was queried.
            - 'table_names' (list[str], optional): List of table names if successful. Empty list if session has no tables.
            - 'count' (int, optional): Number of tables found if successful. Convenient for quick checks.
            - 'error' (str, optional): Human-readable error message if retrieval failed. Omitted on success.
            - 'isError' (bool, optional): Present and True only when success=False. Explicit error flag for frameworks.

    Error Scenarios:
        - Invalid id: Returns error if session doesn't exist or is not accessible
        - Session connection issues: Returns error if unable to communicate with Deephaven server
        - Session not available: Returns error if session is closed or unavailable

    Example Successful Response:
        {
            'success': True,
            'id': 'community:community:local',
            'table_names': ['trades', 'quotes', 'orders'],
            'count': 3
        }

    Example Error Response:
        {
            'success': False,
            'error': 'Session not found: community:community:local',
            'isError': True
        }

    Performance Notes:
        - Very fast operation, typically completes in milliseconds
        - No network data transfer (just metadata query)
        - Safe to call frequently for session monitoring
        - Scales well even with hundreds of tables
    """
    _LOGGER.info(f"[mcp_systems_server:session_tables_list] Invoked: id={id!r}")

    try:
        # Use helper to get session from context
        session = await get_session_from_context("session_tables_list", context, id)

        _LOGGER.debug(
            f"[mcp_systems_server:session_tables_list] Retrieving table names from session '{id}'"
        )
        table_names = await session.tables()

        _LOGGER.info(
            f"[mcp_systems_server:session_tables_list] Success: Retrieved {len(table_names)} table(s) from session '{id}'"
        )

        return {
            "success": True,
            "id": id,
            "table_names": table_names,
            "count": len(table_names),
        }

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_tables_list] Failed for session: '{id}', error: {e!r}",
            exc_info=True,
        )
        return error_response(
            f"Failed to list tables for session '{id}': {exception_summary(e)}"
        )


async def session_table_data(
    context: Context,
    id: str,
    table_name: str,
    max_rows: int | None = 1000,
    head: bool = True,
    format: TableFormat = "optimize-rendering",
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
        context (Context): The MCP context object used to access the session registry.
        id (str): Fully qualified id of the session to query ('type:system:name',
            as returned by sessions_list). Must match an existing active session.
        table_name (str): Name of the table to retrieve data from. Must exist in the specified session.
        max_rows (int | None, optional): Maximum number of rows to retrieve. Defaults to 1000 for safety.
                                        Set to None to retrieve entire table (use with caution for large tables).
        head (bool, optional): Direction of row retrieval. If True (default), retrieve from beginning.
                              If False, retrieve from end (most recent rows for time-series data).
        format (TableFormat, optional): Output format selection. Defaults to "optimize-rendering" for best table display.
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
            - 'id' (str, optional): The session id echoed back if successful.
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
        - Invalid id: Returns error if session doesn't exist or is not accessible
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
            "id": "community:community:local",
            "table_name": "my_table"
        }

        # Get last 500 rows (most recent for time-series)
        Tool: session_table_data
        Parameters: {
            "id": "community:community:local",
            "table_name": "trades",
            "max_rows": 500,
            "head": false
        }

        # Get data in CSV format for efficient parsing
        Tool: session_table_data
        Parameters: {
            "id": "enterprise:prod:analytics",
            "table_name": "market_data",
            "max_rows": 10000,
            "format": "csv"
        }

        # Get data optimized for AI comprehension
        Tool: session_table_data
        Parameters: {
            "id": "community:community:local",
            "table_name": "customer_records",
            "max_rows": 100,
            "format": "optimize-accuracy"
        }

        # Get entire small table in JSON row format
        Tool: session_table_data
        Parameters: {
            "id": "community:community:local",
            "table_name": "config_settings",
            "max_rows": null,
            "format": "json-row"
        }

        # Get data in markdown table format
        Tool: session_table_data
        Parameters: {
            "id": "enterprise:prod:analytics",
            "table_name": "summary_stats",
            "max_rows": 50,
            "format": "markdown-table"
        }
    """
    _LOGGER.info(
        f"[mcp_systems_server:session_table_data] Invoked: id={id!r}, "
        f"table_name={table_name!r}, max_rows={max_rows}, head={head}, format={format!r}"
    )

    result: dict[str, object] = {"success": False}

    try:
        # Use helper to get session from context
        session = await get_session_from_context("session_table_data", context, id)

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
        limits = get_response_limits(context, id)
        estimated_size = row_count * col_count * limits.estimated_bytes_per_cell
        size_error = check_response_size(table_name, estimated_size, limits)

        if size_error:
            return size_error

        # Build response using helper
        _LOGGER.debug(
            f"[mcp_systems_server:session_table_data] Formatting data with format='{format}'"
        )
        response = build_table_data_response(
            arrow_table, is_complete, format, id, table_name=table_name
        )
        result.update(response)

        _LOGGER.info(
            f"[mcp_systems_server:session_table_data] Successfully retrieved {row_count} rows "
            f"from '{table_name}' in '{response['format']}' format"
        )

    except ValueError as e:
        # Format validation error from formatters package
        _LOGGER.error(
            f"[mcp_systems_server:session_table_data] Invalid format parameter '{format}' for table '{table_name}' in session '{id}': {e!r}"
        )
        result["error"] = (
            f"Invalid format parameter '{format}' for table '{table_name}' in session '{id}': {exception_summary(e)}"
        )
        result["isError"] = True

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_table_data] Failed for session '{id}', "
            f"table '{table_name}': {e!r}",
            exc_info=True,
        )
        result["error"] = (
            f"Failed to get data from table '{table_name}' in session '{id}': {exception_summary(e)}"
        )
        result["isError"] = True

    return result


def register_tools(server: FastMCP) -> None:
    """Register all table operation tools with the given FastMCP server.

    These tools are shared between the DHE and DHC servers.

    Args:
        server (FastMCP): The server to register tools with.
    """
    server.tool()(session_table_schema)
    server.tool()(session_tables_list)
    server.tool()(session_table_data)
