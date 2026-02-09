"""
Catalog MCP Tools - Enterprise Core+ Data Catalog Operations.

Provides MCP tools for querying Deephaven Enterprise (Core+) data catalogs:
- catalog_tables_list: List all tables across catalog namespaces
- catalog_namespaces_list: List available catalog namespaces
- catalog_tables_schema: Get schema information for catalog tables
- catalog_table_sample: Sample data from catalog tables

These tools require Deephaven Enterprise (Core+) and are not available in Community.
"""

import logging
from typing import cast

from mcp.server.fastmcp import Context

from deephaven_mcp import queries
from deephaven_mcp._exceptions import UnsupportedOperationError
from deephaven_mcp.client import CorePlusSession


from deephaven_mcp.formatters import format_table_data

from deephaven_mcp.mcp_systems_server._tools.mcp_server import (
    mcp_server,
)
from deephaven_mcp.mcp_systems_server._tools.shared import (
    _check_response_size,
    _format_meta_table_result,
    _get_enterprise_session,
    _get_session_from_context,
)
from deephaven_mcp.mcp_systems_server._tools.table import (
    ESTIMATED_BYTES_PER_CELL,
    _build_table_data_response,
)

_LOGGER = logging.getLogger(__name__)



async def _get_catalog_data(
    context: Context,
    session_id: str,
    *,
    distinct_namespaces: bool,
    max_rows: int | None,
    filters: list[str] | None,
    format: str,
    tool_name: str,
) -> dict:
    """
    Retrieve catalog data (tables or namespaces) from an enterprise session.

    Internal helper function that consolidates common catalog retrieval logic.

    Args:
        context (Context): The MCP context object.
        session_id (str): ID of the Deephaven enterprise session to query.
        distinct_namespaces (bool): If True, retrieve distinct namespaces; if False, retrieve full catalog.
        max_rows (int | None): Maximum number of rows to return. None for unlimited.
        filters (list[str] | None): Optional Deephaven query language filters to apply.
        format (str): Output format for data (e.g., "csv", "json-row", "markdown-table").
        tool_name (str): Name of the calling tool for logging.

    Returns:
        dict: Result dictionary with keys:
            - success (bool): True if operation succeeded, False otherwise
            - session_id (str): The session ID that was queried
            - format (str): The actual format used for data
            - row_count (int): Number of rows returned
            - is_complete (bool): True if all data was returned (not truncated)
            - columns (list[dict]): Schema information for returned data
            - data (str): Formatted catalog data
            - error (str, optional): Error message if success is False
            - isError (bool, optional): True if this is an error response
    """
    result: dict[str, object] = {"success": False}
    data_type = "namespaces" if distinct_namespaces else "catalog entries"

    try:
        # Use helper to get session from context
        session = await _get_session_from_context(tool_name, context, session_id)

        # Get catalog data using queries module (includes enterprise check and filtering)
        _LOGGER.debug(
            f"[mcp_systems_server:{tool_name}] Retrieving {data_type} with filters: {filters}"
        )
        arrow_table, is_complete = await queries.get_catalog_table(
            session,
            max_rows=max_rows,
            filters=filters,
            distinct_namespaces=distinct_namespaces,
        )

        row_count = len(arrow_table)
        _LOGGER.debug(
            f"[mcp_systems_server:{tool_name}] Retrieved {row_count} {data_type} (complete={is_complete})"
        )

        # Estimate response size for safety
        estimated_size = arrow_table.nbytes
        size_check_result = _check_response_size(tool_name, estimated_size)
        if size_check_result:
            return size_check_result

        # Format the data using the formatters package
        _LOGGER.debug(
            f"[mcp_systems_server:{tool_name}] Formatting data with format='{format}'"
        )
        actual_format, formatted_data = format_table_data(arrow_table, format)
        _LOGGER.debug(
            f"[mcp_systems_server:{tool_name}] Data formatted as '{actual_format}'"
        )

        # Extract schema information
        columns = [
            {"name": field.name, "type": str(field.type)}
            for field in arrow_table.schema
        ]

        result.update(
            {
                "success": True,
                "session_id": session_id,
                "format": actual_format,
                "row_count": row_count,
                "is_complete": is_complete,
                "columns": columns,
                "data": formatted_data,
            }
        )

        _LOGGER.info(
            f"[mcp_systems_server:{tool_name}] Successfully retrieved {row_count} {data_type} "
            f"in '{actual_format}' format (complete={is_complete})"
        )

    except UnsupportedOperationError as e:
        # Enterprise-only operation attempted on community session
        _LOGGER.error(
            f"[mcp_systems_server:{tool_name}] Session '{session_id}' is not an enterprise session: {e!r}"
        )
        result["error"] = (
            f"Session '{session_id}' does not support this operation: {type(e).__name__}: {e}"
        )
        result["isError"] = True

    except ValueError as e:
        # Format validation error from formatters package
        _LOGGER.error(
            f"[mcp_systems_server:{tool_name}] Invalid format parameter: {e!r}"
        )
        result["error"] = f"Invalid format parameter: {type(e).__name__}: {e}"
        result["isError"] = True

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:{tool_name}] Failed for session '{session_id}': {e!r}",
            exc_info=True,
        )
        result["error"] = (
            f"Catalog operation failed for session '{session_id}': {type(e).__name__}: {e}"
        )
        result["isError"] = True

    return result




@mcp_server.tool()
async def catalog_tables_list(
    context: Context,
    session_id: str,
    max_rows: int | None = 10000,
    filters: list[str] | None = None,
    format: str = "optimize-rendering",
) -> dict:
    """
    MCP Tool: Retrieve catalog entries as a TABULAR LIST from a Deephaven Enterprise (Core+) session.

    **Returns**: Catalog table entries formatted as TABULAR DATA for display. Each row represents
    a table available in the enterprise catalog/database. This tabular data should be displayed as a table
    to users for easy browsing of available data sources.

    The catalog (also called database) contains metadata about tables accessible via the `deephaven_enterprise.database`
    package (the `db` variable) in an enterprise session. This includes tables that can be accessed
    using methods like `db.live_table(namespace, table_name)` or `db.historical_table(namespace, table_name)`.
    The catalog includes table names, namespaces, schemas, and other descriptive information. This tool
    enables discovery of available tables and their properties. Only works with enterprise sessions.

    **Format Accuracy for AI Agents** (based on empirical research):
    - markdown-kv: 61% accuracy (highest comprehension, more tokens)
    - markdown-table: 55% accuracy (good balance)
    - json-row/json-column: 50% accuracy
    - yaml: 50% accuracy
    - xml: 45% accuracy
    - csv: 44% accuracy (lowest comprehension, fewest tokens)

    For more information, see:
    - https://deephaven.io
    - https://docs.deephaven.io/pycoreplus/latest/worker/code/deephaven_enterprise.database.html

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'ENTERPRISE' sessions run Deephaven Enterprise (also called 'Core+' or 'CorePlus')
    - This tool only works with enterprise sessions; community sessions do not have catalog tables
    - 'Catalog' and 'database' are interchangeable terms - the catalog is the database of available tables
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    Table Rendering:
    - **This tool returns TABULAR CATALOG DATA that MUST be displayed as a table to users**
    - Each row represents one table available in the enterprise catalog
    - Columns include: Namespace, TableName, and other catalog metadata
    - Present as a table for easy browsing and discovery of data sources
    - Do NOT present catalog data as plain text or unstructured lists

    AI Agent Usage:
    - Use this to discover what tables are available in the catalog/database via the `db` variable
    - The catalog is the database of available tables in an enterprise session
    - Tables in the catalog can be accessed using `db.live_table(namespace, table_name)` or `db.historical_table(namespace, table_name)`
    - Filter by namespace to find tables in specific data domains
    - Filter by table name patterns to locate specific tables
    - Check 'is_complete' to know if all catalog entries were returned
    - Combine with catalog_tables_schema to get full metadata for discovered tables
    - Essential first step before querying enterprise data sources
    - Use filters to narrow down large catalogs/databases efficiently

    Filter Syntax Reference:
    Filters use Deephaven query language with backticks (`) for string literals.
    Multiple filters are combined with AND logic.

    Common Filter Patterns:
        Exact Match:
            - Namespace exact: "Namespace = `market_data`"
            - Table name exact: "TableName = `daily_prices`"

        String Contains (case-sensitive):
            - Namespace contains: "Namespace.contains(`market`)"
            - Table name contains: "TableName.contains(`price`)"

        String Contains (case-insensitive):
            - Namespace: "Namespace.toLowerCase().contains(`market`)"
            - Table name: "TableName.toLowerCase().contains(`price`)"

        String Starts/Ends With:
            - Starts with: "TableName.startsWith(`daily_`)"
            - Ends with: "TableName.endsWith(`_prices`)"

        Multiple Values (IN):
            - Namespace in list: "Namespace in `market_data`, `reference_data`"
            - Case-insensitive: "Namespace icase in `market_data`, `reference_data`"

        NOT IN:
            - Exclude namespaces: "Namespace not in `test`, `staging`"
            - Case-insensitive: "Namespace icase not in `test`, `staging`"

        Regex Matching:
            - Pattern match: "TableName.matches(`.*_daily_.*`)"

        Comparison Operators:
            - Not equal: "Namespace != `test`"
            - Greater than: "Size > 1000000"
            - Less than: "RowCount < 100"
            - Range: "inRange(RowCount, 100, 10000)"

        Combining Filters (AND logic):
            filters=["Namespace = `market_data`", "TableName.contains(`price`)"]

    Important Notes About Filters:
        - String literals MUST use backticks (`), not single (') or double (") quotes
        - Filters are case-sensitive by default; use .toLowerCase() for case-insensitive matching
        - Multiple filters in the list are combined with AND (all must match)
        - For OR logic, use a single filter with boolean operators
        - Invalid filter syntax will cause the tool to return an error
        - See https://deephaven.io/core/docs/how-to-guides/use-filters/ for complete syntax

    Args:
        context (Context): The MCP context object.
        session_id (str): ID of the Deephaven enterprise session to query.
        max_rows (int | None): Maximum number of catalog entries to return. Default is 10000.
                               Set to None to retrieve entire catalog (use with caution for large deployments).
        filters (list[str] | None): Optional list of Deephaven where clause expressions to filter catalog.
                                    Multiple filters are combined with AND logic. Use backticks (`) for string literals.
        format (str): Output format for catalog data. Default is "optimize-rendering" for best table display.
                     Options: "optimize-rendering" (default, uses markdown-table), "optimize-accuracy" (uses markdown-kv),
                     "optimize-cost" (uses csv), "optimize-speed" (uses json-column), or explicit formats:
                     "json-row", "json-column", "csv", "markdown-table", "markdown-kv", "yaml", "xml".

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if catalog was retrieved successfully, False on error.
            - 'session_id' (str, optional): The session ID if successful.
            - 'format' (str, optional): Actual format used for data if successful (e.g., "json-row").
            - 'row_count' (int, optional): Number of catalog entries returned if successful.
            - 'is_complete' (bool, optional): True if all catalog entries returned, False if truncated by max_rows.
            - 'columns' (list[dict], optional): Schema of catalog table if successful. Each dict contains:
                {'name': str, 'type': str} describing catalog columns like Namespace, TableName, etc.
            - 'data' (list[dict] | dict | str, optional): Catalog data in requested format if successful:
                - json-row: List of dicts, one per catalog entry
                - json-column: Dict mapping column names to arrays of values
                - csv: String with CSV-formatted catalog data
                - markdown-table: String with pipe-delimited table format
                - markdown-kv: String with record headers and key-value pairs
                - yaml: String with YAML-formatted catalog entries
                - xml: String with XML catalog structure
            - 'error' (str, optional): Human-readable error message if retrieval failed. Omitted on success.
            - 'isError' (bool, optional): Present and True only when success=False. Explicit error flag.

    Error Scenarios:
        - Invalid session_id: Returns error if session doesn't exist or is not accessible
        - Community session: Returns error if session is not an enterprise (Core+) session
        - Invalid filters: Returns error if filter syntax is invalid or references non-existent columns
        - Invalid format: Returns error if format is not one of the supported options
        - Response too large: Returns error if estimated response would exceed 50MB limit
        - Session connection issues: Returns error if unable to communicate with Deephaven server
        - Permission errors: Returns error if session lacks permission to access catalog

    Performance Considerations:
        - Default max_rows of 10000 is safe for most enterprise deployments
        - Use filters to reduce result set size for better performance
        - Catalog retrieval is typically fast but scales with number of tables
        - Large catalogs (10000+ tables) may benefit from more specific filters
        - Response size is validated to prevent memory issues (50MB limit)

    Example Usage:
        # Get first 10000 catalog entries
        Tool: catalog_tables_list
        Parameters: {
            "session_id": "enterprise:prod:analytics"
        }

        # Filter by namespace
        Tool: catalog_tables_list
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "filters": ["Namespace = `market_data`"]
        }

        # Filter by table name pattern
        Tool: catalog_tables_list
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "filters": ["TableName.contains(`price`)"]
        }

        # Multiple filters (AND logic)
        Tool: catalog_tables_list
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "filters": ["Namespace = `market_data`", "TableName.toLowerCase().contains(`daily`)"]
        }

        # Get all catalog entries (use with caution)
        Tool: catalog_tables_list
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "max_rows": null
        }

        # CSV format for easy parsing
        Tool: catalog_tables_list
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "filters": ["Namespace = `reference_data`"],
            "format": "csv"
        }
    """
    _LOGGER.info(
        f"[mcp_systems_server:catalog_tables] Invoked: session_id={session_id!r}, "
        f"max_rows={max_rows}, filters={filters!r}, format={format!r}"
    )

    return await _get_catalog_data(
        context,
        session_id,
        distinct_namespaces=False,
        max_rows=max_rows,
        filters=filters,
        format=format,
        tool_name="catalog_tables",
    )




@mcp_server.tool()
async def catalog_namespaces_list(
    context: Context,
    session_id: str,
    max_rows: int | None = 1000,
    filters: list[str] | None = None,
    format: str = "optimize-rendering",
) -> dict:
    """
    MCP Tool: Retrieve catalog namespaces as a TABULAR LIST from a Deephaven Enterprise (Core+) session.

    **Returns**: Namespace information formatted as TABULAR DATA for display. Each row represents
    a data domain available in the enterprise catalog/database. This tabular data should be displayed as a
    table to users for easy browsing of available data domains.

    This tool retrieves the list of distinct namespaces available via the `deephaven_enterprise.database`
    package (the `db` variable) in an enterprise session. These namespaces represent data domains that
    contain tables in the catalog (database) accessible using methods like `db.live_table(namespace, table_name)` or
    `db.historical_table(namespace, table_name)`. This enables efficient discovery of data domains
    before drilling down into specific tables. This is typically the first step in exploring an
    enterprise data catalog. Only works with enterprise sessions.

    **Format Accuracy for AI Agents** (based on empirical research):
    - markdown-kv: 61% accuracy (highest comprehension, more tokens)
    - markdown-table: 55% accuracy (good balance)
    - json-row/json-column: 50% accuracy
    - yaml: 50% accuracy
    - xml: 45% accuracy
    - csv: 44% accuracy (lowest comprehension, fewest tokens)

    For more information, see:
    - https://deephaven.io
    - https://docs.deephaven.io/pycoreplus/latest/worker/code/deephaven_enterprise.database.html

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'ENTERPRISE' sessions run Deephaven Enterprise (also called 'Core+' or 'CorePlus')
    - This tool only works with enterprise sessions; community sessions do not have catalog tables
    - 'Namespace' refers to a data domain or organizational grouping of tables
    - 'Catalog' and 'database' are interchangeable terms - the catalog is the database of available tables
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    Table Rendering:
    - **This tool returns TABULAR NAMESPACE DATA that MUST be displayed as a table to users**
    - Each row represents one data domain (namespace) in the enterprise catalog
    - Column: Namespace (the name of the data domain)
    - Present as a table for easy browsing and discovery of data domains
    - Do NOT present namespace data as plain text or unstructured lists

    AI Agent Usage:
    - Use this as the first step to discover available data domains in the enterprise catalog/database
    - The catalog is the database of available tables organized by namespaces (data domains)
    - Namespaces represent data domains accessible via `db.live_table(namespace, table_name)` or `db.historical_table(namespace, table_name)`
    - Much faster than retrieving full catalog when you just need to know what domains exist
    - Filter catalog first if you want namespaces from a specific subset of tables
    - Combine with catalog_tables_list to drill down into specific namespaces
    - Essential for top-down data exploration workflow
    - Returns lightweight data (just namespace names) for quick discovery

    Args:
        context (Context): The MCP context object.
        session_id (str): ID of the Deephaven enterprise session to query.
        max_rows (int | None): Maximum number of namespaces to return. Default is 1000.
                               Set to None to retrieve all namespaces (use with caution).
        filters (list[str] | None): Optional list of Deephaven where clause expressions to filter
                                    the catalog before extracting namespaces. Use backticks (`) for string literals.
        format (str): Output format for namespace data. Default is "optimize-rendering" for best table display.
                     Options: "optimize-rendering" (default, uses markdown-table), "optimize-accuracy" (uses markdown-kv),
                     "optimize-cost" (uses csv), "optimize-speed" (uses json-column), or explicit formats:
                     "json-row", "json-column", "csv", "markdown-table", "markdown-kv", "yaml", "xml".

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if namespaces were retrieved successfully, False on error.
            - 'session_id' (str, optional): The session ID if successful.
            - 'format' (str, optional): Actual format used for data if successful (e.g., "json-row").
            - 'row_count' (int, optional): Number of namespaces returned if successful.
            - 'is_complete' (bool, optional): True if all namespaces returned, False if truncated by max_rows.
            - 'columns' (list[dict], optional): Schema of namespace table if successful. Contains:
                {'name': 'Namespace', 'type': 'string'}
            - 'data' (list[dict] | dict | str, optional): Namespace data in requested format if successful:
                - json-row: List of dicts, one per namespace: [{"Namespace": "market_data"}, ...]
                - json-column: Dict mapping column name to array: {"Namespace": ["market_data", ...]}
                - csv: String with CSV-formatted namespace data
                - markdown-table: String with pipe-delimited table format
                - markdown-kv: String with record headers and key-value pairs
                - yaml: String with YAML-formatted namespace list
                - xml: String with XML namespace structure
            - 'error' (str, optional): Human-readable error message if retrieval failed. Omitted on success.
            - 'isError' (bool, optional): Present and True only when success=False. Explicit error flag.

    Error Scenarios:
        - Non-enterprise session: Returns error if session is not an enterprise (Core+) session
        - Session not found: Returns error if session_id does not exist or is not accessible
        - Invalid filter: Returns error if filter syntax is invalid
        - Invalid format: Returns error if format is not one of the supported options
        - Response too large: Returns error if estimated response would exceed 50MB limit
        - Session connection issues: Returns error if unable to communicate with Deephaven server

    Performance Considerations:
        - Default max_rows of 1000 is safe for most enterprise deployments
        - Namespace retrieval is very fast (typically < 1 second)
        - Much more efficient than retrieving full catalog for initial discovery
        - Filters are applied to catalog before extracting namespaces for efficiency

    Example Usage:
        # Get all namespaces (up to 1000)
        Tool: catalog_namespaces_list
        Parameters: {
            "session_id": "enterprise:prod:analytics"
        }

        # Get namespaces from filtered catalog
        Tool: catalog_namespaces_list
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "filters": ["TableName.contains(`daily`)"]
        }

        # CSV format
        Tool: catalog_namespaces_list
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "format": "csv"
        }
    """
    _LOGGER.info(
        f"[mcp_systems_server:catalog_namespaces] Invoked: session_id={session_id!r}, "
        f"max_rows={max_rows}, filters={filters!r}, format={format!r}"
    )

    return await _get_catalog_data(
        context,
        session_id,
        distinct_namespaces=True,
        max_rows=max_rows,
        filters=filters,
        format=format,
        tool_name="catalog_namespaces",
    )




@mcp_server.tool()
async def catalog_tables_schema(
    context: Context,
    session_id: str,
    namespace: str | None = None,
    table_names: list[str] | None = None,
    filters: list[str] | None = None,
    max_tables: int | None = 100,
) -> dict:
    """
    MCP Tool: Retrieve catalog table schemas as TABULAR METADATA from a Deephaven Enterprise (Core+) session.

    **Returns**: Schema information formatted as TABULAR DATA where each row represents a column
    in a catalog/database table. This tabular metadata should be displayed as a table to users for easy
    comprehension of catalog table structures.

    This tool retrieves column schemas for tables in the enterprise catalog (database). The catalog contains
    metadata about tables accessible via the `deephaven_enterprise.database` package (the `db` variable).
    You can filter by namespace, specify exact table names, use custom filters, or discover all schemas
    up to the max_tables limit. This is essential for understanding the structure of catalog tables before
    loading them with `db.live_table()` or `db.historical_table()`. Only works with enterprise sessions.

    For more information, see:
    - https://deephaven.io
    - https://docs.deephaven.io/pycoreplus/latest/worker/code/deephaven_enterprise.database.html

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'ENTERPRISE' sessions run Deephaven Enterprise (also called 'Core+' or 'CorePlus')
    - This tool only works with enterprise sessions; community sessions do not have catalog tables
    - 'Namespace' refers to a data domain or organizational grouping of tables in the catalog
    - 'Catalog' and 'database' are interchangeable terms - the catalog is the database of available tables
    - 'Schema' and 'meta table' are interchangeable terms - both refer to table metadata
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    Table Rendering:
    - **This tool returns TABULAR SCHEMA METADATA that MUST be displayed as a table to users**
    - Each row in the result represents one column from a catalog table
    - The table shows column properties: Name, DataType, IsPartitioning, ComponentType, etc.
    - Present schema data in tabular format (table or grid) for easy comprehension
    - Do NOT present schema data as plain text or unstructured lists

    AI Agent Usage:
    - Use this to understand catalog/database table structures before loading them into a session
    - The catalog is the database of available tables with their schemas
    - Filter by namespace to get schemas for all tables in a specific data domain
    - Specify table_names when you know exactly which tables you need
    - Use filters for complex discovery patterns (e.g., tables containing specific keywords)
    - Default max_tables=100 prevents accidentally fetching thousands of schemas
    - Set max_tables=None only when you intentionally want all schemas (use with caution)
    - Check 'namespace' field in each result to know which domain the table belongs to
    - Use returned schemas to generate correct `db.live_table(namespace, table_name)` calls
    - Individual table failures don't stop processing of other tables (similar to session_tables_schema)
    - Always check 'success' field in each schema result before using the schema data

    Filter Syntax Reference:
    Filters use Deephaven query language with backticks (`) for string literals.
    Multiple filters are combined with AND logic.

    Common Filter Patterns:
        Exact Match:
            - Namespace exact: "Namespace = `market_data`"
            - Table name exact: "TableName = `daily_prices`"

        String Contains (case-sensitive):
            - Namespace contains: "Namespace.contains(`market`)"
            - Table name contains: "TableName.contains(`price`)"

        String Contains (case-insensitive):
            - Namespace: "Namespace.toLowerCase().contains(`market`)"
            - Table name: "TableName.toLowerCase().contains(`price`)"

        Multiple Values (IN):
            - Namespace in list: "Namespace in `market_data`, `reference_data`"
            - Case-insensitive: "Namespace icase in `market_data`, `reference_data`"

        Combining Filters (AND logic):
            filters=["Namespace = `market_data`", "TableName.contains(`price`)"]

    Args:
        context (Context): The MCP context object, required by MCP protocol but not actively used.
        session_id (str): ID of the Deephaven enterprise session to query. Must be an enterprise (Core+) session.
        namespace (str | None, optional): Filter to tables in this specific namespace. If None, searches all namespaces.
                                         Defaults to None.
        table_names (list[str] | None, optional): List of specific table names to retrieve schemas for.
                                                  If None, retrieves schemas for all tables (up to max_tables limit).
                                                  When specified with namespace, only tables in that namespace are considered.
                                                  Defaults to None.
        filters (list[str] | None, optional): List of Deephaven where clause expressions to filter the catalog.
                                             Multiple filters are combined with AND logic. Use backticks (`) for string literals.
                                             Applied before namespace and table_names filtering. Defaults to None.
        max_tables (int | None, optional): Maximum number of table schemas to retrieve. Defaults to 100 for safety.
                                          Set to None to retrieve all matching schemas (use with extreme caution for large catalogs).
                                          This limit is applied after all filtering.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if the operation completed, False if it failed entirely.
            - 'schemas' (list[dict], optional): List of per-table schema results if operation completed. Each contains:
                - 'success' (bool): True if this table's schema was retrieved successfully
                - 'namespace' (str): Namespace (data domain) the table belongs to
                - 'table' (str): Table name
                - 'schema' (list[dict], optional): List of column definitions (name/type pairs) if successful
                - 'error' (str, optional): Error message if this table's schema retrieval failed
                - 'isError' (bool, optional): Present and True if this table had an error
            - 'count' (int, optional): Number of schemas returned if successful
            - 'is_complete' (bool, optional): True if all matching tables were processed, False if truncated by max_tables
            - 'error' (str, optional): Error message if the entire operation failed.
            - 'isError' (bool, optional): Present and True if this is an error response.

    Error Scenarios:
        - Non-enterprise session: Returns error if session is not an enterprise (Core+) session
        - Invalid session_id: Returns error if session doesn't exist or is not accessible
        - Invalid filters: Returns error if filter syntax is invalid or references non-existent columns
        - Session connection issues: Returns error if unable to communicate with Deephaven server
        - Catalog access error: Returns error if unable to retrieve catalog table
        - Individual table errors: Reported in per-table results, don't stop overall operation

    Performance Considerations:
        - Default max_tables=100 is safe for most use cases
        - Fetching schemas for 1000+ tables can take significant time (several minutes)
        - Use namespace or filters to narrow down the search space
        - Specify exact table_names when you know what you need for fastest results
        - Each schema fetch requires a separate query to the catalog

    Example Successful Response (mixed results):
        {
            'success': True,
            'schemas': [
                {
                    'success': True,
                    'namespace': 'market_data',
                    'table': 'daily_prices',
                    'schema': [{'name': 'Date', 'type': 'LocalDate'}, {'name': 'Price', 'type': 'double'}]
                },
                {
                    'success': False,
                    'namespace': 'market_data',
                    'table': 'missing_table',
                    'error': 'Table not found in catalog',
                    'isError': True
                }
            ],
            'count': 2,
            'is_complete': True
        }

    Example Error Response (total failure):
        {
            'success': False,
            'error': 'Session is not an enterprise (Core+) session',
            'isError': True
        }

    Example Usage:
        # Get schemas for all tables in a namespace (up to 100)
        Tool: catalog_tables_schema
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "namespace": "market_data"
        }

        # Get schemas for specific tables in a namespace
        Tool: catalog_tables_schema
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "namespace": "market_data",
            "table_names": ["daily_prices", "intraday_quotes"]
        }

        # Filter-based discovery across namespaces
        Tool: catalog_tables_schema
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "filters": ["TableName.contains(`price`)"]
        }

        # Get more than 100 schemas (explicit limit)
        Tool: catalog_tables_schema
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "namespace": "market_data",
            "max_tables": 500
        }

        # Get all schemas (requires explicit None, use with extreme caution)
        Tool: catalog_tables_schema
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "max_tables": None
        }
    """
    _LOGGER.info(
        f"[mcp_systems_server:catalog_tables_schema] Invoked: session_id={session_id!r}, "
        f"namespace={namespace!r}, table_names={table_names!r}, filters={filters!r}, max_tables={max_tables}"
    )

    schemas = []

    try:
        # Get and validate enterprise session
        session, error = await _get_enterprise_session(
            "catalog_tables_schema", context, session_id
        )

        if error:
            return error

        session = cast(CorePlusSession, session)  # Type narrowing for mypy

        _LOGGER.info(
            f"[mcp_systems_server:catalog_tables_schema] Session established for enterprise session: '{session_id}'"
        )

        # Build combined filters list
        combined_filters = []
        if filters:
            combined_filters.extend(filters)
        if namespace:
            combined_filters.append(f"Namespace = `{namespace}`")
        if table_names:
            table_names_quoted = ", ".join(f"`{name}`" for name in table_names)
            combined_filters.append(f"TableName in {table_names_quoted}")

        _LOGGER.debug(
            f"[mcp_systems_server:catalog_tables_schema] Combined filters: {combined_filters!r}"
        )

        # Get catalog table with filters
        # Use max_tables as max_rows to limit catalog query and prevent excessive RAM usage
        _LOGGER.debug(
            f"[mcp_systems_server:catalog_tables_schema] Retrieving catalog table from session '{session_id}' "
            f"(max_rows={max_tables})"
        )
        catalog_arrow_table, is_complete_catalog = await queries.get_catalog_table(
            session,
            max_rows=max_tables,  # Limit catalog query to match max_tables
            filters=combined_filters if combined_filters else None,
            distinct_namespaces=False,
        )

        # Convert to list of dicts for easier processing
        catalog_entries = catalog_arrow_table.to_pylist()
        _LOGGER.info(
            f"[mcp_systems_server:catalog_tables_schema] Retrieved {len(catalog_entries)} catalog entries after filtering"
        )

        # is_complete_catalog already reflects whether the catalog was truncated
        is_complete = is_complete_catalog

        _LOGGER.debug(
            f"[mcp_systems_server:catalog_tables_schema] Processing {len(catalog_entries)} catalog entries "
            f"(is_complete={is_complete})"
        )

        # Fetch schema for each catalog entry
        for entry in catalog_entries:
            # These fields are required - let it fail if they're missing
            catalog_namespace = entry["Namespace"]
            catalog_table_name = entry["TableName"]

            _LOGGER.debug(
                f"[mcp_systems_server:catalog_tables_schema] Processing catalog table "
                f"'{catalog_namespace}.{catalog_table_name}'"
            )

            try:
                # Get schema for catalog table (tries historical_table first, then live_table)
                _LOGGER.debug(
                    f"[mcp_systems_server:catalog_tables_schema] Retrieving schema for "
                    f"'{catalog_namespace}.{catalog_table_name}'"
                )
                arrow_meta_table = await queries.get_catalog_meta_table(
                    session, catalog_namespace, catalog_table_name
                )

                # Use helper to format result (include namespace for catalog tables)
                result = _format_meta_table_result(
                    arrow_meta_table, catalog_table_name, namespace=catalog_namespace
                )
                schemas.append(result)

                _LOGGER.info(
                    f"[mcp_systems_server:catalog_tables_schema] Success: Retrieved full schema for "
                    f"'{catalog_namespace}.{catalog_table_name}' ({result['row_count']} columns)"
                )

            except Exception as table_exc:
                _LOGGER.error(
                    f"[mcp_systems_server:catalog_tables_schema] Failed to get schema for "
                    f"'{catalog_namespace}.{catalog_table_name}': {table_exc!r}",
                    exc_info=True,
                )
                schemas.append(
                    {
                        "success": False,
                        "namespace": catalog_namespace,
                        "table": catalog_table_name,
                        "error": str(table_exc),
                        "isError": True,
                    }
                )

        _LOGGER.info(
            f"[mcp_systems_server:catalog_tables_schema] Completed: Retrieved {len(schemas)} schema(s), "
            f"is_complete={is_complete}"
        )

        return {
            "success": True,
            "schemas": schemas,
            "count": len(schemas),
            "is_complete": is_complete,
        }

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:catalog_tables_schema] Failed for session: '{session_id}', error: {e!r}",
            exc_info=True,
        )
        return {"success": False, "error": str(e), "isError": True}




@mcp_server.tool()
async def catalog_table_sample(
    context: Context,
    session_id: str,
    namespace: str,
    table_name: str,
    max_rows: int | None = 100,
    head: bool = True,
    format: str = "optimize-rendering",
) -> dict:
    r"""
    MCP Tool: Retrieve sample TABULAR DATA from a catalog table in a Deephaven Enterprise (Core+) session.

    **Returns**: Sample table data formatted as TABULAR DATA for display. This tabular data should be
    displayed as a table to users for previewing catalog table contents.

    This tool loads a catalog table (trying historical_table first, then live_table as fallback) and
    retrieves a sample of its data with flexible formatting options. Use this to preview catalog table
    contents before loading the full table into a session. Only works with enterprise sessions.

    **Format Accuracy for AI Agents** (based on empirical research):
    - markdown-kv: 61% accuracy (highest comprehension, more tokens)
    - markdown-table: 55% accuracy (good balance)
    - json-row/json-column: 50% accuracy
    - yaml: 50% accuracy
    - xml: 45% accuracy
    - csv: 44% accuracy (lowest comprehension, fewest tokens)

    For more information, see:
    - https://deephaven.io
    - https://docs.deephaven.io/pycoreplus/latest/worker/code/deephaven_enterprise.database.html

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'ENTERPRISE' sessions run Deephaven Enterprise (also called 'Core+' or 'CorePlus')
    - This tool only works with enterprise sessions; community sessions do not have catalog tables
    - 'Namespace' refers to a data domain or organizational grouping of tables in the catalog
    - 'Catalog' and 'database' are interchangeable terms - the catalog is the database of available tables
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    Table Rendering:
    - **This tool returns TABULAR SAMPLE DATA that MUST be displayed as a table to users**
    - The 'data' field contains formatted table data ready for display
    - Use 'markdown-table' or 'markdown-kv' formats for best table rendering in AI interfaces
    - Always present the returned data in tabular format (table, grid, or structured rows)
    - Do NOT present table data as plain text or unstructured content

    AI Agent Usage:
    - Use this to preview catalog/database table contents before loading full tables
    - The catalog is the database of available tables with sample data
    - Default max_rows=100 provides safe preview without overwhelming responses
    - Use head=True (default) to get rows from table start, head=False to get from table end
    - Check 'is_complete' to know if the sample represents the entire table
    - Combine with catalog_tables_schema to understand table structure before sampling
    - Use 'optimize-rendering' (default) for best table display in AI interfaces
    - Use 'optimize-accuracy' for highest comprehension (markdown-kv format, more tokens)
    - Check 'format' field in response to know actual format used

    Args:
        context (Context): The MCP context object.
        session_id (str): ID of the Deephaven enterprise session to query.
        namespace (str): The catalog namespace containing the table.
        table_name (str): Name of the catalog table to sample.
        max_rows (int | None, optional): Maximum number of rows to retrieve. Defaults to 100 for safe sampling.
                                         Set to None to retrieve entire table (use with caution for large tables).
        head (bool, optional): Direction of row retrieval. If True (default), retrieve from beginning.
                              If False, retrieve from end (most recent rows for time-series data).
        format (str, optional): Output format selection. Defaults to "optimize-rendering" for best table display.
                               Options:
                               - "optimize-rendering": (DEFAULT) Always use markdown-table (best for AI agent table display)
                               - "optimize-accuracy": Always use markdown-kv (better comprehension, more tokens)
                               - "optimize-cost": Always use csv (fewer tokens, may be harder to parse)
                               - "optimize-speed": Always use json-column (fastest conversion)
                               - "markdown-table": String with pipe-delimited table (| col1 | col2 |\n| --- | --- |\n| val1 | val2 |)
                               - "markdown-kv": String with record headers and key-value pairs (## Record 1\ncol1: val1\ncol2: val2)
                               - "json-row": List of dicts, one per row
                               - "json-column": Dict with column names as keys, value arrays
                               - "csv": String with comma-separated values, includes header row
                               - "yaml": String with YAML-formatted records list
                               - "xml": String with XML records structure

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): Always present. True if sample was retrieved successfully, False on any error.
            - 'namespace' (str, optional): The catalog namespace if successful.
            - 'table_name' (str, optional): Name of the sampled table if successful.
            - 'format' (str, optional): Actual format used for the data if successful. May differ from request when using optimization strategies.
            - 'schema' (list[dict], optional): Array of column definitions if successful. Each dict contains:
                                              {'name': str, 'type': str} describing column name and PyArrow data type.
            - 'row_count' (int, optional): Number of rows in the returned sample if successful.
            - 'is_complete' (bool, optional): True if entire table was retrieved if successful. False if truncated by max_rows.
            - 'data' (list | dict | str, optional): The actual sample data if successful. Type depends on format.
            - 'error' (str, optional): Human-readable error message if retrieval failed. Omitted on success.
            - 'isError' (bool, optional): Present and True only when success=False. Explicit error flag.

    Error Scenarios:
        - Invalid session_id: Returns error if session doesn't exist or is not accessible
        - Community session: Returns error if session is not an enterprise (Core+) session
        - Invalid namespace: Returns error if namespace doesn't exist in the catalog
        - Invalid table_name: Returns error if table doesn't exist in the namespace
        - Invalid format: Returns error if format is not one of the supported options
        - Response too large: Returns error if estimated response would exceed 50MB limit
        - Session connection issues: Returns error if unable to communicate with Deephaven server
        - Table access errors: Returns error if table cannot be accessed via historical_table or live_table

    Performance Considerations:
        - Default max_rows of 100 is safe for previewing catalog tables
        - Use csv format or limit max_rows for very wide tables
        - Default optimize-rendering format provides good table display
        - Response size limit: 50MB maximum to prevent memory issues

    Example Usage:
        # Sample first 100 rows with default format
        Tool: catalog_table_sample
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "namespace": "market_data",
            "table_name": "daily_prices"
        }

        # Sample last 50 rows (most recent for time-series)
        Tool: catalog_table_sample
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "namespace": "market_data",
            "table_name": "trades",
            "max_rows": 50,
            "head": false
        }

        # Sample with CSV format
        Tool: catalog_table_sample
        Parameters: {
            "session_id": "enterprise:prod:analytics",
            "namespace": "reference_data",
            "table_name": "symbols",
            "max_rows": 200,
            "format": "csv"
        }
    """
    _LOGGER.info(
        f"[mcp_systems_server:catalog_table_sample] Invoked: session_id={session_id!r}, "
        f"namespace={namespace!r}, table_name={table_name!r}, max_rows={max_rows}, head={head}, format={format!r}"
    )

    try:
        # Get and validate enterprise session
        session, error = await _get_enterprise_session(
            "catalog_table_sample", context, session_id
        )

        if error:
            return error

        session = cast(CorePlusSession, session)  # Type narrowing for mypy

        _LOGGER.info(
            f"[mcp_systems_server:catalog_table_sample] Session established for enterprise session: '{session_id}'"
        )

        # Get catalog table data using queries module
        _LOGGER.debug(
            f"[mcp_systems_server:catalog_table_sample] Retrieving catalog table data for '{namespace}.{table_name}'"
        )
        arrow_table, is_complete = await queries.get_catalog_table_data(
            session, namespace, table_name, max_rows=max_rows, head=head
        )

        # Check response size before formatting
        row_count = len(arrow_table)
        col_count = len(arrow_table.schema)
        estimated_size = row_count * col_count * ESTIMATED_BYTES_PER_CELL
        size_error = _check_response_size(f"{namespace}.{table_name}", estimated_size)

        if size_error:
            return size_error

        # Build response using helper
        _LOGGER.debug(
            f"[mcp_systems_server:catalog_table_sample] Formatting {row_count} rows in format '{format}'"
        )
        response = _build_table_data_response(
            arrow_table, is_complete, format, table_name=table_name, namespace=namespace
        )

        _LOGGER.info(
            f"[mcp_systems_server:catalog_table_sample] Success: Retrieved {row_count} rows "
            f"from '{namespace}.{table_name}' (is_complete={is_complete}, format={response['format']})"
        )

        return response

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:catalog_table_sample] Failed for session: '{session_id}', "
            f"namespace: '{namespace}', table: '{table_name}', error: {e!r}",
            exc_info=True,
        )
        return {"success": False, "error": str(e), "isError": True}


