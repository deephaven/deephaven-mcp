"""Catalog MCP Tools - Enterprise Core+ Data Catalog Operations.

Provides MCP tools for querying Deephaven Enterprise (Core+) data catalogs:
- catalog_tables_list: List table names across catalog namespaces
- catalog_namespaces_list: List available catalog namespaces
- catalog_table_schema: Get schema information for one catalog table
- catalog_table_sample: Sample data from catalog tables

These tools require Deephaven Enterprise (Core+) and are not available in Community.

The two discovery tools are system-scoped: they name an enterprise system and
read the catalog listing through that system's shared ``WebClientData``
persistent query, scoped to the Enterprise principal the server is configured
with for that system — not to the MCP caller, so every caller sees the same
listing. The two table-level tools are session-scoped: reading a catalog
table's schema or rows is a data access that the server admits only on a worker
the caller administers, so they take a session id.
"""

import logging

from mcp.server.fastmcp import Context, FastMCP

from deephaven_mcp import queries
from deephaven_mcp.client import describe_exception_chain
from deephaven_mcp.formatters import TableFormat
from deephaven_mcp.mcp_systems_server._tools.shared import (
    build_table_data_response,
    check_response_size,
    error_response,
    format_schema_result,
    get_enterprise_session,
    get_enterprise_settings,
    get_response_limits,
    get_wcd_system_session,
)

_LOGGER = logging.getLogger(__name__)

_ENTRY_OVERHEAD_BYTES = 48
"""Per-entry serialization allowance added to Arrow buffer sizes when
estimating the ``tables`` payload: each ``{"namespace": ..., "table_name":
...}`` entry repeats both key names plus quotes, colons, braces, and
delimiters (~38 characters), none of which ``pyarrow.Table.nbytes``
counts."""


def _catalog_failure_error(
    tool_name: str, system: str, e: Exception
) -> dict[str, object]:
    """Log (with traceback) and build the error response for a failed catalog operation.

    Args:
        tool_name (str): Name of the calling tool for logging.
        system (str): The enterprise system name the caller supplied.
        e (Exception): The exception that aborted the operation.

    Returns:
        dict[str, object]: Standard error response dict from :func:`error_response`.
    """
    _LOGGER.error(
        f"[mcp_systems_server:{tool_name}] Failed for system '{system}': {e!r}",
        exc_info=True,
    )
    return error_response(
        f"Catalog operation failed for system '{system}': {describe_exception_chain(e)}"
    )


async def catalog_tables_list(
    context: Context,
    system: str,
    max_rows: int | None = 10000,
    filters: list[str] | None = None,
) -> dict:
    """MCP Tool: List the tables in a Deephaven Enterprise (Core+) catalog.

    **Returns**: A lean discovery list — one ``{"namespace", "table_name"}``
    entry per catalog table. For column definitions use catalog_table_schema;
    for row data use catalog_table_sample.

    The catalog (also called database) lists tables accessible via the
    `deephaven_enterprise.database` package (the `db` variable) in an
    enterprise session, e.g. `db.live_table(namespace, table_name)` or
    `db.historical_table(namespace, table_name)`. Only works with
    enterprise systems.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')
    - 'PQ' is shorthand for Persistent Query
    - This tool only works with enterprise systems; community deployments do not have catalog tables

    AI Agent Usage:
    - Use this to discover what tables exist; follow up with catalog_table_schema
      (per table) for column definitions and catalog_table_sample for row data
    - Filter by namespace or table-name patterns to narrow large catalogs
    - Check 'is_complete' to know if all catalog entries were returned
    - **Catalog listing ≠ data access**: a listed table is not guaranteed to be
      loadable — it may be protected by access controls, not yet populated, or
      otherwise inaccessible. Treat results as a *candidate set* and handle
      per-table fetch failures gracefully.

    Filter Syntax Reference:
    Filters use Deephaven query language on the columns 'Namespace' and
    'TableName', with backticks (`) for string literals — never single or
    double quotes. Multiple filters are combined with AND logic.

    Common Filter Patterns:
        - Namespace exact: "Namespace = `market_data`"
        - Table name contains: "TableName.contains(`price`)"
        - Case-insensitive: "TableName.toLowerCase().contains(`price`)"
        - Starts/ends with: "TableName.startsWith(`daily_`)", "TableName.endsWith(`_prices`)"
        - In list: "Namespace in `market_data`, `reference_data`"
        - Regex: "TableName.matches(`.*_daily_.*`)"
        - See https://deephaven.io/core/docs/how-to-guides/use-filters/ for complete syntax

    Args:
        context (Context): The MCP context object.
        system (str): Enterprise system name (e.g. 'prod'), as returned by list_systems.
        max_rows (int | None): Maximum number of catalog entries to return. Default is 10000.
                               Set to None to retrieve entire catalog (use with caution for large deployments).
        filters (list[str] | None): Optional list of Deephaven where clause expressions to filter catalog.
                                    Multiple filters are combined with AND logic. Use backticks (`) for string literals.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if catalog was retrieved successfully, False on error.
            - 'system' (str, optional): The enterprise system name if successful.
            - 'tables' (list[dict], optional): One entry per catalog table if successful.
                Each contains 'namespace' (str) and 'table_name' (str).
            - 'count' (int, optional): Number of entries returned if successful.
            - 'is_complete' (bool, optional): True if all catalog entries returned, False if truncated by max_rows.
            - 'error' (str, optional): Human-readable error message if retrieval failed. Omitted on success.
            - 'isError' (bool, optional): Present and True only when success=False. Explicit error flag.

    Error Scenarios:
        - Unknown system: Returns error if system is not a configured enterprise system
        - WebClientData unavailable: Returns error if the system's 'WebClientData' PQ is not running
        - Invalid filters: Returns error if filter syntax is invalid or references non-existent columns
        - Response too large: Returns error if estimated response would exceed the configured response-size limit (default 50MB)
        - Connection issues: Returns error if unable to communicate with Deephaven server

    Example Usage:
        # List catalog tables (up to 10000)
        Tool: catalog_tables_list
        Parameters: {
            "system": "prod"
        }

        # Filter by namespace
        Tool: catalog_tables_list
        Parameters: {
            "system": "prod",
            "filters": ["Namespace = `market_data`"]
        }

    Example Successful Response:
        {
            'success': True,
            'system': 'prod',
            'tables': [
                {'namespace': 'market_data', 'table_name': 'daily_prices'},
                {'namespace': 'market_data', 'table_name': 'trades'}
            ],
            'count': 2,
            'is_complete': True
        }
    """
    _LOGGER.info(
        f"[mcp_systems_server:catalog_tables_list] Invoked: system={system!r}, "
        f"max_rows={max_rows}, filters={filters!r}"
    )

    try:
        access = await get_wcd_system_session("catalog_tables_list", context, system)
        settings = get_enterprise_settings(context)

        _LOGGER.debug(
            f"[mcp_systems_server:catalog_tables_list] Retrieving catalog entries with filters: {filters}"
        )
        arrow_table, is_complete = await queries.get_catalog_table(
            access.session,
            operate_as=access.operate_as,
            timeout_seconds=settings.timeouts.client.web_client_data_timeout_seconds,
            max_rows=max_rows,
            filters=filters,
            distinct_namespaces=False,
        )

        # Only the identity columns survive; drop the rest before sizing.
        subset = arrow_table.select(["Namespace", "TableName"])

        # Arrow buffer bytes undercount the serialized list-of-dicts form,
        # so add a per-entry envelope allowance on top.
        estimated_size = subset.nbytes + _ENTRY_OVERHEAD_BYTES * subset.num_rows
        limits = settings.response_limits
        size_check_result = check_response_size(
            "catalog_tables_list", estimated_size, limits
        )
        if size_check_result:
            return size_check_result

        tables = [
            {"namespace": entry["Namespace"], "table_name": entry["TableName"]}
            for entry in subset.to_pylist()
        ]

        _LOGGER.info(
            f"[mcp_systems_server:catalog_tables_list] Successfully retrieved "
            f"{len(tables)} catalog entries (complete={is_complete})"
        )

        return {
            "success": True,
            "system": system,
            "tables": tables,
            "count": len(tables),
            "is_complete": is_complete,
        }

    except Exception as e:
        return _catalog_failure_error("catalog_tables_list", system, e)


async def catalog_namespaces_list(
    context: Context,
    system: str,
    max_rows: int | None = 1000,
    filters: list[str] | None = None,
) -> dict:
    """MCP Tool: List the distinct namespaces in a Deephaven Enterprise (Core+) catalog.

    **Returns**: A plain sorted list of namespace name strings in the 'namespaces' key.
    Each namespace is a data domain available in the enterprise catalog/database.

    This tool retrieves the list of distinct namespaces available via the `deephaven_enterprise.database`
    package (the `db` variable) in an enterprise session. These namespaces represent data domains that
    contain tables in the catalog (database) accessible using methods like `db.live_table(namespace, table_name)` or
    `db.historical_table(namespace, table_name)`. This enables efficient discovery of data domains
    before drilling down into specific tables. This is typically the first step in exploring an
    enterprise data catalog. Only works with enterprise systems.

    For more information, see:
    - https://deephaven.io
    - https://docs.deephaven.io/pycoreplus/latest/worker/code/deephaven_enterprise.database.html

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')
    - 'PQ' is shorthand for Persistent Query
    - This tool only works with enterprise systems; community deployments do not have catalog tables
    - 'Namespace' refers to a data domain or organizational grouping of tables

    AI Agent Usage:
    - Use this as the first step to discover available data domains in the enterprise catalog/database
    - The catalog is the database of available tables organized by namespaces (data domains)
    - Namespaces represent data domains accessible via `db.live_table(namespace, table_name)` or `db.historical_table(namespace, table_name)`
    - Much faster than retrieving full catalog when you just need to know what domains exist
    - Filter catalog first if you want namespaces from a specific subset of tables
    - Combine with catalog_tables_list to drill down into specific namespaces
    - Essential for top-down data exploration workflow
    - Returns lightweight data (just namespace names) for quick discovery
    - Check 'is_complete': when False the list was truncated by max_rows; raise max_rows or add filters

    Args:
        context (Context): The MCP context object.
        system (str): Enterprise system name (e.g. 'prod'), as returned by list_systems.
        max_rows (int | None): Maximum number of namespaces to return. Default is 1000.
                               Set to None to retrieve all namespaces (use with caution).
        filters (list[str] | None): Optional list of Deephaven where clause expressions to filter
                                    the catalog before extracting namespaces. Use backticks (`) for string literals.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if namespaces were retrieved successfully, False on error.
            - 'system' (str, optional): The enterprise system name if successful.
            - 'namespaces' (list[str], optional): Sorted distinct namespace names if successful.
            - 'count' (int, optional): Number of namespaces returned if successful.
            - 'is_complete' (bool, optional): True if all namespaces returned, False if truncated by max_rows.
            - 'error' (str, optional): Human-readable error message if retrieval failed. Omitted on success.
            - 'isError' (bool, optional): Present and True only when success=False. Explicit error flag.

    Error Scenarios:
        - Unknown system: Returns error if system is not a configured enterprise system
        - WebClientData unavailable: Returns error if the system's 'WebClientData' PQ is not running
        - Invalid filter: Returns error if filter syntax is invalid
        - Response too large: Returns error if estimated response would exceed the configured response-size limit (default 50MB)
        - Connection issues: Returns error if unable to communicate with Deephaven server

    Performance Considerations:
        - Default max_rows of 1000 is safe for most enterprise deployments
        - Namespace retrieval is very fast (typically < 1 second)
        - Much more efficient than retrieving full catalog for initial discovery
        - Filters are applied to catalog before extracting namespaces for efficiency

    Example Usage:
        # Get all namespaces (up to 1000)
        Tool: catalog_namespaces_list
        Parameters: {
            "system": "prod"
        }

        # Get namespaces from filtered catalog
        Tool: catalog_namespaces_list
        Parameters: {
            "system": "prod",
            "filters": ["TableName.contains(`daily`)"]
        }

    Example Successful Response:
        {'success': True, 'system': 'prod', 'namespaces': ['market_data', 'reference'], 'count': 2, 'is_complete': True}
    """
    _LOGGER.info(
        f"[mcp_systems_server:catalog_namespaces_list] Invoked: system={system!r}, "
        f"max_rows={max_rows}, filters={filters!r}"
    )

    try:
        access = await get_wcd_system_session(
            "catalog_namespaces_list", context, system
        )
        settings = get_enterprise_settings(context)

        _LOGGER.debug(
            f"[mcp_systems_server:catalog_namespaces_list] Retrieving namespaces with filters: {filters}"
        )
        arrow_table, is_complete = await queries.get_catalog_table(
            access.session,
            operate_as=access.operate_as,
            timeout_seconds=settings.timeouts.client.web_client_data_timeout_seconds,
            max_rows=max_rows,
            filters=filters,
            distinct_namespaces=True,
        )

        # Reject an over-limit response before materializing it as Python objects.
        estimated_size = arrow_table.nbytes
        limits = settings.response_limits
        size_check_result = check_response_size(
            "catalog_namespaces_list", estimated_size, limits
        )
        if size_check_result:
            return size_check_result

        namespaces = arrow_table.column("Namespace").to_pylist()

        _LOGGER.info(
            f"[mcp_systems_server:catalog_namespaces_list] Successfully retrieved "
            f"{len(namespaces)} namespaces (complete={is_complete})"
        )

        return {
            "success": True,
            "system": system,
            "namespaces": namespaces,
            "count": len(namespaces),
            "is_complete": is_complete,
        }

    except Exception as e:
        return _catalog_failure_error("catalog_namespaces_list", system, e)


async def catalog_table_schema(
    context: Context,
    id: str,
    namespace: str,
    table_name: str,
) -> dict:
    """MCP Tool: Retrieve the schema of one catalog table in a Deephaven Enterprise (Core+) session.

    Returns the column definitions for a single catalog table identified by
    namespace and table name. Deliberately single-table: discover tables with
    catalog_namespaces_list / catalog_tables_list first, then fetch schemas
    one table per call (calls can run in parallel). Only works with
    enterprise sessions.

    For more information, see:
    - https://deephaven.io
    - https://docs.deephaven.io/pycoreplus/latest/worker/code/deephaven_enterprise.database.html

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')
    - 'PQ' is shorthand for Persistent Query
    - This tool only works with enterprise sessions; community sessions do not have catalog tables
    - 'Namespace' refers to a data domain or organizational grouping of tables in the catalog

    Session Scope:
    - Takes a SESSION ID, not a system name, unlike catalog_tables_list and
      catalog_namespaces_list
    - Reading a catalog table's data requires a worker you administer, so this
      runs against a session you own; the shared 'WebClientData' worker refuses
      it for anyone who is not an administrator of that worker
    - Run sessions_list or pq_list to find an id, or create one with
      session_enterprise_create

    AI Agent Usage:
    - Use catalog_tables_list first to discover namespace/table pairs, then call this per table
    - Use returned schemas to generate correct `db.live_table(namespace, table_name)` calls
    - Columns with 'column_type': 'Partitioning' are the table's partition columns —
      relevant when filtering catalog_table_sample on partitioned tables
    - 'type' values are Deephaven type names (e.g. "java.lang.String", "int"), not
      PyArrow names - catalog_table_sample's schema field uses PyArrow names instead
    - 'column_type' is omitted from ordinary columns; its absence means a
      Normal column

    Args:
        context (Context): The MCP context object.
        id (str): Fully qualified id of an enterprise (Core+) session
            (e.g. 'enterprise:prod:12345', as returned by sessions_list or pq_list).
        namespace (str): The catalog namespace containing the table.
        table_name (str): Name of the catalog table whose schema to retrieve.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if the schema was retrieved, False on error.
            - 'id' (str, optional): The session id echoed back if successful.
            - 'namespace' (str, optional): The catalog namespace if successful.
            - 'table_name' (str, optional): The table name if successful.
            - 'schema' (list[dict], optional): One entry per column if successful.
              Each contains 'name' (str) and 'type' (str, Deephaven type name),
              plus one sparse key: 'column_type' (str, e.g. 'Partitioning' or
              'Grouping'; omitted for Normal columns).
            - 'column_count' (int, optional): Number of columns if successful.
            - 'error' (str, optional): Error message if retrieval failed.
            - 'isError' (bool, optional): Present and True if this is an error response.

    Error Scenarios:
        - Non-enterprise session: Returns error if session is not an enterprise (Core+) session
        - Invalid id: Returns error if session doesn't exist or is not accessible
        - Access denied: Returns error if you do not administer the worker behind id
        - Table not found: Returns error if the table cannot be loaded from the catalog
        - Session connection issues: Returns error if unable to communicate with Deephaven server

    Example Successful Response:
        {
            'success': True,
            'id': 'enterprise:prod:analytics',
            'namespace': 'market_data',
            'table_name': 'daily_prices',
            'schema': [
                {'name': 'Date', 'type': 'java.lang.String', 'column_type': 'Partitioning'},
                {'name': 'Price', 'type': 'double'}
            ],
            'column_count': 2
        }

    Example Error Response:
        {'success': False, 'error': "Failed to get schema for catalog table ...", 'isError': True}
    """
    _LOGGER.info(
        f"[mcp_systems_server:catalog_table_schema] Invoked: id={id!r}, "
        f"namespace={namespace!r}, table_name={table_name!r}"
    )

    try:
        session = await get_enterprise_session("catalog_table_schema", context, id)

        _LOGGER.debug(
            f"[mcp_systems_server:catalog_table_schema] Retrieving schema for "
            f"'{namespace}.{table_name}'"
        )
        arrow_meta_table = await queries.get_catalog_meta_table(
            session, namespace, table_name
        )
        result = format_schema_result(
            arrow_meta_table, id, table_name, namespace=namespace
        )

        _LOGGER.info(
            f"[mcp_systems_server:catalog_table_schema] Success: Retrieved schema for "
            f"'{namespace}.{table_name}' ({result['column_count']} columns)"
        )
        return result

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:catalog_table_schema] Failed for session '{id}', "
            f"namespace '{namespace}', table '{table_name}': {e!r}",
            exc_info=True,
        )
        return error_response(
            f"Failed to get schema for catalog table '{namespace}.{table_name}' "
            f"in session '{id}': {describe_exception_chain(e)}"
        )


async def catalog_table_sample(
    context: Context,
    id: str,
    namespace: str,
    table_name: str,
    max_rows: int | None = 100,
    head: bool = True,
    format: TableFormat = "optimize-rendering",
    filters: list[str] | None = None,
) -> dict:
    r"""MCP Tool: Retrieve sample TABULAR DATA from a catalog table in a Deephaven Enterprise (Core+) session.

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
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')
    - 'PQ' is shorthand for Persistent Query
    - This tool only works with enterprise sessions; community sessions do not have catalog tables
    - 'Namespace' refers to a data domain or organizational grouping of tables in the catalog

    Session Scope:
    - Takes a SESSION ID, not a system name, unlike catalog_tables_list and
      catalog_namespaces_list
    - Reading a catalog table's data requires a worker you administer, so this
      runs against a session you own; the shared 'WebClientData' worker refuses
      it for anyone who is not an administrator of that worker
    - Run sessions_list or pq_list to find an id, or create one with
      session_enterprise_create

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
    - Combine with catalog_table_schema to understand table structure before sampling
    - Use 'optimize-rendering' (default) for best table display in AI interfaces
    - Use 'optimize-accuracy' for highest comprehension (markdown-kv format, more tokens)
    - Check 'format' field in response to know actual format used
    - **Partitioned tables** (e.g. DbInternal, System tables) may return 0 rows without a
      partition filter. By default (filters=None), the tool auto-detects the table's partition
      columns and applies a filter for the most recent partition with data. Pass filters=[] to
      disable auto-detection, or pass an explicit filter list to override.

    Args:
        context (Context): The MCP context object.
        id (str): Fully qualified id of an enterprise (Core+) session
            (e.g. 'enterprise:prod:12345', as returned by sessions_list or pq_list).
        namespace (str): The catalog namespace containing the table.
        table_name (str): Name of the catalog table to sample.
        max_rows (int | None, optional): Maximum number of rows to retrieve. Defaults to 100 for safe sampling.
                                         Set to None to retrieve entire table (use with caution for large tables).
        head (bool, optional): Direction of row retrieval. If True (default), retrieve from beginning.
                              If False, retrieve from end (most recent rows for time-series data).
        format (TableFormat, optional): Output format selection. Defaults to "optimize-rendering" for best table display.
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
        filters (list[str] | None, optional): Partition filter behavior. Defaults to None (auto-detect).
                              - None (default): automatically detect the table's partition columns and apply
                                a filter for the most recent partition that has data. This prevents silently
                                empty results on partitioned tables such as DbInternal or System tables.
                              - [] (empty list): no filter applied; skips auto-detection entirely.
                              - ["expr", ...]: apply these explicit Deephaven DQL where-clause filters
                                (e.g. ["Date == `2024-01-15`"]) and skip auto-detection.
                              Partition columns can be discovered via catalog_table_schema
                              (columns with 'column_type': 'Partitioning').

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): Always present. True if sample was retrieved successfully, False on any error.
            - 'id' (str, optional): The session id echoed back if successful.
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
        - Invalid id: Returns error if session doesn't exist or is not accessible
        - Community session: Returns error if session is not an enterprise (Core+) session
        - Access denied: Returns error if you do not administer the worker behind id
        - Invalid namespace: Returns error if namespace doesn't exist in the catalog
        - Invalid table_name: Returns error if table doesn't exist in the namespace
        - Inaccessible catalog entry: table is listed in the catalog but cannot be
          loaded (access controls, no data, type incompatibility, etc.). Returns a
          FetchTableOp or similar error. Catalog listings are a candidate set; handle
          this gracefully.
        - Invalid format: Returns error if format is not one of the supported options
        - Response too large: Returns error if estimated response would exceed the configured response-size limit (default 50MB)
        - Session connection issues: Returns error if unable to communicate with Deephaven server
        - Table access errors: Returns error if table cannot be accessed via historical_table or live_table

    Performance Considerations:
        - Default max_rows of 100 is safe for previewing catalog tables
        - Use csv format or limit max_rows for very wide tables
        - Default optimize-rendering format provides good table display
        - Response size limit: configured response-size limit (default 50MB) to prevent memory issues

    Example Usage:
        # Sample first 100 rows with default format
        Tool: catalog_table_sample
        Parameters: {
            "id": "enterprise:prod:analytics",
            "namespace": "market_data",
            "table_name": "daily_prices"
        }

        # Sample last 50 rows (most recent for time-series)
        Tool: catalog_table_sample
        Parameters: {
            "id": "enterprise:prod:analytics",
            "namespace": "market_data",
            "table_name": "trades",
            "max_rows": 50,
            "head": false
        }

        # Sample with CSV format
        Tool: catalog_table_sample
        Parameters: {
            "id": "enterprise:prod:analytics",
            "namespace": "reference_data",
            "table_name": "symbols",
            "max_rows": 200,
            "format": "csv"
        }
    """
    _LOGGER.info(
        f"[mcp_systems_server:catalog_table_sample] Invoked: id={id!r}, "
        f"namespace={namespace!r}, table_name={table_name!r}, max_rows={max_rows}, head={head}, "
        f"format={format!r}, filters={filters!r}"
    )

    try:
        session = await get_enterprise_session("catalog_table_sample", context, id)

        # Get catalog table data using queries module
        _LOGGER.debug(
            f"[mcp_systems_server:catalog_table_sample] Retrieving catalog table data for '{namespace}.{table_name}'"
        )
        arrow_table, is_complete = await queries.get_catalog_table_data(
            session,
            namespace,
            table_name,
            max_rows=max_rows,
            head=head,
            filters=filters,
        )

        # Check response size before formatting
        row_count = len(arrow_table)
        col_count = len(arrow_table.schema)
        limits = get_response_limits(context, id)
        estimated_size = row_count * col_count * limits.estimated_bytes_per_cell
        size_error = check_response_size(
            f"{namespace}.{table_name}", estimated_size, limits
        )

        if size_error:
            return size_error

        # Build response using helper
        _LOGGER.debug(
            f"[mcp_systems_server:catalog_table_sample] Formatting {row_count} rows in format '{format}'"
        )
        response = build_table_data_response(
            arrow_table,
            is_complete,
            format,
            id,
            table_name=table_name,
            namespace=namespace,
        )

        _LOGGER.info(
            f"[mcp_systems_server:catalog_table_sample] Success: Retrieved {row_count} rows "
            f"from '{namespace}.{table_name}' (is_complete={is_complete}, format={response['format']})"
        )

        return response

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:catalog_table_sample] Failed for session: '{id}', "
            f"namespace: '{namespace}', table: '{table_name}', error: {e!r}",
            exc_info=True,
        )
        return error_response(
            f"Failed to sample catalog table '{namespace}.{table_name}' "
            f"in session '{id}': {describe_exception_chain(e)}"
        )


def register_tools(server: FastMCP) -> None:
    """Register all catalog tools with the given FastMCP server.

    These tools are specific to the DHE server and should NOT be registered
    on the DHC server.

    Args:
        server (FastMCP): The server to register tools with.
    """
    server.tool()(catalog_tables_list)
    server.tool()(catalog_namespaces_list)
    server.tool()(catalog_table_schema)
    server.tool()(catalog_table_sample)
