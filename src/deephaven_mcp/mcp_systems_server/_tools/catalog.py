"""Catalog MCP Tools - Enterprise Core+ Data Catalog Operations.

Provides MCP tools for querying Deephaven Enterprise (Core+) data catalogs:
- catalog_tables_list: List table names across catalog namespaces
- catalog_namespaces_list: List available catalog namespaces

These tools require Deephaven Enterprise (Core+) and are not available in Community.

Both tools are system-scoped: they name an enterprise system and read the
catalog listing through that system's shared ``WebClientData`` persistent
query, scoped to the Enterprise principal the server is configured with for
that system — not to the MCP caller, so every caller sees the same listing.
"""

import logging

from mcp.server.fastmcp import Context, FastMCP

from deephaven_mcp import queries
from deephaven_mcp.client import describe_exception_chain
from deephaven_mcp.mcp_systems_server._tools.shared import (
    check_response_size,
    error_response,
    get_enterprise_settings,
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
    entry per catalog table.

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
    - Use this to discover what tables exist in the catalog
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
        settings = get_enterprise_settings(context)

        _LOGGER.debug(
            f"[mcp_systems_server:catalog_tables_list] Retrieving catalog entries with filters: {filters}"
        )
        async with get_wcd_system_session(
            "catalog_tables_list", context, system
        ) as access:
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
        settings = get_enterprise_settings(context)

        _LOGGER.debug(
            f"[mcp_systems_server:catalog_namespaces_list] Retrieving namespaces with filters: {filters}"
        )
        async with get_wcd_system_session(
            "catalog_namespaces_list", context, system
        ) as access:
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


def register_tools(server: FastMCP) -> None:
    """Register all catalog tools with the given FastMCP server.

    These tools are specific to the DHE server and should NOT be registered
    on the DHC server.

    Args:
        server (FastMCP): The server to register tools with.
    """
    server.tool()(catalog_tables_list)
    server.tool()(catalog_namespaces_list)
