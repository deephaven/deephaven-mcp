"""
Async coroutine helpers for Deephaven session table and environment inspection.

This module provides coroutine-compatible utility functions for querying Deephaven tables and inspecting the Python environment within an active Deephaven session. All functions are asynchronous.

**Functions Provided:**
    - `get_table(session, table_name)`: Retrieve a Deephaven table as a pyarrow.Table snapshot.
    - `get_session_meta_table(session, table_name)`: Retrieve a session table's schema/meta table as a pyarrow.Table snapshot.
    - `get_catalog_table(session)`: Retrieve one user's catalog table via the WebClientData widget, with optional filtering and namespace extraction.
    - `get_pip_packages_table(session)`: Get a table of installed pip packages as a pyarrow.Table.
    - `get_programming_language_version_table(session)`: Get a table with Python version information as a pyarrow.Table.
    - `get_programming_language_version(session)`: Get the programming language version string from a Deephaven session.
    - `get_dh_versions(session)`: Get the installed Deephaven Core and Core+ version strings from the session's pip environment.

**Notes:**
- All functions are async coroutines and must be awaited.
- Logging is performed at DEBUG level for traceability of session queries and errors.
- Exceptions are raised for invalid sessions, missing tables, script failures, or data conversion errors. Callers should handle these exceptions as appropriate for internal server/tool logic.

"""

import asyncio
import logging
import textwrap

import pyarrow
from pydeephaven.table import Table

from deephaven_mcp._exceptions import InternalError, UnsupportedOperationError
from deephaven_mcp.client import (
    BaseSession,
    WebClientDataTable,
    fetch_web_client_data_table,
)

_LOGGER = logging.getLogger(__name__)


# ===== Private Helper Functions =====


def _validate_python_session(function_name: str, session: BaseSession) -> None:
    """
    Validate that a session is a Python session.

    Args:
        function_name (str): Name of the calling function for error messages.
        session (BaseSession): The session to validate.

    Raises:
        UnsupportedOperationError: If the session is not a Python session.

    Note:
        This is a private helper function for internal use only.
    """
    if session.programming_language.lower() != "python":
        _LOGGER.warning(
            f"[queries:{function_name}] Unsupported programming language: {session.programming_language}"
        )
        raise UnsupportedOperationError(
            f"{function_name} only supports Python sessions, "
            f"but session uses {session.programming_language}."
        )


async def _apply_filters(
    table: Table,
    filters: list[str] | None,
    *,
    context_name: str,
) -> Table:
    """
    Apply where clause filters to a Deephaven table.

    This helper consolidates the common pattern of applying filters with appropriate logging.

    Args:
        table (Table): The Deephaven table to filter (must have .where() method).
        filters (list[str] | None): List of Deephaven where clause expressions to apply.
                                    Multiple filters are combined with AND logic.
                                    None or empty list means no filtering.
        context_name (str): Context description for logging (e.g., "catalog table", "namespace table").

    Returns:
        Table: The filtered table (or original if no filters provided).

    Note:
        This is a private helper function for internal use only.
    """
    if filters:
        _LOGGER.debug(
            f"[queries:_apply_filters] Applying {len(filters)} filter(s) to {context_name}: {filters}"
        )
        table = await asyncio.to_thread(table.where, filters)
        _LOGGER.debug("[queries:_apply_filters] Filters applied successfully.")
    else:
        _LOGGER.debug("[queries:_apply_filters] No filters to apply.")

    return table


async def _apply_row_limit(
    table: Table,
    max_rows: int | None,
    *,
    head: bool = True,
    context_name: str,
) -> tuple[Table, bool]:
    """
    Apply row limiting to a Deephaven table and determine if result is complete.

    This helper consolidates the common pattern of limiting table rows using head() or tail(),
    checking if the full table was retrieved, and logging appropriate warnings.

    Uses a probe approach (requesting max_rows+1 rows then checking that probe table's .size)
    to reliably detect completeness without relying on the original table's .size, which can
    return unreliable values for live/ticking tables before they are fully populated.

    Args:
        table (Table): The Deephaven table to limit (must have .size, .head(), .tail() methods).
        max_rows (int | None): Maximum number of rows to retrieve.
                               None means retrieve entire table (logs warning).
        head (bool): If True, use head() to get first rows. If False, use tail() for last rows.
                    Ignored when max_rows=None. Default is True.
        context_name (str): Context description for logging (e.g., "table 'my_table'", "catalog table").

    Returns:
        tuple[Table, bool]: A tuple containing:
            - Table: The limited table (or original if max_rows=None)
            - bool: True if entire table was retrieved, False if truncated

    Note:
        This is a private helper function for internal use only.
        The returned table is NOT converted to Arrow format - caller must do that.
    """
    if max_rows is not None:
        # Probe with max_rows+1: the bounded probe table's .size is reliable even for live tables,
        # unlike the original table's .size which may be a partial count before full population.
        probe_n = max_rows + 1
        if head:
            probe_table = await asyncio.to_thread(lambda: table.head(probe_n))
        else:
            probe_table = await asyncio.to_thread(lambda: table.tail(probe_n))

        probe_size = await asyncio.to_thread(lambda: probe_table.size)
        if probe_size is None:
            raise InternalError(
                f"[queries:_apply_row_limit] Table .size returned None for {context_name}"
            )

        if probe_size > max_rows:
            # More rows exist — apply the actual limit
            if head:
                limited_table = await asyncio.to_thread(lambda: table.head(max_rows))
                _LOGGER.debug(
                    f"[queries:_apply_row_limit] Limited to first {max_rows} rows of {context_name}"
                )
            else:
                limited_table = await asyncio.to_thread(lambda: table.tail(max_rows))
                _LOGGER.debug(
                    f"[queries:_apply_row_limit] Limited to last {max_rows} rows of {context_name}"
                )
            is_complete = False
        else:
            # probe_size <= max_rows: all rows fit within the limit
            limited_table = probe_table
            is_complete = True
            _LOGGER.debug(
                f"[queries:_apply_row_limit] {context_name.capitalize()} complete: {probe_size} rows"
            )

        return limited_table, is_complete
    else:
        # Full table requested - log warning for safety
        _LOGGER.warning(
            f"[queries:_apply_row_limit] Retrieving ENTIRE {context_name} - this may cause memory issues for large tables!"
        )
        return table, True


async def _snapshot_filtered(
    table: Table,
    *,
    filters: list[str] | None,
    max_rows: int | None,
    head: bool = True,
    context_name: str,
) -> tuple[pyarrow.Table, bool]:
    """
    Filter a Deephaven table, cap its rows, and snapshot it to Arrow.

    This helper is the shared tail of every table-listing query: filters are
    evaluated server-side, the row cap is applied, and the result is converted
    to Arrow in one step.

    Args:
        table (Table): The Deephaven table to snapshot.
        filters (list[str] | None): Deephaven where clause expressions, combined
                                    with AND logic. None or empty means no filtering.
        max_rows (int | None): Maximum number of rows to retrieve. None means the
                               entire table (logs a warning).
        head (bool): If True, take rows from the start; if False, from the end.
                     Ignored when max_rows is None. Default is True.
        context_name (str): Context description for logging (e.g., "catalog table").

    Returns:
        tuple[pyarrow.Table, bool]: A tuple containing:
            - pyarrow.Table: The filtered, row-capped snapshot
            - bool: True if the entire (filtered) table was retrieved, False if truncated

    Note:
        This is a private helper function for internal use only.
    """
    table = await _apply_filters(table, filters, context_name=context_name)
    limited_table, is_complete = await _apply_row_limit(
        table, max_rows, head=head, context_name=context_name
    )
    arrow_table = await asyncio.to_thread(limited_table.to_arrow)
    _LOGGER.debug(
        f"[queries:_snapshot_filtered] {context_name.capitalize()} converted to Arrow "
        f"({arrow_table.num_rows} rows, is_complete={is_complete})"
    )
    return arrow_table, is_complete


# ===== Public API Functions =====


async def get_table(
    session: BaseSession, table_name: str, *, max_rows: int | None, head: bool = True
) -> tuple[pyarrow.Table, bool]:
    """
    Asynchronously retrieve a Deephaven table as a pyarrow.Table snapshot from a live session.

    This helper uses the async methods of BaseSession to open the specified table and convert it to a pyarrow.Table,
    suitable for further processing or inspection. For safety with large tables, the max_rows parameter is required
    to force intentional usage.

    Args:
        session (BaseSession): An active Deephaven session. Must not be closed.
        table_name (str): The name of the table to retrieve.
        max_rows (int | None): Maximum number of rows to retrieve. Must be specified as keyword argument.
                               Set to None to retrieve the entire table (use with extreme caution for large tables).
                               Set to a positive integer to limit rows (recommended for production use).
        head (bool): If True and max_rows is not None, retrieve rows from the beginning using head().
                    If False and max_rows is not None, retrieve rows from the end using tail().
                    This parameter is ignored when max_rows=None (full table retrieval). Default is True.

    Returns:
        tuple[pyarrow.Table, bool]: A tuple containing:
            - pyarrow.Table: The requested table (or subset) as a pyarrow.Table snapshot
            - bool: True if the entire table was retrieved, False if only a subset was returned

    Raises:
        Exception: If the table does not exist, the session is closed, or if conversion to Arrow fails.

    Warning:
        Setting max_rows=None on large tables (millions/billions of rows) can cause memory exhaustion and system crashes.
        Always use a reasonable row limit in production environments.

    Examples:
        # Safe usage with row limit from beginning
        table, is_complete = await get_table(session, "my_table", max_rows=1000)

        # Get last 1000 rows
        table, is_complete = await get_table(session, "my_table", max_rows=1000, head=False)

        # Full table retrieval (dangerous for large tables)
        table, is_complete = await get_table(session, "small_table", max_rows=None)  # is_complete will be True

    Note:
        - max_rows must be specified as a keyword argument to force intentional usage
        - head parameter is ignored when max_rows=None
        - Logging is performed at DEBUG level for entry, exit, and error tracing
        - This function is intended for internal use only
    """
    _LOGGER.debug(
        f"[queries:get_table] Retrieving table '{table_name}' from session (max_rows={max_rows}, head={head})..."
    )

    # Open the table
    original_table = await session.open_table(table_name)

    # Apply row limiting using helper function
    table, is_complete = await _apply_row_limit(
        original_table,
        max_rows,
        head=head,
        context_name=f"table '{table_name}'",
    )

    # Convert to Arrow format (single conversion point)
    arrow_table = await asyncio.to_thread(table.to_arrow)

    _LOGGER.debug(
        f"[queries:get_table] Table '{table_name}' converted to Arrow format successfully."
    )
    return arrow_table, is_complete


async def _extract_meta_table(table: Table, context: str) -> pyarrow.Table:
    """
    Extract meta_table from a Deephaven table and convert to Arrow format.

    This internal helper consolidates the common pattern of extracting and converting
    a table's meta_table to Arrow format, used by both session and catalog meta table functions.

    Args:
        table (Table): A Deephaven table object with a meta_table property.
        context (str): Context string for logging (e.g., table name or namespace.table).

    Returns:
        pyarrow.Table: The meta table containing schema/metadata information.

    Raises:
        Exception: If the meta table cannot be accessed or converted to Arrow format.

    Note:
        This is an internal helper function used by get_session_meta_table.
    """
    meta_table = await asyncio.to_thread(lambda: table.meta_table)
    arrow_meta_table = await asyncio.to_thread(meta_table.to_arrow)
    _LOGGER.debug(
        f"[queries:_extract_meta_table] Meta table for '{context}' retrieved successfully."
    )
    return arrow_meta_table


async def get_session_meta_table(
    session: BaseSession, table_name: str
) -> pyarrow.Table:
    """
    Asynchronously retrieve the meta table (schema/metadata) for a Deephaven session table as a pyarrow.Table.

    This function opens a table from the session's namespace and retrieves its meta table.
    Use this for tables that exist in the session (created via scripts, queries, or bound tables).

    Args:
        session (BaseSession): An active Deephaven session. Must not be closed.
        table_name (str): The name of the table to retrieve the meta table for.

    Returns:
        pyarrow.Table: The meta table containing schema/metadata information for the specified table.
                      Each row represents a column with fields like 'Name' and 'DataType'.

    Raises:
        Exception: If the table does not exist, the session is closed, or if meta table retrieval fails.

    Note:
        - Logging is performed at DEBUG level for entry, exit, and error tracing
        - This function is intended for internal use only
    """
    _LOGGER.debug(
        f"[queries:get_session_meta_table] Retrieving meta table for session table '{table_name}'..."
    )
    table = await session.open_table(table_name)
    return await _extract_meta_table(table, table_name)


async def get_programming_language_version_table(session: BaseSession) -> pyarrow.Table:
    """
    Asynchronously retrieve Python version information from a Deephaven session as a pyarrow.Table.

    This function runs a Python script in the given session to create a temporary table with Python version details,
    then retrieves it as a pyarrow.Table snapshot. Useful for environment inspection and compatibility checking.

    Args:
        session (BaseSession): An active Deephaven session in which to run the script and retrieve the resulting table.

    Returns:
        pyarrow.Table: A table with columns for Python version information, including:
            - 'Version' (str): The short Python version string (e.g., '3.9.7')
            - 'Major' (int): Major version number
            - 'Minor' (int): Minor version number
            - 'Micro' (int): Micro/patch version number
            - 'Implementation' (str): Python implementation (e.g., 'CPython')
            - 'FullVersion' (str): The complete Python version string with build info

    Raises:
        UnsupportedOperationError: If the session is not a Python session.
        Exception: If the script fails to execute, the table cannot be retrieved, or conversion to Arrow fails.

    Example:
        >>> arrow_table = await get_programming_language_version_table(session)

    Note:
        - The temporary table '_python_version_table' is created in the session and is not automatically deleted.
        - Logging is performed at DEBUG level for script execution and table retrieval.
        - Currently only supports Python sessions. Support for other programming languages may be added in the future.
    """
    _LOGGER.debug(
        "[queries:get_programming_language_version_table] Retrieving Python version information from session..."
    )

    # Check if the session is a Python session
    # TODO: Add support for other programming languages.
    _validate_python_session("get_programming_language_version_table", session)

    script = textwrap.dedent("""
        from deephaven import new_table
        from deephaven.column import string_col, int_col
        import sys
        import platform

        def _make_python_version_table():
            version_info = sys.version_info
            version_str = sys.version.split()[0]
            implementation = platform.python_implementation()
            
            return new_table([
                string_col('Version', [version_str]),
                int_col('Major', [version_info.major]),
                int_col('Minor', [version_info.minor]),
                int_col('Micro', [version_info.micro]),
                string_col('Implementation', [implementation]),
                string_col('FullVersion', [sys.version]),
            ])

        _python_version_table = _make_python_version_table()
        """)
    _LOGGER.debug(
        "[queries:get_programming_language_version_table] Running Python version script in session..."
    )
    await session.run_script(script)
    _LOGGER.debug(
        "[queries:get_programming_language_version_table] Script executed successfully."
    )
    arrow_table, _ = await get_table(session, "_python_version_table", max_rows=None)
    _LOGGER.debug(
        "[queries:get_programming_language_version_table] Table '_python_version_table' retrieved successfully."
    )
    return arrow_table


async def get_programming_language_version(session: BaseSession) -> str:
    """
    Asynchronously retrieve the programming language version string from a Deephaven session.

    This function gets the programming language version table and extracts the version string.

    Args:
        session (BaseSession): An active Deephaven session.

    Returns:
        str: The programming language version string (e.g., "3.9.7").

    Raises:
        UnsupportedOperationError: If the session is not a Python session.
        Exception: If the version information cannot be retrieved.
    """
    _LOGGER.debug(
        "[queries:get_programming_language_version] Retrieving programming language version..."
    )
    version_table = await get_programming_language_version_table(session)

    # Extract the version string from the first row of the Version column
    version_column = version_table.column("Version")
    version_str = str(version_column[0].as_py())

    _LOGGER.debug(
        f"[queries:get_programming_language_version] Retrieved version: {version_str}"
    )
    return version_str


async def get_pip_packages_table(session: BaseSession) -> pyarrow.Table:
    """
    Asynchronously retrieve a table of installed pip packages from a Deephaven session as a pyarrow.Table.

    This function runs a Python script in the given session to create a temporary table listing all installed pip packages and their versions, then retrieves it as a pyarrow.Table snapshot. Useful for environment inspection and version reporting.

    Args:
        session (BaseSession): An active Deephaven session in which to run the script and retrieve the resulting table.

    Returns:
        pyarrow.Table: A table with columns 'Package' (str) and 'Version' (str), listing all installed pip packages.

    Raises:
        UnsupportedOperationError: If the session is not a Python session.
        Exception: If the script fails to execute, the table cannot be retrieved, or conversion to Arrow fails.

    Example:
        >>> arrow_table = await get_pip_packages_table(session)

    Note:
        - The temporary table '_pip_packages_table' is created in the session and is not automatically deleted.
        - Logging is performed at DEBUG level for script execution and table retrieval.
        - Currently only supports Python sessions. Support for other programming languages may be added in the future.
    """
    _LOGGER.debug(
        "[queries:get_pip_packages_table] Retrieving pip packages from session..."
    )

    # Check if the session is a Python session
    # TODO: Add support for other programming languages.
    _validate_python_session("get_pip_packages_table", session)

    script = textwrap.dedent("""
        from deephaven import new_table
        from deephaven.column import string_col
        import importlib.metadata as importlib_metadata

        def _make_pip_packages_table():
            seen = {}
            for dist in importlib_metadata.distributions():
                name = dist.metadata['Name']
                if name not in seen:
                    seen[name] = dist.version
            return new_table([
                string_col('Package', list(seen.keys())),
                string_col('Version', list(seen.values())),
            ])

        _pip_packages_table = _make_pip_packages_table()
        """)
    _LOGGER.debug(
        "[queries:get_pip_packages_table] Running pip packages script in session..."
    )
    await session.run_script(script)
    _LOGGER.debug("[queries:get_pip_packages_table] Script executed successfully.")
    arrow_table, _ = await get_table(session, "_pip_packages_table", max_rows=None)
    _LOGGER.debug(
        "[queries:get_pip_packages_table] Table '_pip_packages_table' retrieved successfully."
    )
    return arrow_table


async def get_dh_versions(session: BaseSession) -> tuple[str | None, str | None]:
    """
    Asynchronously retrieve the Deephaven Core and Core+ version strings installed in a given Deephaven session.

    This function uses `get_pip_packages_table` to obtain a table of installed pip packages, then parses it to find the versions of 'deephaven-core' and 'deephaven_coreplus_worker'.

    Args:
        session (BaseSession): An active Deephaven session object.

    Returns:
        tuple[str | None, str | None]:
            - Index 0: The version string for Deephaven Core, or None if not found.
            - Index 1: The version string for Deephaven Core+, or None if not found.

    Raises:
        UnsupportedOperationError: If the session is not a Python session.
        Exception: If the pip packages table cannot be retrieved.

    Note:
        - Returns (None, None) if neither package is found in the session environment.
        - Logging is performed at DEBUG level for entry, exit, and version reporting.
        - Currently only supports Python sessions. Support for other programming languages may be added in the future.
    """
    # Check if the session is a Python session
    # TODO: Add support for other programming languages.
    _validate_python_session("get_dh_versions", session)

    _LOGGER.debug(
        "[queries:get_dh_versions] Retrieving Deephaven Core and Core+ versions from session..."
    )
    arrow_table = await get_pip_packages_table(session)
    if arrow_table is None:
        _LOGGER.debug(
            "[queries:get_dh_versions] No pip packages table found. Returning (None, None)."
        )
        return None, None

    packages_dict = arrow_table.to_pydict()
    packages = zip(packages_dict["Package"], packages_dict["Version"], strict=False)

    dh_core_version = None
    dh_coreplus_version = None

    for pkg_name, version in packages:
        pkg_name_lower = pkg_name.lower()
        if pkg_name_lower == "deephaven-core" and dh_core_version is None:
            dh_core_version = version
        elif (
            pkg_name_lower == "deephaven_coreplus_worker"
            and dh_coreplus_version is None
        ):
            dh_coreplus_version = version
        if dh_core_version and dh_coreplus_version:
            break

    _LOGGER.debug(
        f"[queries:get_dh_versions] Found versions: deephaven-core={dh_core_version}, deephaven_coreplus_worker={dh_coreplus_version}"
    )
    return dh_core_version, dh_coreplus_version


async def get_catalog_table(
    session: BaseSession,
    *,
    operate_as: str,
    timeout_seconds: float,
    max_rows: int | None,
    filters: list[str] | None = None,
    distinct_namespaces: bool,
) -> tuple[pyarrow.Table, bool]:
    """
    Asynchronously retrieve the catalog table for one user from a WebClientData session.

    The catalog table lists the tables accessible via the `deephaven_enterprise.database`
    package (the `db` variable), e.g. `db.live_table(namespace, table_name)` or
    `db.historical_table(namespace, table_name)`. It is fetched through the
    ``WebClientData`` table-factory widget, which builds it with ``operate_as``'s
    ACLs applied. That identity is the server's configured Enterprise principal
    for the system, not the MCP caller, so every caller sees the same listing.
    Fetching the
    catalog directly off a shared worker instead would be refused for anyone who
    does not administer that worker.

    For more information, see:
    - https://deephaven.io
    - https://docs.deephaven.io/pycoreplus/latest/worker/code/deephaven_enterprise.database.html

    Args:
        session (BaseSession): An active session connected to the system's
                               ``WebClientData`` persistent query. Must be a
                               CorePlusSession.
        operate_as (str): Identity whose ACLs the catalog is built with.
        timeout_seconds (float): Budget for the widget request/response round-trip.
        max_rows (int | None): Maximum number of rows to retrieve. Must be specified as keyword argument.
                               Set to None to retrieve the entire catalog (use with caution for large catalogs).
                               Set to a positive integer to limit rows (recommended for production use).
        filters (list[str] | None): Optional list of Deephaven where clause expressions to filter catalog results.
                                    Multiple filters are combined with AND logic. Filters use Deephaven query
                                    language syntax with backticks (`) for string literals.
        distinct_namespaces (bool): Required. If True, returns only distinct namespaces (sorted) instead of full catalog.
                                   Filters are applied to the full catalog before the namespaces are extracted, so a
                                   filter may reference TableName. Must be explicitly specified.

    Returns:
        tuple[pyarrow.Table, bool]: A tuple containing:
            - pyarrow.Table: The catalog table (or filtered subset) as a pyarrow.Table snapshot
            - bool: True if the entire catalog was retrieved, False if only a subset was returned

    Raises:
        UnsupportedOperationError: If the session is not an enterprise (Core+) session.
        WebClientDataError: If the widget cannot produce the catalog table.
        Exception: If filters are invalid or conversion to Arrow fails.

    Warning:
        Setting max_rows=None on large enterprise deployments with thousands of tables can cause
        memory exhaustion. Always use a reasonable row limit in production environments.

    Examples:
        # Get first 1000 catalog entries
        catalog, is_complete = await get_catalog_table(
            session,
            operate_as="jdoe",
            timeout_seconds=30.0,
            max_rows=1000,
            distinct_namespaces=False,
        )

        # Filter by namespace
        catalog, is_complete = await get_catalog_table(
            session,
            operate_as="jdoe",
            timeout_seconds=30.0,
            max_rows=1000,
            filters=["Namespace = `market_data`"],
            distinct_namespaces=False,
        )

        # Get distinct namespaces only
        namespaces, is_complete = await get_catalog_table(
            session,
            operate_as="jdoe",
            timeout_seconds=30.0,
            max_rows=1000,
            distinct_namespaces=True,
        )

    Note:
        - max_rows must be specified as a keyword argument to force intentional usage
        - Filters use Deephaven query language syntax (see https://deephaven.io/core/docs/how-to-guides/use-filters/)
        - String literals in filters must use backticks (`), not single or double quotes
        - This function is intended for internal use by MCP tools
        - Catalog columns are ``Namespace``, ``NamespaceSet``, and ``TableName``
    """
    from deephaven_mcp.client import CorePlusSession

    _LOGGER.debug(
        f"[queries:get_catalog_table] Retrieving catalog table via WebClientData "
        f"(operate_as={operate_as!r}, max_rows={max_rows}, filters={filters})..."
    )

    # Check if the session is an enterprise session
    if not isinstance(session, CorePlusSession):
        _LOGGER.error(
            f"[queries:get_catalog_table] Session is not an enterprise (Core+) session: {type(session).__name__}"
        )
        raise UnsupportedOperationError(
            f"get_catalog_table only supports enterprise (Core+) sessions, "
            f"but session is {type(session).__name__}."
        )

    catalog_table = await fetch_web_client_data_table(
        session,
        WebClientDataTable.CATALOG,
        operate_as=operate_as,
        timeout_seconds=timeout_seconds,
    )
    _LOGGER.debug("[queries:get_catalog_table] Catalog table retrieved successfully.")

    # Determine table type for logging
    table_type = "namespace table" if distinct_namespaces else "catalog table"

    # Filters run before the namespace projection: documented expressions
    # reference TableName, which select_distinct("Namespace") would drop.
    catalog_table = await _apply_filters(
        catalog_table, filters, context_name=table_type
    )

    if distinct_namespaces:
        _LOGGER.debug("[queries:get_catalog_table] Extracting distinct namespaces...")
        catalog_table = await asyncio.to_thread(
            lambda: catalog_table.select_distinct("Namespace")
        )
        catalog_table = await asyncio.to_thread(lambda: catalog_table.sort("Namespace"))
        _LOGGER.debug(
            "[queries:get_catalog_table] Distinct namespaces extracted and sorted."
        )

    return await _snapshot_filtered(
        catalog_table,
        filters=None,
        max_rows=max_rows,
        head=True,
        context_name=table_type,
    )
