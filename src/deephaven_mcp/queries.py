"""
Async coroutine helpers for Deephaven session table and environment inspection.

This module provides coroutine-compatible utility functions for querying Deephaven tables and inspecting the Python environment within an active Deephaven session. All functions are asynchronous.

**Functions Provided:**
    - `get_table(session, table_name)`: Retrieve a Deephaven table as a pyarrow.Table snapshot.
    - `get_meta_table(session, table_name)`: Retrieve a table's schema/meta table as a pyarrow.Table snapshot.
    - `get_catalog_table(session)`: Retrieve the catalog table from an enterprise session with optional filtering and namespace extraction.
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

from deephaven_mcp._exceptions import UnsupportedOperationError
from deephaven_mcp.client import BaseSession

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
            "[queries:%s] Unsupported programming language: %s",
            function_name,
            session.programming_language,
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
            "[queries:_apply_filters] Applying %d filter(s) to %s: %s",
            len(filters),
            context_name,
            filters,
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
    is_complete = False

    if max_rows is not None:
        # Get original table size before applying limits
        original_size = await asyncio.to_thread(lambda: table.size)

        if head:
            limited_table = await asyncio.to_thread(lambda: table.head(max_rows))
            _LOGGER.debug(
                "[queries:_apply_row_limit] Limited to first %d rows of %s",
                max_rows,
                context_name,
            )
        else:
            limited_table = await asyncio.to_thread(lambda: table.tail(max_rows))
            _LOGGER.debug(
                "[queries:_apply_row_limit] Limited to last %d rows of %s",
                max_rows,
                context_name,
            )

        # Determine if we got the complete table
        is_complete = original_size <= max_rows
        _LOGGER.debug(
            "[queries:_apply_row_limit] %s has %d total rows",
            context_name.capitalize(),
            original_size,
        )
        return limited_table, is_complete
    else:
        # Full table requested - log warning for safety
        _LOGGER.warning(
            "[queries:_apply_row_limit] Retrieving ENTIRE %s - this may cause memory issues for large tables!",
            context_name,
        )
        return table, True


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
        "[queries:get_table] Retrieving table '%s' from session (max_rows=%s, head=%s)...",
        table_name,
        max_rows,
        head,
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
        "[queries:get_table] Table '%s' converted to Arrow format successfully.",
        table_name,
    )
    return arrow_table, is_complete


async def get_meta_table(session: BaseSession, table_name: str) -> pyarrow.Table:
    """
    Asynchronously retrieve the meta table (schema/metadata) for a Deephaven table as a pyarrow.Table snapshot.

    This helper uses the async methods of BaseSession to open the specified table, access its meta_table property, and convert it to a pyarrow.Table.

    Args:
        session (BaseSession): An active Deephaven session. Must not be closed.
        table_name (str): The name of the table to retrieve the meta table for.

    Returns:
        pyarrow.Table: The meta table containing schema/metadata information for the specified table.

    Raises:
        Exception: If the table or its meta table does not exist, the session is closed, or if conversion to Arrow fails.

    Note:
        - Logging is performed at DEBUG level for entry, exit, and error tracing.
        - This function is intended for internal use only.
    """
    _LOGGER.debug(
        "[queries:get_meta_table] Retrieving meta table for '%s' from session...",
        table_name,
    )
    table = await session.open_table(table_name)
    meta_table = await asyncio.to_thread(lambda: table.meta_table)
    arrow_meta_table = await asyncio.to_thread(meta_table.to_arrow)
    _LOGGER.debug(
        "[queries:get_meta_table] Meta table for '%s' retrieved successfully.",
        table_name,
    )
    return arrow_meta_table


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

    script = textwrap.dedent(
        """
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
        """
    )
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
        "[queries:get_programming_language_version] Retrieved version: %s",
        version_str,
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

    script = textwrap.dedent(
        """
        from deephaven import new_table
        from deephaven.column import string_col
        import importlib.metadata as importlib_metadata

        def _make_pip_packages_table():
            names = []
            versions = []
            for dist in importlib_metadata.distributions():
                names.append(dist.metadata['Name'])
                versions.append(dist.version)
            return new_table([
                string_col('Package', names),
                string_col('Version', versions),
            ])

        _pip_packages_table = _make_pip_packages_table()
        """
    )
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
        "[queries:get_dh_versions] Found versions: deephaven-core=%s, deephaven_coreplus_worker=%s",
        dh_core_version,
        dh_coreplus_version,
    )
    return dh_core_version, dh_coreplus_version


async def get_catalog_table(
    session: BaseSession,
    *,
    max_rows: int | None,
    filters: list[str] | None = None,
    distinct_namespaces: bool,
) -> tuple[pyarrow.Table, bool]:
    """
    Asynchronously retrieve the catalog table from a Deephaven Enterprise (Core+) session.

    The catalog table contains metadata about tables accessible via the `deephaven_enterprise.database`
    package (the `db` variable) in an enterprise session. This includes tables that can be accessed
    using methods like `db.live_table(namespace, table_name)` or `db.historical_table(namespace, table_name)`.
    The catalog includes table names, namespaces, schemas, and other descriptive information. This
    function is only available for enterprise sessions (CorePlusSession).
    
    For more information, see:
    - https://deephaven.io
    - https://docs.deephaven.io/pycoreplus/latest/worker/code/deephaven_enterprise.database.html

    Args:
        session (BaseSession): An active Deephaven enterprise session. Must be a CorePlusSession.
        max_rows (int | None): Maximum number of rows to retrieve. Must be specified as keyword argument.
                               Set to None to retrieve the entire catalog (use with caution for large catalogs).
                               Set to a positive integer to limit rows (recommended for production use).
        filters (list[str] | None): Optional list of Deephaven where clause expressions to filter catalog results.
                                    Multiple filters are combined with AND logic. Filters use Deephaven query
                                    language syntax with backticks (`) for string literals.
        distinct_namespaces (bool): Required. If True, returns only distinct namespaces (sorted) instead of full catalog.
                                   Filters are applied after selecting distinct namespaces. Must be explicitly specified.

    Returns:
        tuple[pyarrow.Table, bool]: A tuple containing:
            - pyarrow.Table: The catalog table (or filtered subset) as a pyarrow.Table snapshot
            - bool: True if the entire catalog was retrieved, False if only a subset was returned

    Raises:
        UnsupportedOperationError: If the session is not an enterprise (Core+) session.
        Exception: If the catalog cannot be retrieved, filters are invalid, or conversion to Arrow fails.

    Warning:
        Setting max_rows=None on large enterprise deployments with thousands of tables can cause
        memory exhaustion. Always use a reasonable row limit in production environments.

    Examples:
        # Get first 1000 catalog entries
        catalog, is_complete = await get_catalog_table(
            session, max_rows=1000, distinct_namespaces=False
        )

        # Filter by namespace
        catalog, is_complete = await get_catalog_table(
            session,
            max_rows=1000,
            filters=["Namespace = `market_data`"],
            distinct_namespaces=False,
        )

        # Filter by table name pattern (case-sensitive contains)
        catalog, is_complete = await get_catalog_table(
            session,
            max_rows=1000,
            filters=["TableName.contains(`price`)"],
            distinct_namespaces=False,
        )

        # Multiple filters (AND logic)
        catalog, is_complete = await get_catalog_table(
            session,
            max_rows=1000,
            filters=["Namespace = `market_data`", "TableName.contains(`daily`)"],
            distinct_namespaces=False,
        )

        # Get distinct namespaces only
        namespaces, is_complete = await get_catalog_table(
            session, max_rows=1000, distinct_namespaces=True
        )

        # Full catalog retrieval (dangerous for large deployments)
        catalog, is_complete = await get_catalog_table(
            session, max_rows=None, distinct_namespaces=False
        )

    Note:
        - max_rows must be specified as a keyword argument to force intentional usage
        - Filters use Deephaven query language syntax (see https://deephaven.io/core/docs/how-to-guides/use-filters/)
        - String literals in filters must use backticks (`), not single or double quotes
        - This function is intended for internal use by MCP tools
        - Only works with enterprise (Core+) sessions that have catalog_table() method
    """
    from deephaven_mcp.client import CorePlusSession

    _LOGGER.debug(
        "[queries:get_catalog_table] Retrieving catalog table from enterprise session (max_rows=%s, filters=%s)...",
        max_rows,
        filters,
    )

    # Check if the session is an enterprise session
    if not isinstance(session, CorePlusSession):
        _LOGGER.error(
            "[queries:get_catalog_table] Session is not an enterprise (Core+) session: %s",
            type(session).__name__,
        )
        raise UnsupportedOperationError(
            f"get_catalog_table only supports enterprise (Core+) sessions, "
            f"but session is {type(session).__name__}."
        )

    # Get the catalog table
    catalog_table = await session.catalog_table()
    _LOGGER.debug("[queries:get_catalog_table] Catalog table retrieved successfully.")

    # Handle distinct namespaces case
    if distinct_namespaces:
        _LOGGER.debug("[queries:get_catalog_table] Extracting distinct namespaces...")
        # Step 1: Select distinct namespaces
        catalog_table = await asyncio.to_thread(
            lambda: catalog_table.select_distinct("Namespace")
        )
        # Step 2: Sort namespaces
        catalog_table = await asyncio.to_thread(
            lambda: catalog_table.sort("Namespace")
        )
        _LOGGER.debug("[queries:get_catalog_table] Distinct namespaces extracted and sorted.")

    # Determine table type for logging
    table_type = "namespace table" if distinct_namespaces else "catalog table"

    # Apply filters if provided (works for both full catalog and distinct namespaces)
    catalog_table = await _apply_filters(catalog_table, filters, context_name=table_type)

    # Apply row limiting using helper function (always from head for catalog tables)
    catalog_table, is_complete = await _apply_row_limit(
        catalog_table,
        max_rows,
        head=True,
        context_name=table_type,
    )

    # Convert to Arrow format
    arrow_table = await asyncio.to_thread(catalog_table.to_arrow)

    _LOGGER.debug(
        "[queries:get_catalog_table] Catalog table converted to Arrow format successfully."
    )
    return arrow_table, is_complete
