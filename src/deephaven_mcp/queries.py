"""
Async coroutine helpers for Deephaven session table and environment inspection.

This module provides coroutine-compatible utility functions for querying Deephaven tables and inspecting the Python environment within an active Deephaven session. All functions are asynchronous.

**Functions Provided:**
    - `get_table(session, table_name)`: Retrieve a Deephaven table as a pyarrow.Table snapshot.
    - `get_meta_table(session, table_name)`: Retrieve a table's schema/meta table as a pyarrow.Table snapshot.
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

from deephaven_mcp._exceptions import UnsupportedOperationError
from deephaven_mcp.client import BaseSession

_LOGGER = logging.getLogger(__name__)


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
    is_complete = False

    # Apply row limiting if specified
    if max_rows is not None:
        # Get original table size before applying limits
        original_size = await asyncio.to_thread(lambda: original_table.size)

        if head:
            table = await asyncio.to_thread(lambda: original_table.head(max_rows))
            _LOGGER.debug(
                "[queries:get_table] Limited to first %d rows of table '%s'",
                max_rows,
                table_name,
            )
        else:
            table = await asyncio.to_thread(lambda: original_table.tail(max_rows))
            _LOGGER.debug(
                "[queries:get_table] Limited to last %d rows of table '%s'",
                max_rows,
                table_name,
            )

        # Determine if we got the complete table
        is_complete = original_size <= max_rows
        _LOGGER.debug(
            "[queries:get_table] Original table '%s' has %d total rows",
            table_name,
            original_size,
        )
    else:
        # Full table requested - log warning for safety
        table = original_table
        _LOGGER.warning(
            "[queries:get_table] Retrieving ENTIRE table '%s' - this may cause memory issues for large tables!",
            table_name,
        )
        is_complete = True

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
    if session.programming_language.lower() != "python":
        # TODO: Add support for other programming languages.
        _LOGGER.warning(
            "[queries:get_programming_language_version_table] Unsupported programming language: %s",
            session.programming_language,
        )
        raise UnsupportedOperationError(
            f"get_programming_language_version_table only supports Python sessions, "
            f"but session uses {session.programming_language}."
        )

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
    # Check if the session is a Python session
    if session.programming_language.lower() != "python":
        _LOGGER.warning(
            "[queries:get_pip_packages_table] Unsupported programming language: %s",
            session.programming_language,
        )
        raise UnsupportedOperationError(
            f"get_pip_packages_table only supports Python sessions, "
            f"but session uses {session.programming_language}."
        )

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
    if session.programming_language.lower() != "python":
        # TODO: Add support for other programming languages.
        _LOGGER.warning(
            "[queries:get_dh_versions] Unsupported programming language: %s",
            session.programming_language,
        )
        raise UnsupportedOperationError(
            f"get_dh_versions only supports Python sessions, "
            f"but session uses {session.programming_language}."
        )

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
