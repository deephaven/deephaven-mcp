"""
Async coroutine helpers for Deephaven session table and environment inspection.

This module provides coroutine-compatible utility functions for querying Deephaven tables and inspecting the Python environment within an active Deephaven session. All functions are asynchronous.

**Functions Provided:**
    - `get_table(session, table_name)`: Retrieve a Deephaven table as a pyarrow.Table snapshot.
    - `get_meta_table(session, table_name)`: Retrieve a table's schema/meta table as a pyarrow.Table snapshot.
    - `get_pip_packages_table(session)`: Get a table of installed pip packages as a pyarrow.Table.
    - `get_programming_language_version_table(session)`: Get a table with Python version information as a pyarrow.Table.
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
from deephaven_mcp.client._session import BaseSession

_LOGGER = logging.getLogger(__name__)


async def get_table(session: BaseSession, table_name: str) -> pyarrow.Table:
    """
    Asynchronously retrieve a Deephaven table as a pyarrow.Table snapshot from a live session.

    This helper uses the async methods of BaseSession to open the specified table and convert it to a pyarrow.Table, suitable for further processing or inspection.

    Args:
        session (BaseSession): An active Deephaven session. Must not be closed.
        table_name (str): The name of the table to retrieve.

    Returns:
        pyarrow.Table: The requested table as a pyarrow.Table snapshot.

    Raises:
        Exception: If the table does not exist, the session is closed, or if conversion to Arrow fails.

    Note:
        - Logging is performed at DEBUG level for entry, exit, and error tracing.
        - This function is intended for internal use only.
    """
    _LOGGER.debug(
        "[queries:get_table] Retrieving table '%s' from session...", table_name
    )
    table = await session.open_table(table_name)
    arrow_table = await asyncio.to_thread(table.to_arrow)
    _LOGGER.debug("[queries:get_table] Table '%s' retrieved successfully.", table_name)
    return arrow_table


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
        from deephaven import new_table, string_col, int_col
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
    arrow_table = await get_table(session, "_python_version_table")
    _LOGGER.debug(
        "[queries:get_programming_language_version_table] Table '_python_version_table' retrieved successfully."
    )
    return arrow_table


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
        from deephaven import new_table, string_col
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
    arrow_table = await get_table(session, "_pip_packages_table")
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
