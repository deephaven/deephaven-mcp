"""
Async coroutine helpers for Deephaven session table and environment inspection (internal use).

This private module provides coroutine-compatible utility functions for querying
Deephaven tables and installed pip packages within an active Deephaven session.
All functions are async, offloading blocking operations to threads, and are intended for internal use only.

Functions:
    - get_table(session, table_name): Retrieve a Deephaven table as a pyarrow.Table snapshot.
    - get_meta_table(session, table_name): Retrieve a table's schema/meta table as a pyarrow.Table snapshot.
    - get_pip_packages_table(session): Get a table of installed pip packages as a pyarrow.Table.
    - get_dh_versions(session): Get the installed Deephaven Core and Core+ version strings from the session's pip environment.

All functions may raise exceptions if the session is invalid, the table does not exist, or if there are issues running scripts in the session.
Not part of the public API; intended for use by the Deephaven MCP sessions package only.
"""

import asyncio
import logging
import textwrap

import pyarrow
from pydeephaven import Session

_LOGGER = logging.getLogger(__name__)


async def get_table(session: Session, table_name: str) -> pyarrow.Table:
    """
    Asynchronously retrieve a Deephaven table as a pyarrow.Table snapshot.

    This function runs blocking Deephaven API calls in a background thread. The returned Arrow table is a snapshot of the data at the time of the call.

    Args:
        session (Session): The Deephaven session to retrieve the table from. Must be alive.
        table_name (str): The name of the table to retrieve.

    Returns:
        pyarrow.Table: The requested table as a pyarrow.Table snapshot.

    Raises:
        Exception: If the table does not exist, the session is closed, or if conversion to Arrow fails.
    """
    table = await asyncio.to_thread(session.open_table, table_name)
    return await asyncio.to_thread(table.to_arrow)


async def get_meta_table(session: Session, table_name: str) -> pyarrow.Table:
    """
    Asynchronously retrieve the meta table (schema/metadata) for a Deephaven table as a pyarrow.Table snapshot.

    This function runs blocking Deephaven API calls in a background thread. The meta table provides schema and column metadata for the specified table.

    Args:
        session (Session): The Deephaven session to retrieve the meta table from. Must be alive.
        table_name (str): The name of the table to retrieve the meta table for.

    Returns:
        pyarrow.Table: The meta table containing schema/metadata information for the specified table.

    Raises:
        Exception: If the table or its meta table does not exist, the session is closed, or if conversion to Arrow fails.
    """
    table = await asyncio.to_thread(session.open_table, table_name)
    meta_table = await asyncio.to_thread(lambda: table.meta_table)
    return await asyncio.to_thread(meta_table.to_arrow)


async def get_pip_packages_table(session: Session) -> pyarrow.Table:
    """
    Asynchronously retrieve a table of installed pip packages from a Deephaven session as a pyarrow.Table.

    This function runs a Python script in the session to create a temporary table listing all installed pip packages and their versions, then retrieves it as a pyarrow.Table snapshot. Blocking operations are run in a background thread.

    Args:
        session (Session): An active Deephaven session in which to run the script and retrieve the resulting table.

    Returns:
        pyarrow.Table: A table with columns 'Package' (str) and 'Version' (str), listing all installed pip packages.

    Raises:
        Exception: If the script fails to execute, the table cannot be retrieved, or conversion to Arrow fails.

    Example:
        >>> arrow_table = await get_pip_packages_table(session)

    Note:
        The temporary table '_pip_packages_table' is created in the session and is not automatically deleted.
        This function is intended for internal use only and may raise exceptions if the session or script is invalid.
    """
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
    _LOGGER.info("Running pip packages script in session...")
    await asyncio.to_thread(session.run_script, script)
    _LOGGER.info("Script executed successfully.")
    arrow_table = await get_table(session, "_pip_packages_table")
    _LOGGER.info("Table retrieved successfully.")
    return arrow_table


async def get_dh_versions(session: Session) -> tuple[str | None, str | None]:
    """
    Asynchronously retrieve the Deephaven Core and Core+ version strings installed in a given Deephaven session.

    This function runs a script in the session to generate a table of installed pip packages, then parses it to find the versions of 'deephaven-core' and 'deephaven_coreplus_worker'. Blocking operations are run in a background thread.

    Args:
        session (Session): An active Deephaven session object.

    Returns:
        tuple[str | None, str | None]: A tuple containing:
            - The version string for Deephaven Core, or None if not found (index 0).
            - The version string for Deephaven Core+, or None if not found (index 1).

    Raises:
        Exception: If the script fails, the pip packages table cannot be retrieved, or conversion to pandas fails.

    Note:
        Returns (None, None) if neither package is found in the session environment.
    """
    arrow_table = await get_pip_packages_table(session)
    if arrow_table is None:
        return None, None
    df = arrow_table.to_pandas()
    dh_core_version = None
    dh_coreplus_version = None
    raw_packages = df.to_dict(orient="records")
    for pkg in raw_packages:
        pkg_name = pkg.get("Package", "").lower()
        version = pkg.get("Version", "")
        if pkg_name == "deephaven-core" and dh_core_version is None:
            dh_core_version = version
        elif pkg_name == "deephaven_coreplus_worker" and dh_coreplus_version is None:
            dh_coreplus_version = version
        if dh_core_version and dh_coreplus_version:
            break
    return dh_core_version, dh_coreplus_version
