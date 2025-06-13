"""
Async Deephaven session query helpers for table and package inspection.

This private module provides coroutine-compatible utility functions for querying
Deephaven tables and installed pip packages within an active Deephaven session.

Functions:
    - get_table(session, table_name): Retrieve a table as a pyarrow.Table.
    - get_meta_table(session, table_name): Retrieve a table's schema/meta table as a pyarrow.Table.
    - get_pip_packages_table(session): Get a table of installed pip packages.
    - get_dh_versions(session): Get the installed Deephaven Core and Core+ version strings.

All functions are async and intended for internal use by the sessions package.
"""

import asyncio
import logging

import pyarrow
from pydeephaven import Session

_LOGGER = logging.getLogger(__name__)


async def get_table(session: Session, table_name: str) -> pyarrow.Table:
    """
    Retrieve a table from a Deephaven session as a pyarrow.Table.
    Args:
        session (Session): The Deephaven session to retrieve the table from.
        table_name (str): The name of the table to retrieve.
    Returns:
        pyarrow.Table: The table as a pyarrow.Table.
    """
    table = await asyncio.to_thread(session.open_table, table_name)
    return await asyncio.to_thread(table.to_arrow)


async def get_meta_table(session: Session, table_name: str) -> pyarrow.Table:
    """
    Retrieve the meta table (schema/metadata) for a Deephaven table as a pyarrow.Table.
    Args:
        session (Session): The Deephaven session to retrieve the meta table from.
        table_name (str): The name of the table to retrieve the meta table for.
    Returns:
        pyarrow.Table: The meta table containing schema/metadata information for the specified table.
    """
    table = await asyncio.to_thread(session.open_table, table_name)
    meta_table = await asyncio.to_thread(lambda: table.meta_table)
    return await asyncio.to_thread(meta_table.to_arrow)


async def get_pip_packages_table(session: Session) -> pyarrow.Table:
    """
    Returns a table of installed pip packages from a Deephaven session.
    Args:
        session (Session):
            An active Deephaven session in which to run the script and retrieve the resulting table.
    Returns:
        pyarrow.Table:
            A pyarrow.Table containing two columns: 'Package' (str) and 'Version' (str), listing all installed pip packages.
    Raises:
        Exception: On failure to run the script or retrieve the table.
    Example:
        >>> arrow_table = await get_pip_packages_table(session)
    """
    script = """
    from deephaven import new_table, string_col
    import importlib.metadata as importlib_metadata
    names = []
    versions = []
    for dist in importlib_metadata.distributions():
        names.append(dist.metadata['Name'])
        versions.append(dist.version)
    result = new_table([
        string_col('Package', names),
        string_col('Version', versions),
    ])
    """
    _LOGGER.info("Running pip packages script in session...")
    await asyncio.to_thread(session.run_script, script)
    _LOGGER.info("Script executed successfully.")
    arrow_table = await get_table(session, "_pip_packages_table")
    _LOGGER.info("Table retrieved successfully.")
    return arrow_table


async def get_dh_versions(session: Session) -> tuple[str | None, str | None]:
    """
    Retrieve the Deephaven Core and Core+ versions installed in a given Deephaven session.
    These versions are retrieved by running a script in the session that queries the installed pip packages.
    Args:
        session (Session): An active Deephaven session object.
    Returns:
        Tuple[Optional[str], Optional[str]]: A tuple containing:
            - The version string for Deephaven Core, or None if not found (index 0).
            - The version string for Deephaven Core+, or None if not found (index 1).
    Raises:
        Exception: If the script or table retrieval fails.
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
