"""
Script Execution MCP Tools - Run Python Scripts and Manage Packages.

Provides MCP tools for executing code and managing packages in sessions:
- session_script_run: Execute Python scripts in a session
- session_pip_list: List installed Python packages

These tools work with both Community and Enterprise sessions.
"""

import logging

import aiofiles
from mcp.server.fastmcp import Context

from deephaven_mcp import queries

from deephaven_mcp.mcp_systems_server._tools.mcp_server import (
    mcp_server,
)
from deephaven_mcp.mcp_systems_server._tools.shared import (
    _get_session_from_context,
)

_LOGGER = logging.getLogger(__name__)



@mcp_server.tool()
async def session_script_run(
    context: Context,
    session_id: str,
    script: str | None = None,
    script_path: str | None = None,
) -> dict:
    r"""
    MCP Tool: Execute a script on a specified Deephaven session.

    Executes a script on the specified Deephaven session and returns execution status. The script
    can be provided either as a string in the 'script' parameter or as a file path in the 'script_path'
    parameter. Exactly one of these parameters must be provided.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Use 'script' parameter for inline script execution
    - Use 'script_path' parameter to execute scripts from files
    - Check 'success' field in response to verify execution completed without errors
    - Script executes in the session's environment with access to session state
    - Any tables or variables created will persist in the session for future use
    - Script language depends on the session's configured programming language

    Args:
        context (Context): The MCP context object.
        session_id (str): ID of the Deephaven session on which to execute the script. This argument is required.
        script (str, optional): The script to execute. Defaults to None.
        script_path (str, optional): Path to a script file to execute. Defaults to None.

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): True if the script executed successfully, False otherwise.
            - 'error' (str, optional): Error message if execution failed. Omitted on success.
            - 'isError' (bool, optional): Present and True if this is an error response (i.e., success is False).

    Example Successful Response:
        {'success': True}

    Example Error Responses:
        {'success': False, 'error': 'Must provide either script or script_path.', 'isError': True}
        {'success': False, 'error': 'Script execution failed: ...', 'isError': True}

    Example Usage:
        # Execute inline Python script
        Tool: session_script_run
        Parameters: {
            "session_id": "community:localhost:10000",
            "script": "from deephaven import new_table\nfrom deephaven.column import int_col\nmy_table = new_table([int_col('ID', [1, 2, 3])])"
        }

        # Execute script from file
        Tool: session_script_run
        Parameters: {
            "session_id": "community:localhost:10000",
            "script_path": "/path/to/analysis_script.py"
        }
    """
    _LOGGER.info(
        f"[mcp_systems_server:session_script_run] Invoked: session_id={session_id!r}, script={'<provided>' if script else None}, script_path={script_path!r}"
    )
    result: dict[str, object] = {"success": False}
    try:
        _LOGGER.debug(
            f"[mcp_systems_server:session_script_run] Validating script parameters for session '{session_id}'"
        )
        if script is None and script_path is None:
            _LOGGER.warning(
                "[mcp_systems_server:session_script_run] No script or script_path provided. Returning error."
            )
            result["error"] = "Must provide either script or script_path."
            result["isError"] = True
            return result

        if script is None:
            _LOGGER.info(
                f"[mcp_systems_server:session_script_run] Reading script from file: {script_path!r}"
            )
            if script_path is None:
                raise RuntimeError(
                    "Internal error: script_path is None after prior guard"
                )  # pragma: no cover
            _LOGGER.debug(
                f"[mcp_systems_server:session_script_run] Opening script file '{script_path}' for reading"
            )
            async with aiofiles.open(script_path) as f:
                script = await f.read()
            _LOGGER.debug(
                f"[mcp_systems_server:session_script_run] Successfully read {len(script)} characters from script file"
            )

        # Use helper to get session from context
        session = await _get_session_from_context(
            "session_script_run", context, session_id
        )
        _LOGGER.info(
            f"[mcp_systems_server:session_script_run] Session established for session: '{session_id}'"
        )

        _LOGGER.info(
            f"[mcp_systems_server:session_script_run] Executing script on session: '{session_id}'"
        )
        _LOGGER.debug(
            f"[mcp_systems_server:session_script_run] Script length: {len(script)} characters"
        )

        await session.run_script(script)

        _LOGGER.info(
            f"[mcp_systems_server:session_script_run] Script executed successfully on session: '{session_id}'"
        )
        result["success"] = True
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_script_run] Failed for session: '{session_id}', error: {e!r}",
            exc_info=True,
        )
        result["error"] = (
            f"Script execution failed for session '{session_id}': {type(e).__name__}: {e}"
        )
        result["isError"] = True
    return result




@mcp_server.tool()
async def session_pip_list(context: Context, session_id: str) -> dict:
    """
    MCP Tool: Retrieve installed pip packages as a TABULAR LIST from a Deephaven session.

    **Returns**: Package information formatted as TABULAR DATA with columns for package name and version.
    This tabular data should be displayed as a table to users for easy scanning of available libraries.

    Queries the specified Deephaven session for installed pip packages using importlib.metadata.
    Returns package names and versions for all Python packages available in the session's environment.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    Table Rendering:
    - **This tool returns TABULAR PACKAGE DATA that MUST be displayed as a table to users**
    - Each row represents one installed package
    - Columns: package (name), version
    - Present as a table for easy scanning of available libraries
    - Do NOT present package data as plain text or unstructured lists

    AI Agent Usage:
    - Use this to understand what libraries are available in a session before running scripts
    - Check 'result' array for list of installed packages with names and versions
    - Useful for determining if specific libraries need to be installed before script execution
    - Essential for generating code that uses available libraries and avoiding import errors
    - Helps inform decisions about which libraries to use when multiple options are available

    Args:
        context (Context): The MCP context object.
        session_id (str): ID of the Deephaven session to query.

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): True if the packages were retrieved successfully, False otherwise.
            - 'result' (list[dict], optional): List of pip package dicts if successful. Each contains:
                - 'package' (str): Package name
                - 'version' (str): Package version
            - 'error' (str, optional): Error message if retrieval failed.
            - 'isError' (bool, optional): Present and True if this is an error response (i.e., success is False).

    Example Successful Response:
        {'success': True, 'result': [{"package": "numpy", "version": "1.25.0"}, ...]}

    Example Error Response:
        {'success': False, 'error': 'Failed to get pip packages: ...', 'isError': True}
    """
    _LOGGER.info(
        f"[mcp_systems_server:session_pip_list] Invoked for session_id: {session_id!r}"
    )
    result: dict = {"success": False}
    try:
        # Use helper to get session from context
        session = await _get_session_from_context(
            "session_pip_list", context, session_id
        )
        _LOGGER.info(
            f"[mcp_systems_server:session_pip_list] Session established for session: '{session_id}'"
        )

        _LOGGER.debug(
            f"[mcp_systems_server:session_pip_list] Querying pip packages for session '{session_id}'"
        )
        arrow_table = await queries.get_pip_packages_table(session)
        _LOGGER.debug(
            f"[mcp_systems_server:session_pip_list] Retrieved pip packages table for session '{session_id}'"
        )
        _LOGGER.info(
            f"[mcp_systems_server:session_pip_list] Pip packages table retrieved successfully for session: '{session_id}'"
        )

        # Convert the Arrow table to a list of dicts
        packages: list[dict[str, str]] = []
        if arrow_table is not None:
            # Convert to pandas DataFrame for easy dict conversion
            df = arrow_table.to_pandas()
            raw_packages = df.to_dict(orient="records")
            # Validate and convert keys to lowercase
            packages = []
            for pkg in raw_packages:
                if (
                    not isinstance(pkg, dict)
                    or "Package" not in pkg
                    or "Version" not in pkg
                ):
                    raise ValueError(
                        "Malformed package data: missing 'Package' or 'Version' key"
                    )
                # Results should have lower case names.  The query had to use Upper case names to avoid invalid column names
                packages.append({"package": pkg["Package"], "version": pkg["Version"]})

        result["success"] = True
        result["result"] = packages
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_pip_list] Failed for session: '{session_id}', error: {e!r}",
            exc_info=True,
        )
        result["error"] = (
            f"Failed to list pip packages for session '{session_id}': {type(e).__name__}: {e}"
        )
        result["isError"] = True
    return result


