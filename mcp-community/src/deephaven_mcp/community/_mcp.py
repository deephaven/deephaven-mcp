"""
Deephaven MCP Community Tools Module

This module defines the set of MCP (Multi-Cluster Platform) tool functions for managing and interacting with Deephaven workers in a multi-server environment. All functions are designed for use as MCP tools and are decorated with @mcp_server.tool().

Key Features:
    - Structured, protocol-compliant error handling: all tools return dicts or lists of dicts with 'success' and 'error' keys as appropriate.
    - Async, coroutine-safe operations for configuration and session management.
    - Detailed logging for all tool invocations, results, and errors.
    - All docstrings are optimized for agentic and programmatic consumption and describe both user-facing and technical details.

Tools Provided:
    - refresh: Reload configuration and clear all sessions atomically.
    - default_worker: Get the default Deephaven worker name.
    - worker_names: List all configured Deephaven worker names.
    - table_schemas: Retrieve schemas for one or more tables from a worker.
    - run_script: Execute a script on a specified Deephaven worker.

Return Types:
    - All tools return structured dicts or lists of dicts, never raise exceptions to the MCP layer.
    - On success, 'success': True. On error, 'success': False and 'error': str.

See individual tool docstrings for full argument, return, and error details.
"""

import logging
import asyncio
from typing import Optional
from mcp.server.fastmcp import FastMCP
from deephaven_mcp import config
import deephaven_mcp.community._sessions as sessions
import aiofiles

_LOGGER = logging.getLogger(__name__)

# Module-level lock for refresh to prevent concurrent refresh operations.
_REFRESH_LOCK = asyncio.Lock()

mcp_server = FastMCP("deephaven-mcp-community")
"""
FastMCP Server Instance for Deephaven MCP Community Tools

This object is the singleton FastMCP server for the Deephaven MCP community toolset. It is responsible for registering and exposing all MCP tool functions defined in this module (such as refresh, default_worker, worker_names, table_schemas, and run_script) to the MCP runtime environment.

Key Details:
    - The server is instantiated with the name 'deephaven-mcp-community', which uniquely identifies this toolset in the MCP ecosystem.
    - All functions decorated with @mcp_server.tool() are automatically registered as MCP tools and made available for remote invocation.
    - The server manages protocol compliance, tool metadata, and integration with the broader MCP infrastructure.
    - This object should not be instantiated more than once per process/module.

Usage:
    - Do not call methods on mcp_server directly; instead, use the @mcp_server.tool() decorator to register new tools.
    - The MCP runtime will discover and invoke registered tools as needed.

See the module-level docstring for an overview of the available tools and error handling conventions.
"""


@mcp_server.tool()
async def refresh() -> dict:
    """
    MCP Tool: Reload and refresh Deephaven worker configuration and session cache.

    This tool atomically reloads the Deephaven worker configuration from disk and clears all active session objects for all workers. This ensures that any changes to the configuration (such as adding, removing, or updating workers) are applied immediately and that all sessions are reopened to reflect the new configuration. The operation is protected by a module-level lock to prevent concurrent refreshes, reducing race conditions.

    This tool is typically used by administrators or automated agents to force a full reload of the MCP environment after configuration changes.

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): True if the refresh completed successfully, False otherwise.
            - 'error' (str, optional): Error message if the refresh failed. Omitted on success.
            - 'isError' (bool, optional): Present and True if this is an error response (i.e., success is False).

    Example Successful Response:
        {'success': True}

    Example Error Response:
        {'success': False, 'error': 'Failed to reload configuration: ...', 'isError': True}

    Logging:
        - Logs tool invocation, success, and error details at INFO/ERROR levels.
    """
    _LOGGER.info(
        "[refresh] Invoked: refreshing worker configuration and session cache."
    )
    # Acquire the refresh lock to prevent concurrent refreshes. This does not
    # guarantee atomicity with respect to other config/session operations, but
    # it does ensure that only one refresh runs at a time and reduces race risk.
    try:
        async with _REFRESH_LOCK:
            await config.DEFAULT_CONFIG_MANAGER.clear_config_cache()
            await sessions.DEFAULT_SESSION_MANAGER.clear_all_sessions()
        _LOGGER.info(
            "[refresh] Success: Worker configuration and session cache have been reloaded."
        )
        return {"success": True}
    except Exception as e:
        _LOGGER.error(
            f"[refresh] Failed to refresh worker configuration/session cache: {e!r}",
            exc_info=True,
        )
        return {"success": False, "error": str(e), "isError": True}


@mcp_server.tool()
async def default_worker() -> dict:
    """
    MCP Tool: Retrieve the default Deephaven worker name as defined in configuration.

    This tool returns the name of the default Deephaven worker, as specified in the loaded configuration file. The default worker is used by other tools when no explicit worker name is provided.

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): True if the default worker was found, False otherwise.
            - 'result' (str, optional): The name of the default worker if successful.
            - 'error' (str, optional): Error message if retrieval failed. Omitted on success.
            - 'isError' (bool, optional): Present and True if this is an error response (i.e., success is False).

    Example Successful Response:
        {'success': True, 'result': 'local'}

    Example Error Response:
        {'success': False, 'error': 'No default worker set in configuration', 'isError': True}

    Logging:
        - Logs tool invocation, returned worker, and error details at INFO/ERROR levels.
    """
    _LOGGER.info("[default_worker] Invoked: retrieving default worker name.")
    try:
        worker = await config.DEFAULT_CONFIG_MANAGER.get_worker_name_default()
        _LOGGER.info(f"[default_worker] Success: Default worker is '{worker}'.")
        return {"success": True, "result": worker}
    except Exception as e:
        _LOGGER.error(
            f"[default_worker] Failed to get default worker: {e!r}", exc_info=True
        )
        return {"success": False, "error": str(e), "isError": True}


@mcp_server.tool()
async def worker_names() -> dict:
    """
    MCP Tool: List all configured Deephaven worker names.

    This tool returns the list of all worker names currently defined in the loaded configuration. Useful for populating UI dropdowns, validating worker names, or for agents to discover available workers.

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): True if worker names were retrieved successfully, False otherwise.
            - 'result' (list[str], optional): List of worker names if successful.
            - 'error' (str, optional): Error message if retrieval failed. Omitted on success.
            - 'isError' (bool, optional): Present and True if this is an error response (i.e., success is False).

    Example Successful Response:
        {'success': True, 'result': ['local', 'remote1', ...]}

    Example Error Response:
        {'success': False, 'error': 'Failed to load configuration: ...', 'isError': True}

    Logging:
        - Logs tool invocation, returned worker names, and error details at INFO/ERROR levels.
    """
    _LOGGER.info(
        "[worker_names] Invoked: retrieving list of all configured worker names."
    )
    try:
        names = await config.DEFAULT_CONFIG_MANAGER.get_worker_names()
        _LOGGER.info(f"[worker_names] Success: Found workers: {names!r}")
        return {"success": True, "result": names}
    except Exception as e:
        _LOGGER.error(
            f"[worker_names] Failed to get worker names: {e!r}", exc_info=True
        )
        return {"success": False, "error": str(e), "isError": True}


@mcp_server.tool()
async def table_schemas(
    worker_name: Optional[str] = None, table_names: Optional[list[str]] = None
) -> list:
    """
    MCP Tool: Retrieve schemas for one or more tables from a Deephaven worker.

    This tool returns the column schemas for the specified tables in the given Deephaven worker. If no table_names are provided, schemas for all tables in the worker are returned. If no worker_name is provided, the default worker is used.

    Arguments:
        worker_name (str, optional): Name of the Deephaven worker to query. Uses the default worker if None.
        table_names (list[str], optional): List of table names to fetch schemas for. If None, all tables are included.

    Returns:
        list: List of dicts, one per table. Each dict contains:
            - 'success' (bool): True if schema retrieval succeeded, False otherwise.
            - 'table' (str or None): Table name. None if the operation failed for all tables.
            - 'schema' (list[dict], optional): List of column definitions (name/type pairs) if successful.
            - 'error' (str, optional): Error message if schema retrieval failed for this table.
            - 'isError' (bool, optional): Present and True if this is an error response (i.e., success is False).

    Example Successful Response:
        [
            {'success': True, 'table': 'MyTable', 'schema': [{'name': 'Col1', 'type': 'int'}, ...]},
            {'success': False, 'table': 'MissingTable', 'error': 'Table not found', 'isError': True}
        ]

    Example Error Response (total failure):
        [
            {'success': False, 'table': None, 'error': 'Failed to connect to worker: ...', 'isError': True}
        ]

    Logging:
        - Logs tool invocation, per-table results, and error details at INFO/ERROR levels.
    """
    _LOGGER.info(
        f"[table_schemas] Invoked: worker_name={worker_name!r}, table_names={table_names!r}"
    )
    results = []
    try:
        session = await sessions.DEFAULT_SESSION_MANAGER.get_or_create_session(worker_name)
        _LOGGER.info(f"[table_schemas] Session established for worker: '{worker_name}'")

        if table_names is not None:
            selected_table_names = table_names
            _LOGGER.info(
                f"[table_schemas] Fetching schemas for specified tables: {selected_table_names!r}"
            )
        else:
            selected_table_names = list(session.tables)
            _LOGGER.info(
                f"[table_schemas] Fetching schemas for all tables in worker: {selected_table_names!r}"
            )

        for table_name in selected_table_names:
            try:
                meta_table = session.open_table(table_name).meta_table.to_arrow()
                # meta_table is a pyarrow.Table with columns: 'Name', 'DataType', etc.
                schema = [
                    {"name": row["Name"], "type": row["DataType"]}
                    for row in meta_table.to_pylist()
                ]
                results.append({"success": True, "table": table_name, "schema": schema})
                _LOGGER.info(
                    f"[table_schemas] Success: Retrieved schema for table '{table_name}'"
                )
            except Exception as table_exc:
                _LOGGER.error(
                    f"[table_schemas] Failed to get schema for table '{table_name}': {table_exc!r}",
                    exc_info=True,
                )
                results.append(
                    {
                        "success": False,
                        "table": table_name,
                        "error": str(table_exc),
                        "isError": True,
                    }
                )
        _LOGGER.info(f"[table_schemas] Returning schemas: {results!r}")
        return results
    except Exception as e:
        _LOGGER.error(
            f"[table_schemas] Failed for worker: '{worker_name}', error: {e!r}",
            exc_info=True,
        )
        return [{"success": False, "table": None, "error": str(e), "isError": True}]


@mcp_server.tool()
async def run_script(
    worker_name: Optional[str] = None,
    script: Optional[str] = None,
    script_path: Optional[str] = None,
) -> dict:
    """
    MCP Tool: Execute a script on a specified Deephaven worker.

    This tool executes a user-provided script (either as a direct string or loaded from a file path) on the specified Deephaven worker. The worker's language (e.g., Python, Groovy) is determined by its configuration. Script execution is performed in an isolated session for the worker. The tool returns a structured result indicating success or failure, along with error details if applicable.

    Arguments:
        worker_name (str, optional): Name of the Deephaven worker on which to execute the script. If not provided, the default worker is used.
        script (str, optional): The script source code to execute. Must be provided unless script_path is specified.
        script_path (str, optional): Path to a file containing the script to execute. Used if script is not provided.

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

    Logging:
        - Logs tool invocation, script source/path, execution status, and error details at INFO/WARNING/ERROR levels.
    """
    _LOGGER.info(
        f"[run_script] Invoked: worker_name={worker_name!r}, script={'<provided>' if script else None}, script_path={script_path!r}"
    )
    result = {"success": False, "error": ""}
    try:
        if script is None and script_path is None:
            _LOGGER.warning(
                "[run_script] No script or script_path provided. Returning error."
            )
            result["error"] = "Must provide either script or script_path."
            result["isError"] = True
            return result

        if script is None:
            _LOGGER.info(f"[run_script] Loading script from file: {script_path!r}")
            async with aiofiles.open(script_path, "r") as f:
                script = await f.read()

        session = await sessions.DEFAULT_SESSION_MANAGER.get_or_create_session(worker_name)
        _LOGGER.info(f"[run_script] Session established for worker: '{worker_name}'")

        _LOGGER.info(f"[run_script] Executing script on worker: '{worker_name}'")
        await asyncio.to_thread(session.run_script, script)
        _LOGGER.info(
            f"[run_script] Script executed successfully on worker: '{worker_name}'"
        )
        result["success"] = True
    except Exception as e:
        _LOGGER.error(
            f"[run_script] Failed for worker: '{worker_name}', error: {e!r}",
            exc_info=True,
        )
        result["error"] = str(e)
        result["isError"] = True
    return result
