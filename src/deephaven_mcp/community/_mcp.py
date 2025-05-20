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
    - worker_names: List all configured Deephaven worker names.
    - table_schemas: Retrieve schemas for one or more tables from a worker (requires worker_name).
    - run_script: Execute a script on a specified Deephaven worker (requires worker_name).
    - pip_packages: Retrieve all installed pip packages (name and version) from a specified Deephaven worker using importlib.metadata, returned as a list of dicts.

Return Types:
    - All tools return structured dicts or lists of dicts, never raise exceptions to the MCP layer.
    - On success, 'success': True. On error, 'success': False and 'error': str.

See individual tool docstrings for full argument, return, and error details.
"""

import asyncio
import logging
import textwrap
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import aiofiles
from mcp.server.fastmcp import Context, FastMCP

import deephaven_mcp.community._sessions as sessions
from deephaven_mcp import config

_LOGGER = logging.getLogger(__name__)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[dict[str, object]]:
    """
    Async context manager for the FastMCP server application lifespan.

    This function manages the startup and shutdown lifecycle of the MCP server. It is responsible for:
      - Instantiating a ConfigManager and SessionManager for Deephaven worker configuration and session management.
      - Creating a coroutine-safe asyncio.Lock (refresh_lock) for atomic configuration/session refreshes.
      - Loading and validating the Deephaven worker configuration before the server accepts requests.
      - Yielding a context dictionary containing config_manager, session_manager, and refresh_lock for use by all tool functions via MCPRequest.context.
      - Ensuring all session resources are properly cleaned up on shutdown.

    Startup actions:
      - Loads and validates the Deephaven worker configuration using the ConfigManager.
      - Logs progress and yields a context dict containing the loaded managers/lock.

    Shutdown actions:
      - Logs server shutdown initiation.
      - Clears all active Deephaven sessions using the SessionManager.
      - Logs completion of server shutdown.

    Args:
        server: The FastMCP server instance (required by the FastMCP API, not directly used).

    Yields:
        dict: A context dictionary with the following keys for use in all MCP tool requests:
            - 'config_manager': The ConfigManager instance for worker configuration.
            - 'session_manager': The SessionManager instance for session management.
            - 'refresh_lock': An asyncio.Lock for atomic refresh operations.
    """
    _LOGGER.info("Starting MCP server '%s'", server.name)
    session_manager = None

    try:
        config_manager = config.ConfigManager()

        # Make sure config can be loaded before starting
        _LOGGER.info("Loading configuration...")
        await config_manager.get_config()
        _LOGGER.info("Configuration loaded.")

        session_manager = sessions.SessionManager(config_manager)

        # lock for refresh to prevent concurrent refresh operations.
        refresh_lock = asyncio.Lock()

        yield {
            "config_manager": config_manager,
            "session_manager": session_manager,
            "refresh_lock": refresh_lock,
        }
    finally:
        _LOGGER.info("Shutting down MCP server '%s'", server.name)
        if session_manager is not None:
            await session_manager.clear_all_sessions()
        _LOGGER.info("MCP server '%s' shut down.", server.name)


mcp_server = FastMCP("deephaven-mcp-community", lifespan=app_lifespan)
"""
FastMCP Server Instance for Deephaven MCP Community Tools

This object is the singleton FastMCP server for the Deephaven MCP community toolset. It is responsible for registering and exposing all MCP tool functions defined in this module (such as refresh, worker_names, table_schemas, and run_script) to the MCP runtime environment.

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
async def refresh(context: Context) -> dict:
    """
    MCP Tool: Reload and refresh Deephaven worker configuration and session cache.

    This tool atomically reloads the Deephaven worker configuration from disk and clears all active session objects for all workers. It uses dependency injection via the MCPRequest context to access the config manager, session manager, and a coroutine-safe refresh lock (all provided by app_lifespan). This ensures that any changes to the configuration (such as adding, removing, or updating workers) are applied immediately and that all sessions are reopened to reflect the new configuration. The operation is protected by the provided lock to prevent concurrent refreshes, reducing race conditions.

    This tool is typically used by administrators or automated agents to force a full reload of the MCP environment after configuration changes.

    Args:
        context (Context): The FastMCP Context for this tool call.

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
        refresh_lock = context.request_context.lifespan_context["refresh_lock"]
        config_manager = context.request_context.lifespan_context["config_manager"]
        session_manager = context.request_context.lifespan_context["session_manager"]

        async with refresh_lock:
            await config_manager.clear_config_cache()
            await session_manager.clear_all_sessions()
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
async def worker_statuses(context: Context) -> dict:
    """
    MCP Tool: List all configured Deephaven workers and their availability status.

    This tool returns the list of all worker names currently defined in the loaded configuration, along with a boolean indicating whether each worker is available (i.e., a session can be created for the worker). Configuration and session management are accessed via dependency injection using the MCPRequest context.

    Args:
        context (Context): The FastMCP Context for this tool call.

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): True if statuses were retrieved successfully, False otherwise.
            - 'result' (list[dict], optional): List of dicts with keys 'worker' (str) and 'available' (bool).
            - 'error' (str, optional): Error message if retrieval failed. Omitted on success.
            - 'isError' (bool, optional): Present and True if this is an error response (i.e., success is False).

    Example Successful Response:
        {'success': True, 'result': [{'worker': 'local', 'available': True}, {'worker': 'remote1', 'available': False}, ...]}

    Example Error Response:
        {'success': False, 'error': 'Failed to check worker statuses: ...', 'isError': True}

    Logging:
        - Logs tool invocation, checked workers, statuses, and error details at INFO/ERROR levels.
    """
    _LOGGER.info(
        "[worker_statuses] Invoked: retrieving status of all configured workers."
    )
    try:
        config_manager = context.request_context.lifespan_context["config_manager"]
        session_manager = context.request_context.lifespan_context["session_manager"]
        names = await config_manager.get_worker_names()
        results = []
        for name in names:
            try:
                # Try to get or create a session for the worker. If this fails, mark as unavailable.
                session = await session_manager.get_or_create_session(name)
                available = session is not None and session.is_alive
            except Exception as e:
                _LOGGER.warning(f"[worker_statuses] Worker '{name}' unavailable: {e!r}")
                available = False
            results.append({"worker": name, "available": available})
        _LOGGER.info(f"[worker_statuses] Statuses: {results!r}")
        return {"success": True, "result": results}
    except Exception as e:
        _LOGGER.error(
            f"[worker_statuses] Failed to get worker statuses: {e!r}", exc_info=True
        )
        return {"success": False, "error": str(e), "isError": True}


@mcp_server.tool()
async def table_schemas(
    context: Context, worker_name: str, table_names: list[str] | None = None
) -> list:
    """
    MCP Tool: Retrieve schemas for one or more tables from a Deephaven worker.

    This tool returns the column schemas for the specified tables in the given Deephaven worker. If no table_names are provided, schemas for all tables in the worker are returned. Session management is accessed via dependency injection from the MCPRequest context.

    Args:
        context (Context): The FastMCP Context for this tool call.
        worker_name (str): Name of the Deephaven worker to query. This argument is required.
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
        session_manager = context.request_context.lifespan_context["session_manager"]
        session = await session_manager.get_or_create_session(worker_name)
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
                meta_table = await sessions.get_table(session, table_name)
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
    context: Context,
    worker_name: str,
    script: str | None = None,
    script_path: str | None = None,
) -> dict:
    """
    MCP Tool: Execute a script on a specified Deephaven worker.

    This tool executes a user-provided script (either as a direct string or loaded from a file path) on the specified Deephaven worker. The worker's language (e.g., Python, Groovy) is determined by its configuration. Script execution is performed in an isolated session for the worker. All session management is accessed via dependency injection using the MCPRequest context.

    Args:
        context (Context): The FastMCP Context for this tool call.
        worker_name (str): Name of the Deephaven worker on which to execute the script. This argument is required.
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
            if script_path is None:
                raise ValueError("script_path must not be None")
            async with aiofiles.open(script_path) as f:
                script = await f.read()

        session_manager = context.request_context.lifespan_context["session_manager"]
        session = await session_manager.get_or_create_session(worker_name)
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


@mcp_server.tool()
async def pip_packages(context: Context, worker_name: str) -> dict:
    """
    MCP Tool: Get all installed pip packages on a specified Deephaven worker.

    This tool connects to the specified Deephaven worker using pydeephaven and retrieves the list of installed pip packages by executing `pip list --format=json` in the worker's Python environment.

    Args:
        context (Context): The FastMCP Context for this tool call.
        worker_name (str): Name of the Deephaven worker to query.

    Returns:
        dict: Structured result object with the following keys:
            - 'success' (bool): True if the packages were retrieved successfully, False otherwise.
            - 'result' (list[dict], optional): List of pip package dicts (name, version) if successful.
            - 'error' (str, optional): Error message if retrieval failed.
            - 'isError' (bool, optional): Present and True if this is an error response (i.e., success is False).

    Example Successful Response:
        {'success': True, 'result': [{"package": "numpy", "version": "1.25.0"}, ...]}

    Example Error Response:
        {'success': False, 'error': 'Failed to get pip packages: ...', 'isError': True}

    Logging:
        - Logs tool invocation, script execution, and error details at INFO/ERROR levels.
    """
    _LOGGER.info(f"[pip_packages] Invoked for worker: {worker_name!r}")
    result: dict = {"success": False}
    try:
        session_manager = context.request_context.lifespan_context["session_manager"]
        session = await session_manager.get_or_create_session(worker_name)
        _LOGGER.info(f"[pip_packages] Session established for worker: '{worker_name}'")

        # Script to create a table with the pip list
        script = textwrap.dedent(
            """
            from deephaven import new_table
            from deephaven.column import string_col
            import importlib.metadata

            def _make_pip_packages_table():
                names = []
                versions = []
                for dist in importlib.metadata.distributions():
                    names.append(dist.metadata["Name"])
                    versions.append(dist.version)
                return new_table([
                    string_col("Package", names),
                    string_col("Version", versions),
                ])

            _pip_packages_table = _make_pip_packages_table()
        """
        )

        await asyncio.to_thread(session.run_script, script)
        _LOGGER.info(
            f"[pip_packages] Script executed successfully on worker: '{worker_name}'"
        )

        # Get the table using a thread for sync operations
        arrow_table = await sessions.get_table(session, "_pip_packages_table")
        _LOGGER.info(
            f"[pip_packages] Table retrieved successfully on worker: '{worker_name}'"
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
            f"[pip_packages] Failed for worker: '{worker_name}', error: {e!r}",
            exc_info=True,
        )
        result["error"] = str(e)
        result["isError"] = True
    return result
