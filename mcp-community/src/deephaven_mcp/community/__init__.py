"""
dhmcp package

This package implements the core logic for an MCP (Model Context Protocol) server using FastMCP, and defines all tools available to MCP clients.

Configuration:
- All Deephaven worker connection information is managed via a JSON configuration file (default: `deephaven_workers.json` in the project root, or set with the `DH_MCP_CONFIG_FILE` environment variable).
- The config file must contain a `workers` dictionary, keyed by worker name, with each value a dictionary of connection parameters (see README for schema).
- Strict validation is performed on config load: only allowed fields are permitted and all fields must be the correct type. All worker fields are optional; define only those needed for your deployment.
- The config may also specify a `default_worker` key, which names the default worker for UI or documentation purposes, but all API calls require an explicit worker name.

How to Add Tools:
- Define a new function in this file.
- Decorate it with `@mcp_server.tool()`.
- Write a clear docstring describing its arguments and return value.

Usage:
- Import `mcp_server` in your server entry point (e.g., `mcp_server.py`) and call `mcp_server.run()`.
- Use the provided client script or MCP Inspector to discover and invoke tools.
- Use `deephaven_worker_names()` to enumerate available workers, and pass the desired worker name to any tool that requires it.

Available Tools:
- `echo_tool(message: str) -> str`: Echoes a message back to the caller.
- `gnome_count_colorado() -> int`: Returns the number of gnomes in Colorado.
- `deephaven_worker_names() -> list[str]`: Returns all configured Deephaven worker names.
- `deephaven_list_tables(worker_name: str) -> list`: Lists tables for the specified Deephaven worker.
- `deephaven_table_schemas(worker_name: str) -> list`: Returns schemas for all tables in the specified Deephaven worker.

See the project README for more information on configuration, running the server, and interacting with tools.
"""

import logging
from typing import Optional
from mcp.server.fastmcp import FastMCP
from deephaven_mcp import config
from ._sessions import get_or_create_session, clear_all_sessions, _SESSION_CACHE_LOCK


mcp_server = FastMCP("deephaven-mcp-community")

def run_server(transport: str = "stdio") -> None:
    """
    Start the MCP server with the specified transport.
    
    Args:
        transport (str, optional): The transport type ('stdio' or 'sse'). Defaults to 'stdio'.
    """
    #TODO: can the log_level just be set via env?
    # Set log level based on transport
    log_level = logging.ERROR if transport == "stdio" else logging.DEBUG
    logging.basicConfig(level=log_level, format='[%(asctime)s] %(levelname)s: %(message)s')

    logging.info(f"Starting MCP server '{mcp_server.name}' with transport={transport}")

    # Make sure config can be loaded before starting
    config.get_config()

    try:
        mcp_server.run(transport=transport)
    finally:
        logging.info(f"MCP server '{mcp_server.name}' stopped.")



@mcp_server.tool()
def echo_tool(message: str) -> str:
    """
    Echo the input message, prefixed with 'Echo: '.

    Args:
        message (str): The message to echo back to the caller.

    Returns:
        str: The echoed message, prefixed with 'Echo: '.
    """
    logging.info(f"CALL: echo_tool called with message={message!r}")
    result = f"Echo: {message}"
    logging.info("echo_tool called with message: %r, returning: %r", message, result)
    return result


@mcp_server.tool()
def gnome_count_colorado() -> int:
    """
    Return the current number of gnomes in Colorado.

    Returns:
        int: The number of gnomes in Colorado.
    """
    logging.info("CALL: gnome_count_colorado called with no arguments")
    count = 53
    logging.info("gnome_count_colorado called, returning: %d", count)
    return count


@mcp_server.tool()
def deephaven_refresh() -> None:
    """
    Reloads and refreshes the Deephaven worker configuration and session cache.
    This allows new workers to be added or existing workers to be removed.
    It also reopens all sessions to the workers to handle any expired or disconnected sessions.
    """
    logging.info("CALL: deephaven_refresh called with no arguments")
    with config._CONFIG_CACHE_LOCK:
        with _SESSION_CACHE_LOCK:
            config.clear_config_cache()
            clear_all_sessions()
    logging.info("Deephaven worker configuration and session cache reloaded via MCP tool.")


@mcp_server.tool()
def deephaven_default_worker() -> Optional[str]:
    """
    MCP Tool: Get the default Deephaven worker name.

    Returns the name of the default Deephaven worker as specified in the configuration file.
    This is used when a worker name is not explicitly provided to other tools.

    Returns:
        str: The default worker name as defined in the config file.
    """
    logging.info("CALL: deephaven_default_worker called with no arguments")
    return config.get_worker_name_default()

@mcp_server.tool()
def deephaven_worker_names() -> list[str]:
    """
    MCP Tool: List all Deephaven worker names.

    Retrieves the names of all Deephaven workers defined in the configuration file.
    Useful for populating UI dropdowns or validating worker names.

    Returns:
        list[str]: List of all Deephaven worker names from the config file.
    """
    logging.info("CALL: deephaven_worker_names called with no arguments")
    return config.get_worker_names()

@mcp_server.tool()
def deephaven_list_table_names(worker_name: Optional[str] = None) -> list:
    """
    MCP Tool: List table names in a Deephaven worker.

    Returns a list of table names available in the specified Deephaven worker. If no
    worker_name is provided, the default worker from the configuration is used.

    Args:
        worker_name (str, optional): Name of the Deephaven worker to use. If not provided, uses default_worker from config.

    Returns:
        list: List of table names available in the Deephaven worker.

    Raises:
        Exception: If the session cannot be created or tables cannot be retrieved. Errors are logged.
    """
    logging.info(f"CALL: deephaven_list_table_names called with worker_name={worker_name!r}")
    try:
        session = get_or_create_session(worker_name)
        logging.info(f"deephaven_list_tables: Session obtained successfully for worker: '{worker_name}'")
        tables = list(session.tables)
        logging.info(f"deephaven_list_tables: Retrieved tables from session: {tables!r}")
        return tables
    except Exception as e:
        logging.error(f"deephaven_list_tables failed for worker: '{worker_name}', error: {e!r}", exc_info=True)
        return [f"Error: {e}"]


@mcp_server.tool()
def deephaven_table_schemas(worker_name: Optional[str] = None, table_names: Optional[list[str]] = None) -> list:
    """
    MCP Tool: Get the schemas for one or more Deephaven tables.

    Returns the names and schemas of the specified tables in the given Deephaven worker. If no table_names list is provided,
    returns schemas for all tables in the worker. If no worker_name is provided, uses the default worker from config.

    Args:
        worker_name (str, optional): Name of the Deephaven worker to use. If not provided, uses default_worker from config.
        table_names (list[str], optional): List of table names to get schemas for. If not provided, gets schemas for all tables.
    Returns:
        list: List of dicts with table name and schema (list of column name/type pairs).
    Example return value:
        [
            {"table": "t1", "schema": [{"name": "C1", "type": "int"}, ...]},
            ...
        ]
    """
    logging.info(f"CALL: deephaven_table_schemas called with worker_name={worker_name!r}, table_names={table_names!r}")
    results = []
    try:
        session = get_or_create_session(worker_name)
        logging.info(f"deephaven_table_schemas: Session obtained successfully for worker: '{worker_name}'")

        if table_names is not None:
            selected_table_names = table_names
            logging.info(f"deephaven_table_schemas: Fetching schemas for user-provided tables: {selected_table_names!r}")
        else:
            selected_table_names = list(session.tables)
            logging.info(f"deephaven_table_schemas: Fetching schemas for all tables in worker (default): {selected_table_names!r}")

        for table_name in selected_table_names:
            try:
                meta_table = session.open_table(table_name).meta_table.to_arrow()
                # meta_table is a pyarrow.Table with columns: 'Name', 'DataType', etc.
                schema = [
                    {"name": row["Name"], "type": row["DataType"]}
                    for row in meta_table.to_pylist()
                ]
                results.append({"table": table_name, "schema": schema})
            except Exception as table_exc:
                logging.error(f"deephaven_table_schemas: failed to get schema for table '{table_name}': {table_exc!r}", exc_info=True)
                results.append({"table": table_name, "error": str(table_exc)})
        logging.info(f"deephaven_table_schemas: returning: {results!r}")
        return results
    except Exception as e:
        logging.error(f"deephaven_table_schemas: failed for worker: '{worker_name}', error: {e!r}", exc_info=True)
        return [f"Error: {e}"]


@mcp_server.tool()
def deephaven_run_script(worker_name: Optional[str] = None, script: Optional[str] = None, script_path: Optional[str] = None) -> dict:
    """
    MCP Tool: Run a script on a Deephaven server.

    Executes the provided script (as a string or from a file path) on the specified Deephaven worker.
    The script language is determined by the worker configuration (e.g., Python, Groovy, etc.).
    Returns a dict with 'success' and/or 'error'.

    Args:
        worker_name (str, optional): Name of the Deephaven worker. Uses default if not provided.
        script (str, optional): Script source code to execute.
        script_path (str, optional): Path to a script file to execute (if script is not provided).
    Returns:
        dict: {'success': bool, 'error': str (if any)}
    """
    logging.info(f"CALL: deephaven_run_script called with worker_name={worker_name!r}, script={(script[:40] + '...') if script and len(script) > 40 else script!r}, script_path={script_path!r}")
    result = {"success": False, "error": ""}
    try:
        if script is None and script_path is None:
            result["error"] = "Must provide either script or script_path."
            return result

        if script is None:
            with open(script_path, "r") as f:
                script = f.read()

        session = get_or_create_session(worker_name)
        logging.info(f"deephaven_run_script: Session obtained successfully for worker: '{worker_name}'")

        logging.info(f"deephaven_run_script: Executing script on worker: '{worker_name}'")
        session.run_script(script)
        logging.info(f"deephaven_run_script: Script executed successfully on worker: '{worker_name}'")
        result["success"] = True
    except Exception as e:
        logging.error(f"deephaven_run_script: failed for worker: '{worker_name}', error: {e!r}", exc_info=True)
        result["error"] = str(e)
    return result

