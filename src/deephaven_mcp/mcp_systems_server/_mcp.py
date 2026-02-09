"""
Deephaven MCP Systems Tools Module.

This module defines the set of MCP (Multi-Cluster Platform) tool functions for managing and interacting with Deephaven sessions in a multi-server environment. All functions are designed for use as MCP tools and are decorated with @mcp_server.tool().

Key Features:
    - Structured, protocol-compliant error handling: all tools return consistent dict structures with 'success' and 'error' keys as appropriate.
    - Async, coroutine-safe operations for configuration and session management.
    - Detailed logging for all tool invocations, results, and errors.
    - All docstrings are optimized for agentic and programmatic consumption and describe both user-facing and technical details.

Tools Provided:
    Configuration and System Management:
    - mcp_reload: Reload configuration and clear all sessions atomically.
    - enterprise_systems_status: List all enterprise (Core+) systems with their status and configuration details.

    Session Management:
    - sessions_list: List all sessions (community and enterprise) with basic metadata.
    - session_details: Get detailed information about a specific session.
    - session_community_create: Create a new dynamically launched Community session via Docker or python.
    - session_community_delete: Delete a dynamically created Community session and clean up resources.
    - session_community_credentials: SECURITY SENSITIVE - Retrieve connection credentials for browser access (disabled by default, requires security.community.credential_retrieval_mode configuration).
    - session_enterprise_create: Create a new enterprise session with configurable parameters and resource limits.
    - session_enterprise_delete: Delete an existing enterprise session and clean up resources.

    Session Table Operations:
    - session_tables_list: Retrieve names of all tables in a session (lightweight alternative to session_tables_schema).
    - session_tables_schema: Retrieve full metadata schemas for one or more tables from a session (requires session_id).
    - session_table_data: Retrieve table data with flexible formatting (json-row, json-column, csv) and optional row limiting for safe access to large tables.

    Session Script and Package Management:
    - session_script_run: Execute a script on a specified Deephaven session (requires session_id).
    - session_pip_list: Retrieve all installed pip packages (name and version) from a specified Deephaven session using importlib.metadata, returned as a list of dicts.

    Catalog Operations (Enterprise Core+ Only):
    - catalog_tables_list: Retrieve catalog table entries from enterprise (Core+) sessions with optional filtering by namespace or table name patterns.
    - catalog_namespaces_list: Retrieve distinct namespaces from enterprise (Core+) catalog for efficient discovery of data domains.
    - catalog_tables_schema: Retrieve full schemas for catalog tables in enterprise (Core+) sessions with flexible filtering by namespace, table names, or custom filters.
    - catalog_table_sample: Retrieve sample data from a catalog table in enterprise (Core+) sessions with flexible formatting and row limiting for safe previewing.

    Persistent Query (PQ) Management (Enterprise Core+ Only):
    - pq_name_to_id: Convert a PQ name to its canonical pq_id for use with other PQ tools.
    - pq_list: List all persistent queries on an enterprise system with their status and configuration.
    - pq_details: Get detailed information about a specific persistent query including state, configuration, and metadata.
    - pq_create: Create a new persistent query with configurable resource allocation and settings.
    - pq_delete: Permanently delete a persistent query and release its resources.
    - pq_modify: Modify an existing persistent query configuration with optional restart.
    - pq_start: Start a persistent query and wait for it to reach RUNNING state.
    - pq_stop: Stop one or more running persistent queries (supports bulk operations).
    - pq_restart: Restart one or more stopped persistent queries (supports bulk operations).

Return Types:
    - All tools return structured dict objects, never raise exceptions to the MCP layer.
    - On success, 'success': True. On error, 'success': False and 'error': str.
    - Tools that return multiple items use nested structures (e.g., 'systems', 'sessions', 'schemas' arrays within the main dict).

See individual tool docstrings for full argument, return, and error details.
"""

from deephaven_mcp.mcp_systems_server._tools.mcp_server import mcp_server

__all__ = [
    "mcp_server",
]
