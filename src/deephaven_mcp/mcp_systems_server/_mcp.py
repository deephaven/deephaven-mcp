"""
Deephaven MCP Systems Server - Main Module.

This module provides the primary entry point for the Deephaven MCP Systems Server
by re-exporting the FastMCP server instance from the _tools.mcp_server module.

The actual MCP tools are defined in the _tools submodules:
- _tools.mcp_server: Server infrastructure and configuration management
- _tools.session: Session listing and querying
- _tools.session_community: Community session lifecycle management
- _tools.session_enterprise: Enterprise session management
- _tools.table: Table operations and data export
- _tools.script: Script execution and package management
- _tools.catalog: Enterprise catalog operations
- _tools.pq: Enterprise persistent query management
- _tools.shared: Internal utilities

All tools are automatically registered with the MCP server via decorators.

Usage:
    from deephaven_mcp.mcp_systems_server._mcp import mcp_server
    mcp_server.run(transport="stdio")
"""

from deephaven_mcp.mcp_systems_server._tools.mcp_server import mcp_server

# Import all tool modules to execute their @mcp_server.tool() decorators
# This registers all tools with the mcp_server instance
# Using underscore aliases to keep imports private (not part of public API)
from deephaven_mcp.mcp_systems_server._tools import (
    catalog as _catalog,
    pq as _pq,
    script as _script,
    session as _session,
    session_community as _session_community,
    session_enterprise as _session_enterprise,
    table as _table,
)

__all__ = [
    "mcp_server",
]
