"""
MCP Systems Server Tools Package.

This package contains the implementation of all Deephaven MCP tools organized by
functional area. Each module provides MCP tools decorated with @mcp_server.tool()
that are automatically registered with the FastMCP server instance.

Modules:
    mcp_server: Server infrastructure and configuration management
    session: Session listing and querying (both Community and Enterprise)
    session_community: Community session lifecycle management
    session_enterprise: Enterprise session management
    table: Table operations and data export
    script: Script execution and package management
    catalog: Enterprise catalog operations
    pq: Enterprise persistent query management
    shared: Internal utility functions (not MCP tools)

All MCP tools follow consistent patterns:
    - Return structured dict responses with 'success' and 'error' keys
    - Never raise exceptions to the MCP layer
    - Use async/await for all I/O operations
    - Include comprehensive docstrings for AI agent consumption
"""
