"""MCP Systems Server Tools Package.

This package contains the implementation of every Deephaven MCP tool the
multiplexed systems server registers. Each module exposes a
``register_tools(server: FastMCP)`` function invoked by
:func:`deephaven_mcp.mcp_systems_server.server._register_tools` at
startup.

Modules:
    session: Session listing and querying across all configured systems.
    session_community: Community session lifecycle management.
    session_enterprise: Enterprise session management (per ``system``).
    table: Table operations and data export.
    script: Script execution and package management.
    catalog: Enterprise catalog operations (per ``system``).
    pq: Enterprise persistent-query management (per ``system``).
    shared: Internal utility functions (not MCP tools).

All MCP tools follow consistent patterns:

- Return structured dict responses with 'success', 'error', and 'isError' keys.
- Never raise exceptions to the MCP layer.
- Use async/await for all I/O operations.
- Include comprehensive docstrings for AI agent consumption.
"""
