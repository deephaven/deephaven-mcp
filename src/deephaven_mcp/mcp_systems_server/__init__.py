"""deephaven_mcp.mcp_systems_server package.

Contains two MCP server implementations sharing a common tool set:

- ``enterprise`` (``server.enterprise``): DHE (Deephaven Enterprise) MCP server — one system per server instance.
- ``community`` (``server.community``): DHC (Deephaven Community) MCP server — one server, multiple workers.

Both servers use HTTP (streamable-http) transport only and are started via their respective
CLI entry points (``dh-mcp-enterprise-server`` and ``dh-mcp-community-server``).

Key modules:
- ``server``: CLI entry points, argument parsing, environment setup, and shared tool registration.
- ``_lifespan``: FastMCP lifespan factories for server startup/shutdown.
- ``_tools/``: Individual MCP tool implementations.
"""
