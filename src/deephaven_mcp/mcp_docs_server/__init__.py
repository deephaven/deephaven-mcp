"""
deephaven_mcp.mcp_docs_server package.

This module serves as the entrypoint for the Deephaven MCP Docs server package. It provides access to the MCP server instance (`mcp_server`).

All MCP tool definitions are implemented in the internal module `_mcp.py`.

Exports:
    - mcp_server: The FastMCP server instance with all registered tools.

Usage:
    from deephaven_mcp.mcp_docs_server import mcp_server

See the project README for configuration details, available tools, and usage examples.
"""

from ._mcp import mcp_server

__all__ = ["mcp_server"]
