"""Inbound authentication middleware for MCP servers.

Holds Starlette middleware classes that gate inbound HTTP requests to
an MCP server. Provides :class:`PSKMiddleware`.
"""

from ._psk import PSK_HEADER_NAME, PSKMiddleware

__all__ = ["PSK_HEADER_NAME", "PSKMiddleware"]
