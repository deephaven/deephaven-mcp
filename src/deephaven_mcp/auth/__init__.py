"""Authentication primitives for the Deephaven MCP servers.

Subpackages:

- :mod:`deephaven_mcp.auth.credentials` — outbound bearer credential
  dataclasses passed into
  :meth:`deephaven_mcp.client.CoreSession.from_credentials` and
  :meth:`deephaven_mcp.client.CorePlusSessionFactory.from_credentials`.
- :mod:`deephaven_mcp.auth.tls` — outbound transport-layer TLS
  material for community sessions: optional server-trust bundle and
  optional mTLS client certificate.
- :mod:`deephaven_mcp.auth.middleware` — inbound Starlette middleware
  that authenticates clients calling into an MCP server's HTTP
  transport. Provides :class:`PSKMiddleware`.
"""
