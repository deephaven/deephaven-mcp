"""Single source of truth for the liveness/readiness probe path.

Two MCP servers register a GET handler at :data:`HEALTH_PATH` via
``FastMCP.custom_route``:

- The systems server
  (:func:`deephaven_mcp.mcp_systems_server.server._register_health_endpoint`).
  Because this server mounts
  :class:`~deephaven_mcp.auth.middleware.TlsEnforcementMiddleware` and
  :class:`~deephaven_mcp.auth.middleware.AuthenticationMiddleware`, its
  startup code also lists :data:`HEALTH_PATH` in
  :attr:`~deephaven_mcp.auth.middleware.TransportSecurityPolicy.bypass_paths`
  and in the ``bypass_paths`` argument to the auth middleware so that
  probes succeed regardless of peer, scheme, or credentials.
- The docs server (:mod:`deephaven_mcp.mcp_docs_server._mcp`). This
  server does not mount any auth/TLS middleware, so no bypass list is
  needed.

Defining the constant here — outside of any middleware module — keeps
the auth/middleware layer free of application-route knowledge while
still letting both servers (and their tests) import a single canonical
value.
"""

from typing import Final

HEALTH_PATH: Final[str] = "/health"
"""Canonical liveness/readiness probe path (``str``).

The string includes a single leading slash and no trailing slash so it
matches Starlette's exact-match path routing used by
``FastMCP.custom_route``. Treat as immutable: declared :class:`~typing.Final`
so static analyzers flag any accidental rebinding.
"""

__all__ = ["HEALTH_PATH"]
