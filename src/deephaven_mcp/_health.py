"""Single source of truth for the liveness/readiness probe path.

Two MCP servers register a GET handler at :data:`HEALTH_PATH` via
``FastMCP.custom_route``:

- The systems server
  (:func:`deephaven_mcp.mcp_systems_server._fastmcp._register_health_endpoint`).
  Its HTTP transport mounts :class:`~deephaven_mcp.auth.middleware.PSKMiddleware`
  and lists :data:`HEALTH_PATH` in that middleware's ``bypass_paths`` (in
  :mod:`deephaven_mcp.mcp_systems_server._http`) so probes succeed without the
  pre-shared key.
- The docs server (:mod:`deephaven_mcp.mcp_docs_server._mcp`). This
  server does not mount any auth middleware, so no bypass is needed.

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
