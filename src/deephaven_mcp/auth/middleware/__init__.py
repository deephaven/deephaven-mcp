"""ASGI middleware integration for the ``auth`` framework.

This subpackage adapts the pure-function backend chain
(:mod:`deephaven_mcp.auth.backends`) to Starlette/ASGI by exposing two
ASGI middlewares that the MCP servers compose in front of the FastMCP
streamable-HTTP app:

- :class:`AuthenticationMiddleware` reads HTTP headers, runs the
  configured backend chain, and on success attaches the resulting
  :class:`~deephaven_mcp.auth.credentials.Principal` and
  :class:`~deephaven_mcp.auth.credentials.Credentials` to the ASGI scope
  under the keys :data:`SCOPE_KEY_PRINCIPAL` and
  :data:`SCOPE_KEY_CREDENTIALS` respectively. On failure it short-circuits
  with ``401 Unauthorized``.
- :class:`TlsEnforcementMiddleware` decides whether the request's
  *transport* is acceptable to carry the auth headers' secrets. It never
  reads the auth headers themselves; it inspects scheme, peer IP, and
  optionally a trusted ``X-Forwarded-Proto`` header, and rejects
  cleartext non-loopback traffic with ``426 Upgrade Required``.
  :class:`TransportSecurityPolicy` is the immutable policy object the
  middleware consumes; :func:`parse_forwarded_allow_ips` parses its
  CIDR-aware peer-IP allowlist.

In production the servers mount the TLS layer **outermost** (so cleartext
secrets are rejected before they ever reach the auth layer) and the auth
layer immediately inside it.

This is the only subpackage in :mod:`deephaven_mcp.auth` that depends on
Starlette/ASGI. Non-HTTP consumers (e.g. a future CLI, tests) use
:func:`deephaven_mcp.auth.backends.authenticate_and_resolve` directly.
"""

from ._middleware import (
    SCOPE_KEY_CREDENTIALS,
    SCOPE_KEY_PRINCIPAL,
    AuthenticationMiddleware,
)
from ._tls import (
    TlsEnforcementMiddleware,
    TransportSecurityPolicy,
    parse_forwarded_allow_ips,
)

__all__ = [
    "SCOPE_KEY_CREDENTIALS",
    "SCOPE_KEY_PRINCIPAL",
    "AuthenticationMiddleware",
    "TlsEnforcementMiddleware",
    "TransportSecurityPolicy",
    "parse_forwarded_allow_ips",
]
