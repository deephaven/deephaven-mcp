"""ASGI middleware integration for the ``auth`` framework.

This subpackage adapts the pure-function backend chain
(:mod:`deephaven_mcp.auth.backends`) to Starlette/ASGI by wrapping it in
:class:`AuthenticationMiddleware`. The middleware reads HTTP headers,
runs the configured backend chain, and attaches the resulting
:class:`~deephaven_mcp.auth.credentials.Principal` and
:data:`~deephaven_mcp.auth.credentials.Credentials` to the ASGI scope
under the keys :data:`SCOPE_KEY_PRINCIPAL` and
:data:`SCOPE_KEY_CREDENTIALS` respectively.

This is the only subpackage in :mod:`deephaven_mcp.auth` that depends on
Starlette/ASGI. Non-HTTP consumers (e.g. a future CLI, tests) use
:func:`deephaven_mcp.auth.backends.authenticate_and_resolve` directly.
"""

from ._middleware import (
    SCOPE_KEY_CREDENTIALS,
    SCOPE_KEY_PRINCIPAL,
    AuthenticationMiddleware,
)

__all__ = [
    "SCOPE_KEY_CREDENTIALS",
    "SCOPE_KEY_PRINCIPAL",
    "AuthenticationMiddleware",
]
