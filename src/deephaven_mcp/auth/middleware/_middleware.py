"""ASGI middleware enforcing :class:`AuthBackend`-based authentication.

Mounted by the streamable-HTTP MCP servers in front of the FastMCP ASGI app.
For every incoming ``http`` request:

1. Lowercase the request headers into a plain ``dict[str, str]``.
2. Delegate to :func:`authenticate_and_resolve`, which walks the registered
   backends in order and returns the first :class:`Principal` (and derived
   :data:`Credentials`) produced.
3. On success, attach the principal and credentials to ``scope`` under the
   keys ``"deephaven_mcp.principal"`` and ``"deephaven_mcp.credentials"``,
   then pass the request through to the inner app.
4. On failure (any backend raised :class:`AuthenticationError`, or no backend
   produced a principal), short-circuit with a ``401 Unauthorized`` response
   whose ``WWW-Authenticate`` header advertises every registered backend's
   challenge and whose JSON body's ``detail`` field carries the resolver's
   error message.

Non-``http`` scopes (``lifespan``, ``websocket``) pass through unchanged:
the middleware is intentionally a no-op for them.

Bypass paths
------------
Some routes MUST be reachable without credentials (for example the
``/.well-known/oauth-protected-resource`` URL defined by the MCP 2025-06-18
auth spec). The ``bypass_paths`` parameter holds an exact-match set of such
paths; requests whose ``scope["path"]`` is in the set are allowed through
with no principal attached. The community server passes an empty set (no
OAuth endpoints in Phase 1); enterprise and future OAuth work can extend
this.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence

from starlette.types import ASGIApp, Receive, Scope, Send

from ..backends import AuthBackend, AuthenticationError, authenticate_and_resolve

__all__ = [
    "AuthenticationMiddleware",
    "SCOPE_KEY_PRINCIPAL",
    "SCOPE_KEY_CREDENTIALS",
]

_LOGGER = logging.getLogger(__name__)

SCOPE_KEY_PRINCIPAL = "deephaven_mcp.principal"
"""ASGI scope key under which an authenticated :class:`Principal` is
attached. Downstream handlers read this to learn the caller's identity.
"""

SCOPE_KEY_CREDENTIALS = "deephaven_mcp.credentials"
"""ASGI scope key under which the backend-derived :data:`Credentials` are
attached.
"""


class AuthenticationMiddleware:
    """ASGI middleware that runs registered backends against each request.

    Attributes:
        app (ASGIApp): The inner ASGI app (the FastMCP streamable-HTTP
            Starlette application).
        backends (tuple[AuthBackend, ...]): Backends consulted, in order.
        bypass_paths (frozenset[str]): Paths for which auth is skipped
            entirely (e.g. future well-known OAuth metadata endpoints).
    """

    def __init__(
        self,
        app: ASGIApp,
        backends: Sequence[AuthBackend],
        bypass_paths: frozenset[str] = frozenset(),
    ) -> None:
        """Initialize the middleware.

        Args:
            app (ASGIApp): The inner ASGI application to wrap.
            backends (Sequence[AuthBackend]): Backends to try on each
                ``http`` request, in order. Must be non-empty.
            bypass_paths (frozenset[str]): Exact paths to allow through
                without authentication. Defaults to the empty set.

        Raises:
            ValueError: If ``backends`` is empty (a middleware with no
                backends would reject every request and is always a
                configuration bug).
        """
        if not backends:
            raise ValueError("AuthenticationMiddleware requires at least one backend.")
        self.app = app
        self.backends: tuple[AuthBackend, ...] = tuple(backends)
        self.bypass_paths = bypass_paths

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """ASGI entry point.

        Non-``http`` scopes and paths listed in :attr:`bypass_paths` are
        forwarded to the inner app unchanged. ``http`` requests are
        authenticated via :func:`authenticate_and_resolve`; on success the
        principal and credentials are attached to ``scope`` and the request
        is forwarded; on failure a 401 is emitted.

        Note that ``await self.app(...)`` runs *outside* the
        ``try/except AuthenticationError`` block — downstream errors that
        happen to be of this class must propagate normally and never be
        converted into a 401.
        """
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")
        if path in self.bypass_paths:
            await self.app(scope, receive, send)
            return

        headers = _lower_headers(scope.get("headers", ()))
        try:
            principal, credentials = await authenticate_and_resolve(
                self.backends, headers
            )
        except AuthenticationError as exc:
            _LOGGER.warning(
                f"[AuthenticationMiddleware] Rejected request to {path!r}: {exc}"
            )
            await _send_401(send, self.backends, str(exc))
            return

        scope[SCOPE_KEY_PRINCIPAL] = principal
        scope[SCOPE_KEY_CREDENTIALS] = credentials
        await self.app(scope, receive, send)


def _lower_headers(
    raw_headers: Sequence[tuple[bytes, bytes]],
) -> dict[str, str]:
    """Convert ASGI header pairs to a lowercase-keyed ``dict[str, str]``.

    Headers are ``latin-1`` decoded per the ASGI HTTP spec (see
    https://asgi.readthedocs.io/en/latest/specs/www.html#http-connection-scope,
    which requires header name/value bytes to be valid ``latin-1``).

    Later values silently overwrite earlier ones; this matches how most
    authentication headers are treated (and the tiny number of headers the
    auth layer cares about never legitimately appear twice).
    """
    out: dict[str, str] = {}
    for name, value in raw_headers:
        out[name.decode("latin-1").lower()] = value.decode("latin-1")
    return out


async def _send_401(
    send: Send,
    backends: Sequence[AuthBackend],
    detail: str,
) -> None:
    """Emit a compact JSON ``401 Unauthorized`` response."""
    challenges = ", ".join(b.challenge() for b in backends)
    body = json.dumps({"error": "unauthorized", "detail": detail}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 401,
            "headers": [
                (b"content-type", b"application/json"),
                (b"www-authenticate", challenges.encode("latin-1")),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})
