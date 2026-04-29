"""Pure-function credential resolver reusable outside the ASGI middleware.

The middleware (:class:`~deephaven_mcp.auth.middleware.AuthenticationMiddleware`) is the
normal path used by the streamable-HTTP servers, but some callers have a
mapping of headers and no ASGI request: for example a future CLI that talks
to the same backends directly, or unit tests that want to exercise a
backend chain without spinning up Starlette. Those callers use
:func:`authenticate_and_resolve`, which implements the same
"first-backend-wins" logic as the middleware over a plain dict of headers.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from ..credentials import Credentials, Principal
from ._base import AuthBackend, AuthenticationError

__all__ = ["authenticate_and_resolve"]


async def authenticate_and_resolve(
    backends: Sequence[AuthBackend],
    headers: Mapping[str, str],
) -> tuple[Principal, Credentials]:
    """Run ``backends`` against ``headers`` and return the first match.

    Applies the same "first backend to return a :class:`Principal` wins"
    rule as :class:`AuthenticationMiddleware`. If a backend raises
    :class:`AuthenticationError`, that error is re-raised immediately
    (remaining backends are not tried); this matches the middleware's
    short-circuit behavior.

    ``headers`` is converted to a lowercase-key mapping before each backend
    is called, so callers may pass headers with any casing.

    Args:
        backends (Sequence[AuthBackend]): The backends to try, in order.
        headers (Mapping[str, str]): The request headers to authenticate.

    Returns:
        tuple[Principal, Credentials]: The authenticated principal and the
            credentials derived from it by the matching backend.

    Raises:
        AuthenticationError: If any backend rejected the request, if no
            backend produced a principal (the message lists the backends
            that were tried), or if ``backends`` is empty (indicating a
            server-side misconfiguration).
    """
    lowered: dict[str, str] = {k.lower(): v for k, v in headers.items()}
    for backend in backends:
        principal = await backend.authenticate(lowered)
        if principal is not None:
            credentials = await backend.derive_credentials(principal, lowered)
            return principal, credentials
    if not backends:
        raise AuthenticationError(
            "No authentication backends are configured on this server."
        )
    tried = ", ".join(b.name for b in backends)
    raise AuthenticationError(
        f"No registered authentication backend accepted the supplied headers "
        f"(tried: {tried})."
    )
