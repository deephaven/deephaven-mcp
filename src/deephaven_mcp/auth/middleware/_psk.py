"""Pre-shared-key gate for an MCP server's HTTP transport.

A reusable Starlette middleware that authenticates inbound HTTP
requests against a single shared secret. It is mounted by the
multiplexed systems server when its HTTP transport is selected; other
MCP servers (e.g. the docs server) may mount it as well.

Security model
--------------
Every request must carry the PSK in the ``X-Deephaven-PSK`` header.
The middleware verifies the header value with
:func:`hmac.compare_digest` and rejects anything that does not match
with HTTP 401. Bypass paths (typically the ``/health`` route) are
forwarded without checking the header so external probes can run
without sharing the PSK.

This is a transport-level gate — "is this client allowed to talk to
this server at all?" — and produces no per-user identity. Outbound
credentials used to authenticate the MCP server *to* downstream
Deephaven workers live in :mod:`deephaven_mcp.auth.credentials` and
are an orthogonal concern.
"""

from __future__ import annotations

import hmac
import logging
from collections.abc import Iterable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp

__all__ = ["MINIMUM_PSK_LENGTH", "PSK_HEADER_NAME", "PSKMiddleware"]

_LOGGER = logging.getLogger(__name__)

PSK_HEADER_NAME = "X-Deephaven-PSK"
"""Name of the header carrying the pre-shared key on every authenticated request."""

MINIMUM_PSK_LENGTH = 16
"""Minimum permitted length of an expected PSK, in characters.

Shorter values are refused at construction time so that the deployed
gate is not brute-forceable in practice. Operators who need a longer
key can supply any string at or above this length.
"""


class PSKMiddleware(BaseHTTPMiddleware):
    """Reject requests whose ``X-Deephaven-PSK`` header does not match the configured PSK.

    The middleware is constructed with the *expected* PSK and an
    optional set of bypass paths. Each request that does not match a
    bypass path must present a header equal (constant-time) to the
    expected PSK; otherwise the middleware returns HTTP 401 and the
    rest of the application is not invoked.

    The expected PSK must be at least :data:`MINIMUM_PSK_LENGTH`
    characters long. Shorter values (including the empty string) are
    refused at construction time with :class:`ValueError`.

    Rejected responses carry a ``WWW-Authenticate: Deephaven-PSK
    realm="mcp"`` header and a JSON body of the form::

        {"error": "Unauthorized", "code": "<code>", "detail": "<text>"}

    where ``code`` is a stable machine-readable identifier:

    - ``psk_missing`` — the ``X-Deephaven-PSK`` header was absent.
    - ``psk_invalid`` — the header was present but its value did not
      match the configured PSK (empty values fall in this bucket).

    The ``detail`` string is human-readable and never reveals the
    expected key or its length.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        expected_psk: str,
        bypass_paths: Iterable[str] = (),
    ) -> None:
        """Configure the middleware.

        Args:
            app (ASGIApp): The downstream ASGI application this
                middleware wraps.
            expected_psk (str): The PSK that incoming requests must
                present. Must be at least
                :data:`MINIMUM_PSK_LENGTH` characters long; shorter
                values (including the empty string) are refused at
                construction time so the gate is not brute-forceable.
            bypass_paths (Iterable[str]): Iterable of exact request
                paths that bypass the PSK check entirely. Each entry
                is matched with ``==``; prefix matching is
                deliberately not supported so bypass surfaces stay
                tight. A typical value is ``("/health",)`` so
                liveness probes work without sharing the PSK.
                Defaults to ``()`` (no bypass).

        Raises:
            ValueError: When ``expected_psk`` is shorter than
                :data:`MINIMUM_PSK_LENGTH` characters (this includes
                the empty string).
        """
        if len(expected_psk) < MINIMUM_PSK_LENGTH:
            raise ValueError(
                f"PSKMiddleware requires expected_psk to be at least "
                f"{MINIMUM_PSK_LENGTH} characters; configure a longer value."
            )
        super().__init__(app)
        self._expected_psk = expected_psk
        self._bypass_paths = frozenset(bypass_paths)

    async def dispatch(self, request: Request, call_next: object) -> Response:
        """Run the PSK gate or bypass and forward to the downstream app.

        Args:
            request (Request): The incoming HTTP request.
            call_next: The Starlette callable that invokes the
                downstream app and returns its :class:`Response`.

        Returns:
            Response: HTTP 401 when the request is rejected; otherwise
                the downstream app's response.
        """
        path = request.url.path
        if path in self._bypass_paths:
            return await call_next(request)  # type: ignore[no-any-return,operator]

        presented = request.headers.get(PSK_HEADER_NAME)
        if presented is None:
            reason = "missing"
            code = "psk_missing"
            detail = (
                f"Authentication required: request is missing the "
                f"'{PSK_HEADER_NAME}' HTTP header, which must carry the "
                f"server's pre-shared key (PSK)."
            )
        elif not hmac.compare_digest(
            presented.encode("utf-8"), self._expected_psk.encode("utf-8")
        ):
            reason = "invalid"
            code = "psk_invalid"
            detail = (
                f"Authentication failed: the '{PSK_HEADER_NAME}' HTTP header "
                f"was present but its value does not match the server's "
                f"configured pre-shared key (PSK)."
            )
        else:
            return await call_next(request)  # type: ignore[no-any-return,operator]

        _LOGGER.warning(
            f"[PSKMiddleware] Rejected {request.method} {path}: "
            f"{reason} {PSK_HEADER_NAME} header"
        )
        return JSONResponse(
            {"error": "Unauthorized", "code": code, "detail": detail},
            status_code=401,
            headers={"WWW-Authenticate": 'Deephaven-PSK realm="mcp"'},
        )
