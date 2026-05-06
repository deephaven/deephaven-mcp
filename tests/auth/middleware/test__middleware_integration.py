"""End-to-end integration tests for the auth middleware chain.

Exercises the *real* chain that ships in production:

    TlsEnforcementMiddleware (outermost)
        -> AuthenticationMiddleware
            -> downstream Starlette app

using a Starlette :class:`TestClient` for happy-path requests, and direct
ASGI calls (raw scope dicts) for cases where we need to spoof the peer IP
(Starlette's ``TestClient`` sets ``scope["client"] = None``).

The goal is to catch wiring bugs that the unit tests cannot see, such as:

- TLS rejection happening BEFORE auth (so leaked secrets in headers can
  still be rejected even if a backend is misconfigured).
- The TLS layer wrongly stripping the auth headers before the auth layer
  reads them.
- ``X-Forwarded-Proto`` trust accidentally bypassing both layers.
- ``WWW-Authenticate`` and ``Upgrade`` headers being emitted on the
  correct status code.

Naming follows the project's "_integration" suffix convention.
"""

from __future__ import annotations

import asyncio
import base64

import pytest
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient
from starlette.types import ASGIApp, Receive, Scope, Send

from deephaven_mcp.auth.backends import (
    HEADER_PASSWORD,
    HEADER_PRIVATE_KEY,
    HEADER_PSK,
    HEADER_USERNAME,
    PasswordBackend,
    PrivateKeyBackend,
    PSKBackend,
)
from deephaven_mcp.auth.middleware import (
    SCOPE_KEY_CREDENTIALS,
    SCOPE_KEY_PRINCIPAL,
    AuthenticationMiddleware,
    TlsEnforcementMiddleware,
    TransportSecurityPolicy,
    parse_forwarded_allow_ips,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _SetClientMiddleware:
    """Test-only ASGI middleware that pins ``scope['client']`` to a fixed peer.

    Starlette's :class:`TestClient` sets ``scope['client'] = None``; the
    real production server gets a populated tuple from the underlying
    socket. This wrapper bridges that gap so tests using ``TestClient``
    can still exercise the loopback / non-loopback branches of the TLS
    middleware.
    """

    def __init__(self, app: ASGIApp, client: tuple[str, int]) -> None:
        self.app = app
        self._client = client

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") == "http":
            scope = dict(scope)
            scope["client"] = self._client
        await self.app(scope, receive, send)


async def _whoami(request: Request) -> JSONResponse:
    """Echo the authenticated identity attached by the middleware chain.

    The downstream handler reads from the ASGI scope. If the auth layer
    never ran (or short-circuited with 401), neither key is present —
    making this a robust probe for "did auth actually happen?".
    """
    scope = request.scope
    principal = scope.get(SCOPE_KEY_PRINCIPAL)
    creds = scope.get(SCOPE_KEY_CREDENTIALS)
    return JSONResponse(
        {
            "subject": getattr(principal, "subject", None),
            "backend": (principal.raw.get("backend") if principal else None),
            "credentials_type": type(creds).__name__ if creds else None,
        }
    )


def _build_app(
    *,
    backends,
    tls_policy: TransportSecurityPolicy,
    auth_bypass_paths: frozenset[str] = frozenset(),
    test_client_peer: tuple[str, int] = ("127.0.0.1", 12345),
) -> Starlette:
    """Build the production middleware stack (TLS outermost, auth inner).

    The outermost layer pins ``scope['client']`` so Starlette's
    :class:`TestClient` (which leaves it as ``None``) can still drive
    loopback/non-loopback decision branches deterministically.
    """
    routes = [Route("/whoami", _whoami, methods=["GET", "POST"])]
    return Starlette(
        routes=routes,
        middleware=[
            # Test-only: pin the peer BEFORE the TLS layer reads it.
            Middleware(_SetClientMiddleware, client=test_client_peer),
            # Order below matches what the real server wires.
            Middleware(TlsEnforcementMiddleware, policy=tls_policy),
            Middleware(
                AuthenticationMiddleware,
                backends=backends,
                bypass_paths=auth_bypass_paths,
            ),
        ],
    )


def _strict_loopback_policy() -> TransportSecurityPolicy:
    """Default loopback-only policy used by most happy-path tests."""
    return TransportSecurityPolicy()


def _drive_asgi(
    app: ASGIApp,
    *,
    headers: dict[str, str] | None = None,
    client: tuple[str, int] = ("203.0.113.5", 51234),
    path: str = "/whoami",
    scheme: str = "http",
) -> tuple[int, dict[bytes, bytes], bytes]:
    """Synchronously drive the ASGI app with a synthetic scope.

    Returns ``(status, headers_dict, body)``. Used by tests that need
    to spoof a non-loopback peer (Starlette's ``TestClient`` does not
    expose a way to override ``scope['client']``).
    """
    captured: list[dict] = []

    async def _send(msg):
        captured.append(msg)

    async def _receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    encoded_headers = [
        (name.encode("latin-1"), value.encode("latin-1"))
        for name, value in (headers or {}).items()
    ]
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": scheme,
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": encoded_headers,
        "client": client,
        "server": ("testserver", 80),
    }

    asyncio.new_event_loop().run_until_complete(app(scope, _receive, _send))
    start = next(m for m in captured if m["type"] == "http.response.start")
    body = b"".join(m["body"] for m in captured if m["type"] == "http.response.body")
    return start["status"], dict(start["headers"]), body


def _raw_app(
    *,
    backends,
    tls_policy: TransportSecurityPolicy,
    auth_bypass_paths: frozenset[str] = frozenset(),
) -> Starlette:
    """Like :func:`_build_app` but WITHOUT the test-only client-pin layer.

    Used by tests that drive the ASGI app with their own synthetic
    scope (so the peer they set is the peer the middleware sees).
    """
    routes = [Route("/whoami", _whoami, methods=["GET", "POST"])]
    return Starlette(
        routes=routes,
        middleware=[
            Middleware(TlsEnforcementMiddleware, policy=tls_policy),
            Middleware(
                AuthenticationMiddleware,
                backends=backends,
                bypass_paths=auth_bypass_paths,
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Loopback (127.0.0.1) bypass: TestClient default peer
# ---------------------------------------------------------------------------


class TestLoopbackHappyPath:
    """The TestClient is pinned to peer=127.0.0.1, so plain HTTP is allowed."""

    def test_psk_succeeds_and_attaches_principal(self):
        app = _build_app(
            backends=[PSKBackend("s3cret")],
            tls_policy=_strict_loopback_policy(),
        )
        with TestClient(app) as client:
            r = client.get("/whoami", headers={HEADER_PSK: "s3cret"})
        assert r.status_code == 200
        body = r.json()
        assert body["subject"] == "psk"
        assert body["backend"] == "psk"
        assert body["credentials_type"] == "PSKCredentials"

    def test_password_succeeds(self):
        app = _build_app(
            backends=[PasswordBackend()],
            tls_policy=_strict_loopback_policy(),
        )
        with TestClient(app) as client:
            r = client.get(
                "/whoami",
                headers={HEADER_USERNAME: "alice", HEADER_PASSWORD: "pw"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["subject"] == "alice"
        assert body["backend"] == "password"
        assert body["credentials_type"] == "PasswordCredentials"

    def test_private_key_succeeds(self):
        key_text = "key value abc=\n"
        b64 = base64.b64encode(key_text.encode()).decode()
        app = _build_app(
            backends=[PrivateKeyBackend()],
            tls_policy=_strict_loopback_policy(),
        )
        with TestClient(app) as client:
            r = client.get(
                "/whoami",
                headers={HEADER_USERNAME: "bob", HEADER_PRIVATE_KEY: b64},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["subject"] == "bob"
        assert body["backend"] == "private_key"
        assert body["credentials_type"] == "PrivateKeyCredentials"

    def test_chained_backends_first_match_wins(self):
        """PSK precedes Password; a PSK request never reaches Password."""
        app = _build_app(
            backends=[PSKBackend("topsecret"), PasswordBackend()],
            tls_policy=_strict_loopback_policy(),
        )
        with TestClient(app) as client:
            r = client.get("/whoami", headers={HEADER_PSK: "topsecret"})
        assert r.status_code == 200
        assert r.json()["backend"] == "psk"

    def test_no_credentials_returns_401_with_all_challenges(self):
        app = _build_app(
            backends=[PSKBackend("x"), PasswordBackend(), PrivateKeyBackend()],
            tls_policy=_strict_loopback_policy(),
        )
        with TestClient(app) as client:
            r = client.get("/whoami")
        assert r.status_code == 401
        # Every backend's challenge must appear, comma-joined.
        challenge = r.headers["www-authenticate"]
        assert "DeephavenPSK" in challenge
        assert "DeephavenPassword" in challenge
        assert "DeephavenPrivateKey" in challenge
        # Body conveys structured detail.
        body = r.json()
        assert body["error"] == "unauthorized"

    def test_invalid_psk_returns_401(self):
        app = _build_app(
            backends=[PSKBackend("right")],
            tls_policy=_strict_loopback_policy(),
        )
        with TestClient(app) as client:
            r = client.get("/whoami", headers={HEADER_PSK: "wrong"})
        assert r.status_code == 401
        # Auth failure carries the standard challenge, NOT TLS-upgrade headers.
        assert "www-authenticate" in r.headers
        assert "upgrade" not in r.headers


# ---------------------------------------------------------------------------
# TLS enforcement: layered ordering tests (raw ASGI, non-loopback peer)
# ---------------------------------------------------------------------------


class TestTlsEnforcedBeforeAuth:
    """Verify TLS rejection happens BEFORE the auth layer reads headers.

    A 426 response means the auth headers (which carry secrets) never
    reached the auth layer or any downstream handler. This is the
    central security property of the layered design.
    """

    def test_non_loopback_cleartext_rejected_before_auth_reads_headers(self):
        """Even with a perfectly valid PSK, cleartext from a public peer is 426'd."""
        app = _raw_app(
            backends=[PSKBackend("right")],
            tls_policy=TransportSecurityPolicy(),
        )
        status, headers, _ = _drive_asgi(
            app,
            headers={HEADER_PSK: "right"},
            client=("203.0.113.5", 51234),
        )
        assert status == 426
        # Upgrade header is the TLS layer's signature.
        assert b"upgrade" in headers
        # Auth layer's WWW-Authenticate must NOT appear — proof the
        # request was rejected BEFORE auth ran.
        assert b"www-authenticate" not in headers

    def test_non_loopback_https_scheme_lets_auth_run(self):
        """Native TLS (scope.scheme=='https') allows the request through."""
        app = _raw_app(
            backends=[PSKBackend("right")],
            tls_policy=TransportSecurityPolicy(),
        )
        status, _, body = _drive_asgi(
            app,
            headers={HEADER_PSK: "right"},
            client=("203.0.113.5", 51234),
            scheme="https",
        )
        assert status == 200
        assert b'"backend":"psk"' in body or b'"backend": "psk"' in body


class TestForwardedProtoTrustOverHttp:
    """When a trusted proxy passes ``X-Forwarded-Proto: https``, auth still runs."""

    def test_trusted_proxy_https_header_allows_auth_to_run(self):
        nets, _ = parse_forwarded_allow_ips("10.0.0.0/8")
        policy = TransportSecurityPolicy(
            trust_forwarded_proto=True,
            forwarded_allow_ips=nets,
        )
        app = _raw_app(backends=[PSKBackend("k")], tls_policy=policy)
        status, _, body = _drive_asgi(
            app,
            headers={HEADER_PSK: "k", "x-forwarded-proto": "https"},
            client=("10.5.5.5", 51234),
        )
        assert status == 200
        assert b'"backend":"psk"' in body or b'"backend": "psk"' in body

    def test_trusted_proxy_http_header_still_rejects(self):
        """``X-Forwarded-Proto: http`` must NOT be treated as TLS."""
        nets, _ = parse_forwarded_allow_ips("10.0.0.0/8")
        policy = TransportSecurityPolicy(
            trust_forwarded_proto=True,
            forwarded_allow_ips=nets,
        )
        app = _raw_app(backends=[PSKBackend("k")], tls_policy=policy)
        status, headers, _ = _drive_asgi(
            app,
            headers={HEADER_PSK: "k", "x-forwarded-proto": "http"},
            client=("10.5.5.5", 51234),
        )
        assert status == 426
        assert b"upgrade" in headers

    def test_untrusted_peer_cannot_spoof_https_header(self):
        """Spoofing defense: a non-allowlisted peer sending the header is still rejected."""
        nets, _ = parse_forwarded_allow_ips("10.0.0.0/8")
        policy = TransportSecurityPolicy(
            trust_forwarded_proto=True,
            forwarded_allow_ips=nets,
        )
        app = _raw_app(backends=[PSKBackend("k")], tls_policy=policy)
        status, _, _ = _drive_asgi(
            app,
            headers={HEADER_PSK: "k", "x-forwarded-proto": "https"},
            # Public peer outside the 10/8 allowlist.
            client=("198.51.100.42", 51234),
        )
        assert status == 426


class TestBypassPathsAndAuth:
    """Bypass-path cells of the matrix interact with both layers."""

    def test_health_path_bypasses_both_layers(self):
        """``/health`` is in BOTH layers' bypass lists, reachable on cleartext.

        This is the documented production health-check shape: configure
        the auth layer to bypass ``/health`` so probes from arbitrary
        load balancers (any peer, any scheme) can succeed.
        """
        app = _raw_app(
            backends=[PSKBackend("k")],
            # Caller is responsible for putting /health in TLS bypass; the
            # middleware default is an empty frozenset.
            tls_policy=TransportSecurityPolicy(bypass_paths=frozenset({"/health"})),
            auth_bypass_paths=frozenset({"/health"}),  # /health in auth bypass
        )
        # Non-loopback peer to prove the TLS bypass really fires.
        status, _, _ = _drive_asgi(
            app,
            client=("203.0.113.5", 51234),
            path="/health",
        )
        # Path is registered as /whoami so we expect 404 from the inner
        # router; what matters is that we got *past* both middleware
        # layers without 426 or 401.
        assert status == 404


class TestAllowCleartextEscapeHatch:
    """When ``allow_cleartext=True``, even non-loopback HTTP reaches auth."""

    def test_allow_cleartext_lets_auth_run_on_non_loopback(self):
        policy = TransportSecurityPolicy(allow_cleartext=True)
        app = _raw_app(backends=[PSKBackend("k")], tls_policy=policy)
        status, _, _ = _drive_asgi(
            app,
            headers={HEADER_PSK: "k"},
            client=("203.0.113.5", 51234),
        )
        assert status == 200

    def test_allow_cleartext_does_not_disable_auth(self):
        """Cleartext opt-in lets the request reach auth; auth still gates it."""
        policy = TransportSecurityPolicy(allow_cleartext=True)
        app = _raw_app(backends=[PSKBackend("right")], tls_policy=policy)
        status, headers, _ = _drive_asgi(
            app,
            headers={HEADER_PSK: "wrong"},
            client=("203.0.113.5", 51234),
        )
        # Auth ran (got past TLS), then rejected.
        assert status == 401
        assert b"www-authenticate" in headers


class TestHeaderCaseInsensitivity:
    """HTTP header names are case-insensitive (RFC 7230); auth must honor that."""

    @pytest.mark.parametrize(
        "header_name", ["X-Deephaven-PSK", "x-deephaven-psk", "X-DEEPHAVEN-PSK"]
    )
    def test_psk_header_case_insensitive(self, header_name):
        app = _build_app(
            backends=[PSKBackend("k")], tls_policy=_strict_loopback_policy()
        )
        with TestClient(app) as client:
            r = client.get("/whoami", headers={header_name: "k"})
        assert r.status_code == 200
        assert r.json()["backend"] == "psk"
