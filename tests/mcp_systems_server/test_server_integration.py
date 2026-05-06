"""End-to-end integration tests for the MCP systems server middleware stack.

Reaches *into* :mod:`deephaven_mcp.mcp_systems_server.server` and uses
its **real** internal builders — :func:`_build_community_middleware`,
:func:`_build_enterprise_middleware`, :func:`_validate_transport_security_or_exit`,
and the same TLS-middleware-appending order that :func:`_run_server`
applies — to assemble the production middleware stack on top of a
synthetic Starlette inner app. The whole chain is then driven via
direct ASGI calls.

Why direct ASGI rather than ``TestClient``?
    Several of the security branches we want to verify (non-loopback
    bind, ``X-Forwarded-Proto`` trust, peer-IP allowlists) depend on the
    ASGI ``scope['client']`` field. ``starlette.testclient.TestClient``
    sets that field to ``None``, which means the loopback-bypass cell
    of the TLS decision matrix can never be exercised through it. The
    helpers below build raw scopes so every cell is reachable.

These tests guard the wiring contract — order of middleware layers,
the TLS layer being OUTERMOST, the policy returned by
``_validate_transport_security_or_exit`` being the same one the middleware
gets — without ever spinning up uvicorn.

Naming follows the project's "_integration" suffix convention.
"""

from __future__ import annotations

import asyncio

import pytest
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

import deephaven_mcp.mcp_systems_server.server as server
from deephaven_mcp.auth.backends import (
    HEADER_PASSWORD,
    HEADER_PSK,
    HEADER_USERNAME,
)
from deephaven_mcp.auth.middleware import (
    SCOPE_KEY_PRINCIPAL,
    TlsEnforcementMiddleware,
    TransportSecurityPolicy,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _echo(request: Request) -> JSONResponse:
    """Inner endpoint: reports whether auth attached a principal."""
    principal = request.scope.get(SCOPE_KEY_PRINCIPAL)
    return JSONResponse(
        {
            "subject": getattr(principal, "subject", None),
            "backend": principal.raw.get("backend") if principal else None,
        }
    )


def _make_args(**overrides) -> server._ParsedArgs:
    """Build a :class:`_ParsedArgs` with safe defaults."""
    base = {
        "config_path": None,
        "host": "127.0.0.1",
        "port": 8003,
        "ssl_keyfile": None,
        "ssl_certfile": None,
        "trust_forwarded_proto": False,
        "forwarded_allow_ips": "127.0.0.1",
        "allow_cleartext": False,
    }
    base.update(overrides)
    return server._ParsedArgs(**base)


def _assemble_app(
    *,
    auth_middleware: list[Middleware],
    policy: TransportSecurityPolicy,
) -> Starlette:
    """Reproduce the production middleware-build order from :func:`_run_server`.

    The production driver (:func:`_run_with_middleware`) loops the
    ``middleware`` list and calls
    :meth:`Starlette.add_middleware` for each entry. ``add_middleware``
    inserts at index 0, so the LAST entry added becomes the OUTERMOST
    layer at runtime — i.e. the TLS layer fires first on every request,
    then the auth layer, then the inner app. This fixture mirrors that
    *exact* sequence (rather than the
    ``Starlette(middleware=...)`` constructor, which preserves list order
    and would invert the layering).
    """

    async def _health(_request: Request) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    routes = [
        Route("/x", _echo, methods=["GET"]),
        Route("/health", _health, methods=["GET"]),
    ]
    middleware = list(auth_middleware) + [
        Middleware(TlsEnforcementMiddleware, policy=policy)
    ]
    app = Starlette(routes=routes)
    for entry in middleware:
        app.add_middleware(entry.cls, *entry.args, **entry.kwargs)
    return app


def _drive(
    app,
    *,
    headers: dict[str, str] | None = None,
    client: tuple[str, int] = ("127.0.0.1", 12345),
    scheme: str = "http",
    path: str = "/x",
) -> tuple[int, dict[bytes, bytes], bytes]:
    """Synchronously drive an ASGI app with a synthetic scope."""
    captured: list[dict] = []

    async def _send(msg):
        captured.append(msg)

    async def _receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    encoded = [
        (k.encode("latin-1"), v.encode("latin-1")) for k, v in (headers or {}).items()
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
        "headers": encoded,
        "client": client,
        "server": ("testserver", 80),
    }
    asyncio.new_event_loop().run_until_complete(app(scope, _receive, _send))
    start = next(m for m in captured if m["type"] == "http.response.start")
    body = b"".join(m["body"] for m in captured if m["type"] == "http.response.body")
    return start["status"], dict(start["headers"]), body


# ---------------------------------------------------------------------------
# Community server: PSK + TLS
# ---------------------------------------------------------------------------


class TestCommunityFullStack:
    """End-to-end stack for the community server's auth+TLS layering."""

    def test_validated_policy_drives_real_middleware(self):
        """The policy returned by ``_validate_transport_security_or_exit`` is the
        same instance the middleware uses, and works on the wire."""
        args = _make_args(host="127.0.0.1")
        policy, key, cert = server._validate_transport_security_or_exit(
            label="community", args=args
        )
        assert key is None and cert is None
        auth_mw = server._build_community_middleware("the-psk", host="127.0.0.1")
        app = _assemble_app(auth_middleware=auth_mw, policy=policy)

        # Loopback peer + valid PSK -> 200, principal attached.
        status, _, body = _drive(app, headers={HEADER_PSK: "the-psk"})
        assert status == 200
        assert b'"backend":"psk"' in body or b'"backend": "psk"' in body

    def test_invalid_psk_returns_401(self):
        args = _make_args(host="127.0.0.1")
        policy, _, _ = server._validate_transport_security_or_exit(
            label="community", args=args
        )
        auth_mw = server._build_community_middleware("right-psk", host="127.0.0.1")
        app = _assemble_app(auth_middleware=auth_mw, policy=policy)

        status, headers, _ = _drive(app, headers={HEADER_PSK: "wrong-psk"})
        assert status == 401
        assert b"www-authenticate" in headers

    def test_non_loopback_cleartext_rejected_at_tls_layer(self):
        """Even with a valid PSK, cleartext from a public peer never reaches auth."""
        # Validation requires --allow-cleartext OR loopback OR TLS to start at
        # all on a non-loopback bind; here we want the bind to be loopback for
        # validation but exercise the middleware at runtime against a
        # non-loopback peer (the 'TLS terminator misbehaves' scenario).
        args = _make_args(host="127.0.0.1")
        policy, _, _ = server._validate_transport_security_or_exit(
            label="community", args=args
        )
        auth_mw = server._build_community_middleware("the-psk", host="127.0.0.1")
        app = _assemble_app(auth_middleware=auth_mw, policy=policy)

        status, headers, _ = _drive(
            app,
            headers={HEADER_PSK: "the-psk"},
            client=("203.0.113.5", 51234),
        )
        assert status == 426
        assert b"upgrade" in headers
        # Auth never ran -> no WWW-Authenticate.
        assert b"www-authenticate" not in headers

    def test_disabled_auth_loopback_only_lets_request_through(self):
        """When auth is disabled, a loopback request reaches the inner app."""
        args = _make_args(host="127.0.0.1")
        policy, _, _ = server._validate_transport_security_or_exit(
            label="community", args=args
        )
        # state=None mirrors auth.enabled=false on loopback (allowed).
        auth_mw = server._build_community_middleware(None, host="127.0.0.1")
        assert auth_mw == []  # no auth layer at all
        app = _assemble_app(auth_middleware=auth_mw, policy=policy)

        status, _, body = _drive(app)  # no headers, loopback peer
        assert status == 200
        # No auth ran -> principal is None in the echo body.
        assert b'"subject":null' in body or b'"subject": null' in body


# ---------------------------------------------------------------------------
# Enterprise server: password backend + TLS
# ---------------------------------------------------------------------------


class TestEnterpriseFullStack:
    """End-to-end stack for the enterprise server's auth+TLS layering."""

    def test_password_succeeds_on_loopback(self):
        args = _make_args(host="127.0.0.1")
        policy, _, _ = server._validate_transport_security_or_exit(
            label="enterprise", args=args
        )
        auth_mw = server._build_enterprise_middleware(
            (["password"], False), "127.0.0.1"
        )
        app = _assemble_app(auth_middleware=auth_mw, policy=policy)

        status, _, body = _drive(
            app,
            headers={HEADER_USERNAME: "alice", HEADER_PASSWORD: "pw"},
        )
        assert status == 200
        assert b'"subject":"alice"' in body or b'"subject": "alice"' in body
        assert b'"backend":"password"' in body or b'"backend": "password"' in body

    def test_missing_credentials_returns_401_with_password_challenge(self):
        args = _make_args(host="127.0.0.1")
        policy, _, _ = server._validate_transport_security_or_exit(
            label="enterprise", args=args
        )
        auth_mw = server._build_enterprise_middleware(
            (["password"], False), "127.0.0.1"
        )
        app = _assemble_app(auth_middleware=auth_mw, policy=policy)

        status, headers, _ = _drive(app)
        assert status == 401
        challenge = headers[b"www-authenticate"]
        assert b"DeephavenPassword" in challenge

    def test_tls_layer_blocks_cleartext_before_auth_reads_password(self):
        """Critical security property: a cleartext password never reaches auth."""
        args = _make_args(host="127.0.0.1")
        policy, _, _ = server._validate_transport_security_or_exit(
            label="enterprise", args=args
        )
        auth_mw = server._build_enterprise_middleware(
            (["password"], False), "127.0.0.1"
        )
        app = _assemble_app(auth_middleware=auth_mw, policy=policy)

        status, headers, _ = _drive(
            app,
            headers={HEADER_USERNAME: "alice", HEADER_PASSWORD: "pw"},
            client=("203.0.113.5", 51234),
        )
        assert status == 426
        # Auth layer never ran.
        assert b"www-authenticate" not in headers


# ---------------------------------------------------------------------------
# Decision-matrix integration: validation -> policy -> runtime behavior
# ---------------------------------------------------------------------------


class TestStartupValidationDrivesRuntimeBehavior:
    """The policy returned at startup must produce the documented runtime behavior."""

    def test_native_tls_path_runtime_passes_https_scheme(self, tmp_path):
        """``--ssl-keyfile/--ssl-certfile`` -> https scheme is honored."""
        key_file = tmp_path / "k.pem"
        cert_file = tmp_path / "c.pem"
        key_file.write_text("dummy")
        cert_file.write_text("dummy")
        args = _make_args(
            host="0.0.0.0",
            ssl_keyfile=str(key_file),
            ssl_certfile=str(cert_file),
        )
        policy, key, cert = server._validate_transport_security_or_exit(
            label="enterprise", args=args
        )
        assert (key, cert) == (str(key_file), str(cert_file))
        auth_mw = server._build_enterprise_middleware((["password"], False), "0.0.0.0")
        app = _assemble_app(auth_middleware=auth_mw, policy=policy)

        status, _, _ = _drive(
            app,
            headers={HEADER_USERNAME: "u", HEADER_PASSWORD: "p"},
            scheme="https",
            client=("203.0.113.5", 51234),
        )
        assert status == 200

    def test_trusted_proxy_path_runtime_honors_x_forwarded_proto(self):
        args = _make_args(
            host="0.0.0.0",
            trust_forwarded_proto=True,
            forwarded_allow_ips="10.0.0.0/8",
        )
        policy, _, _ = server._validate_transport_security_or_exit(
            label="enterprise", args=args
        )
        auth_mw = server._build_enterprise_middleware((["password"], False), "0.0.0.0")
        app = _assemble_app(auth_middleware=auth_mw, policy=policy)

        status, _, _ = _drive(
            app,
            headers={
                HEADER_USERNAME: "u",
                HEADER_PASSWORD: "p",
                "x-forwarded-proto": "https",
            },
            client=("10.5.5.5", 51234),
        )
        assert status == 200

    def test_allow_cleartext_path_runtime_passes_plain_http_anywhere(self):
        args = _make_args(host="0.0.0.0", allow_cleartext=True)
        policy, _, _ = server._validate_transport_security_or_exit(
            label="enterprise", args=args
        )
        auth_mw = server._build_enterprise_middleware((["password"], False), "0.0.0.0")
        app = _assemble_app(auth_middleware=auth_mw, policy=policy)

        status, _, _ = _drive(
            app,
            headers={HEADER_USERNAME: "u", HEADER_PASSWORD: "p"},
            client=("198.51.100.42", 51234),
        )
        assert status == 200

    def test_validation_refuses_to_start_for_misconfiguration(self):
        """No transport-security opt-in on non-loopback bind -> SystemExit."""
        args = _make_args(host="0.0.0.0")
        with pytest.raises(SystemExit) as exc_info:
            server._validate_transport_security_or_exit(label="enterprise", args=args)
        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# /health bypass: both layers must let probes through
# ---------------------------------------------------------------------------


class TestHealthBypassesBothLayers:
    """``/health`` must succeed cleartext, non-loopback, no credentials.

    Health probes from arbitrary load balancers or orchestrator agents
    cannot present TLS or auth, but they must succeed. The systems
    servers achieve this by configuring both middleware layers with
    ``bypass_paths`` containing ``/health`` (sourced from
    :data:`deephaven_mcp._health.HEALTH_PATH`): the systems-server
    startup helper passes it to :class:`TransportSecurityPolicy`, and
    the auth-middleware builders pass it explicitly. These tests
    assert both bypasses fire end-to-end.
    """

    def test_community_health_passes_cleartext_no_creds(self):
        """No PSK, non-loopback peer, plain HTTP -> 200 from /health."""
        args = _make_args(host="127.0.0.1")
        policy, _, _ = server._validate_transport_security_or_exit(
            label="community", args=args
        )
        auth_mw = server._build_community_middleware("the-psk", host="127.0.0.1")
        app = _assemble_app(auth_middleware=auth_mw, policy=policy)

        status, _, body = _drive(
            app,
            headers=None,
            client=("203.0.113.5", 51234),
            path="/health",
        )
        assert status == 200
        assert b'"status":"ok"' in body or b'"status": "ok"' in body

    def test_enterprise_health_passes_cleartext_no_creds(self):
        """No username/password, non-loopback peer, plain HTTP -> 200 from /health."""
        args = _make_args(host="127.0.0.1")
        policy, _, _ = server._validate_transport_security_or_exit(
            label="enterprise", args=args
        )
        auth_mw = server._build_enterprise_middleware(
            (["password"], False), "127.0.0.1"
        )
        app = _assemble_app(auth_middleware=auth_mw, policy=policy)

        status, _, body = _drive(
            app,
            headers=None,
            client=("203.0.113.5", 51234),
            path="/health",
        )
        assert status == 200
        assert b'"status":"ok"' in body or b'"status": "ok"' in body

    def test_health_bypass_is_exact_match_only(self):
        """``/healthz`` must NOT be bypassed (close-but-not-equal)."""
        args = _make_args(host="127.0.0.1")
        policy, _, _ = server._validate_transport_security_or_exit(
            label="community", args=args
        )
        auth_mw = server._build_community_middleware("the-psk", host="127.0.0.1")
        app = _assemble_app(auth_middleware=auth_mw, policy=policy)

        status, _, _ = _drive(
            app,
            client=("203.0.113.5", 51234),
            path="/healthz",
        )
        # TLS layer rejects before either auth or the inner router runs.
        assert status == 426
