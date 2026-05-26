"""Tests for ``deephaven_mcp.auth.middleware._psk``.

``PSKMiddleware`` is a reusable Starlette middleware that gates an MCP
server's inbound HTTP transport on a single shared key. Tests below
cover:

- Constructor refusal of an empty PSK.
- Header acceptance for the configured PSK.
- 401 rejection for missing or wrong PSKs.
- Bypass-path forwarding without inspecting the header.
- Constant-time comparison of the presented key.
"""

from __future__ import annotations

import pytest
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from deephaven_mcp.auth.middleware import (
    PSK_HEADER_NAME,
    PSKMiddleware,
)
from deephaven_mcp.auth.middleware._psk import MINIMUM_PSK_LENGTH

# Sample PSKs at or above the minimum length used throughout the test
# fixtures. The middleware refuses anything shorter than
# ``MINIMUM_PSK_LENGTH`` so production-shaped tests must use a key of
# realistic length.
_VALID_PSK = "x" * MINIMUM_PSK_LENGTH
_OTHER_PSK = "y" * MINIMUM_PSK_LENGTH

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _hello(_request: Request) -> JSONResponse:
    return JSONResponse({"ok": True})


async def _health(_request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


def _client(*, expected_psk: str, bypass_paths: tuple[str, ...] = ()) -> TestClient:
    routes = [
        Route("/api", _hello, methods=["GET"]),
        Route("/health", _health, methods=["GET"]),
    ]
    middleware = [
        Middleware(
            PSKMiddleware,
            expected_psk=expected_psk,
            bypass_paths=bypass_paths,
        )
    ]
    app = Starlette(routes=routes, middleware=middleware)
    return TestClient(app)


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------


def test_psk_middleware_rejects_empty_psk():
    with pytest.raises(ValueError, match="at least"):
        PSKMiddleware(app=lambda *_: None, expected_psk="")


def test_psk_middleware_rejects_short_psk():
    """Refuse a non-empty but brute-forceable PSK at construction time."""
    with pytest.raises(ValueError, match="at least"):
        PSKMiddleware(app=lambda *_: None, expected_psk="x")


def test_psk_middleware_accepts_exactly_minimum_length_psk():
    """A PSK exactly ``MINIMUM_PSK_LENGTH`` characters long is accepted."""
    psk = "a" * MINIMUM_PSK_LENGTH
    # Construction should succeed without raising.
    PSKMiddleware(app=lambda *_: None, expected_psk=psk)


# ---------------------------------------------------------------------------
# Authenticated requests
# ---------------------------------------------------------------------------


def test_correct_psk_passes_through():
    client = _client(expected_psk=_VALID_PSK)
    response = client.get("/api", headers={PSK_HEADER_NAME: _VALID_PSK})
    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_missing_psk_returns_401():
    client = _client(expected_psk=_VALID_PSK)
    response = client.get("/api")
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == 'Deephaven-PSK realm="mcp"'
    body = response.json()
    assert body["error"] == "Unauthorized"
    assert body["code"] == "psk_missing"
    assert "missing" in body["detail"].lower()
    assert PSK_HEADER_NAME in body["detail"]
    assert "pre-shared key" in body["detail"].lower()


def test_wrong_psk_returns_401():
    client = _client(expected_psk=_VALID_PSK)
    response = client.get("/api", headers={PSK_HEADER_NAME: _OTHER_PSK})
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == 'Deephaven-PSK realm="mcp"'
    body = response.json()
    assert body["code"] == "psk_invalid"
    assert "does not match" in body["detail"].lower()
    assert PSK_HEADER_NAME in body["detail"]
    # Never leak the expected secret.
    assert _VALID_PSK not in body["detail"]


def test_empty_header_value_returns_401():
    client = _client(expected_psk=_VALID_PSK)
    response = client.get("/api", headers={PSK_HEADER_NAME: ""})
    assert response.status_code == 401
    body = response.json()
    assert body["code"] == "psk_invalid"


# ---------------------------------------------------------------------------
# Bypass paths
# ---------------------------------------------------------------------------


def test_bypass_path_skips_authentication():
    client = _client(expected_psk=_VALID_PSK, bypass_paths=("/health",))
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_bypass_path_requires_exact_match():
    """Bypass paths match by ``==``, not by prefix; subpaths still need the PSK."""
    client = _client(expected_psk=_VALID_PSK, bypass_paths=("/health",))
    # Subpath under a bypass entry is *not* bypassed: the middleware
    # rejects it with 401 because the path does not exactly equal
    # any configured bypass entry.
    routes_response = client.get("/health/extra")
    assert routes_response.status_code == 401


def test_non_bypass_path_still_requires_psk():
    client = _client(expected_psk=_VALID_PSK, bypass_paths=("/health",))
    response = client.get("/api")
    assert response.status_code == 401


# ---------------------------------------------------------------------------
# Constant-time comparison
# ---------------------------------------------------------------------------


def test_psk_comparison_is_constant_time(monkeypatch):
    """``hmac.compare_digest`` is used so that mismatch length doesn't short-circuit."""
    import deephaven_mcp.auth.middleware._psk as mod

    seen: list[tuple[bytes, bytes]] = []
    real_compare = mod.hmac.compare_digest

    def _spy(a, b):
        seen.append((a, b))
        return real_compare(a, b)

    monkeypatch.setattr(mod.hmac, "compare_digest", _spy)
    psk = "h" * MINIMUM_PSK_LENGTH
    client = _client(expected_psk=psk)
    client.get("/api", headers={PSK_HEADER_NAME: psk})
    assert seen == [(psk.encode("utf-8"), psk.encode("utf-8"))]
