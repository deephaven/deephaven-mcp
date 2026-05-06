"""Tests for deephaven_mcp.auth.middleware._middleware.AuthenticationMiddleware."""

import json

import pytest

from deephaven_mcp.auth.backends import AuthenticationError
from deephaven_mcp.auth.credentials import (
    PasswordCredentials,
    Principal,
    PSKCredentials,
)
from deephaven_mcp.auth.middleware import (
    SCOPE_KEY_CREDENTIALS,
    SCOPE_KEY_PRINCIPAL,
    AuthenticationMiddleware,
)


class _RecordingApp:
    """ASGI app that records the (scope, events) it was called with."""

    def __init__(self):
        self.calls: list[dict] = []

    async def __call__(self, scope, receive, send):
        self.calls.append(scope)
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"ok"})


class _StaticBackend:
    name = "static"

    def __init__(self, token: str):
        self._token = token

    async def authenticate(self, headers):
        header = headers.get("authorization")
        if header is None:
            return None
        if header != f"Bearer {self._token}":
            raise AuthenticationError("bad token")
        return Principal(subject="community", display_name="community")

    async def derive_credentials(self, principal, headers):
        return PSKCredentials(psk="x")

    def challenge(self):
        return 'Bearer realm="test"'


class _UserHeaderBackend:
    name = "user"

    async def authenticate(self, headers):
        if "x-user" not in headers:
            return None
        return Principal(subject=headers["x-user"], display_name=headers["x-user"])

    async def derive_credentials(self, principal, headers):
        return PasswordCredentials(
            username=principal.subject, password=headers["x-password"]
        )

    def challenge(self):
        return 'DeephavenHeaders realm="enterprise"'


async def _receive_empty():
    return {"type": "http.request", "body": b"", "more_body": False}


async def _collect_send(sink: list[dict]):
    async def _send(msg):
        sink.append(msg)

    return _send


def _http_scope(path: str = "/mcp", headers: dict[str, str] | None = None) -> dict:
    encoded = [
        (name.encode("latin-1"), value.encode("latin-1"))
        for name, value in (headers or {}).items()
    ]
    return {
        "type": "http",
        "path": path,
        "headers": encoded,
        "method": "POST",
    }


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_init_requires_at_least_one_backend():
    with pytest.raises(ValueError, match="at least one backend"):
        AuthenticationMiddleware(app=_RecordingApp(), backends=())


def test_backends_are_stored_as_tuple():
    backend = _StaticBackend("t")
    mw = AuthenticationMiddleware(app=_RecordingApp(), backends=[backend])
    assert mw.backends == (backend,)


# ---------------------------------------------------------------------------
# Request handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_valid_request_populates_scope_and_calls_inner_app():
    app = _RecordingApp()
    mw = AuthenticationMiddleware(app=app, backends=[_StaticBackend("secret")])
    scope = _http_scope(headers={"Authorization": "Bearer secret"})
    sent: list[dict] = []
    send = await _collect_send(sent)

    await mw(scope, _receive_empty, send)

    assert len(app.calls) == 1
    assert app.calls[0] is scope
    assert scope[SCOPE_KEY_PRINCIPAL].subject == "community"
    assert isinstance(scope[SCOPE_KEY_CREDENTIALS], PSKCredentials)
    assert sent[0]["status"] == 200


@pytest.mark.asyncio
async def test_missing_credentials_returns_401_with_www_authenticate():
    app = _RecordingApp()
    mw = AuthenticationMiddleware(
        app=app, backends=[_StaticBackend("secret"), _UserHeaderBackend()]
    )
    scope = _http_scope()
    sent: list[dict] = []
    send = await _collect_send(sent)

    await mw(scope, _receive_empty, send)

    assert app.calls == []
    assert sent[0]["status"] == 401
    header_map = {name: value for name, value in sent[0]["headers"]}
    auth_header = header_map[b"www-authenticate"].decode()
    assert "Bearer" in auth_header
    assert "DeephavenHeaders" in auth_header
    # JSON body mentions "unauthorized" and -- per the consolidated chain
    # runner -- lists the backends that were attempted.
    body = json.loads(sent[1]["body"])
    assert body["error"] == "unauthorized"
    assert "tried:" in body["detail"]
    assert "static" in body["detail"]
    assert "user" in body["detail"]


@pytest.mark.asyncio
async def test_bad_credentials_short_circuits_with_401():
    app = _RecordingApp()
    mw = AuthenticationMiddleware(
        app=app, backends=[_StaticBackend("secret"), _UserHeaderBackend()]
    )
    scope = _http_scope(headers={"Authorization": "Bearer wrong"})
    sent: list[dict] = []
    send = await _collect_send(sent)

    await mw(scope, _receive_empty, send)

    assert app.calls == []
    assert sent[0]["status"] == 401
    # User backend is NOT tried once the token backend raised.
    body = json.loads(sent[1]["body"])
    assert body["detail"] == "bad token"


@pytest.mark.asyncio
async def test_second_backend_matches_when_first_returns_none():
    app = _RecordingApp()
    mw = AuthenticationMiddleware(
        app=app, backends=[_StaticBackend("secret"), _UserHeaderBackend()]
    )
    scope = _http_scope(headers={"X-User": "alice", "X-Password": "pw"})
    sent: list[dict] = []
    send = await _collect_send(sent)

    await mw(scope, _receive_empty, send)

    assert len(app.calls) == 1
    assert scope[SCOPE_KEY_PRINCIPAL].subject == "alice"
    assert isinstance(scope[SCOPE_KEY_CREDENTIALS], PasswordCredentials)


@pytest.mark.asyncio
async def test_non_http_scope_passes_through_unchanged():
    app = _RecordingApp()
    mw = AuthenticationMiddleware(app=app, backends=[_StaticBackend("secret")])
    scope = {"type": "lifespan"}
    sent: list[dict] = []
    send = await _collect_send(sent)

    await mw(scope, _receive_empty, send)

    assert app.calls == [scope]
    assert SCOPE_KEY_PRINCIPAL not in scope


@pytest.mark.asyncio
async def test_bypass_paths_allow_unauthenticated_access():
    app = _RecordingApp()
    mw = AuthenticationMiddleware(
        app=app,
        backends=[_StaticBackend("secret")],
        bypass_paths=frozenset({"/.well-known/oauth-protected-resource"}),
    )
    scope = _http_scope(path="/.well-known/oauth-protected-resource")
    sent: list[dict] = []
    send = await _collect_send(sent)

    await mw(scope, _receive_empty, send)

    assert len(app.calls) == 1
    assert SCOPE_KEY_PRINCIPAL not in scope


# ---------------------------------------------------------------------------
# Header handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_headers_are_lowercased_before_backends_see_them():
    """Backends always read lowercase header keys. A client sending
    ``Authorization: Bearer ...`` (capitalized, as browsers and most
    HTTP clients do) must reach a backend that looks up
    ``headers["authorization"]``. Pins down the contract that the
    middleware's ``_lower_headers`` helper is responsible for the
    lowercasing, not each individual backend.
    """
    app = _RecordingApp()
    mw = AuthenticationMiddleware(app=app, backends=[_StaticBackend("secret")])
    # Note: capitalized header name; the backend's authenticate() reads
    # the lowercase form.
    scope = _http_scope(headers={"Authorization": "Bearer secret"})
    sent: list[dict] = []
    send = await _collect_send(sent)

    await mw(scope, _receive_empty, send)

    assert len(app.calls) == 1
    assert sent[0]["status"] == 200


@pytest.mark.asyncio
async def test_duplicate_header_later_value_wins():
    """``_lower_headers`` promises that when the same header appears
    twice the later value overwrites the earlier one. Pin this down so
    a future refactor (e.g. switching to a multi-map) doesn't silently
    change semantics for the auth headers.
    """
    app = _RecordingApp()
    mw = AuthenticationMiddleware(app=app, backends=[_StaticBackend("secret")])
    scope = {
        "type": "http",
        "path": "/mcp",
        "method": "POST",
        "headers": [
            (b"authorization", b"Bearer wrong"),
            (b"authorization", b"Bearer secret"),
        ],
    }
    sent: list[dict] = []
    send = await _collect_send(sent)

    await mw(scope, _receive_empty, send)

    # Second header value won; request was authenticated and forwarded.
    assert len(app.calls) == 1
    assert sent[0]["status"] == 200


# ---------------------------------------------------------------------------
# 401 response shape
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_401_response_has_json_content_type_and_matching_content_length():
    """The 401 response must be a well-formed HTTP response: the
    ``content-type`` header advertises JSON, and the declared
    ``content-length`` matches the actual body length (otherwise
    proxies and clients will mis-frame the response).
    """
    app = _RecordingApp()
    mw = AuthenticationMiddleware(app=app, backends=[_StaticBackend("secret")])
    scope = _http_scope()
    sent: list[dict] = []
    send = await _collect_send(sent)

    await mw(scope, _receive_empty, send)

    header_map = {name: value for name, value in sent[0]["headers"]}
    assert header_map[b"content-type"] == b"application/json"
    body_bytes = sent[1]["body"]
    assert header_map[b"content-length"] == str(len(body_bytes)).encode()


@pytest.mark.asyncio
async def test_401_body_is_valid_json_with_error_and_detail():
    """The 401 body must be parseable JSON with the documented shape
    (``error`` and ``detail`` keys). Consumers reading the body
    programmatically rely on this.
    """
    app = _RecordingApp()
    mw = AuthenticationMiddleware(app=app, backends=[_StaticBackend("secret")])
    scope = _http_scope()
    sent: list[dict] = []
    send = await _collect_send(sent)

    await mw(scope, _receive_empty, send)

    body = json.loads(sent[1]["body"])
    assert body["error"] == "unauthorized"
    assert isinstance(body["detail"], str)
    assert body["detail"]  # non-empty


# ---------------------------------------------------------------------------
# Regression: downstream AuthenticationError must not be swallowed
# ---------------------------------------------------------------------------


class _RaisingApp:
    """ASGI app that raises ``AuthenticationError`` from within itself.

    Used to verify the middleware does NOT catch exceptions from the
    downstream app. Before the refactor that consolidated the chain
    runner, the ``await self.app(...)`` call lived inside the
    ``try/except AuthenticationError`` block and would silently
    intercept any such exception, mangling the response.
    """

    async def __call__(self, scope, receive, send):
        raise AuthenticationError("from inside downstream app")


@pytest.mark.asyncio
async def test_downstream_authentication_error_is_not_swallowed():
    """If the inner ASGI app itself raises ``AuthenticationError``
    (e.g. a deeper auth check failed), the middleware must let the
    exception propagate rather than catching it and emitting a 401.
    The middleware's try/except is scoped to the authentication step
    only -- once authentication succeeds and control passes to the
    inner app, the middleware is no longer in the error-handling path.
    """
    mw = AuthenticationMiddleware(
        app=_RaisingApp(), backends=[_StaticBackend("secret")]
    )
    scope = _http_scope(headers={"Authorization": "Bearer secret"})
    sent: list[dict] = []
    send = await _collect_send(sent)

    with pytest.raises(AuthenticationError, match="from inside downstream app"):
        await mw(scope, _receive_empty, send)

    # Middleware must NOT have emitted a 401 in response to the
    # downstream exception -- control never got there.
    assert sent == []
