"""Tests for deephaven_mcp.auth._resolve.authenticate_and_resolve."""

import pytest

from deephaven_mcp.auth.backends import AuthenticationError, authenticate_and_resolve
from deephaven_mcp.auth.credentials import (
    PasswordCredentials,
    Principal,
    PSKCredentials,
)


class _PasswordHeaderBackend:
    name = "password"

    async def authenticate(self, headers):
        if "x-user" in headers:
            return Principal(subject=headers["x-user"], display_name=headers["x-user"])
        return None

    async def derive_credentials(self, principal, headers):
        return PasswordCredentials(
            username=principal.subject, password=headers["x-password"]
        )

    def challenge(self):
        return 'DeephavenHeaders realm="enterprise"'


class _TokenBackend:
    name = "token"

    def __init__(self, expected: str):
        self._expected = expected

    async def authenticate(self, headers):
        header = headers.get("authorization")
        if header is None:
            return None
        if header != f"Bearer {self._expected}":
            raise AuthenticationError("bad token")
        return Principal(subject="community", display_name="community")

    async def derive_credentials(self, principal, headers):
        return PSKCredentials(psk="a")

    def challenge(self):
        return 'Bearer realm="community"'


@pytest.mark.asyncio
async def test_first_matching_backend_wins():
    first = _TokenBackend("a")
    second = _PasswordHeaderBackend()
    principal, creds = await authenticate_and_resolve(
        [first, second], {"Authorization": "Bearer a"}
    )
    assert principal.subject == "community"
    assert isinstance(creds, PSKCredentials)


@pytest.mark.asyncio
async def test_headers_are_lowercased_before_backend_call():
    backend = _PasswordHeaderBackend()
    _, creds = await authenticate_and_resolve(
        [backend],
        {"X-User": "alice", "X-Password": "pw"},
    )
    assert isinstance(creds, PasswordCredentials)
    assert creds.username == "alice"
    assert creds.password == "pw"


@pytest.mark.asyncio
async def test_authentication_error_is_raised_immediately():
    backend = _TokenBackend("expected")
    with pytest.raises(AuthenticationError, match="bad token"):
        await authenticate_and_resolve([backend], {"Authorization": "Bearer wrong"})


@pytest.mark.asyncio
async def test_no_backend_matches_raises():
    backend = _TokenBackend("expected")
    with pytest.raises(AuthenticationError, match="No registered"):
        await authenticate_and_resolve([backend], {})


@pytest.mark.asyncio
async def test_no_backend_matches_error_lists_tried_backend_names():
    """The error message must name every backend that was attempted so a
    client/operator can tell which headers the server would have accepted.
    Regression guard against a return to a generic, undiagnosable message.
    """
    first = _TokenBackend("expected")
    second = _PasswordHeaderBackend()
    with pytest.raises(AuthenticationError) as exc_info:
        await authenticate_and_resolve([first, second], {})
    message = str(exc_info.value)
    assert "tried:" in message
    assert "token" in message
    assert "password" in message


@pytest.mark.asyncio
async def test_empty_backend_chain_raises_misconfiguration_error():
    """An empty backend chain is a server-side misconfiguration, not a
    client authentication failure. The error message must reflect that
    distinction so operators can diagnose it from logs.
    """
    with pytest.raises(AuthenticationError, match="No authentication backends"):
        await authenticate_and_resolve([], {"Authorization": "Bearer anything"})


@pytest.mark.asyncio
async def test_later_backends_are_tried_when_earlier_returns_none():
    first = _TokenBackend("t")
    second = _PasswordHeaderBackend()
    principal, creds = await authenticate_and_resolve(
        [first, second], {"X-User": "bob", "X-Password": "pw"}
    )
    assert principal.subject == "bob"
    assert isinstance(creds, PasswordCredentials)
