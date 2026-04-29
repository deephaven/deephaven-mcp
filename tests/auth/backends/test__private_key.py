"""Tests for deephaven_mcp.auth.backends._private_key."""

import base64

import pytest

from deephaven_mcp.auth.backends import (
    AuthBackend,
    AuthenticationError,
    PrivateKeyBackend,
)
from deephaven_mcp.auth.credentials import Principal, PrivateKeyCredentials


def _encode(key: bytes) -> str:
    return base64.b64encode(key).decode("ascii")


def test_conforms_to_auth_backend_protocol():
    assert isinstance(PrivateKeyBackend(), AuthBackend)


def test_name_is_stable():
    assert PrivateKeyBackend.name == "private_key"


def test_default_realm():
    assert PrivateKeyBackend().realm == "deephaven-mcp"


def test_realm_is_overridable():
    assert PrivateKeyBackend(realm="custom").realm == "custom"


@pytest.mark.asyncio
async def test_missing_key_header_returns_none():
    backend = PrivateKeyBackend()
    assert await backend.authenticate({}) is None


@pytest.mark.asyncio
async def test_empty_key_header_raises():
    backend = PrivateKeyBackend()
    with pytest.raises(AuthenticationError, match="must not be empty"):
        await backend.authenticate(
            {
                "x-deephaven-username": "alice",
                "x-deephaven-private-key": "",
            }
        )


@pytest.mark.asyncio
async def test_missing_username_raises():
    backend = PrivateKeyBackend()
    with pytest.raises(AuthenticationError, match="x-deephaven-username"):
        await backend.authenticate({"x-deephaven-private-key": _encode(b"keydata")})


@pytest.mark.asyncio
async def test_empty_username_raises():
    backend = PrivateKeyBackend()
    with pytest.raises(AuthenticationError, match="x-deephaven-username"):
        await backend.authenticate(
            {
                "x-deephaven-username": "",
                "x-deephaven-private-key": _encode(b"keydata"),
            }
        )


@pytest.mark.asyncio
async def test_invalid_base64_raises():
    backend = PrivateKeyBackend()
    with pytest.raises(AuthenticationError, match="not valid base64"):
        await backend.authenticate(
            {
                "x-deephaven-username": "alice",
                "x-deephaven-private-key": "not base64!!",
            }
        )


@pytest.mark.asyncio
async def test_valid_key_returns_principal():
    backend = PrivateKeyBackend()
    result = await backend.authenticate(
        {
            "x-deephaven-username": "alice",
            "x-deephaven-private-key": _encode(b"keydata"),
        }
    )
    assert isinstance(result, Principal)
    assert result.subject == "alice"
    assert result.display_name == "alice"
    assert result.raw == {"backend": "private_key"}


@pytest.mark.asyncio
async def test_derive_credentials_decodes_key_text():
    backend = PrivateKeyBackend()
    raw_text = "DH key material\n"
    principal = Principal(subject="alice", display_name="alice")
    creds = await backend.derive_credentials(
        principal,
        {
            "x-deephaven-username": "alice",
            "x-deephaven-private-key": _encode(raw_text.encode("utf-8")),
        },
    )
    assert isinstance(creds, PrivateKeyCredentials)
    assert creds.key_text == raw_text


@pytest.mark.asyncio
async def test_derive_credentials_rejects_non_utf8_key_bytes():
    """The backend validates that the base64-decoded key material is
    valid UTF-8 so that :class:`PrivateKeyCredentials` can carry
    ``key_text: str`` and downstream consumers don't need to re-validate.
    Invalid UTF-8 must raise ``AuthenticationError`` at this layer, not
    propagate as an unhandled ``UnicodeDecodeError`` to a downstream
    consumer.
    """
    backend = PrivateKeyBackend()
    # b"\xff\xfe\xfd" is never valid UTF-8 (no leading byte pattern
    # matches 0xff). Base64-encoding it gives us a header value that
    # passes the base64 validation in authenticate() but fails the UTF-8
    # decode in derive_credentials().
    principal = Principal(subject="alice", display_name="alice")
    with pytest.raises(AuthenticationError, match="not valid UTF-8"):
        await backend.derive_credentials(
            principal,
            {
                "x-deephaven-username": "alice",
                "x-deephaven-private-key": _encode(b"\xff\xfe\xfd"),
            },
        )


def test_challenge_mentions_expected_headers_with_default_realm():
    challenge = PrivateKeyBackend().challenge()
    assert 'realm="deephaven-mcp"' in challenge
    assert "x-deephaven-username" in challenge
    assert "x-deephaven-private-key" in challenge


def test_challenge_uses_configured_realm():
    challenge = PrivateKeyBackend(realm="custom-realm").challenge()
    assert 'realm="custom-realm"' in challenge
