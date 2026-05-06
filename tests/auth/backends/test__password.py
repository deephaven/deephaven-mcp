"""Tests for deephaven_mcp.auth.backends._password."""

import pytest

from deephaven_mcp.auth.backends import (
    AuthBackend,
    AuthenticationError,
    PasswordBackend,
)
from deephaven_mcp.auth.credentials import PasswordCredentials, Principal


def test_conforms_to_auth_backend_protocol():
    assert isinstance(PasswordBackend(), AuthBackend)


def test_name_is_stable():
    assert PasswordBackend.name == "password"


def test_default_disallows_effective_user():
    assert PasswordBackend().allow_effective_user is False


def test_default_realm():
    assert PasswordBackend().realm == "deephaven-mcp"


def test_realm_is_overridable():
    assert PasswordBackend(realm="custom").realm == "custom"


@pytest.mark.asyncio
async def test_missing_password_header_returns_none():
    backend = PasswordBackend()
    assert await backend.authenticate({}) is None


@pytest.mark.asyncio
async def test_empty_password_raises():
    backend = PasswordBackend()
    with pytest.raises(AuthenticationError, match="must not be empty"):
        await backend.authenticate(
            {"x-deephaven-username": "alice", "x-deephaven-password": ""}
        )


@pytest.mark.asyncio
async def test_missing_username_raises():
    backend = PasswordBackend()
    with pytest.raises(AuthenticationError, match="x-deephaven-username"):
        await backend.authenticate({"x-deephaven-password": "pw"})


@pytest.mark.asyncio
async def test_empty_username_raises():
    backend = PasswordBackend()
    with pytest.raises(AuthenticationError, match="x-deephaven-username"):
        await backend.authenticate(
            {"x-deephaven-username": "", "x-deephaven-password": "pw"}
        )


@pytest.mark.asyncio
async def test_valid_password_returns_principal():
    backend = PasswordBackend()
    result = await backend.authenticate(
        {"x-deephaven-username": "alice", "x-deephaven-password": "pw"}
    )
    assert isinstance(result, Principal)
    assert result.subject == "alice"
    assert result.display_name == "alice"
    assert result.raw == {"backend": "password"}


@pytest.mark.asyncio
async def test_effective_user_disallowed_raises():
    backend = PasswordBackend(allow_effective_user=False)
    with pytest.raises(AuthenticationError, match="not permitted"):
        await backend.authenticate(
            {
                "x-deephaven-username": "alice",
                "x-deephaven-password": "pw",
                "x-deephaven-effective-user": "bob",
            }
        )


@pytest.mark.asyncio
async def test_effective_user_ignored_when_empty_even_if_disallowed():
    # An empty header is treated as absent.
    backend = PasswordBackend(allow_effective_user=False)
    result = await backend.authenticate(
        {
            "x-deephaven-username": "alice",
            "x-deephaven-password": "pw",
            "x-deephaven-effective-user": "",
        }
    )
    assert isinstance(result, Principal)
    assert "effective_user" not in result.raw


@pytest.mark.asyncio
async def test_effective_user_allowed_captured_on_principal():
    backend = PasswordBackend(allow_effective_user=True)
    result = await backend.authenticate(
        {
            "x-deephaven-username": "alice",
            "x-deephaven-password": "pw",
            "x-deephaven-effective-user": "bob",
        }
    )
    assert isinstance(result, Principal)
    assert result.raw["effective_user"] == "bob"


@pytest.mark.asyncio
async def test_derive_credentials_returns_password_creds():
    backend = PasswordBackend()
    principal = Principal(subject="alice", display_name="alice")
    creds = await backend.derive_credentials(
        principal,
        {"x-deephaven-username": "alice", "x-deephaven-password": "pw"},
    )
    assert isinstance(creds, PasswordCredentials)
    assert creds.username == "alice"
    assert creds.password == "pw"
    assert creds.effective_user is None


@pytest.mark.asyncio
async def test_derive_credentials_with_effective_user():
    backend = PasswordBackend(allow_effective_user=True)
    principal = Principal(subject="alice", display_name="alice")
    creds = await backend.derive_credentials(
        principal,
        {
            "x-deephaven-username": "alice",
            "x-deephaven-password": "pw",
            "x-deephaven-effective-user": "bob",
        },
    )
    assert creds.effective_user == "bob"


@pytest.mark.asyncio
async def test_derive_credentials_ignores_effective_user_when_disallowed():
    backend = PasswordBackend(allow_effective_user=False)
    principal = Principal(subject="alice", display_name="alice")
    # Would be rejected by authenticate() first; derive_credentials is
    # still defensive.
    creds = await backend.derive_credentials(
        principal,
        {
            "x-deephaven-username": "alice",
            "x-deephaven-password": "pw",
            "x-deephaven-effective-user": "bob",
        },
    )
    assert creds.effective_user is None


@pytest.mark.asyncio
async def test_derive_credentials_empty_effective_user_when_allowed():
    backend = PasswordBackend(allow_effective_user=True)
    principal = Principal(subject="alice", display_name="alice")
    creds = await backend.derive_credentials(
        principal,
        {
            "x-deephaven-username": "alice",
            "x-deephaven-password": "pw",
            "x-deephaven-effective-user": "",
        },
    )
    assert creds.effective_user is None


def test_challenge_mentions_expected_headers_with_default_realm():
    challenge = PasswordBackend().challenge()
    assert 'realm="deephaven-mcp"' in challenge
    assert "x-deephaven-username" in challenge
    assert "x-deephaven-password" in challenge


def test_challenge_uses_configured_realm():
    challenge = PasswordBackend(realm="custom-realm").challenge()
    assert 'realm="custom-realm"' in challenge
