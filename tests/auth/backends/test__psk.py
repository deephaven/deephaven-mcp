"""Tests for deephaven_mcp.auth.backends._psk."""

import pytest

from deephaven_mcp.auth.backends import (
    AuthBackend,
    AuthenticationError,
    PSKBackend,
)
from deephaven_mcp.auth.credentials import Principal, PSKCredentials


def test_requires_non_empty_psk():
    with pytest.raises(ValueError, match="non-empty"):
        PSKBackend(expected_psk="")


def test_conforms_to_auth_backend_protocol():
    backend = PSKBackend(expected_psk="abc")
    assert isinstance(backend, AuthBackend)


def test_name_is_stable():
    assert PSKBackend.name == "psk"


def test_default_principal_subject_and_realm():
    backend = PSKBackend(expected_psk="abc")
    assert backend.principal_subject == "psk"
    assert backend.realm == "deephaven-mcp"


def test_principal_subject_and_realm_are_overridable():
    backend = PSKBackend(
        expected_psk="abc",
        principal_subject="my-service",
        realm="my-realm",
    )
    assert backend.principal_subject == "my-service"
    assert backend.realm == "my-realm"


@pytest.mark.asyncio
async def test_missing_psk_header_returns_none():
    backend = PSKBackend(expected_psk="abc")
    result = await backend.authenticate({})
    assert result is None


@pytest.mark.asyncio
async def test_valid_psk_returns_principal():
    backend = PSKBackend(expected_psk="abc")
    result = await backend.authenticate({"x-deephaven-psk": "abc"})
    assert isinstance(result, Principal)
    assert result.subject == "psk"
    assert result.display_name == "psk"
    assert result.raw == {"backend": "psk"}


@pytest.mark.asyncio
async def test_principal_subject_propagates_to_principal():
    backend = PSKBackend(expected_psk="abc", principal_subject="my-service")
    result = await backend.authenticate({"x-deephaven-psk": "abc"})
    assert isinstance(result, Principal)
    assert result.subject == "my-service"
    assert result.display_name == "my-service"


@pytest.mark.asyncio
async def test_empty_psk_header_raises():
    backend = PSKBackend(expected_psk="abc")
    with pytest.raises(AuthenticationError, match="must not be empty"):
        await backend.authenticate({"x-deephaven-psk": ""})


@pytest.mark.asyncio
async def test_wrong_psk_raises():
    backend = PSKBackend(expected_psk="abc")
    with pytest.raises(AuthenticationError, match="Invalid pre-shared key"):
        await backend.authenticate({"x-deephaven-psk": "wrong"})


@pytest.mark.asyncio
async def test_authorization_bearer_header_is_ignored():
    # The legacy Authorization: Bearer header is no longer recognised.
    # A request carrying only that header should fall through (return None)
    # so other backends in the chain can claim it.
    backend = PSKBackend(expected_psk="abc")
    result = await backend.authenticate({"authorization": "Bearer abc"})
    assert result is None


@pytest.mark.asyncio
async def test_derive_credentials_returns_psk_credentials_with_value():
    backend = PSKBackend(expected_psk="abc")
    principal = Principal(subject="psk", display_name="psk")
    creds = await backend.derive_credentials(principal, {"x-deephaven-psk": "abc"})
    assert isinstance(creds, PSKCredentials)
    assert creds.psk == "abc"


@pytest.mark.asyncio
async def test_derive_credentials_forwards_observed_header_value():
    """Regression guard: the credential must carry the value from the
    request headers, NOT the server's configured ``expected_psk``.

    In a real authenticated flow the two are byte-equal (because
    ``hmac.compare_digest`` only lets matching values through), so we
    deliberately call ``derive_credentials`` directly with a different
    header value to prove which source the credential is built from.
    The test fails immediately if a future refactor reverts to using
    ``self.expected_psk`` -- preventing a TOCTOU regression if
    ``expected_psk`` ever becomes mutable (e.g. config hot-reload).
    """
    backend = PSKBackend(expected_psk="server-configured-psk")
    principal = Principal(subject="psk", display_name="psk")
    creds = await backend.derive_credentials(
        principal,
        {"x-deephaven-psk": "value-from-this-request"},
    )
    assert creds.psk == "value-from-this-request"
    assert creds.psk != backend.expected_psk


@pytest.mark.asyncio
async def test_derive_credentials_raises_keyerror_without_header():
    """``derive_credentials`` is only valid after ``authenticate`` has
    returned a Principal, which guarantees the header is present. If a
    caller violates that contract (calls ``derive_credentials`` without
    a prior successful ``authenticate``), the missing header surfaces
    as a ``KeyError`` rather than silently producing a credential with
    the wrong value.
    """
    backend = PSKBackend(expected_psk="abc")
    principal = Principal(subject="psk", display_name="psk")
    with pytest.raises(KeyError):
        await backend.derive_credentials(principal, {})


def test_challenge_includes_default_realm_and_header():
    backend = PSKBackend(expected_psk="abc")
    assert backend.challenge() == (
        'DeephavenPSK realm="deephaven-mcp", headers="x-deephaven-psk"'
    )


def test_challenge_uses_configured_realm():
    backend = PSKBackend(expected_psk="abc", realm="custom-realm")
    assert backend.challenge() == (
        'DeephavenPSK realm="custom-realm", headers="x-deephaven-psk"'
    )
