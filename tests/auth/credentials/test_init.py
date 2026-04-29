"""Smoke tests for deephaven_mcp.auth.credentials public re-export surface.

Verifies that all symbols declared in __all__ are importable from the package
and have the expected identity. Behavior is tested in test__credentials.py
and test__principal.py.
"""

import deephaven_mcp.auth.credentials as creds_pkg
from deephaven_mcp.auth.credentials import (
    Credentials,
    PasswordCredentials,
    Principal,
    PrivateKeyCredentials,
    PSKCredentials,
)
from deephaven_mcp.auth.credentials._credentials import Credentials as _CanonicalCreds
from deephaven_mcp.auth.credentials._credentials import (
    PasswordCredentials as _CanonicalPassword,
)
from deephaven_mcp.auth.credentials._credentials import (
    PrivateKeyCredentials as _CanonicalPrivateKey,
)
from deephaven_mcp.auth.credentials._credentials import PSKCredentials as _CanonicalPSK
from deephaven_mcp.auth.credentials._principal import Principal as _CanonicalPrincipal


def test_credentials_is_canonical():
    assert Credentials is _CanonicalCreds


def test_psk_credentials_is_canonical():
    assert PSKCredentials is _CanonicalPSK
    assert issubclass(PSKCredentials, Credentials)


def test_password_credentials_is_canonical():
    assert PasswordCredentials is _CanonicalPassword
    assert issubclass(PasswordCredentials, Credentials)


def test_private_key_credentials_is_canonical():
    assert PrivateKeyCredentials is _CanonicalPrivateKey
    assert issubclass(PrivateKeyCredentials, Credentials)


def test_principal_is_canonical():
    assert Principal is _CanonicalPrincipal


def test_all_surface_importable():
    """All symbols in __all__ can be imported from the package."""
    for name in creds_pkg.__all__:
        assert hasattr(
            creds_pkg, name
        ), f"{name!r} declared in __all__ but not found on package"


def test_all_surface_complete():
    """__all__ matches the documented re-export surface."""
    assert set(creds_pkg.__all__) == {
        "Credentials",
        "PasswordCredentials",
        "Principal",
        "PrivateKeyCredentials",
        "PSKCredentials",
    }
