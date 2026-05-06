"""Smoke tests for deephaven_mcp.auth.backends public re-export surface.

Verifies that all symbols declared in ``__all__`` are importable from the
package and have the expected identity (i.e. re-exports are the canonical
objects, not shadows). Behavior of each symbol is tested in its own
dedicated test module (test__base.py, test__headers.py, test__psk.py,
test__password.py, test__private_key.py, test__resolve.py).
"""

import deephaven_mcp.auth.backends as backends_pkg
from deephaven_mcp.auth.backends import (
    HEADER_EFFECTIVE_USER,
    HEADER_PASSWORD,
    HEADER_PRIVATE_KEY,
    HEADER_PSK,
    HEADER_USERNAME,
    AuthBackend,
    AuthenticationError,
    PasswordBackend,
    PrivateKeyBackend,
    PSKBackend,
    authenticate_and_resolve,
)
from deephaven_mcp.auth.backends._base import AuthBackend as _CanonicalAuthBackend
from deephaven_mcp.auth.backends._base import (
    AuthenticationError as _CanonicalAuthenticationError,
)
from deephaven_mcp.auth.backends._headers import (
    HEADER_EFFECTIVE_USER as _CANON_HEADER_EFFECTIVE_USER,
)
from deephaven_mcp.auth.backends._headers import (
    HEADER_PASSWORD as _CANON_HEADER_PASSWORD,
)
from deephaven_mcp.auth.backends._headers import (
    HEADER_PRIVATE_KEY as _CANON_HEADER_PRIVATE_KEY,
)
from deephaven_mcp.auth.backends._headers import HEADER_PSK as _CANON_HEADER_PSK
from deephaven_mcp.auth.backends._headers import (
    HEADER_USERNAME as _CANON_HEADER_USERNAME,
)
from deephaven_mcp.auth.backends._password import PasswordBackend as _CanonicalPassword
from deephaven_mcp.auth.backends._private_key import (
    PrivateKeyBackend as _CanonicalPrivateKey,
)
from deephaven_mcp.auth.backends._psk import PSKBackend as _CanonicalPSK
from deephaven_mcp.auth.backends._resolve import (
    authenticate_and_resolve as _canonical_resolve,
)


def test_auth_backend_is_canonical():
    assert AuthBackend is _CanonicalAuthBackend


def test_authentication_error_is_canonical():
    assert AuthenticationError is _CanonicalAuthenticationError


def test_psk_backend_is_canonical():
    assert PSKBackend is _CanonicalPSK
    assert issubclass(PSKBackend, AuthBackend)


def test_password_backend_is_canonical():
    assert PasswordBackend is _CanonicalPassword
    assert issubclass(PasswordBackend, AuthBackend)


def test_private_key_backend_is_canonical():
    assert PrivateKeyBackend is _CanonicalPrivateKey
    assert issubclass(PrivateKeyBackend, AuthBackend)


def test_authenticate_and_resolve_is_canonical():
    assert authenticate_and_resolve is _canonical_resolve


def test_header_constants_are_canonical():
    """Re-exported HEADER_* constants must be the same objects as those
    in _headers.py (the single source of truth). A shadow copy would
    silently drift if _headers.py were updated.
    """
    assert HEADER_EFFECTIVE_USER is _CANON_HEADER_EFFECTIVE_USER
    assert HEADER_PASSWORD is _CANON_HEADER_PASSWORD
    assert HEADER_PRIVATE_KEY is _CANON_HEADER_PRIVATE_KEY
    assert HEADER_PSK is _CANON_HEADER_PSK
    assert HEADER_USERNAME is _CANON_HEADER_USERNAME


def test_all_surface_importable():
    """All symbols in ``__all__`` can be imported from the package."""
    for name in backends_pkg.__all__:
        assert hasattr(
            backends_pkg, name
        ), f"{name!r} declared in __all__ but not found on package"


def test_all_surface_complete():
    """``__all__`` matches the documented re-export surface.

    Regression guard: if someone adds a new public symbol to the
    package (or removes one), this test forces them to update
    ``__all__`` explicitly rather than letting the surface drift.
    """
    assert set(backends_pkg.__all__) == {
        "AuthBackend",
        "AuthenticationError",
        "HEADER_EFFECTIVE_USER",
        "HEADER_PASSWORD",
        "HEADER_PRIVATE_KEY",
        "HEADER_PSK",
        "HEADER_USERNAME",
        "PasswordBackend",
        "PSKBackend",
        "PrivateKeyBackend",
        "authenticate_and_resolve",
    }
