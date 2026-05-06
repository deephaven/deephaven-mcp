"""Smoke tests for deephaven_mcp.auth.middleware public re-export surface.

Verifies that all symbols declared in ``__all__`` are importable from the
package and have the expected identity (i.e. re-exports are the canonical
objects, not shadows). Behavior is tested in test__middleware.py.
"""

import deephaven_mcp.auth.middleware as middleware_pkg
from deephaven_mcp.auth.middleware import (
    SCOPE_KEY_CREDENTIALS,
    SCOPE_KEY_PRINCIPAL,
    AuthenticationMiddleware,
)
from deephaven_mcp.auth.middleware._middleware import (
    SCOPE_KEY_CREDENTIALS as _CANONICAL_SCOPE_KEY_CREDENTIALS,
)
from deephaven_mcp.auth.middleware._middleware import (
    SCOPE_KEY_PRINCIPAL as _CANONICAL_SCOPE_KEY_PRINCIPAL,
)
from deephaven_mcp.auth.middleware._middleware import (
    AuthenticationMiddleware as _CanonicalAuthenticationMiddleware,
)


def test_authentication_middleware_is_canonical():
    assert AuthenticationMiddleware is _CanonicalAuthenticationMiddleware


def test_scope_key_principal_is_canonical():
    assert SCOPE_KEY_PRINCIPAL is _CANONICAL_SCOPE_KEY_PRINCIPAL


def test_scope_key_credentials_is_canonical():
    assert SCOPE_KEY_CREDENTIALS is _CANONICAL_SCOPE_KEY_CREDENTIALS


def test_all_surface_importable():
    """All symbols in ``__all__`` can be imported from the package."""
    for name in middleware_pkg.__all__:
        assert hasattr(
            middleware_pkg, name
        ), f"{name!r} declared in __all__ but not found on package"


def test_all_surface_complete():
    """``__all__`` matches the documented re-export surface.

    Regression guard: adding a new public symbol without updating
    ``__all__`` (or vice-versa) fails this test loudly.
    """
    assert set(middleware_pkg.__all__) == {
        "AuthenticationMiddleware",
        "SCOPE_KEY_CREDENTIALS",
        "SCOPE_KEY_PRINCIPAL",
        # TLS enforcement (added with the transport-security work).
        "TlsEnforcementMiddleware",
        "TransportSecurityPolicy",
        "parse_forwarded_allow_ips",
    }
