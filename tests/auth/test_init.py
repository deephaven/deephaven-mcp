"""Tests for deephaven_mcp.auth package top-level surface.

The ``deephaven_mcp.auth`` package is a pure namespace: its module
docstring promises that it "intentionally re-exports **nothing**" to
keep the layering (``middleware -> backends -> credentials``) visible
at every use-site. These tests pin down that invariant so a future
well-meaning refactor cannot silently start flattening the public API.

Behavior of the three subpackages is covered by their own test trees
(``tests/auth/backends/``, ``tests/auth/credentials/``,
``tests/auth/middleware/``).
"""

import deephaven_mcp.auth as auth_pkg


def test_auth_package_reexports_nothing():
    """The top-level ``auth`` package must not re-export any public names.

    Per the module docstring, consumers must import from the specific
    subpackage that owns the symbol (``deephaven_mcp.auth.credentials``,
    ``deephaven_mcp.auth.backends``, ``deephaven_mcp.auth.middleware``).
    """
    public = [name for name in vars(auth_pkg) if not name.startswith("_")]
    # Subpackage names are allowed (they appear as attributes once the
    # subpackages are imported), but no classes, functions, or other
    # public symbols should leak up to this level.
    allowed_subpackage_names = {"backends", "credentials", "middleware"}
    unexpected = set(public) - allowed_subpackage_names
    assert not unexpected, (
        f"deephaven_mcp.auth must re-export nothing, but found unexpected "
        f"public attribute(s): {sorted(unexpected)}"
    )


def test_auth_package_has_no_all():
    """``__all__`` must be absent (or empty) to reflect the no-reexport policy.

    Defining ``__all__`` would invite ``from deephaven_mcp.auth import *``
    style usage, which directly contradicts the documented intent that
    callers reach into the owning subpackage.
    """
    assert getattr(auth_pkg, "__all__", []) == []


def test_auth_package_has_a_docstring():
    """Documents the subpackage layering rule — required reading for
    anyone adding a new backend or credential kind.
    """
    assert auth_pkg.__doc__ is not None
    assert "middleware" in auth_pkg.__doc__
    assert "backends" in auth_pkg.__doc__
    assert "credentials" in auth_pkg.__doc__
