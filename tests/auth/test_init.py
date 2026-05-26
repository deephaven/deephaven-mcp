"""Tests for ``deephaven_mcp.auth`` package surface.

The ``auth`` package itself exports nothing; it is a documented
container for three peer subpackages (``credentials``, ``tls``,
``middleware``). These tests pin those structural invariants so a
refactor cannot silently rename, drop, or merge a subpackage.
"""

from __future__ import annotations

import importlib

import deephaven_mcp.auth as auth_pkg


def test_auth_package_exports_no_public_names() -> None:
    """The ``auth`` package is a container; it must not re-export symbols.

    Public consumers must import from the subpackages
    (``deephaven_mcp.auth.credentials`` / ``.tls`` / ``.middleware``)
    rather than from ``deephaven_mcp.auth`` directly.
    """
    declared = getattr(auth_pkg, "__all__", [])
    assert list(declared) == []


def test_auth_subpackages_are_importable() -> None:
    """All three documented subpackages must be importable as attributes."""
    for sub in ("credentials", "tls", "middleware"):
        mod = importlib.import_module(f"deephaven_mcp.auth.{sub}")
        assert mod.__name__ == f"deephaven_mcp.auth.{sub}"


def test_auth_package_has_module_docstring() -> None:
    """The package docstring documents the three-subpackage layout."""
    doc = auth_pkg.__doc__ or ""
    assert "credentials" in doc
    assert "tls" in doc
    assert "middleware" in doc
