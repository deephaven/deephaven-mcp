"""Tests for the ``deephaven_mcp._platform`` package ``__init__``."""

from __future__ import annotations

import deephaven_mcp._platform as platform_pkg


def test_package_has_docstring() -> None:
    """The package ``__init__`` is documented (and otherwise import-free)."""
    assert platform_pkg.__doc__
    assert "OS abstraction layer" in platform_pkg.__doc__


def test_package_exposes_no_public_api() -> None:
    """The ``__init__`` defines no ``__all__``; the contract lives in ``_os_support``.

    Submodules import the contract from the leaf ``_os_support`` module, not
    from this package, so the package surface stays empty and cannot become
    an import-cycle hub.
    """
    assert not hasattr(platform_pkg, "__all__")
    assert not hasattr(platform_pkg, "SUPPORTED_OS_NAMES")
