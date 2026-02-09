"""Tests for deephaven_mcp.mcp_systems_server._mcp module."""

import pytest


def test_mcp_module_exports_all_match():
    """Validate that _mcp.py __all__ matches actual exports."""
    import deephaven_mcp.mcp_systems_server._mcp as mcp_mod

    # Get __all__ list from _mcp.py
    expected_exports = set(mcp_mod.__all__)

    # Everything in __all__ should be importable
    for name in expected_exports:
        assert hasattr(mcp_mod, name), f"{name} in __all__ but not found in module"

    # Everything public should be in __all__
    public_attrs = set(name for name in dir(mcp_mod) if not name.startswith("_"))
    dunder_attrs = {
        "__all__",
        "__doc__",
        "__file__",
        "__name__",
        "__package__",
        "__spec__",
        "__cached__",
        "__loader__",
    }
    unlisted_public = public_attrs - expected_exports - dunder_attrs

    assert (
        not unlisted_public
    ), f"Public attributes not in __all__: {sorted(unlisted_public)}"


def test_mcp_module_exports_count():
    """Validate _mcp.py exports the expected number of items."""
    import deephaven_mcp.mcp_systems_server._mcp as mcp_mod

    assert len(mcp_mod.__all__) == 1, f"Expected 1 export, found {len(mcp_mod.__all__)}"
