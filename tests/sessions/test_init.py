"""
Tests for deephaven_mcp.sessions.__init__ (import/export surface).
"""

def test_imports_and_all():
    import deephaven_mcp.sessions as mod
    # __all__ should include key public symbols
    assert hasattr(mod, "SessionManager")
    assert hasattr(mod, "Session")
    assert hasattr(mod, "SessionType")
    assert hasattr(mod, "get_dh_versions")
    assert hasattr(mod, "get_meta_table")
    assert hasattr(mod, "get_pip_packages_table")
    assert hasattr(mod, "get_table")
    assert hasattr(mod, "SessionCreationError")
    assert "SessionManager" in mod.__all__
    assert "Session" in mod.__all__
    assert "SessionType" in mod.__all__
    assert "get_dh_versions" in mod.__all__
    assert "get_meta_table" in mod.__all__
    assert "get_pip_packages_table" in mod.__all__
    assert "get_table" in mod.__all__
    assert "SessionCreationError" in mod.__all__
