"""
Tests for deephaven_mcp.session_manager.__init__ (import/export surface).
"""


def test_imports_and_all():
    import deephaven_mcp.session_manager as mod
    from deephaven_mcp.session_manager import (
        BaseSessionManager,
        CommunitySessionManager,
        EnterpriseSessionManager,
        SessionRegistry,
        SessionManagerType,
    )

    # __all__ should include key public symbols exactly
    expected_all = [
        "BaseSessionManager",
        "CommunitySessionManager",
        "EnterpriseSessionManager",
        "SessionRegistry",
        "SessionManagerType",
    ]
    assert mod.__all__ == expected_all

    # each symbol should be the correct object from the submodules
    assert mod.BaseSessionManager is BaseSessionManager
    assert mod.CommunitySessionManager is CommunitySessionManager
    assert mod.EnterpriseSessionManager is EnterpriseSessionManager
    assert mod.SessionRegistry is SessionRegistry
    assert mod.SessionManagerType is SessionManagerType

    # star import should bring in only expected symbols
    imported = {}
    exec("from deephaven_mcp.session_manager import *", {}, imported)
    for symbol in expected_all:
        assert symbol in imported, f"{symbol} missing from star import"
    for symbol in imported:
        if not symbol.startswith("__"):
            assert symbol in expected_all, f"Unexpected symbol in star import: {symbol}"
