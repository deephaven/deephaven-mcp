"""
Tests for the resource_manager __init__.py file.
"""


def test_imports_and_all():
    import deephaven_mcp.resource_manager as mod
    from deephaven_mcp.resource_manager import (
        BaseItemManager,
        CommunitySessionManager,
        CommunitySessionRegistry,
        CorePlusSessionFactoryManager,
        CorePlusSessionFactoryRegistry,
        EnterpriseSessionManager,
        ResourceLivenessStatus,
    )

    # __all__ should be defined and contain all the public symbols
    assert hasattr(mod, "__all__")

    expected_all = [
        "BaseItemManager",
        "CombinedSessionRegistry",
        "CommunitySessionManager",
        "DynamicCommunitySessionManager",
        "EnterpriseSessionManager",
        "CorePlusSessionFactoryManager",
        "CommunitySessionRegistry",
        "CorePlusSessionFactoryRegistry",
        "ResourceLivenessStatus",
        "SystemType",
        "LaunchedSession",
        "DockerLaunchedSession",
        "PipLaunchedSession",
        "launch_session",
        "find_available_port",
        "generate_auth_token",
    ]
    assert sorted(mod.__all__) == sorted(expected_all)

    # each symbol should be the correct object from the submodules
    assert mod.BaseItemManager is BaseItemManager
    assert mod.CommunitySessionManager is CommunitySessionManager
    assert mod.EnterpriseSessionManager is EnterpriseSessionManager
    assert mod.CorePlusSessionFactoryManager is CorePlusSessionFactoryManager
    assert mod.CommunitySessionRegistry is CommunitySessionRegistry
    assert mod.CorePlusSessionFactoryRegistry is CorePlusSessionFactoryRegistry

    # star import should bring in only expected symbols
    imported = {}
    exec("from deephaven_mcp.resource_manager import *", {}, imported)
    for symbol in expected_all:
        assert symbol in imported, f"{symbol} missing from star import"
    for symbol in imported:
        if not symbol.startswith("__"):
            assert symbol in expected_all, f"Unexpected symbol in star import: {symbol}"
