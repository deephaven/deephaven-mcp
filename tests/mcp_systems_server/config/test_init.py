"""Tests for ``deephaven_mcp.mcp_systems_server.config`` package surface.

Pins the public re-exports so a refactor of the private modules cannot
silently change what ``from deephaven_mcp.mcp_systems_server.config
import ...`` resolves to.
"""

from __future__ import annotations

import deephaven_mcp.mcp_systems_server.config as config_pkg
from deephaven_mcp import _taxonomy
from deephaven_mcp.mcp_systems_server.config import (
    _community,
    _enterprise,
    _multi,
    _server,
)

_EXPECTED_PUBLIC_NAMES = {
    # Manager + top-level model
    "MultiSystemConfigManager",
    "MultiSystemConfig",
    # Per-section umbrella models
    "ServerConfig",
    "CommunityConfig",
    "CommunitySettings",
    "EnterpriseConfig",
    # Nested settings schemas
    "CommunitySecurity",
    "CommunitySessionCreation",
    "CommunitySessionCreationDefaults",
    "CommunityTimeouts",
    "EnterpriseSettings",
    "EnterpriseTimeouts",
    # Taxonomy
    "SessionOrigin",
    "SystemRef",
    "SystemType",
}


def test_all_lists_documented_public_names() -> None:
    assert set(config_pkg.__all__) == _EXPECTED_PUBLIC_NAMES


def test_all_names_in_all_are_resolvable_attributes() -> None:
    for name in config_pkg.__all__:
        assert hasattr(config_pkg, name), name


def test_reexports_are_same_objects_as_internal_definitions() -> None:
    assert config_pkg.MultiSystemConfigManager is _multi.MultiSystemConfigManager
    assert config_pkg.MultiSystemConfig is _multi.MultiSystemConfig
    assert config_pkg.ServerConfig is _server.ServerConfig
    assert config_pkg.CommunityConfig is _community.CommunityConfig
    assert config_pkg.CommunitySettings is _community.CommunitySettings
    assert config_pkg.EnterpriseConfig is _enterprise.EnterpriseConfig
    assert config_pkg.EnterpriseSettings is _enterprise.EnterpriseSettings

    assert config_pkg.CommunitySessionCreation is _community.CommunitySessionCreation
    assert (
        config_pkg.CommunitySessionCreationDefaults
        is _community.CommunitySessionCreationDefaults
    )
    assert config_pkg.CommunitySecurity is _community.CommunitySecurity

    assert config_pkg.SystemType is _taxonomy.SystemType
    assert config_pkg.SystemRef is _taxonomy.SystemRef
    assert config_pkg.SessionOrigin is _taxonomy.SessionOrigin


def test_no_private_names_leak_into_package_surface() -> None:
    private = {n for n in config_pkg.__all__ if n.startswith("_")}
    assert private == set()
