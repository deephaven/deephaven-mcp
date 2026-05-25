"""Tests for the slim ``deephaven_mcp.config`` package surface.

This package now owns only general-purpose primitives. All
systems-server-specific schemas/orchestration live in
:mod:`deephaven_mcp.mcp_systems_server.config` and are pinned by a
separate ``test_init.py`` there.
"""

from __future__ import annotations

import deephaven_mcp.config as config_pkg
from deephaven_mcp import _exceptions as _exceptions_pkg
from deephaven_mcp.config import _config_dir, _dir_permissions

_EXPECTED_PUBLIC_NAMES = {
    "CONFIG_DIR_ENV_VAR",
    "ConfigurationError",
    "default_config_dir",
    "resolve_config_dir",
    "verify_config_directory_permissions",
}


def test_all_lists_documented_public_names() -> None:
    """``__all__`` must list exactly the package's documented public surface."""
    assert set(config_pkg.__all__) == _EXPECTED_PUBLIC_NAMES


def test_all_names_in_all_are_resolvable_attributes() -> None:
    """Every name advertised by ``__all__`` must exist on the module."""
    for name in config_pkg.__all__:
        assert hasattr(config_pkg, name), name


def test_reexports_are_same_objects_as_internal_definitions() -> None:
    """Package symbols must be the same objects as their submodule sources."""
    assert config_pkg.CONFIG_DIR_ENV_VAR is _config_dir.CONFIG_DIR_ENV_VAR
    assert config_pkg.default_config_dir is _config_dir.default_config_dir
    assert config_pkg.resolve_config_dir is _config_dir.resolve_config_dir
    assert (
        config_pkg.verify_config_directory_permissions
        is _dir_permissions.verify_config_directory_permissions
    )
    assert config_pkg.ConfigurationError is _exceptions_pkg.ConfigurationError


def test_no_private_names_leak_into_package_surface() -> None:
    """``__all__`` must not advertise any underscore-prefixed names."""
    private = {n for n in config_pkg.__all__ if n.startswith("_")}
    assert private == set()


def test_systems_server_schemas_are_not_reexported_here() -> None:
    """Schema models must live in ``mcp_systems_server.config`` only.

    The set below mirrors ``mcp_systems_server.config.__all__`` so that
    if a new schema is added there, this test enforces the same "lives
    only in the systems-server config package" rule for it.
    """
    moved = {
        "CommunityConfig",
        "CommunitySecurity",
        "CommunitySessionCreation",
        "CommunitySessionCreationDefaults",
        "CommunitySettings",
        "CommunityTimeouts",
        "EnterpriseConfig",
        "EnterpriseSettings",
        "EnterpriseTimeouts",
        "MultiSystemConfig",
        "MultiSystemConfigManager",
        "ServerConfig",
        "SessionOrigin",
        "SystemRef",
        "SystemType",
    }
    for name in moved:
        assert name not in config_pkg.__all__, name
        assert not hasattr(config_pkg, name), name
