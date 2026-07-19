"""Tests for the ``deephaven_mcp.config.schema`` package surface.

Pins the public re-exports so a refactor of the private modules cannot
silently change what ``from deephaven_mcp.config.schema import ...``
resolves to.
"""

from __future__ import annotations

import deephaven_mcp.config.schema as schema_pkg
from deephaven_mcp.config.schema import (
    _cli,
    _community,
    _enterprise,
    _pq_config,
    _response_limits,
    _server,
)

_EXPECTED_PUBLIC_NAMES = {
    # cli.json
    "CliConfig",
    "DaemonControlConfig",
    "DaemonReuseAction",
    "DaemonReusePolicy",
    "DaemonTimeouts",
    "DocsConfig",
    "DocsTimeouts",
    "OutputConfig",
    "RequestConfig",
    "RequestTimeouts",
    "load_cli",
    # server.json
    "DaemonProcessConfig",
    "ServerConfig",
    "load_server",
    # community/
    "CommunityConfig",
    "CommunitySecurity",
    "CommunitySessionCreation",
    "CommunitySessionCreationDefaults",
    "CommunitySettings",
    "CommunityTimeouts",
    "DockerImages",
    "DockerLaunchOptions",
    "LaunchMethod",
    "ProgrammingLanguage",
    "PythonLaunchOptions",
    "load_community",
    # enterprise/
    "EnterpriseConfig",
    "EnterpriseSettings",
    "EnterpriseTimeouts",
    "load_enterprise",
    # tool-tunable schemas embedded by the section models
    "PqToolsConfig",
    "ResponseLimits",
}


def test_all_lists_documented_public_names() -> None:
    """``__all__`` must list exactly the package's documented public surface."""
    assert set(schema_pkg.__all__) == _EXPECTED_PUBLIC_NAMES


def test_all_names_in_all_are_resolvable_attributes() -> None:
    """Every name advertised by ``__all__`` must exist on the package."""
    for name in schema_pkg.__all__:
        assert hasattr(schema_pkg, name), name


def test_reexports_are_same_objects_as_internal_definitions() -> None:
    """Package symbols must be the same objects as their submodule sources."""
    assert schema_pkg.CliConfig is _cli.CliConfig
    assert schema_pkg.DaemonControlConfig is _cli.DaemonControlConfig
    assert schema_pkg.load_cli is _cli.load_cli
    assert schema_pkg.ServerConfig is _server.ServerConfig
    assert schema_pkg.DaemonProcessConfig is _server.DaemonProcessConfig
    assert schema_pkg.load_server is _server.load_server
    assert schema_pkg.CommunityConfig is _community.CommunityConfig
    assert schema_pkg.CommunitySettings is _community.CommunitySettings
    assert schema_pkg.LaunchMethod is _community.LaunchMethod
    assert schema_pkg.ProgrammingLanguage is _community.ProgrammingLanguage
    assert schema_pkg.load_community is _community.load_community
    assert schema_pkg.EnterpriseConfig is _enterprise.EnterpriseConfig
    assert schema_pkg.EnterpriseSettings is _enterprise.EnterpriseSettings
    assert schema_pkg.load_enterprise is _enterprise.load_enterprise
    assert schema_pkg.ResponseLimits is _response_limits.ResponseLimits
    assert schema_pkg.PqToolsConfig is _pq_config.PqToolsConfig


def test_no_private_names_leak_into_package_surface() -> None:
    """``__all__`` must not advertise any underscore-prefixed names."""
    private = {n for n in schema_pkg.__all__ if n.startswith("_")}
    assert private == set()
