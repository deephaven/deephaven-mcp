"""Tests for the slim ``deephaven_mcp.config`` package surface.

This package root owns only general-purpose primitives. The Pydantic
section schemas live in :mod:`deephaven_mcp.config.schema` (pinned by
``schema/test_init.py``) and the aggregator lives in
:mod:`deephaven_mcp.config.tree`; neither is re-exported from the
package root, so importing :mod:`deephaven_mcp.config` stays cheap.
"""

from __future__ import annotations

import deephaven_mcp.config as config_pkg
from deephaven_mcp import _exceptions as _exceptions_pkg
from deephaven_mcp._platform import dir_permissions as _platform_dir_permissions
from deephaven_mcp.config import (
    _config_dir,
    _data_root,
    _dir_permissions,
    _runtime_dir,
)

_EXPECTED_PUBLIC_NAMES = {
    "DATA_DIR_ENV_VAR",
    "ConfigurationError",
    "daemon_dir",
    "harden_private_dir",
    "resolve_config_dir",
    "resolve_data_root",
    "resolve_runtime_dir",
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
    assert config_pkg.DATA_DIR_ENV_VAR is _data_root.DATA_DIR_ENV_VAR
    assert config_pkg.resolve_data_root is _data_root.resolve_data_root
    assert config_pkg.resolve_config_dir is _config_dir.resolve_config_dir
    assert config_pkg.resolve_runtime_dir is _runtime_dir.resolve_runtime_dir
    assert config_pkg.daemon_dir is _runtime_dir.daemon_dir
    assert (
        config_pkg.verify_config_directory_permissions
        is _dir_permissions.verify_config_directory_permissions
    )
    # ``harden_private_dir`` is defined in the OS-abstraction package and
    # re-exported here; the config module no longer defines it.
    assert config_pkg.harden_private_dir is _platform_dir_permissions.harden_private_dir
    assert config_pkg.ConfigurationError is _exceptions_pkg.ConfigurationError


def test_no_private_names_leak_into_package_surface() -> None:
    """``__all__`` must not advertise any underscore-prefixed names."""
    private = {n for n in config_pkg.__all__ if n.startswith("_")}
    assert private == set()


def test_schemas_and_tree_are_not_reexported_from_package_root() -> None:
    """Schemas and the aggregator must stay out of the package root.

    Every name on :mod:`deephaven_mcp.config.schema`, plus the
    aggregator types, must be reachable only via the ``config.schema``
    / ``config.tree`` submodules — never re-exported from the
    ``deephaven_mcp.config`` root, so the primitive import path stays
    cheap. Sourcing the set from ``config.schema.__all__`` keeps this
    test current as schemas are added.
    """
    import deephaven_mcp.config.schema as schema_pkg

    must_not_leak = set(schema_pkg.__all__) | {"ConfigTree", "ConfigTreeLoader"}
    for name in must_not_leak:
        assert name not in config_pkg.__all__, name
        assert not hasattr(config_pkg, name), name
