"""Tests for ``deephaven_mcp.cli.config`` package surface.

Pins the public re-exports so a refactor of the private modules cannot
silently change what ``from deephaven_mcp.cli.config import ...``
resolves to.
"""

from __future__ import annotations

import deephaven_mcp.cli.config as config_pkg
from deephaven_mcp.cli.config import _cli

_EXPECTED_PUBLIC_NAMES = {
    # Top-level model
    "CliConfig",
    # Domain sub-models
    "OutputConfig",
    "DaemonConfig",
    "RequestConfig",
    # Per-domain timeouts sub-models
    "DaemonTimeouts",
    "RequestTimeouts",
    # Loader
    "load_cli",
}


def test_all_lists_documented_public_names() -> None:
    """``__all__`` matches the documented set of public names."""
    assert set(config_pkg.__all__) == _EXPECTED_PUBLIC_NAMES


def test_all_names_in_all_are_resolvable_attributes() -> None:
    """Every name in ``__all__`` is actually resolvable on the package."""
    for name in config_pkg.__all__:
        assert hasattr(config_pkg, name), name


def test_reexports_are_same_objects_as_internal_definitions() -> None:
    """Package re-exports are the same objects defined in ``_cli``."""
    assert config_pkg.CliConfig is _cli.CliConfig
    assert config_pkg.OutputConfig is _cli.OutputConfig
    assert config_pkg.DaemonConfig is _cli.DaemonConfig
    assert config_pkg.DaemonTimeouts is _cli.DaemonTimeouts
    assert config_pkg.RequestConfig is _cli.RequestConfig
    assert config_pkg.RequestTimeouts is _cli.RequestTimeouts
    assert config_pkg.load_cli is _cli.load_cli


def test_no_private_names_leak_into_package_surface() -> None:
    """``__all__`` exposes only public names — no leading-underscore leakage."""
    private = {n for n in config_pkg.__all__ if n.startswith("_")}
    assert private == set()
