"""Tests for the ``deephaven_mcp.sessions`` package surface.

Pins the public re-exports so a refactor of the private modules cannot
silently change what ``from deephaven_mcp.sessions import ...``
resolves to.
"""

from __future__ import annotations

import deephaven_mcp.sessions as sessions_pkg
from deephaven_mcp.sessions import _community, _enterprise

_EXPECTED_PUBLIC_NAMES = {
    "VALID_LAUNCH_METHODS",
    "VALID_PROGRAMMING_LANGUAGES",
    "CommunitySessionConfig",
    "EnterpriseSessionCreation",
    "EnterpriseSessionCreationDefaults",
    "EnterpriseSystemConfig",
    "LaunchMethod",
    "ProgrammingLanguage",
}


def test_all_lists_documented_public_names() -> None:
    """``__all__`` matches the documented set of public names."""
    assert set(sessions_pkg.__all__) == _EXPECTED_PUBLIC_NAMES


def test_all_names_in_all_are_resolvable_attributes() -> None:
    """Every name in ``__all__`` is actually resolvable on the package."""
    for name in sessions_pkg.__all__:
        assert hasattr(sessions_pkg, name), name


def test_reexports_are_same_objects_as_internal_definitions() -> None:
    """Package re-exports are the same objects defined in the private modules."""
    assert sessions_pkg.CommunitySessionConfig is _community.CommunitySessionConfig
    assert (
        sessions_pkg.EnterpriseSessionCreation is _enterprise.EnterpriseSessionCreation
    )
    assert (
        sessions_pkg.EnterpriseSessionCreationDefaults
        is _enterprise.EnterpriseSessionCreationDefaults
    )
    assert sessions_pkg.EnterpriseSystemConfig is _enterprise.EnterpriseSystemConfig


def test_no_private_names_leak_into_package_surface() -> None:
    """``__all__`` exposes only public names — no leading-underscore leakage."""
    private = {n for n in sessions_pkg.__all__ if n.startswith("_")}
    assert private == set()
