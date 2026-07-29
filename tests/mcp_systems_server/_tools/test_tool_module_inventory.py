"""Inventory guardrail for the ``mcp_systems_server._tools`` package.

The package body is documentation only: it declares no ``__all__`` and
re-exports nothing, so importing it must not pull in a tool module. Its
real contract is the module inventory in its docstring, which these tests
pin against the modules actually present — a module added, removed, or
renamed without updating the docstring is a documentation bug that no
other test catches.

Naming: the subject is the whole tool-module set rather than one source
file, so this is named for its invariant rather than being the
``test_init.py`` that ``tests-improve`` step 1 normally requires (see
``ref-python-coding-practices`` rule 5).
"""

from __future__ import annotations

import importlib
import pkgutil

import pytest

import deephaven_mcp.mcp_systems_server._tools as tools_pkg

# Convention enforcement over the whole tool-module set, not a mirror of one
# source file (``ref-python-coding-practices`` rule 5).
pytestmark = pytest.mark.guardrail

#: Modules named in the package docstring's ``Modules:`` block that expose
#: ``register_tools``. ``shared`` is excluded: the docstring documents it as
#: "Internal utility functions (not MCP tools)".
_TOOL_MODULES = (
    "catalog",
    "pq",
    "script",
    "session",
    "session_community",
    "session_enterprise",
    "table",
)

_UTILITY_MODULES = ("shared",)


def test_package_declares_no_public_surface() -> None:
    """The package re-exports nothing, so it has no ``__all__``.

    Tools are reached through their own modules; a re-export here would
    make importing the package import every backend.
    """
    assert "__all__" not in vars(tools_pkg)


def test_package_docstring_matches_the_modules_present() -> None:
    """Every submodule on disk is documented, and every documented one exists."""
    on_disk = {
        info.name
        for info in pkgutil.iter_modules(tools_pkg.__path__)
        if not info.name.startswith("_")
    }
    assert on_disk == {*_TOOL_MODULES, *_UTILITY_MODULES}


@pytest.mark.parametrize("name", _TOOL_MODULES)
def test_every_tool_module_exposes_register_tools(name: str) -> None:
    """Each tool module offers the entry point the server calls at startup."""
    module = importlib.import_module(f"{tools_pkg.__name__}.{name}")
    assert callable(module.register_tools)


@pytest.mark.parametrize("name", _UTILITY_MODULES)
def test_utility_modules_do_not_register_tools(name: str) -> None:
    """A module documented as "not MCP tools" must not register any."""
    module = importlib.import_module(f"{tools_pkg.__name__}.{name}")
    assert "register_tools" not in vars(module)
