"""Tests for the ``deephaven_mcp.cli._commands`` package surface.

The package is a namespace for click subcommand modules; it declares
no public re-exports. These tests pin that contract so a future
refactor that accidentally adds an export or fails to import a
command module is caught.
"""

from __future__ import annotations

import importlib

import click

import deephaven_mcp.cli._commands as commands_pkg

_COMMAND_MODULES = (
    "agents",
    "catalog",
    "config",
    "daemon",
    "docs",
    "pq",
    "session",
    "system",
    "table",
    "tool",
)


def test_package_declares_no_public_surface() -> None:
    """The package is a namespace only — it defines no ``__all__``."""
    assert not hasattr(commands_pkg, "__all__")


def test_command_modules_import_and_expose_a_click_command() -> None:
    """Each subcommand module imports and exposes its click entry point."""
    for name in _COMMAND_MODULES:
        module = importlib.import_module(f"deephaven_mcp.cli._commands.{name}")
        command = getattr(module, name)
        assert isinstance(command, click.Command), name
