"""Tests for ``deephaven_mcp.cli`` package surface.

Pins the package's public re-exports — currently none. The CLI is
exposed exclusively via the ``dh-mcp`` console script in
``pyproject.toml`` (which references :func:`deephaven_mcp.cli._main.main`),
not via ``from deephaven_mcp.cli import ...``. This test exists so a
refactor that adds a public name to ``__all__`` is a deliberate change
visible in the test diff, not a silent surface expansion.
"""

from __future__ import annotations

import deephaven_mcp.cli as cli_pkg


def test_all_is_empty_by_design() -> None:
    """The CLI package exposes no public names via ``from deephaven_mcp.cli import``.

    The entry point is wired via the ``dh-mcp`` console script in
    ``pyproject.toml``, which directly references
    :func:`deephaven_mcp.cli._main.main`.
    """
    assert cli_pkg.__all__ == []


def test_all_names_in_all_are_resolvable_attributes() -> None:
    """Every name in ``__all__`` is actually resolvable on the package."""
    for name in cli_pkg.__all__:
        assert hasattr(cli_pkg, name), name
