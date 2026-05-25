"""Tests for ``deephaven_mcp.auth.middleware`` package surface.

These tests pin the public re-exports of the package so that a refactor of
the underlying ``_psk`` module cannot silently change what
``from deephaven_mcp.auth.middleware import ...`` resolves to.
"""

from __future__ import annotations

import deephaven_mcp.auth.middleware as middleware_pkg
from deephaven_mcp.auth.middleware import PSK_HEADER_NAME, PSKMiddleware
from deephaven_mcp.auth.middleware._psk import (
    PSK_HEADER_NAME as PSK_HEADER_NAME_INTERNAL,
)
from deephaven_mcp.auth.middleware._psk import PSKMiddleware as PSKMiddleware_INTERNAL


def test_all_lists_documented_public_names() -> None:
    """``__all__`` must list exactly the package's public surface."""
    assert set(middleware_pkg.__all__) == {"PSK_HEADER_NAME", "PSKMiddleware"}


def test_psk_header_name_is_reexport_of_internal_constant() -> None:
    """The package re-export must be the same object as the internal symbol."""
    assert PSK_HEADER_NAME is PSK_HEADER_NAME_INTERNAL


def test_psk_middleware_is_reexport_of_internal_class() -> None:
    """The package re-export must be the same class as the internal symbol."""
    assert PSKMiddleware is PSKMiddleware_INTERNAL


def test_psk_header_name_value_is_x_deephaven_psk() -> None:
    """The PSK header name is part of the wire contract; pin its value."""
    assert PSK_HEADER_NAME == "X-Deephaven-PSK"


def test_all_names_in_all_are_resolvable_attributes() -> None:
    """Every name in ``__all__`` must actually exist on the module."""
    for name in middleware_pkg.__all__:
        assert hasattr(middleware_pkg, name), name
