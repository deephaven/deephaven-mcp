"""Tests for ``deephaven_mcp._platform._os_support``."""

from __future__ import annotations

from unittest.mock import patch

from deephaven_mcp._exceptions import InternalError
from deephaven_mcp._platform import _os_support


def test_supported_os_names_value() -> None:
    """The supported set is exactly POSIX and Windows."""
    assert _os_support.SUPPORTED_OS_NAMES == frozenset({"posix", "nt"})


def test_supported_os_names_is_immutable() -> None:
    """The contract is a frozenset so call sites cannot mutate it."""
    assert isinstance(_os_support.SUPPORTED_OS_NAMES, frozenset)


def test_unsupported_os_error_returns_internal_error() -> None:
    """The factory returns (does not raise) an ``InternalError``."""
    err = _os_support.unsupported_os_error("widget")
    assert isinstance(err, InternalError)


def test_unsupported_os_error_message_contents() -> None:
    """The message names the component, the current os.name, and the set."""
    with patch.object(_os_support.os, "name", "java"):
        err = _os_support.unsupported_os_error("widget")
    msg = str(err)
    assert "widget" in msg
    assert "java" in msg
    assert "nt" in msg and "posix" in msg
