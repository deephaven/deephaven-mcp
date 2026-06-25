"""Tests pinning the ``deephaven_mcp.cli._daemon`` package public surface.

The package re-exports a stable set of names so the command layer's
``from deephaven_mcp.cli._daemon import ...`` import sites stay valid even as
the subsystem's internal module layout evolves.
"""

from __future__ import annotations

from deephaven_mcp._exceptions import (
    DaemonClientError,
    DaemonReuseRefusedError,
    DaemonStartupTimeoutError,
)
from deephaven_mcp.cli import _daemon
from deephaven_mcp.cli._daemon._context import DaemonContext
from deephaven_mcp.cli._daemon._lifecycle import (
    get_or_start_daemon,
    stop_daemon,
)

_EXPECTED_PUBLIC_NAMES = {
    "DaemonClientError",
    "DaemonContext",
    "DaemonReuseRefusedError",
    "DaemonStartupTimeoutError",
    "get_or_start_daemon",
    "stop_daemon",
}


def test_all_matches_expected_names() -> None:
    """``__all__`` is exactly the documented public surface."""
    assert set(_daemon.__all__) == _EXPECTED_PUBLIC_NAMES


def test_all_names_are_importable_from_package() -> None:
    """Every name in ``__all__`` resolves as a package attribute."""
    for name in _daemon.__all__:
        assert hasattr(_daemon, name), name


def test_reexports_are_the_canonical_objects() -> None:
    """The re-exported names are the same objects as their source definitions."""
    assert _daemon.DaemonClientError is DaemonClientError
    assert _daemon.DaemonReuseRefusedError is DaemonReuseRefusedError
    assert _daemon.DaemonStartupTimeoutError is DaemonStartupTimeoutError
    assert _daemon.DaemonContext is DaemonContext
    assert _daemon.get_or_start_daemon is get_or_start_daemon
    assert _daemon.stop_daemon is stop_daemon
