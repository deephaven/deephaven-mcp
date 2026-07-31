"""Tests for ``deephaven_mcp.cli._daemon._context``."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

from deephaven_mcp.cli._daemon._context import DaemonContext, _build_spawn_command
from deephaven_mcp.daemon_registry import DaemonDirectory


def _build_runtime(tmp_path: Path) -> MagicMock:
    runtime = MagicMock()
    runtime.config_dir = tmp_path / "cfg"
    runtime.runtime_dir = tmp_path / "rt"
    runtime.daemon_dir = DaemonDirectory(tmp_path / "rt" / "daemon")
    return runtime


def test_build_spawn_command_has_expected_shape(tmp_path: Path) -> None:
    """The spawn argv is sys.executable + ``-m`` + module + paired flags."""
    runtime = _build_runtime(tmp_path)
    cmd = _build_spawn_command(runtime)
    assert cmd[0] == sys.executable
    assert cmd[1] == "-m"
    assert cmd[2] == "deephaven_mcp.mcp_systems_server"
    assert "--daemon" in cmd
    # ``--config-dir`` and ``--runtime-dir`` are each followed by
    # the matching runtime path.
    assert cmd[cmd.index("--config-dir") + 1] == str(runtime.config_dir)
    assert cmd[cmd.index("--runtime-dir") + 1] == str(runtime.runtime_dir)


def test_from_runtime_wires_runtime_fields(tmp_path: Path) -> None:
    """``DaemonContext.from_runtime`` maps the runtime's daemon dir / argv / cwd."""
    runtime = _build_runtime(tmp_path)
    ctx = DaemonContext.from_runtime(runtime)
    assert isinstance(ctx, DaemonContext)
    assert ctx.directory is runtime.daemon_dir
    assert ctx.spawn_cwd == runtime.runtime_dir
    assert ctx.spawn_argv == _build_spawn_command(runtime)


def test_daemon_context_is_frozen(tmp_path: Path) -> None:
    """``DaemonContext`` is an immutable value object."""
    ctx = DaemonContext.from_runtime(_build_runtime(tmp_path))
    try:
        ctx.spawn_cwd = tmp_path  # type: ignore[misc]
    except AttributeError:
        return
    raise AssertionError("DaemonContext should be frozen")
