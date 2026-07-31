"""Tests for ``deephaven_mcp._platform.spawn``.

The OS dispatch is exercised by patching :data:`os.name` and
:class:`subprocess.Popen` so every branch (POSIX, Windows, and the
unsupported-OS catch-all) is covered hermetically and
platform-independently.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from deephaven_mcp._exceptions import InternalError
from deephaven_mcp._platform import spawn as spawn_mod
from deephaven_mcp._platform.spawn import spawn_detached


def test_spawn_detached_invokes_subprocess(tmp_path: Path) -> None:
    """The POSIX branch passes the argv, cwd, and ``start_new_session``."""
    argv = ["prog", "--daemon", "--flag", "value"]
    log_path = tmp_path / "daemon.log"
    fake_proc = MagicMock()
    with patch.object(spawn_mod.subprocess, "Popen", return_value=fake_proc) as popen:
        spawn_detached(argv, cwd=tmp_path, log_path=log_path)
    args, kwargs = popen.call_args
    assert args[0] == argv
    assert kwargs.get("cwd") == tmp_path
    assert kwargs.get("start_new_session") is True
    # Security properties: no terminal inheritance, no fd leakage.
    assert kwargs.get("stdin") is spawn_mod.subprocess.DEVNULL
    assert kwargs.get("close_fds") is True


def test_spawn_detached_uses_windows_creationflags(tmp_path: Path) -> None:
    """The Windows branch sets ``creationflags=CREATE_NEW_PROCESS_GROUP``."""
    fake_proc = MagicMock()
    captured: dict[str, Any] = {}

    def fake_popen(_argv: object, **kwargs: object) -> MagicMock:
        captured.update(kwargs)
        return fake_proc

    with (
        patch.object(spawn_mod.os, "name", "nt"),
        patch.object(spawn_mod.subprocess, "Popen", side_effect=fake_popen),
    ):
        spawn_detached(["prog"], cwd=tmp_path, log_path=tmp_path / "daemon.log")
    assert captured.get("creationflags") == 0x00000200  # CREATE_NEW_PROCESS_GROUP
    assert "start_new_session" not in captured


def test_spawn_detached_closes_parent_log_handle(tmp_path: Path) -> None:
    """The parent must close its log handle after ``Popen`` returns.

    Regression test for the fd leak: ``Popen`` duplicates the
    descriptor for the child, so the parent's handle has to be
    closed once spawn completes.
    """
    captured: list[Any] = []

    def fake_popen(_argv: object, **kwargs: object) -> MagicMock:
        captured.append(kwargs["stdout"])
        return MagicMock()

    with patch.object(spawn_mod.subprocess, "Popen", side_effect=fake_popen):
        spawn_detached(["prog"], cwd=tmp_path, log_path=tmp_path / "daemon.log")
    assert captured, "Popen was not invoked"
    log_fh = captured[0]
    assert log_fh.closed, "spawn_detached must close the parent's log handle"


def test_spawn_detached_rejects_unsupported_os(tmp_path: Path) -> None:
    """An unsupported ``os.name`` fails fast rather than assuming Windows.

    The dispatch is decided before the log file is opened, so the
    catch-all must raise without creating the log or invoking ``Popen``.
    """
    log_path = tmp_path / "daemon.log"
    with (
        patch.object(spawn_mod.os, "name", "java"),
        patch.object(spawn_mod.subprocess, "Popen") as popen,
        pytest.raises(InternalError, match="java"),
    ):
        spawn_detached(["prog"], cwd=tmp_path, log_path=log_path)
    popen.assert_not_called()
    assert not log_path.exists(), "no empty log file should be created on failure"
