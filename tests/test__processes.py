"""Tests for ``deephaven_mcp._processes``.

Coverage strategy: ``ProcessIdentity.of_current_process`` runs for
real (the test process is always alive). Every branch of
:meth:`ProcessIdentity.is_alive` and
:meth:`ProcessIdentity.send_signal_safely` is exercised by patching
:class:`psutil.Process` and :func:`os.kill` so the tests are fully
hermetic and platform-independent.
"""

from __future__ import annotations

import os
import signal
from unittest.mock import MagicMock, patch

import psutil
import pytest

from deephaven_mcp import _processes as proc_mod
from deephaven_mcp._processes import ProcessIdentity, SignalOutcome

# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_of_current_process_captures_real_identity() -> None:
    """The current process is always alive at capture time."""
    identity = ProcessIdentity.of_current_process()
    assert identity.pid == os.getpid()
    assert identity.create_time_ns > 0
    assert identity.is_alive()


def test_of_pid_returns_none_for_no_such_process() -> None:
    """``of_pid`` returns ``None`` when the PID does not exist."""
    with patch.object(
        proc_mod.psutil, "Process", side_effect=psutil.NoSuchProcess(pid=42)
    ):
        assert ProcessIdentity.of_pid(42) is None


def test_of_pid_returns_none_for_access_denied() -> None:
    """``of_pid`` returns ``None`` when the OS refuses metadata access."""
    with patch.object(
        proc_mod.psutil, "Process", side_effect=psutil.AccessDenied(pid=42)
    ):
        assert ProcessIdentity.of_pid(42) is None


def test_of_pid_returns_none_for_value_error() -> None:
    """``of_pid`` returns ``None`` when ``psutil`` rejects the PID range."""
    with patch.object(proc_mod.psutil, "Process", side_effect=ValueError("bad pid")):
        assert ProcessIdentity.of_pid(-1) is None


def test_of_pid_returns_none_when_create_time_vanishes() -> None:
    """The process exits between handle construction and create-time read."""
    fake_proc = MagicMock()
    fake_proc.create_time.side_effect = psutil.NoSuchProcess(pid=42)
    with patch.object(proc_mod.psutil, "Process", return_value=fake_proc):
        assert ProcessIdentity.of_pid(42) is None


def test_of_pid_returns_none_when_create_time_denied() -> None:
    """The process exists but its create_time read is denied."""
    fake_proc = MagicMock()
    fake_proc.create_time.side_effect = psutil.AccessDenied(pid=42)
    with patch.object(proc_mod.psutil, "Process", return_value=fake_proc):
        assert ProcessIdentity.of_pid(42) is None


def test_of_pid_captures_identity_for_live_process() -> None:
    """The happy path: a live PID yields a populated identity."""
    fake_proc = MagicMock(pid=42)
    # ``create_time`` is documented to return seconds (float). The
    # ns conversion is the production code's responsibility; we
    # provide the float and assert the integer ns derivation.
    fake_proc.create_time.return_value = 1234.567_890_123
    with patch.object(proc_mod.psutil, "Process", return_value=fake_proc):
        identity = ProcessIdentity.of_pid(42)
    assert identity is not None
    assert identity.pid == 42
    assert identity.create_time_ns == int(1234.567_890_123 * 1_000_000_000)


# ---------------------------------------------------------------------------
# is_alive
# ---------------------------------------------------------------------------


def test_is_alive_true_when_create_time_matches() -> None:
    fake_proc = MagicMock()
    fake_proc.create_time.return_value = 1.0
    identity = ProcessIdentity(pid=42, create_time_ns=1_000_000_000)
    with patch.object(proc_mod.psutil, "Process", return_value=fake_proc):
        assert identity.is_alive() is True


def test_is_alive_false_when_create_time_mismatches() -> None:
    """PID was recycled: same pid, different create time."""
    fake_proc = MagicMock()
    fake_proc.create_time.return_value = 2.0
    identity = ProcessIdentity(pid=42, create_time_ns=1_000_000_000)
    with patch.object(proc_mod.psutil, "Process", return_value=fake_proc):
        assert identity.is_alive() is False


def test_is_alive_false_when_no_such_process() -> None:
    identity = ProcessIdentity(pid=42, create_time_ns=1)
    with patch.object(
        proc_mod.psutil, "Process", side_effect=psutil.NoSuchProcess(pid=42)
    ):
        assert identity.is_alive() is False


def test_is_alive_false_when_access_denied() -> None:
    identity = ProcessIdentity(pid=42, create_time_ns=1)
    with patch.object(
        proc_mod.psutil, "Process", side_effect=psutil.AccessDenied(pid=42)
    ):
        assert identity.is_alive() is False


def test_is_alive_false_when_value_error() -> None:
    identity = ProcessIdentity(pid=42, create_time_ns=1)
    with patch.object(proc_mod.psutil, "Process", side_effect=ValueError("bad pid")):
        assert identity.is_alive() is False


def test_is_alive_false_when_create_time_read_fails() -> None:
    """``Process()`` succeeds but ``create_time()`` raises mid-flight."""
    fake_proc = MagicMock()
    fake_proc.create_time.side_effect = psutil.NoSuchProcess(pid=42)
    identity = ProcessIdentity(pid=42, create_time_ns=1)
    with patch.object(proc_mod.psutil, "Process", return_value=fake_proc):
        assert identity.is_alive() is False


def test_is_alive_false_when_create_time_denied_mid_flight() -> None:
    """``Process()`` succeeds but ``create_time()`` is denied mid-flight."""
    fake_proc = MagicMock()
    fake_proc.create_time.side_effect = psutil.AccessDenied(pid=42)
    identity = ProcessIdentity(pid=42, create_time_ns=1)
    with patch.object(proc_mod.psutil, "Process", return_value=fake_proc):
        assert identity.is_alive() is False


# ---------------------------------------------------------------------------
# send_signal_safely
# ---------------------------------------------------------------------------


def test_send_signal_delivered_on_match() -> None:
    """The happy path: create_time matches, ``os.kill`` runs, DELIVERED."""
    fake_proc = MagicMock()
    fake_proc.create_time.return_value = 1.0
    identity = ProcessIdentity(pid=42, create_time_ns=1_000_000_000)
    with (
        patch.object(proc_mod.psutil, "Process", return_value=fake_proc),
        patch.object(proc_mod.os, "kill") as mock_kill,
    ):
        outcome = identity.send_signal_safely(signal.SIGTERM)
    assert outcome is SignalOutcome.DELIVERED
    mock_kill.assert_called_once_with(42, signal.SIGTERM)


def test_send_signal_recycled_on_mismatch() -> None:
    """create_time mismatch: kill is *not* attempted; outcome RECYCLED."""
    fake_proc = MagicMock()
    fake_proc.create_time.return_value = 2.0
    identity = ProcessIdentity(pid=42, create_time_ns=1_000_000_000)
    with (
        patch.object(proc_mod.psutil, "Process", return_value=fake_proc),
        patch.object(proc_mod.os, "kill") as mock_kill,
    ):
        outcome = identity.send_signal_safely(signal.SIGTERM)
    assert outcome is SignalOutcome.RECYCLED
    mock_kill.assert_not_called()


def test_send_signal_gone_when_no_such_process() -> None:
    identity = ProcessIdentity(pid=42, create_time_ns=1)
    with (
        patch.object(
            proc_mod.psutil, "Process", side_effect=psutil.NoSuchProcess(pid=42)
        ),
        patch.object(proc_mod.os, "kill") as mock_kill,
    ):
        outcome = identity.send_signal_safely(signal.SIGTERM)
    assert outcome is SignalOutcome.GONE
    mock_kill.assert_not_called()


def test_send_signal_gone_when_value_error() -> None:
    """psutil rejects an out-of-range PID before we issue any signal."""
    identity = ProcessIdentity(pid=-1, create_time_ns=1)
    with (
        patch.object(proc_mod.psutil, "Process", side_effect=ValueError("bad")),
        patch.object(proc_mod.os, "kill") as mock_kill,
    ):
        outcome = identity.send_signal_safely(signal.SIGTERM)
    assert outcome is SignalOutcome.GONE
    mock_kill.assert_not_called()


def test_send_signal_denied_when_access_denied_on_probe() -> None:
    identity = ProcessIdentity(pid=42, create_time_ns=1)
    with (
        patch.object(
            proc_mod.psutil, "Process", side_effect=psutil.AccessDenied(pid=42)
        ),
        patch.object(proc_mod.os, "kill") as mock_kill,
    ):
        outcome = identity.send_signal_safely(signal.SIGTERM)
    assert outcome is SignalOutcome.DENIED
    mock_kill.assert_not_called()


def test_send_signal_gone_when_kill_lookup_error() -> None:
    """The PID exits between the create-time read and the kill syscall."""
    fake_proc = MagicMock()
    fake_proc.create_time.return_value = 1.0
    identity = ProcessIdentity(pid=42, create_time_ns=1_000_000_000)
    with (
        patch.object(proc_mod.psutil, "Process", return_value=fake_proc),
        patch.object(proc_mod.os, "kill", side_effect=ProcessLookupError("gone")),
    ):
        outcome = identity.send_signal_safely(signal.SIGTERM)
    assert outcome is SignalOutcome.GONE


def test_send_signal_denied_when_kill_permission_error() -> None:
    fake_proc = MagicMock()
    fake_proc.create_time.return_value = 1.0
    identity = ProcessIdentity(pid=42, create_time_ns=1_000_000_000)
    with (
        patch.object(proc_mod.psutil, "Process", return_value=fake_proc),
        patch.object(proc_mod.os, "kill", side_effect=PermissionError("eperm")),
    ):
        outcome = identity.send_signal_safely(signal.SIGTERM)
    assert outcome is SignalOutcome.DENIED


# ---------------------------------------------------------------------------
# Equality / hashing (frozen dataclass)
# ---------------------------------------------------------------------------


def test_process_identity_is_frozen_and_hashable() -> None:
    """``frozen=True``: the dataclass forbids assignment and is hashable."""
    identity = ProcessIdentity(pid=1, create_time_ns=2)
    with pytest.raises(Exception):
        identity.pid = 99  # type: ignore[misc]
    # Hashable -> usable as a dict key / set member.
    assert {identity: True}[identity] is True


def test_process_identity_equality() -> None:
    a = ProcessIdentity(pid=1, create_time_ns=2)
    b = ProcessIdentity(pid=1, create_time_ns=2)
    c = ProcessIdentity(pid=1, create_time_ns=3)
    assert a == b
    assert a != c
