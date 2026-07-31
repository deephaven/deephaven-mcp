"""Tests for ``deephaven_mcp._platform.fsutil``.

Coverage strategy: every branch of every function is exercised by
a unit test. The two POSIX primitives run for real on the CI host;
the two Windows primitives are exercised on POSIX by injecting a
fake ``msvcrt`` module into :data:`sys.modules` before the lazy
``import msvcrt`` inside each helper. Dispatch logic in
``_try_acquire`` / ``_release`` is verified by patching ``os.name``.
"""

from __future__ import annotations

import errno
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from deephaven_mcp._exceptions import FileLockTimeoutError, InternalError
from deephaven_mcp._platform.fsutil import (
    AdvisoryFileLock,
    _fsync_dir,
    _release,
    _release_nt,
    _release_posix,
    _try_acquire,
    _try_acquire_nt,
    _try_acquire_posix,
    atomic_write_private,
    replace_with_retry,
    unlink_with_retry,
)

# ---------------------------------------------------------------------------
# AdvisoryFileLock (end-to-end on the real host)
# ---------------------------------------------------------------------------


def test_lock_acquires_and_releases(tmp_path: Path) -> None:
    """Entering the context manager creates the lock file."""
    lock_path = tmp_path / "sub" / "app.lock"
    with AdvisoryFileLock(lock_path):
        assert lock_path.exists()


def test_lock_acquires_after_release(tmp_path: Path) -> None:
    """Releasing the lock allows a subsequent acquire to succeed."""
    lock_path = tmp_path / "app.lock"
    with AdvisoryFileLock(lock_path):
        pass
    with AdvisoryFileLock(lock_path):
        pass


def test_lock_exit_without_enter_is_noop(tmp_path: Path) -> None:
    """Calling ``__exit__`` without ``__enter__`` is a no-op.

    Defensive: a caller that constructs the lock without using it
    as a context manager must not crash on cleanup.
    """
    lock = AdvisoryFileLock(tmp_path / "app.lock")
    lock.__exit__(None, None, None)


def test_lock_creates_parent_directory(tmp_path: Path) -> None:
    """Missing parent directories are created on entry."""
    lock_path = tmp_path / "deeply" / "nested" / "app.lock"
    with AdvisoryFileLock(lock_path):
        assert lock_path.parent.is_dir()
        assert lock_path.exists()


def test_lock_rejects_unsupported_os_name(tmp_path: Path) -> None:
    """``__init__`` raises ``InternalError`` on an unsupported platform."""
    with patch("deephaven_mcp._platform.fsutil.os.name", "java"):
        with pytest.raises(InternalError, match="os.name"):
            AdvisoryFileLock(tmp_path / "app.lock")


def test_lock_times_out_when_already_held(tmp_path: Path) -> None:
    """A second acquire on a held lock raises ``FileLockTimeoutError``.

    ``fcntl.flock`` locks the open file description, so a second
    :class:`AdvisoryFileLock` on a fresh fd (even in the same
    process) cannot acquire while the first holds it. The bounded
    acquire converts the indefinite wait into a timeout.
    """
    lock_path = tmp_path / "app.lock"
    with AdvisoryFileLock(lock_path):
        contender = AdvisoryFileLock(
            lock_path, timeout_seconds=0.2, poll_interval_seconds=0.02
        )
        with pytest.raises(FileLockTimeoutError, match="Could not acquire"):
            contender.__enter__()
        # The contender closed its handle before raising; no fd leak.
        assert contender._fh is None


def test_lock_acquires_after_one_failed_poll(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A contended-then-free lock acquires on a later poll iteration.

    Exercises the poll loop's ``sleep`` + retry branch: the first
    attempt reports contended, the second succeeds.
    """
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.time.sleep", lambda _s: None)
    attempts = iter([False, True])
    monkeypatch.setattr(
        "deephaven_mcp._platform.fsutil._try_acquire",
        lambda _fd: next(attempts),
    )
    lock = AdvisoryFileLock(tmp_path / "app.lock")
    with lock:
        assert lock._fh is not None


# ---------------------------------------------------------------------------
# Platform helpers — POSIX (real, on the CI host)
# ---------------------------------------------------------------------------


def test_try_acquire_posix_calls_fcntl_flock_with_lock_ex_nb(tmp_path: Path) -> None:
    """``_try_acquire_posix`` calls ``fcntl.flock`` with ``LOCK_EX | LOCK_NB``."""
    import fcntl

    with patch("fcntl.flock") as flock:
        assert _try_acquire_posix(42) is True
    flock.assert_called_once_with(42, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_try_acquire_posix_returns_false_when_contended() -> None:
    """A ``BlockingIOError`` (lock held elsewhere) maps to ``False``."""
    with patch("fcntl.flock", side_effect=BlockingIOError):
        assert _try_acquire_posix(42) is False


def test_release_posix_calls_fcntl_flock_with_lock_un(tmp_path: Path) -> None:
    """``_release_posix`` calls ``fcntl.flock`` with ``LOCK_UN``."""
    import fcntl

    with patch("fcntl.flock") as flock:
        _release_posix(42)
    flock.assert_called_once_with(42, fcntl.LOCK_UN)


# ---------------------------------------------------------------------------
# Platform helpers — Windows (faked msvcrt on POSIX CI)
# ---------------------------------------------------------------------------


def _install_fake_msvcrt(
    monkeypatch: pytest.MonkeyPatch, *, locking_side_effect: object = None
) -> SimpleNamespace:
    """Inject a fake ``msvcrt`` module so the Windows helpers can run on POSIX."""
    fake = SimpleNamespace(
        locking=MagicMock(side_effect=locking_side_effect),
        LK_NBLCK=3,
        LK_UNLCK=2,
    )
    monkeypatch.setitem(sys.modules, "msvcrt", fake)
    return fake


def test_try_acquire_nt_calls_msvcrt_locking_with_lk_nblck(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_try_acquire_nt`` calls ``msvcrt.locking`` with ``LK_NBLCK`` on byte 1."""
    fake = _install_fake_msvcrt(monkeypatch)
    assert _try_acquire_nt(42) is True
    fake.locking.assert_called_once_with(42, fake.LK_NBLCK, 1)


def test_try_acquire_nt_returns_false_when_contended(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A contended-errno ``OSError`` (lock held elsewhere) maps to ``False``."""
    _install_fake_msvcrt(
        monkeypatch,
        locking_side_effect=OSError(errno.EACCES, "locked"),
    )
    assert _try_acquire_nt(42) is False


def test_try_acquire_nt_reraises_unexpected_oserror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-contended ``OSError`` (e.g. bad descriptor) propagates.

    Masking it as contention would make the bounded acquire poll
    until its deadline and then raise a misleading "holder" timeout.
    """
    _install_fake_msvcrt(
        monkeypatch,
        locking_side_effect=OSError(errno.EBADF, "bad file descriptor"),
    )
    with pytest.raises(OSError, match="bad file descriptor"):
        _try_acquire_nt(42)


def test_release_nt_calls_msvcrt_locking_with_lk_unlck(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_release_nt`` calls ``msvcrt.locking`` with ``LK_UNLCK`` on byte 1."""
    fake = _install_fake_msvcrt(monkeypatch)
    _release_nt(42)
    fake.locking.assert_called_once_with(42, fake.LK_UNLCK, 1)


def test_release_nt_swallows_oserror(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_release_nt`` swallows ``OSError`` from ``msvcrt.locking``.

    Lets the close path stay clean if the region was not locked.
    """
    _install_fake_msvcrt(monkeypatch, locking_side_effect=OSError("not locked"))
    # Must not raise.
    _release_nt(42)


# ---------------------------------------------------------------------------
# Dispatch — _acquire / _release branch on os.name
# ---------------------------------------------------------------------------


def test_try_acquire_dispatches_to_posix_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On POSIX, ``_try_acquire`` delegates to ``_try_acquire_posix``."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "posix")
    posix = MagicMock(return_value=True)
    monkeypatch.setattr("deephaven_mcp._platform.fsutil._try_acquire_posix", posix)
    assert _try_acquire(42) is True
    posix.assert_called_once_with(42)


def test_try_acquire_dispatches_to_nt_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    """On Windows, ``_try_acquire`` delegates to ``_try_acquire_nt``."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "nt")
    nt = MagicMock(return_value=True)
    monkeypatch.setattr("deephaven_mcp._platform.fsutil._try_acquire_nt", nt)
    assert _try_acquire(42) is True
    nt.assert_called_once_with(42)


def test_try_acquire_raises_internal_error_on_unsupported_os(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_try_acquire`` raises ``InternalError`` on an unsupported ``os.name``.

    Guards the case where ``__init__``'s up-front check is bypassed
    or the helper is reached through some other path.
    """
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "java")
    with pytest.raises(InternalError, match="os.name"):
        _try_acquire(42)


def test_release_dispatches_to_posix_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    """On POSIX, ``_release`` delegates to ``_release_posix``."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "posix")
    posix = MagicMock()
    monkeypatch.setattr("deephaven_mcp._platform.fsutil._release_posix", posix)
    _release(42)
    posix.assert_called_once_with(42)


def test_release_dispatches_to_nt_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    """On Windows, ``_release`` delegates to ``_release_nt``."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "nt")
    nt = MagicMock()
    monkeypatch.setattr("deephaven_mcp._platform.fsutil._release_nt", nt)
    _release(42)
    nt.assert_called_once_with(42)


def test_release_raises_internal_error_on_unsupported_os(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_release`` raises ``InternalError`` on an unsupported ``os.name``."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "java")
    with pytest.raises(InternalError, match="os.name"):
        _release(42)


# ---------------------------------------------------------------------------
# replace_with_retry / unlink_with_retry
# ---------------------------------------------------------------------------


def test_replace_with_retry_posix_calls_os_replace_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """On POSIX the helper is a single ``os.replace`` call."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "posix")
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.write_text("payload")
    fake_replace = MagicMock()
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.replace", fake_replace)
    replace_with_retry(src, dst)
    fake_replace.assert_called_once_with(src, dst)


def test_replace_with_retry_windows_succeeds_after_transient(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Windows: a transient ``PermissionError`` is retried then succeeds."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "nt")
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.time.sleep", lambda _s: None)
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    calls: list[int] = []

    def flaky(s: object, d: object) -> None:
        calls.append(1)
        if len(calls) < 3:
            raise PermissionError("sharing violation")

    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.replace", flaky)
    replace_with_retry(src, dst)
    assert len(calls) == 3


def test_replace_with_retry_windows_raises_after_exhaustion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Windows: persistent ``PermissionError`` raises the last exception."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "nt")
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.time.sleep", lambda _s: None)

    def always_fail(s: object, d: object) -> None:
        raise PermissionError("locked")

    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.replace", always_fail)
    with pytest.raises(PermissionError, match="locked"):
        replace_with_retry(tmp_path / "a", tmp_path / "b")


def test_replace_with_retry_propagates_non_permission_oserror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Non-``PermissionError`` ``OSError`` is not retried; propagates immediately."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "nt")

    def boom(s: object, d: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.replace", boom)
    with pytest.raises(OSError, match="disk full"):
        replace_with_retry(tmp_path / "a", tmp_path / "b")


def test_unlink_with_retry_posix_uses_missing_ok(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """POSIX path: missing-file is a no-op (``missing_ok=True``)."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "posix")
    target = tmp_path / "absent"
    # Must not raise.
    unlink_with_retry(target)


def test_unlink_with_retry_windows_succeeds_after_transient(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Windows: a transient ``PermissionError`` is retried then succeeds."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "nt")
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.time.sleep", lambda _s: None)
    target = tmp_path / "f"
    calls: list[int] = []
    real_unlink = Path.unlink

    def flaky(self: Path, *args: object, **kwargs: object) -> None:
        calls.append(1)
        if len(calls) < 2:
            raise PermissionError("locked")
        return real_unlink(self, *args, **kwargs)

    target.write_text("x")
    monkeypatch.setattr(Path, "unlink", flaky)
    unlink_with_retry(target)
    assert len(calls) == 2
    assert not target.exists()


def test_unlink_with_retry_windows_raises_after_exhaustion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Windows: persistent ``PermissionError`` raises the last exception."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "nt")
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.time.sleep", lambda _s: None)

    def always_fail(self: Path, *args: object, **kwargs: object) -> None:
        raise PermissionError("locked")

    monkeypatch.setattr(Path, "unlink", always_fail)
    with pytest.raises(PermissionError, match="locked"):
        unlink_with_retry(tmp_path / "f")


# ---------------------------------------------------------------------------
# atomic_write_private / _fsync_dir
# ---------------------------------------------------------------------------


def test_atomic_write_private_writes_contents(tmp_path: Path) -> None:
    """The bytes land at the destination path exactly as supplied."""
    dest = tmp_path / "data.json"
    atomic_write_private(dest, b"payload-bytes")
    assert dest.read_bytes() == b"payload-bytes"


def test_atomic_write_private_replaces_existing(tmp_path: Path) -> None:
    """An existing file is replaced atomically with the new contents."""
    dest = tmp_path / "data.json"
    dest.write_bytes(b"old")
    atomic_write_private(dest, b"new")
    assert dest.read_bytes() == b"new"


def test_atomic_write_private_sets_owner_only_mode_on_posix(tmp_path: Path) -> None:
    """On POSIX the published file is mode ``0o600`` by default."""
    if os.name != "posix":  # pragma: no cover - POSIX-only assertion
        pytest.skip("POSIX file-mode semantics only")
    dest = tmp_path / "secret"
    atomic_write_private(dest, b"x")
    assert (dest.stat().st_mode & 0o777) == 0o600


def test_atomic_write_private_honors_explicit_mode_on_posix(tmp_path: Path) -> None:
    """A caller-supplied ``mode`` overrides the ``0o600`` default on POSIX."""
    if os.name != "posix":  # pragma: no cover - POSIX-only assertion
        pytest.skip("POSIX file-mode semantics only")
    dest = tmp_path / "shared"
    atomic_write_private(dest, b"x", mode=0o640)
    assert (dest.stat().st_mode & 0o777) == 0o640


def test_atomic_write_private_leaves_no_temp_files(tmp_path: Path) -> None:
    """A successful write leaves only the destination, no ``.tmp`` siblings."""
    dest = tmp_path / "data.json"
    atomic_write_private(dest, b"x")
    assert list(tmp_path.iterdir()) == [dest]


def test_atomic_write_private_cleans_up_temp_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure during rename removes the temp file and re-raises.

    The pre-existing destination (if any) must be left untouched.
    """
    dest = tmp_path / "data.json"
    dest.write_bytes(b"original")

    def boom(_src: object, _dst: object) -> None:
        raise OSError("rename failed")

    monkeypatch.setattr("deephaven_mcp._platform.fsutil.replace_with_retry", boom)
    with pytest.raises(OSError, match="rename failed"):
        atomic_write_private(dest, b"new")
    # No temp leftovers, and the original contents survive.
    assert list(tmp_path.iterdir()) == [dest]
    assert dest.read_bytes() == b"original"


def test_atomic_write_private_fsyncs_parent_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The parent directory is fsynced after the rename (POSIX durability)."""
    if os.name != "posix":  # pragma: no cover - POSIX-only assertion
        pytest.skip("directory fsync is POSIX-only")
    synced: list[Path] = []
    real_fsync_dir = _fsync_dir

    def spy(directory: Path) -> None:
        synced.append(directory)
        real_fsync_dir(directory)

    monkeypatch.setattr("deephaven_mcp._platform.fsutil._fsync_dir", spy)
    dest = tmp_path / "data.json"
    atomic_write_private(dest, b"x")
    assert synced == [tmp_path]


def test_fsync_dir_noop_on_non_posix(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_fsync_dir`` is a no-op on non-POSIX platforms."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "nt")
    opened: list[object] = []
    monkeypatch.setattr(
        "deephaven_mcp._platform.fsutil.os.open",
        lambda *a, **k: opened.append(a) or 0,  # pragma: no cover - must not run
    )
    _fsync_dir(Path("/whatever"))
    assert opened == []


def test_fsync_dir_swallows_open_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A directory that cannot be opened is tolerated (best-effort)."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "posix")

    def boom(*_a: object, **_k: object) -> int:
        raise OSError("cannot open dir")

    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.open", boom)
    # Must not raise.
    _fsync_dir(tmp_path)


def test_fsync_dir_swallows_fsync_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A filesystem that rejects directory ``fsync`` is tolerated, fd closed."""
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.name", "posix")
    closed: list[int] = []
    real_close = os.close

    def fake_fsync(_fd: int) -> None:
        raise OSError("fsync unsupported")

    def tracking_close(fd: int) -> None:
        closed.append(fd)
        real_close(fd)

    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.fsync", fake_fsync)
    monkeypatch.setattr("deephaven_mcp._platform.fsutil.os.close", tracking_close)
    # Must not raise, and the directory fd must still be closed.
    _fsync_dir(tmp_path)
    assert len(closed) == 1
