"""Cross-platform filesystem utilities: advisory locking and atomic writes.

OS-specific filesystem mechanics that several call sites need but
that do not belong in any one domain module. POSIX and Windows
primitives are dispatched on ``os.name`` and imported lazily so the
module loads cleanly on either platform.

Exports:

- :class:`AdvisoryFileLock` — a cross-platform advisory file lock
  usable as a context manager. Constructing the lock captures a
  path; entering its context manager creates the file (if missing),
  acquires an exclusive lock, and blocks any other process
  attempting to acquire the same lock until the context exits.
- :func:`atomic_write_private` — write bytes to a path atomically
  (write-temp-then-rename) with owner-only permissions on POSIX,
  fsyncing both the file and its parent directory so the rename
  survives a crash.
- :func:`replace_with_retry` / :func:`unlink_with_retry` — atomic
  rename and unlink wrappers that, on Windows, retry briefly on
  :class:`PermissionError`. A concurrent reader's open file handle
  on Windows can transiently block ``MoveFileEx`` /
  ``DeleteFile``; the retry absorbs that transient without
  requiring readers to acquire the lock.

Platform support:

- POSIX: :func:`fcntl.flock` with ``LOCK_EX | LOCK_NB`` (advisory,
  process-scoped, whole-file), polled to a deadline.
- Windows: :func:`msvcrt.locking` with ``LK_NBLCK`` on the first
  byte of the file (mandatory at the byte-range level; used here
  as an advisory cross-process gate), polled to a deadline.
- Any other ``os.name``: :class:`AdvisoryFileLock` raises
  :class:`InternalError` at construction time, and the internal
  :func:`_try_acquire` / :func:`_release` dispatch helpers also
  raise if reached.

Acquisition is bounded: :meth:`AdvisoryFileLock.__enter__` polls a
non-blocking attempt until it succeeds or ``timeout_seconds``
elapses, raising :class:`~deephaven_mcp._exceptions.FileLockTimeoutError`
rather than blocking forever on a wedged or crashed holder.
"""

from __future__ import annotations

__all__ = [
    "AdvisoryFileLock",
    "atomic_write_private",
    "replace_with_retry",
    "unlink_with_retry",
]

import errno
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import IO

from deephaven_mcp._exceptions import FileLockTimeoutError
from deephaven_mcp._platform._os_support import (
    SUPPORTED_OS_NAMES,
    unsupported_os_error,
)

_LOGGER = logging.getLogger(__name__)

_NT_LOCK_CONTENDED_ERRNOS = frozenset(
    code
    for code in (
        getattr(errno, "EACCES", None),
        getattr(errno, "EDEADLOCK", None),
        getattr(errno, "EDEADLK", None),
    )
    if code is not None
)
"""``errno`` values ``msvcrt.locking(LK_NBLCK)`` raises when the region
is already locked by another holder. Any other ``OSError`` from the
call is a genuine fault (e.g. a bad descriptor) and is re-raised rather
than masked as contention. Built with :func:`getattr` fallbacks because
the deadlock aliases are not defined on every platform."""

_DEFAULT_LOCK_TIMEOUT_SECONDS = 60.0
"""Default deadline for :class:`AdvisoryFileLock` acquisition. The
bounded acquire is a safety net that converts an indefinite block on a
wedged or crashed holder into a :class:`FileLockTimeoutError`; this
deadline is set generously so that normal contention resolves well
within it and a timeout reliably signals a stuck holder."""

_LOCK_POLL_INTERVAL_SECONDS = 0.05
"""Sleep between non-blocking acquire attempts (50 ms) while waiting for
a contended lock."""

_WINDOWS_RETRY_ATTEMPTS = 5
"""Number of times Windows file ops retry on ``PermissionError``."""

_WINDOWS_RETRY_BACKOFF_SECONDS = 0.02
"""Sleep between Windows retries (20 ms); total worst-case wait is
``(_WINDOWS_RETRY_ATTEMPTS - 1) * _WINDOWS_RETRY_BACKOFF_SECONDS``."""

# ``fcntl`` and ``msvcrt`` are imported lazily inside each helper
# so this module loads cleanly on the opposite platform; the
# helper is only reached after ``_try_acquire`` / ``_release`` have
# verified ``os.name`` matches.


def _try_acquire_posix(fd: int) -> bool:
    """Attempt a non-blocking exclusive whole-file lock on ``fd`` (POSIX).

    Args:
        fd (int): File descriptor of an open file.

    Returns:
        bool: ``True`` if the lock was acquired, ``False`` if it is
            currently held by another process.

    Raises:
        OSError: Any failure other than the lock being contended.
    """
    import fcntl

    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def _release_posix(fd: int) -> None:
    """Release the whole-file lock held on ``fd`` (POSIX).

    Args:
        fd (int): File descriptor of the same file passed to
            :func:`_try_acquire_posix`.
    """
    import fcntl

    fcntl.flock(fd, fcntl.LOCK_UN)


def _try_acquire_nt(fd: int) -> bool:
    """Attempt a non-blocking exclusive single-byte lock on ``fd`` (Windows).

    Locks the first byte of the file; treated by
    :class:`AdvisoryFileLock` as an advisory cross-process gate.

    Args:
        fd (int): File descriptor of an open file.

    Returns:
        bool: ``True`` if the lock was acquired, ``False`` if it is
            currently held by another process.

    Raises:
        OSError: Any failure other than the lock being contended.
    """
    import msvcrt

    try:
        msvcrt.locking(  # type: ignore[attr-defined]
            fd, msvcrt.LK_NBLCK, 1  # type: ignore[attr-defined]
        )
    except OSError as exc:
        # ``LK_NBLCK`` raises EDEADLOCK / EACCES when the region is
        # already locked; treat only those as contention. Any other
        # ``OSError`` (e.g. a bad descriptor) is a genuine fault and
        # must propagate rather than be retried until the deadline.
        if exc.errno in _NT_LOCK_CONTENDED_ERRNOS:
            return False
        raise
    return True


def _release_nt(fd: int) -> None:
    """Release the single-byte lock on ``fd`` (Windows); best-effort.

    Swallows :class:`OSError` from ``LK_UNLCK`` so the file-close
    path stays clean if the region is not locked (e.g. the lock
    was already released by another caller).

    Args:
        fd (int): File descriptor of the same file passed to
            :func:`_try_acquire_nt`.
    """
    import msvcrt

    try:
        msvcrt.locking(  # type: ignore[attr-defined]
            fd, msvcrt.LK_UNLCK, 1  # type: ignore[attr-defined]
        )
    except OSError:
        pass


def _try_acquire(fd: int) -> bool:
    """Attempt a non-blocking exclusive lock on ``fd``, dispatching by ``os.name``.

    Args:
        fd (int): File descriptor of an open file.

    Returns:
        bool: ``True`` if the lock was acquired, ``False`` if it is
            currently held by another process.

    Raises:
        InternalError: If ``os.name`` is not in
            ``{"posix", "nt"}``.
    """
    if os.name == "posix":
        return _try_acquire_posix(fd)
    elif os.name == "nt":
        return _try_acquire_nt(fd)
    else:
        raise unsupported_os_error("AdvisoryFileLock")


def _release(fd: int) -> None:
    """Release the exclusive lock on ``fd``, dispatching by ``os.name``.

    Args:
        fd (int): File descriptor of the same file passed to
            :func:`_acquire`.

    Raises:
        InternalError: If ``os.name`` is not in
            ``{"posix", "nt"}``.
    """
    if os.name == "posix":
        _release_posix(fd)
    elif os.name == "nt":
        _release_nt(fd)
    else:
        raise unsupported_os_error("AdvisoryFileLock")


class AdvisoryFileLock:
    """Cross-platform advisory file lock, usable as a context manager.

    Construction captures the lock file path and an acquisition
    deadline; no I/O happens until ``__enter__``. On enter, the
    parent directory is created if missing, the lock file is opened
    (created if missing, never truncated), and an exclusive lock is
    acquired by polling a non-blocking attempt until it succeeds or
    the timeout elapses. On exit, the lock is released and the file
    handle closed even if the protected block raises.

    The bounded acquire is deliberate: a blocking ``flock`` against
    a crashed or wedged holder would hang the caller forever with no
    diagnostic. Polling with a deadline converts that into a
    :class:`~deephaven_mcp._exceptions.FileLockTimeoutError` the
    caller can surface.

    A single instance is not reentrant — exit one context before
    entering it again. A second :class:`AdvisoryFileLock` instance
    pointing at the same path (in this or another process) will wait
    on ``__enter__`` until the current holder exits or the timeout
    elapses.

    Usage::

        with AdvisoryFileLock(path):
            ...  # critical section
    """

    def __init__(
        self,
        path: Path,
        *,
        timeout_seconds: float = _DEFAULT_LOCK_TIMEOUT_SECONDS,
        poll_interval_seconds: float = _LOCK_POLL_INTERVAL_SECONDS,
    ) -> None:
        """Capture the lock file path and acquisition deadline; no I/O here.

        Args:
            path (Path): Filesystem path of the lock file. The
                parent directory is created on ``__enter__`` if
                missing.
            timeout_seconds (float): Maximum wall-clock time
                ``__enter__`` will wait for the lock before raising
                :class:`~deephaven_mcp._exceptions.FileLockTimeoutError`.
                Defaults to :data:`_DEFAULT_LOCK_TIMEOUT_SECONDS`.
            poll_interval_seconds (float): Sleep between
                non-blocking acquire attempts while the lock is
                contended. Defaults to
                :data:`_LOCK_POLL_INTERVAL_SECONDS`.

        Raises:
            InternalError: If ``os.name`` is not in
                ``{"posix", "nt"}``. Fails fast at construction so
                callers do not build a lock they cannot use.
        """
        if os.name not in SUPPORTED_OS_NAMES:
            raise unsupported_os_error("AdvisoryFileLock")
        self._path = path
        self._timeout_seconds = timeout_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._fh: IO[bytes] | None = None

    def __enter__(self) -> AdvisoryFileLock:
        """Open the lock file and acquire an exclusive lock, bounded by the timeout.

        Creates the parent directory if missing, opens the lock file
        in ``ab+`` mode (create-if-missing, never-truncate), and
        polls a non-blocking acquire until it succeeds or
        ``timeout_seconds`` elapses.

        Returns:
            AdvisoryFileLock: ``self``, for ``with ... as lock:`` form.

        Raises:
            FileLockTimeoutError: If the lock is not acquired within
                ``timeout_seconds``. The lock file handle is closed
                before raising.
            OSError: If the lock file or its parent directory cannot
                be created or opened.
        """
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # ``ab+`` creates the file if missing and never truncates,
        # so concurrent acquirers share the same inode safely.
        self._fh = open(self._path, "ab+")  # noqa: SIM115 - released in __exit__
        deadline = time.monotonic() + self._timeout_seconds
        while True:
            if _try_acquire(self._fh.fileno()):
                return self
            if time.monotonic() >= deadline:
                # Close the handle we opened so a timed-out acquire
                # does not leak a descriptor; the lock was never held.
                self._fh.close()
                self._fh = None
                raise FileLockTimeoutError(
                    f"Could not acquire advisory lock at {self._path} within "
                    f"{self._timeout_seconds}s; another process may be holding "
                    f"it or a previous holder crashed without releasing."
                )
            time.sleep(self._poll_interval_seconds)

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Release the lock and close the file handle.

        No-op if ``__enter__`` was never called (e.g. the instance
        was constructed but never used as a context manager).
        """
        if self._fh is None:
            return
        try:
            _release(self._fh.fileno())
        finally:
            self._fh.close()
            self._fh = None


def replace_with_retry(src: Path, dst: Path) -> None:
    """Atomically rename ``src`` to ``dst``, retrying transient Windows errors.

    On POSIX this is a single :func:`os.replace`. On Windows a
    concurrent reader's open handle (or antivirus / indexer
    transiently scanning ``dst``) can cause ``MoveFileExW`` to
    raise :class:`PermissionError` even though the lock invariant
    is satisfied; the retry loop absorbs that without forcing
    readers into the lock protocol.

    Args:
        src (Path): Existing file to rename.
        dst (Path): Destination path; replaced if it exists.

    Raises:
        PermissionError: When all Windows retries are exhausted.
        OSError: Any non-``PermissionError`` failure from
            :func:`os.replace`.
    """
    if os.name != "nt":
        os.replace(src, dst)
        return
    for attempt in range(_WINDOWS_RETRY_ATTEMPTS):
        try:
            os.replace(src, dst)
            return
        except PermissionError as exc:
            if attempt + 1 >= _WINDOWS_RETRY_ATTEMPTS:
                raise
            _LOGGER.debug(
                f"[_platform.fsutil:replace_with_retry] Retry {attempt + 1} "
                f"after PermissionError replacing {src} -> {dst}: {exc}"
            )
            time.sleep(_WINDOWS_RETRY_BACKOFF_SECONDS)


def unlink_with_retry(path: Path) -> None:
    """Delete ``path``, retrying transient Windows ``PermissionError``.

    On POSIX this is a single :meth:`pathlib.Path.unlink` (with
    ``missing_ok=True``). On Windows a concurrent reader's open
    handle can transiently block deletion; the retry loop absorbs
    that. A genuinely-absent file is never an error on either
    platform.

    Args:
        path (Path): File to remove.

    Raises:
        PermissionError: When all Windows retries are exhausted.
        OSError: Any non-``PermissionError``, non-``FileNotFoundError``
            failure from :meth:`pathlib.Path.unlink`.
    """
    if os.name != "nt":
        path.unlink(missing_ok=True)
        return
    for attempt in range(_WINDOWS_RETRY_ATTEMPTS):
        try:
            path.unlink(missing_ok=True)
            return
        except PermissionError as exc:
            if attempt + 1 >= _WINDOWS_RETRY_ATTEMPTS:
                raise
            _LOGGER.debug(
                f"[_platform.fsutil:unlink_with_retry] Retry {attempt + 1} "
                f"after PermissionError unlinking {path}: {exc}"
            )
            time.sleep(_WINDOWS_RETRY_BACKOFF_SECONDS)


def _fsync_dir(directory: Path) -> None:
    """Best-effort ``fsync`` of a directory so a rename into it persists.

    POSIX-only and advisory: some filesystems reject ``O_DIRECTORY``
    or directory ``fsync`` (returning ``EINVAL`` / ``EACCES``). Such
    failures are logged at debug and swallowed rather than failing
    the write — durability of the directory entry is a best-effort
    hardening, not a correctness precondition. No-op on Windows,
    which has no directory-handle ``fsync`` equivalent.

    Args:
        directory (Path): The directory whose entries were just
            modified by a rename.
    """
    if os.name != "posix":
        return
    try:
        dir_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    except OSError as exc:
        _LOGGER.debug(
            f"[_platform.fsutil:_fsync_dir] Cannot open {directory} for fsync; "
            f"skipping directory durability: {exc}"
        )
        return
    try:
        os.fsync(dir_fd)
    except OSError as exc:
        _LOGGER.debug(
            f"[_platform.fsutil:_fsync_dir] fsync of {directory} unsupported; "
            f"skipping directory durability: {exc}"
        )
    finally:
        os.close(dir_fd)


def atomic_write_private(path: Path, data: bytes, *, mode: int = 0o600) -> None:
    """Atomically write ``data`` to ``path`` with owner-only permissions.

    Writes to a temporary file in ``path``'s parent directory,
    fsyncs it, sets the file mode on POSIX, then renames it over
    ``path``. The rename is atomic at the directory-entry level, so
    a concurrent reader observes either the old contents or the new
    ones, never a torn intermediate. The parent directory is fsynced
    after the rename so the new entry survives a crash. On any
    failure the temporary file is removed and the exception
    re-raised, leaving any pre-existing ``path`` untouched.

    The parent directory must already exist; callers own its
    creation and hardening.

    Args:
        path (Path): Destination file path. Replaced atomically if
            it already exists.
        data (bytes): Exact bytes to write.
        mode (int): POSIX file mode applied to the published file.
            Defaults to ``0o600`` (owner read/write only). Ignored
            on Windows, where ``os.chmod`` cannot express a full
            ACL; the file inherits its container's per-user ACL.

    Raises:
        OSError: If the temporary file cannot be created, written,
            or renamed over ``path``.
    """
    directory = path.parent
    # ``mkstemp`` creates the temp file with mode 0o600 on POSIX;
    # the explicit chmod below pins the mode against umask quirks.
    fd, tmp_name = tempfile.mkstemp(
        dir=directory, prefix=path.name + ".", suffix=".tmp"
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        if os.name == "posix":
            os.chmod(tmp_path, mode)
        # On Windows, ``os.chmod`` cannot express a 0o600 ACL (only
        # the read-only bit) so we deliberately do not call it; the
        # file is user-private by virtue of its container's per-user
        # ACL. Explicit ``icacls`` SID restriction is tracked in
        # ``docs/SECURITY.md``.
        replace_with_retry(tmp_path, path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    _fsync_dir(directory)
