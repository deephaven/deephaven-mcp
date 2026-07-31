"""Detached background-process spawn, dispatched on ``os.name``.

Launches a fully detached child whose stdout/stderr are redirected to a
log file and whose process group is isolated from the parent so signals
to the parent do not propagate to it: ``start_new_session`` on POSIX,
``creationflags=CREATE_NEW_PROCESS_GROUP`` on Windows. Any other
``os.name`` is rejected via
:func:`~deephaven_mcp._platform._os_support.unsupported_os_error`.
"""

from __future__ import annotations

__all__ = ["spawn_detached"]

import logging
import os
import subprocess
from pathlib import Path
from typing import IO

from deephaven_mcp._platform._os_support import unsupported_os_error

_LOGGER = logging.getLogger(__name__)

_CREATE_NEW_PROCESS_GROUP = 0x00000200
"""Windows ``CreateProcess`` flag value mirroring
``subprocess.CREATE_NEW_PROCESS_GROUP`` (the constant is only
defined on Windows builds of the standard library; the literal
is used here so the module imports on POSIX). Puts the spawned
process into a fresh console process group so signals delivered
to the parent do not propagate to it."""


def _spawn_posix(argv: list[str], *, cwd: Path, log_fh: IO[bytes]) -> None:
    """Spawn ``argv`` detached on POSIX via ``start_new_session=True``.

    Args:
        argv (list[str]): Command line as an argv list (no shell).
        cwd (Path): Working directory the child inherits.
        log_fh (IO[bytes]): Open binary file the child's stdout/stderr
            are redirected to. The caller owns closing it.
    """
    # ``argv`` is a list (no ``shell=True``), which prevents shell
    # injection; ``S603`` is bandit's generic subprocess warning.
    subprocess.Popen(  # noqa: S603
        argv,
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        close_fds=True,
        start_new_session=True,
    )


def _spawn_nt(argv: list[str], *, cwd: Path, log_fh: IO[bytes]) -> None:
    """Spawn ``argv`` detached on Windows via ``CREATE_NEW_PROCESS_GROUP``.

    Args:
        argv (list[str]): Command line as an argv list (no shell).
        cwd (Path): Working directory the child inherits.
        log_fh (IO[bytes]): Open binary file the child's stdout/stderr
            are redirected to. The caller owns closing it.
    """
    subprocess.Popen(  # noqa: S603
        argv,
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        close_fds=True,
        creationflags=_CREATE_NEW_PROCESS_GROUP,
    )


def spawn_detached(argv: list[str], *, cwd: Path, log_path: Path) -> None:
    """Spawn ``argv`` as a detached background process.

    The child's stdin is ``/dev/null``; its stdout and stderr are
    redirected to ``log_path`` (append mode) so any startup error is
    captured even after the parent exits. On POSIX,
    ``start_new_session=True`` puts the child in a fresh process
    group so signals delivered to the parent do not propagate; on
    Windows the equivalent ``creationflags=CREATE_NEW_PROCESS_GROUP``
    is set.

    **Precondition:** ``log_path``'s parent directory must already
    exist (callers own its creation and any permission hardening).

    Args:
        argv (list[str]): The command line to launch, as an argv
            list (no shell). The list form prevents shell injection.
        cwd (Path): Working directory the spawned process inherits.
        log_path (Path): File the child's stdout/stderr are appended
            to.

    Raises:
        InternalError: If ``os.name`` is not in ``{"posix", "nt"}``.
        OSError: If ``log_path`` cannot be opened or the process
            cannot be spawned.
    """
    # Select the per-OS spawn primitive before any I/O so an
    # unsupported platform fails fast without creating an empty log
    # file. The explicit ``else`` is the catch-all: a future os.name
    # added to the supported set without a matching branch here is
    # rejected rather than silently assuming one platform's path.
    if os.name == "posix":
        spawn = _spawn_posix
    elif os.name == "nt":
        spawn = _spawn_nt
    else:
        raise unsupported_os_error("spawn_detached")

    _LOGGER.info(f"[_platform.spawn:spawn_detached] Spawning: {' '.join(argv)}")
    # ``Popen`` duplicates the underlying file descriptor for the
    # child; the parent's handle must be closed once ``Popen`` has
    # returned, otherwise the parent leaks one fd per spawn (and
    # holds a reference to the log file that prevents log rotation
    # from reclaiming the inode on POSIX). The ``with`` block closes
    # ``log_fh`` after the spawn returns so only the child's
    # duplicated descriptor remains.
    with open(log_path, "ab") as log_fh:
        spawn(argv, cwd=cwd, log_fh=log_fh)
