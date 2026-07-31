"""Cross-platform process identity and signaling.

Provides :class:`ProcessIdentity`, a frozen ``(pid, create_time_ns)``
pair that uniquely identifies a process across its lifetime, and
:class:`SignalOutcome`, the four-state result of
:meth:`ProcessIdentity.send_signal_safely`.

:attr:`ProcessIdentity.create_time_ns` is an integer nanosecond count
derived from :meth:`psutil.Process.create_time` by
``int(value * 1e9)``. Comparison in :meth:`ProcessIdentity.is_alive`
is exact integer equality.

These primitives are portable (psutil / :func:`os.kill`) and do not
branch on ``os.name``; detached process spawn, which does, lives in
:mod:`deephaven_mcp._platform.spawn`.
"""

from __future__ import annotations

__all__ = ["ProcessIdentity", "SignalOutcome"]

import logging
import os
import signal
from dataclasses import dataclass
from enum import StrEnum

import psutil

_LOGGER = logging.getLogger(__name__)


class SignalOutcome(StrEnum):
    """Result of :meth:`ProcessIdentity.send_signal_safely`."""

    DELIVERED = "delivered"
    """The signal was delivered to the recorded process. The
    receiver is not guaranteed to have acted on it yet."""

    RECYCLED = "recycled"
    """The PID exists but its create time no longer matches the
    captured value. No signal was sent."""

    GONE = "gone"
    """The PID no longer exists. No signal was sent."""

    DENIED = "denied"
    """The OS refused the signal with ``EPERM``. No signal was sent."""


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    """A PID paired with the kernel's create-time token.

    The pair uniquely identifies a single process across its
    lifetime even in the presence of PID reuse.
    """

    pid: int
    """The OS process ID. Must be ``> 0``."""

    create_time_ns: int
    """The kernel-reported process creation time in integer
    nanoseconds (derived from :meth:`psutil.Process.create_time`
    * 1e9). Compared by exact equality."""

    @classmethod
    def of_current_process(cls) -> ProcessIdentity:
        """Capture the identity of the calling process.

        Returns:
            ProcessIdentity: The current process's identity.
        """
        proc = psutil.Process(os.getpid())
        return cls(pid=proc.pid, create_time_ns=_create_time_ns(proc))

    @classmethod
    def of_pid(cls, pid: int) -> ProcessIdentity | None:
        """Capture the identity of an arbitrary PID, if it exists.

        Args:
            pid (int): The OS process ID to probe.

        Returns:
            ProcessIdentity | None: The identity, or ``None`` when
                the PID does not exist, the caller lacks permission
                to read its metadata, or ``pid`` is out of range.
        """
        try:
            proc = psutil.Process(pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError):
            return None
        try:
            create_time_ns = _create_time_ns(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return None
        return cls(pid=pid, create_time_ns=create_time_ns)

    def is_alive(self) -> bool:
        """Return ``True`` iff the recorded process is still running.

        "Still running" means: the PID currently exists *and* its
        create time matches :attr:`create_time_ns`. A mismatch is
        treated as not-alive. Any :mod:`psutil` failure (PID gone,
        permission denied, garbled state) is also treated as
        not-alive. Does not log on mismatch.

        Returns:
            bool: ``True`` when both the PID exists and its create
                time matches; ``False`` otherwise.
        """
        try:
            proc = psutil.Process(self.pid)
            observed_ns = _create_time_ns(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError):
            return False
        return observed_ns == self.create_time_ns

    def send_signal_safely(self, sig: signal.Signals) -> SignalOutcome:
        """Deliver ``sig`` to :attr:`pid` only if the create time still matches.

        Re-reads the live process's create time and refuses to
        signal on a mismatch. Logs a warning on mismatch.

        On Windows, :func:`os.kill` only honors ``SIGTERM`` /
        ``SIGKILL`` (both mapped to ``TerminateProcess``) and the
        console events (``CTRL_C_EVENT`` / ``CTRL_BREAK_EVENT``);
        passing any other signal raises :class:`OSError` from the
        underlying Win32 API.

        Args:
            sig (signal.Signals): The signal to deliver.

        Returns:
            SignalOutcome: One of:

                - :attr:`SignalOutcome.DELIVERED` — signal was sent.
                - :attr:`SignalOutcome.RECYCLED` — PID exists but
                  create-time mismatch; signal *not* sent.
                - :attr:`SignalOutcome.GONE` — PID does not exist;
                  signal *not* sent.
                - :attr:`SignalOutcome.DENIED` — OS refused with
                  ``EPERM``; signal *not* sent.
        """
        try:
            proc = psutil.Process(self.pid)
            observed_ns = _create_time_ns(proc)
        except (psutil.NoSuchProcess, ValueError):
            return SignalOutcome.GONE
        except psutil.AccessDenied:
            # Cannot verify the create time; refuse to signal.
            return SignalOutcome.DENIED

        if observed_ns != self.create_time_ns:
            _LOGGER.warning(
                f"[_processes:ProcessIdentity.send_signal_safely] "
                f"pid={self.pid} create_time_ns mismatch "
                f"(captured={self.create_time_ns}, observed={observed_ns}); "
                f"refusing to deliver {sig.name}"
            )
            return SignalOutcome.RECYCLED

        try:
            os.kill(self.pid, sig)
        except ProcessLookupError:
            # Lost the race between create-time read and the kill.
            return SignalOutcome.GONE
        except PermissionError:
            return SignalOutcome.DENIED
        return SignalOutcome.DELIVERED


def _create_time_ns(proc: psutil.Process) -> int:
    """Return ``proc``'s create time in integer nanoseconds.

    Args:
        proc (psutil.Process): The live process handle.

    Returns:
        int: The process create time in nanoseconds since the
            POSIX epoch.

    Raises:
        psutil.NoSuchProcess: If the process has vanished.
        psutil.AccessDenied: If the caller cannot read the
            process's metadata.
    """
    return int(proc.create_time() * 1_000_000_000)
