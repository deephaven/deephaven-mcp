"""CLI daemon lifecycle — spawn, locate, and stop the local daemon.

Public functions take a small input value object plus the explicit
tunables each operation needs, rather than the full :class:`Runtime`:

- :class:`DaemonContext` — *what* daemon is being managed: its
  on-disk directory, the argv used to spawn it, and the working
  directory the spawn should run in. Consumed whole by the spawn
  path (:func:`get_or_start_daemon`).
- Per-operation keyword tunables — *how* it should be managed:
  :func:`get_or_start_daemon` takes ``auto_start`` and
  ``startup_deadline_seconds``; :func:`stop_daemon` takes only a
  :class:`~deephaven_mcp.daemon_registry.DaemonDirectory` and
  ``kill_after_seconds``. Each signature declares exactly the inputs
  that operation reads.

The command layer (:mod:`deephaven_mcp.cli._commands.daemon`)
builds a :class:`DaemonContext` via :func:`build_daemon_context` once
per verb invocation and reads the tunables from
``runtime.config.cli.daemon``. Keeping the lifecycle functions off
:class:`Runtime` lets tests call them with focused scalar arguments
rather than constructing a full runtime.

This module is pure orchestration: the OS-specific spawn mechanic
lives in :func:`deephaven_mcp._platform.spawn.spawn_detached`, and the
PID-reuse defense in
:class:`~deephaven_mcp._processes.ProcessIdentity`. Liveness is
decided by
:meth:`~deephaven_mcp.daemon_registry.DaemonRegistryEntry.is_live`.
What remains here:

- **Registry polling** (``_poll_for_registry``): wait for a freshly
  spawned daemon to publish its ``daemon.json`` entry.
- **Lifecycle orchestration** (:func:`get_or_start_daemon`,
  :func:`stop_daemon`): the "find existing or spawn new" and
  "terminate with PID-reuse defense" semantics CLI subcommands
  actually call.

Public API:

- :class:`DaemonContext` — the spawn-path input value object.
- :func:`build_daemon_context` — the :class:`Runtime` ->
  :class:`DaemonContext` translation. The only public function that
  depends on :class:`Runtime`; consumed by the command layer once
  per verb invocation.
- :func:`get_or_start_daemon` — read the registry, validate
  liveness, and (when ``auto_start`` is true) spawn a fresh daemon
  if no running instance is found.
- :func:`stop_daemon` — terminate the registered daemon process,
  escalating to ``SIGKILL`` after ``kill_after_seconds``.

The daemon lifecycle exceptions
(:class:`~deephaven_mcp._exceptions.DaemonClientError` and
:class:`~deephaven_mcp._exceptions.DaemonStartupTimeoutError`)
live in :mod:`deephaven_mcp._exceptions` per the project's
exception-organization rule, and are re-exported from this module
for caller convenience.
"""

from __future__ import annotations

__all__ = [
    "DaemonClientError",
    "DaemonContext",
    "DaemonStartupTimeoutError",
    "build_daemon_context",
    "get_or_start_daemon",
    "stop_daemon",
]

import asyncio
import logging
import signal
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from deephaven_mcp._exceptions import (
    DaemonClientError,
    DaemonStartupTimeoutError,
)
from deephaven_mcp._platform.spawn import spawn_detached
from deephaven_mcp._processes import ProcessIdentity, SignalOutcome
from deephaven_mcp.config import harden_private_dir
from deephaven_mcp.daemon_registry import DaemonDirectory, DaemonRegistryEntry

from ._runtime import Runtime

_LOGGER = logging.getLogger(__name__)

_POLL_INTERVAL_SECONDS = 0.05
"""Polling cadence for ``daemon.json`` appearance and post-SIGTERM
liveness checks. Short enough that subcommand startup feels snappy
when the daemon is already cached, low enough to avoid wasted CPU."""


# ---------------------------------------------------------------------------
# Input value objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DaemonContext:
    """Spawn-path inputs to :func:`get_or_start_daemon`.

    Carries everything needed to *spawn* a daemon. :func:`stop_daemon`
    needs no spawn parameters, so it takes a bare
    :class:`~deephaven_mcp.daemon_registry.DaemonDirectory` instead of
    a :class:`DaemonContext`.
    """

    directory: DaemonDirectory
    """Typed handle to ``<runtime_dir>/daemon/`` exposing the
    registry / lock / log paths and atomic registry CRUD."""

    spawn_argv: list[str]
    """Argv list that launches a fresh daemon process."""

    spawn_cwd: Path
    """Working directory the spawned daemon process inherits."""


# ---------------------------------------------------------------------------
# Runtime -> DaemonContext bridge
# ---------------------------------------------------------------------------


def _build_spawn_command(runtime: Runtime) -> list[str]:
    """Return the argv that launches ``dh-mcp-systems-server --daemon``.

    Built from :data:`sys.executable` rather than the
    ``dh-mcp-systems-server`` entry-point script so the spawned
    daemon runs under the same Python interpreter as the caller.

    Args:
        runtime (Runtime): The CLI's pre-resolved runtime context.
            Read for ``config_dir`` and ``runtime_dir`` only.

    Returns:
        list[str]: The argv list, suitable for
            :class:`DaemonContext.spawn_argv`.
    """
    return [
        sys.executable,
        "-m",
        "deephaven_mcp.mcp_systems_server",
        "--daemon",
        "--config-dir",
        str(runtime.config_dir),
        "--runtime-dir",
        str(runtime.runtime_dir),
    ]


def build_daemon_context(runtime: Runtime) -> DaemonContext:
    """Translate a :class:`Runtime` into a :class:`DaemonContext`.

    The lifecycle tunables (``auto_start``, ``startup_deadline_seconds``,
    ``kill_after_seconds``) are read directly off
    ``runtime.config.cli.daemon`` at the call site and passed as
    keyword arguments to :func:`get_or_start_daemon` / :func:`stop_daemon`.

    Args:
        runtime (Runtime): The CLI's pre-resolved runtime.

    Returns:
        DaemonContext: The spawn-path input for
            :func:`get_or_start_daemon`.
    """
    return DaemonContext(
        directory=runtime.daemon_dir,
        spawn_argv=_build_spawn_command(runtime),
        spawn_cwd=runtime.runtime_dir,
    )


# ---------------------------------------------------------------------------
# Registry polling
# ---------------------------------------------------------------------------


async def _poll_for_registry(
    ctx: DaemonContext, *, deadline_seconds: int
) -> DaemonRegistryEntry:
    """Poll ``daemon.json`` until a live entry appears or we time out.

    The poll interval is short (50 ms) so subcommand startup feels
    snappy even when the daemon is already cached and warm.

    Args:
        ctx (DaemonContext): The daemon-management context;
            supplies the directory whose ``daemon.json`` is polled.
        deadline_seconds (int): Maximum wall-clock time to wait.

    Returns:
        DaemonRegistryEntry: The validated registry entry.

    Raises:
        DaemonStartupTimeoutError: When the deadline elapses without
            a valid, live entry appearing.
        RegistryCorruptError: Propagated unchanged from
            :meth:`DaemonDirectory.read_entry` if the registry file
            cannot be parsed. The daemon writes its registry once
            atomically via :func:`os.replace`, so corruption in
            this window is a real bug (most likely an external
            process scribbling on the file); silently retrying
            would mask the diagnostic until the deadline elapses
            and then surface as the wrong error type.
    """
    start = time.monotonic()
    deadline = start + deadline_seconds
    while True:
        entry = ctx.directory.read_entry()
        if entry is not None and entry.is_live():
            return entry
        if time.monotonic() >= deadline:
            log_path = ctx.directory.log_path
            raise DaemonStartupTimeoutError(
                f"Daemon did not start within {deadline_seconds}s. "
                f"Inspect {log_path} for diagnostics."
            )
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Lifecycle orchestration
# ---------------------------------------------------------------------------


async def get_or_start_daemon(
    ctx: DaemonContext, *, auto_start: bool, startup_deadline_seconds: int
) -> DaemonRegistryEntry:
    """Return a :class:`DaemonRegistryEntry` for a live daemon, spawning one if needed.

    The function is the single entry point all CLI subcommands use
    to obtain connection details.

    Resolution order:

    1. Lock-free read of ``daemon.json``. If it parses cleanly and
       :meth:`~deephaven_mcp.daemon_registry.DaemonRegistryEntry.is_live`
       confirms the recorded ``(pid, create_time_ns)`` still maps to a
       running process, return immediately.
    2. Otherwise enter :meth:`DaemonDirectory.locked`, re-read
       inside the lock, delete a stale entry if present, and —
       when ``auto_start`` is true — decide whether to spawn.
       A *fresh* ``daemon.starting`` marker means a peer CLI is
       already mid-spawn, so this caller defers and only polls; a
       missing or stale marker means this caller claims the spawn:
       it stamps a new marker, hardens the directory, and launches
       the daemon. Either way the lock is released and the caller
       polls until the daemon publishes its entry, then returns.
       On a startup timeout the caller clears any marker it wrote so
       the next attempt is not blocked behind a stale flag.
    3. When ``auto_start`` is false and no live daemon is
       registered, raise :class:`DaemonClientError` (after cleaning
       up any stale entry under the lock).

    Args:
        ctx (DaemonContext): The daemon-management context;
            supplies the directory and spawn parameters.
        auto_start (bool): When ``True``, spawn a fresh daemon if no
            live one is registered. When ``False``, the absence of a
            registered daemon is an error.
        startup_deadline_seconds (int): Maximum wall-clock seconds to
            wait for a freshly spawned daemon to publish its registry
            entry before raising :class:`DaemonStartupTimeoutError`.

    Returns:
        DaemonRegistryEntry: The validated registry entry for the
            live daemon. Carries everything callers need to connect
            (``host``, ``port``, ``psk``) plus the operator-facing
            telemetry (``pid``, ``create_time_ns``, ``process_name``,
            ``started_at``, ``config_dir``, ``server_name``).

    Raises:
        DaemonClientError: When no live daemon is found and
            ``auto_start`` is false.
        DaemonStartupTimeoutError: When the spawned daemon does not
            publish a registry entry within
            ``startup_deadline_seconds``.
        RegistryCorruptError: When ``daemon.json`` exists but cannot
            be parsed. Propagated unchanged to the command layer,
            which translates to ``CliError(DAEMON_REGISTRY_CORRUPT)``
            with a recovery hint pointing at ``dh-mcp daemon reset``.
    """
    entry = ctx.directory.read_entry()
    if entry is not None and entry.is_live():
        _LOGGER.debug(
            f"[_daemon:get_or_start_daemon] Reusing existing "
            f"daemon pid={entry.pid} on {entry.host}:{entry.port}"
        )
        return entry

    # Slow path: any decision below mutates the registry, so take
    # the lock and re-read inside it.
    spawned = False
    with ctx.directory.locked() as reg:
        entry = reg.read()
        if entry is not None and entry.is_live():
            return entry
        if entry is not None:
            _LOGGER.info(
                f"[_daemon:get_or_start_daemon] Stale registry "
                f"(pid={entry.pid} not running); removing"
            )
            reg.delete()
        if not auto_start:
            raise DaemonClientError(
                "No daemon is running and auto-start is disabled. "
                "Run `dh-mcp daemon start` or set "
                "`daemon.auto_start: true` in cli.json."
            )
        # Double-spawn guard: a *fresh* start marker means a peer
        # CLI already launched a daemon and is waiting for it to
        # publish. Defer to that spawn and only poll. A missing or
        # stale marker (previous spawner crashed before publishing)
        # means we claim the spawn ourselves.
        marker = reg.read_start_marker()
        now = datetime.now(UTC)
        if marker is not None and (now - marker) < timedelta(
            seconds=startup_deadline_seconds
        ):
            _LOGGER.info(
                f"[_daemon:get_or_start_daemon] Spawn already in progress "
                f"(marker {marker.isoformat()}); awaiting peer's daemon"
            )
        else:
            # Harden inside the lock and before spawn so the first
            # daemon I/O lands in a 0o700 directory.
            reg.write_start_marker(now)
            harden_private_dir(ctx.directory.path)
            spawn_detached(
                ctx.spawn_argv,
                cwd=ctx.spawn_cwd,
                log_path=ctx.directory.log_path,
            )
            spawned = True

    # Poll outside the lock so the spawned daemon can acquire the
    # lock to publish its entry (and clear the marker).
    try:
        return await _poll_for_registry(ctx, deadline_seconds=startup_deadline_seconds)
    except DaemonStartupTimeoutError:
        # Clear the marker we wrote so the next attempt is not blocked
        # behind a stale "spawn in progress" flag for the remainder of
        # the staleness window. A daemon that publishes late clears its
        # own marker, so only the spawner that timed out cleans up here.
        if spawned:
            with ctx.directory.locked() as reg:
                reg.clear_start_marker()
        raise


async def stop_daemon(directory: DaemonDirectory, *, kill_after_seconds: int) -> bool:
    """Terminate a running daemon if one is registered.

    Holds the :meth:`DaemonDirectory.locked` session for the entire
    operation — read → SIGTERM → wait → (optional) SIGKILL →
    delete — so a peer ``dh-mcp`` cannot publish a replacement
    daemon while this one is being torn down. Other CLI commands
    block on the lock for the duration of the stop (typically well
    under one second; up to ``kill_after_seconds`` on a stuck
    daemon).

    Sends ``signal.SIGTERM`` via :func:`os.kill` on both POSIX and
    Windows (on Windows this maps to ``TerminateProcess``), waits
    up to ``kill_after_seconds`` for the process to exit, then
    escalates to ``signal.SIGKILL`` (``TerminateProcess`` again
    on Windows; effectively a forced kill in both cases). A
    dedicated ``CTRL_BREAK_EVENT`` path for Windows console daemons
    is not currently implemented.

    Args:
        directory (DaemonDirectory): Typed handle to the daemon
            directory whose registry is read and mutated. Stopping
            never spawns, so the spawn argv / cwd carried by
            :class:`DaemonContext` are not needed here.
        kill_after_seconds (int): Maximum wall-clock seconds to wait
            after sending SIGTERM before escalating to SIGKILL.

    Returns:
        bool: ``True`` when a daemon was found and terminated;
            ``False`` when no live daemon was registered.

    Raises:
        DaemonClientError: When the caller lacks permission to
            signal the registered PID.
    """
    with directory.locked() as reg:
        entry = reg.read()
        if entry is None:
            return False
        if not entry.is_live():
            # Stale registry — clean up and exit.
            reg.delete()
            return False

        # The PID-reuse defense lives entirely in
        # :class:`ProcessIdentity`: the captured ``(pid,
        # create_time_ns)`` pair is read straight off the registry
        # entry, every signal goes through
        # :meth:`ProcessIdentity.send_signal_safely` (which re-checks
        # create-time at the kernel before delivering), and every
        # poll uses :meth:`ProcessIdentity.is_alive`.
        identity = entry.identity
        sigterm_outcome = identity.send_signal_safely(signal.SIGTERM)
        if sigterm_outcome is SignalOutcome.DELIVERED:
            _LOGGER.info(f"[_daemon:stop_daemon] Sent SIGTERM to pid={entry.pid}")
        elif sigterm_outcome in (SignalOutcome.GONE, SignalOutcome.RECYCLED):
            # Process is already gone or its PID was recycled between
            # the liveness gate and now. Either way, no daemon to
            # terminate; clean up the registry and exit.
            reg.delete()
            return False
        else:  # SignalOutcome.DENIED
            raise DaemonClientError(
                f"Cannot signal daemon pid={entry.pid}: permission denied. "
                f"The CLI must run as the same user that started the daemon."
            )

        if await _wait_for_exit(identity, kill_after_seconds=kill_after_seconds):
            reg.delete()
            return True

        _LOGGER.warning(
            f"[_daemon:stop_daemon] pid={entry.pid} did not exit after "
            f"{kill_after_seconds}s; escalating to SIGKILL"
        )
        sigkill_outcome = identity.send_signal_safely(signal.SIGKILL)
        if sigkill_outcome is SignalOutcome.DENIED:
            raise DaemonClientError(
                f"Cannot SIGKILL daemon pid={entry.pid}: permission denied."
            )
        # GONE / RECYCLED on SIGKILL means the daemon exited on its
        # own between the wait deadline and the kill syscall. That is
        # the desired post-condition; treat as success.
        reg.delete()
        return True


async def _wait_for_exit(identity: ProcessIdentity, *, kill_after_seconds: int) -> bool:
    """Wait up to ``kill_after_seconds`` for ``identity`` to exit.

    Polls :meth:`ProcessIdentity.is_alive` at
    :data:`_POLL_INTERVAL_SECONDS` cadence; also re-checks once
    after the deadline so a process that exited between the last
    in-loop poll and the deadline is recognized.

    Args:
        identity (ProcessIdentity): The captured pid + create-time
            pair.
        kill_after_seconds (int): Maximum wall-clock seconds to wait.

    Returns:
        bool: ``True`` when the process exited within the deadline;
            ``False`` otherwise.
    """
    deadline = time.monotonic() + kill_after_seconds
    while time.monotonic() < deadline:
        if not identity.is_alive():
            return True
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)
    # Lost the race between the last in-loop poll and the deadline:
    # the daemon may have exited just before we returned False above.
    return not identity.is_alive()
