"""CLI daemon lifecycle orchestration — find an existing daemon or spawn one.

The orchestration core of the ``_daemon`` package: the "find existing or spawn
new" and "terminate with PID-reuse defense" semantics the CLI subcommands call.
The surrounding concerns live in sibling modules:

- :mod:`._context` — the :class:`DaemonContext` spawn-path input value object
  and the :class:`~deephaven_mcp.cli._runtime.Runtime` bridge.
- :mod:`._reuse` — the pure per-field reuse-policy engine
  (:func:`~deephaven_mcp.cli._daemon._reuse.decide_reuse`); this module applies
  its decision.

PID-reuse-safe ``SIGTERM``/``SIGKILL`` termination (:func:`terminate_identity`)
lives in this module, since both :func:`stop_daemon` and the ``restart`` path
of :func:`get_or_start_daemon` drive it.

Public functions take a small input value object plus the explicit
tunables each operation needs, rather than the full :class:`Runtime`:

- :func:`get_or_start_daemon` takes a :class:`DaemonContext` plus ``auto_start``
  and ``startup_deadline_seconds`` (and the build-identity / reuse-policy
  inputs); :func:`stop_daemon` takes only a
  :class:`~deephaven_mcp.daemon_registry.DaemonDirectory` and
  ``kill_after_seconds``. Each signature declares exactly the inputs that
  operation reads, so tests call them with focused scalar arguments rather than
  constructing a full runtime.

This module is pure orchestration: the OS-specific spawn mechanic lives in
:func:`deephaven_mcp._platform.spawn.spawn_detached`, and the PID-reuse defense
in :class:`~deephaven_mcp._processes.ProcessIdentity`. Liveness is decided by
:meth:`~deephaven_mcp.daemon_registry.DaemonRegistryEntry.is_live`.

The daemon lifecycle exceptions
(:class:`~deephaven_mcp._exceptions.DaemonClientError`,
:class:`~deephaven_mcp._exceptions.DaemonReuseRefusedError`, and
:class:`~deephaven_mcp._exceptions.DaemonStartupTimeoutError`) live in
:mod:`deephaven_mcp._exceptions` per the project's exception-organization rule,
and are re-exported from the package's ``__init__`` for caller convenience.
"""

from __future__ import annotations

__all__ = [
    "get_or_start_daemon",
    "stop_daemon",
]

import asyncio
import logging
import signal
import time
from datetime import UTC, datetime, timedelta
from enum import Enum

from deephaven_mcp._exceptions import (
    DaemonClientError,
    DaemonReuseRefusedError,
    DaemonStartupTimeoutError,
    InternalError,
)
from deephaven_mcp._platform.spawn import spawn_detached
from deephaven_mcp._processes import ProcessIdentity, SignalOutcome
from deephaven_mcp.cli._errors import render_warning
from deephaven_mcp.cli._format import OutputMode
from deephaven_mcp.config import harden_private_dir
from deephaven_mcp.config.schema import DaemonReuseAction, DaemonReusePolicy
from deephaven_mcp.daemon_registry import (
    DaemonBuildIdentity,
    DaemonDirectory,
    DaemonRegistryEntry,
    LockedRegistry,
)

from ._context import DaemonContext
from ._reuse import decide_reuse, describe_difference

_LOGGER = logging.getLogger(__name__)

_POLL_INTERVAL_SECONDS = 0.05
"""Polling cadence for ``daemon.json`` appearance and post-SIGTERM
liveness checks. Short enough that subcommand startup feels snappy
when the daemon is already cached, low enough to avoid wasted CPU."""


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
# Reuse decision (applied)
# ---------------------------------------------------------------------------


class _ReuseVerdict(Enum):
    """Whether a live daemon may be reused or must be restarted.

    The two terminal outcomes of applying the reuse policy to a live daemon.
    The third policy possibility, ``refuse``, is not a verdict: it raises
    :class:`~deephaven_mcp._exceptions.DaemonReuseRefusedError` rather than
    returning.
    """

    REUSE = "reuse"
    """The live daemon is compatible (``ignore``/``warn``); reuse it as-is."""

    RESTART = "restart"
    """The live daemon is incompatible (``restart``); the caller terminates
    it and spawns a replacement."""


def _decide_for_live_entry(
    entry: DaemonRegistryEntry,
    *,
    expected_identity: DaemonBuildIdentity,
    reuse_policy: DaemonReusePolicy,
    can_spawn: bool,
    output_mode: OutputMode,
) -> _ReuseVerdict:
    """Resolve the reuse policy for a live daemon and apply its side effects.

    Delegates the pure decision to
    :func:`~deephaven_mcp.cli._daemon._reuse.decide_reuse`, then dispatches the
    resolved action: ``ignore`` reuses silently, ``warn`` emits a stderr
    warning and reuses, ``refuse`` raises, and ``restart`` signals the caller
    to terminate and respawn.

    Args:
        entry (DaemonRegistryEntry): The live daemon's registry entry.
        expected_identity (DaemonBuildIdentity): This CLI's own build identity.
        reuse_policy (DaemonReusePolicy): The per-field action policy.
        can_spawn (bool): Whether the caller may spawn a replacement; a
            ``restart`` degrades to ``refuse`` when ``False``.
        output_mode (OutputMode): Active output mode for the ``warn`` warning.

    Returns:
        _ReuseVerdict: :attr:`_ReuseVerdict.REUSE` for ``ignore``/``warn``;
            :attr:`_ReuseVerdict.RESTART` for ``restart``.

    Raises:
        DaemonReuseRefusedError: When the resolved action is ``refuse``.
    """
    decision = decide_reuse(
        expected_identity, entry.build_identity, reuse_policy, can_spawn=can_spawn
    )
    match decision.action:
        case DaemonReuseAction.IGNORE:
            return _ReuseVerdict.REUSE
        case DaemonReuseAction.WARN:
            detail = describe_difference(expected_identity, entry.build_identity)
            render_warning(
                f"Reusing daemon (pid {entry.pid}) that differs from this CLI: "
                f"{detail}. Run 'dh-mcp daemon restart' to replace it.",
                output=output_mode,
            )
            return _ReuseVerdict.REUSE
        case DaemonReuseAction.REFUSE:
            detail = describe_difference(expected_identity, entry.build_identity)
            raise DaemonReuseRefusedError(
                f"The running daemon (pid {entry.pid}) is a different build than "
                f"this CLI: {detail}. Run 'dh-mcp daemon restart' to replace it, "
                f"or adjust the daemon.reuse policy in cli.json.",
                differing=decision.differing,
            )
        case DaemonReuseAction.RESTART:
            return _ReuseVerdict.RESTART
        case _ as unexpected:
            # Unreachable while every DaemonReuseAction member is matched
            # above; raised as the project's internal-invariant signal if a
            # future member is added without a branch here.
            raise InternalError(
                f"Unhandled DaemonReuseAction in _decide_for_live_entry: "
                f"{unexpected!r}"
            )


# ---------------------------------------------------------------------------
# Spawn coordination
# ---------------------------------------------------------------------------


def _spawn_or_defer(
    reg: LockedRegistry, ctx: DaemonContext, *, startup_deadline_seconds: int
) -> bool:
    """Claim the spawn, or defer to a peer CLI already mid-spawn.

    Must be called while holding the registry lock. A *fresh*
    ``daemon.starting`` marker means a peer CLI already launched a daemon and
    is waiting for it to publish, so this caller defers and only polls. A
    missing or stale marker (the previous spawner crashed before publishing)
    means this caller claims the spawn: it stamps a new marker, hardens the
    directory inside the lock and before spawn so the first daemon I/O lands in
    a ``0o700`` directory, and launches the daemon.

    Args:
        reg (LockedRegistry): The active lock-holding registry session.
        ctx (DaemonContext): Supplies the spawn argv / cwd and the directory.
        startup_deadline_seconds (int): Staleness window for an existing start
            marker; a marker older than this is treated as abandoned.

    Returns:
        bool: ``True`` when this caller spawned the daemon (and is therefore
            responsible for clearing the marker on a startup timeout);
            ``False`` when it deferred to a peer's in-progress spawn.
    """
    marker = reg.read_start_marker()
    now = datetime.now(UTC)
    if marker is not None and (now - marker) < timedelta(
        seconds=startup_deadline_seconds
    ):
        _LOGGER.info(
            f"[_lifecycle:_spawn_or_defer] Spawn already in progress "
            f"(marker {marker.isoformat()}); awaiting peer's daemon"
        )
        return False
    reg.write_start_marker(now)
    harden_private_dir(ctx.directory.path)
    spawn_detached(
        ctx.spawn_argv,
        cwd=ctx.spawn_cwd,
        log_path=ctx.directory.log_path,
    )
    return True


# ---------------------------------------------------------------------------
# Lifecycle orchestration
# ---------------------------------------------------------------------------


async def get_or_start_daemon(
    ctx: DaemonContext,
    *,
    auto_start: bool,
    startup_deadline_seconds: int,
    expected_identity: DaemonBuildIdentity,
    reuse_policy: DaemonReusePolicy,
    output_mode: OutputMode,
    kill_after_seconds: int,
) -> DaemonRegistryEntry:
    """Return a :class:`DaemonRegistryEntry` for a live daemon, spawning one if needed.

    The function is the single entry point all CLI subcommands use
    to obtain connection details.

    Resolution order (a single locked decision point — every branch that might
    mutate the registry runs under the lock, so there is no lock-free fast
    path to re-validate):

    1. Enter :meth:`DaemonDirectory.locked` and read ``daemon.json``. If it
       parses cleanly and
       :meth:`~deephaven_mcp.daemon_registry.DaemonRegistryEntry.is_live`
       confirms the recorded ``(pid, create_time_ns)`` still maps to a running
       process, apply the per-field reuse policy via
       :func:`_decide_for_live_entry`: reuse the daemon and return immediately
       unless the policy resolves to ``restart``, in which case terminate it
       under the lock and fall through to spawn a replacement.
    2. A stale entry (registered PID not running) is deleted under the lock.
    3. When ``auto_start`` is false and no live daemon remains, raise
       :class:`DaemonClientError`.
    4. Otherwise :func:`_spawn_or_defer` either claims the spawn (stamping a
       marker, hardening the directory, and launching the daemon) or defers to
       a peer CLI already mid-spawn. The lock is released and the caller polls
       until the daemon publishes its entry. On a startup timeout a caller that
       spawned clears its own marker so the next attempt is not blocked behind
       a stale flag.

    Args:
        ctx (DaemonContext): The daemon-management context;
            supplies the directory and spawn parameters.
        auto_start (bool): When ``True``, spawn a fresh daemon if no
            live one is registered. When ``False``, the absence of a
            registered daemon is an error.
        startup_deadline_seconds (int): Maximum wall-clock seconds to
            wait for a freshly spawned daemon to publish its registry
            entry before raising :class:`DaemonStartupTimeoutError`.
        expected_identity (DaemonBuildIdentity): This CLI's own build
            identity, compared against a live daemon's recorded identity
            to decide whether reuse is safe.
        reuse_policy (DaemonReusePolicy): Per-field action policy applied
            when the identities differ.
        output_mode (OutputMode): Active output mode, used to render a
            ``warn``-action reuse warning to stderr.
        kill_after_seconds (int): Maximum wall-clock seconds to wait
            after SIGTERM when a ``restart``-action reuse decision terminates
            the incompatible daemon before spawning a replacement.

    Returns:
        DaemonRegistryEntry: The validated registry entry for the
            live daemon. Carries everything callers need to connect
            (``host``, ``port``, ``psk``) plus the operator-facing
            telemetry (``pid``, ``create_time_ns``, ``process_name``,
            ``started_at``, ``config_dir``, ``server_name``).

    Raises:
        DaemonClientError: When no live daemon is found and
            ``auto_start`` is false.
        DaemonReuseRefusedError: When a live daemon's build identity differs
            from ``expected_identity`` and the resolved policy action is
            ``refuse``.
        DaemonStartupTimeoutError: When the spawned daemon does not
            publish a registry entry within
            ``startup_deadline_seconds``.
        RegistryCorruptError: When ``daemon.json`` exists but cannot
            be parsed. Propagated unchanged to the command layer,
            which translates to ``CliError(DAEMON_REGISTRY_CORRUPT)``
            with a recovery hint pointing at ``dh-mcp daemon repair``.
    """
    spawned = False
    with ctx.directory.locked() as reg:
        entry = reg.read()
        if entry is not None and entry.is_live():
            verdict = _decide_for_live_entry(
                entry,
                expected_identity=expected_identity,
                reuse_policy=reuse_policy,
                can_spawn=auto_start,
                output_mode=output_mode,
            )
            if verdict is _ReuseVerdict.REUSE:
                _LOGGER.debug(
                    f"[_lifecycle:get_or_start_daemon] Reusing existing "
                    f"daemon pid={entry.pid} on {entry.host}:{entry.port}"
                )
                return entry
            # _ReuseVerdict.RESTART: terminate the live but incompatible daemon
            # under the lock, then fall through to spawn a replacement.
            _LOGGER.info(
                f"[_lifecycle:get_or_start_daemon] Daemon build mismatch; "
                f"restarting daemon pid={entry.pid}"
            )
            await terminate_identity(
                entry.identity, kill_after_seconds=kill_after_seconds
            )
            reg.delete()
        elif entry is not None:
            _LOGGER.info(
                f"[_lifecycle:get_or_start_daemon] Stale registry "
                f"(pid={entry.pid} not running); removing"
            )
            reg.delete()
        if not auto_start:
            raise DaemonClientError(
                "No daemon is running and auto-start is disabled. "
                "Run `dh-mcp daemon start` or set "
                "`daemon.auto_start: true` in cli.json."
            )
        spawned = _spawn_or_defer(
            reg, ctx, startup_deadline_seconds=startup_deadline_seconds
        )

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
        terminated = await terminate_identity(
            entry.identity, kill_after_seconds=kill_after_seconds
        )
        reg.delete()
        return terminated


# ---------------------------------------------------------------------------
# Termination (PID-reuse-safe SIGTERM/SIGKILL)
# ---------------------------------------------------------------------------


async def terminate_identity(
    identity: ProcessIdentity, *, kill_after_seconds: int
) -> bool:
    """Terminate ``identity`` with SIGTERM, escalating to SIGKILL.

    Lock-free and registry-free: the caller holds the registry lock and is
    responsible for deleting the registry entry afterward. Shared by
    :func:`stop_daemon` and the ``restart``-action path of
    :func:`get_or_start_daemon`.

    The PID-reuse defense lives entirely in :class:`ProcessIdentity`: every
    signal goes through :meth:`ProcessIdentity.send_signal_safely`, which
    re-checks the kernel create-time before delivering, and every poll uses
    :meth:`ProcessIdentity.is_alive`.

    Args:
        identity (ProcessIdentity): The captured ``(pid, create_time_ns)``
            pair of the daemon to terminate.
        kill_after_seconds (int): Maximum wall-clock seconds to wait after
            SIGTERM before escalating to SIGKILL.

    Returns:
        bool: ``True`` when SIGTERM was delivered to a live process (which
            was then waited on and, if needed, SIGKILLed); ``False`` when the
            process was already gone or its PID recycled before signaling.

    Raises:
        DaemonClientError: When the caller lacks permission to signal the
            process.
    """
    sigterm_outcome = identity.send_signal_safely(signal.SIGTERM)
    if sigterm_outcome in (SignalOutcome.GONE, SignalOutcome.RECYCLED):
        # Process is already gone or its PID was recycled between the
        # liveness gate and now. Nothing to terminate.
        return False
    if sigterm_outcome is SignalOutcome.DENIED:
        raise DaemonClientError(
            f"Cannot signal daemon pid={identity.pid}: permission denied. "
            f"The CLI must run as the same user that started the daemon."
        )
    _LOGGER.info(f"[_lifecycle:terminate_identity] Sent SIGTERM to pid={identity.pid}")
    if await _wait_for_exit(identity, kill_after_seconds=kill_after_seconds):
        return True
    _LOGGER.warning(
        f"[_lifecycle:terminate_identity] pid={identity.pid} did not exit after "
        f"{kill_after_seconds}s; escalating to SIGKILL"
    )
    sigkill_outcome = identity.send_signal_safely(signal.SIGKILL)
    if sigkill_outcome is SignalOutcome.DENIED:
        raise DaemonClientError(
            f"Cannot SIGKILL daemon pid={identity.pid}: permission denied."
        )
    # GONE / RECYCLED on SIGKILL means the daemon exited on its own between
    # the wait deadline and the kill syscall — the desired post-condition.
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
