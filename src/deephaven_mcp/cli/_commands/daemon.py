"""``dhcli daemon`` noun group: lifecycle of the local daemon.

Verbs: ``start``, ``stop``, ``status``, ``restart``, ``repair``, ``logs``.

All callbacks are async and wrapped with the :func:`run_async` adapter.
Failures raise :class:`CliError` with a stable :class:`ErrorCode`;
the root command's exception handler renders them according to the
active output mode.
"""

from __future__ import annotations

__all__ = ["daemon"]

import asyncio
from enum import StrEnum
from pathlib import Path

import click

from deephaven_mcp._pydantic import dump_redacted
from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._command import HelpfulGroup
from deephaven_mcp.cli._commands._acquire import (
    acquire_daemon,
    registry_corrupt_message,
)
from deephaven_mcp.cli._daemon import DaemonClientError, stop_daemon
from deephaven_mcp.cli._echo import echo_payload
from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode
from deephaven_mcp.cli._help import (
    HelpSpec,
    OutputField,
    OutputSpec,
)
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.daemon_registry import DaemonRegistryEntry, RegistryCorruptError


def _registry_corrupt_message(exc: RegistryCorruptError, *, verb: str) -> str:
    """Format the corrupt-registry message with a verb-specific recovery hint.

    Args:
        exc (RegistryCorruptError): The underlying corruption error.
        verb (str): The daemon verb the operator invoked, threaded into
            the recovery hint (e.g. ``"status"`` -> ``dhcli daemon status``).
    """
    return registry_corrupt_message(exc, retry_command=f"dhcli daemon {verb}")


async def _acquire_daemon(runtime: Runtime, *, verb: str) -> DaemonRegistryEntry:
    """Call :func:`get_or_start_daemon` with ``auto_start=True`` and remap errors.

    Args:
        runtime (Runtime): The active CLI runtime.
        verb (str): The daemon verb the operator invoked
            (``"start"`` or ``"restart"``). Used only in the
            corrupt-registry recovery hint.

    Returns:
        DaemonRegistryEntry: The validated registry entry for the
            running daemon.

    Raises:
        CliError: Wrapping :class:`DaemonStartupTimeoutError`,
            :class:`DaemonClientError`, :class:`DaemonReuseRefusedError`, or
            :class:`RegistryCorruptError` with the corresponding stable
            :class:`ErrorCode`.
    """
    return await acquire_daemon(
        runtime,
        auto_start=True,
        client_error_code=ErrorCode.DAEMON_CLIENT_ERROR,
        on_registry_corrupt=lambda exc: CliError(
            _registry_corrupt_message(exc, verb=verb),
            code=ErrorCode.DAEMON_REGISTRY_CORRUPT,
        ),
    )


async def _release_daemon(runtime: Runtime, *, verb: str) -> bool:
    """Call :func:`stop_daemon` and remap errors to :class:`CliError`.

    Args:
        runtime (Runtime): The active CLI runtime.
        verb (str): The daemon verb the operator invoked
            (``"stop"`` or ``"restart"``). Used only in the
            corrupt-registry recovery hint.

    Returns:
        bool: ``True`` when a daemon was found and terminated;
            ``False`` when no live daemon was registered.

    Raises:
        CliError: Wrapping :class:`DaemonClientError` or
            :class:`RegistryCorruptError` with the corresponding
            stable :class:`ErrorCode`.
    """
    try:
        return await stop_daemon(
            runtime.daemon_dir,
            kill_after_seconds=runtime.config.cli.daemon.timeouts.kill_after_seconds,
        )
    except DaemonClientError as exc:
        raise CliError(str(exc), code=ErrorCode.DAEMON_CLIENT_ERROR) from exc
    except RegistryCorruptError as exc:
        # The registry is unreadable, so we cannot identify the
        # running daemon to signal it. Surface as an actionable
        # error rather than silently no-op'ing — there may be a
        # daemon still bound to the loopback port that the operator
        # now needs to clean up manually.
        raise CliError(
            _registry_corrupt_message(exc, verb=verb),
            code=ErrorCode.DAEMON_REGISTRY_CORRUPT,
        ) from exc


@click.group(cls=HelpfulGroup)
def daemon() -> None:
    """Manage the local dhcli daemon.

    The daemon is a per-user background process that hosts the MCP
    systems server; the runtime commands ('session', 'system',
    'table', 'catalog', 'pq', and 'tool') connect to it. Use these
    verbs to start, stop, restart, and inspect the daemon, tail its
    log, or repair a corrupt registry file. Runtime commands
    auto-start the daemon on demand, so an explicit 'daemon start'
    is rarely required.
    """


class DaemonState(StrEnum):
    """The state reported by ``daemon status``, ``start``, and ``restart``."""

    RUNNING = "running"
    """A live daemon process is registered."""
    STOPPED = "stopped"
    """No daemon is registered (never started, or cleanly stopped)."""
    CRASHED = "crashed"
    """A registry entry exists but its process is dead (did not exit cleanly)."""


def _paths_payload(runtime: Runtime) -> dict[str, str]:
    """Return the stable filesystem locations the CLI always knows.

    Args:
        runtime (Runtime): The active CLI runtime.

    Returns:
        dict[str, str]: The ``config``, ``runtime``, ``registry``, and ``log``
            absolute paths, present regardless of whether a daemon is running.
    """
    return {
        "config": str(runtime.config_dir),
        "runtime": str(runtime.runtime_dir),
        "registry": str(runtime.daemon_dir.registry_path),
        "log": str(runtime.daemon_dir.log_path),
    }


def _running_message(entry: DaemonRegistryEntry) -> str:
    """Return the one-line human summary for a running daemon.

    Args:
        entry (DaemonRegistryEntry): The registry entry of the running daemon.

    Returns:
        str: A summary naming the daemon's pid and loopback endpoint.
    """
    return f"Daemon running: pid {entry.pid} at {entry.host}:{entry.port}."


def _report_envelope(
    runtime: Runtime,
    *,
    state: DaemonState,
    message: str,
    entry: DaemonRegistryEntry | None = None,
) -> dict[str, object]:
    """Build the ``{state, message, daemon?, paths}`` daemon-report envelope.

    Args:
        runtime (Runtime): The active CLI runtime.
        state (DaemonState): The reported state.
        message (str): The human-readable one-line summary.
        entry (DaemonRegistryEntry | None): The running daemon's registry entry.
            When given, a ``daemon`` object is included; when ``None`` (stopped
            or crashed) the ``daemon`` key is omitted.

    Returns:
        dict[str, object]: Keys in most- to least-important order: ``state``,
            ``message``, ``daemon`` (only when ``entry`` is given), ``paths``.
            The ``daemon`` value is the registry entry itself, redacted — so
            ``status`` and ``daemon.json`` never drift and an operator can
            cross-read them.
    """
    payload: dict[str, object] = {"state": state.value, "message": message}
    if entry is not None:
        # The daemon view is the registry entry verbatim, with the psk masked
        # to the project REDACTED sentinel by dump_redacted.
        payload["daemon"] = dump_redacted(entry)
    payload["paths"] = _paths_payload(runtime)
    return payload


def _running_payload(runtime: Runtime, entry: DaemonRegistryEntry) -> dict[str, object]:
    """Return the running-daemon envelope shared by start, status, and restart.

    Args:
        runtime (Runtime): The active CLI runtime.
        entry (DaemonRegistryEntry): The registry entry of the running daemon.

    Returns:
        dict[str, object]: The ``{state, message, daemon, paths}`` envelope with
            ``state`` ``"running"``.
    """
    return _report_envelope(
        runtime,
        state=DaemonState.RUNNING,
        message=_running_message(entry),
        entry=entry,
    )


# Output fields shared by the daemon-reporting commands (start, status, restart).
_DAEMON_FIELD = OutputField(
    "daemon",
    "object",
    "The live daemon instance (the redacted registry entry), present only when "
    "state is 'running' (omitted otherwise): pid, create_time_ns, process_name, "
    "host, port, psk (redacted), started_at, config_dir, server_name, and "
    "build_identity (an object with version, venv, fingerprint).",
)
_PATHS_FIELD = OutputField(
    "paths",
    "object",
    "Stable filesystem locations the CLI always knows, present in every state: "
    "config, runtime, registry, log.",
)
_MESSAGE_FIELD = OutputField("message", "string", "Human-readable one-line summary.")
# state/message/daemon/paths envelope for start and restart (state always 'running').
_RUNNING_FIELDS = (
    OutputField("state", "string", "Always 'running' on success."),
    _MESSAGE_FIELD,
    _DAEMON_FIELD,
    _PATHS_FIELD,
)


# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------

_OUTPUT_START = OutputSpec(
    "object",
    _RUNNING_FIELDS,
    note="The running daemon (psk redacted); 'paths' is always present.",
)


@daemon.command(
    "start",
    help_spec=HelpSpec(
        summary="Start the daemon (idempotent).",
        description=(
            "Spawns the per-user daemon if none is running, then prints its "
            "state and connection details (host, port, pid). Re-running against "
            "an already-running daemon prints the existing details without "
            "spawning a second process. Tool commands auto-start the daemon, so "
            "explicit start is only needed to pre-warm it or inspect the "
            "connection details."
        ),
        output=_OUTPUT_START,
        examples=(
            "$ dhcli daemon start",
            "$ dhcli -o json daemon start | jq .daemon.port",
        ),
        see_also=("dhcli daemon status", "dhcli daemon stop"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.DAEMON_STARTUP_TIMEOUT,
            ErrorCode.DAEMON_CLIENT_ERROR,
            ErrorCode.DAEMON_REGISTRY_CORRUPT,
            ErrorCode.DAEMON_REUSE_REFUSED,
        ),
    ),
)
@click.pass_obj
@run_async
async def daemon_start(runtime: Runtime) -> None:
    """Start the daemon (idempotent)."""
    entry = await _acquire_daemon(runtime, verb="start")
    echo_payload(runtime, _running_payload(runtime, entry), sort_keys=False)


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------

_OUTPUT_STOP = OutputSpec(
    "object",
    (
        OutputField("stopped", "boolean", "True if a daemon was terminated."),
        _MESSAGE_FIELD,
    ),
)


@daemon.command(
    "stop",
    help_spec=HelpSpec(
        summary="Stop the daemon (idempotent).",
        description=(
            "Sends SIGTERM (escalating to SIGKILL) to the registered daemon "
            "and removes the registry file. Succeeds even when no daemon was "
            "running."
        ),
        output=_OUTPUT_STOP,
        examples=(
            "$ dhcli daemon stop",
            "$ dhcli daemon stop | jq -r .status",
        ),
        see_also=("dhcli daemon start", "dhcli daemon status"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.DAEMON_CLIENT_ERROR,
            ErrorCode.DAEMON_REGISTRY_CORRUPT,
        ),
    ),
)
@click.pass_obj
@run_async
async def daemon_stop(runtime: Runtime) -> None:
    """Stop the daemon (idempotent)."""
    terminated = await _release_daemon(runtime, verb="stop")
    msg = "Daemon stopped." if terminated else "No daemon was running."
    echo_payload(runtime, {"stopped": terminated, "message": msg}, sort_keys=False)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

_OUTPUT_STATUS = OutputSpec(
    "object",
    (
        OutputField(
            "state", "string", "Daemon state: 'running', 'stopped', or 'crashed'."
        ),
        _MESSAGE_FIELD,
        _DAEMON_FIELD,
        _PATHS_FIELD,
    ),
    note=(
        "'daemon' is present only when state is 'running' (omitted otherwise); "
        "'paths' is always present."
    ),
)


@daemon.command(
    "status",
    help_spec=HelpSpec(
        summary="Report daemon status.",
        description=(
            "Reads the daemon registry and reports the daemon's state: "
            "'running' (a live process is registered), 'stopped' (none "
            "registered), or 'crashed' (a registry entry exists but its process "
            "is dead). Exits 0 in all three cases so callers branch on the "
            "'state' field without parsing exit codes. This command is "
            "read-only: a 'crashed' entry is reported but left in place — run "
            "'dhcli daemon start' or 'dhcli daemon repair' to clean it up."
        ),
        output=_OUTPUT_STATUS,
        examples=(
            "$ dhcli daemon status",
            "$ dhcli -o json daemon status | jq .state",
        ),
        see_also=("dhcli daemon start", "dhcli daemon logs"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(ErrorCode.DAEMON_REGISTRY_CORRUPT,),
    ),
)
@click.pass_obj
@run_async
async def daemon_status(runtime: Runtime) -> None:
    """Report daemon status."""
    try:
        entry = runtime.daemon_dir.read_entry()
    except RegistryCorruptError as exc:
        # Surface as a structured error rather than the misleading
        # "stopped" we would otherwise emit if we treated a corrupt
        # file as absent. The operator needs to know the registry is
        # bad — automatic recovery for ``daemon status`` would silently
        # hide the diagnostic.
        raise CliError(
            _registry_corrupt_message(exc, verb="status"),
            code=ErrorCode.DAEMON_REGISTRY_CORRUPT,
        ) from exc
    if entry is None:
        payload = _report_envelope(
            runtime, state=DaemonState.STOPPED, message="No daemon is running."
        )
    elif entry.is_live():
        payload = _running_payload(runtime, entry)
    else:
        # Read-only: report the dead entry but do not delete or quarantine
        # it. Cleanup is the job of ``daemon start`` (which auto-handles a
        # stale entry) and ``daemon repair``.
        payload = _report_envelope(
            runtime,
            state=DaemonState.CRASHED,
            message=(
                f"Daemon not running: a previous instance (pid {entry.pid}) "
                f"exited without cleanup. Run 'dhcli daemon start' to clean up "
                f"and relaunch."
            ),
        )
    echo_payload(runtime, payload, sort_keys=False)


# ---------------------------------------------------------------------------
# restart
# ---------------------------------------------------------------------------

_OUTPUT_RESTART = OutputSpec(
    "object",
    _RUNNING_FIELDS,
    note="The restarted daemon (psk redacted); 'paths' is always present.",
)


@daemon.command(
    "restart",
    help_spec=HelpSpec(
        summary="Restart the daemon: stop (if running) then start.",
        description=(
            "Equivalent to 'dhcli daemon stop' followed by 'dhcli daemon "
            "start', but single-command, and reports the new daemon's state "
            "and connection details on success."
        ),
        output=_OUTPUT_RESTART,
        examples=(
            "$ dhcli daemon restart",
            "$ dhcli -o json daemon restart | jq .daemon.pid",
        ),
        see_also=("dhcli daemon start", "dhcli daemon stop"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(
            ErrorCode.DAEMON_STARTUP_TIMEOUT,
            ErrorCode.DAEMON_CLIENT_ERROR,
            ErrorCode.DAEMON_REGISTRY_CORRUPT,
        ),
    ),
)
@click.pass_obj
@run_async
async def daemon_restart(runtime: Runtime) -> None:
    """Stop (if running) and start the daemon."""
    await _release_daemon(runtime, verb="restart")
    entry = await _acquire_daemon(runtime, verb="restart")
    echo_payload(runtime, _running_payload(runtime, entry), sort_keys=False)


# ---------------------------------------------------------------------------
# repair
# ---------------------------------------------------------------------------

_OUTPUT_REPAIR = OutputSpec(
    "object",
    (
        OutputField(
            "repaired",
            "boolean",
            "True if a corrupt registry file was moved aside.",
        ),
        OutputField(
            "quarantined_to",
            "string",
            "Path of the moved-aside file (when repaired is true).",
        ),
        OutputField(
            "message",
            "string",
            "Human-readable summary (when repaired is false).",
        ),
    ),
)


@daemon.command(
    "repair",
    help_spec=HelpSpec(
        summary="Recover from a corrupt daemon registry file.",
        description=(
            "Use this when 'dhcli daemon status' (or start, stop, restart) "
            "reports the daemon_registry_corrupt error. It moves the "
            "unreadable daemon.json aside to a timestamped "
            "daemon.json.corrupt-<ts> sibling so the next 'dhcli daemon "
            "start' can write a clean one. The corrupt bytes are preserved "
            "on disk for postmortem.\n\n"
            "Refuses to run while a live daemon is still registered, so you "
            "cannot accidentally orphan a running process. Run 'dhcli "
            "daemon stop' first in that case."
        ),
        output=_OUTPUT_REPAIR,
        examples=(
            "$ dhcli daemon repair",
            "$ dhcli -o json daemon repair | jq .quarantined_to",
        ),
        see_also=("dhcli daemon status", "dhcli daemon stop"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(ErrorCode.DAEMON_REGISTRY_LIVE,),
    ),
)
@click.pass_obj
@run_async
async def daemon_repair(runtime: Runtime) -> None:
    """Recover from a corrupt daemon registry file."""
    registry_path = runtime.daemon_dir.registry_path

    # Hold the registry lock for the entire decision-and-mutate
    # window: a peer daemon publishing between the liveness check
    # and the rename would otherwise see its entry moved aside.
    with runtime.daemon_dir.locked() as reg:
        if not registry_path.exists():
            echo_payload(
                runtime,
                {"repaired": False, "message": "No registry to repair."},
                sort_keys=False,
            )
            return

        # Parse-or-not-parse is irrelevant for the *action* (we
        # move the file aside either way), but a parseable registry
        # pointing at a live daemon is the one case we refuse:
        # moving it out from under a running daemon would orphan
        # the process from the CLI's perspective. A corrupt registry
        # cannot be liveness-checked, so it is always safe to repair.
        try:
            entry = reg.read()
        except RegistryCorruptError:
            entry = None
        if entry is not None and entry.is_live():
            raise CliError(
                f"Refusing to repair registry while daemon pid={entry.pid} is "
                f"live on {entry.host}:{entry.port}. Run `dhcli daemon stop` "
                f"first.",
                code=ErrorCode.DAEMON_REGISTRY_LIVE,
            )

        quarantined = reg.quarantine()

    if quarantined is None:
        # Race tolerated: an external process (outside the lock
        # protocol) removed the registry between ``exists()`` and
        # the rename. Report as no-op.
        echo_payload(
            runtime,
            {"repaired": False, "message": "No registry to repair."},
            sort_keys=False,
        )
        return
    echo_payload(
        runtime,
        {"repaired": True, "quarantined_to": str(quarantined)},
        sort_keys=False,
    )


# ---------------------------------------------------------------------------
# logs
# ---------------------------------------------------------------------------

_OUTPUT_LOGS = OutputSpec(
    "text",
    note=(
        "Raw lines from daemon.log; not structured even under -o json. With "
        "--path, prints only the absolute log-file path on a single line."
    ),
)


def _tail(text: str, n: int) -> str:
    """Return the verbatim last ``n`` lines of ``text`` (all of it when n <= 0).

    Slices on line boundaries with newlines preserved so the result is a
    byte-faithful suffix of the file: no newline is added or dropped. The
    whole file is materialized because daemon logs are bounded to a few MB
    in practice; a tail-from-end block reader is a future optimization.
    """
    if n <= 0:
        return text
    return "".join(text.splitlines(keepends=True)[-n:])


async def _tail_and_follow(
    path: Path, *, lines: int, follow: bool, poll_interval: float = 0.25
) -> None:
    """Print the last ``lines`` lines verbatim, then optionally follow appends.

    A single open handle bridges the tail and the follow: the file
    position after the initial read is the exact byte offset the follow
    resumes from, so lines appended during the handoff are neither
    dropped nor duplicated. Output is the file's bytes verbatim (echoed
    with nl=False), so a record written without its trailing newline and
    completed by a later write renders as one line, not two. Timestamps
    and tracebacks therefore reach humans and grep exactly as logged.
    """
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        click.echo(_tail(fh.read(), lines), nl=False)  # fh now sits at EOF.
        if not follow:
            return
        while True:
            chunk = fh.readline()
            if chunk:
                click.echo(chunk, nl=False)
            else:
                await asyncio.sleep(poll_interval)


@daemon.command(
    "logs",
    help_spec=HelpSpec(
        summary="Tail the daemon log file.",
        description=(
            "Without -f, prints the last --lines lines and exits. With -f, "
            "follows the file until interrupted (Ctrl-C). Output is raw log "
            "text in every output mode. With --path, prints the absolute path "
            "to the log file and exits without reading it (works even if the "
            "daemon has never started)."
        ),
        output=_OUTPUT_LOGS,
        examples=(
            "$ dhcli daemon logs",
            "$ dhcli daemon logs -n 500",
            "$ dhcli daemon logs -f",
            "$ dhcli daemon logs --path",
        ),
        see_also=("dhcli daemon status",),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(ErrorCode.DAEMON_NOT_RUNNING,),
    ),
)
@click.option(
    "-f",
    "--follow",
    is_flag=True,
    default=False,
    help="Follow the log file (like 'tail -f').",
)
@click.option(
    "-n",
    "--lines",
    type=int,
    default=100,
    show_default=True,
    help="Number of trailing lines to print before following.",
)
@click.option(
    "--path",
    "show_path",
    is_flag=True,
    default=False,
    help="Print the absolute path to the log file and exit.",
)
@click.pass_obj
@run_async
async def daemon_logs(
    runtime: Runtime, follow: bool, lines: int, show_path: bool
) -> None:
    """Tail the daemon log file."""
    log_path = runtime.daemon_dir.log_path
    if show_path:
        # Discover-the-path mode: report the location without touching the
        # file, so it works before the daemon has ever started.
        click.echo(str(log_path))
        return
    if not log_path.exists():
        raise CliError(
            f"No daemon log at {log_path}. Has the daemon been started?",
            code=ErrorCode.DAEMON_NOT_RUNNING,
        )
    try:
        await _tail_and_follow(log_path, lines=lines, follow=follow)
    except (KeyboardInterrupt, asyncio.CancelledError):  # pragma: no cover
        return
