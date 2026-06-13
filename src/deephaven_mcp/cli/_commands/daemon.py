"""``dh-mcp daemon`` noun group: lifecycle of the local daemon.

Verbs: ``start``, ``stop``, ``status``, ``restart``, ``reset``, ``logs``.

All callbacks are async and wrapped with the :func:`run_async` adapter.
Failures raise :class:`CliError` with a stable :class:`ErrorCode`;
the root command's exception handler renders them according to the
active output mode.
"""

from __future__ import annotations

__all__ = ["daemon"]

import asyncio
from pathlib import Path

import click

from deephaven_mcp._pydantic import dump_redacted
from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._commands._acquire import (
    acquire_daemon,
    registry_corrupt_message,
)
from deephaven_mcp.cli._daemon import DaemonClientError, stop_daemon
from deephaven_mcp.cli._errors import CliError, ErrorCode, ExitCode
from deephaven_mcp.cli._format import format_output
from deephaven_mcp.cli._help import (
    HelpfulGroup,
    OutputField,
    OutputSpec,
    build_help,
)
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.daemon_registry import DaemonRegistryEntry, RegistryCorruptError


def _registry_corrupt_message(exc: RegistryCorruptError, *, verb: str) -> str:
    """Format the corrupt-registry message with a verb-specific recovery hint.

    Args:
        exc (RegistryCorruptError): The underlying corruption error.
        verb (str): The daemon verb the operator invoked, threaded into
            the recovery hint (e.g. ``"status"`` -> ``dh-mcp daemon status``).
    """
    return registry_corrupt_message(exc, retry_command=f"dh-mcp daemon {verb}")


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
            :class:`DaemonClientError`, or :class:`RegistryCorruptError`
            with the corresponding stable :class:`ErrorCode`.
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
    """Manage the local dh-mcp daemon.

    The daemon is a per-user background process that hosts the MCP
    systems server; the 'tool' commands connect to it. Use these
    verbs to start, stop, restart, and inspect the daemon, tail its
    log, or quarantine a corrupt registry file. Tool commands
    auto-start the daemon on demand, so an explicit 'daemon start'
    is rarely required.
    """


# Fields emitted by dump_redacted(entry) for the daemon registry entry;
# shared by start, status, and restart (psk is redacted).
_ENTRY_FIELDS = (
    OutputField("pid", "integer", "OS process ID of the daemon."),
    OutputField("host", "string", "Loopback bind address (127.0.0.1)."),
    OutputField(
        "port", "integer", "TCP port the streamable-HTTP transport is bound to."
    ),
    OutputField("server_name", "string", "Configured server identifier."),
    OutputField(
        "config_dir", "string", "Config directory the daemon was started against."
    ),
    OutputField("started_at", "string", "ISO-8601 UTC time the entry was written."),
    OutputField("psk", "string", "Pre-shared key, redacted to the REDACTED sentinel."),
    OutputField(
        "create_time_ns", "integer", "Kernel create-time, paired with pid for liveness."
    ),
    OutputField(
        "process_name", "string", "Expected process-name token used for liveness."
    ),
)


# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------

_OUTPUT_START = OutputSpec(
    "object", _ENTRY_FIELDS, note="The daemon's registry entry (psk redacted)."
)


@daemon.command(
    "start",
    output_spec=_OUTPUT_START,
    help=build_help(
        summary="Start the daemon (idempotent).",
        description=(
            "Spawns the per-user daemon if none is running, then prints its "
            "registry entry. Re-running against an already-running daemon "
            "prints the existing entry without spawning a second process. "
            "Tool commands auto-start the daemon, so explicit start is only "
            "needed to pre-warm it or inspect the connection details."
        ),
        output=_OUTPUT_START,
        examples=(
            "$ dh-mcp daemon start",
            "$ dh-mcp -o json daemon start | jq .port",
        ),
        see_also=("dh-mcp daemon status", "dh-mcp daemon stop"),
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
async def daemon_start(runtime: Runtime) -> None:
    """Start the daemon (idempotent)."""
    entry = await _acquire_daemon(runtime, verb="start")
    # ``dump_redacted`` emits the full registry-entry schema with
    # the PSK replaced by the project ``REDACTED`` sentinel
    # (:class:`RedactableSchema`). Operators see every field the
    # daemon advertised, mirroring ``daemon status`` and matching the
    # project-wide redaction convention used by ``config show`` and
    # the systems-server enterprise tool.
    payload = dump_redacted(entry)
    click.echo(format_output(payload, output=runtime.config.cli.output.format))


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------

_OUTPUT_STOP = OutputSpec(
    "object",
    (
        OutputField("stopped", "boolean", "True if a daemon was terminated."),
        OutputField("message", "string", "Human-readable summary."),
    ),
)


@daemon.command(
    "stop",
    output_spec=_OUTPUT_STOP,
    help=build_help(
        summary="Stop the daemon (idempotent).",
        description=(
            "Sends SIGTERM (escalating to SIGKILL) to the registered daemon "
            "and removes the registry file. Succeeds even when no daemon was "
            "running."
        ),
        output=_OUTPUT_STOP,
        examples=("$ dh-mcp daemon stop",),
        see_also=("dh-mcp daemon start", "dh-mcp daemon status"),
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
    click.echo(
        format_output(
            {"stopped": terminated, "message": msg},
            output=runtime.config.cli.output.format,
        )
    )


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

_OUTPUT_STATUS = OutputSpec(
    "object",
    (
        OutputField("running", "boolean", "Whether a live daemon is registered."),
        OutputField("stale_pid", "integer", "PID of a dead entry that was cleaned up."),
        *_ENTRY_FIELDS,
    ),
    note="When running is true, the registry-entry fields are also present.",
)


@daemon.command(
    "status",
    output_spec=_OUTPUT_STATUS,
    help=build_help(
        summary="Report daemon status.",
        description=(
            "Reads the daemon registry and reports whether a live daemon "
            "process is registered. Exits 0 regardless: a missing or stale "
            "registry is reported as 'running: false' so callers branch on "
            "that field without parsing exit codes. A stale entry (dead pid) "
            "is cleaned up and its pid reported as stale_pid."
        ),
        output=_OUTPUT_STATUS,
        examples=(
            "$ dh-mcp daemon status",
            "$ dh-mcp -o json daemon status | jq .running",
        ),
        see_also=("dh-mcp daemon start", "dh-mcp daemon logs"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(ErrorCode.DAEMON_REGISTRY_CORRUPT,),
    ),
)
@click.pass_obj
@run_async
async def daemon_status(runtime: Runtime) -> None:
    """Report daemon status."""
    output = runtime.config.cli.output.format
    try:
        entry = runtime.daemon_dir.read_entry()
    except RegistryCorruptError as exc:
        # Surface as a structured error rather than the misleading
        # "running: false" we would otherwise emit if we treated a
        # corrupt file as absent. The operator needs to know the
        # registry is bad — automatic recovery for ``daemon status``
        # would silently hide the diagnostic.
        raise CliError(
            _registry_corrupt_message(exc, verb="status"),
            code=ErrorCode.DAEMON_REGISTRY_CORRUPT,
        ) from exc
    if entry is None:
        click.echo(format_output({"running": False}, output=output))
        return
    if not entry.is_live():
        # Re-read inside the lock before deleting so we do not blast
        # a fresh entry that a peer published between the lock-free
        # read above and the delete.
        with runtime.daemon_dir.locked() as reg:
            entry = reg.read()
            if entry is not None and not entry.is_live():
                reg.delete()
                stale_pid = entry.pid
            else:
                stale_pid = None
        if stale_pid is not None:
            click.echo(
                format_output({"running": False, "stale_pid": stale_pid}, output=output)
            )
            return
        # The entry vanished or became live during the re-read; fall
        # through and re-evaluate the entry we now hold.
        if entry is None:
            click.echo(format_output({"running": False}, output=output))
            return

    # Dump the entry through the project-canonical redact pipeline
    # so ``datetime`` and ``Path`` round-trip to JSON-safe values
    # automatically. The ``redact`` context substitutes the
    # ``REDACTED`` sentinel for the PSK; the field is kept (rather
    # than popped) to preserve schema honesty for structured-output
    # consumers.
    payload = dump_redacted(entry)
    payload["running"] = True
    click.echo(format_output(payload, output=output))


# ---------------------------------------------------------------------------
# restart
# ---------------------------------------------------------------------------

_OUTPUT_RESTART = OutputSpec(
    "object",
    (OutputField("restarted", "boolean", "Always true on success."), *_ENTRY_FIELDS),
    note="The new daemon's registry entry (psk redacted).",
)


@daemon.command(
    "restart",
    output_spec=_OUTPUT_RESTART,
    help=build_help(
        summary="Restart the daemon: stop (if running) then start.",
        description=(
            "Equivalent to 'dh-mcp daemon stop' followed by 'dh-mcp daemon "
            "start', but single-command, and reports the new daemon's "
            "registry entry on success."
        ),
        output=_OUTPUT_RESTART,
        examples=(
            "$ dh-mcp daemon restart",
            "$ dh-mcp -o json daemon restart | jq .pid",
        ),
        see_also=("dh-mcp daemon start", "dh-mcp daemon stop"),
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
    # See ``daemon_start`` for the redaction rationale.
    payload = dump_redacted(entry)
    payload["restarted"] = True
    click.echo(format_output(payload, output=runtime.config.cli.output.format))


# ---------------------------------------------------------------------------
# reset
# ---------------------------------------------------------------------------

_OUTPUT_RESET = OutputSpec(
    "object",
    (
        OutputField("reset", "boolean", "True if a registry file was quarantined."),
        OutputField(
            "quarantined_to",
            "string",
            "Path of the quarantined file (when reset is true).",
        ),
        OutputField(
            "message", "string", "Human-readable summary (when reset is false)."
        ),
    ),
)


@daemon.command(
    "reset",
    output_spec=_OUTPUT_RESET,
    help=build_help(
        summary="Quarantine the daemon registry file and exit.",
        description=(
            "Renames daemon.json to a timestamped daemon.json.corrupt-* "
            "sibling so the well-known path is free for a fresh 'dh-mcp "
            "daemon start'. The malformed bytes (if any) are preserved on "
            "disk for operator postmortem.\n\n"
            "Intended as the explicit recovery verb when status, stop, "
            "start, or restart report the daemon_registry_corrupt error. "
            "Refuses to run while a live daemon is still registered so the "
            "operator cannot accidentally orphan a running process \u2014 run "
            "'dh-mcp daemon stop' first in that case."
        ),
        output=_OUTPUT_RESET,
        examples=(
            "$ dh-mcp daemon reset",
            "$ dh-mcp -o json daemon reset | jq .quarantined_to",
        ),
        see_also=("dh-mcp daemon status", "dh-mcp daemon stop"),
        exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR),
        error_codes=(ErrorCode.DAEMON_REGISTRY_LIVE,),
    ),
)
@click.pass_obj
@run_async
async def daemon_reset(runtime: Runtime) -> None:
    """Quarantine the daemon registry file and exit."""
    output = runtime.config.cli.output.format
    registry_path = runtime.daemon_dir.registry_path

    # Hold the registry lock for the entire decision-and-mutate
    # window: a peer daemon publishing between the liveness check
    # and the rename would otherwise see its entry quarantined.
    with runtime.daemon_dir.locked() as reg:
        if not registry_path.exists():
            click.echo(
                format_output(
                    {"reset": False, "message": "No registry to reset."},
                    output=output,
                )
            )
            return

        # Parse-or-not-parse is irrelevant for the *action* (we
        # quarantine either way), but a parseable registry pointing
        # at a live daemon is the one case we refuse: quarantining
        # out from under a running daemon would orphan the process
        # from the CLI's perspective. A corrupt registry cannot be
        # liveness-checked, so it is always safe to quarantine.
        try:
            entry = reg.read()
        except RegistryCorruptError:
            entry = None
        if entry is not None and entry.is_live():
            raise CliError(
                f"Refusing to reset registry while daemon pid={entry.pid} is "
                f"live on {entry.host}:{entry.port}. Run `dh-mcp daemon stop` "
                f"first.",
                code=ErrorCode.DAEMON_REGISTRY_LIVE,
            )

        quarantined = reg.quarantine()

    if quarantined is None:
        # Race tolerated: an external process (outside the lock
        # protocol) removed the registry between ``exists()`` and
        # the rename. Report as no-op.
        click.echo(
            format_output(
                {"reset": False, "message": "No registry to reset."},
                output=output,
            )
        )
        return
    click.echo(
        format_output(
            {"reset": True, "quarantined_to": str(quarantined)},
            output=output,
        )
    )


# ---------------------------------------------------------------------------
# logs
# ---------------------------------------------------------------------------

_OUTPUT_LOGS = OutputSpec(
    "text", note="Raw lines from daemon.log; not structured even under -o json."
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
    output_spec=_OUTPUT_LOGS,
    help=build_help(
        summary="Tail the daemon log file.",
        description=(
            "Without -f, prints the last --lines lines and exits. With -f, "
            "follows the file until interrupted (Ctrl-C). Output is raw log "
            "text in every output mode."
        ),
        output=_OUTPUT_LOGS,
        examples=(
            "$ dh-mcp daemon logs",
            "$ dh-mcp daemon logs -n 500",
            "$ dh-mcp daemon logs -f",
        ),
        see_also=("dh-mcp daemon status",),
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
@click.pass_obj
@run_async
async def daemon_logs(runtime: Runtime, follow: bool, lines: int) -> None:
    """Tail the daemon log file."""
    path = runtime.daemon_dir.log_path
    if not path.exists():
        raise CliError(
            f"No daemon log at {path}. Has the daemon been started?",
            code=ErrorCode.DAEMON_NOT_RUNNING,
        )
    try:
        await _tail_and_follow(path, lines=lines, follow=follow)
    except (KeyboardInterrupt, asyncio.CancelledError):  # pragma: no cover
        return
