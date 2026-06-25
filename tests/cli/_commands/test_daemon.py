"""Tests for ``deephaven_mcp.cli._commands.daemon``."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner

from deephaven_mcp.cli import _main
from deephaven_mcp.cli._commands import _acquire as acquire_mod
from deephaven_mcp.cli._commands import daemon as daemon_mod
from deephaven_mcp.cli._daemon import (
    DaemonClientError,
    DaemonStartupTimeoutError,
)
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.daemon_registry import (
    DaemonBuildIdentity,
    DaemonRegistryEntry,
    RegistryCorruptError,
)

from .._helpers import (
    fake_load_runtime,
    make_entry,
    make_runtime,
)


def _invoke(args: list[str], runtime: Runtime):
    runner = CliRunner()
    with patch.object(_main, "load_runtime", fake_load_runtime(runtime)):
        return runner.invoke(cli, args)


def _expected_paths(rt: Runtime, tmp_path: Path) -> dict[str, str]:
    """Point the mock daemon_dir at known paths; return the ``paths`` block.

    ``daemon status`` always emits ``config``, ``runtime``, ``registry``, and
    ``log`` under ``paths``. ``config``/``runtime`` are real on the Runtime
    built by ``make_runtime``; the registry/log paths come from the (mocked)
    DaemonDirectory, so set them to deterministic values here.
    """
    registry = tmp_path / "rt" / "daemon" / "daemon.json"
    log = tmp_path / "rt" / "daemon" / "daemon.log"
    rt.daemon_dir.registry_path = registry  # type: ignore[union-attr]
    rt.daemon_dir.log_path = log  # type: ignore[union-attr]
    return {
        "config": str(rt.config_dir),
        "runtime": str(rt.runtime_dir),
        "registry": str(registry),
        "log": str(log),
    }


# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------


def test_start_prints_handle(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    paths = _expected_paths(rt, tmp_path)
    with patch.object(
        acquire_mod, "get_or_start_daemon", AsyncMock(return_value=make_entry())
    ):
        result = _invoke(["-o", "json", "daemon", "start"], rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["state"] == "running"
    assert payload["daemon"]["pid"] == 1
    assert payload["daemon"]["port"] == 9999
    assert payload["paths"] == paths


def test_start_handles_timeout(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        acquire_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=DaemonStartupTimeoutError("timeout")),
    ):
        result = _invoke(["daemon", "start"], rt)
    assert result.exit_code == 2


def test_start_handles_client_error(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        acquire_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=DaemonClientError("nope")),
    ):
        result = _invoke(["daemon", "start"], rt)
    assert result.exit_code == 2


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------


def test_stop_terminated(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(daemon_mod, "stop_daemon", AsyncMock(return_value=True)):
        result = _invoke(["daemon", "stop"], rt)
    assert result.exit_code == 0
    assert "stopped" in result.output.lower()


def test_stop_no_daemon(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(daemon_mod, "stop_daemon", AsyncMock(return_value=False)):
        result = _invoke(["daemon", "stop"], rt)
    assert result.exit_code == 0


def test_stop_client_error_returns_2(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        daemon_mod, "stop_daemon", AsyncMock(side_effect=DaemonClientError("nope"))
    ):
        result = _invoke(["daemon", "stop"], rt)
    assert result.exit_code == 2


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def test_status_stopped(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    rt.daemon_dir.read_entry.return_value = None  # type: ignore[union-attr]
    paths = _expected_paths(rt, tmp_path)
    result = _invoke(["-o", "json", "daemon", "status"], rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == {
        "state": "stopped",
        "message": "No daemon is running.",
        "paths": paths,
    }
    # Keys are emitted in most- to least-important order (sort_keys=False),
    # not alphabetically — guards the command's sort_keys=False wiring, which
    # the order-insensitive json.loads comparison above cannot catch.
    assert (
        result.output.index('"state"')
        < result.output.index('"message"')
        < result.output.index('"paths"')
    )


def test_status_crashed_reports_without_cleanup(tmp_path: Path) -> None:
    """A dead registry entry reports state 'crashed' and is left in place.

    ``status`` is read-only: it must not delete or quarantine the stale entry
    (cleanup is the job of ``daemon start`` / ``daemon repair``), it must not
    acquire the registry lock, and the dead pid appears in the message.
    """
    rt = make_runtime(tmp_path)
    entry = make_entry()
    rt.daemon_dir.read_entry.return_value = entry  # type: ignore[union-attr]
    paths = _expected_paths(rt, tmp_path)
    with patch.object(DaemonRegistryEntry, "is_live", return_value=False):
        result = _invoke(["-o", "json", "daemon", "status"], rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["state"] == "crashed"
    assert "daemon" not in payload
    assert payload["paths"] == paths
    assert str(entry.pid) in payload["message"]
    # Read-only: no lock acquired, nothing deleted or quarantined.
    rt.daemon_dir.locked.assert_not_called()  # type: ignore[union-attr]


def test_status_running_returns_daemon_fields(tmp_path: Path) -> None:
    """A live registered daemon surfaces the full registry entry, redacted.

    The ``daemon`` object is the entry the daemon wrote to ``daemon.json`` with
    no curation — every field is present (so ``status`` and the registry never
    drift) — except :attr:`DaemonRegistryEntry.psk`, which is replaced with the
    project ``REDACTED`` sentinel.
    """
    from deephaven_mcp._redaction import REDACTED

    rt = make_runtime(tmp_path)
    entry = make_entry()
    rt.daemon_dir.read_entry.return_value = entry  # type: ignore[union-attr]
    paths = _expected_paths(rt, tmp_path)
    with patch.object(DaemonRegistryEntry, "is_live", return_value=True):
        result = _invoke(["-o", "json", "daemon", "status"], rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == {
        "state": "running",
        "message": (f"Daemon running: pid {entry.pid} at {entry.host}:{entry.port}."),
        "daemon": {
            "pid": entry.pid,
            "create_time_ns": entry.create_time_ns,
            "process_name": entry.process_name,
            "host": entry.host,
            "port": entry.port,
            "psk": REDACTED,
            # Pydantic ``mode="json"`` emits the ``Z`` short form for UTC
            # datetimes; compare via the same serialization path.
            "started_at": entry.model_dump(mode="json")["started_at"],
            "config_dir": str(entry.config_dir),
            "server_name": entry.server_name,
            "build_identity": entry.model_dump(mode="json")["build_identity"],
        },
        "paths": paths,
    }
    # Defense-in-depth: the PSK plaintext must never appear in the output.
    assert entry.psk.get_secret_value() not in result.output


def test_status_running_surfaces_build_identity(tmp_path: Path) -> None:
    """A daemon with a build identity surfaces version/venv/fingerprint."""
    rt = make_runtime(tmp_path)
    entry = make_entry().model_copy(
        update={
            "build_identity": DaemonBuildIdentity(
                version="9.9.9", venv="/venv/here", fingerprint="abc123def456"
            )
        }
    )
    rt.daemon_dir.read_entry.return_value = entry  # type: ignore[union-attr]
    with patch.object(DaemonRegistryEntry, "is_live", return_value=True):
        result = _invoke(["-o", "json", "daemon", "status"], rt)
    assert result.exit_code == 0
    build_identity = json.loads(result.output)["daemon"]["build_identity"]
    assert build_identity == {
        "version": "9.9.9",
        "venv": "/venv/here",
        "fingerprint": "abc123def456",
    }


def test_status_paths_present_when_down(tmp_path: Path) -> None:
    """paths (config/runtime/registry/log) is fully populated with no daemon."""
    rt = make_runtime(tmp_path)
    rt.daemon_dir.read_entry.return_value = None  # type: ignore[union-attr]
    paths = _expected_paths(rt, tmp_path)
    result = _invoke(["-o", "json", "daemon", "status"], rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["state"] == "stopped"
    assert payload["paths"] == paths


# ---------------------------------------------------------------------------
# restart
# ---------------------------------------------------------------------------


def test_restart_stops_then_starts(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    handle = make_entry()
    paths = _expected_paths(rt, tmp_path)
    with (
        patch.object(daemon_mod, "stop_daemon", AsyncMock(return_value=True)) as stop,
        patch.object(
            acquire_mod, "get_or_start_daemon", AsyncMock(return_value=handle)
        ) as start,
    ):
        result = _invoke(["-o", "json", "daemon", "restart"], rt)
    assert result.exit_code == 0
    stop.assert_awaited_once()
    start.assert_awaited_once()
    payload = json.loads(result.output)
    assert payload["state"] == "running"
    assert payload["daemon"]["pid"] == handle.pid
    assert payload["paths"] == paths


def test_restart_propagates_stop_failure(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        daemon_mod, "stop_daemon", AsyncMock(side_effect=DaemonClientError("nope"))
    ):
        result = _invoke(["daemon", "restart"], rt)
    assert result.exit_code == 2


def test_restart_propagates_start_timeout(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with (
        patch.object(daemon_mod, "stop_daemon", AsyncMock(return_value=False)),
        patch.object(
            acquire_mod,
            "get_or_start_daemon",
            AsyncMock(side_effect=DaemonStartupTimeoutError("slow")),
        ),
    ):
        result = _invoke(["daemon", "restart"], rt)
    assert result.exit_code == 2


def test_restart_propagates_start_client_error(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with (
        patch.object(daemon_mod, "stop_daemon", AsyncMock(return_value=False)),
        patch.object(
            acquire_mod,
            "get_or_start_daemon",
            AsyncMock(side_effect=DaemonClientError("nope")),
        ),
    ):
        result = _invoke(["daemon", "restart"], rt)
    assert result.exit_code == 2


# ---------------------------------------------------------------------------
# RegistryCorruptError surfacing
# ---------------------------------------------------------------------------


def test_status_corrupt_registry_returns_2(tmp_path: Path) -> None:
    """A corrupt registry must surface as an actionable error, not running=false.

    Locks the change in behavior: previously ``read_entry`` swallowed
    corruption and returned ``None``, so ``daemon status`` would
    misleadingly print ``running=false`` over a still-running daemon
    whose registry got scribbled on.
    """
    rt = make_runtime(tmp_path)
    rt.daemon_dir.read_entry.side_effect = RegistryCorruptError(  # type: ignore[union-attr]
        "malformed"
    )
    result = _invoke(["daemon", "status"], rt)
    assert result.exit_code == 2
    assert "malformed" in result.output.lower()


def test_stop_corrupt_registry_returns_2(tmp_path: Path) -> None:
    """``daemon stop`` must surface corruption — silently no-op'ing
    risks leaving an orphan daemon bound to the loopback port that
    the operator now needs to clean up manually."""
    rt = make_runtime(tmp_path)
    with patch.object(
        daemon_mod,
        "stop_daemon",
        AsyncMock(side_effect=RegistryCorruptError("malformed")),
    ):
        result = _invoke(["daemon", "stop"], rt)
    assert result.exit_code == 2
    assert "malformed" in result.output.lower()


def test_start_corrupt_registry_returns_2(tmp_path: Path) -> None:
    """``daemon start`` surfaces corruption with a recovery hint.

    ``get_or_start_daemon`` propagates ``RegistryCorruptError``
    unchanged (no more auto-quarantine); the command layer wraps it
    in ``CliError(DAEMON_REGISTRY_CORRUPT)`` and surfaces the
    ``dh-mcp daemon repair`` recovery procedure to the operator.
    """
    rt = make_runtime(tmp_path)
    with patch.object(
        acquire_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=RegistryCorruptError("malformed")),
    ):
        result = _invoke(["daemon", "start"], rt)
    assert result.exit_code == 2
    assert "malformed" in result.output.lower()


def test_restart_corrupt_on_stop_returns_2(tmp_path: Path) -> None:
    """``daemon restart`` surfaces corrupt-on-stop rather than
    auto-recovering: the restart's start path would race against an
    unkillable orphan daemon if we silently deleted and continued."""
    rt = make_runtime(tmp_path)
    with patch.object(
        daemon_mod,
        "stop_daemon",
        AsyncMock(side_effect=RegistryCorruptError("malformed")),
    ):
        result = _invoke(["daemon", "restart"], rt)
    assert result.exit_code == 2
    assert "malformed" in result.output.lower()


def test_restart_corrupt_on_start_returns_2(tmp_path: Path) -> None:
    """The post-stop start path also surfaces corrupt-on-poll."""
    rt = make_runtime(tmp_path)
    with (
        patch.object(daemon_mod, "stop_daemon", AsyncMock(return_value=False)),
        patch.object(
            acquire_mod,
            "get_or_start_daemon",
            AsyncMock(side_effect=RegistryCorruptError("malformed")),
        ),
    ):
        result = _invoke(["daemon", "restart"], rt)
    assert result.exit_code == 2
    assert "malformed" in result.output.lower()


# ---------------------------------------------------------------------------
# repair
# ---------------------------------------------------------------------------


def _make_real_runtime_with_daemon_dir(tmp_path: Path) -> Runtime:
    """Build a :class:`Runtime` backed by a real :class:`DaemonDirectory`.

    The default :func:`make_runtime` returns a ``MagicMock`` for
    ``daemon_dir`` because most command tests stub the lifecycle
    helpers. ``daemon repair`` reaches through to real file I/O
    (``registry_path.exists``, ``reg.read``, ``reg.quarantine``),
    so repair tests need an honest directory rooted at ``tmp_path``.
    """
    from deephaven_mcp.daemon_registry import DaemonDirectory

    daemon_dir = DaemonDirectory(tmp_path / "daemon")
    daemon_dir.path.mkdir(parents=True, exist_ok=True)
    return make_runtime(tmp_path, daemon_dir=daemon_dir)


@pytest.fixture
def _silence_registry_logger() -> Iterator[None]:
    """Suppress ``deephaven_mcp.daemon_registry`` logging during repair tests.

    The registry layer emits INFO / WARNING records during
    ``reg.write`` and ``reg.quarantine``. ``CliRunner`` mixes
    those records into the captured stdout (click 8.x default), so
    a ``json.loads(result.output)`` then fails on the leading log
    prefix. Raise the level past WARNING for the duration of the
    test so the captured output is pure click.echo content.
    """
    import logging

    logger = logging.getLogger("deephaven_mcp.daemon_registry")
    previous = logger.level
    logger.setLevel(logging.CRITICAL + 1)
    try:
        yield
    finally:
        logger.setLevel(previous)


def test_repair_no_registry_is_noop(tmp_path: Path) -> None:
    """`daemon repair` with no registry file emits ``repaired=false``.

    The verb is idempotent: rerunning after a successful repair must
    not error.
    """
    rt = _make_real_runtime_with_daemon_dir(tmp_path)
    result = _invoke(["-o", "json", "daemon", "repair"], rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == {"repaired": False, "message": "No registry to repair."}


def test_repair_quarantines_stale_registry(
    tmp_path: Path, _silence_registry_logger: None
) -> None:
    """A parseable-but-stale registry is moved aside cleanly.

    The well-known path is freed (so a subsequent ``daemon start``
    can publish a fresh entry) and the malformed-or-stale bytes are
    preserved on a timestamped sibling for postmortem.
    """
    rt = _make_real_runtime_with_daemon_dir(tmp_path)
    with rt.daemon_dir.locked() as reg:
        reg.write(make_entry())
    with patch.object(DaemonRegistryEntry, "is_live", return_value=False):
        result = _invoke(["-o", "json", "daemon", "repair"], rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["repaired"] is True
    assert "corrupt-" in payload["quarantined_to"]
    assert not rt.daemon_dir.registry_path.exists()
    quarantined = list(rt.daemon_dir.path.glob("daemon.json.corrupt-*"))
    assert len(quarantined) == 1


def test_repair_quarantines_corrupt_registry(
    tmp_path: Path, _silence_registry_logger: None
) -> None:
    """A registry that fails to parse is moved aside unconditionally.

    Liveness cannot be checked on a corrupt file (we have no PID),
    so ``daemon repair`` proceeds. The operator has already been told
    by the corrupt-registry recovery hint to verify no daemon is
    running before invoking ``repair``.
    """
    rt = _make_real_runtime_with_daemon_dir(tmp_path)
    rt.daemon_dir.registry_path.write_text("not json")
    result = _invoke(["-o", "json", "daemon", "repair"], rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["repaired"] is True
    quarantined_path = Path(payload["quarantined_to"])
    assert quarantined_path.read_text() == "not json"
    assert not rt.daemon_dir.registry_path.exists()


def test_repair_refuses_when_daemon_is_live(
    tmp_path: Path, _silence_registry_logger: None
) -> None:
    """`daemon repair` refuses while a live daemon is registered.

    Moving the registry aside out from under a running daemon would
    orphan the process from the CLI's perspective. The safety rail
    forces the operator to ``daemon stop`` first.
    """
    rt = _make_real_runtime_with_daemon_dir(tmp_path)
    entry = make_entry()
    with rt.daemon_dir.locked() as reg:
        reg.write(entry)
    with patch.object(DaemonRegistryEntry, "is_live", return_value=True):
        result = _invoke(["daemon", "repair"], rt)
    # ``CliRunner`` invokes ``cli`` directly (``standalone_mode=True``)
    # and does not pass through the ``main()`` wrapper that renders
    # structured ``error_code`` payloads; we therefore assert on the
    # human-mode message just like the other corrupt-registry tests.
    assert result.exit_code == 2
    assert f"pid={entry.pid}" in result.output
    assert "daemon stop" in result.output.lower()
    # The registry is left untouched so the operator can `daemon stop`.
    assert rt.daemon_dir.registry_path.exists()


def test_repair_handles_race_where_file_disappears(
    tmp_path: Path, _silence_registry_logger: None
) -> None:
    """The file exists at the gate but vanishes before quarantine.

    ``reg.quarantine`` returns ``None`` in this race (another
    process removed the file). The command must report it as a
    no-op rather than failing.
    """
    from deephaven_mcp.daemon_registry import LockedRegistry

    rt = _make_real_runtime_with_daemon_dir(tmp_path)
    with rt.daemon_dir.locked() as reg:
        reg.write(make_entry())

    # Simulate the race: the gate sees the file, but ``reg.quarantine``
    # returns None as if another process removed it.
    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=False),
        patch.object(LockedRegistry, "quarantine", return_value=None),
    ):
        result = _invoke(["-o", "json", "daemon", "repair"], rt)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == {"repaired": False, "message": "No registry to repair."}


# ---------------------------------------------------------------------------
# logs
# ---------------------------------------------------------------------------


def test_logs_missing_file_returns_2(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    daemon_dir = tmp_path / "daemon"
    daemon_dir.mkdir()
    rt.daemon_dir.log_path = daemon_dir / "daemon.log"  # type: ignore[union-attr]
    result = _invoke(["daemon", "logs"], rt)
    assert result.exit_code == 2


def test_logs_path_flag_prints_absolute_path(tmp_path: Path) -> None:
    """--path prints the log-file path and exits 0 even when the file is absent."""
    rt = make_runtime(tmp_path)
    log_path = tmp_path / "rt" / "daemon" / "daemon.log"  # never created
    rt.daemon_dir.log_path = log_path  # type: ignore[union-attr]
    result = _invoke(["daemon", "logs", "--path"], rt)
    assert result.exit_code == 0
    assert result.output.strip() == str(log_path)


def test_logs_tails_last_n_lines(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    daemon_dir = tmp_path / "daemon"
    daemon_dir.mkdir()
    log_path = daemon_dir / "daemon.log"
    log_path.write_text("\n".join(f"line {i}" for i in range(20)) + "\n")
    rt.daemon_dir.log_path = daemon_dir / "daemon.log"  # type: ignore[union-attr]
    result = _invoke(["daemon", "logs", "-n", "3"], rt)
    assert result.exit_code == 0
    lines = [line for line in result.output.splitlines() if line.startswith("line ")]
    assert lines == ["line 17", "line 18", "line 19"]


def test_tail_returns_verbatim_suffix() -> None:
    """``_tail`` slices the last n lines without adding or dropping newlines."""
    assert daemon_mod._tail("a\nb\nc\n", 2) == "b\nc\n"
    assert daemon_mod._tail("a\nb\nc\n", 0) == "a\nb\nc\n"
    assert daemon_mod._tail("a\nb\nc\n", -1) == "a\nb\nc\n"
    # A file without a trailing newline is returned verbatim — no newline added.
    assert daemon_mod._tail("a\nb", 5) == "a\nb"
    assert daemon_mod._tail("ab", 1) == "ab"


async def _run_follow_capturing(
    log_path: Path, *, lines: int, append: str
) -> list[str]:
    """Drive ``_tail_and_follow`` in follow mode, append, and capture echoes.

    Starts the streamer as a task, lets it emit the initial tail, appends
    ``append`` to the file, gives the follower a poll cycle to observe it,
    then cancels. Returns every chunk passed to ``click.echo``.
    """
    import asyncio as _asyncio

    captured: list[str] = []

    def fake_echo(text: str = "", *, nl: bool = True, **_kwargs: object) -> None:
        captured.append(text)

    with patch.object(daemon_mod.click, "echo", fake_echo):
        task = _asyncio.create_task(
            daemon_mod._tail_and_follow(
                log_path, lines=lines, follow=True, poll_interval=0.01
            )
        )
        try:
            await _asyncio.sleep(0.05)  # Let the initial tail emit.
            with log_path.open("a") as fh:
                fh.write(append)
                fh.flush()
            await _asyncio.sleep(0.05)  # Let the follower observe the append.
        finally:
            task.cancel()
            try:
                await task
            except _asyncio.CancelledError:
                pass

    return captured


@pytest.mark.asyncio
async def test_tail_and_follow_no_gap(tmp_path: Path) -> None:
    """The tail is emitted and post-handoff appends are followed, losing nothing.

    Regression for the dropped-lines race: the seed lines (which the old
    seek-to-EOF follow skipped) are part of the tail, and a line appended
    after the handoff is still captured.
    """
    log_path = tmp_path / "daemon.log"
    log_path.write_text("a\nb\n")

    captured = await _run_follow_capturing(log_path, lines=10, append="c\n")

    joined = "".join(captured)
    assert "a\nb\n" in joined  # tail not dropped
    assert "c\n" in joined  # post-handoff append followed


@pytest.mark.asyncio
async def test_tail_and_follow_verbatim_torn_line(tmp_path: Path) -> None:
    """A record split across two writes renders as one line, not two.

    The seed ends mid-line (no trailing newline); the continuation arrives
    via the follow. Because output is verbatim (nl=False, no injected
    newline), the fragment and its continuation join into a single line.
    """
    log_path = tmp_path / "daemon.log"
    log_path.write_text("x\nab")  # Last line "ab" has no trailing newline.

    captured = await _run_follow_capturing(log_path, lines=10, append="cde\n")

    assert "".join(captured) == "x\nabcde\n"


def test_logs_n_zero_returns_all_lines(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    daemon_dir = tmp_path / "daemon"
    daemon_dir.mkdir()
    log_path = daemon_dir / "daemon.log"
    log_path.write_text("a\nb\nc\n")
    rt.daemon_dir.log_path = daemon_dir / "daemon.log"  # type: ignore[union-attr]
    result = _invoke(["daemon", "logs", "-n", "0"], rt)
    assert result.exit_code == 0
    assert "a" in result.output
    assert "c" in result.output
