"""Tests for ``deephaven_mcp.cli._daemon``."""

from __future__ import annotations

import os
import signal
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import psutil
import pytest

from deephaven_mcp.cli import _daemon as dl
from deephaven_mcp.cli._daemon import (
    DaemonClientError,
    DaemonStartupTimeoutError,
    get_or_start_daemon,
    stop_daemon,
)
from deephaven_mcp.daemon_registry import (
    DaemonDirectory,
    DaemonRegistryEntry,
    LockedRegistry,
    RegistryCorruptError,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_entry(**overrides: Any) -> DaemonRegistryEntry:
    defaults: dict[str, Any] = {
        "pid": os.getpid(),
        # Pulled from the live process so identity.is_alive() succeeds
        # against the actual test runner. Tests that need a sentinel
        # value pass create_time_ns explicitly.
        "create_time_ns": int(psutil.Process(os.getpid()).create_time() * 1e9),
        "process_name": "python",
        "host": "127.0.0.1",
        "port": 12345,
        "psk": "shhhhhhhhhhhhhhhh",
        "started_at": "2026-05-27T00:00:00+00:00",
        "config_dir": "/tmp/cfg",
        "server_name": "dh-test",
    }
    defaults.update(overrides)
    return DaemonRegistryEntry.model_validate(defaults)


def _build_runtime(tmp_path: Path, *, auto_start: bool = True) -> MagicMock:
    from deephaven_mcp.config.schema import CliConfig, ServerConfig
    from deephaven_mcp.config.tree import ConfigTree

    runtime = MagicMock()
    cli_config = CliConfig.model_validate({"daemon": {"auto_start": auto_start}})
    runtime.config = ConfigTree(
        config_dir=tmp_path / "cfg",
        cli=cli_config,
        server=ServerConfig(),
    )
    runtime.config_dir = tmp_path / "cfg"
    runtime.runtime_dir = tmp_path / "rt"
    runtime.daemon_dir = DaemonDirectory(tmp_path / "rt" / "daemon")
    return runtime


def _build_ctx(runtime: MagicMock) -> dl.DaemonContext:
    """Build the production :class:`DaemonContext` for the mock runtime.

    Tests use the same builder the command layer uses so they cannot
    drift from production.
    """
    return dl.build_daemon_context(runtime)


def _start_kwargs(
    runtime: MagicMock, *, startup_deadline_seconds: int | None = None
) -> dict[str, Any]:
    """Keyword arguments for :func:`get_or_start_daemon`.

    Mirrors the command layer: ``auto_start`` and
    ``startup_deadline_seconds`` are read from
    ``runtime.config.cli.daemon`` unless a test overrides the deadline
    (e.g. to keep a timeout test from idling for the configured
    default).
    """
    daemon_cfg = runtime.config.cli.daemon
    return {
        "auto_start": daemon_cfg.auto_start,
        "startup_deadline_seconds": (
            daemon_cfg.timeouts.startup_deadline_seconds
            if startup_deadline_seconds is None
            else startup_deadline_seconds
        ),
    }


# ---------------------------------------------------------------------------
# get_or_start_daemon
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_or_start_returns_existing_live_daemon(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)
    with patch.object(DaemonRegistryEntry, "is_live", return_value=True):
        entry = await get_or_start_daemon(ctx, **_start_kwargs(runtime))
    assert isinstance(entry, dl.DaemonRegistryEntry)
    assert entry.pid == os.getpid()


@pytest.mark.asyncio
async def test_get_or_start_raises_when_no_daemon_and_auto_start_off(
    tmp_path: Path,
) -> None:
    runtime = _build_runtime(tmp_path, auto_start=False)
    ctx = _build_ctx(runtime)
    with pytest.raises(DaemonClientError, match="auto-start"):
        await get_or_start_daemon(ctx, **_start_kwargs(runtime))


@pytest.mark.asyncio
async def test_get_or_start_purges_stale_registry_when_disabled(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path, auto_start=False)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)
    with patch.object(DaemonRegistryEntry, "is_live", return_value=False):
        with pytest.raises(DaemonClientError):
            await get_or_start_daemon(ctx, **_start_kwargs(runtime))
    assert runtime.daemon_dir.read_entry() is None


@pytest.mark.asyncio
async def test_get_or_start_spawns_when_no_daemon(tmp_path: Path) -> None:
    """auto_start path: lock taken, spawn called, registry polled."""
    runtime = _build_runtime(tmp_path)
    spawned_entry = _make_entry()

    def _spawn(*_args: Any, **_kwargs: Any) -> None:
        # Simulate the daemon coming up by writing the registry.
        # In production the spawn happens in a *separate process* so
        # the daemon takes its own ``locked()`` session. Here the
        # callback runs synchronously inside the parent CLI's held
        # lock; re-entering ``locked()`` would block on the advisory
        # lock (a second fd in the same process) until the bounded
        # acquire timed out. ``LockedRegistry.write`` performs no lock
        # acquisition of its own, so calling it on an unentered
        # session writes the file under the parent's held lock.
        LockedRegistry(runtime.daemon_dir).write(spawned_entry)

    ctx = _build_ctx(runtime)
    # ``read_entry()`` returns None on the fast path, so the call site
    # short-circuits before ``is_live()``; once an entry is published,
    # the patched ``is_live`` reports it live so the poll returns it.
    with (
        patch.object(dl, "spawn_detached", side_effect=_spawn) as mock_spawn,
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
    ):
        handle = await get_or_start_daemon(ctx, **_start_kwargs(runtime))
    assert handle.port == spawned_entry.port
    mock_spawn.assert_called_once()


@pytest.mark.asyncio
async def test_get_or_start_recovers_from_inside_lock_race(tmp_path: Path) -> None:
    """Another process publishes the registry while we held the lock."""
    runtime = _build_runtime(tmp_path)
    other_entry = _make_entry(port=23456)

    def _spawn(*_args: Any, **_kwargs: Any) -> None:  # pragma: no cover - never reached
        raise AssertionError(
            "spawn should not be called when the registry appears inside the lock"
        )

    runtime.daemon_dir.path.mkdir(parents=True, exist_ok=True)

    # Patch read so the first call (outside the lock) sees nothing,
    # but the second call (inside the lock) sees the freshly published
    # entry.
    calls = {"n": 0}

    def fake_read() -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            return None
        return other_entry

    ctx = _build_ctx(runtime)
    with (
        patch.object(runtime.daemon_dir, "read_entry", side_effect=fake_read),
        patch.object(dl, "spawn_detached", side_effect=_spawn),
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
    ):
        handle = await get_or_start_daemon(ctx, **_start_kwargs(runtime))
    assert handle.port == other_entry.port


@pytest.mark.asyncio
async def test_get_or_start_raises_startup_timeout(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    ctx = _build_ctx(runtime)
    # Use a 1-second deadline so the test does not idle for the
    # configured 30-second default.
    with patch.object(dl, "spawn_detached"):
        with pytest.raises(DaemonStartupTimeoutError):
            await get_or_start_daemon(
                ctx, **_start_kwargs(runtime, startup_deadline_seconds=1)
            )


@pytest.mark.asyncio
async def test_get_or_start_defers_when_fresh_marker_present(tmp_path: Path) -> None:
    """A fresh spawn marker means a peer is starting; do not double-spawn.

    The double-spawn guard: when the slow path finds a recent
    ``daemon.starting`` marker, it must skip ``spawn_detached`` and
    only poll for the peer's daemon.
    """
    runtime = _build_runtime(tmp_path)
    runtime.daemon_dir.path.mkdir(parents=True, exist_ok=True)
    with runtime.daemon_dir.locked() as reg:
        reg.write_start_marker(datetime.now(UTC))
    peer_entry = _make_entry(port=34567)
    ctx = _build_ctx(runtime)
    with (
        patch.object(dl, "spawn_detached") as mock_spawn,
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        patch.object(dl, "_poll_for_registry", AsyncMock(return_value=peer_entry)),
    ):
        handle = await get_or_start_daemon(ctx, **_start_kwargs(runtime))
    assert handle is peer_entry
    mock_spawn.assert_not_called()


@pytest.mark.asyncio
async def test_get_or_start_claims_spawn_when_marker_stale(tmp_path: Path) -> None:
    """A stale spawn marker (crashed spawner) is overwritten and we spawn.

    A marker older than ``startup_deadline_seconds`` indicates the
    previous spawner died before publishing; this caller claims the
    spawn, stamps a fresh marker, and launches the daemon.
    """
    runtime = _build_runtime(tmp_path)
    runtime.daemon_dir.path.mkdir(parents=True, exist_ok=True)
    with runtime.daemon_dir.locked() as reg:
        reg.write_start_marker(datetime.now(UTC) - timedelta(hours=1))
    spawned = _make_entry(port=45678)
    ctx = _build_ctx(runtime)
    with (
        patch.object(dl, "spawn_detached") as mock_spawn,
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        patch.object(dl, "_poll_for_registry", AsyncMock(return_value=spawned)),
    ):
        handle = await get_or_start_daemon(ctx, **_start_kwargs(runtime))
    assert handle is spawned
    mock_spawn.assert_called_once()
    # The stale marker was overwritten with a fresh timestamp.
    with runtime.daemon_dir.locked() as reg:
        marker = reg.read_start_marker()
    assert marker is not None
    assert datetime.now(UTC) - marker < timedelta(seconds=60)


@pytest.mark.asyncio
async def test_get_or_start_clears_marker_on_timeout(tmp_path: Path) -> None:
    """A spawn that never publishes clears its own marker on timeout.

    Otherwise the stale "spawn in progress" flag would block a retry
    for the remainder of the staleness window.
    """
    runtime = _build_runtime(tmp_path)
    ctx = _build_ctx(runtime)
    # ``spawn_detached`` is a no-op, so no daemon ever publishes and
    # the poll times out.
    with patch.object(dl, "spawn_detached"):
        with pytest.raises(DaemonStartupTimeoutError):
            await get_or_start_daemon(
                ctx, **_start_kwargs(runtime, startup_deadline_seconds=1)
            )
    assert not runtime.daemon_dir.starting_path.exists()


# ---------------------------------------------------------------------------
# stop_daemon
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_returns_false_when_no_registry(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    ctx = _build_ctx(runtime)
    assert await stop_daemon(ctx.directory, kill_after_seconds=10) is False


@pytest.mark.asyncio
async def test_stop_cleans_stale_registry(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)
    with patch.object(DaemonRegistryEntry, "is_live", return_value=False):
        assert await stop_daemon(ctx.directory, kill_after_seconds=10) is False
    assert runtime.daemon_dir.read_entry() is None


def _patch_send_signal_safely(outcomes: list[dl.SignalOutcome]) -> Any:
    """Return a context manager that patches ``ProcessIdentity.send_signal_safely``.

    Each call to ``send_signal_safely`` returns the next outcome in
    ``outcomes`` (in order). Lets stop_daemon tests script the
    sequence of SIGTERM / SIGKILL outcomes deterministically.
    """
    iterator = iter(outcomes)

    def _impl(self: dl.ProcessIdentity, sig: signal.Signals) -> dl.SignalOutcome:
        try:
            return next(iterator)
        except StopIteration:
            return dl.SignalOutcome.GONE

    return patch.object(dl.ProcessIdentity, "send_signal_safely", _impl)


@pytest.mark.asyncio
async def test_stop_terminates_live_daemon(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)

    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        _patch_send_signal_safely([dl.SignalOutcome.DELIVERED]),
        patch.object(dl.ProcessIdentity, "is_alive", return_value=False),
    ):
        result = await stop_daemon(ctx.directory, kill_after_seconds=1)
    assert result is True
    assert runtime.daemon_dir.read_entry() is None


@pytest.mark.asyncio
async def test_stop_escalates_to_sigkill(tmp_path: Path) -> None:
    """SIGTERM delivered, process still alive after deadline -> SIGKILL."""
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)

    sent_signals: list[signal.Signals] = []

    def _impl(self: dl.ProcessIdentity, sig: signal.Signals) -> dl.SignalOutcome:
        sent_signals.append(sig)
        return dl.SignalOutcome.DELIVERED

    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        patch.object(dl.ProcessIdentity, "send_signal_safely", _impl),
        # is_alive: True throughout the wait, then True at the
        # post-loop check too (so SIGKILL fires).
        patch.object(dl.ProcessIdentity, "is_alive", return_value=True),
    ):
        result = await stop_daemon(ctx.directory, kill_after_seconds=0)
    assert result is True
    assert sent_signals == [signal.SIGTERM, signal.SIGKILL]


@pytest.mark.asyncio
async def test_stop_treats_recycled_on_sigkill_as_success(tmp_path: Path) -> None:
    """SIGKILL returning RECYCLED means the daemon exited on its own -> success.

    SIGTERM is DELIVERED, the process stays alive through the wait so
    SIGKILL fires, and by then the PID has been recycled. A non-DENIED
    SIGKILL outcome is the desired post-condition: delete and return True.
    """
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)

    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        _patch_send_signal_safely(
            [dl.SignalOutcome.DELIVERED, dl.SignalOutcome.RECYCLED]
        ),
        patch.object(dl.ProcessIdentity, "is_alive", return_value=True),
    ):
        result = await stop_daemon(ctx.directory, kill_after_seconds=0)
    assert result is True
    assert runtime.daemon_dir.read_entry() is None


@pytest.mark.asyncio
async def test_stop_skips_sigkill_when_process_exits_after_loop(tmp_path: Path) -> None:
    """Post-loop liveness check catches a daemon that exited just before the deadline."""
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)

    sent_signals: list[signal.Signals] = []

    def _impl(self: dl.ProcessIdentity, sig: signal.Signals) -> dl.SignalOutcome:
        sent_signals.append(sig)
        return dl.SignalOutcome.DELIVERED

    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        patch.object(dl.ProcessIdentity, "send_signal_safely", _impl),
        # ``is_alive`` returns False on the first probe so the
        # post-loop check short-circuits and SIGKILL is not sent.
        patch.object(dl.ProcessIdentity, "is_alive", return_value=False),
    ):
        result = await stop_daemon(ctx.directory, kill_after_seconds=0)
    assert result is True
    assert sent_signals == [signal.SIGTERM]


@pytest.mark.asyncio
async def test_stop_polls_until_process_exits(tmp_path: Path) -> None:
    """The stop loop sleeps between liveness checks until the process exits."""
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)

    # Two iterations of the wait loop: still alive, then gone.
    alive_states = iter([True, False])

    def fake_is_alive(self: dl.ProcessIdentity) -> bool:
        return next(alive_states, False)

    sent_signals: list[signal.Signals] = []

    def _impl(self: dl.ProcessIdentity, sig: signal.Signals) -> dl.SignalOutcome:
        sent_signals.append(sig)
        return dl.SignalOutcome.DELIVERED

    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        patch.object(dl.ProcessIdentity, "send_signal_safely", _impl),
        patch.object(dl.ProcessIdentity, "is_alive", fake_is_alive),
    ):
        result = await stop_daemon(ctx.directory, kill_after_seconds=5)
    assert result is True
    assert sent_signals == [signal.SIGTERM]
    assert runtime.daemon_dir.read_entry() is None


@pytest.mark.asyncio
async def test_stop_handles_sigterm_gone(tmp_path: Path) -> None:
    """``GONE`` from the SIGTERM probe means the process already exited."""
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)
    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        _patch_send_signal_safely([dl.SignalOutcome.GONE]),
    ):
        assert await stop_daemon(ctx.directory, kill_after_seconds=10) is False
    assert runtime.daemon_dir.read_entry() is None


@pytest.mark.asyncio
async def test_stop_handles_sigterm_recycled(tmp_path: Path) -> None:
    """``RECYCLED`` from the SIGTERM probe is treated like a vanished process.

    A different create_time than the captured ``create_time_ns``
    means the kernel reused the PID for an unrelated process. The
    daemon is gone; do not signal a stranger.
    """
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)
    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        _patch_send_signal_safely([dl.SignalOutcome.RECYCLED]),
    ):
        assert await stop_daemon(ctx.directory, kill_after_seconds=10) is False
    assert runtime.daemon_dir.read_entry() is None


@pytest.mark.asyncio
async def test_stop_raises_on_sigterm_denied(tmp_path: Path) -> None:
    """``DENIED`` from the SIGTERM probe maps to ``DaemonClientError``."""
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)
    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        _patch_send_signal_safely([dl.SignalOutcome.DENIED]),
        pytest.raises(DaemonClientError, match="same user"),
    ):
        await stop_daemon(ctx.directory, kill_after_seconds=10)


@pytest.mark.asyncio
async def test_stop_raises_on_sigkill_denied(tmp_path: Path) -> None:
    """A successful SIGTERM but ``DENIED`` SIGKILL still surfaces.

    Defense-in-depth: should never happen in practice (same-user
    constraint applies to both signals), but if the OS produces an
    ``EPERM`` only on SIGKILL the operator gets a structured error
    rather than a silent failure.
    """
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)
    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        _patch_send_signal_safely(
            [dl.SignalOutcome.DELIVERED, dl.SignalOutcome.DENIED]
        ),
        patch.object(dl.ProcessIdentity, "is_alive", return_value=True),
        pytest.raises(DaemonClientError, match="SIGKILL"),
    ):
        await stop_daemon(ctx.directory, kill_after_seconds=0)


@pytest.mark.asyncio
async def test_stop_handles_sigkill_gone(tmp_path: Path) -> None:
    """SIGTERM delivered, SIGKILL says ``GONE`` (process exited): success."""
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)
    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        _patch_send_signal_safely([dl.SignalOutcome.DELIVERED, dl.SignalOutcome.GONE]),
        patch.object(dl.ProcessIdentity, "is_alive", return_value=True),
    ):
        result = await stop_daemon(ctx.directory, kill_after_seconds=0)
    assert result is True
    assert runtime.daemon_dir.read_entry() is None


@pytest.mark.asyncio
async def test_get_or_start_propagates_registry_corrupt_error(
    tmp_path: Path,
) -> None:
    """A corrupt registry surfaces ``RegistryCorruptError`` unchanged.

    Locks the fail-fast contract: ``get_or_start_daemon`` no longer
    auto-quarantines. Corruption propagates to the command layer,
    which translates it to ``CliError(DAEMON_REGISTRY_CORRUPT)``
    with a recovery hint pointing at ``dh-mcp daemon repair``. The
    well-known path is left untouched so the operator's manual
    diagnostic does not race with an implicit rename.
    """
    runtime = _build_runtime(tmp_path)
    runtime.daemon_dir.path.mkdir(parents=True, exist_ok=True)
    registry_path = runtime.daemon_dir.registry_path
    registry_path.write_text("not json")
    ctx = _build_ctx(runtime)

    with pytest.raises(RegistryCorruptError):
        await get_or_start_daemon(ctx, **_start_kwargs(runtime))

    # The corrupt file is left in place for the operator to inspect /
    # quarantine via ``dh-mcp daemon repair``.
    assert registry_path.exists()
    assert registry_path.read_text() == "not json"
    quarantined = list(runtime.daemon_dir.path.glob("daemon.json.corrupt-*"))
    assert quarantined == []


# ---------------------------------------------------------------------------
# _build_spawn_command
# ---------------------------------------------------------------------------


def test_build_spawn_command_has_expected_shape(tmp_path: Path) -> None:
    """The spawn argv is sys.executable + ``-m`` + module + paired flags."""
    import sys

    runtime = _build_runtime(tmp_path)
    cmd = dl._build_spawn_command(runtime)
    assert cmd[0] == sys.executable
    assert cmd[1] == "-m"
    assert cmd[2] == "deephaven_mcp.mcp_systems_server"
    assert "--daemon" in cmd
    # ``--config-dir`` and ``--runtime-dir`` are each followed by
    # the matching runtime path.
    assert cmd[cmd.index("--config-dir") + 1] == str(runtime.config_dir)
    assert cmd[cmd.index("--runtime-dir") + 1] == str(runtime.runtime_dir)


# ---------------------------------------------------------------------------
# _poll_for_registry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_poll_returns_live_entry_immediately(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)
    with patch.object(DaemonRegistryEntry, "is_live", return_value=True):
        entry = await dl._poll_for_registry(ctx, deadline_seconds=1)
    assert entry.pid == os.getpid()


@pytest.mark.asyncio
async def test_poll_raises_timeout(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    runtime.daemon_dir.path.mkdir(parents=True, exist_ok=True)
    ctx = _build_ctx(runtime)
    # No registry written; the poll loop should bail out at the deadline.
    with pytest.raises(DaemonStartupTimeoutError, match="did not start"):
        await dl._poll_for_registry(ctx, deadline_seconds=1)


@pytest.mark.asyncio
async def test_poll_skips_stale_entry_until_live(tmp_path: Path) -> None:
    """A stale entry is ignored until ``is_live`` reports True."""
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)

    seen: dict[str, int] = {"n": 0}

    def fake_check() -> bool:
        seen["n"] += 1
        # First call: stale; second call: live.
        return seen["n"] >= 2

    with patch.object(DaemonRegistryEntry, "is_live", side_effect=fake_check):
        entry = await dl._poll_for_registry(ctx, deadline_seconds=2)
    assert entry is not None
    assert seen["n"] >= 2


def test_get_or_start_does_not_clobber_fresh_entry_inside_lock(
    tmp_path: Path,
) -> None:
    """The lock-protected slow path returns the fresh entry, not a delete-and-respawn.

    Simulates the canonical race: lock-free read sees nothing
    (or a stale entry), but inside the lock another process has
    published a fresh, live entry. The function must observe the
    fresh entry on the second read and return it without touching
    the registry.
    """
    import asyncio

    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)

    fresh = _make_entry(port=23456)

    # First read returns None (lock-free fast path); second read
    # (inside the lock) returns the freshly-published entry.
    reads: dict[str, int] = {"n": 0}

    def fake_read() -> Any:
        reads["n"] += 1
        if reads["n"] == 1:
            return None
        return fresh

    ctx = dl.DaemonContext(
        directory=dd,
        spawn_argv=["true"],
        spawn_cwd=tmp_path,
    )

    def boom(
        *_args: Any, **_kwargs: Any
    ) -> None:  # pragma: no cover - must not be called
        raise AssertionError("spawn_detached must not be called")

    with (
        patch.object(dd, "read_entry", side_effect=fake_read),
        patch.object(dl, "spawn_detached", side_effect=boom),
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
    ):
        result = asyncio.run(
            dl.get_or_start_daemon(ctx, auto_start=True, startup_deadline_seconds=1)
        )
    assert result is fresh
