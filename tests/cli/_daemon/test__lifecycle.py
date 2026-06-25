"""Tests for ``deephaven_mcp.cli._daemon._lifecycle``."""

from __future__ import annotations

import os
import signal
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import psutil
import pytest

from deephaven_mcp._exceptions import InternalError
from deephaven_mcp._processes import ProcessIdentity, SignalOutcome
from deephaven_mcp.cli._daemon import (
    DaemonClientError,
    DaemonContext,
    DaemonReuseRefusedError,
    DaemonStartupTimeoutError,
)
from deephaven_mcp.cli._daemon import _lifecycle as ll
from deephaven_mcp.cli._daemon import (
    get_or_start_daemon,
    stop_daemon,
)
from deephaven_mcp.cli._daemon._reuse import ReuseDecision
from deephaven_mcp.config.schema import DaemonReusePolicy
from deephaven_mcp.daemon_registry import (
    DaemonBuildIdentity,
    DaemonDirectory,
    DaemonRegistryEntry,
    LockedRegistry,
    RegistryCorruptError,
)

_CURRENT_IDENTITY = DaemonBuildIdentity.current()
"""This process's build identity; written into test entries so the default
reuse policy treats them as a matching build (reuse) unless a test overrides
an identity field to force a difference."""


def _identity(**overrides: str) -> DaemonBuildIdentity:
    """Return the current build identity with the given fields overridden."""
    return _CURRENT_IDENTITY.model_copy(update=overrides)


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
        "build_identity": _CURRENT_IDENTITY,
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


def _build_ctx(runtime: MagicMock) -> DaemonContext:
    """Build the production :class:`DaemonContext` for the mock runtime.

    Tests use the same builder the command layer uses so they cannot
    drift from production.
    """
    return DaemonContext.from_runtime(runtime)


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
        "expected_identity": DaemonBuildIdentity.current(),
        "reuse_policy": daemon_cfg.reuse,
        "output_mode": runtime.config.cli.output.format,
        "kill_after_seconds": daemon_cfg.timeouts.kill_after_seconds,
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
    assert isinstance(entry, DaemonRegistryEntry)
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
    # The single in-lock read returns None (no entry), so the function
    # claims the spawn; once the entry is published the patched
    # ``is_live`` reports it live so the poll returns it.
    with (
        patch.object(ll, "spawn_detached", side_effect=_spawn) as mock_spawn,
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
    ):
        handle = await get_or_start_daemon(ctx, **_start_kwargs(runtime))
    assert handle.port == spawned_entry.port
    mock_spawn.assert_called_once()


@pytest.mark.asyncio
async def test_get_or_start_spawns_after_purging_stale_entry(tmp_path: Path) -> None:
    """A stale entry with auto_start on is deleted, then a fresh daemon spawns."""
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    fresh = _make_entry(port=45678)
    ctx = _build_ctx(runtime)
    # is_live False -> the entry is stale; the elif branch deletes it and the
    # function spawns a replacement.
    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=False),
        patch.object(ll, "spawn_detached") as mock_spawn,
        patch.object(ll, "_poll_for_registry", AsyncMock(return_value=fresh)),
    ):
        handle = await get_or_start_daemon(ctx, **_start_kwargs(runtime))
    assert handle is fresh
    mock_spawn.assert_called_once()


@pytest.mark.asyncio
async def test_get_or_start_raises_startup_timeout(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    ctx = _build_ctx(runtime)
    # Use a 1-second deadline so the test does not idle for the
    # configured 30-second default.
    with patch.object(ll, "spawn_detached"):
        with pytest.raises(DaemonStartupTimeoutError):
            await get_or_start_daemon(
                ctx, **_start_kwargs(runtime, startup_deadline_seconds=1)
            )


@pytest.mark.asyncio
async def test_get_or_start_defers_when_fresh_marker_present(tmp_path: Path) -> None:
    """A fresh spawn marker means a peer is starting; do not double-spawn.

    The double-spawn guard in ``_spawn_or_defer``: when a recent
    ``daemon.starting`` marker is present, skip ``spawn_detached`` and
    only poll for the peer's daemon.
    """
    runtime = _build_runtime(tmp_path)
    runtime.daemon_dir.path.mkdir(parents=True, exist_ok=True)
    with runtime.daemon_dir.locked() as reg:
        reg.write_start_marker(datetime.now(UTC))
    peer_entry = _make_entry(port=34567)
    ctx = _build_ctx(runtime)
    with (
        patch.object(ll, "spawn_detached") as mock_spawn,
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        patch.object(ll, "_poll_for_registry", AsyncMock(return_value=peer_entry)),
    ):
        handle = await get_or_start_daemon(ctx, **_start_kwargs(runtime))
    assert handle is peer_entry
    mock_spawn.assert_not_called()


@pytest.mark.asyncio
async def test_get_or_start_claims_spawn_when_marker_stale(tmp_path: Path) -> None:
    """A stale spawn marker (crashed spawner) is overwritten and we spawn.

    A marker older than ``startup_deadline_seconds`` indicates the
    previous spawner died before publishing; ``_spawn_or_defer`` claims
    the spawn, stamps a fresh marker, and launches the daemon.
    """
    runtime = _build_runtime(tmp_path)
    runtime.daemon_dir.path.mkdir(parents=True, exist_ok=True)
    with runtime.daemon_dir.locked() as reg:
        reg.write_start_marker(datetime.now(UTC) - timedelta(hours=1))
    spawned = _make_entry(port=45678)
    ctx = _build_ctx(runtime)
    with (
        patch.object(ll, "spawn_detached") as mock_spawn,
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        patch.object(ll, "_poll_for_registry", AsyncMock(return_value=spawned)),
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
    with patch.object(ll, "spawn_detached"):
        with pytest.raises(DaemonStartupTimeoutError):
            await get_or_start_daemon(
                ctx, **_start_kwargs(runtime, startup_deadline_seconds=1)
            )
    assert not runtime.daemon_dir.starting_path.exists()


# ---------------------------------------------------------------------------
# get_or_start_daemon: reuse-policy handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_or_start_warns_on_fingerprint_diff(tmp_path: Path) -> None:
    """fingerprint-only diff with the default policy (warn) reuses + warns."""
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry(build_identity=_identity(fingerprint="deadbeefdeadbeef")))
    ctx = _build_ctx(runtime)
    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        patch.object(ll, "render_warning") as mock_warn,
    ):
        entry = await get_or_start_daemon(ctx, **_start_kwargs(runtime))
    assert entry.pid == os.getpid()
    mock_warn.assert_called_once()


@pytest.mark.asyncio
async def test_get_or_start_refuses_on_version_diff(tmp_path: Path) -> None:
    """A version diff with the default policy (refuse) raises DaemonReuseRefusedError."""
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry(build_identity=_identity(version="0.0.0-other")))
    ctx = _build_ctx(runtime)
    with patch.object(DaemonRegistryEntry, "is_live", return_value=True):
        with pytest.raises(DaemonReuseRefusedError, match="different build") as excinfo:
            await get_or_start_daemon(ctx, **_start_kwargs(runtime))
    assert excinfo.value.differing == ("version",)


@pytest.mark.asyncio
async def test_get_or_start_ignores_diff_when_policy_ignore(tmp_path: Path) -> None:
    """An all-ignore policy reuses a differing daemon silently (no warning)."""
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry(build_identity=_identity(version="0.0.0-other")))
    ctx = _build_ctx(runtime)
    kwargs = _start_kwargs(runtime)
    kwargs["reuse_policy"] = DaemonReusePolicy(
        version="ignore", venv="ignore", fingerprint="ignore"
    )
    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        patch.object(ll, "render_warning") as mock_warn,
    ):
        entry = await get_or_start_daemon(ctx, **kwargs)
    assert entry.pid == os.getpid()
    mock_warn.assert_not_called()


@pytest.mark.asyncio
async def test_get_or_start_restarts_on_diff(tmp_path: Path) -> None:
    """A restart-action diff terminates the stale daemon and respawns."""
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry(build_identity=_identity(version="0.0.0-other")))
    ctx = _build_ctx(runtime)
    kwargs = _start_kwargs(runtime)
    kwargs["reuse_policy"] = DaemonReusePolicy(
        version="restart", venv="restart", fingerprint="restart"
    )
    fresh = _make_entry(port=56789)
    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        patch.object(
            ProcessIdentity, "send_signal_safely", return_value=SignalOutcome.DELIVERED
        ),
        patch.object(ProcessIdentity, "is_alive", return_value=False),
        patch.object(ll, "spawn_detached") as mock_spawn,
        patch.object(ll, "_poll_for_registry", AsyncMock(return_value=fresh)),
    ):
        handle = await get_or_start_daemon(ctx, **kwargs)
    assert handle is fresh
    mock_spawn.assert_called_once()


@pytest.mark.asyncio
async def test_get_or_start_restart_degrades_to_refuse_without_autostart(
    tmp_path: Path,
) -> None:
    """A restart action with auto_start off degrades to refuse (a restart spawns)."""
    runtime = _build_runtime(tmp_path, auto_start=False)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry(build_identity=_identity(version="0.0.0-other")))
    ctx = _build_ctx(runtime)
    kwargs = _start_kwargs(runtime)
    kwargs["reuse_policy"] = DaemonReusePolicy(
        version="restart", venv="restart", fingerprint="restart"
    )
    with patch.object(DaemonRegistryEntry, "is_live", return_value=True):
        with pytest.raises(DaemonReuseRefusedError):
            await get_or_start_daemon(ctx, **kwargs)


def test_decide_for_live_entry_rejects_unknown_action(tmp_path: Path) -> None:
    """An out-of-band reuse action trips the ``InternalError`` safety net.

    Unreachable while every ``DaemonReuseAction`` member is matched; we patch
    ``decide_reuse`` to return a bogus action so the defensive fallthrough is
    exercised rather than silently passing through.
    """
    entry = _make_entry()
    # Suppression justified: deliberately constructing a value the
    # DaemonReuseAction-typed field rejects so the defensive fallthrough is
    # covered. Bracketed arg-type names what is silenced; mypy still flags any
    # unintentional misuse at real call sites.
    bogus = ReuseDecision(action="bogus", differing=("version",))  # type: ignore[arg-type]
    with patch.object(ll, "decide_reuse", return_value=bogus):
        with pytest.raises(InternalError, match="Unhandled DaemonReuseAction"):
            ll._decide_for_live_entry(
                entry,
                expected_identity=DaemonBuildIdentity.current(),
                reuse_policy=DaemonReusePolicy(),
                can_spawn=True,
                output_mode="json",
            )


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


@pytest.mark.asyncio
async def test_stop_terminates_live_daemon(tmp_path: Path) -> None:
    """The live path delegates to ``terminate_identity`` then deletes the entry."""
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)
    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        patch.object(
            ll, "terminate_identity", AsyncMock(return_value=True)
        ) as mock_terminate,
    ):
        result = await stop_daemon(ctx.directory, kill_after_seconds=1)
    assert result is True
    mock_terminate.assert_awaited_once()
    assert runtime.daemon_dir.read_entry() is None


@pytest.mark.asyncio
async def test_stop_propagates_terminate_error(tmp_path: Path) -> None:
    """A ``DaemonClientError`` from termination surfaces and leaves the entry."""
    runtime = _build_runtime(tmp_path)
    with runtime.daemon_dir.locked() as reg:
        reg.write(_make_entry())
    ctx = _build_ctx(runtime)
    with (
        patch.object(DaemonRegistryEntry, "is_live", return_value=True),
        patch.object(
            ll,
            "terminate_identity",
            AsyncMock(side_effect=DaemonClientError("denied")),
        ),
        pytest.raises(DaemonClientError, match="denied"),
    ):
        await stop_daemon(ctx.directory, kill_after_seconds=1)


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
        entry = await ll._poll_for_registry(ctx, deadline_seconds=1)
    assert entry.pid == os.getpid()


@pytest.mark.asyncio
async def test_poll_raises_timeout(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    runtime.daemon_dir.path.mkdir(parents=True, exist_ok=True)
    ctx = _build_ctx(runtime)
    # No registry written; the poll loop should bail out at the deadline.
    with pytest.raises(DaemonStartupTimeoutError, match="did not start"):
        await ll._poll_for_registry(ctx, deadline_seconds=1)


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
        entry = await ll._poll_for_registry(ctx, deadline_seconds=2)
    assert entry is not None
    assert seen["n"] >= 2


# ---------------------------------------------------------------------------
# get_or_start_daemon: registry corruption
# ---------------------------------------------------------------------------


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
# terminate_identity (PID-reuse-safe SIGTERM/SIGKILL)
# ---------------------------------------------------------------------------


def _process_identity() -> ProcessIdentity:
    """Return a ProcessIdentity for the live test runner process."""
    return ProcessIdentity(
        pid=os.getpid(),
        create_time_ns=int(psutil.Process(os.getpid()).create_time() * 1e9),
    )


def _patch_send_signal_safely(outcomes: list[SignalOutcome]) -> Any:
    """Patch ``ProcessIdentity.send_signal_safely`` to return scripted outcomes.

    Each call returns the next outcome in ``outcomes``; once exhausted it
    returns ``GONE``. Lets tests script the SIGTERM / SIGKILL sequence
    deterministically.
    """
    iterator = iter(outcomes)

    def _impl(self: ProcessIdentity, sig: signal.Signals) -> SignalOutcome:
        try:
            return next(iterator)
        except StopIteration:
            return SignalOutcome.GONE

    return patch.object(ProcessIdentity, "send_signal_safely", _impl)


@pytest.mark.asyncio
async def test_terminate_returns_false_when_sigterm_gone() -> None:
    """``GONE`` from the SIGTERM probe means the process already exited."""
    with _patch_send_signal_safely([SignalOutcome.GONE]):
        assert (
            await ll.terminate_identity(_process_identity(), kill_after_seconds=10)
            is False
        )


@pytest.mark.asyncio
async def test_terminate_returns_false_when_sigterm_recycled() -> None:
    """``RECYCLED`` means the PID maps to an unrelated process; do not signal it."""
    with _patch_send_signal_safely([SignalOutcome.RECYCLED]):
        assert (
            await ll.terminate_identity(_process_identity(), kill_after_seconds=10)
            is False
        )


@pytest.mark.asyncio
async def test_terminate_raises_on_sigterm_denied() -> None:
    """``DENIED`` from the SIGTERM probe maps to ``DaemonClientError``."""
    with (
        _patch_send_signal_safely([SignalOutcome.DENIED]),
        pytest.raises(DaemonClientError, match="same user"),
    ):
        await ll.terminate_identity(_process_identity(), kill_after_seconds=10)


@pytest.mark.asyncio
async def test_terminate_with_sigterm_only_when_process_exits() -> None:
    """SIGTERM delivered, process exits during the wait -> no SIGKILL."""
    sent: list[signal.Signals] = []

    def _impl(self: ProcessIdentity, sig: signal.Signals) -> SignalOutcome:
        sent.append(sig)
        return SignalOutcome.DELIVERED

    with (
        patch.object(ProcessIdentity, "send_signal_safely", _impl),
        patch.object(ProcessIdentity, "is_alive", return_value=False),
    ):
        assert (
            await ll.terminate_identity(_process_identity(), kill_after_seconds=1)
            is True
        )
    assert sent == [signal.SIGTERM]


@pytest.mark.asyncio
async def test_terminate_escalates_to_sigkill_when_still_alive() -> None:
    """SIGTERM delivered, alive past the deadline -> SIGKILL."""
    sent: list[signal.Signals] = []

    def _impl(self: ProcessIdentity, sig: signal.Signals) -> SignalOutcome:
        sent.append(sig)
        return SignalOutcome.DELIVERED

    with (
        patch.object(ProcessIdentity, "send_signal_safely", _impl),
        patch.object(ProcessIdentity, "is_alive", return_value=True),
    ):
        assert (
            await ll.terminate_identity(_process_identity(), kill_after_seconds=0)
            is True
        )
    assert sent == [signal.SIGTERM, signal.SIGKILL]


@pytest.mark.asyncio
async def test_terminate_treats_recycled_on_sigkill_as_success() -> None:
    """SIGKILL returning RECYCLED means the daemon exited on its own -> success."""
    with (
        _patch_send_signal_safely([SignalOutcome.DELIVERED, SignalOutcome.RECYCLED]),
        patch.object(ProcessIdentity, "is_alive", return_value=True),
    ):
        assert (
            await ll.terminate_identity(_process_identity(), kill_after_seconds=0)
            is True
        )


@pytest.mark.asyncio
async def test_terminate_handles_sigkill_gone() -> None:
    """SIGTERM delivered, SIGKILL says ``GONE`` (process exited): success."""
    with (
        _patch_send_signal_safely([SignalOutcome.DELIVERED, SignalOutcome.GONE]),
        patch.object(ProcessIdentity, "is_alive", return_value=True),
    ):
        assert (
            await ll.terminate_identity(_process_identity(), kill_after_seconds=0)
            is True
        )


@pytest.mark.asyncio
async def test_terminate_raises_on_sigkill_denied() -> None:
    """A successful SIGTERM but ``DENIED`` SIGKILL still surfaces.

    Defense-in-depth: should never happen in practice (same-user constraint
    applies to both signals), but if the OS produces an ``EPERM`` only on
    SIGKILL the operator gets a structured error rather than a silent failure.
    """
    with (
        _patch_send_signal_safely([SignalOutcome.DELIVERED, SignalOutcome.DENIED]),
        patch.object(ProcessIdentity, "is_alive", return_value=True),
        pytest.raises(DaemonClientError, match="SIGKILL"),
    ):
        await ll.terminate_identity(_process_identity(), kill_after_seconds=0)


@pytest.mark.asyncio
async def test_terminate_polls_until_process_exits() -> None:
    """The wait loop sleeps between liveness checks until the process exits."""
    sent: list[signal.Signals] = []

    def _impl(self: ProcessIdentity, sig: signal.Signals) -> SignalOutcome:
        sent.append(sig)
        return SignalOutcome.DELIVERED

    # Two iterations of the wait loop: still alive, then gone.
    alive_states = iter([True, False])

    def fake_is_alive(self: ProcessIdentity) -> bool:
        return next(alive_states, False)

    with (
        patch.object(ProcessIdentity, "send_signal_safely", _impl),
        patch.object(ProcessIdentity, "is_alive", fake_is_alive),
    ):
        assert (
            await ll.terminate_identity(_process_identity(), kill_after_seconds=5)
            is True
        )
    assert sent == [signal.SIGTERM]


@pytest.mark.asyncio
async def test_terminate_post_loop_check_catches_late_exit() -> None:
    """The post-loop liveness check recognizes an exit just before the deadline."""
    sent: list[signal.Signals] = []

    def _impl(self: ProcessIdentity, sig: signal.Signals) -> SignalOutcome:
        sent.append(sig)
        return SignalOutcome.DELIVERED

    # ``is_alive`` returns False on the first probe so the post-loop check
    # short-circuits and SIGKILL is not sent.
    with (
        patch.object(ProcessIdentity, "send_signal_safely", _impl),
        patch.object(ProcessIdentity, "is_alive", return_value=False),
    ):
        assert (
            await ll.terminate_identity(_process_identity(), kill_after_seconds=0)
            is True
        )
    assert sent == [signal.SIGTERM]
