"""Tests for ``deephaven_mcp.daemon_registry``."""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import SecretStr, ValidationError

from deephaven_mcp._platform.fsutil import _DEFAULT_LOCK_TIMEOUT_SECONDS
from deephaven_mcp._processes import ProcessIdentity
from deephaven_mcp._redaction import REDACTED
from deephaven_mcp.daemon_registry import (
    _DAEMON_LOCK_FILENAME,
    _DAEMON_LOG_FILENAME,
    _DAEMON_REGISTRY_FILENAME,
    _DAEMON_STARTING_FILENAME,
    DaemonBuildIdentity,
    DaemonDirectory,
    DaemonRegistryEntry,
    LockedRegistry,
    RegistryCorruptError,
    _compute_source_fingerprint,
)

_PSK_PLAINTEXT = "shhhhhhhhhhhhhhhh"


# ---------------------------------------------------------------------------
# DaemonRegistryEntry
# ---------------------------------------------------------------------------


def _make_entry(**overrides: Any) -> DaemonRegistryEntry:
    defaults: dict[str, Any] = {
        "pid": 4321,
        "create_time_ns": 1_700_000_000_000_000_000,
        "process_name": "dh-mcp-systems-server",
        "host": "127.0.0.1",
        "port": 51234,
        "psk": SecretStr(_PSK_PLAINTEXT),
        "started_at": datetime(2026, 5, 27, 0, 0, 0, tzinfo=UTC),
        "config_dir": Path("/tmp/cfg"),
        "server_name": "dh-test",
        "build_identity": {
            "version": "1.2.3",
            "venv": "/venv/x",
            "fingerprint": "f" * 64,
        },
    }
    defaults.update(overrides)
    return DaemonRegistryEntry.model_validate(defaults)


def test_registry_entry_round_trips_through_json() -> None:
    """``model_dump(mode='json')`` + ``model_validate`` is a no-op.

    Uses ``context={'reveal': True}`` because the default JSON dump
    masks the SecretStr; without reveal the round-trip would replace
    the PSK with ``"**********"``.
    """
    entry = _make_entry()
    payload = json.dumps(entry.model_dump(mode="json", context={"reveal": True}))
    rebuilt = DaemonRegistryEntry.model_validate(json.loads(payload))
    assert rebuilt == entry


# ---------------------------------------------------------------------------
# DaemonRegistryEntry — field-level invariants
#
# The model annotations encode the daemon's wire-format guarantees so a
# corrupted ``daemon.json`` is rejected at ``model_validate`` time rather
# than producing surprising downstream behavior. Each invariant below maps
# to a docstring claim on the model field.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_pid", [0, -1, -9999])
def test_entry_rejects_non_positive_pid(bad_pid: int) -> None:
    with pytest.raises(ValidationError):
        _make_entry(pid=bad_pid)


@pytest.mark.parametrize("bad_port", [0, -1, 65536, 100000])
def test_entry_rejects_out_of_range_port(bad_port: int) -> None:
    with pytest.raises(ValidationError):
        _make_entry(port=bad_port)


@pytest.mark.parametrize("bad_host", ["::1", "localhost", "0.0.0.0", "10.0.0.5", ""])
def test_entry_rejects_non_ipv4_loopback_host(bad_host: str) -> None:
    """Daemon mode is IPv4-loopback-only; the schema enforces it.

    Even ``::1`` and ``localhost`` — which the *operator* HTTP path
    accepts — are rejected here because the daemon registry wire
    format and the CLI's MCP client both assume IPv4 loopback.
    """
    with pytest.raises(ValidationError):
        _make_entry(host=bad_host)


def test_entry_rejects_naive_started_at() -> None:
    """``AwareDatetime`` rejects naive datetimes.

    The daemon writes ``datetime.now(UTC)``; any naive timestamp on
    disk indicates corruption or a foreign writer.
    """
    with pytest.raises(ValidationError):
        _make_entry(started_at=datetime(2026, 5, 27, 0, 0, 0))


@pytest.mark.parametrize("field_name", ["process_name", "server_name"])
def test_entry_rejects_empty_string_identifiers(field_name: str) -> None:
    """Empty identifiers leave downstream output / cross-checks ambiguous."""
    with pytest.raises(ValidationError):
        _make_entry(**{field_name: ""})


def test_entry_identity_pairs_pid_and_create_time() -> None:
    """``entry.identity`` carries the ``(pid, create_time_ns)`` pair."""
    entry = _make_entry()
    identity = entry.identity
    assert identity.pid == entry.pid
    assert identity.create_time_ns == entry.create_time_ns


def test_entry_is_live_true_when_identity_alive() -> None:
    """``is_live()`` returns True when the recorded process is alive."""
    entry = _make_entry()
    with patch.object(ProcessIdentity, "is_alive", return_value=True):
        assert entry.is_live() is True


def test_entry_is_live_false_when_identity_dead() -> None:
    """``is_live()`` returns False when the PID is gone or recycled."""
    entry = _make_entry()
    with patch.object(ProcessIdentity, "is_alive", return_value=False):
        assert entry.is_live() is False


# ---------------------------------------------------------------------------
# DaemonBuildIdentity + build_identity
# ---------------------------------------------------------------------------


def test_build_identity_current_populates_all_fields() -> None:
    """``current()`` returns a fully-populated identity for this process."""
    import sys

    identity = DaemonBuildIdentity.current()
    assert identity.version
    assert identity.venv == sys.prefix
    assert identity.fingerprint
    assert identity == DaemonBuildIdentity.current()


def test_build_identity_equality_is_all_or_nothing() -> None:
    """Equality is True only when every field is equal (Pydantic ``==``)."""
    base = DaemonBuildIdentity(version="1", venv="/v", fingerprint="f")
    assert base == DaemonBuildIdentity(version="1", venv="/v", fingerprint="f")
    assert base != DaemonBuildIdentity(version="2", venv="/v", fingerprint="f")


def test_source_fingerprint_changes_when_a_file_stat_changes(tmp_path: Path) -> None:
    """The fingerprint folds file size + mtime, so an edit shifts the digest."""
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    f = pkg / "mod.py"
    f.write_text("x = 1\n")
    first = _compute_source_fingerprint(pkg)
    assert first == _compute_source_fingerprint(pkg)  # stable across calls
    # Grow the file (changes both size and mtime).
    f.write_text("x = 1\ny = 2\n")
    assert _compute_source_fingerprint(pkg) != first


def test_source_fingerprint_skips_unreadable_file(tmp_path: Path) -> None:
    """A file that raises on ``stat`` is skipped, not fatal to the walk."""
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "a.py").write_text("a = 1\n")

    real_stat = Path.stat

    def flaky_stat(self: Path, *args: Any, **kwargs: Any) -> Any:
        if self.name == "a.py":
            raise OSError("vanished")
        return real_stat(self, *args, **kwargs)

    with patch.object(Path, "stat", flaky_stat):
        # Does not raise; the unreadable file is simply omitted.
        assert isinstance(_compute_source_fingerprint(pkg), str)


def test_build_identity_round_trips_on_entry() -> None:
    """The nested identity persists through a JSON round-trip."""
    entry = _make_entry(
        build_identity={
            "version": "1.2.3",
            "venv": "/venv/x",
            "fingerprint": "abc123",
        }
    )
    payload = json.dumps(entry.model_dump(mode="json", context={"reveal": True}))
    rebuilt = DaemonRegistryEntry.model_validate(json.loads(payload))
    assert rebuilt.build_identity == DaemonBuildIdentity(
        version="1.2.3", venv="/venv/x", fingerprint="abc123"
    )


def test_build_identity_is_required() -> None:
    """A payload missing the ``build_identity`` key fails validation."""
    defaults = _make_entry().model_dump(mode="json", context={"reveal": True})
    del defaults["build_identity"]
    with pytest.raises(ValidationError):
        DaemonRegistryEntry.model_validate(defaults)


# ---------------------------------------------------------------------------
# Redaction guarantees on DaemonRegistryEntry
# ---------------------------------------------------------------------------


def test_psk_repr_does_not_leak_plaintext() -> None:
    """``repr(entry)`` masks the PSK via SecretStr's default repr."""
    entry = _make_entry()
    assert _PSK_PLAINTEXT not in repr(entry)


def test_default_dump_does_not_leak_plaintext() -> None:
    """``model_dump`` without context masks the PSK."""
    entry = _make_entry()
    dump = entry.model_dump(mode="json")
    assert _PSK_PLAINTEXT not in json.dumps(dump)


def test_redact_dump_emits_REDACTED() -> None:
    """``context={'redact': True}`` replaces the PSK with REDACTED."""
    entry = _make_entry()
    dump = entry.model_dump(mode="json", context={"redact": True})
    assert dump["psk"] == REDACTED
    assert _PSK_PLAINTEXT not in json.dumps(dump)


def test_reveal_dump_emits_plaintext() -> None:
    """``context={'reveal': True}`` returns the PSK plaintext.

    This is the mode :class:`LockedRegistry.write` uses internally so
    the persisted ``daemon.json`` carries the value the CLI needs.
    The on-disk file is mode 0o600 inside a 0o700 directory.
    """
    entry = _make_entry()
    dump = entry.model_dump(mode="json", context={"reveal": True})
    assert dump["psk"] == _PSK_PLAINTEXT


# ---------------------------------------------------------------------------
# DaemonDirectory
# ---------------------------------------------------------------------------


def test_directory_artifact_paths_compose_from_filename_constants(
    tmp_path: Path,
) -> None:
    """The three artifact-path properties are derived from filename constants.

    Locks coverage onto :attr:`lock_path` / :attr:`log_path` so a
    refactor that drops the typed accessors (forcing callers to
    string-concatenate filenames again) fails the test rather than
    silently regressing.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    assert dd.path == tmp_path / "daemon"
    assert dd.registry_path == dd.path / _DAEMON_REGISTRY_FILENAME
    assert dd.lock_path == dd.path / _DAEMON_LOCK_FILENAME
    assert dd.log_path == dd.path / _DAEMON_LOG_FILENAME


def test_directory_for_runtime_dir_appends_daemon_subdir(tmp_path: Path) -> None:
    """``for_runtime_dir`` roots the handle at ``runtime_dir / "daemon"``.

    The convention is encoded once (via
    :func:`deephaven_mcp.config.daemon_dir`); this test asserts the
    factory composes the expected absolute path and does not touch
    disk. Pinning the call site against a literal ``runtime_dir /
    "daemon"`` so a refactor that quietly relocates the daemon
    subdirectory would also fail this test.
    """
    runtime_dir = tmp_path / "rt"
    dd = DaemonDirectory.for_runtime_dir(runtime_dir)
    assert isinstance(dd, DaemonDirectory)
    assert dd.path == runtime_dir / "daemon"
    # Path-construction only — neither the runtime nor the daemon
    # subdir is created.
    assert not runtime_dir.exists()
    assert not dd.path.exists()


def test_directory_locked_creates_lock_at_lock_path(tmp_path: Path) -> None:
    """``locked()`` returns a session that creates the lock at ``lock_path``."""
    dd = DaemonDirectory(tmp_path / "daemon")
    with dd.locked() as reg:
        assert isinstance(reg, LockedRegistry)
        assert dd.lock_path.exists()


def test_directory_starting_path_composes_from_filename_constant(
    tmp_path: Path,
) -> None:
    """``starting_path`` is derived from the marker filename constant."""
    dd = DaemonDirectory(tmp_path / "daemon")
    assert dd.starting_path == dd.path / _DAEMON_STARTING_FILENAME


def test_registry_read_returns_none_when_absent(tmp_path: Path) -> None:
    dd = DaemonDirectory(tmp_path / "daemon")
    assert dd.read_entry() is None


def test_mutators_live_on_locked_registry_not_directory(tmp_path: Path) -> None:
    """Mutation methods are reachable only through the locked session.

    The capability-object design enforces lock-holding
    *structurally*: ``DaemonDirectory`` exposes no ``write`` /
    ``delete`` / ``quarantine`` of its own, so a caller cannot
    mutate the registry without first entering ``locked()``.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    for mutator in ("write", "delete", "quarantine"):
        assert not hasattr(dd, mutator)
    with dd.locked() as reg:
        for mutator in ("read", "write", "delete", "quarantine"):
            assert callable(getattr(reg, mutator))


def test_locked_releases_lock_on_exception(tmp_path: Path) -> None:
    """``locked()``'s ``__exit__`` releases the lock even on exception.

    A second ``locked()`` acquire must succeed afterwards; if the
    lock were not released on the exception path the next acquire
    would time out.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    with pytest.raises(RuntimeError):
        with dd.locked():
            raise RuntimeError("boom")
    # Re-acquire promptly — the lock was released by ``__exit__``.
    with dd.locked() as reg:
        assert isinstance(reg, LockedRegistry)


def test_registry_write_creates_dir_and_file(tmp_path: Path) -> None:
    """``reg.write`` creates a hardened directory and writes a 0o600 file.

    ``write`` hardens the daemon directory (``0o700`` on POSIX) via
    :func:`deephaven_mcp.config.harden_private_dir` before writing, so
    a write that lazily creates the directory never leaves the
    PSK-bearing registry under a group/world-readable directory. The
    file mode (``0o600``) is the class's responsibility because it
    carries the PSK.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    entry = _make_entry()
    with dd.locked() as reg:
        reg.write(entry)
    assert dd.registry_path.is_file()
    if os.name == "posix":
        assert (dd.registry_path.stat().st_mode & 0o777) == 0o600
        assert (dd.path.stat().st_mode & 0o777) == 0o700


def test_registry_write_hardens_preexisting_loose_dir(tmp_path: Path) -> None:
    """``reg.write`` tightens a pre-existing group/world-readable directory."""
    if os.name != "posix":  # pragma: no cover - POSIX-only mode semantics
        pytest.skip("POSIX directory-mode semantics only")
    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)
    os.chmod(dd.path, 0o755)  # Looser than 0o700.
    with dd.locked() as reg:
        reg.write(_make_entry())
    assert (dd.path.stat().st_mode & 0o777) == 0o700


def test_registry_write_persists_psk_plaintext(tmp_path: Path) -> None:
    """``reg.write`` uses ``reveal`` mode so the CLI can recover the PSK.

    The file must be mode 0o600 (asserted in the test above).
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    with dd.locked() as reg:
        reg.write(_make_entry())
    on_disk = json.loads(dd.registry_path.read_text())
    assert on_disk["psk"] == _PSK_PLAINTEXT


def test_registry_read_returns_written_entry(tmp_path: Path) -> None:
    dd = DaemonDirectory(tmp_path / "daemon")
    entry = _make_entry()
    with dd.locked() as reg:
        reg.write(entry)
    assert dd.read_entry() == entry


def test_locked_read_mirrors_directory_read(tmp_path: Path) -> None:
    """``reg.read`` returns the same entry as the lock-free read."""
    dd = DaemonDirectory(tmp_path / "daemon")
    entry = _make_entry()
    with dd.locked() as reg:
        assert reg.read() is None
        reg.write(entry)
        assert reg.read() == entry


def test_registry_write_is_atomic(tmp_path: Path) -> None:
    """A failed mid-write must not leave a partial ``daemon.json``.

    The write-temp-then-rename mechanics live in
    :func:`deephaven_mcp._platform.fsutil.atomic_write_private`; this asserts
    the *integration* — a rename failure surfaced from there leaves
    the previously-published registry untouched.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    with dd.locked() as reg:
        reg.write(_make_entry(port=10001))
    before = dd.registry_path.read_text()

    # Simulate a write failure by patching the rename ``write`` relies
    # on (in ``_platform.fsutil``) to raise.
    with (
        dd.locked() as reg,
        patch("deephaven_mcp._platform.fsutil.replace_with_retry") as fake,
    ):
        fake.side_effect = OSError("disk full")
        with pytest.raises(OSError):
            reg.write(_make_entry(port=20002))

    # Original content survives; the temp file is cleaned up by
    # ``atomic_write_private`` and the registry path is untouched.
    assert dd.registry_path.read_text() == before
    assert list(dd.path.glob("daemon.json.*.tmp")) == []


def test_registry_write_delegates_to_atomic_write_private(tmp_path: Path) -> None:
    """``reg.write`` hands the serialized bytes to ``atomic_write_private``.

    The OS-level write mechanics belong to ``_platform.fsutil`` (and are
    tested there); ``write``'s own responsibility is composing the
    JSON payload and targeting the registry path.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    entry = _make_entry(port=33445)
    with (
        dd.locked() as reg,
        patch("deephaven_mcp.daemon_registry.atomic_write_private") as fake,
    ):
        reg.write(entry)
    fake.assert_called_once()
    written_path, written_bytes = fake.call_args.args
    assert written_path == dd.registry_path
    assert json.loads(written_bytes)["port"] == entry.port


# ---------------------------------------------------------------------------
# Lock-acquire timeout
# ---------------------------------------------------------------------------


def test_locked_session_uses_default_acquire_deadline(tmp_path: Path) -> None:
    """``locked()`` builds an ``AdvisoryFileLock`` with the generic deadline.

    The registry no longer derives its own timeout; the session
    inherits the bounded-acquire safety net from ``_platform.fsutil``.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    reg = dd.locked()
    assert reg._lock._timeout_seconds == _DEFAULT_LOCK_TIMEOUT_SECONDS


def test_registry_read_raises_on_malformed_json(tmp_path: Path) -> None:
    """Invalid JSON must surface as :class:`RegistryCorruptError`.

    Conflating with absent (``None``) would let the CLI's auto-spawn
    path silently re-spawn over a still-running daemon whose registry
    happened to be corrupted by some external scribbler.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)
    dd.registry_path.write_text("not json")
    with pytest.raises(RegistryCorruptError, match="Malformed daemon registry"):
        dd.read_entry()


def test_registry_read_raises_on_missing_fields(tmp_path: Path) -> None:
    """A schema-violating file (missing required fields) raises corrupt."""
    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)
    dd.registry_path.write_text(json.dumps({"pid": 1}))
    with pytest.raises(RegistryCorruptError, match="Malformed daemon registry"):
        dd.read_entry()


def test_registry_read_raises_on_unknown_extra_field(tmp_path: Path) -> None:
    """A future-version file with an unknown field raises corrupt.

    ``extra='forbid'`` on :class:`DaemonRegistryEntry` is the schema
    drift detector; the resulting :class:`pydantic.ValidationError`
    is wrapped in :class:`RegistryCorruptError` so callers cannot
    silently treat schema drift as "no daemon".
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)
    payload = _make_entry().model_dump(mode="json", context={"reveal": True})
    payload["future_field_we_dont_know_about"] = "surprise"
    dd.registry_path.write_text(json.dumps(payload))
    with pytest.raises(RegistryCorruptError, match="Malformed daemon registry"):
        dd.read_entry()


def test_registry_delete_removes_file(tmp_path: Path) -> None:
    dd = DaemonDirectory(tmp_path / "daemon")
    with dd.locked() as reg:
        reg.write(_make_entry())
        reg.delete()
    assert not dd.registry_path.exists()


def test_registry_delete_is_idempotent(tmp_path: Path) -> None:
    dd = DaemonDirectory(tmp_path / "daemon")
    # No file present.
    with dd.locked() as reg:
        reg.delete()  # must not raise
        reg.delete()


def test_registry_quarantine_renames_to_timestamped_sibling(tmp_path: Path) -> None:
    """``reg.quarantine`` renames the registry to a corrupt-suffix sibling.

    The well-known path must be free after the call (so a fresh spawn
    can publish a new entry), but the malformed bytes must remain on
    disk under the new name for operator postmortem.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)
    dd.registry_path.write_text("not json")
    with dd.locked() as reg:
        quarantined = reg.quarantine()
    assert quarantined is not None
    assert quarantined.exists()
    assert quarantined.read_text() == "not json"
    assert not dd.registry_path.exists()
    # Path layout: same parent, ``daemon.json.corrupt-{UTC ts}``.
    assert quarantined.parent == dd.path
    assert quarantined.name.startswith("daemon.json.corrupt-")
    # Timestamp shape: ``YYYYMMDDTHHMMSSZ`` (16 chars after the prefix).
    suffix = quarantined.name[len("daemon.json.corrupt-") :]
    assert len(suffix) == 16
    assert suffix.endswith("Z")
    assert suffix[8] == "T"


def test_registry_quarantine_returns_none_when_absent(tmp_path: Path) -> None:
    """``reg.quarantine`` is a no-op when no registry file is present."""
    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)
    with dd.locked() as reg:
        assert reg.quarantine() is None


def test_registry_quarantine_collision_appends_numeric_suffix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two quarantines in the same second produce distinct files.

    The implementation freezes its UTC timestamp at second precision
    so two failures within a single second would collide; the
    fallback path appends ``.1``, ``.2``, ... to keep both diagnostics.
    """
    from deephaven_mcp import daemon_registry as registry_module

    # Freeze the clock at a single instant so both calls compute the
    # same timestamp string.
    fixed = datetime(2026, 5, 27, 1, 2, 3, tzinfo=UTC)

    class _FrozenDatetime:
        @staticmethod
        def now(tz: object = None) -> datetime:  # noqa: ARG004
            return fixed

    monkeypatch.setattr(registry_module, "datetime", _FrozenDatetime)

    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)

    dd.registry_path.write_text("first")
    with dd.locked() as reg:
        first = reg.quarantine()
    assert first is not None

    dd.registry_path.write_text("second")
    with dd.locked() as reg:
        second = reg.quarantine()
    assert second is not None

    assert first != second
    assert first.read_text() == "first"
    assert second.read_text() == "second"
    # Second collision gets a ``.1`` suffix.
    assert second.name.endswith(".1")


def test_registry_quarantine_tolerates_disappearance_after_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A TOCTOU disappearance between ``exists()`` and ``os.replace`` is benign.

    Regression test: if another process removes ``daemon.json``
    between the existence check and the rename, ``os.replace``
    raises ``FileNotFoundError``. The caller's invariant (file gone
    after the call) is already satisfied, so the helper must report
    a no-op quarantine instead of crashing.
    """
    from deephaven_mcp import daemon_registry as registry_mod

    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)
    dd.registry_path.write_text("not json")

    def boom(_src: Any, _dst: Any) -> None:
        raise FileNotFoundError

    monkeypatch.setattr(registry_mod, "replace_with_retry", boom)
    with dd.locked() as reg:
        assert reg.quarantine() is None


def test_registry_read_raises_on_oserror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-``FileNotFoundError`` ``OSError`` raises corrupt, not ``None``.

    Returning ``None`` here would let a permission-denied read
    silently look like "no daemon" — dangerous if the perimeter
    chmod regressed and the file is now unreadable to the CLI but
    still readable by an attacker.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    with dd.locked() as reg:
        reg.write(_make_entry())

    original_read_text = Path.read_text

    def boom(self: Path, *args: Any, **kwargs: Any) -> str:
        if self == dd.registry_path:
            raise OSError("read failure")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", boom)
    with pytest.raises(RegistryCorruptError, match="Cannot read daemon registry"):
        dd.read_entry()


# ---------------------------------------------------------------------------
# LockedRegistry — spawn-in-progress marker (double-spawn guard)
# ---------------------------------------------------------------------------


def test_start_marker_round_trips(tmp_path: Path) -> None:
    """``write_start_marker`` then ``read_start_marker`` returns the instant."""
    dd = DaemonDirectory(tmp_path / "daemon")
    now = datetime(2026, 5, 27, 12, 0, 0, tzinfo=UTC)
    with dd.locked() as reg:
        reg.write_start_marker(now)
        assert dd.starting_path.is_file()
        assert reg.read_start_marker() == now


def test_start_marker_absent_reads_none(tmp_path: Path) -> None:
    """``read_start_marker`` returns ``None`` when no marker exists."""
    dd = DaemonDirectory(tmp_path / "daemon")
    with dd.locked() as reg:
        assert reg.read_start_marker() is None


def test_clear_start_marker_removes_file(tmp_path: Path) -> None:
    """``clear_start_marker`` removes the marker and is idempotent."""
    dd = DaemonDirectory(tmp_path / "daemon")
    now = datetime(2026, 5, 27, 12, 0, 0, tzinfo=UTC)
    with dd.locked() as reg:
        reg.write_start_marker(now)
        reg.clear_start_marker()
        assert not dd.starting_path.exists()
        # Idempotent: clearing again is a no-op.
        reg.clear_start_marker()
        assert reg.read_start_marker() is None


def test_start_marker_malformed_reads_none(tmp_path: Path) -> None:
    """A marker whose contents are not ISO-8601 is treated as absent.

    Treating a corrupt marker as absent lets the caller overwrite it
    and claim the spawn rather than wedging forever behind garbage.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)
    dd.starting_path.write_text("not-a-timestamp", encoding="utf-8")
    with dd.locked() as reg:
        assert reg.read_start_marker() is None


def test_start_marker_unreadable_reads_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-``FileNotFoundError`` ``OSError`` on read is treated as absent."""
    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)
    dd.starting_path.write_text(
        datetime(2026, 5, 27, tzinfo=UTC).isoformat(), encoding="utf-8"
    )

    original_read_text = Path.read_text

    def boom(self: Path, *args: Any, **kwargs: Any) -> str:
        if self == dd.starting_path:
            raise OSError("read failure")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", boom)
    with dd.locked() as reg:
        assert reg.read_start_marker() is None


# ---------------------------------------------------------------------------
# Cross-process lock invariants (in-process threads simulate peer processes)
# ---------------------------------------------------------------------------


def test_registry_lock_serializes_two_writers(tmp_path: Path) -> None:
    """Two threads racing under the lock have non-overlapping critical sections.

    Locks the design invariant: the ``locked()`` session is mutually
    exclusive across threads. Each thread records the time it
    entered and exited the protected region; the lock guarantees
    those intervals do not overlap.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)

    started = threading.Event()
    intervals: list[tuple[float, float]] = []
    intervals_lock = threading.Lock()

    def publish(port: int, hold_for: float) -> None:
        started.wait()
        with dd.locked() as reg:
            enter = time.monotonic()
            reg.write(_make_entry(port=port))
            time.sleep(hold_for)
            exit_ = time.monotonic()
        with intervals_lock:
            intervals.append((enter, exit_))

    t1 = threading.Thread(target=publish, args=(11111, 0.05))
    t2 = threading.Thread(target=publish, args=(22222, 0.05))
    t1.start()
    t2.start()
    started.set()
    t1.join(timeout=5)
    t2.join(timeout=5)
    assert not t1.is_alive() and not t2.is_alive()
    assert dd.read_entry() is not None
    assert len(intervals) == 2
    # Order by entry time, then assert the second interval starts
    # after the first one ended.
    intervals.sort(key=lambda iv: iv[0])
    (a_enter, a_exit), (b_enter, _b_exit) = intervals
    assert a_exit <= b_enter, (
        f"Lock failed to serialize: thread A held [{a_enter}, {a_exit}], "
        f"thread B entered at {b_enter}"
    )


def test_registry_lock_does_not_block_pure_reads(tmp_path: Path) -> None:
    """``read_entry`` is lock-free; it does not contend with a held lock.

    Held lock + pure read must complete promptly. A regression that
    forced reads to acquire the lock would deadlock or stall here.
    """
    dd = DaemonDirectory(tmp_path / "daemon")
    dd.path.mkdir(parents=True, exist_ok=True)
    with dd.locked() as reg:
        reg.write(_make_entry())

    holder_entered = threading.Event()
    holder_release = threading.Event()

    def hold_lock() -> None:
        with dd.locked():
            holder_entered.set()
            holder_release.wait(timeout=5)

    t = threading.Thread(target=hold_lock)
    t.start()
    try:
        assert holder_entered.wait(timeout=5)
        # Read must succeed promptly while another thread holds the lock.
        start = time.monotonic()
        entry = dd.read_entry()
        elapsed = time.monotonic() - start
        assert entry is not None
        assert elapsed < 1.0
    finally:
        holder_release.set()
        t.join(timeout=5)
