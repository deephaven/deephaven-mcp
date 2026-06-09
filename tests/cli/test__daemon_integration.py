"""End-to-end integration tests for the ``dh-mcp`` CLI.

These tests:

- Launch a real Deephaven Community worker via
  :class:`PythonLaunchedSession` (the same fixture pattern used by
  ``tests/resource_manager/test_launcher_integration.py``).
- Seed a temporary configuration tree pointing the multiplexed
  systems server at that worker.
- Drive the ``dh-mcp`` CLI as a real subprocess, exercising the
  full daemon lifecycle: ``start`` → ``status`` → ``tool list`` →
  ``tool show`` → ``tool call`` → ``restart`` → ``logs`` → ``stop``,
  plus the error/exit-code paths and the config / introspect verbs
  that need no worker.

They are marked ``@pytest.mark.integration`` and skipped by the
default ``uv run pytest`` run. Invoke with::

    uv run pytest -s -m integration -k cli

Prerequisites:

- ``deephaven-server`` must be importable in the current environment.
- ``dh-mcp`` must be on ``$PATH`` (provided by ``uv sync``).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
import pytest_asyncio

_LOGGER = logging.getLogger(__name__)

_PORT_ALLOCATION_LOCK = threading.Lock()


def _is_deephaven_server_importable() -> bool:
    """Return ``True`` when ``deephaven-server`` is installable here."""
    try:
        import deephaven_server  # type: ignore[import-not-found] # noqa: F401
    except ImportError:
        return False
    return True


def _find_available_port_locked() -> int:
    """Thread-safe port allocation for parallel integration runs."""
    from deephaven_mcp.resource_manager import find_available_port

    with _PORT_ALLOCATION_LOCK:
        port = find_available_port()
        time.sleep(0.1)
        return port


@pytest.fixture
def community_worker_port() -> int:
    """Allocate a free port the worker fixture will bind to."""
    return _find_available_port_locked()


@pytest_asyncio.fixture
async def community_worker(community_worker_port: int):
    """Launch a Deephaven Community worker for the duration of the test.

    Yields the worker's auth token; the fixture stops the worker on
    teardown unconditionally.
    """
    from deephaven_mcp.resource_manager import PythonLaunchedSession

    auth_token = "integration-cli-token"
    session = await PythonLaunchedSession.launch(
        session_name="cli-integration-worker",
        port=community_worker_port,
        auth_token=auth_token,
        heap_size_gb=2,
        extra_jvm_args=[],
        environment_vars={},
        python_venv_path=None,
    )
    try:
        ready = await session.wait_until_ready(
            timeout_seconds=240, check_interval_seconds=5
        )
        if not ready:
            pytest.fail(
                "Community worker did not become ready within 240s; "
                "see captured stderr for diagnostics."
            )
        yield auth_token
    finally:
        await session.stop()


def _seed_config_dir(
    cfg_dir: Path,
    *,
    worker_port: int,
    auth_token: str,
) -> None:
    """Write a minimal multi-system config tree pointing at the worker."""
    cfg_dir.mkdir(parents=True, exist_ok=True)
    sessions_dir = cfg_dir / "community" / "sessions"
    sessions_dir.mkdir(parents=True, exist_ok=True)
    session_path = sessions_dir / "demo.json"
    session_path.write_text(
        json.dumps(
            {
                "host": "127.0.0.1",
                "port": worker_port,
                "auth": {
                    "credentials": {
                        "type": "psk",
                        "token": auth_token,
                    }
                },
            }
        )
    )
    # The directory-permission audit refuses anything looser than
    # 0o700/0o600.
    for sub in (cfg_dir, cfg_dir / "community", sessions_dir):
        os.chmod(sub, 0o700)
    os.chmod(session_path, 0o600)


def _run_cli(
    args: list[str],
    *,
    config_dir: Path,
    runtime_dir: Path,
    root_flags: list[str] | None = None,
    timeout: int = 60,
) -> subprocess.CompletedProcess[str]:
    """Drive the ``dh-mcp`` CLI as a subprocess.

    The runtime + config dir overrides are forwarded verbatim so each
    test gets an isolated daemon. ``root_flags`` are extra top-level
    options (e.g. ``--no-auto-start``) inserted before the subcommand,
    since click requires root flags to precede the verb.
    """
    cmd = [
        "dh-mcp",
        "--config-dir",
        str(config_dir),
        "--runtime-dir",
        str(runtime_dir),
        "--output",
        "json",
        *(root_flags or []),
        *args,
    ]
    _LOGGER.info(f"[integration:_run_cli] {' '.join(cmd)}")
    return subprocess.run(  # noqa: S603 - argv is fully constructed locally
        cmd,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )


def _structured_error(stderr: str) -> dict:
    """Extract the structured-error JSON object emitted on stderr.

    The CLI writes the ``-o json`` error payload to stderr, but the
    root logger also targets stderr; tests that read the payload run
    with ``-q`` to silence non-error logs, and this helper additionally
    slices to the brace-delimited object so any stray line cannot break
    the parse.
    """
    start = stderr.index("{")
    end = stderr.rindex("}") + 1
    return json.loads(stderr[start:end])


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.timeout(360)
@pytest.mark.skipif(
    not _is_deephaven_server_importable(),
    reason="deephaven-server not installed",
)
@pytest.mark.skipif(
    shutil.which("dh-mcp") is None,
    reason="dh-mcp entry point not on PATH",
)
async def test_full_daemon_lifecycle(
    tmp_path: Path, community_worker_port: int, community_worker: str
) -> None:
    """Exercise start → status → list → show → call → restart → logs → stop."""
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "rt"
    runtime_dir.mkdir()
    os.chmod(runtime_dir, 0o700)
    _seed_config_dir(
        cfg_dir,
        worker_port=community_worker_port,
        auth_token=community_worker,
    )

    try:
        # 1) start: spawns a daemon, prints connection metadata.
        result = _run_cli(
            ["daemon", "start"], config_dir=cfg_dir, runtime_dir=runtime_dir
        )
        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["pid"] > 0
        assert payload["host"] == "127.0.0.1"
        assert payload["port"] > 0

        # 2) status: confirm the daemon is registered and reachable.
        result = _run_cli(
            ["daemon", "status"], config_dir=cfg_dir, runtime_dir=runtime_dir
        )
        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["running"] is True
        assert payload["pid"] > 0
        assert payload["port"] > 0

        # 3) tool list: enumerate registered application tools.
        result = _run_cli(["tool", "list"], config_dir=cfg_dir, runtime_dir=runtime_dir)
        assert result.returncode == 0, result.stderr
        tools = json.loads(result.stdout)
        names = {t["name"] for t in tools}
        # Internal ``_``-prefixed tools are hidden without ``--all``.
        assert all(not n.startswith("_") for n in names)
        # ``sessions_list`` is registered unconditionally by the
        # systems server, so it is a stable target for show/call.
        assert "sessions_list" in names

        # 4) tool show: print one tool's metadata + input schema.
        result = _run_cli(
            ["tool", "show", "sessions_list"],
            config_dir=cfg_dir,
            runtime_dir=runtime_dir,
        )
        assert result.returncode == 0, result.stderr
        shown = json.loads(result.stdout)
        assert shown["name"] == "sessions_list"
        assert "inputSchema" in shown

        # 5) tool call: the real MCP round-trip to the worker.
        result = _run_cli(
            ["tool", "call", "sessions_list"],
            config_dir=cfg_dir,
            runtime_dir=runtime_dir,
        )
        assert result.returncode == 0, result.stderr
        call_payload = json.loads(result.stdout)
        assert call_payload["isError"] is False

        # 6) start is idempotent: re-running returns the same handle.
        result = _run_cli(
            ["daemon", "start"], config_dir=cfg_dir, runtime_dir=runtime_dir
        )
        assert result.returncode == 0, result.stderr
        payload2 = json.loads(result.stdout)
        assert payload2["pid"] == payload["pid"]

        # 7) restart: stop + start in one shot; reports the new handle.
        result = _run_cli(
            ["daemon", "restart"], config_dir=cfg_dir, runtime_dir=runtime_dir
        )
        assert result.returncode == 0, result.stderr
        restart_payload = json.loads(result.stdout)
        assert restart_payload["restarted"] is True
        assert restart_payload["pid"] > 0

        # 8) logs: raw daemon.log text (not JSON, even under -o json).
        result = _run_cli(
            ["daemon", "logs", "-n", "50"],
            config_dir=cfg_dir,
            runtime_dir=runtime_dir,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() != ""

    finally:
        # stop: terminate; idempotent.
        result = _run_cli(
            ["daemon", "stop"], config_dir=cfg_dir, runtime_dir=runtime_dir
        )
        assert result.returncode == 0, result.stderr
        # Run again to confirm idempotence.
        result = _run_cli(
            ["daemon", "stop"], config_dir=cfg_dir, runtime_dir=runtime_dir
        )
        assert result.returncode == 0, result.stderr


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.skipif(
    shutil.which("dh-mcp") is None,
    reason="dh-mcp entry point not on PATH",
)
def test_status_when_no_daemon(tmp_path: Path) -> None:
    """``daemon status`` against an empty runtime dir reports ``running=false``."""
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "rt"
    runtime_dir.mkdir()
    os.chmod(runtime_dir, 0o700)
    # ``daemon status`` does not auto-start, so an empty config tree is
    # fine — the runtime read happens before the multi-config load.
    cfg_dir.mkdir()
    (cfg_dir / "community").mkdir()
    (cfg_dir / "community" / "sessions").mkdir()
    sess = cfg_dir / "community" / "sessions" / "demo.json"
    sess.write_text(
        json.dumps(
            {
                "host": "127.0.0.1",
                "port": 10000,
                "auth": {"credentials": {"type": "anonymous"}},
            }
        )
    )
    for d in (cfg_dir, cfg_dir / "community", cfg_dir / "community" / "sessions"):
        os.chmod(d, 0o700)
    os.chmod(sess, 0o600)

    result = _run_cli(["daemon", "status"], config_dir=cfg_dir, runtime_dir=runtime_dir)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["running"] is False


def _seed_anonymous_config(cfg_dir: Path) -> None:
    """Write a minimal, valid community config that needs no live worker.

    Anonymous credentials let the eager config load succeed without a
    reachable Deephaven server, so the verbs that never connect
    (``config``, ``daemon`` lifecycle, ``--no-auto-start`` failures)
    can be exercised without the slow worker fixture.
    """
    sessions_dir = cfg_dir / "community" / "sessions"
    sessions_dir.mkdir(parents=True, exist_ok=True)
    session_path = sessions_dir / "demo.json"
    session_path.write_text(
        json.dumps(
            {
                "host": "127.0.0.1",
                "port": 10000,
                "auth": {"credentials": {"type": "anonymous"}},
            }
        )
    )
    for sub in (cfg_dir, cfg_dir / "community", sessions_dir):
        os.chmod(sub, 0o700)
    os.chmod(session_path, 0o600)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.timeout(360)
@pytest.mark.skipif(
    not _is_deephaven_server_importable(),
    reason="deephaven-server not installed",
)
@pytest.mark.skipif(
    shutil.which("dh-mcp") is None,
    reason="dh-mcp entry point not on PATH",
)
async def test_tool_call_error_exits_3(
    tmp_path: Path, community_worker_port: int, community_worker: str
) -> None:
    """A tool that reports an error exits 3 with ``tool_returned_error``.

    ``session_details`` requires ``session_id``; omitting it makes the
    server-side argument validation fail, which surfaces as a tool
    result with ``isError=true`` — the CLI's exit-3 contract.
    """
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "rt"
    runtime_dir.mkdir()
    os.chmod(runtime_dir, 0o700)
    _seed_config_dir(
        cfg_dir,
        worker_port=community_worker_port,
        auth_token=community_worker,
    )
    try:
        result = _run_cli(
            ["tool", "call", "session_details"],
            config_dir=cfg_dir,
            runtime_dir=runtime_dir,
            root_flags=["-q"],
        )
        assert result.returncode == 3, (result.returncode, result.stdout, result.stderr)
        error = _structured_error(result.stderr)
        assert error["error_code"] == "tool_returned_error"
    finally:
        _run_cli(["daemon", "stop"], config_dir=cfg_dir, runtime_dir=runtime_dir)


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.skipif(
    shutil.which("dh-mcp") is None,
    reason="dh-mcp entry point not on PATH",
)
def test_no_auto_start_without_daemon_exits_2(tmp_path: Path) -> None:
    """``--no-auto-start`` against a dead daemon exits 2 with ``daemon_not_running``."""
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "rt"
    runtime_dir.mkdir()
    os.chmod(runtime_dir, 0o700)
    _seed_anonymous_config(cfg_dir)

    result = _run_cli(
        ["tool", "list"],
        config_dir=cfg_dir,
        runtime_dir=runtime_dir,
        root_flags=["-q", "--no-auto-start"],
    )
    assert result.returncode == 2, (result.stdout, result.stderr)
    error = _structured_error(result.stderr)
    assert error["error_code"] == "daemon_not_running"


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.skipif(
    shutil.which("dh-mcp") is None,
    reason="dh-mcp entry point not on PATH",
)
def test_config_validate_and_show(tmp_path: Path) -> None:
    """``config validate`` and ``config show`` succeed on a valid tree."""
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "rt"
    runtime_dir.mkdir()
    os.chmod(runtime_dir, 0o700)
    _seed_anonymous_config(cfg_dir)

    result = _run_cli(
        ["config", "validate"], config_dir=cfg_dir, runtime_dir=runtime_dir
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["valid"] is True

    result = _run_cli(["config", "show"], config_dir=cfg_dir, runtime_dir=runtime_dir)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert "cli" in payload
    assert "community" in payload


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.skipif(
    shutil.which("dh-mcp") is None,
    reason="dh-mcp entry point not on PATH",
)
def test_introspect_emits_json_without_config(tmp_path: Path) -> None:
    """``introspect`` emits the command manifest without touching config.

    The config directory is deliberately left unseeded: introspect
    bypasses the eager config load, so it must still succeed.
    """
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "rt"
    runtime_dir.mkdir()
    os.chmod(runtime_dir, 0o700)

    result = _run_cli(["introspect"], config_dir=cfg_dir, runtime_dir=runtime_dir)
    assert result.returncode == 0, result.stderr
    manifest = json.loads(result.stdout)
    assert "commands" in manifest
    assert "error_codes" in manifest
    assert {"daemon", "tool", "config"} <= set(manifest["commands"])


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.skipif(
    shutil.which("dh-mcp") is None,
    reason="dh-mcp entry point not on PATH",
)
def test_daemon_reset_quarantines_corrupt_registry(tmp_path: Path) -> None:
    """``daemon reset`` quarantines an unparseable ``daemon.json``."""
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "rt"
    runtime_dir.mkdir()
    os.chmod(runtime_dir, 0o700)
    _seed_anonymous_config(cfg_dir)

    daemon_dir = runtime_dir / "daemon"
    daemon_dir.mkdir()
    os.chmod(daemon_dir, 0o700)
    registry_path = daemon_dir / "daemon.json"
    registry_path.write_text("{ this is not valid json")
    os.chmod(registry_path, 0o600)

    result = _run_cli(["daemon", "reset"], config_dir=cfg_dir, runtime_dir=runtime_dir)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["reset"] is True
    assert "quarantined_to" in payload
    # The well-known path is freed; the corrupt bytes survive under the
    # timestamped sibling for postmortem.
    assert not registry_path.exists()
