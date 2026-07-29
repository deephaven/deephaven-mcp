"""End-to-end integration tests for the ``dhcli`` CLI.

These tests:

- Launch a real Deephaven Community worker via
  :class:`PythonLaunchedSession` (the same fixture pattern used by
  ``tests/resource_manager/test__launcher_integration.py``).
- Seed a temporary configuration tree pointing the multiplexed
  systems server at that worker.
- Drive the ``dhcli`` CLI as a real subprocess, exercising the
  full daemon lifecycle: ``start`` → ``status`` → ``tool list`` →
  ``tool show`` → ``tool call`` → ``restart`` → ``logs`` → ``stop``,
  the runtime tool-wrapper verbs against the live worker (``session
  list`` / ``show`` / ``credentials`` / ``url`` / ``open``, ``system
  list`` / ``status``, ``table list`` / ``schema`` / ``data``, ``script
  run`` / ``pip-list``), plus the error/exit-code paths (``config_invalid``,
  ``daemon_not_running``, ``tool_returned_error``) and the config /
  agents verbs that need no worker.

They are marked ``@pytest.mark.integration`` and skipped by the
default ``uv run pytest`` run. Invoke with::

    uv run pytest -s -m integration -k cli

Prerequisites:

- ``deephaven-server`` must be importable in the current environment.
- ``dhcli`` must be on ``$PATH`` (provided by ``uv sync``).

The offline ``config`` authoring verbs (``get``/``set``/``unset``/
``keys``/``files``, ``session``/``system`` entity verbs, ``init``/
``edit``) are covered subprocess-level by the worker-free sibling
``_commands/test_config_integration.py``; this file only touches
``config validate``/``show`` where the daemon flows need them.
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


def _write_community_settings(cfg_dir: Path, *, credential_retrieval_mode: str) -> None:
    """Write ``community/settings.json`` enabling credential retrieval.

    The default config gates credential retrieval off (``mode='none'``),
    so the e2e test that exercises ``session credentials`` / ``url`` /
    ``open`` against the static session must turn it on here.
    """
    settings = {"security": {"credential_retrieval_mode": credential_retrieval_mode}}
    community_dir = cfg_dir / "community"
    community_dir.mkdir(parents=True, exist_ok=True)
    settings_path = community_dir / "settings.json"
    settings_path.write_text(json.dumps(settings))
    os.chmod(community_dir, 0o700)
    os.chmod(settings_path, 0o600)


def _write_session_startup_timeout(
    cfg_dir: Path, *, startup_timeout_seconds: int
) -> None:
    """Raise ``session_creation.defaults.startup_timeout_seconds``.

    A daemon-launched Python worker can take longer than the 60s default to
    become ready (cold imports), so the create/delete round-trip test bumps
    the startup budget to match the worker fixture's tolerance.
    """
    settings = {
        "session_creation": {
            "defaults": {"startup_timeout_seconds": startup_timeout_seconds}
        }
    }
    community_dir = cfg_dir / "community"
    community_dir.mkdir(parents=True, exist_ok=True)
    settings_path = community_dir / "settings.json"
    settings_path.write_text(json.dumps(settings))
    os.chmod(community_dir, 0o700)
    os.chmod(settings_path, 0o600)


def _run_cli(
    args: list[str],
    *,
    config_dir: Path,
    runtime_dir: Path,
    root_flags: list[str] | None = None,
    timeout: int = 60,
) -> subprocess.CompletedProcess[str]:
    """Drive the ``dhcli`` CLI as a subprocess.

    The runtime + config dir overrides are forwarded verbatim so each
    test gets an isolated daemon. ``root_flags`` are extra top-level
    options (e.g. ``--no-auto-start``) inserted before the subcommand,
    since click requires root flags to precede the verb.
    """
    cmd = [
        "dhcli",
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
    shutil.which("dhcli") is None,
    reason="dhcli entry point not on PATH",
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
        assert payload["state"] == "running"
        assert payload["daemon"]["pid"] > 0
        assert payload["daemon"]["host"] == "127.0.0.1"
        assert payload["daemon"]["port"] > 0
        assert payload["paths"]["config"]

        # 2) status: confirm the daemon is registered and reachable.
        result = _run_cli(
            ["daemon", "status"], config_dir=cfg_dir, runtime_dir=runtime_dir
        )
        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["state"] == "running"
        assert payload["daemon"]["pid"] > 0
        assert payload["daemon"]["port"] > 0

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
        assert payload2["daemon"]["pid"] == payload["daemon"]["pid"]

        # 7) restart: stop + start in one shot; reports the new handle.
        result = _run_cli(
            ["daemon", "restart"], config_dir=cfg_dir, runtime_dir=runtime_dir
        )
        assert result.returncode == 0, result.stderr
        restart_payload = json.loads(result.stdout)
        assert restart_payload["state"] == "running"
        assert restart_payload["daemon"]["pid"] > 0

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
    shutil.which("dhcli") is None,
    reason="dhcli entry point not on PATH",
)
def test_status_when_no_daemon(tmp_path: Path) -> None:
    """``daemon status`` against an empty runtime dir reports ``state=stopped``."""
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
    assert payload["state"] == "stopped"


def _seed_anonymous_config(cfg_dir: Path) -> None:
    """Write a minimal, valid community config that needs no live worker.

    Anonymous credentials let the pre-body config load succeed without a
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
    shutil.which("dhcli") is None,
    reason="dhcli entry point not on PATH",
)
async def test_tool_call_error_exits_3(
    tmp_path: Path, community_worker_port: int, community_worker: str
) -> None:
    """A tool that reports an error exits 3 with ``tool_returned_error``.

    ``session_details`` requires ``id``; omitting it makes the
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
    shutil.which("dhcli") is None,
    reason="dhcli entry point not on PATH",
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
    shutil.which("dhcli") is None,
    reason="dhcli entry point not on PATH",
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
    shutil.which("dhcli") is None,
    reason="dhcli entry point not on PATH",
)
def test_root_options_accepted_after_subcommand(tmp_path: Path) -> None:
    """End-to-end through the real ``dhcli`` binary: every root option is
    accepted at any position on the command line.

    ``_lift_root_options`` (``cli/_main.py``) is exhaustively unit-tested
    against the lifter's contract, and ``test__main.py`` exercises the
    full ``main()`` entry point in-process. This test closes the loop at
    the OS-subprocess level, where Python is started fresh by the shell
    and ``sys.argv`` is populated by the OS — the path that matters in
    production for any operator typing ``dhcli config show -o json``.
    """
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "rt"
    runtime_dir.mkdir()
    os.chmod(runtime_dir, 0o700)
    _seed_anonymous_config(cfg_dir)

    # All root options placed *after* the subcommand. Without
    # ``_lift_root_options`` click would fail with ``No such option
    # '--config-dir'`` at the first one.
    cmd = [
        "dhcli",
        "config",
        "show",
        "--config-dir",
        str(cfg_dir),
        "--runtime-dir",
        str(runtime_dir),
        "--output",
        "json",
    ]
    result = subprocess.run(  # noqa: S603 - argv is fully constructed locally
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert "cli" in payload
    assert "community" in payload

    # Long ``--opt=value`` form at the tail end is equivalent to the
    # bare ``--opt value`` form. Click does not accept ``-o=value``
    # for short options (it parses ``-oVALUE`` with no separator, so
    # ``-o=yaml`` would become ``-o`` + value ``=yaml`` — broken
    # regardless of position), so only the long ``=`` form is asserted.
    cmd_equals = [
        "dhcli",
        "config",
        "show",
        f"--config-dir={cfg_dir}",
        f"--runtime-dir={runtime_dir}",
        "--output=yaml",
    ]
    result = subprocess.run(  # noqa: S603 - argv is fully constructed locally
        cmd_equals,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    # YAML mode: line-oriented output, not JSON.
    assert "cli:" in result.stdout
    assert "community:" in result.stdout


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.skipif(
    shutil.which("dhcli") is None,
    reason="dhcli entry point not on PATH",
)
def test_help_at_depth_still_routes_to_subcommand(tmp_path: Path) -> None:
    """``--help`` is deliberately not lifted: ``dhcli daemon --help``
    must render the daemon group's help, not the root group's, even
    though ``daemon`` precedes the flag on the command line.

    Pins the exclusion-from-lift contract end-to-end against the real
    binary so any future regression that starts lifting ``--help``
    surfaces here as a test failure rather than as a confusing
    user-facing behavior change.
    """
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "rt"
    runtime_dir.mkdir()
    os.chmod(runtime_dir, 0o700)
    _seed_anonymous_config(cfg_dir)

    # ``--help`` does not require a daemon, and the help fast-path
    # short-circuits before runtime loading; ``--config-dir`` is
    # supplied only because ``_run_cli`` is not in play and the root
    # callback expects the standard arg order.
    cmd = [
        "dhcli",
        "--config-dir",
        str(cfg_dir),
        "--runtime-dir",
        str(runtime_dir),
        "daemon",
        "--help",
    ]
    result = subprocess.run(  # noqa: S603 - argv is fully constructed locally
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    # Daemon group help (lists daemon verbs); not root help (which
    # lists top-level nouns like 'tool', 'session', etc.).
    assert "Manage the local dhcli daemon" in result.stdout
    assert "repair" in result.stdout


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.skipif(
    shutil.which("dhcli") is None,
    reason="dhcli entry point not on PATH",
)
def test_config_invalid_exits_2(tmp_path: Path) -> None:
    """A malformed config tree fails fast with ``config_invalid`` (exit 2).

    The pre-body load (``HelpfulCommand.invoke``) parses the whole config
    tree before any subcommand body runs, so a syntactically broken session
    file exits 2 with ``config_invalid`` even for a worker-free verb like
    ``config validate``.
    """
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "rt"
    runtime_dir.mkdir()
    os.chmod(runtime_dir, 0o700)
    sessions_dir = cfg_dir / "community" / "sessions"
    sessions_dir.mkdir(parents=True, exist_ok=True)
    session_path = sessions_dir / "demo.json"
    session_path.write_text("{ this is not valid json")
    for sub in (cfg_dir, cfg_dir / "community", sessions_dir):
        os.chmod(sub, 0o700)
    os.chmod(session_path, 0o600)

    result = _run_cli(
        ["config", "validate"],
        config_dir=cfg_dir,
        runtime_dir=runtime_dir,
        root_flags=["-q"],
    )
    assert result.returncode == 2, (result.stdout, result.stderr)
    error = _structured_error(result.stderr)
    assert error["error_code"] == "config_invalid"


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.skipif(
    shutil.which("dhcli") is None,
    reason="dhcli entry point not on PATH",
)
def test_agents_emits_json_without_config(tmp_path: Path) -> None:
    """``agents tree --full`` emits the complete manifest without touching config.

    The config directory is deliberately left unseeded: the agents verbs
    are ``needs_runtime=False`` and never load config, so this must still
    succeed. ``--full`` is required for the whole-manifest keys asserted
    below (``error_codes``, per-node ``wraps``); the default ``tree``
    output is the summary tree, which carries neither.
    """
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "rt"
    runtime_dir.mkdir()
    os.chmod(runtime_dir, 0o700)

    result = _run_cli(
        ["-o", "json", "agents", "tree", "--full"],
        config_dir=cfg_dir,
        runtime_dir=runtime_dir,
    )
    assert result.returncode == 0, result.stderr
    manifest = json.loads(result.stdout)
    assert "commands" in manifest
    assert "error_codes" in manifest
    assert {
        "daemon",
        "tool",
        "config",
        "session",
        "system",
        "table",
        "catalog",
        "pq",
    } <= set(manifest["commands"])
    # Wrapper commands carry their tool binding for the drift tooling.
    assert manifest["commands"]["system"]["subcommands"]["list"]["wraps"]["tools"] == [
        "list_systems"
    ]


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.skipif(
    shutil.which("dhcli") is None,
    reason="dhcli entry point not on PATH",
)
def test_daemon_repair_quarantines_corrupt_registry(tmp_path: Path) -> None:
    """``daemon repair`` moves aside an unparseable ``daemon.json``."""
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

    result = _run_cli(["daemon", "repair"], config_dir=cfg_dir, runtime_dir=runtime_dir)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["repaired"] is True
    assert "quarantined_to" in payload
    # The well-known path is freed; the corrupt bytes survive under the
    # timestamped sibling for postmortem.
    assert not registry_path.exists()


_DEMO_ID = "community:community:demo"


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.timeout(360)
@pytest.mark.skipif(
    not _is_deephaven_server_importable(),
    reason="deephaven-server not installed",
)
@pytest.mark.skipif(
    shutil.which("dhcli") is None,
    reason="dhcli entry point not on PATH",
)
async def test_session_wrapper_verbs_e2e(
    tmp_path: Path, community_worker_port: int, community_worker: str
) -> None:
    """Exercise the read/credential wrapper verbs against the static session.

    Covers ``session list`` / ``show`` / ``credentials`` / ``url`` /
    ``open --print``, ``system list``, ``table list`` / ``schema``, and
    ``session exec`` / ``pip-list`` end-to-end against the live worker. The
    seeded ``demo`` session is static, so credential retrieval is enabled
    with ``credential_retrieval_mode='all'``. Dynamic ``create`` /
    ``delete`` are covered separately (they launch their own worker).
    """
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "rt"
    runtime_dir.mkdir()
    os.chmod(runtime_dir, 0o700)
    _seed_config_dir(
        cfg_dir, worker_port=community_worker_port, auth_token=community_worker
    )
    _write_community_settings(cfg_dir, credential_retrieval_mode="all")

    def run(args: list[str]) -> subprocess.CompletedProcess[str]:
        return _run_cli(args, config_dir=cfg_dir, runtime_dir=runtime_dir)

    try:
        assert run(["daemon", "start"]).returncode == 0

        # system list → the community umbrella is present because the seeded
        # community section has a static session (a settings-only section
        # would be omitted; see ConfigTree.list_systems).
        result = run(["system", "list"])
        assert result.returncode == 0, result.stderr
        systems = json.loads(result.stdout)
        assert {"name": "community", "type": "community"} in systems

        # system status → Enterprise-only health; all-community reports none.
        # The verb emits the `systems` field unwrapped (see
        # `call_and_echo_field` with `field="systems"`), so the payload is
        # the list itself, not `{"systems": [...]}`.
        result = run(["system", "status"])
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == []

        # session list → the seeded static session is discoverable.
        result = run(["session", "list"])
        assert result.returncode == 0, result.stderr
        ids = {s["id"] for s in json.loads(result.stdout)}
        assert _DEMO_ID in ids

        # session show → detail object for the static session.
        result = run(["session", "show", _DEMO_ID])
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["id"] == _DEMO_ID

        # table list → empty array on a fresh worker (success, not error).
        result = run(["table", "list", _DEMO_ID])
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == []

        # session exec → create a table with one column in the worker.
        result = run(
            [
                "session",
                "exec",
                _DEMO_ID,
                "--script",
                'from deephaven import empty_table\nt = empty_table(5).update(["X = i"])',
            ]
        )
        assert result.returncode == 0, result.stderr

        # table list again → the created table is now present.
        result = run(["table", "list", _DEMO_ID])
        assert result.returncode == 0, result.stderr
        assert "t" in json.loads(result.stdout)

        # table schema → column definitions for the new table (column X present).
        result = run(["table", "schema", _DEMO_ID, "t"])
        assert result.returncode == 0, result.stderr
        assert "X" in result.stdout

        # table data → real row-fetch round-trip: 5 rows of X = 0..4.
        result = run(["table", "data", _DEMO_ID, "t"])
        assert result.returncode == 0, result.stderr
        data = json.loads(result.stdout)
        assert data["table_name"] == "t"
        assert data["row_count"] == 5

        # session pip-list → packages available in the worker.
        result = run(["session", "pip-list", _DEMO_ID])
        assert result.returncode == 0, result.stderr
        assert isinstance(json.loads(result.stdout), list)

        # session credentials → plaintext token (gate enabled above).
        result = run(["session", "credentials", _DEMO_ID])
        assert result.returncode == 0, result.stderr
        creds = json.loads(result.stdout)
        assert creds["auth_token"] == community_worker
        assert "connection_url_with_auth" in creds

        # session url → just the authenticated URL.
        result = run(["session", "url", _DEMO_ID])
        assert result.returncode == 0, result.stderr
        assert community_worker in result.stdout

        # session open --print → prints the URL without launching a browser.
        result = run(["session", "open", _DEMO_ID, "--print"])
        assert result.returncode == 0, result.stderr
        opened = json.loads(result.stdout)
        assert opened["launched"] is False
        assert community_worker in opened["opened"]
    finally:
        run(["daemon", "stop"])


@pytest.mark.integration
@pytest.mark.timeout(420)
@pytest.mark.skipif(
    not _is_deephaven_server_importable(),
    reason="deephaven-server not installed",
)
@pytest.mark.skipif(
    shutil.which("dhcli") is None,
    reason="dhcli entry point not on PATH",
)
def test_session_create_delete_roundtrip_e2e(tmp_path: Path) -> None:
    """``session create`` then a *separate* ``session delete`` round-trips.

    The CLI drives a long-lived daemon, so a dynamically-created community
    worker persists in the daemon's registry across CLI invocations: a later
    ``delete`` from a different connection finds and reaps it. This exercises
    the daemon-launched-worker path end to end — the daemon, not the test,
    launches the worker.
    """
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "rt"
    runtime_dir.mkdir()
    os.chmod(runtime_dir, 0o700)
    _seed_anonymous_config(cfg_dir)
    _write_session_startup_timeout(cfg_dir, startup_timeout_seconds=240)

    name = "itest-dynamic"
    sid = f"community:community:{name}"

    def run(
        args: list[str], *, timeout: int = 60, root_flags: list[str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        return _run_cli(
            args,
            config_dir=cfg_dir,
            runtime_dir=runtime_dir,
            timeout=timeout,
            root_flags=root_flags,
        )

    try:
        assert run(["daemon", "start"]).returncode == 0

        # create → the daemon launches a fresh Python worker (slow); a long
        # request timeout keeps the MCP call open until the worker is ready.
        result = run(
            ["session", "create", name, "--launch-method", "python"],
            timeout=320,
            root_flags=["--timeout", "300"],
        )
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["id"] == sid

        # list → a *separate* invocation sees the persisted dynamic session.
        result = run(["session", "list"])
        assert result.returncode == 0, result.stderr
        assert sid in {s["id"] for s in json.loads(result.stdout)}

        # delete → a *separate* invocation finds and reaps the worker.
        result = run(["session", "delete", sid])
        assert result.returncode == 0, result.stderr

        # list again → the dynamic session is gone.
        result = run(["session", "list"])
        assert result.returncode == 0, result.stderr
        assert sid not in {s["id"] for s in json.loads(result.stdout)}
    finally:
        # Best-effort reap if an assertion failed after create, then stop the
        # daemon (graceful SIGTERM closes the registry and any live worker).
        run(["session", "delete", sid])
        run(["daemon", "stop"])
