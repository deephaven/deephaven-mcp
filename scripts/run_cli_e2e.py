#!/usr/bin/env python3
"""End-to-end developer harness for the ``dhcli`` CLI.

Spins up a Deephaven Community worker via
:class:`PythonLaunchedSession` (or, with ``--enterprise``, expects an
operator-managed enterprise system), seeds a temporary configuration
tree pointing at it, and drives every supported ``dhcli`` subcommand
in sequence: ``daemon start`` → ``daemon status`` → ``tool list`` →
``tool call`` → idempotent ``daemon start`` → ``daemon stop`` →
idempotent ``daemon stop``.

The script intentionally lives outside the unit-test tree because:

- It launches a real Deephaven worker (slow, environment-dependent).
- Its enterprise mode requires operator-side setup that CI cannot do.
- It is the canonical "smoke test" the CI workflow runs for
  community deployments and that operators use locally for
  enterprise verification.

Usage
-----

Community (default):

    uv run python scripts/run_cli_e2e.py

Enterprise (operator-managed system already configured):

    uv run python scripts/run_cli_e2e.py \\
        --enterprise \\
        --enterprise-system-name prod \\
        --enterprise-config-dir ~/.deephaven/ai/config

Exits ``0`` when every step succeeds; non-zero with a descriptive
message otherwise.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
_LOGGER = logging.getLogger("cli-e2e")

# Generous worker-ready budget: a cold JVM launch plus Deephaven init
# can take well over a minute.
_WORKER_READY_TIMEOUT_SECONDS = 240
_WORKER_READY_INTERVAL_SECONDS = 5
_WORKER_HEAP_GB = 2
_CLI_TIMEOUT_SECONDS = 60


def _parse_args() -> argparse.Namespace:
    """Parse the CLI arguments for the harness."""
    parser = argparse.ArgumentParser(
        description="End-to-end test harness for the dhcli CLI.",
    )
    parser.add_argument(
        "--enterprise",
        action="store_true",
        help=(
            "Run against an operator-managed enterprise system instead "
            "of spawning a community worker. Requires "
            "--enterprise-config-dir and --enterprise-system-name."
        ),
    )
    parser.add_argument(
        "--enterprise-config-dir",
        type=Path,
        default=None,
        help="Existing config directory containing enterprise/systems/.",
    )
    parser.add_argument(
        "--enterprise-system-name",
        default=None,
        help="Name of the enterprise system to verify (must already exist).",
    )
    parser.add_argument(
        "--keep-runtime-dir",
        action="store_true",
        help="Skip removal of the temporary runtime directory on exit.",
    )
    return parser.parse_args()


def _run_cli(args: list[str], *, timeout: int = _CLI_TIMEOUT_SECONDS) -> str:
    """Execute ``dhcli ...`` synchronously and return its stdout.

    The subprocess inherits the parent environment. Raises
    ``RuntimeError`` on non-zero exit or timeout; the message includes
    both stdout and stderr to make CI logs actionable.
    """
    cmd = ["dhcli", *args]
    _LOGGER.info(f"$ {shlex.join(cmd)}")
    try:
        completed = subprocess.run(  # noqa: S603 - argv is fully constructed
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"`dhcli {' '.join(args)}` timed out after {timeout}s.\n"
            f"--- stdout ---\n{exc.stdout}\n"
            f"--- stderr ---\n{exc.stderr}"
        ) from exc
    if completed.returncode != 0:
        raise RuntimeError(
            f"`dhcli {' '.join(args)}` failed with exit code "
            f"{completed.returncode}.\n"
            f"--- stdout ---\n{completed.stdout}\n"
            f"--- stderr ---\n{completed.stderr}"
        )
    return completed.stdout


def _run_json(
    common: list[str], verb: list[str], *, timeout: int = _CLI_TIMEOUT_SECONDS
) -> Any:
    """Run a ``dhcli`` subcommand and parse its JSON stdout."""
    return json.loads(_run_cli([*common, *verb], timeout=timeout))


def _seed_community_config(cfg_dir: Path, *, worker_port: int, auth_token: str) -> None:
    """Write a community-only multi-system config tree under ``cfg_dir``.

    Mirrors ``_seed_config_dir`` in
    ``tests/cli/test__daemon_integration.py``; keep the two in sync when
    the on-disk config shape changes.
    """
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
    for sub in (cfg_dir, cfg_dir / "community", sessions_dir):
        os.chmod(sub, 0o700)
    os.chmod(session_path, 0o600)


def _verify_lifecycle(cfg_dir: Path, runtime_dir: Path) -> None:
    """Drive the full subcommand sequence and assert each result.

    Sequence: ``daemon start`` → ``daemon status`` → ``tool list`` →
    ``tool call`` → idempotent ``daemon start`` → ``daemon stop`` →
    idempotent ``daemon stop``. The ``--config-dir`` / ``--runtime-dir``
    flags set the paths explicitly, so no environment override is needed.
    """
    common = [
        "--config-dir",
        str(cfg_dir),
        "--runtime-dir",
        str(runtime_dir),
        "--output",
        "json",
    ]

    # Start
    start_payload = _run_json(common, ["daemon", "start"])
    start_daemon = start_payload.get("daemon") or {}
    if start_daemon.get("pid", 0) <= 0 or start_daemon.get("port", 0) <= 0:
        raise RuntimeError(f"`daemon start` returned bad payload: {start_payload}")
    _LOGGER.info(
        f"daemon started: pid={start_daemon['pid']} port={start_daemon['port']}"
    )

    # Status
    status_payload = _run_json(common, ["daemon", "status"])
    if status_payload.get("state") != "running":
        raise RuntimeError(f"`daemon status` reports not running: {status_payload}")
    status_daemon = status_payload.get("daemon") or {}
    _LOGGER.info(f"daemon status: running on port {status_daemon.get('port')}")

    # Tool list
    tools = _run_json(common, ["tool", "list"])
    if not isinstance(tools, list):
        raise RuntimeError(f"`tool list` did not return a list: {tools!r}")
    _LOGGER.info(f"daemon registered {len(tools)} application tool(s)")

    # Tool call: exercise the real MCP round-trip end-to-end.
    call_result = _run_json(common, ["tool", "call", "sessions_list"])
    if call_result.get("isError"):
        raise RuntimeError(
            f"`tool call sessions_list` reported isError: {call_result!r}"
        )
    _LOGGER.info("tool call sessions_list returned a non-error result")

    # Idempotent re-start
    second = _run_json(common, ["daemon", "start"])
    second_pid = (second.get("daemon") or {}).get("pid")
    if second_pid != start_daemon["pid"]:
        raise RuntimeError(
            f"second `daemon start` returned a different pid "
            f"({second_pid} vs {start_daemon['pid']}); spawn was not idempotent."
        )

    # Stop
    stop_payload = _run_json(common, ["daemon", "stop"])
    if not stop_payload.get("stopped"):
        raise RuntimeError(f"`daemon stop` reports nothing was stopped: {stop_payload}")
    _LOGGER.info("daemon stopped cleanly")

    # Idempotent stop
    final = _run_json(common, ["daemon", "stop"])
    if final.get("stopped"):
        raise RuntimeError(f"second `daemon stop` claimed to terminate: {final}")
    _LOGGER.info("second `daemon stop` is a no-op (as expected)")


@contextlib.contextmanager
def _runtime_dir(*, keep: bool) -> Iterator[Path]:
    """Yield a mode-0o700 temp runtime dir, preserving it when ``keep``.

    On exit, when ``keep`` is set, the directory is copied to a
    timestamped ``dhcli-e2e-runtime-<ts>`` path under the system temp
    dir and the location is logged; otherwise it is removed.
    """
    with tempfile.TemporaryDirectory(prefix="dhcli-e2e-rt-") as runtime_str:
        runtime_dir = Path(runtime_str)
        os.chmod(runtime_dir, 0o700)
        try:
            yield runtime_dir
        finally:
            if keep:
                persisted = (
                    Path(tempfile.gettempdir())
                    / f"dhcli-e2e-runtime-{int(time.time())}"
                )
                shutil.copytree(runtime_dir, persisted)
                _LOGGER.info(f"Preserved runtime dir at {persisted}")


async def _run_community(args: argparse.Namespace) -> int:
    """Spawn a community worker, seed a config dir, drive the CLI."""
    try:
        from deephaven_mcp.resource_manager import (
            PythonLaunchedSession,
            find_available_port,
        )
    except ImportError as exc:
        _LOGGER.error(f"deephaven-mcp resource manager unavailable: {exc}")
        return 2

    port = find_available_port()
    auth_token = "cli-e2e-token"
    _LOGGER.info(f"Launching community worker on port {port} ...")
    session = await PythonLaunchedSession.launch(
        session_name="cli-e2e-community",
        port=port,
        auth_token=auth_token,
        heap_size_gb=_WORKER_HEAP_GB,
        extra_jvm_args=[],
        environment_vars={},
        python_venv_path=None,
    )
    try:
        ready = await session.wait_until_ready(
            timeout_seconds=_WORKER_READY_TIMEOUT_SECONDS,
            check_interval_seconds=_WORKER_READY_INTERVAL_SECONDS,
        )
        if not ready:
            _LOGGER.error(
                f"Community worker did not become ready within "
                f"{_WORKER_READY_TIMEOUT_SECONDS}s."
            )
            return 2

        with tempfile.TemporaryDirectory(prefix="dhcli-e2e-cfg-") as cfg_str:
            cfg_dir = Path(cfg_str)
            os.chmod(cfg_dir, 0o700)
            _seed_community_config(cfg_dir, worker_port=port, auth_token=auth_token)
            with _runtime_dir(keep=args.keep_runtime_dir) as runtime_dir:
                _verify_lifecycle(cfg_dir, runtime_dir)
        _LOGGER.info("Community e2e: PASS")
        return 0
    finally:
        await session.stop()


def _run_enterprise(args: argparse.Namespace) -> int:
    """Drive the CLI against a pre-existing enterprise system."""
    if not args.enterprise_config_dir or not args.enterprise_system_name:
        _LOGGER.error(
            "--enterprise requires --enterprise-config-dir and "
            "--enterprise-system-name."
        )
        return 2
    cfg_dir = args.enterprise_config_dir.expanduser().resolve()
    if not cfg_dir.is_dir():
        _LOGGER.error(f"--enterprise-config-dir does not exist: {cfg_dir}")
        return 2
    system_path = (
        cfg_dir / "enterprise" / "systems" / f"{args.enterprise_system_name}.json"
    )
    if not system_path.is_file():
        _LOGGER.error(
            f"Enterprise system file not found: {system_path}. "
            f"Configure it before re-running."
        )
        return 2
    with _runtime_dir(keep=args.keep_runtime_dir) as runtime_dir:
        _verify_lifecycle(cfg_dir, runtime_dir)
    _LOGGER.info("Enterprise e2e: PASS")
    return 0


def main() -> int:
    """Entry point for the harness; returns the desired exit code."""
    args = _parse_args()
    if shutil.which("dhcli") is None:
        _LOGGER.error("`dhcli` is not on PATH. Install with `uv sync --all-extras`.")
        return 2
    try:
        if args.enterprise:
            return _run_enterprise(args)
        return asyncio.run(_run_community(args))
    except RuntimeError as exc:
        _LOGGER.error(str(exc))
        return 2


if __name__ == "__main__":
    sys.exit(main())
