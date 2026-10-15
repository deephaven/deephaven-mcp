#!/usr/bin/env python3
"""Check the PQ lifecycle ``wait`` fix against a real Deephaven Enterprise server.

No virtualenv needed: this script uses only the Python standard library, and it
launches ``dh-mcp-systems-server`` from this checkout with ``uv tool run`` (an
isolated, cached environment). ``PYTHONPATH`` points the server at this
checkout's ``src/``, so it always runs the code you have checked out.

It creates two temporary PQs (one healthy, one whose script raises), runs the
checks below through the MCP tools, deletes the PQs, and prints PASS or FAIL.

    1. restart wait=false is accepted every time (no DEADLINE_EXCEEDED) and the
       restart takes effect.
    2. restart wait=true returns the new run as RUNNING.
    3. restart wait=true of a failing script returns FAILED, not a timeout.
    4. stop wait=true returns STOPPED.
    5. start wait=true returns RUNNING.

Usage:
    python3 scripts/pq_wait_test.py --url https://dhe.example.com:8123 --username alice
    python3 scripts/pq_wait_test.py --url https://dhe.example.com:8123 --key-file ~/priv.base64.txt
    python3 scripts/pq_wait_test.py --system prod      # a system in your existing config

Credentials: the password is read from $DH_PQ_WAIT_TEST_PASSWORD or prompted
for. It never goes on a command line or into a file: the throwaway config
references ${env:DH_PQ_WAIT_TEST_PASSWORD}, and only the server process's
environment holds the value. A --key-file is referenced as ${file:PATH}, not
copied. The throwaway config lives in a private temp dir removed on exit.

Exit codes: 0 PASS, 1 FAIL, 2 could not run.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import queue
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import IO, Any

_REPO = Path(__file__).resolve().parent.parent
_LOGIN_ENV = "DH_PQ_WAIT_TEST_PASSWORD"

_STARTED = {"RUNNING", "EXECUTING"}
_FAILED = {"FAILED", "ERROR"}

_GOOD_SCRIPT = "pq_wait_test_value = 1\n"
_BAD_SCRIPT = 'raise Exception("pq_wait_test: intentional startup failure")\n'

_SETTLE_SECONDS = 300
"""How long to poll pq_details for a PQ to reach a state."""

_CALL_SECONDS = 900
"""Ceiling on one MCP request; the first server start may build its environment."""


class _Abort(Exception):
    """The test cannot continue."""


class McpServer:
    """Minimal MCP client over stdio (newline-delimited JSON-RPC)."""

    def __init__(self, cmd: list[str], env: dict[str, str], log: IO[str] | None):
        """Start the server process; its stderr goes to ``log`` (None = inherit)."""
        self._proc = subprocess.Popen(  # noqa: S603 - the command is the user's own
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            env=env,
            cwd=tempfile.gettempdir(),
            text=True,
        )
        self._lines: queue.Queue[str | None] = queue.Queue()
        self._next_id = 0
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self) -> None:
        for line in self._proc.stdout or ():
            self._lines.put(line)
        self._lines.put(None)

    def _send(self, message: dict[str, Any]) -> None:
        if self._proc.stdin is None or self._proc.poll() is not None:
            raise _Abort("the server exited")
        self._proc.stdin.write(json.dumps(message) + "\n")
        self._proc.stdin.flush()

    def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        self._next_id += 1
        request_id = self._next_id
        self._send(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        )
        deadline = time.monotonic() + _CALL_SECONDS
        while True:
            try:
                line = self._lines.get(timeout=max(0.0, deadline - time.monotonic()))
            except queue.Empty:
                raise _Abort(f"no reply to {method} within {_CALL_SECONDS}s") from None
            if line is None:
                raise _Abort("the server exited")
            try:
                message = json.loads(line)
            except ValueError:
                continue
            if "method" in message:
                if "id" in message:  # a server-to-client request such as ping
                    self._send({"jsonrpc": "2.0", "id": message["id"], "result": {}})
                continue
            if message.get("id") == request_id:
                if "error" in message:
                    raise _Abort(f"{method}: {message['error'].get('message')}")
                return dict(message["result"])

    def initialize(self) -> None:
        """Perform the MCP handshake."""
        self._request(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "pq_wait_test", "version": "1"},
            },
        )
        self._send({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def call(self, tool: str, **arguments: Any) -> dict[str, Any]:
        """Call a tool and return its payload dict."""
        result = self._request("tools/call", {"name": tool, "arguments": arguments})
        if isinstance(result.get("structuredContent"), dict):
            return dict(result["structuredContent"])
        for block in result.get("content", []):
            if block.get("type") == "text":
                try:
                    payload = json.loads(block["text"])
                except ValueError:
                    return {"success": False, "error": block["text"]}
                if isinstance(payload, dict):
                    return payload
        return {"success": False, "error": str(result)}

    def close(self) -> None:
        """Stop the server process."""
        try:
            if self._proc.stdin:
                self._proc.stdin.close()
            self._proc.wait(timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            self._proc.kill()


class Tester:
    """Runs the checks and records their outcome."""

    def __init__(self, server: McpServer, verbose: bool):
        """Wrap a started, initialized server."""
        self.server = server
        self.verbose = verbose
        self.results: list[tuple[bool, str]] = []

    def call(self, tool: str, **arguments: Any) -> dict[str, Any]:
        """Call a tool, echoing it when verbose."""
        payload = self.server.call(tool, **arguments)
        if self.verbose:
            print(f"    {tool}({arguments}) -> {json.dumps(payload, default=str)}")
        return payload

    def lifecycle(self, tool: str, pq_id: str, wait: bool) -> tuple[dict, float]:
        """Call pq_start/pq_stop/pq_restart for one id; return (item, seconds)."""
        started = time.monotonic()
        payload = self.call(tool, id=pq_id, wait=wait)
        elapsed = time.monotonic() - started
        if not payload.get("success"):
            return {"success": False, "error": payload.get("error")}, elapsed
        return dict(payload["results"][0]), elapsed

    def state(self, pq_id: str) -> dict[str, Any]:
        """Return pq_details' state_details."""
        payload = self.call("pq_details", id=pq_id)
        if not payload.get("success"):
            raise _Abort(f"pq_details failed: {payload.get('error')}")
        return dict(payload.get("state_details") or {})

    def wait_for(
        self, pq_id: str, done: Callable[[dict[str, Any]], bool], what: str
    ) -> dict[str, Any]:
        """Poll pq_details until ``done`` holds; raise on timeout."""
        deadline = time.monotonic() + _SETTLE_SECONDS
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            last = self.state(pq_id)
            if done(last):
                return last
            time.sleep(2)
        raise _CheckFailed(
            f"PQ never reached {what} within {_SETTLE_SECONDS}s "
            f"(last state {last.get('status')})"
        )

    def run_check(self, name: str, check: Callable[[], str]) -> None:
        """Run one check and print a single PASS/FAIL line for it."""
        if sys.stdout.isatty() and not self.verbose:
            print(f"[ .. ] {name}", end="\r", flush=True)
        try:
            detail = check()
            ok = True
        except _CheckFailed as exc:
            detail = str(exc)
            ok = False
        self.results.append((ok, name))
        mark = "PASS" if ok else "FAIL"
        print(f"[{mark}] {name} -- {detail}".ljust(79), flush=True)


class _CheckFailed(Exception):
    """One check failed; the run continues."""


def _expect(ok: bool, reason: str) -> None:
    if not ok:
        raise _CheckFailed(reason)


def _outcome(item: dict[str, Any], elapsed: float) -> str:
    if item.get("success"):
        return f"state {item.get('state')} in {elapsed:.0f}s"
    return f"error after {elapsed:.0f}s: {item.get('error')}"


def check_restart_no_wait(t: Tester, pq_id: str, repeats: int) -> str:
    """Check 1: restart wait=false is accepted every time and takes effect."""
    slowest = 0.0
    for i in range(1, repeats + 1):
        before = t.wait_for(pq_id, lambda s: s.get("status") in _STARTED, "RUNNING")
        item, elapsed = t.lifecycle("pq_restart", pq_id, wait=False)
        slowest = max(slowest, elapsed)
        _expect(item.get("success") is True, f"attempt {i}: {_outcome(item, elapsed)}")
        init = before.get("initialization_start_nanos")
        t.wait_for(
            pq_id,
            lambda s: s.get("status") in _STARTED
            and s.get("initialization_start_nanos") != init,  # noqa: B023
            f"a new RUNNING run after attempt {i}",
        )
    return f"{repeats}/{repeats} accepted (slowest {slowest:.1f}s), all took effect"


def check_restart_wait(t: Tester, pq_id: str) -> str:
    """Check 2: restart wait=true returns the new run as RUNNING."""
    before = t.wait_for(pq_id, lambda s: s.get("status") in _STARTED, "RUNNING")
    item, elapsed = t.lifecycle("pq_restart", pq_id, wait=True)
    _expect(item.get("state_category") == "ACTIVE", _outcome(item, elapsed))
    after = t.state(pq_id)
    _expect(
        after.get("initialization_start_nanos")
        != before.get("initialization_start_nanos"),
        "returned the old run, not a new one",
    )
    return _outcome(item, elapsed)


def check_failing_restart(t: Tester, pq_id: str) -> str:
    """Check 3: restart wait=true of a failing script returns FAILED."""
    t.wait_for(pq_id, lambda s: s.get("status") in _FAILED, "its first failure")
    item, elapsed = t.lifecycle("pq_restart", pq_id, wait=True)
    _expect(
        item.get("success") is True and item.get("state") in _FAILED,
        _outcome(item, elapsed),
    )
    return _outcome(item, elapsed)


def check_stop_wait(t: Tester, pq_id: str) -> str:
    """Check 4: stop wait=true returns STOPPED."""
    item, elapsed = t.lifecycle("pq_stop", pq_id, wait=True)
    _expect(item.get("state") == "STOPPED", _outcome(item, elapsed))
    return _outcome(item, elapsed)


def check_start_wait(t: Tester, pq_id: str) -> str:
    """Check 5: start wait=true returns RUNNING."""
    item, elapsed = t.lifecycle("pq_start", pq_id, wait=True)
    _expect(item.get("state_category") == "ACTIVE", _outcome(item, elapsed))
    return _outcome(item, elapsed)


def run_checks(t: Tester, args: argparse.Namespace) -> None:
    """Create the PQs, run every check, and delete the PQs."""
    suffix = uuid.uuid4().hex[:8]
    created: list[str] = []
    try:
        for kind, script in (("good", _GOOD_SCRIPT), ("bad", _BAD_SCRIPT)):
            payload = t.call(
                "pq_create",
                system=args.system,
                pq_name=f"pq-wait-test-{kind}-{suffix}",
                heap_size_gb=args.heap_gb,
                script_body=script,
            )
            if not payload.get("success"):
                raise _Abort(f"could not create a test PQ: {payload.get('error')}")
            created.append(str(payload["id"]))
        good, bad = created
        print(f"Created temporary PQs {good} and {bad}.\n")

        t.run_check(
            f"restart wait=false x{args.repeats}",
            lambda: check_restart_no_wait(t, good, args.repeats),
        )
        t.run_check("restart wait=true", lambda: check_restart_wait(t, good))
        t.run_check(
            "restart wait=true, failing script", lambda: check_failing_restart(t, bad)
        )
        t.run_check("stop wait=true", lambda: check_stop_wait(t, good))
        t.run_check("start wait=true", lambda: check_start_wait(t, good))
    finally:
        if created and not args.keep:
            payload = t.call("pq_delete", id=created)
            deleted = {
                r.get("id") for r in payload.get("results", []) if r.get("success")
            }
            left = [pid for pid in created if pid not in deleted]
            if left:
                t.results.append((False, "cleanup"))
                reason = payload.get("error") or payload.get("message")
                print(f"\n[FAIL] cleanup -- not deleted: {', '.join(left)} ({reason})")
            else:
                print("\nDeleted the temporary PQs.")
        elif created:
            print(f"\nKept the temporary PQs: {', '.join(created)}")


def parse_args() -> argparse.Namespace:
    """Parse the command line (see the module docstring)."""
    parser = argparse.ArgumentParser(
        description="Check the PQ lifecycle wait fix against a real DHE server."
    )
    parser.add_argument("--url", help="Enterprise server URL (or connection.json URL)")
    parser.add_argument("--username", help="password login (password is prompted)")
    parser.add_argument("--key-file", help="private-key login instead of a password")
    parser.add_argument(
        "--system", help="use this system from your existing config instead of --url"
    )
    parser.add_argument("--config-dir", help="config dir for --system")
    parser.add_argument(
        "--server-cmd",
        help="server command (default: uv tool run of this checkout)",
    )
    parser.add_argument(
        "--python", default="3.12", help="Python for uv tool run (default: 3.12)"
    )
    parser.add_argument("--repeats", type=int, default=5, help="default: 5")
    parser.add_argument("--heap-gb", type=float, default=1.0, help="default: 1")
    parser.add_argument("--keep", action="store_true", help="keep the test PQs")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="show tool calls and server log"
    )
    args = parser.parse_args()
    if args.url:
        if args.config_dir:
            parser.error("--config-dir cannot be combined with --url")
        if bool(args.username) == bool(args.key_file):
            parser.error("--url needs exactly one of --username or --key-file")
        args.system = args.system or "pqwait"
    elif not args.system:
        parser.error("pass --url (with --username or --key-file) or --system")
    if args.config_dir:
        # The server runs from a temp dir, so a relative path would resolve wrongly.
        args.config_dir = str(Path(args.config_dir).expanduser().resolve())
    return args


def _mkdir_private(path: Path) -> None:
    path.mkdir(mode=0o700)
    path.chmod(0o700)


def _write_config(args: argparse.Namespace, root: Path) -> dict[str, str]:
    """Write a one-system config tree under ``root``; return env vars for the server."""
    env = {"DH_AI_DATA_DIR": str(root)}
    if args.key_file:
        key_path = Path(args.key_file).expanduser().resolve()
        if not key_path.is_file():
            raise _Abort(f"--key-file {key_path} does not exist")
        credentials = {"type": "private_key", "key_text": f"${{file:{key_path}}}"}
    else:
        env[_LOGIN_ENV] = os.environ.get(_LOGIN_ENV) or getpass.getpass(
            f"Password for {args.username}: "
        )
        credentials = {
            "type": "password",
            "username": args.username,
            "password": f"${{env:{_LOGIN_ENV}}}",
        }
    url = args.url
    if not url.endswith(".json"):
        url = url.rstrip("/") + "/iris/connection.json"
    args.url = url

    directory = root / "config"
    _mkdir_private(directory)
    for part in ("enterprise", "systems"):
        directory /= part
        _mkdir_private(directory)
    fd = os.open(
        directory / f"{args.system}.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
    )
    with os.fdopen(fd, "w") as f:
        json.dump({"connection_json_url": url, "auth": {"credentials": credentials}}, f)
    return env


def _server_command(args: argparse.Namespace) -> list[str]:
    if args.server_cmd:
        cmd = shlex.split(args.server_cmd)
    else:
        if shutil.which("uv") is None:
            raise _Abort("uv is not on PATH; install it or pass --server-cmd")
        cmd = ["uv", "tool", "run", "--python", args.python, "--from", str(_REPO)]
        cmd += ["dh-mcp-systems-server", "--transport", "stdio"]
    if args.config_dir:
        cmd += ["--config-dir", args.config_dir]
    return cmd


def main() -> int:
    """Run the test and return the exit code."""
    args = parse_args()
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (str(_REPO / "src"), env.get("PYTHONPATH")) if p
    )
    env.setdefault("PYTHONLOGLEVEL", "WARNING")
    temp_root = Path(tempfile.mkdtemp(prefix="pq-wait-test-"))
    log_path = temp_root.parent / f"{temp_root.name}-server.log"
    server: McpServer | None = None
    tester: Tester | None = None
    log: IO[str] | None = None
    try:
        if args.url:
            env.update(_write_config(args, temp_root))
        target = f" at {args.url}" if args.url else ""
        print(f"PQ wait test: system {args.system!r}{target}")
        print("Starting the server from this checkout (first run builds its env)...")
        log = None if args.verbose else open(log_path, "w")
        server = McpServer(_server_command(args), env, log)
        server.initialize()
        tester = Tester(server, args.verbose)
        run_checks(tester, args)
    except _Abort as exc:
        print(f"\nCOULD NOT RUN: {exc}")
        if not args.verbose:
            print(f"Server log: {log_path}")
        return 2
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130
    finally:
        if server is not None:
            server.close()
        if log is not None:
            log.close()
        shutil.rmtree(temp_root, ignore_errors=True)

    passed = sum(ok for ok, _ in tester.results)
    total = len(tester.results)
    verdict = "PASS" if passed == total else "FAIL"
    print(f"\n{'=' * 40}\n  {verdict}: {passed}/{total} checks passed\n{'=' * 40}")
    if verdict == "PASS":
        log_path.unlink(missing_ok=True)
        return 0
    if not args.verbose:
        print(f"Server log: {log_path}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
