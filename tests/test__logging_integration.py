"""
Integration tests for signal handler termination behavior in _logging.py.

These tests spawn real subprocesses, send signals, and verify that the process
actually terminates with the expected exit status.  They do NOT require Docker
or any external services.

Platform notes:
- These tests are POSIX-only (Linux/macOS).  On Windows, ``os.kill(pid, SIGTERM)``
  terminates the target process directly at the OS level without invoking Python's
  signal handler machinery, so the test assertion would pass even if the MCP handler
  never ran.  Windows signal delivery via ``CTRL_BREAK_EVENT`` / ``SIGBREAK`` requires
  a separate process group and is out of scope here.
- SIGHUP is Unix-only; on non-Windows POSIX systems it is always available.

Run with: uv run pytest -m integration tests/test__logging_integration.py
"""

import os
import signal
import subprocess
import sys
import textwrap
import threading

import pytest

# Script run in the subprocess: install signal handlers, then block indefinitely.
_SIGNAL_HANDLER_SCRIPT = textwrap.dedent("""
    import sys
    import time
    from deephaven_mcp._logging import setup_logging, setup_signal_handler_logging

    setup_logging()
    setup_signal_handler_logging()

    # Signal ready by writing to stdout, then block.
    sys.stdout.write("ready\\n")
    sys.stdout.flush()

    while True:
        time.sleep(0.1)
""")

_STARTUP_TIMEOUT = 5.0  # seconds to wait for subprocess "ready"
_EXIT_TIMEOUT = 5.0  # seconds to wait for subprocess to exit after signal
_KILL_WAIT_TIMEOUT = 5.0  # seconds to wait for subprocess to exit after SIGKILL


def _start_subprocess() -> "subprocess.Popen[bytes]":
    """Start the signal-handler subprocess and wait until it signals readiness.

    Spawns a Python subprocess that installs the MCP signal handlers and then
    writes ``ready`` to stdout.  This function blocks until that line is received
    or the startup timeout elapses.

    Returns:
        subprocess.Popen[bytes]: The running subprocess, with stdout/stderr pipes open.

    Raises:
        RuntimeError: If the subprocess exits before printing ``ready``, or if it
            prints an unexpected line instead of ``ready``.
        TimeoutError: If ``ready`` is not received within ``_STARTUP_TIMEOUT`` seconds.
    """
    proc = subprocess.Popen(
        [sys.executable, "-c", _SIGNAL_HANDLER_SCRIPT],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # readline() blocks indefinitely, so read in a background thread and use
    # threading.Event to enforce the startup timeout without relying on the
    # deadline check occurring only between blocking calls.
    ready_event = threading.Event()
    first_line: list[bytes] = []

    def _reader() -> None:
        line = proc.stdout.readline()
        first_line.append(line)
        ready_event.set()

    threading.Thread(target=_reader, daemon=True).start()

    if not ready_event.wait(timeout=_STARTUP_TIMEOUT):
        proc.kill()
        try:
            proc.communicate(timeout=_KILL_WAIT_TIMEOUT)
        except subprocess.TimeoutExpired:
            pass  # Process is stuck even after SIGKILL; abandon it
        raise TimeoutError(
            f"Subprocess did not print 'ready' within {_STARTUP_TIMEOUT}s"
        )

    if proc.poll() is not None:
        raise RuntimeError(
            f"Subprocess exited before ready (rc={proc.returncode})\n"
            f"stderr: {proc.stderr.read().decode()}"
        )

    if first_line[0].strip() != b"ready":
        proc.kill()
        try:
            proc.communicate(timeout=_KILL_WAIT_TIMEOUT)
        except subprocess.TimeoutExpired:
            pass  # Process is stuck even after SIGKILL; abandon it
        raise RuntimeError(
            f"Subprocess printed unexpected startup line: {first_line[0]!r}"
        )

    return proc


# SIGHUP is Unix-only; build the parametrize list conditionally so the test
# collection does not fail on Windows where the attribute does not exist.
_SIGNALS_TO_TEST = [signal.SIGTERM, signal.SIGINT]
if hasattr(signal, "SIGHUP"):
    _SIGNALS_TO_TEST.append(signal.SIGHUP)


@pytest.mark.integration
@pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "os.kill on Windows terminates the process at the OS level without invoking "
        "Python signal handlers, so this test cannot verify that the MCP handler ran."
    ),
)
@pytest.mark.parametrize("sig", _SIGNALS_TO_TEST)
def test_signal_handler_terminates_process(sig: signal.Signals) -> None:
    """Signal handler must terminate the process promptly after logging.

    Sends ``sig`` to a subprocess that has registered the MCP signal handlers
    via ``setup_signal_handler_logging()``, then asserts:

    1. The process exits within ``_EXIT_TIMEOUT`` seconds (i.e., the handler
       actually terminates the process rather than only logging).
    2. ``subprocess.returncode == -signum``, confirming the process was killed
       by the signal's OS default action (the SIG_DFL re-raise pattern), not
       by a ``sys.exit()`` call.  On POSIX, ``waitpid`` reports ``-signum``;
       shells commonly display this as ``128 + signum``.

    This is a regression test for the bug where ``_signal_handler`` only logged
    but never re-raised the signal, causing orphaned high-CPU processes when the
    parent MCP client (e.g., Claude Code) sent SIGTERM on exit.
    """
    proc = _start_subprocess()
    try:
        os.kill(proc.pid, sig)
        try:
            proc.wait(timeout=_EXIT_TIMEOUT)
        except subprocess.TimeoutExpired:
            proc.kill()
            pytest.fail(
                f"Process did not terminate within {_EXIT_TIMEOUT}s after {sig.name}. "
                f"Signal handler is not terminating the process."
            )

        # Drain and close the pipes to avoid file descriptor leaks across
        # parametrized runs.  The process has already exited so communicate()
        # returns immediately.
        proc.communicate()

        # subprocess.returncode == -N when the process was killed by signal N
        # with the default OS action.  Our re-raise pattern restores SIG_DFL
        # and calls os.kill(), so the OS delivers the signal natively.
        assert proc.returncode == -sig, (
            f"Expected returncode {-sig} (killed by {sig.name} default action), "
            f"got {proc.returncode}"
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            try:
                proc.communicate(timeout=_KILL_WAIT_TIMEOUT)
            except subprocess.TimeoutExpired:
                pass  # Process is stuck even after SIGKILL; abandon it
