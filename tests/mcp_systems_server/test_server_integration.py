"""Integration tests for the ``dh-mcp-systems-server`` console script.

These tests are CLI-level smoke tests that exercise the wiring between
``main()`` and its helpers without booting a real transport. They
complement the unit tests in :mod:`tests.mcp_systems_server.test_server`
by also covering the ``--help`` output, version-of-truth defaults, and
the absence of references to retired transports.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deephaven_mcp.mcp_systems_server import _http as http_module
from deephaven_mcp.mcp_systems_server import server as server_module


def test_help_text_lists_supported_transports():
    """``--help`` advertises only stdio + http (sse is gone)."""
    proc = subprocess.run(
        [sys.executable, "-m", "deephaven_mcp.mcp_systems_server", "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0
    out = proc.stdout
    assert "stdio" in out
    assert "http" in out
    # The retired SSE transport must not appear as a transport choice.
    # Use a word-boundary check; substring matching produces
    # false positives on legitimate words like "Bypasses".
    assert not re.search(r"\bsse\b", out, flags=re.IGNORECASE)
    assert "loopback" in out.lower()


def test_console_script_help_runs_dh_mcp_systems_server():
    """The installed entry point ``dh-mcp-systems-server`` exposes --help."""
    proc = subprocess.run(
        ["dh-mcp-systems-server", "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0
    assert "Multiplexed Deephaven MCP systems server" in proc.stdout


@pytest.fixture
def _mute_logging_setup():
    patches = [
        patch.object(server_module, "setup_logging"),
        patch.object(server_module, "setup_global_exception_logging"),
        patch.object(server_module, "setup_signal_handler_logging"),
        patch.object(server_module, "monkeypatch_uvicorn_exception_handling"),
    ]
    for p in patches:
        p.start()
    yield
    for p in patches:
        p.stop()


def _patch_default_server_config():
    """Stub ``_load_multi_config_or_exit`` with a default ``ConfigTree`` mock."""
    multi = MagicMock()
    multi.server = None  # main() falls back to ServerConfig() defaults
    multi.config_dir = Path("/tmp/cfg")
    return patch.object(
        server_module,
        "_load_multi_config_or_exit",
        AsyncMock(return_value=multi),
    )


def test_main_defaults_to_stdio(_mute_logging_setup):
    """No args + default ``ServerConfig`` ⇒ stdio transport, no HTTP work."""
    with (
        _patch_default_server_config(),
        patch.object(server_module, "_build_fastmcp", return_value=MagicMock()),
        patch.object(server_module, "_run_stdio") as mock_stdio,
        patch.object(server_module, "_run_http") as mock_http,
    ):
        server_module.main([])
    mock_stdio.assert_called_once()
    mock_http.assert_not_called()


def test_main_http_with_default_loopback_host(_mute_logging_setup):
    """``--transport http`` with default host '127.0.0.1' is allowed."""
    with (
        _patch_default_server_config(),
        patch.object(http_module, "_resolve_psk_or_exit", MagicMock(return_value="pw")),
        patch.object(server_module, "_build_fastmcp", return_value=MagicMock()),
        patch.object(server_module, "_run_http") as mock_http,
    ):
        server_module.main(["--transport", "http"])
    mock_http.assert_called_once()
    # Plan-then-run: ``main`` builds an ``_HttpRun`` plan and passes it
    # positionally to the unified runner. Inspect the plan rather than
    # the previous host/psk kwargs.
    plan = mock_http.call_args.args[0]
    assert plan.bind.host == "127.0.0.1"
    assert plan.bind.sock is None
    assert plan.psk == "pw"
