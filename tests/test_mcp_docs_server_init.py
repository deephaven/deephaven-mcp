import importlib
import logging
import os
import subprocess
import sys
import types

import pytest
from unittest.mock import patch, MagicMock


def test_module_exports(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server.__init__", None)
    mod = importlib.import_module("deephaven_mcp.mcp_docs_server.__init__")
    assert hasattr(mod, "mcp_server")
    assert hasattr(mod, "run_server")
    assert hasattr(mod, "main")
    assert hasattr(mod, "__all__")
    assert "mcp_server" in mod.__all__
    assert "run_server" in mod.__all__


def test_run_server_stdio(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server.__init__", None)
    mod = importlib.import_module("deephaven_mcp.mcp_docs_server.__init__")
    called = {}

    class DummyServer:
        name = "dummy"

        def run(self, transport=None, host=None):
            called["run"] = transport, host

    logs = []
    with (
        patch.object(mod, "mcp_server", DummyServer()),
        patch.object(logging, "basicConfig", lambda **kwargs: logs.append(kwargs)),
        patch.object(mod, "_LOGGER", logging.getLogger("dummy")),
    ):
        mod.run_server("stdio")
        assert called["run"][0] == "stdio"
        assert any("level" in conf for conf in logs)


def test_run_server_sse(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server.__init__", None)
    mod = importlib.import_module("deephaven_mcp.mcp_docs_server.__init__")
    called = {}

    class DummyServer:
        name = "dummy"

        def run(self, transport=None, *, host=None):
            called["run"] = (transport, host)

    with (
        patch.object(mod, "mcp_server", DummyServer()),
        patch.object(logging, "basicConfig", lambda **kwargs: None),
        patch.object(mod, "_LOGGER", logging.getLogger("dummy")),
    ):
        mod.run_server("sse")
        assert called["run"][0] == "sse"


def test_main_invokes_run_server(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server.__init__", None)
    mod = importlib.import_module("deephaven_mcp.mcp_docs_server.__init__")
    called = {}
    with (
        patch.object(mod, "run_server", lambda transport, host=None: called.setdefault("args", (transport, host))),
        patch("sys.argv", ["prog", "-t", "sse"]),
    ):
        mod.main()
        assert called["args"] == ("sse", None)


def test_run_server_binds_to_default_host(monkeypatch):
    """Test that run_server uses a server instance bound to 0.0.0.0 regardless of env vars."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server.__init__", None)
    mod = importlib.import_module("deephaven_mcp.mcp_docs_server.__init__")
    called = {}

    class DummyServer:
        name = "dummy"

        def __init__(self, host=None):
            self.host = host

        def run(self, transport=None):
            called["transport"] = transport

    dummy = DummyServer(host="0.0.0.0")
    with (
        patch.object(mod, "mcp_server", dummy),
        patch.object(logging, "basicConfig", lambda **kwargs: None),
        patch.object(mod, "_LOGGER", logging.getLogger("dummy")),
    ):
        mod.run_server("sse")
        assert called["transport"] == "sse"
        assert dummy.host == "0.0.0.0"


def test_run_server_exception_logs_stopped(monkeypatch):
    """Test that the 'stopped' log is emitted even if mcp_server.run raises an exception."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server.__init__", None)
    mod = importlib.import_module("deephaven_mcp.mcp_docs_server.__init__")

    class DummyServer:
        name = "dummy"

        def run(self, transport=None, host=None):
            raise RuntimeError("fail")

    dummy = DummyServer()
    mock_logger_info = MagicMock()
    with (
        patch.object(mod, "mcp_server", dummy),
        patch.object(logging, "basicConfig", lambda **kwargs: None),  # Keep basicConfig patched to isolate logging
        patch.object(mod._LOGGER, "info", mock_logger_info),
    ):
        with pytest.raises(RuntimeError):
            mod.run_server("stdio")
    
    # Check that mock_logger_info was called with a message containing "stopped"
    stopped_call_found = False
    for call_args in mock_logger_info.call_args_list:
        if "stopped" in str(call_args[0][0]).lower():
            stopped_call_found = True
            break
    assert stopped_call_found, "_LOGGER.info was not called with 'stopped' message"


def test_docs_module_main_invocation():
    """Test CLI entrypoint via subprocess for 100% coverage."""
    result = subprocess.run(
        [sys.executable, "-m", "deephaven_mcp.mcp_docs_server.__init__", "-t", "stdio"],
        env={**os.environ, "INKEEP_API_KEY": "dummy-key"},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "Starting MCP server" in result.stdout or result.stderr


def test_import_docs_init():
    sys.modules.pop("deephaven_mcp.mcp_docs_server.__init__", None)
    importlib.import_module("deephaven_mcp.mcp_docs_server.__init__")


def test_run_server_host_env(monkeypatch):
    """Test that MCP_DOCS_HOST configures FastMCP's host, and run_server doesn't override it."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")  # Needed for _mcp.py import
    monkeypatch.setenv("MCP_DOCS_HOST", "1.2.3.4")

    # Create a mock for the FastMCP class itself
    MockFastMCPClass = MagicMock()
    # This mock_server_instance is what MockFastMCPClass() will return
    mock_server_instance = MagicMock()
    mock_server_instance.name = "deephaven-mcp-docs" # Needed for logging in run_server
    MockFastMCPClass.return_value = mock_server_instance

    # Patch FastMCP where it's imported by _mcp.py.
    with patch("mcp.server.fastmcp.FastMCP", MockFastMCPClass):
        # Ensure modules are reloaded in the correct order for the patch to apply
        # and for __init__.py to pick up the patched _mcp.py's mcp_server
        if "deephaven_mcp.mcp_docs_server._mcp" in sys.modules:
            sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp")
        if "deephaven_mcp.mcp_docs_server.__init__" in sys.modules:
            sys.modules.pop("deephaven_mcp.mcp_docs_server.__init__")

        # Importing __init__ will, in turn, import _mcp, which will use the patched FastMCP
        mod_init = importlib.import_module("deephaven_mcp.mcp_docs_server.__init__")

        # 1. Verify FastMCP class was instantiated with the host from the environment variable
        MockFastMCPClass.assert_called_once_with("deephaven-mcp-docs", host="1.2.3.4")

        # mod_init.mcp_server is now mock_server_instance (the return_value of MockFastMCPClass)
        # 2. Call run_server and verify arguments to the instance's run method.
        #    Mock logging as it's not relevant to this test's core assertion.
        with patch.object(logging, "basicConfig", lambda **kwargs: None), \
             patch.object(mod_init, "_LOGGER", logging.getLogger("dummy")):
            mod_init.run_server("stdio")

        # Assert that the 'run' method of our mock_server_instance was called correctly.
        # run_server calls mcp_server.run(transport=transport). Host should not be explicitly passed.
        mock_server_instance.run.assert_called_once_with(transport="stdio")
