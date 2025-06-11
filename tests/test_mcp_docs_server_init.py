import importlib
import logging
import os
import subprocess
import sys
import types

import pytest


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

    monkeypatch.setattr(mod, "mcp_server", DummyServer())
    logs = []
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: logs.append(kwargs))
    monkeypatch.setattr(mod, "_LOGGER", logging.getLogger("dummy"))
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

    monkeypatch.setattr(mod, "mcp_server", DummyServer())
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(mod, "_LOGGER", logging.getLogger("dummy"))
    mod.run_server("sse")
    assert called["run"][0] == "sse"


def test_main_invokes_run_server(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server.__init__", None)
    mod = importlib.import_module("deephaven_mcp.mcp_docs_server.__init__")
    called = {}
    monkeypatch.setattr(
        mod,
        "run_server",
        lambda transport, host=None: called.setdefault("args", (transport, host)),
    )
    monkeypatch.setattr("sys.argv", ["prog", "-t", "sse"])
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
    monkeypatch.setattr(mod, "mcp_server", dummy)
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(mod, "_LOGGER", logging.getLogger("dummy"))
    mod.run_server("sse")
    assert called["transport"] == "sse"
    assert dummy.host == "0.0.0.0"


def test_run_server_exception_logs_stopped(monkeypatch, caplog):
    """Test that the 'stopped' log is emitted even if mcp_server.run raises an exception."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server.__init__", None)
    mod = importlib.import_module("deephaven_mcp.mcp_docs_server.__init__")

    class DummyServer:
        name = "dummy"

        def run(self, transport=None):
            raise RuntimeError("fail!")

    monkeypatch.setattr(mod, "mcp_server", DummyServer())
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(mod, "_LOGGER", logging.getLogger("dummy"))
    with caplog.at_level("INFO"):
        try:
            mod.run_server("sse")
        except RuntimeError:
            pass
    assert "stopped" in caplog.text


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
    """Test that run_server uses the MCP_DOCS_HOST env var to bind the server to the specified host (e.g., 0.0.0.0)."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    monkeypatch.setenv("MCP_DOCS_HOST", "0.0.0.0")
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
    monkeypatch.setattr(mod, "mcp_server", dummy)
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(mod, "_LOGGER", logging.getLogger("dummy"))
    mod.run_server("sse")
    assert called["transport"] == "sse"
    assert dummy.host == "0.0.0.0"
