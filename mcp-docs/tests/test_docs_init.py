import importlib
import sys
import types
import pytest
import logging


def test_module_exports(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.docs.__init__", None)
    mod = importlib.import_module("deephaven_mcp.docs.__init__")
    assert hasattr(mod, "mcp_server")
    assert hasattr(mod, "run_server")
    assert hasattr(mod, "main")
    assert hasattr(mod, "__all__")
    assert "mcp_server" in mod.__all__
    assert "run_server" in mod.__all__


def test_run_server_stdio(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.docs.__init__", None)
    mod = importlib.import_module("deephaven_mcp.docs.__init__")
    called = {}
    
    class DummyServer:
        name = "dummy"
        def run(self, transport=None):
            called["run"] = transport
    
    monkeypatch.setattr(mod, "mcp_server", DummyServer())
    logs = []
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: logs.append(kwargs))
    monkeypatch.setattr(mod, "_LOGGER", logging.getLogger("dummy"))
    mod.run_server("stdio")
    assert called["run"] == "stdio"
    assert any("level" in conf for conf in logs)


def test_run_server_sse(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.docs.__init__", None)
    mod = importlib.import_module("deephaven_mcp.docs.__init__")
    called = {}
    class DummyServer:
        name = "dummy"
        def run(self, transport=None):
            called["run"] = transport
    monkeypatch.setattr(mod, "mcp_server", DummyServer())
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(mod, "_LOGGER", logging.getLogger("dummy"))
    mod.run_server("sse")
    assert called["run"] == "sse"


def test_main_invokes_run_server(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.docs.__init__", None)
    mod = importlib.import_module("deephaven_mcp.docs.__init__")
    called = {}
    monkeypatch.setattr(mod, "run_server", lambda transport: called.setdefault("transport", transport))
    monkeypatch.setattr("sys.argv", ["prog", "-t", "sse"])
    mod.main()
    assert called["transport"] == "sse"
