import importlib
import os
import sys
import types
import pytest


def test_env_var_required(monkeypatch):
    # Remove INKEEP_API_KEY if present
    monkeypatch.delenv("INKEEP_API_KEY", raising=False)
    # Remove module from sys.modules to force reload
    sys.modules.pop("deephaven_mcp.docs._mcp", None)
    with pytest.raises(RuntimeError) as excinfo:
        importlib.import_module("deephaven_mcp.docs._mcp")
    assert "INKEEP_API_KEY environment variable must be set" in str(excinfo.value)


def test_mcp_server_and_docs_chat(monkeypatch):
    # Set INKEEP_API_KEY so import works
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.docs._mcp", None)
    mod = importlib.import_module("deephaven_mcp.docs._mcp")
    from mcp.server.fastmcp import FastMCP
    assert hasattr(mod, "mcp_server")
    assert isinstance(mod.mcp_server, FastMCP)
    # Check __all__
    assert hasattr(mod, "__all__")
    assert "mcp_server" in mod.__all__

import asyncio

class DummyOpenAIClient:
    def __init__(self, response=None, exc=None):
        self.response = response
        self.exc = exc
    async def chat(self, prompt, history=None):
        if self.exc:
            raise self.exc
        return self.response

def test_docs_chat_success(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.docs._mcp", None)
    import deephaven_mcp.docs._mcp as mcp_mod
    # Patch inkeep_client with dummy
    mcp_mod.inkeep_client = DummyOpenAIClient(response="Hello from docs!")
    coro = mcp_mod.docs_chat("hi", [{"role": "user", "content": "hi"}])
    result = asyncio.run(coro)
    assert result == "Hello from docs!"

def test_docs_chat_error(monkeypatch):
    from deephaven_mcp.openai import OpenAIClientError
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.docs._mcp", None)
    import deephaven_mcp.docs._mcp as mcp_mod
    # Patch inkeep_client with dummy that raises
    mcp_mod.inkeep_client = DummyOpenAIClient(exc=OpenAIClientError("fail!"))
    coro = mcp_mod.docs_chat("fail", None)
    try:
        asyncio.run(coro)
    except OpenAIClientError as e:
        assert "fail!" in str(e)
    else:
        assert False, "OpenAIClientError not raised"
