import asyncio
import importlib
import os
import sys
import types

import pytest
from starlette.requests import Request


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
        self.last_system_prompts = None

    async def chat(self, prompt, history=None, system_prompts=None, **kwargs):
        self.last_system_prompts = system_prompts
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


def test_docs_chat_with_pip_packages(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.docs._mcp", None)
    import deephaven_mcp.docs._mcp as mcp_mod

    dummy_client = DummyOpenAIClient(response="Pip-aware!")
    mcp_mod.inkeep_client = dummy_client
    pip_pkgs = [
        {"package": "numpy", "version": "1.25.0"},
        {"package": "pandas", "version": "2.1.0"},
    ]
    coro = mcp_mod.docs_chat("Which version of pandas?", None, pip_packages=pip_pkgs)
    result = asyncio.run(coro)
    assert result == "Pip-aware!"
    # Should only include the base system prompt
    assert len(dummy_client.last_system_prompts) == 1
    assert "helpful assistant" in dummy_client.last_system_prompts[0]


def test_docs_chat_with_deephaven_core(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.docs._mcp", None)
    import deephaven_mcp.docs._mcp as mcp_mod

    dummy_client = DummyOpenAIClient(response="core!")
    mcp_mod.inkeep_client = dummy_client
    pip_pkgs = [{"package": "deephaven", "version": "0.39.0"}]
    coro = mcp_mod.docs_chat("core version?", None, pip_packages=pip_pkgs)
    result = asyncio.run(coro)
    assert result == "core!"
    assert any("Core version: 0.39.0" in p for p in dummy_client.last_system_prompts)


def test_docs_chat_with_coreplus(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.docs._mcp", None)
    import deephaven_mcp.docs._mcp as mcp_mod

    dummy_client = DummyOpenAIClient(response="coreplus!")
    mcp_mod.inkeep_client = dummy_client
    pip_pkgs = [{"package": "deephaven_coreplus_worker", "version": "1.2.3"}]
    coro = mcp_mod.docs_chat("coreplus version?", None, pip_packages=pip_pkgs)
    result = asyncio.run(coro)
    assert result == "coreplus!"
    assert any(
        "Core+ (Enterprise) version: 1.2.3" in p
        for p in dummy_client.last_system_prompts
    )


def test_docs_chat_with_both_versions(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.docs._mcp", None)
    import deephaven_mcp.docs._mcp as mcp_mod

    dummy_client = DummyOpenAIClient(response="both!")
    mcp_mod.inkeep_client = dummy_client
    pip_pkgs = [
        {"package": "deephaven", "version": "0.39.0"},
        {"package": "deephaven_coreplus_worker", "version": "1.2.3"},
    ]
    coro = mcp_mod.docs_chat("both?", None, pip_packages=pip_pkgs)
    result = asyncio.run(coro)
    assert result == "both!"
    prompts = dummy_client.last_system_prompts
    assert any("Core version: 0.39.0" in p for p in prompts)
    assert any("Core+ (Enterprise) version: 1.2.3" in p for p in prompts)


@pytest.mark.asyncio
async def test_health_check_direct():
    import importlib
    import sys

    sys.modules.pop("deephaven_mcp.docs._mcp", None)
    mod = importlib.import_module("deephaven_mcp.docs._mcp")
    # Minimal ASGI scope for Request
    scope = {"type": "http", "method": "GET", "path": "/health"}
    req = Request(scope)
    resp = await mod.health_check(req)
    assert resp.status_code == 200
    assert resp.body == b'{"status":"ok"}'


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
