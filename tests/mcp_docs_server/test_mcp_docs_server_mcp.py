import asyncio
import importlib
import os
import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp.server.fastmcp import Context
from starlette.requests import Request


def test_all_exports(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    assert hasattr(mcp_mod, "mcp_server")
    assert "mcp_server" in mcp_mod.__all__


def test_env_var_required(monkeypatch):
    # Remove INKEEP_API_KEY if present
    monkeypatch.delenv("INKEEP_API_KEY", raising=False)
    # Remove module from sys.modules to force reload
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    with pytest.raises(RuntimeError) as excinfo:
        importlib.import_module("deephaven_mcp.mcp_docs_server._mcp")
    assert "INKEEP_API_KEY environment variable must be set" in str(excinfo.value)


def test_mcp_server_and_docs_chat(monkeypatch):
    # Set INKEEP_API_KEY so import works
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    mod = importlib.import_module("deephaven_mcp.mcp_docs_server._mcp")
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

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass  # No cleanup needed for dummy client


def create_mock_context(inkeep_client):
    """Create a mock context that matches FastMCP's context.request_context.lifespan_context structure."""

    class MockLifespanContext:
        def __init__(self, data):
            self._data = data

        def __getitem__(self, key):
            return self._data[key]

    class MockRequestContext:
        def __init__(self, lifespan_data):
            self.lifespan_context = MockLifespanContext(lifespan_data)

    class MockContext:
        def __init__(self, lifespan_data):
            self.request_context = MockRequestContext(lifespan_data)

    return MockContext({"inkeep_client": inkeep_client})


@pytest.mark.asyncio
async def test_docs_chat_programming_language(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    dummy_client = DummyOpenAIClient(response="lang!")

    # Mock OpenAI client creation to return our dummy client
    with patch(
        "deephaven_mcp.mcp_docs_server._mcp.OpenAIClient", return_value=dummy_client
    ):
        result = await mcp_mod.docs_chat(
            context={}, prompt="language?", history=None, programming_language="groovy"
        )
        assert result == {"success": True, "response": "lang!"}
        prompts = dummy_client.last_system_prompts
        assert any(
            "Worker environment: Programming language: groovy" in p for p in prompts
        )


@pytest.mark.asyncio
async def test_docs_chat_programming_language_invalid(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    dummy_client = DummyOpenAIClient(response="should not matter")

    # Mock OpenAI client creation (though it won't be called due to early validation error)
    with patch(
        "deephaven_mcp.mcp_docs_server._mcp.OpenAIClient", return_value=dummy_client
    ):
        result = await mcp_mod.docs_chat(
            context={}, prompt="language?", history=None, programming_language="java"
        )
        assert result["success"] is False
        assert "Unsupported programming language: java" in result["error"]
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_docs_chat_success(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    # Create mock client
    mock_client = DummyOpenAIClient(response="Hello from docs!")

    # Mock OpenAI client creation to return our mock client
    with patch(
        "deephaven_mcp.mcp_docs_server._mcp.OpenAIClient", return_value=mock_client
    ):
        result = await mcp_mod.docs_chat(
            context={},
            prompt="hi",
            history=[{"role": "user", "content": "hi"}],
            programming_language=None,
        )
        assert result == {"success": True, "response": "Hello from docs!"}


@pytest.mark.asyncio
async def test_docs_chat_with_core_version(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    dummy_client = DummyOpenAIClient(response="core!")

    with patch(
        "deephaven_mcp.mcp_docs_server._mcp.OpenAIClient", return_value=dummy_client
    ):
        result = await mcp_mod.docs_chat(
            context={},
            prompt="core version?",
            history=None,
            deephaven_core_version="0.39.0",
            programming_language=None,
        )
        assert result == {"success": True, "response": "core!"}
        prompts = dummy_client.last_system_prompts
        assert any("Deephaven Community Core version: 0.39.0" in p for p in prompts)
        assert any("helpful assistant" in p for p in prompts)


@pytest.mark.asyncio
async def test_docs_chat_with_enterprise_version(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    dummy_client = DummyOpenAIClient(response="enterprise!")

    with patch(
        "deephaven_mcp.mcp_docs_server._mcp.OpenAIClient", return_value=dummy_client
    ):
        result = await mcp_mod.docs_chat(
            context={},
            prompt="enterprise version?",
            history=None,
            deephaven_enterprise_version="1.2.3",
            programming_language=None,
        )
        assert result == {"success": True, "response": "enterprise!"}
        prompts = dummy_client.last_system_prompts
        assert any("Deephaven Core+ (Enterprise) version: 1.2.3" in p for p in prompts)
        assert any("helpful assistant" in p for p in prompts)


@pytest.mark.asyncio
async def test_docs_chat_with_both_versions(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    dummy_client = DummyOpenAIClient(response="both!")

    with patch(
        "deephaven_mcp.mcp_docs_server._mcp.OpenAIClient", return_value=dummy_client
    ):
        result = await mcp_mod.docs_chat(
            context={},
            prompt="both?",
            history=None,
            deephaven_core_version="0.39.0",
            deephaven_enterprise_version="1.2.3",
            programming_language=None,
        )
        assert result == {"success": True, "response": "both!"}
        prompts = dummy_client.last_system_prompts
        assert any("Deephaven Community Core version: 0.39.0" in p for p in prompts)
        assert any("Deephaven Core+ (Enterprise) version: 1.2.3" in p for p in prompts)
        assert any("helpful assistant" in p for p in prompts)


@pytest.mark.asyncio
async def test_docs_chat_with_neither_version(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    dummy_client = DummyOpenAIClient(response="no version!")

    with patch(
        "deephaven_mcp.mcp_docs_server._mcp.OpenAIClient", return_value=dummy_client
    ):
        result = await mcp_mod.docs_chat(
            context={}, prompt="no version?", history=None, programming_language=None
        )
        assert result == {"success": True, "response": "no version!"}
        prompts = dummy_client.last_system_prompts
        # Only the base system prompt should be present
        assert any("helpful assistant" in p for p in prompts)
        assert not any(
            "Core version" in p or "Core+ (Enterprise) version" in p for p in prompts
        )


@pytest.mark.asyncio
async def test_health_check_direct(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    mod = importlib.import_module("deephaven_mcp.mcp_docs_server._mcp")
    # Minimal ASGI scope for Request
    scope = {"type": "http", "method": "GET", "path": "/health"}
    req = Request(scope)
    resp = await mod.health_check(req)
    assert resp.status_code == 200
    assert resp.body == b'{"status":"ok"}'


@pytest.mark.asyncio
async def test_docs_chat_error(monkeypatch):
    from deephaven_mcp.openai import OpenAIClientError

    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    # Create dummy client that raises
    dummy_client = DummyOpenAIClient(exc=OpenAIClientError("fail!"))

    with patch(
        "deephaven_mcp.mcp_docs_server._mcp.OpenAIClient", return_value=dummy_client
    ):
        result = await mcp_mod.docs_chat(
            context={}, prompt="fail", history=None, programming_language=None
        )
        assert result["success"] is False
        assert "OpenAIClientError: fail!" in result["error"]
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_docs_chat_generic_exception(monkeypatch):
    """Test docs_chat handles generic (non-OpenAIClientError) exceptions."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    # Create dummy client that raises a generic exception
    dummy_client = DummyOpenAIClient(exc=ValueError("Generic error!"))

    with patch(
        "deephaven_mcp.mcp_docs_server._mcp.OpenAIClient", return_value=dummy_client
    ):
        result = await mcp_mod.docs_chat(
            context={}, prompt="fail", history=None, programming_language=None
        )
        assert result["success"] is False
        assert "ValueError: Generic error!" in result["error"]
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_app_lifespan_cleanup_exception(monkeypatch):
    """Test app_lifespan context manager yields empty context and handles startup/shutdown."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger:
        # Test the app_lifespan context manager
        async with mcp_mod.app_lifespan(None) as context:
            # Context should be empty since we create clients per-request
            assert context == {}

        # Verify startup and shutdown logging
        mock_logger.info.assert_called()


@pytest.mark.asyncio
async def test_app_lifespan_successful_cleanup(monkeypatch):
    """Test app_lifespan logs startup and shutdown messages."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger:
        # Test the app_lifespan context manager
        async with mcp_mod.app_lifespan(None) as context:
            # Context should be empty since we create clients per-request
            assert context == {}

        # Verify that startup and shutdown logging occurred
        mock_logger.info.assert_called()
