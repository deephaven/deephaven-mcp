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
    context = create_mock_context(dummy_client)

    result = await mcp_mod.docs_chat(
        context=context, prompt="language?", history=None, programming_language="groovy"
    )
    assert result == "lang!"
    prompts = dummy_client.last_system_prompts
    assert any("Worker environment: Programming language: groovy" in p for p in prompts)


@pytest.mark.asyncio
async def test_docs_chat_programming_language_invalid(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    dummy_client = DummyOpenAIClient(response="should not matter")
    context = create_mock_context(dummy_client)

    result = await mcp_mod.docs_chat(
        context=context, prompt="language?", history=None, programming_language="java"
    )
    assert result.startswith("[ERROR]")
    assert "Unsupported programming language: java" in result


@pytest.mark.asyncio
async def test_docs_chat_success(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    # Create mock context with inkeep_client
    mock_client = DummyOpenAIClient(response="Hello from docs!")
    context = create_mock_context(mock_client)

    result = await mcp_mod.docs_chat(
        context=context,
        prompt="hi",
        history=[{"role": "user", "content": "hi"}],
        programming_language=None,
    )
    assert result == "Hello from docs!"


@pytest.mark.asyncio
async def test_docs_chat_with_core_version(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    dummy_client = DummyOpenAIClient(response="core!")
    context = create_mock_context(dummy_client)

    result = await mcp_mod.docs_chat(
        context=context,
        prompt="core version?",
        history=None,
        deephaven_core_version="0.39.0",
        programming_language=None,
    )
    assert result == "core!"
    prompts = dummy_client.last_system_prompts
    assert any("Deephaven Community Core version: 0.39.0" in p for p in prompts)
    assert any("helpful assistant" in p for p in prompts)


@pytest.mark.asyncio
async def test_docs_chat_with_enterprise_version(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    dummy_client = DummyOpenAIClient(response="enterprise!")
    context = create_mock_context(dummy_client)

    result = await mcp_mod.docs_chat(
        context=context,
        prompt="enterprise version?",
        history=None,
        deephaven_enterprise_version="1.2.3",
        programming_language=None,
    )
    assert result == "enterprise!"
    prompts = dummy_client.last_system_prompts
    assert any("Deephaven Core+ (Enterprise) version: 1.2.3" in p for p in prompts)
    assert any("helpful assistant" in p for p in prompts)


@pytest.mark.asyncio
async def test_docs_chat_with_both_versions(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    dummy_client = DummyOpenAIClient(response="both!")
    context = create_mock_context(dummy_client)

    result = await mcp_mod.docs_chat(
        context=context,
        prompt="both?",
        history=None,
        deephaven_core_version="0.39.0",
        deephaven_enterprise_version="1.2.3",
        programming_language=None,
    )
    assert result == "both!"
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
    context = create_mock_context(dummy_client)

    result = await mcp_mod.docs_chat(
        context=context, prompt="no version?", history=None, programming_language=None
    )
    assert result == "no version!"
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

    # Create context with dummy client that raises
    dummy_client = DummyOpenAIClient(exc=OpenAIClientError("fail!"))
    context = create_mock_context(dummy_client)

    result = await mcp_mod.docs_chat(
        context=context, prompt="fail", history=None, programming_language=None
    )
    assert result.startswith("[ERROR]")
    assert "fail!" in result


@pytest.mark.asyncio
async def test_docs_chat_generic_exception(monkeypatch):
    """Test docs_chat handles generic (non-OpenAIClientError) exceptions."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    # Create context with dummy client that raises a generic exception
    dummy_client = DummyOpenAIClient(exc=ValueError("Generic error!"))
    context = create_mock_context(dummy_client)

    result = await mcp_mod.docs_chat(
        context=context, prompt="fail", history=None, programming_language=None
    )
    assert result.startswith("[ERROR]")
    assert "ValueError: Generic error!" in result


@pytest.mark.asyncio
async def test_app_lifespan_cleanup_exception(monkeypatch):
    """Test app_lifespan handles exceptions during client cleanup."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    # Create a mock client that raises an exception on close()
    mock_client = AsyncMock()
    mock_client.close.side_effect = Exception("Cleanup failed")

    # Mock the OpenAIClient constructor to return our mock client
    with (
        patch(
            "deephaven_mcp.mcp_docs_server._mcp.OpenAIClient", return_value=mock_client
        ),
        patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger,
    ):

        # Test the app_lifespan context manager
        async with mcp_mod.app_lifespan(None) as context:
            assert "inkeep_client" in context
            assert context["inkeep_client"] == mock_client

        # Verify that the error was logged during cleanup
        mock_logger.error.assert_called_once_with(
            "[app_lifespan] Error during cleanup: Cleanup failed"
        )


@pytest.mark.asyncio
async def test_app_lifespan_successful_cleanup(monkeypatch):
    """Test app_lifespan logs success message during normal cleanup."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    # Create a mock client that closes successfully
    mock_client = AsyncMock()
    mock_client.close.return_value = None  # Successful close

    # Mock the OpenAIClient constructor to return our mock client
    with (
        patch(
            "deephaven_mcp.mcp_docs_server._mcp.OpenAIClient", return_value=mock_client
        ),
        patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger,
    ):

        # Test the app_lifespan context manager
        async with mcp_mod.app_lifespan(None) as context:
            assert "inkeep_client" in context
            assert context["inkeep_client"] == mock_client

        # Verify that the success message was logged during cleanup
        # Note: info is called multiple times during app lifecycle, so check if our message is in the calls
        success_call = ("[app_lifespan] Successfully closed OpenAI client connections",)
        assert success_call in [call.args for call in mock_logger.info.call_args_list]
