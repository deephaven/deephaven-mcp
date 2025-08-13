import asyncio
import importlib
import os
import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp.server.fastmcp import Context
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse


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


def test_log_asyncio_runtime_error_handling(monkeypatch):
    """Test _log_asyncio_and_thread_state RuntimeError handling for coverage."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    # Mock asyncio.get_running_loop to raise RuntimeError (no event loop)
    with patch("asyncio.get_running_loop", side_effect=RuntimeError("No event loop")):
        with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger:
            # Call the helper function that should handle the RuntimeError
            mcp_mod._log_asyncio_and_thread_state("test")

            # Verify info message was logged for no event loop (line 182)
            mock_logger.info.assert_any_call(
                "[mcp_docs_server:app_lifespan] No asyncio event loop running during test"
            )


@pytest.mark.asyncio
async def test_app_lifespan_exception_in_context(monkeypatch):
    """Test app_lifespan exception handling during context execution."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger:
        # Test exception handling in the context manager
        # With except* syntax, individual exceptions are wrapped in ExceptionGroup
        with pytest.raises(ExceptionGroup) as exc_info:
            async with mcp_mod.app_lifespan(None) as context:
                # Raise an exception inside the context
                raise ValueError("Test exception in context")

        # Verify the ExceptionGroup contains our ValueError
        assert len(exc_info.value.exceptions) == 1
        assert isinstance(exc_info.value.exceptions[0], ValueError)
        assert str(exc_info.value.exceptions[0]) == "Test exception in context"

        # Verify exception was logged with new exception group format
        mock_logger.error.assert_any_call(
            "[mcp_docs_server:app_lifespan] ValueError: Test exception in context"
        )


@pytest.mark.asyncio
async def test_app_lifespan_anyio_closed_resource_error(monkeypatch):
    """Test app_lifespan handling of anyio.ClosedResourceError."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import anyio

    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger:
        with pytest.raises(ExceptionGroup) as exc_info:
            async with mcp_mod.app_lifespan(None) as context:
                raise anyio.ClosedResourceError("Stream closed")

        # Verify the ExceptionGroup contains our ClosedResourceError
        assert len(exc_info.value.exceptions) == 1
        assert isinstance(exc_info.value.exceptions[0], anyio.ClosedResourceError)

        # Verify specific error message for ClosedResourceError (line 372-374)
        mock_logger.error.assert_any_call(
            "[mcp_docs_server:app_lifespan] This indicates a stream/connection was closed unexpectedly during server operation"
        )


@pytest.mark.asyncio
async def test_app_lifespan_cancelled_error_handling(monkeypatch):
    """Test app_lifespan handling of CancelledError (line 376)."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import asyncio

    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    # Create a custom CancelledError subclass that can be used in ExceptionGroup
    class TestCancelledError(asyncio.CancelledError, Exception):
        """Custom CancelledError that inherits from Exception for ExceptionGroup compatibility."""

        pass

    with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger:
        with pytest.raises(ExceptionGroup) as exc_info:
            async with mcp_mod.app_lifespan(None) as context:
                raise TestCancelledError("Task cancelled")

        # Verify the ExceptionGroup contains our exception
        assert len(exc_info.value.exceptions) == 1
        assert isinstance(exc_info.value.exceptions[0], asyncio.CancelledError)

        # Check that the specific CancelledError message was logged (line 376)
        mock_logger.error.assert_any_call(
            "[mcp_docs_server:app_lifespan] This indicates the server task was cancelled during operation"
        )


@pytest.mark.asyncio
async def test_app_lifespan_connection_error(monkeypatch):
    """Test app_lifespan handling of ConnectionError."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger:
        with pytest.raises(ExceptionGroup) as exc_info:
            async with mcp_mod.app_lifespan(None) as context:
                raise ConnectionError("Connection failed")

        # Verify the ExceptionGroup contains our ConnectionError
        assert len(exc_info.value.exceptions) == 1
        assert isinstance(exc_info.value.exceptions[0], ConnectionError)

        # Verify specific error message for ConnectionError (line 380-382)
        mock_logger.error.assert_any_call(
            "[mcp_docs_server:app_lifespan] This indicates a connection or system-level error during server operation"
        )


@pytest.mark.asyncio
async def test_app_lifespan_os_error(monkeypatch):
    """Test app_lifespan handling of OSError."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger:
        with pytest.raises(ExceptionGroup) as exc_info:
            async with mcp_mod.app_lifespan(None) as context:
                raise OSError("System error")

        # Verify the ExceptionGroup contains our OSError
        assert len(exc_info.value.exceptions) == 1
        assert isinstance(exc_info.value.exceptions[0], OSError)

        # Verify specific error message for OSError (line 380-382)
        mock_logger.error.assert_any_call(
            "[mcp_docs_server:app_lifespan] This indicates a connection or system-level error during server operation"
        )


@pytest.mark.asyncio
async def test_app_lifespan_timeout_error_handling(monkeypatch):
    """Test app_lifespan handling of TimeoutError (line 384)."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    # Now that TimeoutError is checked before OSError, we can test it properly
    with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger:
        with pytest.raises(ExceptionGroup) as exc_info:
            async with mcp_mod.app_lifespan(None) as context:
                # Raise a real TimeoutError
                raise TimeoutError("Operation timed out")

        # Verify the ExceptionGroup contains our TimeoutError
        assert len(exc_info.value.exceptions) == 1
        assert isinstance(exc_info.value.exceptions[0], TimeoutError)

        # Check that the specific TimeoutError message was logged (line 379-381)
        mock_logger.error.assert_any_call(
            "[mcp_docs_server:app_lifespan] This indicates an operation timed out during server operation"
        )


@pytest.mark.asyncio
async def test_app_lifespan_diagnostic_logging_exception(monkeypatch):
    """Test app_lifespan handling when diagnostic logging fails (lines 405-406)."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger:
        # Mock log_process_state to raise an exception during diagnostic logging in except* block only
        with (
            patch("deephaven_mcp._logging.log_process_state") as mock_log_process,
            patch(
                "deephaven_mcp.mcp_docs_server._mcp._log_asyncio_and_thread_state"
            ) as mock_log_asyncio,
        ):
            # Set up the mock to work during startup and shutdown, but fail during exception handling
            def side_effect_func(*args, **kwargs):
                # Check if 'exception_group_time' is in any of the arguments
                if (
                    "exception_group_time" in args
                    or "exception_group_time" in kwargs.values()
                ):
                    raise Exception("Diagnostic failed")
                return None  # Work normally for startup and shutdown

            mock_log_process.side_effect = side_effect_func
            mock_log_asyncio.side_effect = side_effect_func

            with pytest.raises(ExceptionGroup) as exc_info:
                async with mcp_mod.app_lifespan(None) as context:
                    raise ValueError("Test exception")

            # Verify the ExceptionGroup contains our ValueError
            assert len(exc_info.value.exceptions) == 1
            assert isinstance(exc_info.value.exceptions[0], ValueError)

            # Check that diagnostic logging failure was handled (lines 405-406)
            mock_logger.error.assert_any_call(
                "[mcp_docs_server:app_lifespan] Failed to log diagnostic state: Diagnostic failed"
            )


@pytest.mark.asyncio
async def test_dependency_version_logging_exception(monkeypatch):
    """Test dependency version logging exception handling for coverage."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    # Mock the import statement inside the try block to raise an exception (lines 279-280)
    original_import = __builtins__["__import__"]

    def mock_import(name, *args, **kwargs):
        if name == "uvicorn":
            raise ImportError("Uvicorn module not found")
        return original_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=mock_import):
        with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger:
            # Test the lifespan function which contains the dependency version logging
            async with mcp_mod.app_lifespan(None) as context:
                assert context == {}

            # Verify warning was logged for dependency version failure
            mock_logger.warning.assert_any_call(
                "[mcp_docs_server:app_lifespan] Could not get dependency versions: Uvicorn module not found"
            )


@pytest.mark.asyncio
async def test_docs_chat_session_id_exception(monkeypatch):
    """Test docs_chat handles session ID exceptions with special logging (line 858)."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    session_error = Exception("No valid session ID provided")
    dummy_client = DummyOpenAIClient(exc=session_error)

    with (
        patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger,
        patch(
            "deephaven_mcp.mcp_docs_server._mcp.OpenAIClient", return_value=dummy_client
        ),
    ):
        result = await mcp_mod.docs_chat(context={}, prompt="test", history=None)

        assert not result["success"]
        assert "No valid session ID provided" in result["error"]

        # Check that the special log message was recorded (line 858)
        # and also that the generic one was called right after.
        expected_session_msg = (
            f"[mcp_docs_server:docs_chat] SESSION ERROR: {session_error} - This may indicate that a request was routed to an instance that doesn't have the session state. "
            f"Consider using a shared session store or constraining to a single instance."
        )
        expected_generic_msg = (
            f"[mcp_docs_server:docs_chat] Unexpected error: {session_error}"
        )

        # Use call_args_list to check the sequence of calls
        calls = mock_logger.exception.call_args_list
        assert len(calls) == 2
        assert calls[0].args[0] == expected_session_msg
        assert calls[1].args[0] == expected_generic_msg


# Error Handling Middleware Tests
@pytest.mark.asyncio
async def test_error_handling_middleware_success_path(monkeypatch):
    """Test that successful requests pass through ErrorHandlingMiddleware without modification."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    middleware = mcp_mod.ErrorHandlingMiddleware(app=MagicMock())
    mock_request = MagicMock(spec=Request)
    mock_request.method = "GET"
    mock_request.url.path = "/health"

    # Mock successful call_next
    mock_response = MagicMock()
    mock_call_next = AsyncMock(return_value=mock_response)

    # Call the middleware
    result = await middleware.dispatch(mock_request, mock_call_next)

    # Verify the response is passed through unchanged
    assert result is mock_response
    mock_call_next.assert_called_once_with(mock_request)


@pytest.mark.asyncio
async def test_error_handling_middleware_http_exception(monkeypatch):
    """Test that HTTPException is caught and converted to JSONResponse."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    middleware = mcp_mod.ErrorHandlingMiddleware(app=MagicMock())
    mock_request = MagicMock(spec=Request)
    mock_request.method = "HEAD"
    mock_request.url.path = "/mcp"

    # Mock call_next that raises HTTPException
    http_exc = HTTPException(status_code=405, detail="Method Not Allowed")
    mock_call_next = AsyncMock(side_effect=http_exc)

    with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger:
        # Call the middleware
        result = await middleware.dispatch(mock_request, mock_call_next)

        # Verify it returns a JSONResponse
        assert isinstance(result, JSONResponse)
        assert result.status_code == 405

        # Verify the response content
        content = result.body.decode()
        assert "Method Not Allowed" in content
        assert "HEAD" in content
        assert "/mcp" in content

        # Verify logging occurred
        mock_logger.warning.assert_called_once()
        log_call = mock_logger.warning.call_args[0][0]
        assert "HTTP exception: 405 Method Not Allowed for HEAD /mcp" in log_call


@pytest.mark.asyncio
async def test_error_handling_middleware_http_exception_no_detail(monkeypatch):
    """Test HTTPException handling when detail is None."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    middleware = mcp_mod.ErrorHandlingMiddleware(app=MagicMock())
    mock_request = MagicMock(spec=Request)
    mock_request.method = "HEAD"
    mock_request.url.path = "/mcp"

    # Mock call_next that raises HTTPException with no detail
    http_exc = HTTPException(status_code=404, detail=None)
    mock_call_next = AsyncMock(side_effect=http_exc)

    with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER"):
        # Call the middleware
        result = await middleware.dispatch(mock_request, mock_call_next)

        # Verify it returns a JSONResponse with HTTPException's default message
        assert isinstance(result, JSONResponse)
        assert result.status_code == 404

        # Verify the response content uses HTTPException's default message
        content = result.body.decode()
        assert "Not Found" in content


@pytest.mark.asyncio
async def test_error_handling_middleware_general_exception(monkeypatch):
    """Test that general exceptions are caught and converted to 500 responses."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    middleware = mcp_mod.ErrorHandlingMiddleware(app=MagicMock())
    mock_request = MagicMock(spec=Request)
    mock_request.method = "HEAD"
    mock_request.url.path = "/mcp"

    # Mock call_next that raises a general exception
    general_exc = ValueError("Something went wrong")
    mock_call_next = AsyncMock(side_effect=general_exc)

    with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger:
        # Call the middleware
        result = await middleware.dispatch(mock_request, mock_call_next)

        # Verify it returns a JSONResponse with 500 status
        assert isinstance(result, JSONResponse)
        assert result.status_code == 500

        # Verify the response content
        content = result.body.decode()
        assert "Internal Server Error" in content
        assert "Something went wrong" in content
        assert "HEAD" in content
        assert "/mcp" in content

        # Verify exception logging occurred
        mock_logger.exception.assert_called_once()
        log_call = mock_logger.exception.call_args[0][0]
        assert "Unexpected error for HEAD /mcp" in log_call


def test_custom_http_exception_handler_with_detail(monkeypatch):
    """Test the custom exception handler with a detailed HTTPException."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    mock_request = MagicMock(spec=Request)
    mock_request.method = "PUT"
    mock_request.url.path = "/health"
    exc = HTTPException(status_code=405, detail="Method Not Allowed")

    with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER") as mock_logger:
        # Call the exception handler
        result = mcp_mod.custom_http_exception_handler(mock_request, exc)

        # Verify it returns a JSONResponse
        assert isinstance(result, JSONResponse)
        assert result.status_code == 405

        # Verify the response content
        content = result.body.decode()
        assert "Method Not Allowed" in content
        assert "PUT" in content
        assert "/health" in content
        assert "This endpoint may not support the requested HTTP method" in content

        # Verify logging occurred
        mock_logger.warning.assert_called_once()
        log_call = mock_logger.warning.call_args[0][0]
        assert "HTTP 405: Method Not Allowed for PUT /health" in log_call


def test_custom_http_exception_handler_no_detail(monkeypatch):
    """Test the custom exception handler with HTTPException without detail."""
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server._mcp", None)
    import deephaven_mcp.mcp_docs_server._mcp as mcp_mod

    mock_request = MagicMock(spec=Request)
    mock_request.method = "PUT"
    mock_request.url.path = "/health"
    exc = HTTPException(status_code=404, detail=None)

    with patch("deephaven_mcp.mcp_docs_server._mcp._LOGGER"):
        # Call the exception handler
        result = mcp_mod.custom_http_exception_handler(mock_request, exc)

        # Verify it returns a JSONResponse with HTTPException's default message
        assert isinstance(result, JSONResponse)
        assert result.status_code == 404

        # Verify the response content uses HTTPException's default message
        content = result.body.decode()
        assert "Not Found" in content
