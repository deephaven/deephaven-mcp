import logging
import sys
import types
from unittest.mock import MagicMock, patch

import pytest


def test_monkeypatch_uvicorn_exception_handling_warns_and_patches(caplog):
    # Patch before import so the function uses the mock
    with patch("deephaven_mcp._monkeypatch.RequestResponseCycle") as MockCycle:
        import deephaven_mcp._monkeypatch as monkeypatch_mod

        # Setup a dummy orig_run_asgi
        dummy_orig_run_asgi = MagicMock()
        MockCycle.run_asgi = dummy_orig_run_asgi
        with caplog.at_level("WARNING"):
            monkeypatch_mod.monkeypatch_uvicorn_exception_handling()
        # Should warn
        assert any(
            "Monkey-patching Uvicorn's RequestResponseCycle" in r.message
            for r in caplog.records
        )
        # Should patch run_asgi
        assert MockCycle.run_asgi != dummy_orig_run_asgi


def test_monkeypatch_uvicorn_exception_handling_wrapped_app_logs_and_raises(
    caplog, capsys
):
    with (
        patch("deephaven_mcp._monkeypatch.RequestResponseCycle") as MockCycle,
        patch("deephaven_mcp._monkeypatch._get_gcp_logger") as MockGetGCPLogger,
    ):
        import deephaven_mcp._monkeypatch as monkeypatch_mod

        # Setup a dummy orig_run_asgi
        async def dummy_orig_run_asgi(self, app):
            await app("foo")

        MockCycle.run_asgi = dummy_orig_run_asgi

        # Setup mock GCP logger
        mock_gcp_logger = MagicMock()
        MockGetGCPLogger.return_value = mock_gcp_logger

        monkeypatch_mod.monkeypatch_uvicorn_exception_handling()
        run_asgi = MockCycle.run_asgi
        dummy_self = MagicMock()

        async def bad_app(*args, **kwargs):
            raise ValueError("fail!")

        with pytest.raises(ValueError):
            import asyncio

            asyncio.run(run_asgi(dummy_self, bad_app))

        # Check that GCP logger was called for the error
        mock_gcp_logger.error.assert_called_once()
        error_call = mock_gcp_logger.error.call_args
        assert "Unhandled exception in ASGI application" in error_call[0][0]
        assert "ValueError" in error_call[0][0]

        # Verify structured metadata was included
        assert error_call[1]["extra"]["exception_type"] == "ValueError"
        assert error_call[1]["extra"]["exception_message"] == "fail!"
        assert "stack_trace" in error_call[1]["extra"]

        # Check that error logs were NOT propagated to root logger (propagate=False)
        # This prevents duplicate log entries from parent loggers
        assert not any(
            "Unhandled exception in ASGI application" in r.getMessage()
            for r in caplog.records
        )


def test_monkeypatch_gcp_logging_exception_handling(caplog, capsys):
    """Test that GCP Logging failures are handled gracefully."""
    with (
        patch("deephaven_mcp._monkeypatch.RequestResponseCycle") as MockCycle,
        patch("deephaven_mcp._monkeypatch._get_gcp_logger") as MockGetGCPLogger,
    ):
        import deephaven_mcp._monkeypatch as monkeypatch_mod

        # Setup a dummy orig_run_asgi
        async def dummy_orig_run_asgi(self, app):
            await app("foo")

        MockCycle.run_asgi = dummy_orig_run_asgi

        # Make GCP logger fail when error() is called
        mock_gcp_logger = MagicMock()
        mock_gcp_logger.error.side_effect = Exception("GCP auth failed")
        MockGetGCPLogger.return_value = mock_gcp_logger

        monkeypatch_mod.monkeypatch_uvicorn_exception_handling()
        run_asgi = MockCycle.run_asgi
        dummy_self = MagicMock()

        async def bad_app(*args, **kwargs):
            raise ValueError("test exception")

        with pytest.raises(ValueError):
            import asyncio

            asyncio.run(run_asgi(dummy_self, bad_app))

        # Check that GCP logging failure was handled via logging
        assert any(
            "GCP Logging failed: GCP auth failed" in record.message
            for record in caplog.records
        )


def test_monkeypatch_gcp_logging_successful_setup(caplog, capsys):
    """Test that GCP Logging setup code is executed when client creation succeeds."""
    with (
        patch("deephaven_mcp._monkeypatch.RequestResponseCycle") as MockCycle,
        patch("deephaven_mcp._monkeypatch._get_gcp_logger") as MockGetGCPLogger,
    ):
        import deephaven_mcp._monkeypatch as monkeypatch_mod

        # Setup a dummy orig_run_asgi
        async def dummy_orig_run_asgi(self, app):
            await app("foo")

        MockCycle.run_asgi = dummy_orig_run_asgi

        # Create a mock GCP logger that works normally
        mock_gcp_logger = MagicMock()
        MockGetGCPLogger.return_value = mock_gcp_logger

        monkeypatch_mod.monkeypatch_uvicorn_exception_handling()
        run_asgi = MockCycle.run_asgi
        dummy_self = MagicMock()

        async def bad_app(*args, **kwargs):
            raise ValueError("test exception")

        with pytest.raises(ValueError):
            import asyncio

            asyncio.run(run_asgi(dummy_self, bad_app))

        # Verify that GCP logger was called (setup happened lazily)
        MockGetGCPLogger.assert_called_once()
        mock_gcp_logger.error.assert_called_once()


def test_lazy_gcp_logger_initialization():
    """Test that _get_gcp_logger creates and caches the GCP logger properly."""
    import deephaven_mcp._monkeypatch as monkeypatch_mod

    with (
        patch("deephaven_mcp._monkeypatch.gcp_logging.Client") as MockGCPClient,
        patch("deephaven_mcp._monkeypatch.CloudLoggingHandler") as MockHandler,
        patch("deephaven_mcp._monkeypatch.logging.getLogger") as MockGetLogger,
    ):
        # Reset the global logger to None to test initialization
        original_gcp_logger = monkeypatch_mod._gcp_logger
        monkeypatch_mod._gcp_logger = None

        # Mock the GCP client and handler
        mock_client = MagicMock()
        MockGCPClient.return_value = mock_client
        mock_handler = MagicMock()
        MockHandler.return_value = mock_handler

        # Mock the logger
        mock_logger = MagicMock()
        mock_logger.handlers = []  # No existing handlers
        MockGetLogger.return_value = mock_logger

        # First call should create the logger
        result1 = monkeypatch_mod._get_gcp_logger()

        # Verify setup was called
        MockGCPClient.assert_called_once()
        MockHandler.assert_called_once_with(mock_client)
        MockGetLogger.assert_called_once_with("gcp_asgi_errors")
        mock_logger.addHandler.assert_called_once_with(mock_handler)
        assert mock_logger.propagate is False
        assert result1 is mock_logger

        # Second call should return the previously cached logger instance
        result2 = monkeypatch_mod._get_gcp_logger()

        # Verify no additional setup calls were made
        MockGCPClient.assert_called_once()  # Still only called once
        MockHandler.assert_called_once()  # Still only called once
        assert result2 is mock_logger  # Same instance returned

        # Restore original state
        monkeypatch_mod._gcp_logger = original_gcp_logger


def test_is_client_disconnect_error():
    """Test that _is_client_disconnect_error correctly identifies client disconnects."""
    import anyio

    import deephaven_mcp._monkeypatch as monkeypatch_mod

    # Test direct ClosedResourceError
    closed_error = anyio.ClosedResourceError()
    assert monkeypatch_mod._is_client_disconnect_error(closed_error) is True

    # Test other exceptions
    value_error = ValueError("test")
    assert monkeypatch_mod._is_client_disconnect_error(value_error) is False

    # Test exception group containing ClosedResourceError
    # Create a mock exception with 'exceptions' attribute for Python 3.9+ compatibility
    class MockExceptionGroup(Exception):
        def __init__(self, exceptions):
            self.exceptions = exceptions

    # Test exception group with ClosedResourceError
    mock_group = MockExceptionGroup([closed_error, value_error])
    assert monkeypatch_mod._is_client_disconnect_error(mock_group) is True

    # Test exception group without ClosedResourceError
    mock_group_no_closed = MockExceptionGroup([value_error, RuntimeError("test")])
    assert monkeypatch_mod._is_client_disconnect_error(mock_group_no_closed) is False

    # Test nested exceptions via __cause__
    nested_error = RuntimeError("wrapper")
    nested_error.__cause__ = closed_error
    assert monkeypatch_mod._is_client_disconnect_error(nested_error) is True

    # Test nested exceptions via __context__
    context_error = RuntimeError("wrapper")
    context_error.__context__ = closed_error
    assert monkeypatch_mod._is_client_disconnect_error(context_error) is True


def test_monkeypatch_client_disconnect_handling(caplog, capsys):
    """Test that client disconnects are logged at DEBUG level and don't re-raise."""
    with (
        patch("deephaven_mcp._monkeypatch.RequestResponseCycle") as MockCycle,
        patch("deephaven_mcp._monkeypatch._get_gcp_logger") as MockGetGCPLogger,
    ):
        import anyio

        import deephaven_mcp._monkeypatch as monkeypatch_mod

        # Setup a dummy orig_run_asgi
        async def dummy_orig_run_asgi(self, app):
            await app("foo")

        MockCycle.run_asgi = dummy_orig_run_asgi

        # Setup mock GCP logger
        mock_gcp_logger = MagicMock()
        MockGetGCPLogger.return_value = mock_gcp_logger

        monkeypatch_mod.monkeypatch_uvicorn_exception_handling()
        run_asgi = MockCycle.run_asgi
        dummy_self = MagicMock()

        # Create app that raises ClosedResourceError (client disconnect)
        async def disconnect_app(*args, **kwargs):
            raise anyio.ClosedResourceError()

        # Should NOT raise - client disconnects should be handled gracefully
        import asyncio

        result = asyncio.run(run_asgi(dummy_self, disconnect_app))
        assert result is None  # Should return None, not raise

        # Verify DEBUG logging was called for client disconnect
        mock_gcp_logger.debug.assert_called_once()
        debug_call = mock_gcp_logger.debug.call_args
        assert "Client disconnect detected" in debug_call[0][0]
        assert debug_call[1]["extra"]["event_type"] == "client_disconnect"

        # Verify ERROR logging was NOT called (no server error)
        mock_gcp_logger.error.assert_not_called()


def test_monkeypatch_client_disconnect_logging_failure(caplog, capsys):
    """Test that client disconnect logging failures are handled gracefully."""
    with (
        patch("deephaven_mcp._monkeypatch.RequestResponseCycle") as MockCycle,
        patch("deephaven_mcp._monkeypatch._get_gcp_logger") as MockGetGCPLogger,
    ):
        import anyio

        import deephaven_mcp._monkeypatch as monkeypatch_mod

        # Setup a dummy orig_run_asgi
        async def dummy_orig_run_asgi(self, app):
            await app("foo")

        MockCycle.run_asgi = dummy_orig_run_asgi

        # Setup mock GCP logger that fails when debug() is called
        mock_gcp_logger = MagicMock()
        mock_gcp_logger.debug.side_effect = RuntimeError("GCP logging failed")
        MockGetGCPLogger.return_value = mock_gcp_logger

        monkeypatch_mod.monkeypatch_uvicorn_exception_handling()
        run_asgi = MockCycle.run_asgi
        dummy_self = MagicMock()

        # Create app that raises ClosedResourceError (client disconnect)
        async def disconnect_app(*args, **kwargs):
            raise anyio.ClosedResourceError()

        # Should NOT raise - client disconnects should be handled gracefully even if logging fails
        import asyncio

        result = asyncio.run(run_asgi(dummy_self, disconnect_app))
        assert result is None  # Should return None, not raise

        # Verify that the logging failure was handled via logging
        assert any(
            "Client disconnect logging failed: GCP logging failed" in record.message
            for record in caplog.records
        )

        # Verify DEBUG logging was attempted
        mock_gcp_logger.debug.assert_called_once()

        # Verify ERROR logging was NOT called (no server error)
        mock_gcp_logger.error.assert_not_called()
