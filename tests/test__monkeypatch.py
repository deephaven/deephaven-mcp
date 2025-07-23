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
    with (patch("deephaven_mcp._monkeypatch.RequestResponseCycle") as MockCycle,):
        import deephaven_mcp._monkeypatch as monkeypatch_mod

        # Setup a dummy orig_run_asgi
        async def dummy_orig_run_asgi(self, app):
            await app("foo")

        MockCycle.run_asgi = dummy_orig_run_asgi
        monkeypatch_mod.monkeypatch_uvicorn_exception_handling()
        run_asgi = MockCycle.run_asgi
        dummy_self = MagicMock()

        async def bad_app(*args, **kwargs):
            raise ValueError("fail!")

        with pytest.raises(ValueError):
            import asyncio

            asyncio.run(run_asgi(dummy_self, bad_app))

        # Check that structured logging was emitted to stderr
        captured = capsys.readouterr()
        assert "Unhandled exception in ASGI application" in captured.err

        # Check that error logs were emitted via Python logging
        assert any(
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

        # Check that GCP logging failure was handled
        captured = capsys.readouterr()
        assert "GCP Logging failed: GCP auth failed" in captured.err


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


def test_monkeypatch_json_logger_exception_handling(caplog, capsys):
    """Test that Python JSON Logger failures are handled gracefully."""
    with (
        patch("deephaven_mcp._monkeypatch.RequestResponseCycle") as MockCycle,
        patch("deephaven_mcp._monkeypatch._get_json_logger") as MockGetJSONLogger,
    ):
        import deephaven_mcp._monkeypatch as monkeypatch_mod

        # Setup a dummy orig_run_asgi
        async def dummy_orig_run_asgi(self, app):
            await app("foo")

        MockCycle.run_asgi = dummy_orig_run_asgi

        # Create a mock logger that fails when error() is called
        mock_json_logger = MagicMock()
        mock_json_logger.error.side_effect = Exception("JSON formatter failed")
        MockGetJSONLogger.return_value = mock_json_logger

        monkeypatch_mod.monkeypatch_uvicorn_exception_handling()
        run_asgi = MockCycle.run_asgi
        dummy_self = MagicMock()

        async def bad_app(*args, **kwargs):
            raise ValueError("test exception")

        with pytest.raises(ValueError):
            import asyncio

            asyncio.run(run_asgi(dummy_self, bad_app))

        # Check that JSON logger failure was handled
        captured = capsys.readouterr()
        assert "Python JSON Logger failed: JSON formatter failed" in captured.err
