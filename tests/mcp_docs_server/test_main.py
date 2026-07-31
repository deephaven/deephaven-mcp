import logging
import os
import sys
from unittest.mock import MagicMock, patch

import pytest


def test_run_server_streamable_http():
    with (
        patch(
            "deephaven_mcp._logging.setup_logging", MagicMock()
        ) as setup_logging_mock,
        patch(
            "deephaven_mcp._logging.setup_global_exception_logging", MagicMock()
        ) as setup_global_exception_logging_mock,
        patch(
            "deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling",
            MagicMock(),
        ) as monkeypatch_uvicorn_mock,
        patch.dict(os.environ, {"INKEEP_API_KEY": "dummy-key"}),
    ):
        sys.modules.pop("deephaven_mcp.mcp_docs_server.main", None)
        import deephaven_mcp.mcp_docs_server.main as mod

        called = {}

        class DummyServer:
            name = "dummy"

            def run(self, transport=None):
                called["run"] = transport

        with (
            patch.object(mod, "mcp_server", DummyServer()),
            patch.object(mod, "_LOGGER", logging.getLogger("dummy")),
        ):
            mod.run_server()
            assert called["run"] == "streamable-http"
        setup_logging_mock.assert_called_once()
        setup_global_exception_logging_mock.assert_called_once()
        monkeypatch_uvicorn_mock.assert_called_once()


def test_main_invokes_run_server():
    with (
        patch(
            "deephaven_mcp._logging.setup_logging", MagicMock()
        ) as setup_logging_mock,
        patch(
            "deephaven_mcp._logging.setup_global_exception_logging", MagicMock()
        ) as setup_global_exception_logging_mock,
        patch(
            "deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling",
            MagicMock(),
        ) as monkeypatch_uvicorn_mock,
        patch.dict(os.environ, {"INKEEP_API_KEY": "dummy-key"}),
    ):
        sys.modules.pop("deephaven_mcp.mcp_docs_server.main", None)
        import deephaven_mcp.mcp_docs_server.main as mod

        called = {}
        with patch.object(
            mod,
            "run_server",
            lambda: called.setdefault("called", True),
        ):
            mod.main()
            assert called.get("called") is True
        setup_logging_mock.assert_called_once()
        setup_global_exception_logging_mock.assert_called_once()
        monkeypatch_uvicorn_mock.assert_called_once()


def test_run_server_exception_logs_stopped():
    with (
        patch(
            "deephaven_mcp._logging.setup_logging", MagicMock()
        ) as setup_logging_mock,
        patch(
            "deephaven_mcp._logging.setup_global_exception_logging", MagicMock()
        ) as setup_global_exception_logging_mock,
        patch(
            "deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling",
            MagicMock(),
        ) as monkeypatch_uvicorn_mock,
        patch.dict(os.environ, {"INKEEP_API_KEY": "dummy-key"}),
    ):
        sys.modules.pop("deephaven_mcp.mcp_docs_server.main", None)
        import deephaven_mcp.mcp_docs_server.main as mod

        class DummyServer:
            name = "dummy"

            def run(self, transport=None, host=None):
                raise RuntimeError("fail")

        dummy = DummyServer()
        mock_logger_info = MagicMock()
        with (
            patch.object(mod, "mcp_server", dummy),
            patch.object(logging, "basicConfig", lambda **kwargs: None),
            patch.object(mod._LOGGER, "info", mock_logger_info),
        ):
            with pytest.raises(RuntimeError):
                mod.run_server()
        stopped_call_found = False
        for call_args in mock_logger_info.call_args_list:
            if "stopped" in str(call_args[0][0]).lower():
                stopped_call_found = True
                break
        assert stopped_call_found, "_LOGGER.info was not called with 'stopped' message"
        setup_logging_mock.assert_called_once()
        setup_global_exception_logging_mock.assert_called_once()
        monkeypatch_uvicorn_mock.assert_called_once()


def test_main_invocation():
    """Test that main() correctly parses args and calls run_server."""
    # We patch environment, logging, and sys.argv before importing to avoid module-level failures
    with (
        patch.dict(os.environ, {"INKEEP_API_KEY": "dummy-key"}),
        patch("sys.argv", ["deephaven-mcp-docs-server"]),  # Mock clean argv
        patch("deephaven_mcp._logging.setup_logging"),  # Mock logging setup
        patch(
            "deephaven_mcp._logging.setup_global_exception_logging"
        ),  # Mock exception logging
        patch(
            "deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"
        ),  # Mock uvicorn patch
    ):
        # Remove module from cache if it was previously imported and failed
        if "deephaven_mcp.mcp_docs_server.main" in sys.modules:
            del sys.modules["deephaven_mcp.mcp_docs_server.main"]
        if "deephaven_mcp.mcp_docs_server._mcp" in sys.modules:
            del sys.modules["deephaven_mcp.mcp_docs_server._mcp"]

        # Import after environment is patched and modules cleared
        from deephaven_mcp.mcp_docs_server.main import main

        # We patch run_server to prevent the server from actually starting.
        with patch("deephaven_mcp.mcp_docs_server.main.run_server") as mock_run_server:
            # Directly call the main function, which is the entry point.
            main()

            # Verify that run_server was called with no arguments.
            mock_run_server.assert_called_once_with()
