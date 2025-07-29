import importlib
import logging
import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest


def test_run_server_stdio():
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

            def run(self, transport=None, host=None):
                called["run"] = transport, host

        logs = []
        with (
            patch.object(mod, "mcp_server", DummyServer()),
            patch.object(logging, "basicConfig", lambda **kwargs: logs.append(kwargs)),
            patch.object(mod, "_LOGGER", logging.getLogger("dummy")),
        ):
            mod.run_server("stdio")
            assert called["run"][0] == "stdio"
        setup_logging_mock.assert_called_once()
        setup_global_exception_logging_mock.assert_called_once()
        monkeypatch_uvicorn_mock.assert_called_once()


def test_run_server_sse():
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

            def run(self, transport=None, *, host=None):
                called["run"] = (transport, host)

        with (
            patch.object(mod, "mcp_server", DummyServer()),
            patch.object(logging, "basicConfig", lambda **kwargs: None),
            patch.object(mod, "_LOGGER", logging.getLogger("dummy")),
        ):
            mod.run_server("sse")
            assert called["run"][0] == "sse"
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
        with (
            patch.object(
                mod,
                "run_server",
                lambda transport, host=None: called.setdefault(
                    "args", (transport, host)
                ),
            ),
            patch("sys.argv", ["prog", "-t", "sse"]),
        ):
            mod.main()
            assert called["args"] == ("sse", None)
        setup_logging_mock.assert_called_once()
        setup_global_exception_logging_mock.assert_called_once()
        monkeypatch_uvicorn_mock.assert_called_once()


def test_run_server_binds_to_default_host():
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

            def __init__(self, host=None):
                self.host = host

            def run(self, transport=None):
                called["transport"] = transport

        dummy = DummyServer(host="0.0.0.0")
        with (
            patch.object(mod, "mcp_server", dummy),
            patch.object(logging, "basicConfig", lambda **kwargs: None),
            patch.object(mod, "_LOGGER", logging.getLogger("dummy")),
        ):
            mod.run_server("sse")
            assert called["transport"] == "sse"
            assert dummy.host == "0.0.0.0"
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
                mod.run_server("stdio")
        stopped_call_found = False
        for call_args in mock_logger_info.call_args_list:
            if "stopped" in str(call_args[0][0]).lower():
                stopped_call_found = True
                break
        assert stopped_call_found, "_LOGGER.info was not called with 'stopped' message"
        setup_logging_mock.assert_called_once()
        setup_global_exception_logging_mock.assert_called_once()
        monkeypatch_uvicorn_mock.assert_called_once()


def test_docs_module_main_invocation():
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
        result = subprocess.run(
            [sys.executable, "-m", "deephaven_mcp.mcp_docs_server.main", "-t", "stdio"],
            env={**os.environ, "INKEEP_API_KEY": "dummy-key"},
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "Starting MCP server" in result.stdout or result.stderr


def test_main_invocation():
    """Test that main() correctly parses args and calls run_server."""
    # We patch run_server to prevent the server from actually starting.
    with patch("deephaven_mcp.mcp_docs_server.main.run_server") as mock_run_server, patch.dict(
        os.environ, {"INKEEP_API_KEY": "dummy-key"}
    ):
        from deephaven_mcp.mcp_docs_server.main import main

        # Directly call the main function, which is the entry point.
        main()

        # Verify that it called run_server with the default transport.
        mock_run_server.assert_called_once_with("streamable-http")
