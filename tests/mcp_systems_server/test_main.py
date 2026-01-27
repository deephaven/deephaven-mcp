import importlib
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
    ):
        sys.modules.pop("deephaven_mcp.mcp_systems_server.main", None)
        import deephaven_mcp.mcp_systems_server.main as mod

        with (
            patch.object(mod, "_LOGGER", MagicMock()),
            patch.object(mod, "mcp_server", MagicMock()),
            patch.object(mod.logging, "basicConfig", MagicMock()),
        ):
            # Setup mocks
            mod.mcp_server.name = "testserver"
            mock_logger = MagicMock()
            with patch.object(mod, "_LOGGER", mock_logger):
                mod.run_server("stdio")
                mock_logger.info.assert_any_call(
                    f"Starting MCP server 'testserver' with transport=stdio"
                )
                mock_logger.info.assert_any_call(f"MCP server 'testserver' stopped.")
        setup_logging_mock.assert_called_once()
        setup_global_exception_logging_mock.assert_called_once()


def test_run_server_sse():
    with (
        patch(
            "deephaven_mcp._logging.setup_logging", MagicMock()
        ) as setup_logging_mock,
        patch(
            "deephaven_mcp._logging.setup_global_exception_logging", MagicMock()
        ) as setup_global_exception_logging_mock,
    ):
        sys.modules.pop("deephaven_mcp.mcp_systems_server.main", None)
        import deephaven_mcp.mcp_systems_server.main as mod

        with (
            patch.object(mod, "_LOGGER", MagicMock()) as logger_mock,
            patch.object(mod, "mcp_server", MagicMock()) as mcp_server_mock,
            patch.object(mod.logging, "basicConfig", MagicMock()) as basicConfig_mock,
        ):
            mcp_server_mock.name = "testserver"
            mod.run_server("sse")
            logger_mock.info.assert_any_call(
                f"Starting MCP server 'testserver' with transport=sse"
            )
            logger_mock.info.assert_any_call(f"MCP server 'testserver' stopped.")
            mcp_server_mock.run.assert_called_once_with(transport="sse")
        setup_logging_mock.assert_called_once()
        setup_global_exception_logging_mock.assert_called_once()


def test_run_server_stdio():
    with (
        patch(
            "deephaven_mcp._logging.setup_logging", MagicMock()
        ) as setup_logging_mock,
        patch(
            "deephaven_mcp._logging.setup_global_exception_logging", MagicMock()
        ) as setup_global_exception_logging_mock,
    ):
        sys.modules.pop("deephaven_mcp.mcp_systems_server.main", None)
        import deephaven_mcp.mcp_systems_server.main as mod

        with (
            patch.object(mod, "_LOGGER", MagicMock()) as logger_mock,
            patch.object(mod, "mcp_server", MagicMock()) as mcp_server_mock,
            patch.object(mod.logging, "basicConfig", MagicMock()) as basicConfig_mock,
        ):
            mcp_server_mock.name = "testserver"
            mod.run_server("stdio")
            logger_mock.info.assert_any_call(
                f"Starting MCP server 'testserver' with transport=stdio"
            )
            logger_mock.info.assert_any_call(f"MCP server 'testserver' stopped.")
            mcp_server_mock.run.assert_called_once_with(transport="stdio")
        setup_logging_mock.assert_called_once()
        setup_global_exception_logging_mock.assert_called_once()


def test_main_calls_run_server():
    with (
        patch(
            "deephaven_mcp._logging.setup_logging", MagicMock()
        ) as setup_logging_mock,
        patch(
            "deephaven_mcp._logging.setup_global_exception_logging", MagicMock()
        ) as setup_global_exception_logging_mock,
    ):
        sys.modules.pop("deephaven_mcp.mcp_systems_server.main", None)
        import deephaven_mcp.mcp_systems_server.main as mod

        with (
            patch("sys.argv", ["prog", "--transport", "sse"]),
            patch.object(mod, "run_server") as mock_run,
        ):
            mod.main()
            mock_run.assert_called_once_with("sse")
        setup_logging_mock.assert_called_once()
        setup_global_exception_logging_mock.assert_called_once()
