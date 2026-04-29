"""
Tests for deephaven_mcp.mcp_systems_server.server.
"""

from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

import deephaven_mcp.mcp_systems_server.server as server
from deephaven_mcp._exceptions import ConfigurationError

# ---------------------------------------------------------------------------
# _parse_args
# ---------------------------------------------------------------------------


def test_parse_args_defaults(monkeypatch):
    """Returns (None, '127.0.0.1', default_port) when no args or env vars are set."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)
    with patch("sys.argv", ["prog"]):
        config_path, host, port = server._parse_args("desc", 8003)
    assert config_path is None
    assert host == "127.0.0.1"
    assert port == 8003


def test_parse_args_cli_takes_priority(monkeypatch):
    """CLI args take priority over env vars."""
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/env/conf.json")
    monkeypatch.setenv("MCP_HOST", "1.2.3.4")
    monkeypatch.setenv("MCP_PORT", "1111")
    with patch(
        "sys.argv",
        ["prog", "--config", "/cli/conf.json", "--host", "0.0.0.0", "--port", "9999"],
    ):
        config_path, host, port = server._parse_args("desc", 8003)
    assert config_path == "/cli/conf.json"
    assert host == "0.0.0.0"
    assert port == 9999


def test_parse_args_env_var_fallback(monkeypatch):
    """Env vars used as fallback when CLI args absent."""
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/env/conf.json")
    monkeypatch.setenv("MCP_HOST", "1.2.3.4")
    monkeypatch.setenv("MCP_PORT", "5555")
    with patch("sys.argv", ["prog"]):
        config_path, host, port = server._parse_args("desc", 8003)
    assert config_path == "/env/conf.json"
    assert host == "1.2.3.4"
    assert port == 5555


def test_parse_args_different_default_port(monkeypatch):
    """Different default_port values work correctly."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)
    with patch("sys.argv", ["prog"]):
        _, _, port = server._parse_args("desc", 8002)
    assert port == 8002


# ---------------------------------------------------------------------------
# _setup_env
# ---------------------------------------------------------------------------


def test_setup_env_calls_all_setup_functions():
    """_setup_env calls all four setup functions exactly once."""
    with (
        patch("deephaven_mcp._logging.setup_logging") as mock_setup_logging,
        patch(
            "deephaven_mcp._logging.setup_global_exception_logging"
        ) as mock_global_exc,
        patch("deephaven_mcp._logging.setup_signal_handler_logging") as mock_signal,
        patch(
            "deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"
        ) as mock_monkeypatch,
    ):
        server._setup_env()
    mock_setup_logging.assert_called_once()
    mock_global_exc.assert_called_once()
    mock_signal.assert_called_once()
    mock_monkeypatch.assert_called_once()


# ---------------------------------------------------------------------------
# _register_shared_tools
# ---------------------------------------------------------------------------


def test_register_shared_tools_registers_all_shared_modules():
    """_register_shared_tools calls register_tools on every module in _SHARED_TOOLS."""
    mock_server = MagicMock()
    mock_modules = [MagicMock() for _ in server._SHARED_TOOLS]

    with patch.object(server, "_SHARED_TOOLS", tuple(mock_modules)):
        server._register_shared_tools(mock_server)

    for mock_module in mock_modules:
        mock_module.register_tools.assert_called_once_with(mock_server)


def test_shared_tools_contains_expected_modules():
    """_SHARED_TOOLS contains exactly the modules shared by both servers."""
    from deephaven_mcp.mcp_systems_server._tools import script, session, table

    assert session in server._SHARED_TOOLS
    assert table in server._SHARED_TOOLS
    assert script in server._SHARED_TOOLS


def test_shared_tools_excludes_reload():
    """reload is NOT in _SHARED_TOOLS; each server registers its own variant."""
    from deephaven_mcp.mcp_systems_server._tools import reload

    assert reload not in server._SHARED_TOOLS


def test_shared_tools_excludes_enterprise_exclusive_modules():
    """Enterprise-exclusive modules are NOT in _SHARED_TOOLS."""
    from deephaven_mcp.mcp_systems_server._tools import catalog, pq, session_enterprise

    assert session_enterprise not in server._SHARED_TOOLS
    assert catalog not in server._SHARED_TOOLS
    assert pq not in server._SHARED_TOOLS


# ---------------------------------------------------------------------------
# _run_startup_validation_or_exit
# ---------------------------------------------------------------------------


def test_run_startup_validation_or_exit_success_passes_manager_to_loader():
    """Success path: a single manager instance is built and handed to the loader."""
    mock_class = MagicMock()
    mock_instance = MagicMock()
    mock_class.return_value = mock_instance
    mock_loader = AsyncMock(return_value=(900.0, "tok"))

    with patch("sys.exit") as mock_exit:
        result = server._run_startup_validation_or_exit(
            "/some/config.json", mock_class, mock_loader, "community"
        )

    assert result == (900.0, "tok")
    mock_class.assert_called_once_with(config_path="/some/config.json")
    mock_loader.assert_awaited_once_with(mock_instance)
    mock_exit.assert_not_called()


def test_run_startup_validation_or_exit_config_error_exits():
    """ConfigurationError raised by the loader triggers sys.exit(1)."""
    mock_class = MagicMock()
    mock_class.return_value = MagicMock()
    mock_loader = AsyncMock(side_effect=ConfigurationError("missing required field"))

    with pytest.raises(SystemExit) as exc_info:
        server._run_startup_validation_or_exit(
            "/bad/config.json", mock_class, mock_loader, "enterprise"
        )
    assert exc_info.value.code == 1


def test_run_startup_validation_or_exit_runtime_error_exits():
    """Any unexpected exception from the loader also triggers sys.exit(1)."""
    mock_class = MagicMock()
    mock_class.return_value = MagicMock()
    mock_loader = AsyncMock(
        side_effect=RuntimeError("Environment variable DH_MCP_CONFIG_FILE is not set.")
    )

    with pytest.raises(SystemExit) as exc_info:
        server._run_startup_validation_or_exit(
            None, mock_class, mock_loader, "community"
        )
    assert exc_info.value.code == 1


def test_run_startup_validation_or_exit_forwards_none_config_path():
    """config_path=None is forwarded to the manager-class constructor."""
    mock_class = MagicMock()
    mock_class.return_value = MagicMock()
    mock_loader = AsyncMock(return_value=60.0)

    with patch("sys.exit"):
        server._run_startup_validation_or_exit(
            None, mock_class, mock_loader, "community"
        )
    mock_class.assert_called_once_with(config_path=None)


def test_run_startup_validation_or_exit_uses_label_in_log_prefix():
    """The label parameter is used verbatim in log-line prefixes."""
    mock_class = MagicMock()
    mock_class.return_value = MagicMock()
    mock_loader = AsyncMock(return_value="payload")
    mock_logger = MagicMock()

    with patch("deephaven_mcp.mcp_systems_server.server._LOGGER", mock_logger):
        result = server._run_startup_validation_or_exit(
            None, mock_class, mock_loader, "some-label"
        )

    assert result == "payload"
    # Both the pre- and post-validation info messages use the label.
    info_messages = [c.args[0] for c in mock_logger.info.call_args_list]
    assert any("[some-label]" in m for m in info_messages)


def test_run_startup_validation_or_exit_error_log_includes_label_and_error():
    """Error log on failure prefixes with label and includes the exception text."""
    mock_class = MagicMock()
    mock_class.return_value = MagicMock()
    mock_loader = AsyncMock(side_effect=ConfigurationError("boom"))
    mock_logger = MagicMock()

    with (
        patch("deephaven_mcp.mcp_systems_server.server._LOGGER", mock_logger),
        pytest.raises(SystemExit),
    ):
        server._run_startup_validation_or_exit(
            None, mock_class, mock_loader, "community"
        )

    error_msgs = [c.args[0] for c in mock_logger.error.call_args_list]
    assert any("[community]" in m and "boom" in m for m in error_msgs)


# ---------------------------------------------------------------------------
# enterprise()
# ---------------------------------------------------------------------------


def test_enterprise_defaults(monkeypatch):
    """enterprise() uses default host/port when no args or env vars are set."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-enterprise"
    mock_lifespan_fn = MagicMock()

    with (
        patch("sys.argv", ["dh-mcp-enterprise-server"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_startup_validation_or_exit",
            return_value=(1800.0, (["password"], False)),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server._build_enterprise_middleware",
            return_value=[],
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_with_middleware"
        ) as mock_run,
        patch("deephaven_mcp.mcp_systems_server.server.SessionRegistryManager"),
        patch(
            "deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server
        ) as mock_fastmcp_cls,
        patch(
            "deephaven_mcp.mcp_systems_server.server.make_enterprise_lifespan",
            return_value=mock_lifespan_fn,
        ) as mock_lifespan,
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_enterprise"),
        patch("deephaven_mcp.mcp_systems_server.server.catalog"),
        patch("deephaven_mcp.mcp_systems_server.server.pq"),
    ):
        server.enterprise()

    mock_fastmcp_cls.assert_called_once_with(
        "deephaven-mcp-enterprise", lifespan=ANY, host="127.0.0.1", port=8002
    )
    mock_run.assert_called_once_with(mock_server, [], "127.0.0.1", 8002)
    mock_lifespan.assert_called_once_with(ANY, None)


def test_enterprise_cli_args(monkeypatch):
    """enterprise() uses --config, --host, and --port when provided."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-enterprise"

    with (
        patch(
            "sys.argv",
            [
                "dh-mcp-enterprise-server",
                "--config",
                "/my/dhe.json",
                "--host",
                "0.0.0.0",
                "--port",
                "9001",
            ],
        ),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_startup_validation_or_exit",
            return_value=(1800.0, (["password"], False)),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server._build_enterprise_middleware",
            return_value=[],
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_with_middleware"
        ) as mock_run,
        patch("deephaven_mcp.mcp_systems_server.server.SessionRegistryManager"),
        patch(
            "deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server
        ) as mock_fastmcp_cls,
        patch(
            "deephaven_mcp.mcp_systems_server.server.make_enterprise_lifespan",
            return_value=MagicMock(),
        ) as mock_lifespan,
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_enterprise"),
        patch("deephaven_mcp.mcp_systems_server.server.catalog"),
        patch("deephaven_mcp.mcp_systems_server.server.pq"),
    ):
        server.enterprise()

    mock_fastmcp_cls.assert_called_once_with(
        "deephaven-mcp-enterprise", lifespan=ANY, host="0.0.0.0", port=9001
    )
    mock_run.assert_called_once_with(mock_server, [], "0.0.0.0", 9001)
    mock_lifespan.assert_called_once_with(ANY, "/my/dhe.json")


def test_enterprise_env_var_fallback(monkeypatch):
    """enterprise() falls back to env vars when CLI args absent."""
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/env/dhe.json")
    monkeypatch.setenv("MCP_HOST", "10.0.0.1")
    monkeypatch.setenv("MCP_PORT", "7777")

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-enterprise"

    with (
        patch("sys.argv", ["dh-mcp-enterprise-server"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_startup_validation_or_exit",
            return_value=(1800.0, (["password"], False)),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server._build_enterprise_middleware",
            return_value=[],
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_with_middleware"
        ) as mock_run,
        patch("deephaven_mcp.mcp_systems_server.server.SessionRegistryManager"),
        patch(
            "deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server
        ) as mock_fastmcp_cls,
        patch(
            "deephaven_mcp.mcp_systems_server.server.make_enterprise_lifespan",
            return_value=MagicMock(),
        ) as mock_lifespan,
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_enterprise"),
        patch("deephaven_mcp.mcp_systems_server.server.catalog"),
        patch("deephaven_mcp.mcp_systems_server.server.pq"),
    ):
        server.enterprise()

    mock_fastmcp_cls.assert_called_once_with(
        "deephaven-mcp-enterprise", lifespan=ANY, host="10.0.0.1", port=7777
    )
    mock_run.assert_called_once_with(mock_server, [], "10.0.0.1", 7777)
    mock_lifespan.assert_called_once_with(ANY, "/env/dhe.json")


def test_enterprise_registers_shared_and_exclusive_tools(monkeypatch):
    """enterprise() registers shared tools, the enterprise reload variant, and DHE-exclusive tools."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-enterprise"
    mock_register_shared = MagicMock()
    mock_reload = MagicMock()
    mock_session_enterprise = MagicMock()
    mock_catalog = MagicMock()
    mock_pq = MagicMock()

    with (
        patch("sys.argv", ["dh-mcp-enterprise-server"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_startup_validation_or_exit",
            return_value=(1800.0, (["password"], False)),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server._build_enterprise_middleware",
            return_value=[],
        ),
        patch("deephaven_mcp.mcp_systems_server.server._run_with_middleware"),
        patch("deephaven_mcp.mcp_systems_server.server.SessionRegistryManager"),
        patch(
            "deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server.make_enterprise_lifespan",
            return_value=MagicMock(),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server._register_shared_tools",
            mock_register_shared,
        ),
        patch("deephaven_mcp.mcp_systems_server.server.reload", mock_reload),
        patch(
            "deephaven_mcp.mcp_systems_server.server.session_enterprise",
            mock_session_enterprise,
        ),
        patch("deephaven_mcp.mcp_systems_server.server.catalog", mock_catalog),
        patch("deephaven_mcp.mcp_systems_server.server.pq", mock_pq),
    ):
        server.enterprise()

    mock_register_shared.assert_called_once_with(mock_server)
    mock_reload.register_enterprise_tools.assert_called_once_with(mock_server)
    mock_session_enterprise.register_tools.assert_called_once_with(mock_server)
    mock_catalog.register_tools.assert_called_once_with(mock_server)
    mock_pq.register_tools.assert_called_once_with(mock_server)


def test_enterprise_logs_stopped_onserver_exit(monkeypatch):
    """enterprise() logs server stopped even when server.run raises."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-enterprise"
    mock_logger = MagicMock()

    with (
        patch("sys.argv", ["dh-mcp-enterprise-server"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_startup_validation_or_exit",
            return_value=(1800.0, (["password"], False)),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server._build_enterprise_middleware",
            return_value=[],
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_with_middleware",
            side_effect=RuntimeError("server crashed"),
        ),
        patch("deephaven_mcp.mcp_systems_server.server.SessionRegistryManager"),
        patch(
            "deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server.make_enterprise_lifespan",
            return_value=MagicMock(),
        ),
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_enterprise"),
        patch("deephaven_mcp.mcp_systems_server.server.catalog"),
        patch("deephaven_mcp.mcp_systems_server.server.pq"),
        patch("deephaven_mcp.mcp_systems_server.server._LOGGER", mock_logger),
    ):
        with pytest.raises(RuntimeError, match="server crashed"):
            server.enterprise()

    mock_logger.info.assert_any_call(
        "[enterprise] MCP server 'deephaven-mcp-enterprise' stopped."
    )


# ---------------------------------------------------------------------------
# community()
# ---------------------------------------------------------------------------


def test_community_defaults(monkeypatch):
    """community() uses default host/port when no args or env vars are set."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-community"
    mock_run = MagicMock()

    with (
        patch("sys.argv", ["dh-mcp-community-server"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_startup_validation_or_exit",
            return_value=(1800.0, "tok"),
        ),
        patch("deephaven_mcp.mcp_systems_server.server.SessionRegistryManager"),
        patch(
            "deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server
        ) as mock_fastmcp_cls,
        patch(
            "deephaven_mcp.mcp_systems_server.server.make_community_lifespan",
            return_value=MagicMock(),
        ) as mock_lifespan,
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_community"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_with_middleware",
            mock_run,
        ),
    ):
        server.community()

    mock_fastmcp_cls.assert_called_once_with(
        "deephaven-mcp-community", lifespan=ANY, host="127.0.0.1", port=8003
    )
    mock_run.assert_called_once()
    mock_lifespan.assert_called_once_with(ANY, None)


def test_community_cli_args(monkeypatch):
    """community() uses --config, --host, and --port when provided."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-community"

    with (
        patch(
            "sys.argv",
            [
                "dh-mcp-community-server",
                "--config",
                "/my/dhc.json",
                "--host",
                "0.0.0.0",
                "--port",
                "9002",
            ],
        ),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_startup_validation_or_exit",
            return_value=(1800.0, "tok"),
        ),
        patch("deephaven_mcp.mcp_systems_server.server.SessionRegistryManager"),
        patch(
            "deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server
        ) as mock_fastmcp_cls,
        patch(
            "deephaven_mcp.mcp_systems_server.server.make_community_lifespan",
            return_value=MagicMock(),
        ) as mock_lifespan,
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_community"),
        patch("deephaven_mcp.mcp_systems_server.server._run_with_middleware"),
    ):
        server.community()

    mock_fastmcp_cls.assert_called_once_with(
        "deephaven-mcp-community", lifespan=ANY, host="0.0.0.0", port=9002
    )
    mock_lifespan.assert_called_once_with(ANY, "/my/dhc.json")


def test_community_env_var_fallback(monkeypatch):
    """community() falls back to env vars when CLI args absent."""
    monkeypatch.setenv("DH_MCP_CONFIG_FILE", "/env/dhc.json")
    monkeypatch.setenv("MCP_HOST", "192.168.1.1")
    monkeypatch.setenv("MCP_PORT", "6666")

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-community"

    with (
        patch("sys.argv", ["dh-mcp-community-server"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_startup_validation_or_exit",
            return_value=(1800.0, "tok"),
        ),
        patch("deephaven_mcp.mcp_systems_server.server.SessionRegistryManager"),
        patch(
            "deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server
        ) as mock_fastmcp_cls,
        patch(
            "deephaven_mcp.mcp_systems_server.server.make_community_lifespan",
            return_value=MagicMock(),
        ) as mock_lifespan,
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_community"),
        patch("deephaven_mcp.mcp_systems_server.server._run_with_middleware"),
    ):
        server.community()

    mock_fastmcp_cls.assert_called_once_with(
        "deephaven-mcp-community", lifespan=ANY, host="192.168.1.1", port=6666
    )
    mock_lifespan.assert_called_once_with(ANY, "/env/dhc.json")


def test_community_registers_shared_and_exclusive_tools(monkeypatch):
    """community() registers shared tools, the community reload variant, and DHC-exclusive tools."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-community"
    mock_register_shared = MagicMock()
    mock_reload = MagicMock()
    mock_session_community = MagicMock()

    with (
        patch("sys.argv", ["dh-mcp-community-server"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_startup_validation_or_exit",
            return_value=(1800.0, "tok"),
        ),
        patch("deephaven_mcp.mcp_systems_server.server.SessionRegistryManager"),
        patch(
            "deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server.make_community_lifespan",
            return_value=MagicMock(),
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server._register_shared_tools",
            mock_register_shared,
        ),
        patch("deephaven_mcp.mcp_systems_server.server.reload", mock_reload),
        patch(
            "deephaven_mcp.mcp_systems_server.server.session_community",
            mock_session_community,
        ),
        patch("deephaven_mcp.mcp_systems_server.server._run_with_middleware"),
    ):
        server.community()

    mock_register_shared.assert_called_once_with(mock_server)
    mock_reload.register_community_tools.assert_called_once_with(mock_server)
    mock_session_community.register_tools.assert_called_once_with(mock_server)


def test_community_logs_stopped_onserver_exit(monkeypatch):
    """community() logs server stopped even when the runner raises."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-community"
    mock_logger = MagicMock()

    with (
        patch("sys.argv", ["dh-mcp-community-server"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_startup_validation_or_exit",
            return_value=(1800.0, "tok"),
        ),
        patch("deephaven_mcp.mcp_systems_server.server.SessionRegistryManager"),
        patch(
            "deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server.make_community_lifespan",
            return_value=MagicMock(),
        ),
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_community"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_with_middleware",
            side_effect=RuntimeError("server crashed"),
        ),
        patch("deephaven_mcp.mcp_systems_server.server._LOGGER", mock_logger),
    ):
        with pytest.raises(RuntimeError, match="server crashed"):
            server.community()

    mock_logger.info.assert_any_call(
        "[community] MCP server 'deephaven-mcp-community' stopped."
    )


# ---------------------------------------------------------------------------
# enterprise() / community() — validation is called with correct args
# ---------------------------------------------------------------------------


def test_enterprise_validates_config_before_start(monkeypatch):
    """enterprise() calls _run_startup_validation_or_exit with the enterprise manager, loader, and label."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-enterprise"
    mock_validate = MagicMock(return_value=(1800.0, (["password"], False)))

    with (
        patch("sys.argv", ["dh-mcp-enterprise-server", "--config", "/my/dhe.json"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_startup_validation_or_exit",
            mock_validate,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server._build_enterprise_middleware",
            return_value=[],
        ),
        patch("deephaven_mcp.mcp_systems_server.server._run_with_middleware"),
        patch("deephaven_mcp.mcp_systems_server.server.SessionRegistryManager"),
        patch(
            "deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server.make_enterprise_lifespan",
            return_value=MagicMock(),
        ),
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_enterprise"),
        patch("deephaven_mcp.mcp_systems_server.server.catalog"),
        patch("deephaven_mcp.mcp_systems_server.server.pq"),
    ):
        server.enterprise()

    mock_validate.assert_called_once_with(
        "/my/dhe.json",
        server.EnterpriseServerConfigManager,
        server._load_enterprise_startup_state,
        "enterprise",
    )


def test_community_validates_config_before_start(monkeypatch):
    """community() calls _run_startup_validation_or_exit with the community manager, loader, and label."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-community"
    mock_validate = MagicMock(return_value=(1800.0, "tok"))

    with (
        patch("sys.argv", ["dh-mcp-community-server", "--config", "/my/dhc.json"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_startup_validation_or_exit",
            mock_validate,
        ),
        patch("deephaven_mcp.mcp_systems_server.server.SessionRegistryManager"),
        patch(
            "deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server.make_community_lifespan",
            return_value=MagicMock(),
        ),
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_community"),
        patch("deephaven_mcp.mcp_systems_server.server._run_with_middleware"),
    ):
        server.community()

    mock_validate.assert_called_once_with(
        "/my/dhc.json",
        server.CommunityServerConfigManager,
        server._load_community_startup_state,
        "community",
    )


# ---------------------------------------------------------------------------
# _load_community_startup_state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_community_startup_state_returns_timeout_and_psk():
    mock_manager = MagicMock()
    mock_manager.get_config = AsyncMock(return_value={"auth": {"psk": "s3cret"}})
    mock_manager.get_mcp_session_idle_timeout_seconds = AsyncMock(return_value=900.0)
    idle, psk = await server._load_community_startup_state(mock_manager)
    assert idle == 900.0
    assert psk == "s3cret"


@pytest.mark.asyncio
async def test_load_community_startup_state_enabled_false_returns_none_psk():
    """auth.enabled = false collapses to a None PSK (loopback-only deployment)."""
    mock_manager = MagicMock()
    mock_manager.get_config = AsyncMock(return_value={"auth": {"enabled": False}})
    mock_manager.get_mcp_session_idle_timeout_seconds = AsyncMock(return_value=900.0)
    idle, psk = await server._load_community_startup_state(mock_manager)
    assert idle == 900.0
    assert psk is None


@pytest.mark.asyncio
async def test_load_community_startup_state_resolves_psk_from_env(monkeypatch):
    """auth.psk_env_var is resolved through resolve_secret_field."""
    monkeypatch.setenv("DH_TEST_STARTUP_PSK", "env-secret")
    mock_manager = MagicMock()
    mock_manager.get_config = AsyncMock(
        return_value={"auth": {"psk_env_var": "DH_TEST_STARTUP_PSK"}}
    )
    mock_manager.get_mcp_session_idle_timeout_seconds = AsyncMock(return_value=900.0)
    idle, psk = await server._load_community_startup_state(mock_manager)
    assert idle == 900.0
    assert psk == "env-secret"


@pytest.mark.asyncio
async def test_load_community_startup_state_psk_env_var_unset_raises(monkeypatch):
    """auth.psk_env_var naming an unset env var surfaces ConfigurationError."""
    monkeypatch.delenv("DH_TEST_STARTUP_PSK", raising=False)
    mock_manager = MagicMock()
    mock_manager.get_config = AsyncMock(
        return_value={"auth": {"psk_env_var": "DH_TEST_STARTUP_PSK"}}
    )
    mock_manager.get_mcp_session_idle_timeout_seconds = AsyncMock(return_value=900.0)
    with pytest.raises(ConfigurationError, match="DH_TEST_STARTUP_PSK"):
        await server._load_community_startup_state(mock_manager)


# ---------------------------------------------------------------------------
# _is_loopback_host
# ---------------------------------------------------------------------------


def test_is_loopback_host_localhost():
    assert server._is_loopback_host("localhost")
    assert server._is_loopback_host("LOCALHOST")


def test_is_loopback_host_ipv4_loopback():
    assert server._is_loopback_host("127.0.0.1")
    assert server._is_loopback_host("127.5.0.1")  # entire /8 is loopback


def test_is_loopback_host_ipv6_loopback():
    assert server._is_loopback_host("::1")


def test_is_loopback_host_public_ipv4():
    assert not server._is_loopback_host("192.168.1.1")
    assert not server._is_loopback_host("0.0.0.0")


def test_is_loopback_host_unresolvable_returns_false():
    # getaddrinfo on a bogus name fails; function returns False.
    assert not server._is_loopback_host("definitely-not-a-real-host.invalid")


def test_is_loopback_host_hostname_resolving_to_loopback():
    with patch(
        "socket.getaddrinfo",
        return_value=[(0, 0, 0, "", ("127.0.0.1", 0))],
    ):
        assert server._is_loopback_host("some.name")


def test_is_loopback_host_hostname_resolving_to_mixed():
    with patch(
        "socket.getaddrinfo",
        return_value=[
            (0, 0, 0, "", ("127.0.0.1", 0)),
            (0, 0, 0, "", ("8.8.8.8", 0)),
        ],
    ):
        assert not server._is_loopback_host("some.name")


# ---------------------------------------------------------------------------
# _build_community_middleware
# ---------------------------------------------------------------------------


def test_build_community_middleware_with_token_returns_single_entry():
    mw = server._build_community_middleware("tok", "127.0.0.1")
    assert len(mw) == 1


def test_build_community_middleware_disabled_on_loopback_returns_empty():
    mw = server._build_community_middleware(None, "127.0.0.1")
    assert mw == []


def test_build_community_middleware_disabled_on_non_loopback_exits():
    with pytest.raises(SystemExit) as exc_info:
        server._build_community_middleware(None, "0.0.0.0")
    assert exc_info.value.code == 1


def test_build_community_middleware_refuse_message_includes_remediation(caplog):
    """The refuse-to-start error must teach a non-expert how to fix it.

    Asserts the actionable phrases are present so a future 'concision'
    PR cannot quietly strip the remediation guidance.
    """
    with caplog.at_level("ERROR"), pytest.raises(SystemExit):
        server._build_community_middleware(None, "0.0.0.0")
    text = caplog.text
    # User must know how to bind to loopback...
    assert "127.0.0.1" in text
    assert "--host" in text or "MCP_HOST" in text
    # ...and how to enable auth instead.
    assert "psk" in text
    assert "psk_env_var" in text


def test_build_community_middleware_disabled_warning_includes_remediation(caplog):
    """The auth-disabled WARNING must be unmissable and self-explanatory."""
    with caplog.at_level("WARNING"):
        server._build_community_middleware(None, "127.0.0.1")
    text = caplog.text
    assert "AUTHENTICATION IS DISABLED" in text
    assert "local development only" in text
    # Spells out the exposure surface for the operator.
    assert "this same machine" in text or "this machine" in text


def test_build_community_middleware_enabled_logs_info(caplog):
    """The success path must log positive confirmation including the header name."""
    with caplog.at_level("INFO"):
        server._build_community_middleware("tok", "127.0.0.1")
    text = caplog.text
    assert "Authentication is ENABLED" in text
    assert "X-Deephaven-PSK" in text


# ---------------------------------------------------------------------------
# _run_with_middleware
# ---------------------------------------------------------------------------


def test_run_with_middleware_adds_each_entry_and_starts_uvicorn():
    from starlette.middleware import Middleware

    fake_app = MagicMock()
    mock_server = MagicMock()
    mock_server.streamable_http_app.return_value = fake_app
    mw = [Middleware(MagicMock, foo="bar")]

    with (
        patch("uvicorn.Config") as mock_config_cls,
        patch("uvicorn.Server") as mock_server_cls,
    ):
        server._run_with_middleware(mock_server, mw, "127.0.0.1", 8003)

    fake_app.add_middleware.assert_called_once_with(mw[0].cls, foo="bar")
    mock_config_cls.assert_called_once_with(
        fake_app, host="127.0.0.1", port=8003, log_config=None
    )
    mock_server_cls.assert_called_once_with(mock_config_cls.return_value)
    mock_server_cls.return_value.run.assert_called_once()


def test_run_with_middleware_empty_list_still_runs_uvicorn():
    fake_app = MagicMock()
    mock_server = MagicMock()
    mock_server.streamable_http_app.return_value = fake_app

    with (
        patch("uvicorn.Config") as mock_config_cls,
        patch("uvicorn.Server") as mock_server_cls,
    ):
        server._run_with_middleware(mock_server, [], "127.0.0.1", 8003)

    fake_app.add_middleware.assert_not_called()
    mock_server_cls.return_value.run.assert_called_once()
    assert mock_config_cls.called


# ---------------------------------------------------------------------------
# _load_enterprise_startup_state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_enterprise_startup_state_returns_idle_backends_and_flag():
    mock_manager = MagicMock()
    mock_manager.get_config = AsyncMock(
        return_value={
            "system_name": "prod",
            "connection_json_url": "u",
            "auth": {"backends": ["password"], "allow_effective_user": True},
        }
    )
    mock_manager.get_mcp_session_idle_timeout_seconds = AsyncMock(return_value=900.0)
    idle, (backends, allow) = await server._load_enterprise_startup_state(mock_manager)
    assert idle == 900.0
    assert backends == ["password"]
    assert allow is True


# ---------------------------------------------------------------------------
# _build_enterprise_middleware
# ---------------------------------------------------------------------------


def test_build_enterprise_middleware_password_only():
    from deephaven_mcp.auth.backends import (
        PasswordBackend,
    )

    mw = server._build_enterprise_middleware((["password"], False), "127.0.0.1")
    assert len(mw) == 1
    backends = mw[0].kwargs["backends"]
    assert len(backends) == 1
    assert isinstance(backends[0], PasswordBackend)
    assert backends[0].allow_effective_user is False


def test_build_enterprise_middleware_password_with_effective_user():
    mw = server._build_enterprise_middleware((["password"], True), "127.0.0.1")
    backends = mw[0].kwargs["backends"]
    assert backends[0].allow_effective_user is True


def test_build_enterprise_middleware_private_key_only():
    from deephaven_mcp.auth.backends import (
        PrivateKeyBackend,
    )

    mw = server._build_enterprise_middleware((["private_key"], False), "127.0.0.1")
    backends = mw[0].kwargs["backends"]
    assert len(backends) == 1
    assert isinstance(backends[0], PrivateKeyBackend)


def test_build_enterprise_middleware_both_in_declared_order():
    from deephaven_mcp.auth.backends import (
        PasswordBackend,
        PrivateKeyBackend,
    )

    mw = server._build_enterprise_middleware(
        (["private_key", "password"], False), "127.0.0.1"
    )
    backends = mw[0].kwargs["backends"]
    assert isinstance(backends[0], PrivateKeyBackend)
    assert isinstance(backends[1], PasswordBackend)


def test_build_enterprise_middleware_unknown_backend_raises():
    """Defensive guard for an unsupported backend name (validator should
    have rejected it earlier)."""
    with pytest.raises(ValueError, match="Unsupported auth backend 'kerberos'"):
        server._build_enterprise_middleware((["kerberos"], False), "127.0.0.1")


# ---------------------------------------------------------------------------
# _register_community_tools / _register_enterprise_tools
# ---------------------------------------------------------------------------


def test_register_community_tools_covers_shared_plus_community_exclusive():
    """_register_community_tools registers shared tools + community-exclusive tools."""
    mock_server = MagicMock()
    mock_session = MagicMock()
    mock_table = MagicMock()
    mock_script = MagicMock()
    mock_reload = MagicMock()
    mock_session_community = MagicMock()

    with (
        patch("deephaven_mcp.mcp_systems_server.server.session", mock_session),
        patch("deephaven_mcp.mcp_systems_server.server.table", mock_table),
        patch("deephaven_mcp.mcp_systems_server.server.script", mock_script),
        patch(
            "deephaven_mcp.mcp_systems_server.server._SHARED_TOOLS",
            (mock_session, mock_table, mock_script),
        ),
        patch("deephaven_mcp.mcp_systems_server.server.reload", mock_reload),
        patch(
            "deephaven_mcp.mcp_systems_server.server.session_community",
            mock_session_community,
        ),
    ):
        server._register_community_tools(mock_server)

    # Shared tools first...
    mock_session.register_tools.assert_called_once_with(mock_server)
    mock_table.register_tools.assert_called_once_with(mock_server)
    mock_script.register_tools.assert_called_once_with(mock_server)
    # ...then community-exclusive.
    mock_reload.register_community_tools.assert_called_once_with(mock_server)
    mock_session_community.register_tools.assert_called_once_with(mock_server)


def test_register_enterprise_tools_covers_shared_plus_enterprise_exclusive():
    """_register_enterprise_tools registers shared tools + enterprise-exclusive tools."""
    mock_server = MagicMock()
    mock_session = MagicMock()
    mock_table = MagicMock()
    mock_script = MagicMock()
    mock_reload = MagicMock()
    mock_session_enterprise = MagicMock()
    mock_catalog = MagicMock()
    mock_pq = MagicMock()

    with (
        patch("deephaven_mcp.mcp_systems_server.server.session", mock_session),
        patch("deephaven_mcp.mcp_systems_server.server.table", mock_table),
        patch("deephaven_mcp.mcp_systems_server.server.script", mock_script),
        patch(
            "deephaven_mcp.mcp_systems_server.server._SHARED_TOOLS",
            (mock_session, mock_table, mock_script),
        ),
        patch("deephaven_mcp.mcp_systems_server.server.reload", mock_reload),
        patch(
            "deephaven_mcp.mcp_systems_server.server.session_enterprise",
            mock_session_enterprise,
        ),
        patch("deephaven_mcp.mcp_systems_server.server.catalog", mock_catalog),
        patch("deephaven_mcp.mcp_systems_server.server.pq", mock_pq),
    ):
        server._register_enterprise_tools(mock_server)

    # Shared tools first...
    mock_session.register_tools.assert_called_once_with(mock_server)
    mock_table.register_tools.assert_called_once_with(mock_server)
    mock_script.register_tools.assert_called_once_with(mock_server)
    # ...then enterprise-exclusive.
    mock_reload.register_enterprise_tools.assert_called_once_with(mock_server)
    mock_session_enterprise.register_tools.assert_called_once_with(mock_server)
    mock_catalog.register_tools.assert_called_once_with(mock_server)
    mock_pq.register_tools.assert_called_once_with(mock_server)


# ---------------------------------------------------------------------------
# _run_server (driver for entry points)
# ---------------------------------------------------------------------------


def test_run_server_drives_full_startup_in_order(monkeypatch):
    """Happy path: _run_server walks setup -> parse -> validate -> middleware -> serve.

    Verifies each phase is invoked with the expected inputs so that future
    refactors of the shared driver can't silently skip a step.
    """
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "my-test-server"

    mock_manager_class = MagicMock()
    mock_loader = AsyncMock(return_value=(1234.0, ("some-mw-state",)))
    mock_lifespan_factory = MagicMock(return_value=MagicMock())
    mock_build_middleware = MagicMock(return_value=[])
    mock_register_tools = MagicMock()

    with (
        patch("sys.argv", ["some-script", "--config", "/cfg.json"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_startup_validation_or_exit",
            return_value=(1234.0, ("some-mw-state",)),
        ) as mock_validate,
        patch(
            "deephaven_mcp.mcp_systems_server.server.SessionRegistryManager"
        ) as mock_srm_cls,
        patch(
            "deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server
        ) as mock_fastmcp_cls,
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_with_middleware"
        ) as mock_run_with_mw,
    ):
        server._run_server(
            label="test",
            description="A test server",
            default_port=9999,
            server_name="my-test-server",
            manager_class=mock_manager_class,
            async_loader=mock_loader,
            registry_class=MagicMock(),
            lifespan_factory=mock_lifespan_factory,
            build_middleware=mock_build_middleware,
            register_tools=mock_register_tools,
        )

    # Validation is invoked with the right args.
    mock_validate.assert_called_once_with(
        "/cfg.json", mock_manager_class, mock_loader, "test"
    )
    # Middleware builder receives the loader's mw-state tuple + host.
    mock_build_middleware.assert_called_once_with(("some-mw-state",), "127.0.0.1")
    # Registry manager is constructed with the idle timeout from the loader.
    mock_srm_cls.assert_called_once()
    assert mock_srm_cls.call_args.kwargs["idle_timeout_seconds"] == 1234.0
    # FastMCP is named with server_name and bound to parsed host/port.
    mock_fastmcp_cls.assert_called_once()
    assert mock_fastmcp_cls.call_args.args[0] == "my-test-server"
    assert mock_fastmcp_cls.call_args.kwargs["host"] == "127.0.0.1"
    assert mock_fastmcp_cls.call_args.kwargs["port"] == 9999
    # All tools registered on the constructed server via single callback.
    mock_register_tools.assert_called_once_with(mock_server)
    # uvicorn runner is called with the middleware and host/port.
    mock_run_with_mw.assert_called_once_with(mock_server, [], "127.0.0.1", 9999)


def test_run_server_logs_stopped_even_when_runner_raises(monkeypatch):
    """The finally-log fires even if _run_with_middleware raises."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "my-test-server"
    mock_logger = MagicMock()

    with (
        patch("sys.argv", ["some-script"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_startup_validation_or_exit",
            return_value=(1.0, None),
        ),
        patch("deephaven_mcp.mcp_systems_server.server.SessionRegistryManager"),
        patch(
            "deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server
        ),
        patch(
            "deephaven_mcp.mcp_systems_server.server._run_with_middleware",
            side_effect=RuntimeError("boom"),
        ),
        patch("deephaven_mcp.mcp_systems_server.server._LOGGER", mock_logger),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            server._run_server(
                label="test",
                description="A test server",
                default_port=1234,
                server_name="my-test-server",
                manager_class=MagicMock(),
                async_loader=AsyncMock(return_value=(1.0, None)),
                registry_class=MagicMock(),
                lifespan_factory=MagicMock(return_value=MagicMock()),
                build_middleware=MagicMock(return_value=[]),
                register_tools=MagicMock(),
            )

    mock_logger.info.assert_any_call("[test] MCP server 'my-test-server' stopped.")
