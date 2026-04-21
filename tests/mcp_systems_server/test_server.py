"""
Tests for deephaven_mcp.mcp_systems_server.server.
"""

from unittest.mock import ANY, MagicMock, patch

import pytest

import deephaven_mcp.mcp_systems_server.server as server


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
    with patch("sys.argv", ["prog", "--config", "/cli/conf.json", "--host", "0.0.0.0", "--port", "9999"]):
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
        patch("deephaven_mcp._logging.setup_global_exception_logging") as mock_global_exc,
        patch("deephaven_mcp._logging.setup_signal_handler_logging") as mock_signal,
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling") as mock_monkeypatch,
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
    from deephaven_mcp.mcp_systems_server._tools import reload, script, session, table

    assert session in server._SHARED_TOOLS
    assert table in server._SHARED_TOOLS
    assert script in server._SHARED_TOOLS
    assert reload in server._SHARED_TOOLS


def test_shared_tools_excludes_enterprise_exclusive_modules():
    """Enterprise-exclusive modules are NOT in _SHARED_TOOLS."""
    from deephaven_mcp.mcp_systems_server._tools import catalog, pq, session_enterprise

    assert session_enterprise not in server._SHARED_TOOLS
    assert catalog not in server._SHARED_TOOLS
    assert pq not in server._SHARED_TOOLS


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

    with (
        patch("sys.argv", ["dh-mcp-enterprise-server"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch("deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server) as mock_fastmcp_cls,
        patch("deephaven_mcp.mcp_systems_server.server.make_enterprise_lifespan", return_value=MagicMock()) as mock_lifespan,
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_enterprise"),
        patch("deephaven_mcp.mcp_systems_server.server.catalog"),
        patch("deephaven_mcp.mcp_systems_server.server.pq"),
    ):
        server.enterprise()

    mock_fastmcp_cls.assert_called_once_with("deephaven-mcp-enterprise", lifespan=ANY, host="127.0.0.1", port=8002)
    mock_server.run.assert_called_once_with(transport="streamable-http")
    mock_lifespan.assert_called_once_with(None)


def test_enterprise_cli_args(monkeypatch):
    """enterprise() uses --config, --host, and --port when provided."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-enterprise"

    with (
        patch("sys.argv", ["dh-mcp-enterprise-server", "--config", "/my/dhe.json", "--host", "0.0.0.0", "--port", "9001"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch("deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server) as mock_fastmcp_cls,
        patch("deephaven_mcp.mcp_systems_server.server.make_enterprise_lifespan", return_value=MagicMock()) as mock_lifespan,
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_enterprise"),
        patch("deephaven_mcp.mcp_systems_server.server.catalog"),
        patch("deephaven_mcp.mcp_systems_server.server.pq"),
    ):
        server.enterprise()

    mock_fastmcp_cls.assert_called_once_with("deephaven-mcp-enterprise", lifespan=ANY, host="0.0.0.0", port=9001)
    mock_server.run.assert_called_once_with(transport="streamable-http")
    mock_lifespan.assert_called_once_with("/my/dhe.json")


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
        patch("deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server) as mock_fastmcp_cls,
        patch("deephaven_mcp.mcp_systems_server.server.make_enterprise_lifespan", return_value=MagicMock()) as mock_lifespan,
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_enterprise"),
        patch("deephaven_mcp.mcp_systems_server.server.catalog"),
        patch("deephaven_mcp.mcp_systems_server.server.pq"),
    ):
        server.enterprise()

    mock_fastmcp_cls.assert_called_once_with("deephaven-mcp-enterprise", lifespan=ANY, host="10.0.0.1", port=7777)
    mock_server.run.assert_called_once_with(transport="streamable-http")
    mock_lifespan.assert_called_once_with("/env/dhe.json")


def test_enterprise_registers_shared_and_exclusive_tools(monkeypatch):
    """enterprise() registers shared tools and DHE-exclusive tools."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-enterprise"
    mock_register_shared = MagicMock()
    mock_session_enterprise = MagicMock()
    mock_catalog = MagicMock()
    mock_pq = MagicMock()

    with (
        patch("sys.argv", ["dh-mcp-enterprise-server"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch("deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server),
        patch("deephaven_mcp.mcp_systems_server.server.make_enterprise_lifespan", return_value=MagicMock()),
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools", mock_register_shared),
        patch("deephaven_mcp.mcp_systems_server.server.session_enterprise", mock_session_enterprise),
        patch("deephaven_mcp.mcp_systems_server.server.catalog", mock_catalog),
        patch("deephaven_mcp.mcp_systems_server.server.pq", mock_pq),
    ):
        server.enterprise()

    mock_register_shared.assert_called_once_with(mock_server)
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
    mock_server.run.side_effect = RuntimeError("server crashed")
    mock_logger = MagicMock()

    with (
        patch("sys.argv", ["dh-mcp-enterprise-server"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch("deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server),
        patch("deephaven_mcp.mcp_systems_server.server.make_enterprise_lifespan", return_value=MagicMock()),
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_enterprise"),
        patch("deephaven_mcp.mcp_systems_server.server.catalog"),
        patch("deephaven_mcp.mcp_systems_server.server.pq"),
        patch("deephaven_mcp.mcp_systems_server.server._LOGGER", mock_logger),
    ):
        with pytest.raises(RuntimeError, match="server crashed"):
            server.enterprise()

    mock_logger.info.assert_any_call(
        "[enterprise] DHE MCP server 'deephaven-mcp-enterprise' stopped."
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

    with (
        patch("sys.argv", ["dh-mcp-community-server"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch("deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server) as mock_fastmcp_cls,
        patch("deephaven_mcp.mcp_systems_server.server.make_community_lifespan", return_value=MagicMock()) as mock_lifespan,
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_community"),
    ):
        server.community()

    mock_fastmcp_cls.assert_called_once_with("deephaven-mcp-community", lifespan=ANY, host="127.0.0.1", port=8003)
    mock_server.run.assert_called_once_with(transport="streamable-http")
    mock_lifespan.assert_called_once_with(None)


def test_community_cli_args(monkeypatch):
    """community() uses --config, --host, and --port when provided."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-community"

    with (
        patch("sys.argv", ["dh-mcp-community-server", "--config", "/my/dhc.json", "--host", "0.0.0.0", "--port", "9002"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch("deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server) as mock_fastmcp_cls,
        patch("deephaven_mcp.mcp_systems_server.server.make_community_lifespan", return_value=MagicMock()) as mock_lifespan,
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_community"),
    ):
        server.community()

    mock_fastmcp_cls.assert_called_once_with("deephaven-mcp-community", lifespan=ANY, host="0.0.0.0", port=9002)
    mock_server.run.assert_called_once_with(transport="streamable-http")
    mock_lifespan.assert_called_once_with("/my/dhc.json")


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
        patch("deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server) as mock_fastmcp_cls,
        patch("deephaven_mcp.mcp_systems_server.server.make_community_lifespan", return_value=MagicMock()) as mock_lifespan,
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_community"),
    ):
        server.community()

    mock_fastmcp_cls.assert_called_once_with("deephaven-mcp-community", lifespan=ANY, host="192.168.1.1", port=6666)
    mock_server.run.assert_called_once_with(transport="streamable-http")
    mock_lifespan.assert_called_once_with("/env/dhc.json")


def test_community_registers_shared_and_exclusive_tools(monkeypatch):
    """community() registers shared tools and DHC-exclusive tools; does NOT register enterprise tools."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-community"
    mock_register_shared = MagicMock()
    mock_session_community = MagicMock()

    with (
        patch("sys.argv", ["dh-mcp-community-server"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch("deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server),
        patch("deephaven_mcp.mcp_systems_server.server.make_community_lifespan", return_value=MagicMock()),
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools", mock_register_shared),
        patch("deephaven_mcp.mcp_systems_server.server.session_community", mock_session_community),
    ):
        server.community()

    mock_register_shared.assert_called_once_with(mock_server)
    mock_session_community.register_tools.assert_called_once_with(mock_server)


def test_community_logs_stopped_onserver_exit(monkeypatch):
    """community() logs server stopped even when server.run raises."""
    monkeypatch.delenv("DH_MCP_CONFIG_FILE", raising=False)
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.delenv("MCP_PORT", raising=False)

    mock_server = MagicMock()
    mock_server.name = "deephaven-mcp-community"
    mock_server.run.side_effect = RuntimeError("server crashed")
    mock_logger = MagicMock()

    with (
        patch("sys.argv", ["dh-mcp-community-server"]),
        patch("deephaven_mcp._logging.setup_logging"),
        patch("deephaven_mcp._logging.setup_global_exception_logging"),
        patch("deephaven_mcp._logging.setup_signal_handler_logging"),
        patch("deephaven_mcp._monkeypatch.monkeypatch_uvicorn_exception_handling"),
        patch("deephaven_mcp.mcp_systems_server.server.FastMCP", return_value=mock_server),
        patch("deephaven_mcp.mcp_systems_server.server.make_community_lifespan", return_value=MagicMock()),
        patch("deephaven_mcp.mcp_systems_server.server._register_shared_tools"),
        patch("deephaven_mcp.mcp_systems_server.server.session_community"),
        patch("deephaven_mcp.mcp_systems_server.server._LOGGER", mock_logger),
    ):
        with pytest.raises(RuntimeError, match="server crashed"):
            server.community()

    mock_logger.info.assert_any_call(
        "[community] DHC MCP server 'deephaven-mcp-community' stopped."
    )
