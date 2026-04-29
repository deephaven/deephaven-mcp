"""MCP Server Entry Points and Assembly.

Provides the CLI entry points for the DHE and DHC MCP servers plus the shared
assembly logic (environment setup, argument parsing, shared tool registration)
used by both.

Entry points (registered in pyproject.toml):
  - ``dh-mcp-enterprise-server``: Start the Deephaven Enterprise (DHE) MCP server.
  - ``dh-mcp-community-server``:  Start the Deephaven Community (DHC) MCP server.

Configuration precedence for both servers (first wins):
  1. ``--config`` CLI argument
  2. ``DH_MCP_CONFIG_FILE`` environment variable

Host/port precedence for both servers (first wins):
  1. ``--host`` / ``--port`` CLI arguments
  2. ``MCP_HOST`` / ``MCP_PORT`` environment variables
  3. Per-server defaults (enterprise: 8002, community: 8003)
"""

import asyncio
import logging
import sys

from mcp.server.fastmcp import FastMCP

from deephaven_mcp.config import (
    CommunityServerConfigManager,
    EnterpriseServerConfigManager,
)
from deephaven_mcp.mcp_systems_server._lifespan import (
    LifespanContext,
    make_community_lifespan,
    make_enterprise_lifespan,
)
from deephaven_mcp.mcp_systems_server._session_registry_manager import (
    SessionRegistryManager,
)
from deephaven_mcp.mcp_systems_server._tools import (
    catalog,
    pq,
    reload,
    script,
    session,
    session_community,
    session_enterprise,
    table,
)
from deephaven_mcp.resource_manager import (
    CommunitySessionRegistry,
    EnterpriseSessionRegistry,
)

_LOGGER = logging.getLogger(__name__)

# Tools registered on every server regardless of type.
# Enterprise-exclusive tools (session_enterprise, catalog, pq) and server-specific
# variants (reload) are registered directly in enterprise()/community() so the
# distinction is explicit.
_SHARED_TOOLS = (session, table, script)


def _setup_env() -> None:
    """Initialize logging and monkeypatching for MCP server entry points."""
    from deephaven_mcp._logging import (
        setup_global_exception_logging,
        setup_logging,
        setup_signal_handler_logging,
    )
    from deephaven_mcp._monkeypatch import monkeypatch_uvicorn_exception_handling

    setup_logging()
    setup_global_exception_logging()
    setup_signal_handler_logging()
    monkeypatch_uvicorn_exception_handling()


def _parse_args(description: str, default_port: int) -> tuple[str | None, str, int]:
    """Parse ``--config`` / ``--host`` / ``--port`` from argv and env vars.

    Precedence for each value (first wins):
      1. CLI argument
      2. Environment variable (``DH_MCP_CONFIG_FILE``, ``MCP_HOST``, ``MCP_PORT``)
      3. Default (``None`` for config, ``"127.0.0.1"`` for host, ``default_port`` for port)

    Args:
        description (str): Description string for the ``ArgumentParser``.
        default_port (int): Default port number when neither CLI arg nor env var is set.

    Returns:
        tuple[str | None, str, int]: ``(config_path, host, port)``
    """
    import argparse
    import os

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-c",
        "--config",
        default=None,
        help="Path to the config file. Falls back to DH_MCP_CONFIG_FILE env var.",
    )
    parser.add_argument(
        "--host",
        default=None,
        help="Host to bind to. Falls back to MCP_HOST env var, then 127.0.0.1.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=f"Port to listen on. Falls back to MCP_PORT env var, then {default_port}.",
    )
    args = parser.parse_args()
    config_path: str | None = args.config or os.environ.get("DH_MCP_CONFIG_FILE")
    host: str = args.host or os.environ.get("MCP_HOST", "127.0.0.1")
    port: int = args.port or int(os.environ.get("MCP_PORT", str(default_port)))
    return config_path, host, port


def _register_shared_tools(server: FastMCP) -> None:
    """Register the tools common to both DHE and DHC servers."""
    for module in _SHARED_TOOLS:
        module.register_tools(server)


def _read_idle_timeout(config_path: str | None, config_manager_class: type) -> float:
    """Read mcp_session_idle_timeout_seconds from config."""
    timeout: float = asyncio.run(
        config_manager_class(
            config_path=config_path
        ).get_mcp_session_idle_timeout_seconds()
    )
    return timeout


def _validate_config_or_exit(
    config_path: str | None, config_manager_class: type, label: str
) -> None:
    """Validate config at server startup; exit(1) if invalid."""
    _LOGGER.info(f"[{label}] Validating configuration before server startup...")
    try:
        asyncio.run(config_manager_class(config_path=config_path).get_config())
    except Exception as e:
        _LOGGER.error(f"[{label}] Configuration error — server will not start: {e}")
        sys.exit(1)
    _LOGGER.info(f"[{label}] Configuration validated successfully.")


def enterprise() -> None:
    """Entry point: start the Deephaven Enterprise (DHE) MCP server."""
    _setup_env()
    config_path, host, port = _parse_args(
        "Start the Deephaven Enterprise MCP server (HTTP transport only).",
        default_port=8002,
    )
    _LOGGER.info(
        f"[enterprise] Starting DHE MCP server: host={host!r}, port={port}, "
        f"config={config_path!r}"
    )
    _validate_config_or_exit(config_path, EnterpriseServerConfigManager, "enterprise")
    idle_timeout = _read_idle_timeout(config_path, EnterpriseServerConfigManager)
    session_registry_manager: SessionRegistryManager[EnterpriseSessionRegistry] = (
        SessionRegistryManager(
            registry_class=EnterpriseSessionRegistry,
            idle_timeout_seconds=idle_timeout,
        )
    )
    server: FastMCP[LifespanContext[EnterpriseSessionRegistry]] = FastMCP(
        "deephaven-mcp-enterprise",
        lifespan=make_enterprise_lifespan(session_registry_manager, config_path),
        host=host,
        port=port,
    )
    _register_shared_tools(server)
    reload.register_enterprise_tools(server)
    session_enterprise.register_tools(server)
    catalog.register_tools(server)
    pq.register_tools(server)
    _LOGGER.info(
        f"[enterprise] DHE MCP server '{server.name}' starting on "
        f"{host}:{port} (streamable-http)"
    )
    try:
        server.run(transport="streamable-http")
    finally:
        _LOGGER.info(f"[enterprise] DHE MCP server '{server.name}' stopped.")


def community() -> None:
    """Entry point: start the Deephaven Community (DHC) MCP server."""
    _setup_env()
    config_path, host, port = _parse_args(
        "Start the Deephaven Community MCP server (HTTP transport only).",
        default_port=8003,
    )
    _LOGGER.info(
        f"[community] Starting DHC MCP server: host={host!r}, port={port}, "
        f"config={config_path!r}"
    )
    _validate_config_or_exit(config_path, CommunityServerConfigManager, "community")
    idle_timeout = _read_idle_timeout(config_path, CommunityServerConfigManager)
    session_registry_manager: SessionRegistryManager[CommunitySessionRegistry] = (
        SessionRegistryManager(
            registry_class=CommunitySessionRegistry,
            idle_timeout_seconds=idle_timeout,
        )
    )
    server: FastMCP[LifespanContext[CommunitySessionRegistry]] = FastMCP(
        "deephaven-mcp-community",
        lifespan=make_community_lifespan(session_registry_manager, config_path),
        host=host,
        port=port,
    )
    _register_shared_tools(server)
    reload.register_community_tools(server)
    session_community.register_tools(server)
    _LOGGER.info(
        f"[community] DHC MCP server '{server.name}' starting on "
        f"{host}:{port} (streamable-http)"
    )
    try:
        server.run(transport="streamable-http")
    finally:
        _LOGGER.info(f"[community] DHC MCP server '{server.name}' stopped.")
