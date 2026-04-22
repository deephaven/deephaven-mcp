"""
Tests for deephaven_mcp.mcp_systems_server._tools.reload.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import MockContext

from deephaven_mcp.mcp_systems_server._tools.reload import _do_reload, mcp_reload_community, mcp_reload_enterprise


@pytest.mark.asyncio
async def test_mcp_reload_missing_context_keys():
    """mcp_reload returns error when session_registry is missing from context."""
    config_manager = AsyncMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(return_value=None)
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock()
    context = MockContext(
        {"config_manager": config_manager, "refresh_lock": refresh_lock}
    )
    result = await mcp_reload_community(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "session_registry" in result["error"]


@pytest.mark.asyncio
async def test_mcp_reload_lock_error():
    """mcp_reload returns error when the refresh lock raises."""
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(side_effect=Exception("lock error"))
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock()
    session_registry.close = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry": session_registry,
            "refresh_lock": refresh_lock,
        }
    )
    result = await mcp_reload_community(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "lock error" in result["error"]


@pytest.mark.asyncio
async def test_mcp_reload_success():
    """mcp_reload clears cache, closes and reinitialises session registry on success."""
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(return_value=None)
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock()
    session_registry.close = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry": session_registry,
            "refresh_lock": refresh_lock,
        }
    )
    result = await mcp_reload_community(context)
    assert result == {"success": True}
    config_manager.clear_config_cache.assert_awaited_once()
    session_registry.close.assert_awaited_once()
    session_registry.initialize.assert_awaited_once_with(config_manager)


@pytest.mark.asyncio
async def test_mcp_reload_enterprise_success():
    """mcp_reload_enterprise delegates to the shared implementation."""
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(return_value=None)
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock()
    session_registry.close = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry": session_registry,
            "refresh_lock": refresh_lock,
        }
    )
    result = await mcp_reload_enterprise(context)
    assert result == {"success": True}


def test_register_community_tools_registers_mcp_reload():
    """register_community_tools() registers mcp_reload on a DHC server."""
    from mcp.server.fastmcp import FastMCP

    from deephaven_mcp.mcp_systems_server._tools.reload import register_community_tools

    server = FastMCP("test-reload-community-server")
    register_community_tools(server)
    assert "mcp_reload" in server._tool_manager._tools


def test_register_enterprise_tools_registers_mcp_reload():
    """register_enterprise_tools() registers mcp_reload on a DHE server."""
    from mcp.server.fastmcp import FastMCP

    from deephaven_mcp.mcp_systems_server._tools.reload import register_enterprise_tools

    server = FastMCP("test-reload-enterprise-server")
    register_enterprise_tools(server)
    assert "mcp_reload" in server._tool_manager._tools


@pytest.mark.asyncio
async def test_mcp_reload_community_calls_manager_close():
    """mcp_reload with a real registry calls close() on every manager — including dynamic sessions."""
    from deephaven_mcp.resource_manager._registry import MutableSessionRegistry

    class _TestRegistry(MutableSessionRegistry):
        async def _load_items(self, config_manager):
            pass

    session_registry = _TestRegistry()
    session_registry._initialized = True

    mock_manager = MagicMock()
    mock_manager.close = AsyncMock()
    session_registry._items["test_key"] = mock_manager

    config_manager = AsyncMock()
    context = MockContext({
        "config_manager": config_manager,
        "session_registry": session_registry,
        "refresh_lock": asyncio.Lock(),
    })

    result = await _do_reload(context)

    assert result == {"success": True}
    mock_manager.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_mcp_reload_failure():
    """mcp_reload returns error when clear_config_cache raises."""
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(return_value=None)
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock(side_effect=RuntimeError("fail"))
    session_registry.close = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry": session_registry,
            "refresh_lock": refresh_lock,
        }
    )
    result = await mcp_reload_community(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "fail" in result["error"]
