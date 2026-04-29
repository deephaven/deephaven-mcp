"""
Tests for deephaven_mcp.mcp_systems_server._tools.reload.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import MockContext, create_mock_session_registry_manager

from deephaven_mcp.mcp_systems_server._tools.reload import (
    _do_reload,
    mcp_reload_community,
    mcp_reload_enterprise,
)


def _make_context(session_registry_manager=None, include_session_manager=True):
    """Build a MockContext with a full lifespan dict."""
    config_manager = AsyncMock()
    config_manager.clear_config_cache = AsyncMock()
    refresh_lock = asyncio.Lock()
    if session_registry_manager is None:
        session_registry_manager = create_mock_session_registry_manager()
    lifespan = {
        "config_manager": config_manager,
        "refresh_lock": refresh_lock,
    }
    if include_session_manager:
        lifespan["session_registry_manager"] = session_registry_manager
    return MockContext(lifespan), config_manager, session_registry_manager


@pytest.mark.asyncio
async def test_mcp_reload_missing_session_registry_manager():
    """_do_reload returns error when session_registry_manager is missing from context."""
    context, _, _ = _make_context(include_session_manager=False)
    result = await _do_reload(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "session_registry_manager" in result["error"]


@pytest.mark.asyncio
async def test_mcp_reload_lock_error():
    """_do_reload returns error when the refresh lock raises."""
    session_registry_manager = create_mock_session_registry_manager()
    config_manager = AsyncMock()
    config_manager.clear_config_cache = AsyncMock()
    refresh_lock = MagicMock()
    refresh_lock.__aenter__ = AsyncMock(side_effect=Exception("lock error"))
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry_manager": session_registry_manager,
            "refresh_lock": refresh_lock,
        }
    )
    result = await mcp_reload_community(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "lock error" in result["error"]


@pytest.mark.asyncio
async def test_mcp_reload_success():
    """_do_reload clears config cache and calls close_session on success."""
    session_registry_manager = create_mock_session_registry_manager()
    context, config_manager, _ = _make_context(session_registry_manager)
    result = await mcp_reload_community(context)
    assert result == {"success": True}
    config_manager.clear_config_cache.assert_awaited_once()
    session_registry_manager.close_session.assert_awaited_once_with(
        "test-mcp-session-id"
    )


@pytest.mark.asyncio
async def test_mcp_reload_calls_close_session_with_correct_id():
    """_do_reload calls close_session with the session ID from the request header."""
    session_registry_manager = create_mock_session_registry_manager()
    config_manager = AsyncMock()
    config_manager.clear_config_cache = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry_manager": session_registry_manager,
            "refresh_lock": asyncio.Lock(),
        },
        mcp_session_id="my-special-session-abc",
    )
    result = await _do_reload(context)
    assert result == {"success": True}
    session_registry_manager.close_session.assert_awaited_once_with(
        "my-special-session-abc"
    )


@pytest.mark.asyncio
async def test_mcp_reload_enterprise_success():
    """mcp_reload_enterprise delegates to the shared implementation."""
    session_registry_manager = create_mock_session_registry_manager()
    context, _, _ = _make_context(session_registry_manager)
    result = await mcp_reload_enterprise(context)
    assert result == {"success": True}


@pytest.mark.asyncio
async def test_mcp_reload_failure():
    """_do_reload returns error when clear_config_cache raises."""
    session_registry_manager = create_mock_session_registry_manager()
    config_manager = AsyncMock()
    config_manager.clear_config_cache = AsyncMock(side_effect=RuntimeError("fail"))
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry_manager": session_registry_manager,
            "refresh_lock": asyncio.Lock(),
        }
    )
    result = await mcp_reload_community(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "fail" in result["error"]


@pytest.mark.asyncio
async def test_mcp_reload_close_session_error():
    """_do_reload returns error when close_session raises."""
    session_registry_manager = create_mock_session_registry_manager()
    session_registry_manager.close_session = AsyncMock(
        side_effect=RuntimeError("close failed")
    )
    config_manager = AsyncMock()
    config_manager.clear_config_cache = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry_manager": session_registry_manager,
            "refresh_lock": asyncio.Lock(),
        }
    )
    result = await _do_reload(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "close failed" in result["error"]


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
