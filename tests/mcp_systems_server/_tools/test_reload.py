"""
Tests for deephaven_mcp.mcp_systems_server._tools.reload.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import MockContext

from deephaven_mcp.mcp_systems_server._tools.reload import (
    _do_reload,
    mcp_reload_community,
    mcp_reload_enterprise,
)


def _make_registry():
    """Build a mock registry with close() and initialize() as AsyncMocks."""
    registry = AsyncMock()
    registry.close = AsyncMock()
    registry.initialize = AsyncMock()
    return registry


def _make_evictor():
    """Build a mock evictor with stop() and start() as AsyncMocks."""
    evictor = AsyncMock()
    evictor.stop = AsyncMock()
    evictor.start = AsyncMock()
    return evictor


def _make_context(registry=None, evictor=None, include_registry=True):
    """Build a MockContext with a full lifespan dict."""
    config_manager = AsyncMock()
    config_manager.clear_config_cache = AsyncMock()
    refresh_lock = asyncio.Lock()
    if registry is None:
        registry = _make_registry()
    if evictor is None:
        evictor = _make_evictor()
    lifespan = {
        "config_manager": config_manager,
        "refresh_lock": refresh_lock,
        "evictor": evictor,
    }
    if include_registry:
        lifespan["registry"] = registry
    return MockContext(lifespan), config_manager, registry, evictor


@pytest.mark.asyncio
async def test_mcp_reload_missing_registry():
    """_do_reload returns error when registry is missing from context."""
    context, _, _, _ = _make_context(include_registry=False)
    result = await _do_reload(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "registry" in result["error"]


@pytest.mark.asyncio
async def test_mcp_reload_lock_error():
    """_do_reload returns error when the refresh lock raises."""
    registry = _make_registry()
    evictor = _make_evictor()
    config_manager = AsyncMock()
    config_manager.clear_config_cache = AsyncMock()
    refresh_lock = MagicMock()
    refresh_lock.__aenter__ = AsyncMock(side_effect=Exception("lock error"))
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    context = MockContext(
        {
            "config_manager": config_manager,
            "registry": registry,
            "evictor": evictor,
            "refresh_lock": refresh_lock,
        }
    )
    result = await mcp_reload_community(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "lock error" in result["error"]


@pytest.mark.asyncio
async def test_mcp_reload_success_resets_registry_and_restarts_evictor():
    """_do_reload stops evictor, resets registry, restarts evictor."""
    registry = _make_registry()
    evictor = _make_evictor()
    context, config_manager, _, _ = _make_context(registry, evictor)
    result = await mcp_reload_community(context)
    assert result == {"success": True}
    evictor.stop.assert_awaited_once_with()
    config_manager.clear_config_cache.assert_awaited_once()
    registry.close.assert_awaited_once_with()
    registry.initialize.assert_awaited_once_with(config_manager)
    evictor.start.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_mcp_reload_enterprise_success():
    """mcp_reload_enterprise delegates to the shared implementation."""
    registry = _make_registry()
    evictor = _make_evictor()
    context, _, _, _ = _make_context(registry, evictor)
    result = await mcp_reload_enterprise(context)
    assert result == {"success": True}
    registry.close.assert_awaited_once_with()
    registry.initialize.assert_awaited_once()
    evictor.stop.assert_awaited_once_with()
    evictor.start.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_mcp_reload_clear_config_cache_failure():
    """_do_reload returns error when clear_config_cache raises."""
    registry = _make_registry()
    evictor = _make_evictor()
    config_manager = AsyncMock()
    config_manager.clear_config_cache = AsyncMock(side_effect=RuntimeError("fail"))
    context = MockContext(
        {
            "config_manager": config_manager,
            "registry": registry,
            "evictor": evictor,
            "refresh_lock": asyncio.Lock(),
        }
    )
    result = await mcp_reload_community(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "fail" in result["error"]


@pytest.mark.asyncio
async def test_mcp_reload_registry_close_error():
    """_do_reload returns error when registry.close() raises."""
    registry = _make_registry()
    registry.close = AsyncMock(side_effect=RuntimeError("close failed"))
    evictor = _make_evictor()
    config_manager = AsyncMock()
    config_manager.clear_config_cache = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "registry": registry,
            "evictor": evictor,
            "refresh_lock": asyncio.Lock(),
        }
    )
    result = await _do_reload(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "close failed" in result["error"]


@pytest.mark.asyncio
async def test_mcp_reload_registry_initialize_error():
    """_do_reload returns error when registry.initialize() raises."""
    registry = _make_registry()
    registry.initialize = AsyncMock(side_effect=RuntimeError("init failed"))
    evictor = _make_evictor()
    config_manager = AsyncMock()
    config_manager.clear_config_cache = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "registry": registry,
            "evictor": evictor,
            "refresh_lock": asyncio.Lock(),
        }
    )
    result = await _do_reload(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "init failed" in result["error"]


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
