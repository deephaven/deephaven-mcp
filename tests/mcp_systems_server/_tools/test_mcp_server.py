"""
Tests for deephaven_mcp.mcp_systems_server._tools.mcp_server.
"""

import asyncio
import os
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from conftest import MockContext, create_mock_instance_tracker

from deephaven_mcp import config
from deephaven_mcp.mcp_systems_server._tools.mcp_server import (
    app_lifespan,
    mcp_reload,
)
from deephaven_mcp.resource_manager import (
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
    PythonLaunchedSession,
    ResourceLivenessStatus,
    SystemType,
)


@pytest.mark.asyncio
async def test_mcp_reload_missing_context_keys():
    # context missing session_registry
    config_manager = AsyncMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(return_value=None)
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock()
    context = MockContext(
        {"config_manager": config_manager, "refresh_lock": refresh_lock}
    )
    result = await mcp_reload(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "session_registry" in result["error"]


@pytest.mark.asyncio
async def test_mcp_reload_lock_error():
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
    result = await mcp_reload(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "lock error" in result["error"]


@pytest.mark.filterwarnings("ignore:unclosed <socket.socket")
@pytest.mark.asyncio
async def test_mcp_reload_success():
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
    result = await mcp_reload(context)
    assert result == {"success": True}
    config_manager.clear_config_cache.assert_awaited_once()
    session_registry.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_mcp_reload_failure():
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
    result = await mcp_reload(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "fail" in result["error"]


@pytest.mark.asyncio
async def test_app_lifespan_yields_context_and_cleans_up():
    class DummyServer:
        name = "dummy-server"

    # Import the app_lifespan function
    from deephaven_mcp.mcp_systems_server._tools.mcp_server import app_lifespan

    # Create mocks
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()

    # Configure necessary mocks for the app_lifespan function to work
    config_manager.get_config = AsyncMock(return_value={})
    session_registry.initialize = AsyncMock()
    session_registry.close = AsyncMock()

    # Use a comprehensive patching approach to handle all dependencies
    with (
        patch(
            "deephaven_mcp.mcp_systems_server._tools.mcp_server.ConfigManager",
            return_value=config_manager,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._tools.mcp_server.CombinedSessionRegistry",
            return_value=session_registry,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._tools.mcp_server.asyncio.Lock",
            return_value=refresh_lock,
        ),
        # Mock get_config_path to avoid environment variable dependency
        patch(
            "deephaven_mcp.config.get_config_path",
            return_value="/mock/config/path.json",
        ),
        # Mock load_and_validate_config to avoid file system dependency
        patch(
            "deephaven_mcp.config.load_and_validate_config",
            AsyncMock(return_value={}),
        ),
    ):
        server = DummyServer()
        async with app_lifespan(server) as context:
            # Just check that the keys exist in the context
            assert "config_manager" in context
            assert "session_registry" in context
            assert "refresh_lock" in context
        session_registry.close.assert_awaited_once()
