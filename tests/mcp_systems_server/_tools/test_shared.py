"""
Tests for deephaven_mcp.mcp_systems_server._tools.shared.
"""

import asyncio
import os
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from conftest import MockContext, create_mock_instance_tracker

from deephaven_mcp import config
from deephaven_mcp._exceptions import RegistryItemNotFoundError
from deephaven_mcp.client import BaseSession, CorePlusSession
from deephaven_mcp.mcp_systems_server._tools.shared import (
    _check_response_size,
    _get_enterprise_session,
    _get_session_from_context,
    _get_system_config,
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
async def test_get_session_from_context_success():
    """Test _get_session_from_context successfully retrieves a session."""
    # Create mock session
    mock_session = MagicMock()

    # Create mock session manager
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    # Create mock session registry
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    # Create context with registry
    context = MockContext({"session_registry": mock_registry})

    # Call the helper
    result = await _get_session_from_context(
        "test_function", context, "test:session:id"
    )

    # Verify the session was returned
    assert result is mock_session

    # Verify the registry was accessed correctly
    mock_registry.get.assert_called_once_with("test:session:id")
    mock_session_manager.get.assert_called_once()


@pytest.mark.asyncio
async def test_get_session_from_context_session_not_found():
    """Test _get_session_from_context propagates RegistryItemNotFoundError from registry."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("No item with name 'nonexistent:session' found")
    )

    context = MockContext({"session_registry": mock_registry})

    with pytest.raises(RegistryItemNotFoundError, match="No item with name"):
        await _get_session_from_context("test_function", context, "nonexistent:session")

    mock_registry.get.assert_called_once_with("nonexistent:session")


@pytest.mark.asyncio
async def test_get_session_from_context_keyerror_still_propagates():
    """Test _get_session_from_context still propagates KeyError (non-RegistryItemNotFoundError)."""
    # Create mock session registry that raises KeyError (different from RegistryItemNotFoundError)
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=KeyError("Session not found"))

    context = MockContext({"session_registry": mock_registry})

    with pytest.raises(KeyError, match="Session not found"):
        await _get_session_from_context("test_function", context, "nonexistent:session")


@pytest.mark.asyncio
async def test_get_session_from_context_session_connection_fails():
    """Test _get_session_from_context propagates exception when session.get() fails."""
    # Create mock session manager that fails on get()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(
        side_effect=Exception("Failed to establish connection")
    )

    # Create mock session registry
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    # Create context with registry
    context = MockContext({"session_registry": mock_registry})

    # Call the helper and expect Exception
    with pytest.raises(Exception, match="Failed to establish connection"):
        await _get_session_from_context("test_function", context, "test:session:id")

    # Verify both registry and manager were accessed
    mock_registry.get.assert_called_once_with("test:session:id")
    mock_session_manager.get.assert_called_once()


def test_check_response_size_acceptable():
    """Test _check_response_size with acceptable size."""
    result = _check_response_size("test_table", 1000000)  # 1MB
    assert result is None


def test_check_response_size_warning_threshold():
    """Test _check_response_size with size above warning threshold."""
    with patch("deephaven_mcp.mcp_systems_server._tools.shared._LOGGER") as mock_logger:
        result = _check_response_size("test_table", 10000000)  # 10MB
        assert result is None
        mock_logger.warning.assert_called_once()
        assert "Large response (~10.0MB)" in mock_logger.warning.call_args[0][0]


def test_check_response_size_over_limit():
    """Test _check_response_size with size over maximum limit."""
    result = _check_response_size("test_table", 60000000)  # 60MB
    assert result == {
        "success": False,
        "error": "Response would be ~60.0MB (max 50MB). Please reduce max_rows.",
        "isError": True,
    }


@pytest.mark.asyncio
async def test_get_system_config_success():
    """Test _get_system_config when system exists in configuration."""
    mock_config_manager = MagicMock()

    enterprise_config = {
        "test-system": {
            "connection_json_url": "https://test.com",
            "auth_type": "password",
        }
    }

    # New config access path uses async get_config() with nested structure
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    system_config, error_response = await _get_system_config(
        "test_function", mock_config_manager, "test-system"
    )

    assert error_response is None
    assert system_config == enterprise_config["test-system"]


@pytest.mark.asyncio
async def test_get_system_config_system_not_found():
    """Test _get_system_config when system doesn't exist in configuration."""
    mock_config_manager = MagicMock()

    enterprise_config = {"other-system": {}}

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    system_config, error_response = await _get_system_config(
        "test_function", mock_config_manager, "nonexistent-system"
    )

    assert system_config == {}
    assert error_response is not None
    assert (
        error_response["error"]
        == "Enterprise system 'nonexistent-system' not found in configuration"
    )
    assert error_response["isError"] is True


@pytest.mark.asyncio
async def test_get_system_config_handles_keyerror_from_get_config_section():
    """Covers the KeyError except block when extracting nested enterprise systems config."""
    mock_config_manager = MagicMock()
    mock_config_manager.get_config = AsyncMock(return_value={"some": "config"})

    with patch(
        "deephaven_mcp.config.get_config_section",
        side_effect=KeyError(),
    ):
        system_config, error_response = await _get_system_config(
            "test_function", mock_config_manager, "missing-system"
        )

    assert system_config == {}
    assert error_response is not None
    assert "missing-system" in error_response["error"]
    assert error_response["isError"] is True


@pytest.mark.asyncio
async def test_get_system_config_empty_config():
    """Test _get_system_config when enterprise config is empty."""
    mock_config_manager = MagicMock()

    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    system_config, error_response = await _get_system_config(
        "test_function", mock_config_manager, "test-system"
    )

    assert system_config == {}
    assert error_response is not None
    assert (
        error_response["error"]
        == "Enterprise system 'test-system' not found in configuration"
    )
    assert error_response["isError"] is True


@pytest.mark.asyncio
async def test_get_enterprise_session_success():
    """Test _get_enterprise_session with a valid CorePlusSession."""
    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    session, error = await _get_enterprise_session(
        "test_function", context, "test-session-id"
    )

    assert session is mock_session  # Returns the validated session
    assert error is None


@pytest.mark.asyncio
async def test_get_enterprise_session_not_enterprise():
    """Test _get_enterprise_session with a non-enterprise session."""
    mock_session = MagicMock(spec=BaseSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    session, error = await _get_enterprise_session(
        "test_function", context, "test-session-id"
    )

    assert session is None
    assert error is not None
    assert error["success"] is False
    assert "test_function only works with enterprise (Core+) sessions" in error["error"]
    assert "test-session-id" in error["error"]
    assert error["isError"] is True
