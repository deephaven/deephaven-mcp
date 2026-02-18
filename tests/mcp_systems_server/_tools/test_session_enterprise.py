"""
Tests for deephaven_mcp.mcp_systems_server._tools.session_enterprise.
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
from deephaven_mcp.mcp_systems_server._tools.session_enterprise import (
    _check_session_id_available,
    _check_session_limits,
    _generate_session_name_if_none,
    _resolve_session_parameters,
    enterprise_systems_status,
    session_enterprise_create,
    session_enterprise_delete,
)
from deephaven_mcp.resource_manager import (
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
    InitializationPhase,
    PythonLaunchedSession,
    RegistrySnapshot,
    ResourceLivenessStatus,
    SystemType,
)


@pytest.mark.asyncio
async def test_session_enterprise_create_auto_name_no_username_and_language_transformer():
    """Covers auto-generated name without username (mcp-worker-...), language transformer execution, and creation_function."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "no-user-system": {
            "connection_json_url": "https://example.com/iris/connection.json",
            "auth_type": "password",
            # Intentionally omit 'username' to exercise the no-username branch
            "password": "pass",
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {
                    "heap_size_gb": 2.0,
                    "auto_delete_timeout": 600,
                    "server": "server-east-1",
                    "engine": "DeephavenCommunity",
                },
            },
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.session_enterprise.datetime"
    ) as mock_datetime:
        mock_datetime.now().strftime.return_value = "20241126-1500"

        # Enterprise factory chain
        mock_enterprise_registry = MagicMock()
        mock_factory_manager = MagicMock()
        mock_factory = MagicMock()
        mock_session = MagicMock()
        mock_registry.enterprise_registry = AsyncMock(
            return_value=mock_enterprise_registry
        )
        mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
        mock_factory_manager.get = AsyncMock(return_value=mock_factory)
        # Set up the factory mock to capture configuration_transformer calls
        captured_config_transformer = None

        def capture_transformer(*args, **kwargs):
            nonlocal captured_config_transformer
            captured_config_transformer = kwargs.get("configuration_transformer")
            return mock_session

        mock_factory.connect_to_new_worker = AsyncMock(side_effect=capture_transformer)

        # Mock the session registry operations
        mock_registry.get = AsyncMock(side_effect=RegistryItemNotFoundError("Session not found"))
        mock_registry.add_session = AsyncMock()
        mock_registry.count_added_sessions = AsyncMock(return_value=0)

        context = MockContext(
            {"config_manager": mock_config_manager, "session_registry": mock_registry}
        )

        # Use a non-Python programming language to exercise configuration_transformer
        result = await session_enterprise_create(
            context,
            "no-user-system",
            None,
            programming_language="Groovy",
        )

        assert result["success"] is True
        # Name should be generated without username prefix
        assert result["session_name"] == "mcp-session-20241126-1500"

        # Verify the factory was called with a configuration_transformer for non-Python language
        mock_factory.connect_to_new_worker.assert_called_once()
        assert captured_config_transformer is not None

        # Test the language transformer - now accesses config.pb.scriptLanguage
        mock_config = MagicMock()
        result_config = captured_config_transformer(mock_config)
        assert result_config is mock_config
        assert mock_config.pb.scriptLanguage == "Groovy"

        # Verify session was added using add_session method - check the call was made
        session_id = f"enterprise:no-user-system:mcp-session-20241126-1500"
        mock_registry.add_session.assert_called_once()
        call_args = mock_registry.add_session.call_args
        session_manager = call_args[0][0]  # First (and only) argument is the manager
        assert session_manager.full_name == session_id
        returned_session = await session_manager._creation_function(
            "no-user-system", "mcp-session-20241126-1500"
        )
        assert returned_session is mock_session


@pytest.mark.asyncio
async def test_session_enterprise_delete_removal_missing_in_registry():
    """Covers branch where pop returns None (lines 1959-1960)."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.close = AsyncMock()

    enterprise_config = {"sys": {"session_creation": {"max_concurrent_sessions": 5}}}

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    # Mock remove_session to return None (simulating session not found in registry)
    mock_registry.remove_session = AsyncMock(return_value=None)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await session_enterprise_delete(context, "sys", "s1")

    assert result["success"] is True


@pytest.mark.asyncio
async def test_session_enterprise_delete_cleanup_created_sessions_empty():
    """Test session removal - session tracking now handled by registry automatically."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.close = AsyncMock()

    enterprise_config = {"sys2": {"session_creation": {"max_concurrent_sessions": 5}}}

    # Mock remove_session to return the manager (simulating successful removal)
    full_id = "enterprise:sys2:solo"
    mock_registry.remove_session = AsyncMock(return_value=mock_session_manager)
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await session_enterprise_delete(context, "sys2", "solo")

    assert result["success"] is True
    # Session tracking is now handled internally by the registry


@pytest.mark.asyncio
async def test_session_enterprise_delete_registry_pop_raises_error():
    """Covers error path on removal (lines 1973-1977)."""

    class BadItems:
        def pop(self, *args, **kwargs):
            raise RuntimeError("pop failed")

    mock_registry = MagicMock()
    mock_config_manager = MagicMock()
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.close = AsyncMock()

    enterprise_config = {"sys3": {"session_creation": {"max_concurrent_sessions": 5}}}
    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_registry.remove_session = AsyncMock(
        side_effect=Exception("Simulated registry error")
    )

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await session_enterprise_delete(context, "sys3", "s3")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Failed to remove session" in result["error"]


@pytest.mark.asyncio
async def test_session_enterprise_delete_outer_exception_logger_info_raises():
    """Force outer exception handler (lines 1991-1998) by making _LOGGER.info raise."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.close = AsyncMock()

    enterprise_config = {"sys4": {"session_creation": {"max_concurrent_sessions": 5}}}
    full_id = "enterprise:sys4:s4"
    mock_registry.remove_session = AsyncMock(return_value=mock_session_manager)
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    # Only raise on the second info() call (the first is before the try block)
    call_counter = {"n": 0}

    def info_side_effect(*args, **kwargs):
        call_counter["n"] += 1
        if call_counter["n"] == 2:
            raise Exception("log fail")
        return None

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    with patch(
        "deephaven_mcp.mcp_systems_server._tools.session_enterprise._LOGGER.info",
        side_effect=info_side_effect,
    ):
        result = await session_enterprise_delete(context, "sys4", "s4")

    assert result["success"] is False
    assert result["isError"] is True
    assert "log fail" in result["error"]


@pytest.mark.asyncio
async def test_enterprise_systems_status_success():
    """Test successful retrieval of enterprise systems status."""
    # Mock factory with liveness_status and is_alive methods
    mock_factory1 = AsyncMock()
    mock_factory1.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.ONLINE, "System is healthy")
    )
    mock_factory1.is_alive = AsyncMock(return_value=True)

    mock_factory2 = AsyncMock()
    mock_factory2.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.OFFLINE, "System is not responding")
    )
    mock_factory2.is_alive = AsyncMock(return_value=False)

    # Mock enterprise registry
    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={"system1": mock_factory1, "system2": mock_factory2})
    )

    # Mock session registry
    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={
            "enterprise": {
                "systems": {
                    "system1": {"url": "http://example.com", "api_key": "secret_key"},
                    "system2": {
                        "url": "http://example2.com",
                        "password": "secret_password",
                    },
                }
            }
        }
    )

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Mock the redact function to match the actual implementation
    with patch(
        "deephaven_mcp.config._enterprise_system.redact_enterprise_system_config"
    ) as mock_redact:
        # Configure the mock to replace only password with [REDACTED]
        def redact_config(config):
            result = config.copy()
            if "password" in result:
                result["password"] = "[REDACTED]"
            return result

        mock_redact.side_effect = redact_config
        # Call the function with default parameters
        result = await enterprise_systems_status(context)

    # Verify the result
    assert result["success"] is True
    assert len(result["systems"]) == 2

    # Check system1
    system1 = next(s for s in result["systems"] if s["name"] == "system1")
    assert system1["liveness_status"] == "ONLINE"
    assert system1["liveness_detail"] == "System is healthy"
    assert system1["is_alive"] is True
    assert system1["config"]["url"] == "http://example.com"
    assert system1["config"]["api_key"] == "secret_key"

    # Check system2
    system2 = next(s for s in result["systems"] if s["name"] == "system2")
    assert system2["liveness_status"] == "OFFLINE"
    assert system2["liveness_detail"] == "System is not responding"
    assert system2["is_alive"] is False
    assert system2["config"]["url"] == "http://example2.com"
    assert system2["config"]["password"] == "[REDACTED]"

    # COMPLETED with no errors should not include initialization info
    assert "initialization" not in result

    # Verify liveness_status was called with attempt_to_connect=False
    mock_factory1.liveness_status.assert_called_once_with(ensure_item=False)
    mock_factory2.liveness_status.assert_called_once_with(ensure_item=False)


@pytest.mark.asyncio
async def test_enterprise_systems_status_with_attempt_to_connect():
    """Test enterprise systems status with attempt_to_connect=True."""
    # Mock factory with liveness_status and is_alive methods
    mock_factory = AsyncMock()
    mock_factory.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.ONLINE, None)
    )
    mock_factory.is_alive = AsyncMock(return_value=True)

    # Mock enterprise registry
    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={"system1": mock_factory})
    )

    # Mock session registry
    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"enterprise": {"factories": {"system1": {}}}}
    )

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Mock the redact function
    with patch(
        "deephaven_mcp.config._enterprise_system.redact_enterprise_system_config",
        return_value={},
    ):
        # Call the function with attempt_to_connect=True
        result = await enterprise_systems_status(context, attempt_to_connect=True)

        # Verify the result
        assert result["success"] is True
        assert len(result["systems"]) == 1

        # Check system1
        system1 = result["systems"][0]
        assert system1["name"] == "system1"
        assert system1["liveness_status"] == "ONLINE"
        assert "liveness_detail" not in system1  # No detail was provided
        assert system1["is_alive"] is True

        # COMPLETED with no errors should not include initialization info
        assert "initialization" not in result

        # Verify liveness_status was called with attempt_to_connect=True
        mock_factory.liveness_status.assert_called_once_with(ensure_item=True)


@pytest.mark.asyncio
async def test_enterprise_systems_status_no_systems():
    """Test enterprise systems status with no systems available."""
    # Mock enterprise registry with no systems
    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all = AsyncMock(return_value=RegistrySnapshot.simple(items={}))

    # Mock session registry
    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"enterprise": {"systems": {}}}
    )

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Call the function
    result = await enterprise_systems_status(context)

    # Verify the result
    assert result["success"] is True
    assert len(result["systems"]) == 0
    # COMPLETED with no errors should not include initialization info
    assert "initialization" not in result


@pytest.mark.asyncio
async def test_enterprise_systems_status_all_status_types():
    """Test enterprise systems status with all possible status types."""
    # Create a mock factory for each status type
    factories = {}
    status_details = {
        "online_system": (ResourceLivenessStatus.ONLINE, "System is healthy"),
        "offline_system": (ResourceLivenessStatus.OFFLINE, "System is not responding"),
        "unauthorized_system": (
            ResourceLivenessStatus.UNAUTHORIZED,
            "Authentication failed",
        ),
        "misconfigured_system": (
            ResourceLivenessStatus.MISCONFIGURED,
            "Invalid configuration",
        ),
        "unknown_system": (ResourceLivenessStatus.UNKNOWN, "Unknown error occurred"),
    }

    for name, (status, detail) in status_details.items():
        mock_factory = AsyncMock()
        mock_factory.liveness_status = AsyncMock(return_value=(status, detail))
        mock_factory.is_alive = AsyncMock(
            return_value=(status == ResourceLivenessStatus.ONLINE)
        )
        factories[name] = mock_factory

    # Mock enterprise registry
    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all = AsyncMock(return_value=RegistrySnapshot.simple(items=factories))

    # Mock session registry
    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    # Mock config manager with empty configs
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"enterprise": {"factories": {name: {} for name in factories}}}
    )

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Mock the redact function
    with patch(
        "deephaven_mcp.config._enterprise_system.redact_enterprise_system_config",
        return_value={},
    ):
        # Call the function
        result = await enterprise_systems_status(context)

        # Verify the result
        assert result["success"] is True
        assert len(result["systems"]) == 5

        # Check each system has the correct status and detail
        for name, (status, detail) in status_details.items():
            system = next(s for s in result["systems"] if s["name"] == name)
            assert system["liveness_status"] == status.name
            assert system["liveness_detail"] == detail
            assert system["is_alive"] == (status == ResourceLivenessStatus.ONLINE)

        # COMPLETED with no errors should not include initialization info
        assert "initialization" not in result


@pytest.mark.asyncio
async def test_enterprise_systems_status_config_error():
    """Test enterprise systems status when config retrieval fails."""
    # Mock session registry
    mock_session_registry = MagicMock()
    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    # Mock config manager that raises an exception
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(side_effect=Exception("Config error"))

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Call the function
    result = await enterprise_systems_status(context)

    # Verify the result
    assert result["success"] is False
    assert result["isError"] is True
    assert "Config error" in result["error"]


@pytest.mark.asyncio
async def test_enterprise_systems_status_registry_error():
    """Test enterprise systems status when registry retrieval fails."""
    # Mock enterprise registry that raises an exception
    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all.side_effect = Exception("Registry error")

    # Mock session registry
    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"enterprise": {"factories": {}}}
    )

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Call the function
    result = await enterprise_systems_status(context)

    # Verify the result
    assert result["success"] is False
    assert result["isError"] is True
    assert "Registry error" in result["error"]


@pytest.mark.asyncio
async def test_enterprise_systems_status_liveness_error():
    """Test enterprise systems status when liveness_status raises an exception."""
    # Mock factory with liveness_status that raises an exception
    mock_factory = AsyncMock()
    mock_factory.liveness_status.side_effect = Exception("Liveness error")
    mock_factory.is_alive.return_value = False

    # Mock enterprise registry
    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={"system1": mock_factory})
    )

    # Mock session registry
    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"enterprise": {"factories": {"system1": {}}}}
    )

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Mock the redact function
    with patch(
        "deephaven_mcp.config._enterprise_system.redact_enterprise_system_config",
        return_value={},
    ):
        # Call the function
        result = await enterprise_systems_status(context)

        # Verify the result
        assert result["success"] is False
        assert result["isError"] is True
        assert "Liveness error" in result["error"]


@pytest.mark.asyncio
async def test_enterprise_systems_status_no_enterprise_registry():
    """Test enterprise systems status when enterprise_registry is None."""
    # Mock session registry with None enterprise registry
    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(return_value=AsyncMock())
    mock_session_registry.enterprise_registry.return_value.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"enterprise": {"systems": {}}}
    )

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Call the function
    result = await enterprise_systems_status(context)

    # Verify the result
    assert result["success"] is True
    assert len(result["systems"]) == 0
    # COMPLETED with no errors should not include initialization info
    assert "initialization" not in result


@pytest.mark.asyncio
async def test_enterprise_systems_status_factory_snapshot_unexpected_phase():
    """Test enterprise_systems_status raises InternalError for unexpected phase."""
    from deephaven_mcp.resource_manager import InitializationPhase

    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={}, phase=InitializationPhase.LOADING, errors={}
        )
    )

    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"enterprise": {"systems": {}}}
    )

    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await enterprise_systems_status(context)

    # InternalError is caught by the tool's except Exception handler
    assert result["success"] is False
    assert "expected SIMPLE" in result["error"]


@pytest.mark.asyncio
async def test_enterprise_systems_status_factory_snapshot_with_errors():
    """Test enterprise_systems_status raises InternalError for factory snapshot errors."""
    from deephaven_mcp.resource_manager import InitializationPhase

    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={},
            phase=InitializationPhase.SIMPLE,
            errors={"factory_reg": "something broke"},
        )
    )

    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(items={})
    )

    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"enterprise": {"systems": {}}}
    )

    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await enterprise_systems_status(context)

    # Simple registry should never have errors - InternalError caught by handler
    assert result["success"] is False
    assert "unexpected errors" in result["error"]


class TestEnterpriseSystemsStatusInitialization:
    """Test enterprise_systems_status surfaces initialization status and errors."""

    @pytest.mark.asyncio
    async def test_enterprise_systems_status_discovery_in_progress(self):
        """Test enterprise_systems_status shows status when discovery is in progress."""
        mock_enterprise_registry = AsyncMock()
        mock_enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={})
        )

        mock_session_registry = MagicMock()
        mock_session_registry.enterprise_registry = AsyncMock(
            return_value=mock_enterprise_registry
        )
        mock_session_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.with_initialization(
                items={},
                phase=InitializationPhase.LOADING,
                errors={},
            )
        )

        mock_config_manager = AsyncMock()
        mock_config_manager.get_config = AsyncMock(
            return_value={"enterprise": {"systems": {}}}
        )

        context = MockContext(
            {
                "session_registry": mock_session_registry,
                "config_manager": mock_config_manager,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await enterprise_systems_status(context)

        assert result["success"] is True
        assert "initialization" in result
        assert "still in progress" in result["initialization"]["status"]

    @pytest.mark.asyncio
    async def test_enterprise_systems_status_discovery_in_progress_with_errors(self):
        """Test enterprise_systems_status shows both status and errors during discovery."""
        mock_enterprise_registry = AsyncMock()
        mock_enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={})
        )

        mock_session_registry = MagicMock()
        mock_session_registry.enterprise_registry = AsyncMock(
            return_value=mock_enterprise_registry
        )
        mock_session_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.with_initialization(
                items={},
                phase=InitializationPhase.LOADING,
                errors={"factory1": "Connection refused"},
            )
        )

        mock_config_manager = AsyncMock()
        mock_config_manager.get_config = AsyncMock(
            return_value={"enterprise": {"systems": {}}}
        )

        context = MockContext(
            {
                "session_registry": mock_session_registry,
                "config_manager": mock_config_manager,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await enterprise_systems_status(context)

        assert result["success"] is True
        assert "initialization" in result
        assert "still in progress" in result["initialization"]["status"]
        assert "errors" in result["initialization"]
        assert "factory1" in result["initialization"]["errors"]

    @pytest.mark.asyncio
    async def test_enterprise_systems_status_completed_with_errors(self):
        """Test enterprise_systems_status shows init_errors."""
        mock_enterprise_registry = AsyncMock()
        mock_enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={})
        )

        mock_session_registry = MagicMock()
        mock_session_registry.enterprise_registry = AsyncMock(
            return_value=mock_enterprise_registry
        )
        mock_session_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.with_initialization(
                items={},
                phase=InitializationPhase.COMPLETED,
                errors={"factory1": "Connection failed: Connection refused"},
            )
        )

        mock_config_manager = AsyncMock()
        mock_config_manager.get_config = AsyncMock(
            return_value={"enterprise": {"systems": {}}}
        )

        context = MockContext(
            {
                "session_registry": mock_session_registry,
                "config_manager": mock_config_manager,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await enterprise_systems_status(context)

        assert result["success"] is True
        assert "initialization" in result
        assert "errors" in result["initialization"]
        assert "factory1" in result["initialization"]["errors"]
        assert "connection issues" in result["initialization"]["status"]

    @pytest.mark.asyncio
    async def test_enterprise_systems_status_completed_no_errors(self):
        """Test enterprise_systems_status omits init fields when no errors."""
        mock_enterprise_registry = AsyncMock()
        mock_enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={})
        )

        mock_session_registry = MagicMock()
        mock_session_registry.enterprise_registry = AsyncMock(
            return_value=mock_enterprise_registry
        )
        mock_session_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={})
        )

        mock_config_manager = AsyncMock()
        mock_config_manager.get_config = AsyncMock(
            return_value={"enterprise": {"systems": {}}}
        )

        context = MockContext(
            {
                "session_registry": mock_session_registry,
                "config_manager": mock_config_manager,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await enterprise_systems_status(context)

        assert result["success"] is True
        assert "initialization" not in result


@pytest.mark.asyncio
async def test_session_enterprise_create_success_with_defaults():
    """Test session_enterprise_create with config defaults."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    # Mock enterprise systems config
    enterprise_config = {
        "prod-system": {
            "connection_json_url": "https://prod.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "admin",
            "password": "secret",
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {
                    "heap_size_gb": 8.0,
                    "auto_delete_timeout": 3600,
                    "server": "server-east-1",
                    "engine": "DeephavenCommunity",
                },
            },
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    # Mock session registry and factories
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_session = MagicMock()

    mock_registry.enterprise_registry = AsyncMock(return_value=mock_enterprise_registry)
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)

    # Mock no existing workers (under limit)
    mock_registry.get_all = AsyncMock(return_value={})
    mock_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Session not found")
    )  # No conflict
    mock_registry.add_session = AsyncMock()
    mock_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await session_enterprise_create(context, "prod-system", "test-worker")

    assert result["success"] is True
    assert result["session_id"] == "enterprise:prod-system:test-worker"
    assert result["system_name"] == "prod-system"
    assert result["session_name"] == "test-worker"
    assert result["configuration"]["heap_size_gb"] == 8.0
    assert result["configuration"]["auto_delete_timeout"] == 3600
    assert result["configuration"]["server"] == "server-east-1"
    assert result["configuration"]["engine"] == "DeephavenCommunity"

    # Verify worker was created with correct parameters
    mock_factory.connect_to_new_worker.assert_called_once_with(
        name="test-worker",
        heap_size_gb=8.0,
        auto_delete_timeout=3600,
        server="server-east-1",
        engine="DeephavenCommunity",
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        timeout_seconds=60,
        configuration_transformer=None,
        session_arguments=None,
    )

    # Verify session was added to registry
    # Verify add_session was called with manager only
    mock_registry.add_session.assert_called_once()
    call_args = mock_registry.add_session.call_args
    session_manager = call_args[0][0]  # Manager is the only argument
    assert session_manager.full_name == "enterprise:prod-system:test-worker"


@pytest.mark.asyncio
async def test_session_enterprise_create_success_with_overrides():
    """Test session_enterprise_create with parameter overrides."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    # Mock enterprise systems config with defaults
    enterprise_config = {
        "prod-system": {
            "connection_json_url": "https://prod.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "admin",
            "password": "secret",
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {"heap_size_gb": 4.0, "auto_delete_timeout": 1800},
            },
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    # Mock session registry and factories
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_session = MagicMock()

    mock_registry.enterprise_registry = AsyncMock(return_value=mock_enterprise_registry)
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)

    mock_registry.get_all = AsyncMock(return_value={})
    mock_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Session not found")
    )  # No conflict
    mock_registry.add_session = AsyncMock()
    mock_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await session_enterprise_create(
        context,
        "prod-system",
        "custom-worker",
        heap_size_gb=16.0,
        auto_delete_timeout=7200,
        server="server-west-1",
        engine="DeephavenEnterprise",
    )

    assert result["success"] is True
    assert result["configuration"]["heap_size_gb"] == 16.0  # Override
    assert result["configuration"]["auto_delete_timeout"] == 7200  # Override
    assert result["configuration"]["server"] == "server-west-1"  # Override
    assert result["configuration"]["engine"] == "DeephavenEnterprise"  # Override

    mock_factory.connect_to_new_worker.assert_called_once_with(
        name="custom-worker",
        heap_size_gb=16.0,
        auto_delete_timeout=7200,
        server="server-west-1",
        engine="DeephavenEnterprise",
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        timeout_seconds=60,
        configuration_transformer=None,
        session_arguments=None,
    )


@pytest.mark.asyncio
async def test_session_enterprise_create_auto_generate_name():
    """Test session_enterprise_create auto-generates worker name when None."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "test-system": {
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "test",
            "password": "test",
            "session_creation": {"max_concurrent_sessions": 3},
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.session_enterprise.datetime"
    ) as mock_datetime:
        mock_datetime.now().strftime.return_value = "20241126-1430"

        # Mock session registry and factories
        mock_enterprise_registry = MagicMock()
        mock_factory_manager = MagicMock()
        mock_factory = MagicMock()
        mock_session = MagicMock()

        mock_registry.enterprise_registry = AsyncMock(
            return_value=mock_enterprise_registry
        )
        mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
        mock_factory_manager.get = AsyncMock(return_value=mock_factory)
        mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)

        mock_registry.get_all = AsyncMock(return_value={})
        mock_registry.get = AsyncMock(
            side_effect=RegistryItemNotFoundError("Session not found")
        )  # No conflict
        mock_registry.add_session = AsyncMock()
        mock_registry.count_added_sessions = AsyncMock(return_value=0)

        context = MockContext(
            {"config_manager": mock_config_manager, "session_registry": mock_registry}
        )

        result = await session_enterprise_create(context, "test-system")

        assert result["success"] is True
        assert result["session_name"] == "mcp-test-20241126-1430"
        assert result["session_id"] == "enterprise:test-system:mcp-test-20241126-1430"


@pytest.mark.asyncio
async def test_session_enterprise_create_system_not_found():
    """Test session_enterprise_create when enterprise system not found."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    # Provide empty enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await session_enterprise_create(context, "nonexistent-system", "worker")

    assert result["success"] is False
    assert "Enterprise system 'nonexistent-system' not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_create_max_workers_exceeded():
    """Test session_enterprise_create when max concurrent workers limit exceeded."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "limited-system": {
            "connection_json_url": "https://limited.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
            "session_creation": {"max_concurrent_sessions": 2},  # Low limit for testing
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    # Mock registry to return 2 existing sessions (at limit)
    mock_registry.count_added_sessions = AsyncMock(return_value=2)

    # Mock session registry get to simulate existing sessions for counting
    async def mock_session_get(session_id):
        if session_id in [
            "enterprise:limited-system:worker1",
            "enterprise:limited-system:worker2",
        ]:
            return MagicMock(spec=EnterpriseSessionManager)
        elif session_id == "enterprise:limited-system:worker3":
            raise RegistryItemNotFoundError("Session not found")  # New session doesn't exist yet
        else:
            raise RegistryItemNotFoundError("Session not found")

    mock_registry.get = AsyncMock(side_effect=mock_session_get)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await session_enterprise_create(context, "limited-system", "worker3")

    assert result["success"] is False
    assert "Max concurrent sessions (2) reached" in result["error"]
    assert result["isError"] is True

    # No cleanup needed - session tracking handled by registry


@pytest.mark.asyncio
async def test_session_enterprise_create_session_conflict():
    """Test session_enterprise_create when session ID already exists."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "conflict-system": {
            "connection_json_url": "https://conflict.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
            "session_creation": {"max_concurrent_sessions": 5},
        }
    }

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    # Mock session registry to return existing session
    mock_existing_session = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_existing_session)
    mock_registry.get_all = AsyncMock(return_value={})
    mock_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await session_enterprise_create(
        context, "conflict-system", "existing-worker"
    )

    assert result["success"] is False
    assert (
        "Session 'enterprise:conflict-system:existing-worker' already exists"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_create_factory_creation_failure():
    """Test session_enterprise_create when worker creation fails."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "failing-system": {
            "connection_json_url": "https://failing.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
            "session_creation": {"max_concurrent_sessions": 5},
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    # Mock session registry - no conflict
    mock_registry.get = AsyncMock(side_effect=RegistryItemNotFoundError("No session found"))
    mock_registry.get_all = AsyncMock(return_value={})
    mock_registry.count_added_sessions = AsyncMock(return_value=0)

    # Mock factory that fails during worker creation
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()

    mock_registry.enterprise_registry = AsyncMock(return_value=mock_enterprise_registry)
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(
        side_effect=Exception("Resource exhausted")
    )

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await session_enterprise_create(
        context, "failing-system", "failing-worker"
    )

    assert result["success"] is False
    assert "Resource exhausted" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_create_disabled_by_zero_max_workers():
    """Test session_enterprise_create when worker creation is disabled (max_concurrent_sessions = 0)."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "disabled-system": {
            "connection_json_url": "https://disabled.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
            "session_creation": {"max_concurrent_sessions": 0},  # Disabled
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await session_enterprise_create(context, "disabled-system", "test-worker")

    assert result["success"] is False
    assert "Session creation is disabled" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_delete_success():
    """Test session_enterprise_delete successful deletion."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "test-system": {
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }
    }

    # Mock existing enterprise session manager
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.close = AsyncMock()

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_registry.remove_session = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await session_enterprise_delete(context, "test-system", "test-worker")

    assert result["success"] is True
    assert result["session_id"] == "enterprise:test-system:test-worker"
    assert result["system_name"] == "test-system"
    assert result["session_name"] == "test-worker"

    # Verify session was closed and removed
    mock_session_manager.close.assert_called_once()
    # Verify remove_session was called
    mock_registry.remove_session.assert_called_once_with(
        "enterprise:test-system:test-worker"
    )


@pytest.mark.asyncio
async def test_session_enterprise_delete_system_not_found():
    """Test session_enterprise_delete when enterprise system not found."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    # No enterprise systems

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await session_enterprise_delete(context, "nonexistent-system", "worker")

    assert result["success"] is False
    assert "Enterprise system 'nonexistent-system' not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_delete_session_not_found():
    """Test session_enterprise_delete when session not found."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "test-system": {
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    mock_registry.get = AsyncMock(side_effect=RegistryItemNotFoundError("Session not found"))

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await session_enterprise_delete(
        context, "test-system", "nonexistent-worker"
    )

    assert result["success"] is False
    assert (
        "Session 'enterprise:test-system:nonexistent-worker' not found"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_delete_not_enterprise_session():
    """Test session_enterprise_delete when session is not an EnterpriseSessionManager."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "test-system": {
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }
    }

    # Mock non-enterprise session manager
    mock_session_manager = MagicMock()  # Not an EnterpriseSessionManager

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await session_enterprise_delete(
        context, "test-system", "wrong-type-worker"
    )

    assert result["success"] is False
    assert "is not an enterprise session" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_delete_close_failure_continues():
    """Test session_enterprise_delete continues removal even if close fails."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "test-system": {
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }
    }

    # Mock session manager that fails to close
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_session_manager.close = AsyncMock(side_effect=Exception("Close failed"))

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_registry.remove_session = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await session_enterprise_delete(
        context, "test-system", "failing-close-worker"
    )

    # Should succeed despite close failure
    assert result["success"] is True
    assert result["session_id"] == "enterprise:test-system:failing-close-worker"

    # Verify session was still removed from registry
    # Verify remove_session was called even after close failure
    mock_registry.remove_session.assert_called_once_with(
        "enterprise:test-system:failing-close-worker"
    )


def test_resolve_session_parameters():
    """Test _resolve_session_parameters helper function."""
    defaults = {
        "heap_size_gb": 4.0,
        "auto_delete_timeout": 1800,
        "server": "default-server",
        "engine": "DeephavenCommunity",
        "extra_jvm_args": ["-Xmx1g"],
        "extra_environment_vars": ["ENV=test"],
        "admin_groups": ["admins"],
        "viewer_groups": ["viewers"],
        "timeout_seconds": 120,
        "session_arguments": {"key": "value"},
        "programming_language": "Python",
    }

    # Test with all parameters provided (should override defaults)
    result = _resolve_session_parameters(
        heap_size_gb=8.0,
        auto_delete_timeout=3600,
        server="custom-server",
        engine="CustomEngine",
        extra_jvm_args=["-Xmx2g"],
        extra_environment_vars=["ENV=prod"],
        admin_groups=["custom-admins"],
        viewer_groups=["custom-viewers"],
        timeout_seconds=240,
        session_arguments={"custom": "args"},
        programming_language="Groovy",
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 8.0
    assert result["auto_delete_timeout"] == 3600
    assert result["server"] == "custom-server"
    assert result["engine"] == "CustomEngine"
    assert result["extra_jvm_args"] == ["-Xmx2g"]
    assert result["extra_environment_vars"] == ["ENV=prod"]
    assert result["admin_groups"] == ["custom-admins"]
    assert result["viewer_groups"] == ["custom-viewers"]
    assert result["timeout_seconds"] == 240
    assert result["session_arguments"] == {"custom": "args"}
    assert result["programming_language"] == "Groovy"

    # Test with no parameters provided (should use defaults)
    result = _resolve_session_parameters(
        heap_size_gb=None,
        auto_delete_timeout=None,
        server=None,
        engine=None,
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        timeout_seconds=None,
        session_arguments=None,
        programming_language=None,
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 4.0
    assert result["auto_delete_timeout"] == 1800
    assert result["server"] == "default-server"
    assert result["engine"] == "DeephavenCommunity"
    assert result["extra_jvm_args"] == ["-Xmx1g"]
    assert result["extra_environment_vars"] == ["ENV=test"]
    assert result["admin_groups"] == ["admins"]
    assert result["viewer_groups"] == ["viewers"]
    assert result["timeout_seconds"] == 120
    assert result["session_arguments"] == {"key": "value"}
    assert result["programming_language"] == "Python"

    # Test with mixed parameters (some provided, some defaults)
    result = _resolve_session_parameters(
        heap_size_gb=16.0,  # Override
        auto_delete_timeout=None,  # Use default
        server="override-server",  # Override
        engine=None,  # Use default
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        timeout_seconds=None,
        session_arguments=None,
        programming_language=None,
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 16.0
    assert result["auto_delete_timeout"] == 1800
    assert result["server"] == "override-server"
    assert result["engine"] == "DeephavenCommunity"

    # Test with empty defaults (should use built-in defaults)
    result = _resolve_session_parameters(
        heap_size_gb=None,
        auto_delete_timeout=None,
        server=None,
        engine=None,
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        timeout_seconds=None,
        session_arguments=None,
        programming_language=None,
        defaults={},
    )

    assert result["heap_size_gb"] is None
    assert result["auto_delete_timeout"] is None
    assert result["server"] is None
    assert result["engine"] == "DeephavenCommunity"  # Built-in default
    assert result["extra_jvm_args"] is None
    assert result["extra_environment_vars"] is None
    assert result["admin_groups"] is None
    assert result["viewer_groups"] is None
    assert result["timeout_seconds"] == 60  # Built-in default
    assert result["session_arguments"] is None
    assert result["programming_language"] == "Python"  # Built-in default


@pytest.mark.asyncio
async def test_session_enterprise_create_success():
    """Test successful enterprise session creation."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_factory_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_session = MagicMock()

    # Configure the chain of mocks
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_factory_registry
    )
    mock_enterprise_factory_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    # Mock session registry get to raise RegistryItemNotFoundError for non-existent sessions
    mock_session_registry.get = AsyncMock(side_effect=RegistryItemNotFoundError("Session not found"))

    # Mock config
    enterprise_config = {
        "test-system": {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {"heap_size_gb": 4.0, "programming_language": "Python"},
            },
            "username": "testuser",
        }
    }

    def mock_get_config_section(manager, section):
        if section == "enterprise_sessions":
            return enterprise_config
        return {}

    mock_config_manager.get_config = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Clear any existing tracking
    _created_sessions = {}

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await session_enterprise_create(
        context,
        system_name="test-system",
        session_name="test-session",
        heap_size_gb=8.0,
        programming_language="Groovy",
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"] == "enterprise:test-system:test-session"
    assert result["system_name"] == "test-system"
    assert result["session_name"] == "test-session"

    # Verify session was added to registry
    mock_session_registry.add_session.assert_called_once()
    call_args = mock_session_registry.add_session.call_args
    session_manager = call_args[0][0]  # Manager is the only argument
    assert session_manager.full_name == "enterprise:test-system:test-session"

    # Session tracking is now verified through registry methods
    # Verify session was added (tracked automatically by add_session)


@pytest.mark.asyncio
async def test_session_enterprise_create_auto_generated_name():
    """Test enterprise session creation with auto-generated session name."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_factory_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_session = MagicMock()

    # Configure the chain of mocks
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_factory_registry
    )
    mock_enterprise_factory_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    # Mock session registry get to raise RegistryItemNotFoundError for non-existent sessions
    mock_session_registry.get = AsyncMock(side_effect=RegistryItemNotFoundError("Session not found"))

    # Mock config with username
    enterprise_config = {
        "test-system": {
            "session_creation": {"max_concurrent_sessions": 5, "defaults": {}},
            "username": "alice",
        }
    }

    def mock_get_config_section(manager, section):
        if section == "enterprise_sessions":
            return enterprise_config
        return {}

    mock_config_manager.get_config = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Clear any existing tracking
    _created_sessions = {}

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await session_enterprise_create(
        context,
        system_name="test-system",
        session_name=None,  # This should trigger auto-generation
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"].startswith("enterprise:test-system:mcp-alice-")
    assert result["system_name"] == "test-system"
    assert result["session_name"].startswith("mcp-alice-")

    # Clean up
    _created_sessions = {}


@pytest.mark.asyncio
async def test_session_enterprise_create_max_sessions_reached():
    """Test enterprise session creation when max concurrent sessions reached."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Mock config with low max limit
    enterprise_config = {
        "test-system": {
            "session_creation": {"max_concurrent_sessions": 2, "defaults": {}}
        }
    }

    def mock_get_config_section(manager, section):
        if section == "enterprise_sessions":
            return enterprise_config
        return {}

    mock_config_manager.get_config = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Mock registry to return 2 existing sessions (at limit)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=2)

    # Mock the session registry to return sessions for count validation
    async def mock_get(session_id):
        if session_id in [
            "enterprise:test-system:session1",
            "enterprise:test-system:session2",
        ]:
            return MagicMock()
        raise RegistryItemNotFoundError(f"Session {session_id} not found")

    mock_session_registry.get = AsyncMock(side_effect=mock_get)

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await session_enterprise_create(
        context, system_name="test-system", session_name="test-session"
    )

    # Verify failure due to max sessions reached
    assert result["success"] is False
    assert result["isError"] is True
    assert "Max concurrent sessions (2) reached" in result["error"]

    # Clean up
    _created_sessions = {}


@pytest.mark.asyncio
async def test_session_enterprise_create_disabled():
    """Test enterprise session creation when session creation is disabled."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Mock config with session creation disabled
    enterprise_config = {
        "test-system": {
            "session_creation": {
                "max_concurrent_sessions": 0,  # Disabled
                "defaults": {},
            }
        }
    }

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    result = await session_enterprise_create(
        context, system_name="test-system", session_name="test-session"
    )

    # Verify failure due to disabled session creation
    assert result["success"] is False
    assert result["isError"] is True
    assert "Session creation is disabled" in result["error"]


@pytest.mark.asyncio
async def test_session_enterprise_create_system_not_found_v2():
    """Test enterprise session creation with non-existent system."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Provide empty systems via async get_config()
    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    result = await session_enterprise_create(
        context, system_name="nonexistent-system", session_name="test-session"
    )

    # Verify failure due to system not found
    assert result["success"] is False
    assert result["isError"] is True
    assert "Enterprise system 'nonexistent-system' not found" in result["error"]


@pytest.mark.asyncio
async def test_session_enterprise_delete_success_v2():
    """Test successful enterprise session deletion."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_session_manager = MagicMock(spec=EnterpriseSessionManager)

    # Mock config
    enterprise_config = {
        "test-system": {"session_creation": {"max_concurrent_sessions": 5}}
    }

    def mock_get_config_section(manager, section):
        if section == "enterprise_sessions":
            return enterprise_config
        return {}

    # Mock session registry
    mock_session_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.close = AsyncMock()
    mock_session_registry.remove_session = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Session tracking is now handled by registry - no manual setup needed

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await session_enterprise_delete(
        context, system_name="test-system", session_name="test-session"
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"] == "enterprise:test-system:test-session"
    assert result["system_name"] == "test-system"
    assert result["session_name"] == "test-session"

    # Verify session was removed from registry
    mock_session_registry.remove_session.assert_called_once_with(
        "enterprise:test-system:test-session"
    )

    # Session tracking cleanup is now handled automatically by remove_session()


@pytest.mark.asyncio
async def test_session_enterprise_delete_not_found():
    """Test enterprise session deletion when session doesn't exist."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Mock config
    enterprise_config = {
        "test-system": {"session_creation": {"max_concurrent_sessions": 5}}
    }
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    # Mock session registry to return RegistryItemNotFoundError for non-existent session
    mock_session_registry.get = AsyncMock(side_effect=RegistryItemNotFoundError("Session not found"))

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_enterprise_delete(
        context, system_name="test-system", session_name="nonexistent-session"
    )

    # Verify failure due to session not found
    assert result["success"] is False
    assert result["isError"] is True
    assert (
        "Session 'enterprise:test-system:nonexistent-session' not found"
        in result["error"]
    )


@pytest.mark.asyncio
async def test_session_enterprise_delete_system_not_found_v2():
    """Test enterprise session deletion with non-existent system."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # No systems configured
    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_enterprise_delete(
        context, system_name="nonexistent-system", session_name="test-session"
    )

    # Verify failure due to system not found
    assert result["success"] is False
    assert result["isError"] is True
    assert "Enterprise system 'nonexistent-system' not found" in result["error"]


@pytest.mark.asyncio
async def test_check_session_limits_disabled():
    """Test _check_session_limits when sessions are disabled (max_sessions = 0)."""
    mock_session_registry = MagicMock()

    result = await _check_session_limits(mock_session_registry, "test-system", 0)

    assert result is not None
    assert (
        result["error"]
        == "Session creation is disabled for system 'test-system' (max_concurrent_sessions = 0)"
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_check_session_limits_under_limit():
    """Test _check_session_limits when under the session limit."""
    mock_session_registry = MagicMock()
    mock_session_registry.count_added_sessions = AsyncMock(return_value=2)

    result = await _check_session_limits(mock_session_registry, "test-system", 5)

    assert result is None  # No error when under limit
    mock_session_registry.count_added_sessions.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_session_limits_at_limit():
    """Test _check_session_limits when at the session limit."""
    mock_session_registry = MagicMock()
    mock_session_registry.count_added_sessions = AsyncMock(return_value=5)

    result = await _check_session_limits(mock_session_registry, "test-system", 5)

    assert result is not None
    assert (
        result["error"]
        == "Max concurrent sessions (5) reached for system 'test-system'"
    )
    assert result["isError"] is True
    mock_session_registry.count_added_sessions.assert_awaited_once()


def test_generate_session_name_if_none_with_name():
    """Test _generate_session_name_if_none when session_name is provided."""
    system_config = {"username": "testuser"}

    result = _generate_session_name_if_none(system_config, "provided-name")

    assert result == "provided-name"


def test_generate_session_name_if_none_with_username():
    """Test _generate_session_name_if_none when no name provided but username exists."""
    system_config = {"username": "testuser"}

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.session_enterprise.datetime"
    ) as mock_datetime:
        mock_datetime.now().strftime.return_value = "20240101-1200"
        result = _generate_session_name_if_none(system_config, None)

    assert result == "mcp-testuser-20240101-1200"


def test_generate_session_name_if_none_without_username():
    """Test _generate_session_name_if_none when no name or username provided."""
    system_config = {}  # No username

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.session_enterprise.datetime"
    ) as mock_datetime:
        mock_datetime.now().strftime.return_value = "20240101-1200"
        result = _generate_session_name_if_none(system_config, None)

    assert result == "mcp-session-20240101-1200"


@pytest.mark.asyncio
async def test_check_session_id_available_success():
    """Test _check_session_id_available when session ID is available."""
    mock_session_registry = MagicMock()
    mock_session_registry.get = AsyncMock(side_effect=RegistryItemNotFoundError("Session not found"))

    result = await _check_session_id_available(mock_session_registry, "test-session-id")

    assert result is None  # No error when session doesn't exist


@pytest.mark.asyncio
async def test_check_session_id_available_conflict():
    """Test _check_session_id_available when session ID already exists."""
    mock_session_registry = MagicMock()
    mock_session_registry.get = AsyncMock(return_value=MagicMock())  # Session exists

    result = await _check_session_id_available(
        mock_session_registry, "existing-session-id"
    )

    assert result is not None
    assert result["error"] == "Session 'existing-session-id' already exists"
    assert result["isError"] is True


def test_resolve_session_parameters_with_defaults():
    """Test _resolve_session_parameters using configuration defaults."""
    defaults = {
        "heap_size_gb": 8.0,
        "auto_delete_timeout": 3600,
        "server": "default-server",
        "programming_language": "Python",
    }

    result = _resolve_session_parameters(
        heap_size_gb=None,
        auto_delete_timeout=None,
        server=None,
        engine=None,
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        timeout_seconds=None,
        session_arguments=None,
        programming_language=None,
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 8.0
    assert result["auto_delete_timeout"] == 3600
    assert result["server"] == "default-server"
    assert result["engine"] == "DeephavenCommunity"  # Default when not specified
    assert result["programming_language"] == "Python"


def test_resolve_session_parameters_with_overrides():
    """Test _resolve_session_parameters with parameter overrides."""
    defaults = {
        "heap_size_gb": 8.0,
        "auto_delete_timeout": 3600,
        "programming_language": "Python",
    }

    result = _resolve_session_parameters(
        heap_size_gb=16.0,  # Override
        auto_delete_timeout=7200,  # Override
        server="custom-server",  # Override
        engine="CustomEngine",  # Override
        extra_jvm_args=["-Xms4g"],
        extra_environment_vars=["VAR=value"],
        admin_groups=["admins"],
        viewer_groups=["viewers"],
        timeout_seconds=300.0,
        session_arguments={"arg": "value"},
        programming_language="Groovy",  # Override
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 16.0
    assert result["auto_delete_timeout"] == 7200
    assert result["server"] == "custom-server"
    assert result["engine"] == "CustomEngine"
    assert result["extra_jvm_args"] == ["-Xms4g"]
    assert result["extra_environment_vars"] == ["VAR=value"]
    assert result["admin_groups"] == ["admins"]
    assert result["viewer_groups"] == ["viewers"]
    assert result["timeout_seconds"] == 300.0
    assert result["session_arguments"] == {"arg": "value"}
    assert result["programming_language"] == "Groovy"


def test_resolve_session_parameters_zero_values():
    """Test _resolve_session_parameters handles zero values correctly."""
    defaults = {
        "auto_delete_timeout": 3600,
        "timeout_seconds": 120.0,
    }

    result = _resolve_session_parameters(
        heap_size_gb=None,
        auto_delete_timeout=0,  # Explicitly set to 0
        server=None,
        engine=None,
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        timeout_seconds=0.0,  # Explicitly set to 0.0
        session_arguments=None,
        programming_language=None,
        defaults=defaults,
    )

    assert result["auto_delete_timeout"] == 0  # Should use explicit 0, not default
    assert result["timeout_seconds"] == 0.0  # Should use explicit 0.0, not default
