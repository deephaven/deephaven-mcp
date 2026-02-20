"""
Tests for deephaven_mcp.mcp_systems_server._tools.session.
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
from deephaven_mcp.mcp_systems_server._tools.session import (
    session_details,
    sessions_list,
)
from deephaven_mcp.mcp_systems_server._tools.session_community import (
    session_community_create,
    session_community_delete,
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

# =============================================================================
# session_community_create edge case tests
# =============================================================================


@pytest.mark.asyncio
async def test_session_community_create_with_auth_token_parameter():
    """Test lines 3740: auth_token parameter takes precedence."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "auth_token": "default_token",
                "auth_token_env_var": "SOME_VAR",
            },
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("not found")
    )

    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?psk=test_token"
    )
    mock_launched_session.container_id = "test"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "test_token"

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.launch_session"
        ) as mock_launch_session,
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.find_available_port",
            return_value=10000,
        ),
        patch.object(
            mock_launched_session,
            "wait_until_ready",
            new=AsyncMock(return_value=True),
        ),
    ):

        mock_launch_session.return_value = mock_launched_session

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await session_community_create(
            context,
            session_name="test-session",
            auth_token="explicit_token",  # This should take precedence
        )

        assert result["success"] is True
        # Verify explicit token was used
        launch_call = mock_launch_session.call_args
        assert launch_call[1]["auth_token"] == "explicit_token"


@pytest.mark.asyncio
async def test_session_community_create_with_auth_token_env_var_set():
    """Test lines 3742-3746: auth_token_env_var when env var exists."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "auth_token_env_var": "TEST_AUTH_TOKEN",
            },
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("not found")
    )

    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?psk=test_token"
    )
    mock_launched_session.container_id = "test"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "test_token"

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.launch_session"
        ) as mock_launch_session,
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.find_available_port",
            return_value=10000,
        ),
        patch.object(
            mock_launched_session,
            "wait_until_ready",
            new=AsyncMock(return_value=True),
        ),
        patch.dict(os.environ, {"TEST_AUTH_TOKEN": "env_token_value"}),
    ):

        mock_launch_session.return_value = mock_launched_session

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await session_community_create(context, session_name="test-session")

        assert result["success"] is True
        # Verify env var token was used
        launch_call = mock_launch_session.call_args
        assert launch_call[1]["auth_token"] == "env_token_value"


@pytest.mark.asyncio
async def test_session_community_create_with_auth_token_from_defaults():
    """Test line 3750: auth_token from defaults."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "auth_token": "default_token",
            },
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("not found")
    )

    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?psk=test_token"
    )
    mock_launched_session.container_id = "test"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "test_token"

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.launch_session"
        ) as mock_launch_session,
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.find_available_port",
            return_value=10000,
        ),
        patch.object(
            mock_launched_session,
            "wait_until_ready",
            new=AsyncMock(return_value=True),
        ),
    ):

        mock_launch_session.return_value = mock_launched_session

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await session_community_create(context, session_name="test-session")

        assert result["success"] is True
        # Verify default token was used
        launch_call = mock_launch_session.call_args
        assert launch_call[1]["auth_token"] == "default_token"


@pytest.mark.asyncio
async def test_session_community_create_session_already_exists():
    """Test lines 3766-3770: session ID already exists."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    # Session already exists — get() returns successfully (no exception)
    mock_session_registry.get = AsyncMock(return_value=MagicMock())

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_create(context, session_name="test-session")

    assert result["success"] is False
    assert "already exists" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_health_check_timeout_with_cleanup():
    """Test lines 3819-3832: health check timeout with successful cleanup."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("not found")
    )

    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?psk=test_token"
    )
    mock_launched_session.container_id = "test"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "test_token"

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.launch_session"
        ) as mock_launch_session,
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.find_available_port",
            return_value=10000,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.generate_auth_token",
            return_value="token",
        ),
        patch.object(
            mock_launched_session,
            "wait_until_ready",
            new=AsyncMock(return_value=False),
        ),
    ):

        mock_launch_session.return_value = mock_launched_session
        mock_launched_session.stop = AsyncMock()  # Cleanup succeeds

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await session_community_create(context, session_name="test-session")

        assert result["success"] is False
        assert "failed to start" in result["error"].lower()
        assert result["isError"] is True
        # Verify cleanup was attempted
        mock_launched_session.stop.assert_called_once()


@pytest.mark.asyncio
async def test_session_community_create_with_python_launch_method():
    """Test lines 3891-3892: python launch method sets process_id."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "launch_method": "python",
            },
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("not found")
    )

    mock_process = MagicMock()
    mock_process.pid = 12345
    mock_launched_session = MagicMock(spec=PythonLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "python"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = "http://localhost:10000"
    mock_launched_session.process = mock_process
    mock_launched_session.auth_type = "anonymous"
    mock_launched_session.auth_token = None

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.launch_session"
        ) as mock_launch_session,
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.find_available_port",
            return_value=10000,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.generate_auth_token",
            return_value="token",
        ),
        patch.object(
            mock_launched_session,
            "wait_until_ready",
            new=AsyncMock(return_value=True),
        ),
    ):

        mock_launch_session.return_value = mock_launched_session

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await session_community_create(context, session_name="test-session")

        assert result["success"] is True
        assert result["process_id"] == 12345


# =============================================================================
# session_community_delete edge case tests
# =============================================================================


@pytest.mark.asyncio
async def test_session_community_delete_non_community_session():
    """Test lines 4005-4009: trying to delete non-community session."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_manager = MagicMock()
    mock_manager.full_name = "enterprise:system:test-session"
    mock_manager._name = "test-session"
    mock_manager.source = "dynamic"
    mock_manager.system_type = SystemType.ENTERPRISE  # Not COMMUNITY

    mock_session_registry.get = AsyncMock(return_value=mock_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(context, session_name="test-session")

    assert result["success"] is False
    assert "not a community session" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_delete_close_fails_but_continues():
    """Test lines 4034-4047: close fails but removal continues."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.launch_method = "docker"

    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.full_name = "community:dynamic:test-session"
    mock_manager._name = "test-session"
    mock_manager.source = "dynamic"
    mock_manager.system_type = SystemType.COMMUNITY
    mock_manager.launched_session = mock_launched_session
    mock_manager.close = AsyncMock(side_effect=Exception("Close failed"))

    mock_session_registry.get = AsyncMock(return_value=mock_manager)
    mock_session_registry.remove_session = AsyncMock(return_value=mock_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(context, session_name="test-session")

    # Should still succeed despite close failure
    assert result["success"] is True
    # Verify removal was still attempted
    mock_session_registry.remove_session.assert_called_once()


@pytest.mark.asyncio
async def test_session_community_delete_removal_fails():
    """Test lines 4055-4060: removal from registry fails."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.launch_method = "docker"

    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.full_name = "community:dynamic:test-session"
    mock_manager._name = "test-session"
    mock_manager.source = "dynamic"
    mock_manager.system_type = SystemType.COMMUNITY
    mock_manager.launched_session = mock_launched_session
    mock_manager.close = AsyncMock()

    mock_session_registry.get = AsyncMock(return_value=mock_manager)
    mock_session_registry.remove_session = AsyncMock(
        side_effect=Exception("Removal failed")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(context, session_name="test-session")

    assert result["success"] is False
    assert "Failed to remove session" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_delete_unexpected_exception():
    """Test lines 4075-4081: unexpected exception during delete."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Make get() raise an unexpected exception
    mock_session_registry.get = AsyncMock(side_effect=RuntimeError("Unexpected error"))

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(context, session_name="test-session")

    assert result["success"] is False
    assert "Unexpected error" in result["error"]
    assert result["isError"] is True


# =============================================================================
# session_details with dynamic community sessions tests
# =============================================================================


@pytest.mark.asyncio
async def test_session_details_dynamic_community_with_all_fields():
    """Test lines 975-998: all dynamic session fields present."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Create a mock DynamicCommunitySessionManager with all fields
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?psk=abc123"
    )
    mock_launched_session.container_id = "de18601a1657"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "abc123"

    session_config = {
        "host": "localhost",
        "port": 10000,
        "auth_type": "PSK",
    }

    # Create actual manager instance
    manager = DynamicCommunitySessionManager(
        name="test-session",
        config=session_config,
        launched_session=mock_launched_session,
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_details(context, session_id="community:dynamic:test-session")

    # Verify all dynamic fields were added
    assert result["success"] is True
    session_info = result["session"]
    assert "connection_url" in session_info
    # Note: connection_url_with_auth removed from to_dict() for security
    assert session_info["auth_type"] == "PSK"
    assert session_info["launch_method"] == "docker"
    assert session_info["port"] == 10000
    assert session_info["container_id"] == "de18601a1657"


@pytest.mark.asyncio
async def test_session_details_dynamic_community_with_python_process_id():
    """Test lines 994-997: process_id field for python launch method."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Create a python-launched session with process
    mock_process = MagicMock()
    mock_process.pid = 54321

    mock_launched_session = MagicMock(spec=PythonLaunchedSession)
    mock_launched_session.port = 10001
    mock_launched_session.launch_method = "python"
    mock_launched_session.connection_url = "http://localhost:10001"
    mock_launched_session.connection_url_with_auth = "http://localhost:10001"
    mock_launched_session.process = mock_process
    mock_launched_session.auth_type = "anonymous"
    mock_launched_session.auth_token = None

    session_config = {
        "host": "localhost",
        "port": 10001,
        "auth_type": "anonymous",
    }

    # Create actual manager instance
    manager = DynamicCommunitySessionManager(
        name="pip-session",
        config=session_config,
        launched_session=mock_launched_session,
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_details(context, session_id="community:dynamic:pip-session")

    # Verify process_id was added
    assert result["success"] is True
    session_info = result["session"]
    assert session_info["launch_method"] == "python"
    assert session_info["process_id"] == 54321
    assert "container_id" not in session_info  # Should not have container_id for pip


@pytest.mark.asyncio
async def test_session_details_dynamic_community_with_partial_fields():
    """Test lines 975-998: only some dynamic fields present."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Create a session with minimal fields
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10002
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10002"
    mock_launched_session.connection_url_with_auth = "http://localhost:10002"
    mock_launched_session.container_id = "minimal123"
    mock_launched_session.auth_type = "anonymous"
    mock_launched_session.auth_token = None

    session_config = {
        "host": "localhost",
        "port": 10002,
        "auth_type": "anonymous",
    }

    # Create actual manager instance
    manager = DynamicCommunitySessionManager(
        name="minimal-session",
        config=session_config,
        launched_session=mock_launched_session,
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_details(
        context, session_id="community:dynamic:minimal-session"
    )

    # Verify fields that should be present
    assert result["success"] is True
    session_info = result["session"]
    assert "connection_url" in session_info
    assert session_info["launch_method"] == "docker"
    assert session_info["port"] == 10002
    assert session_info["container_id"] == "minimal123"


@pytest.mark.asyncio
async def test_session_details_logs_version_info():
    """Test that session_details logs programming language and Deephaven versions when available."""
    # Import the function
    import enum

    from deephaven_mcp.mcp_systems_server._tools.session import session_details
    from deephaven_mcp.resource_manager._manager import ResourceLivenessStatus
    from deephaven_mcp.resource_manager._registry_combined import (
        CombinedSessionRegistry,
    )

    # Create mocks
    context = MagicMock()
    session_id = "test-session"
    session = AsyncMock()

    # Setup session registry and session manager
    session_registry = MagicMock(spec=CombinedSessionRegistry)
    mgr = AsyncMock()

    # Configure session manager with required properties
    mgr.is_alive = AsyncMock(return_value=True)
    mgr.system_type = MagicMock()
    mgr.system_type.name = "COMMUNITY"
    mgr.source = "test-source"
    mgr.name = "test"

    # Mock liveness status
    status_mock = MagicMock(spec=enum.Enum)
    status_mock.name = "ONLINE"
    mgr.liveness_status = AsyncMock(return_value=(status_mock, ""))

    # Configure the session object with programming_language
    session.programming_language = "python"

    # Setup mgr.get to return our session
    mgr.get = AsyncMock(return_value=session)

    # Configure session registry to return our manager
    session_registry.get = AsyncMock(return_value=mgr)

    # Setup context.request_context.lifespan_context properly
    request_context = MagicMock()
    request_context.lifespan_context = {"session_registry": session_registry}
    context.request_context = request_context

    # Mock the queries module to return version information
    mock_queries = MagicMock()
    mock_queries.get_programming_language_version = AsyncMock(return_value="3.9.7")
    mock_queries.get_dh_versions = AsyncMock(return_value=("0.24.0", None))

    # Use a logger mock to verify debug logs
    mock_logger = MagicMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session.queries",
            mock_queries,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session._LOGGER",
            mock_logger,
        ),
    ):
        # Call the function
        result = await session_details(context, session_id, attempt_to_connect=True)

        # Verify the function returned successfully
        assert result["success"] is True
        assert "session" in result
        assert result["session"]["programming_language"] == "python"
        assert result["session"]["programming_language_version"] == "3.9.7"
        assert result["session"]["deephaven_community_version"] == "0.24.0"

        # Verify that the debug log messages were called (lines 447 and 458)
        mock_logger.debug.assert_any_call(
            f"[mcp_systems_server:session_details] Session '{session_id}' programming_language_version: 3.9.7"
        )
        mock_logger.debug.assert_any_call(
            f"[mcp_systems_server:session_details] Session '{session_id}' versions: community=0.24.0, enterprise=None"
        )


@pytest.mark.asyncio
async def test_sessions_list_success():
    """Test sessions_list with multiple sessions of different types."""
    # Mock session registry
    mock_registry = AsyncMock()

    # Create mock session managers
    mock_session_mgr1 = AsyncMock()
    mock_session_mgr1.system_type.name = "COMMUNITY"
    mock_session_mgr1.source = "source1"
    mock_session_mgr1.name = "session1"

    mock_session_mgr2 = AsyncMock()
    mock_session_mgr2.system_type.name = "ENTERPRISE"
    mock_session_mgr2.source = "source2"
    mock_session_mgr2.name = "session2"

    mock_registry.get_all.return_value = RegistrySnapshot.simple(
        items={
            "session1": mock_session_mgr1,
            "session2": mock_session_mgr2,
        },
    )

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await sessions_list(mock_context)

    # Verify results
    assert result["success"] is True
    assert len(result["sessions"]) == 2

    # Check first session
    session1 = next(s for s in result["sessions"] if s["session_id"] == "session1")
    assert session1["type"] == "COMMUNITY"
    assert session1["source"] == "source1"
    assert session1["session_name"] == "session1"
    assert "available" not in session1  # Should not check availability

    # Check second session
    session2 = next(s for s in result["sessions"] if s["session_id"] == "session2")
    assert session2["type"] == "ENTERPRISE"
    assert session2["source"] == "source2"
    assert session2["session_name"] == "session2"
    assert "available" not in session2  # Should not check availability

    # COMPLETED with no errors should not include initialization info
    assert "initialization" not in result


@pytest.mark.asyncio
async def test_sessions_list_with_unknown_type():
    """Test sessions_list with a session that has no system_type attribute."""
    # Mock session registry
    mock_registry = AsyncMock()

    # Create a mock session manager with no system_type
    mock_session_mgr = AsyncMock()
    mock_session_mgr.system_type = None
    mock_session_mgr.source = "source"
    mock_session_mgr.name = "session"

    mock_registry.get_all.return_value = RegistrySnapshot.simple(
        items={"session": mock_session_mgr},
    )

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await sessions_list(mock_context)

    # Verify results
    assert result["success"] is True
    assert len(result["sessions"]) == 1
    # Check that we have an error entry for this session since system_type is None
    assert result["sessions"][0]["session_id"] == "session"
    assert "error" in result["sessions"][0]
    # COMPLETED with no errors should not include initialization info
    assert "initialization" not in result


@pytest.mark.asyncio
async def test_sessions_list_with_processing_error():
    """Test sessions_list when processing a session raises an exception."""
    # Mock session registry
    mock_registry = AsyncMock()

    # Create a session manager that will cause an exception during processing
    mock_session_mgr = AsyncMock()
    # Configure system_type.name to raise an exception when accessed
    mock_system_type = MagicMock()
    type(mock_system_type).name = PropertyMock(
        side_effect=Exception("Processing error")
    )
    mock_session_mgr.system_type = mock_system_type

    mock_registry.get_all.return_value = RegistrySnapshot.simple(
        items={"session": mock_session_mgr},
    )

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await sessions_list(mock_context)

    # Verify results
    assert result["success"] is True
    assert len(result["sessions"]) == 1
    assert "error" in result["sessions"][0]
    assert result["sessions"][0]["session_id"] == "session"
    # COMPLETED with no errors should not include initialization info
    assert "initialization" not in result


@pytest.mark.asyncio
async def test_sessions_list_registry_error():
    """Test sessions_list when the session registry raises an exception."""
    # Mock context with registry that raises an exception
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context.__getitem__.side_effect = Exception(
        "Registry error"
    )

    # Call function
    result = await sessions_list(mock_context)

    # Verify results
    assert result["success"] is False


@pytest.mark.asyncio
async def test_session_details_session_not_found():
    """Test session_details for a non-existent session."""
    mock_registry = AsyncMock()
    mock_registry.get.side_effect = Exception("Session not found")

    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    result = await session_details(mock_context, "nonexistent")

    assert result["success"] is False
    assert "error" in result
    assert "not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_details_with_session_error():
    """Test session_details when getting the session raises an exception."""
    # Mock session registry
    mock_registry = AsyncMock()

    # Create mock session manager that raises an exception when liveness_status is called
    mock_session_mgr = AsyncMock()
    mock_system_type = MagicMock()
    mock_system_type.name = "COMMUNITY"
    mock_session_mgr.system_type = mock_system_type
    mock_session_mgr.source = "source1"
    mock_session_mgr.name = "session1"
    # Set is_alive to raise an exception
    mock_session_mgr.is_alive = AsyncMock(side_effect=Exception("Session error"))
    mock_session_mgr.liveness_status.side_effect = Exception("Liveness status error")

    mock_registry.get.return_value = mock_session_mgr

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await session_details(mock_context, "session1")

    # Verify results
    assert result["success"] is True
    assert "session" in result
    assert result["session"]["available"] is False


@pytest.mark.asyncio
async def test_session_details_with_processing_error():
    """Test session_details when processing a session raises an exception."""
    # Mock session registry
    mock_registry = AsyncMock()

    # Create a session manager that will cause an exception during processing
    mock_session_mgr = AsyncMock()
    # Configure system_type.name to raise an exception when accessed
    mock_system_type = MagicMock()
    type(mock_system_type).name = PropertyMock(
        side_effect=Exception("Processing error")
    )
    mock_session_mgr.system_type = mock_system_type
    mock_session_mgr.is_alive = AsyncMock(return_value=True)
    # Mock liveness_status to return a tuple of (status, detail) as expected by the implementation
    mock_status = MagicMock()
    mock_status.name = "ONLINE"
    mock_session_mgr.liveness_status.return_value = (
        mock_status,
        "All systems operational",
    )

    mock_registry.get.return_value = mock_session_mgr

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await session_details(mock_context, "session1")

    # Verify results
    assert result["success"] is False
    assert "error" in result
    assert "Processing error" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_details_registry_error():
    """Test session_details when the session registry raises an exception."""
    # Mock context with registry that raises an exception
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context.__getitem__.side_effect = Exception(
        "Registry error"
    )

    # Call function
    result = await session_details(mock_context, "session1")

    # Verify results
    assert result["success"] is False
    assert "error" in result
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_details_success_with_programming_language():
    """Test session_details for an existing session with programming_language property."""
    # Mock session registry
    mock_registry = AsyncMock()

    # Create mock session with programming_language
    mock_session = MagicMock()
    mock_session.programming_language = "python"

    # Create mock session manager
    mock_session_mgr = AsyncMock()
    mock_system_type = MagicMock()
    mock_system_type.name = "COMMUNITY"
    mock_session_mgr.system_type = mock_system_type
    mock_session_mgr.source = "source1"
    mock_session_mgr.name = "session1"
    mock_session_mgr.is_alive = AsyncMock(return_value=True)
    mock_session_mgr.get = AsyncMock(return_value=mock_session)
    # Mock liveness_status to return a tuple of (status, detail) as expected by the implementation
    mock_status = MagicMock()
    mock_status.name = "ONLINE"
    mock_session_mgr.liveness_status.return_value = (
        mock_status,
        "All systems operational",
    )

    # Set up registry to return our mock session manager
    mock_registry.get.return_value = mock_session_mgr

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await session_details(mock_context, "session1", attempt_to_connect=True)

    # Verify results
    assert result["success"] is True
    assert "session" in result
    assert result["session"]["session_id"] == "session1"
    assert result["session"]["type"] == "COMMUNITY"
    assert result["session"]["source"] == "source1"
    assert result["session"]["session_name"] == "session1"
    assert result["session"]["available"] is True
    assert result["session"]["liveness_status"] == "ONLINE"
    assert result["session"]["programming_language"] == "python"
    assert result["session"]["liveness_detail"] == "All systems operational"


@pytest.mark.asyncio
async def test_session_details_success_without_programming_language():
    """Test session_details for an existing session without programming_language property."""
    # Mock session registry
    mock_registry = AsyncMock()

    # Create mock session without programming_language attribute
    mock_session = MagicMock(spec=[])

    # Create mock session manager
    mock_session_mgr = AsyncMock()
    mock_system_type = MagicMock()
    mock_system_type.name = "COMMUNITY"
    mock_session_mgr.system_type = mock_system_type
    mock_session_mgr.source = "source1"
    mock_session_mgr.name = "session1"
    mock_session_mgr.is_alive = AsyncMock(return_value=True)
    mock_session_mgr.get = AsyncMock(return_value=mock_session)
    # Mock liveness_status to return a tuple of (status, detail) as expected by the implementation
    mock_status = MagicMock()
    mock_status.name = "ONLINE"
    mock_session_mgr.liveness_status.return_value = (
        mock_status,
        "All systems operational",
    )

    # Set up registry to return our mock session manager
    mock_registry.get.return_value = mock_session_mgr

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await session_details(mock_context, "session1", attempt_to_connect=True)

    # Verify results
    assert result["success"] is True
    assert "session" in result
    assert result["session"]["session_id"] == "session1"
    assert result["session"]["type"] == "COMMUNITY"
    assert result["session"]["source"] == "source1"
    assert result["session"]["session_name"] == "session1"
    assert result["session"]["available"] is True
    assert result["session"]["liveness_status"] == "ONLINE"
    assert "programming_language" not in result["session"]
    assert result["session"]["liveness_detail"] == "All systems operational"


@pytest.mark.asyncio
async def test_dynamic_community_session_has_correct_source():
    """Test that DynamicCommunitySessionManager has source='dynamic'."""
    from unittest.mock import MagicMock

    from deephaven_mcp.resource_manager import (
        DockerLaunchedSession,
        DynamicCommunitySessionManager,
        SystemType,
    )

    # Create a mock launched session
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = "http://localhost:10000"

    # Create DynamicCommunitySessionManager
    manager = DynamicCommunitySessionManager(
        name="test-session",
        config={"host": "localhost", "port": 10000},
        launched_session=mock_launched_session,
    )

    # Verify source is "dynamic"
    assert (
        manager.source == "dynamic"
    ), f"Expected source='dynamic', got source='{manager.source}'"

    # Verify system_type is COMMUNITY
    assert manager.system_type == SystemType.COMMUNITY

    # Verify full_name format is correct
    assert manager.full_name == "community:dynamic:test-session"

    # Verify name
    assert manager.name == "test-session"


@pytest.mark.asyncio
async def test_session_details_to_dict_exception():
    """Test coverage for lines 1021-1022: exception when to_dict() fails."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Create a real DynamicCommunitySessionManager instance
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = "http://localhost:10000/?psk=test"
    mock_launched_session.container_id = "abc123"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "test"

    manager = DynamicCommunitySessionManager(
        name="test-session",
        config={"host": "localhost", "port": 10000},
        launched_session=mock_launched_session,
    )

    # Mock to_dict() to raise an exception
    with patch.object(
        manager, "to_dict", side_effect=RuntimeError("Simulated failure in to_dict")
    ):
        mock_session_registry.get = AsyncMock(return_value=manager)

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await session_details(
            context, session_id="community:dynamic:test-session"
        )

        # Should still succeed despite to_dict() exception
        assert result["success"] is True
        session_info = result["session"]

        # Basic session info should be present
        assert session_info["session_id"] == "community:dynamic:test-session"
        assert session_info["type"] == "COMMUNITY"
        assert session_info["source"] == "dynamic"
        assert session_info["session_name"] == "test-session"

        # Dynamic fields from to_dict() should NOT be present (because it failed)
        # These would normally be added by to_dict() if it succeeded
        assert "connection_url" not in session_info  # This comes from to_dict()
        assert "port" not in session_info  # This comes from to_dict()
        assert "launch_method" not in session_info  # This comes from to_dict()


# =============================================================================
# sessions_list initialization status tests
# =============================================================================


@pytest.mark.asyncio
async def test_sessions_list_discovery_in_progress():
    """Test sessions_list shows status message when discovery is in progress."""
    mock_session_registry = MagicMock()
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={},
            phase=InitializationPhase.LOADING,
            errors={},
        )
    )

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await sessions_list(context)

    assert result["success"] is True
    assert "initialization" in result
    assert "still in progress" in result["initialization"]["status"]


@pytest.mark.asyncio
async def test_sessions_list_discovery_in_progress_with_errors():
    """Test sessions_list shows both in-progress status and errors simultaneously."""
    mock_session_registry = MagicMock()
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={},
            phase=InitializationPhase.LOADING,
            errors={"factory1": "Connection refused"},
        )
    )

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await sessions_list(context)

    assert result["success"] is True
    assert "initialization" in result
    assert "still in progress" in result["initialization"]["status"]
    assert "errors" in result["initialization"]
    assert "factory1" in result["initialization"]["errors"]


@pytest.mark.asyncio
async def test_sessions_list_completed_with_errors():
    """Test sessions_list shows initialization errors when discovery completed with failures."""
    mock_session_registry = MagicMock()
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={},
            phase=InitializationPhase.COMPLETED,
            errors={"factory1": "Connection failed: Connection refused"},
        )
    )

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await sessions_list(context)

    assert result["success"] is True
    assert "initialization" in result
    assert "errors" in result["initialization"]
    assert "factory1" in result["initialization"]["errors"]
    assert "connection issues" in result["initialization"]["status"]


@pytest.mark.asyncio
async def test_sessions_list_completed_no_errors():
    """Test sessions_list does not include status when everything is fine."""
    mock_session_registry = MagicMock()
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.simple(
            items={},
        )
    )

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await sessions_list(context)

    assert result["success"] is True
    assert "initialization" not in result


@pytest.mark.asyncio
async def test_sessions_list_shows_errors_even_with_sessions():
    """Test sessions_list always shows init_errors since they are set once during discovery."""
    # Create mock enterprise session manager
    mock_mgr = MagicMock()
    mock_mgr.full_name = "enterprise:factory1:session1"
    mock_mgr.system_type = SystemType.ENTERPRISE
    mock_mgr.source = "factory1"
    mock_mgr.name = "session1"

    mock_session_registry = MagicMock()
    mock_session_registry.get_all = AsyncMock(
        return_value=RegistrySnapshot.with_initialization(
            items={"enterprise:factory1:session1": mock_mgr},
            phase=InitializationPhase.COMPLETED,
            errors={"factory1": "Connection failed: timeout"},
        )
    )

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await sessions_list(context)

    assert result["success"] is True
    # init_errors are set once during discovery and not cleared, so always shown
    assert "initialization" in result
    assert "errors" in result["initialization"]
    assert "factory1" in result["initialization"]["errors"]
