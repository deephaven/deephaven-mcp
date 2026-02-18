"""
Tests for deephaven_mcp.mcp_systems_server._tools.session_community.
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
from deephaven_mcp.mcp_systems_server._tools.session_community import (
    _normalize_auth_type,
    _resolve_community_session_parameters,
    session_community_create,
    session_community_credentials,
    session_community_delete,
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
async def test_session_community_create_success():
    """Test successful community session creation."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Mock config
    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "launch_method": "docker",
                "auth_type": "PSK",
                "heap_size_gb": 4.0,
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

    # Mock launcher
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?psk=test_token"
    )
    mock_launched_session.container_id = "test_container"
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
            return_value="test_token",
        ),
        patch.object(
            mock_launched_session, "wait_until_ready", new=AsyncMock(return_value=True)
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
        )

        # Verify success
        assert result["success"] is True
        assert result["session_id"] == "community:dynamic:test-session"
        assert result["session_name"] == "test-session"
        assert result["port"] == 10000
        assert "connection_url" in result

        # Verify session was added to registry
        mock_session_registry.add_session.assert_called_once()


@pytest.mark.asyncio
async def test_session_community_create_not_configured():
    """Test community session creation when not configured."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # No session_creation config
    full_config = {"community": {}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

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
    )

    # Verify error
    assert result["success"] is False
    assert "not configured" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_sessions_disabled():
    """Test community session creation when max_concurrent_sessions is 0 (disabled)."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 0,  # Disabled
            "defaults": {},
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
    mock_launched_session.container_id = "test_container"
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
            return_value="test_token",
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
        )

        # Should succeed - limit is disabled so no limit check
        assert result["success"] is True
        assert result["session_id"] == "community:dynamic:test-session"
        # count_added_sessions should NOT have been called since limit is disabled
        mock_session_registry.count_added_sessions.assert_not_called()


@pytest.mark.asyncio
async def test_session_community_create_max_sessions_reached():
    """Test community session creation when max sessions reached."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 2,
            "defaults": {},
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=2)

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
    )

    # Verify error
    assert result["success"] is False
    assert "Session limit reached" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_launch_failure():
    """Test community session creation when launch fails."""
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
    ):

        mock_launch_session.side_effect = Exception("Launch failed")

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
        )

        # Verify error
        assert result["success"] is False
        assert "Launch failed" in result["error"]
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_delete_success():
    """Test successful community session deletion."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Create a mock launched session (Docker by default)
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.launch_method = "docker"

    # Create a mock dynamic session manager
    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.full_name = "community:dynamic:test-session"
    mock_manager._name = "test-session"
    mock_manager.source = "dynamic"
    mock_manager.system_type = SystemType.COMMUNITY
    mock_manager.launched_session = mock_launched_session
    mock_manager.close = AsyncMock()

    mock_session_registry.get = AsyncMock(return_value=mock_manager)
    mock_session_registry.remove_session = AsyncMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(
        context,
        session_name="test-session",
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"] == "community:dynamic:test-session"
    assert result["session_name"] == "test-session"

    # Verify session was closed and removed
    mock_manager.close.assert_called_once()
    mock_session_registry.remove_session.assert_called_once()


@pytest.mark.asyncio
async def test_session_community_delete_python_session():
    """Test deleting a python-launched session to cover untrack_python_process call."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_instance_tracker = create_mock_instance_tracker()

    # Create a mock python-launched session
    mock_launched_session = MagicMock(spec=PythonLaunchedSession)
    mock_launched_session.launch_method = "python"

    # Create a mock python-launched session manager
    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.full_name = "community:dynamic:python-session"
    mock_manager._name = "python-session"
    mock_manager.source = "dynamic"
    mock_manager.system_type = SystemType.COMMUNITY
    mock_manager.launched_session = mock_launched_session
    mock_manager.close = AsyncMock()

    mock_session_registry.get = AsyncMock(return_value=mock_manager)
    mock_session_registry.remove_session = AsyncMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": mock_instance_tracker,
        }
    )

    result = await session_community_delete(
        context,
        session_name="python-session",
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"] == "community:dynamic:python-session"

    # Verify untrack_python_process was called (line 4197)
    mock_instance_tracker.untrack_python_process.assert_called_once_with(
        "python-session"
    )

    # Verify session was closed and removed
    mock_manager.close.assert_called_once()
    mock_session_registry.remove_session.assert_called_once()


@pytest.mark.asyncio
async def test_session_community_delete_not_found():
    """Test community session deletion when session not found."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Not found")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(
        context,
        session_name="nonexistent",
    )

    # Verify error
    assert result["success"] is False
    assert "not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_delete_not_dynamic():
    """Test community session deletion when session is not dynamic."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Create a mock static session manager (not dynamic)
    mock_manager = MagicMock()
    mock_manager.full_name = "community:static:test-session"
    mock_manager.source = "static"  # Not dynamic!
    mock_manager.system_type = SystemType.COMMUNITY

    mock_session_registry.get = AsyncMock(return_value=mock_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(
        context,
        session_name="test-session",
    )

    # Verify error
    assert result["success"] is False
    assert "Only dynamically created sessions" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_case_insensitive_params():
    """Test that launch_method, programming_language, and auth_type are case-insensitive."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Mock config with session creation enabled
    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    # Test case: Mixed case parameters should be normalized
    # Docker + Python + PSK with various casings
    test_cases = [
        ("Docker", "Python", "PSK"),  # Title case
        ("DOCKER", "PYTHON", "psk"),  # Various cases
        ("docker", "python", "Psk"),  # Lower + title
        ("PIP", None, "anonymous"),  # Pip with anonymous (upper + lower)
        ("Pip", None, "ANONYMOUS"),  # Pip with anonymous (title + upper)
    ]

    for launch_method, prog_lang, auth_type in test_cases:
        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        # This should NOT raise validation errors - parameters should be normalized
        # We expect it to fail later (e.g., Docker not available), but NOT on parameter validation
        result = await session_community_create(
            context,
            session_name=f"test-{launch_method.lower()}",
            launch_method=launch_method,
            programming_language=prog_lang,
            auth_type=auth_type,
        )

        # If it fails on validation (not Docker/pip issues), test fails
        if not result["success"]:
            error = result.get("error", "")
            # These are validation errors we DON'T want to see (means normalization failed)
            assert (
                "'programming_language' parameter only applies to docker" not in error
            ), f"Case normalization failed for {launch_method=}, {prog_lang=}"
            # Other errors (like Docker not available) are OK for this test


@pytest.mark.asyncio
async def test_session_community_create_validates_programming_language_with_python():
    """Test that programming_language parameter raises error with python launch method."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should raise validation error: programming_language only for docker
    result = await session_community_create(
        context,
        session_name="test-invalid",
        launch_method="python",
        programming_language="Python",  # Not valid with python!
    )

    assert result["success"] is False
    assert (
        "'programming_language' parameter only applies to docker launch method"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_validates_docker_image_with_python():
    """Test that docker_image parameter raises error with python launch method."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should raise validation error: docker_image only for docker
    result = await session_community_create(
        context,
        session_name="test-invalid",
        launch_method="python",
        docker_image="ghcr.io/deephaven/server:custom",  # Not valid with python!
    )

    assert result["success"] is False
    assert (
        "'docker_image' parameter only applies to docker launch method"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_validates_docker_memory_limit_with_python():
    """Test that docker_memory_limit_gb parameter raises error with python launch method."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should raise validation error: docker_memory_limit_gb only for docker
    result = await session_community_create(
        context,
        session_name="test-invalid",
        launch_method="python",
        docker_memory_limit_gb=8.0,  # Not valid with python!
    )

    assert result["success"] is False
    assert (
        "'docker_memory_limit_gb' parameter only applies to docker launch method"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_validates_docker_cpu_limit_with_python():
    """Test that docker_cpu_limit parameter raises error with python launch method."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should raise validation error: docker_cpu_limit only for docker
    result = await session_community_create(
        context,
        session_name="test-invalid",
        launch_method="python",
        docker_cpu_limit=2.0,  # Not valid with python!
    )

    assert result["success"] is False
    assert (
        "'docker_cpu_limit' parameter only applies to docker launch method"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_validates_docker_volumes_with_python():
    """Test that docker_volumes parameter raises error with python launch method."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should raise validation error: docker_volumes only for docker
    result = await session_community_create(
        context,
        session_name="test-invalid",
        launch_method="python",
        docker_volumes=["/data:/opt/data:ro"],  # Not valid with python!
    )

    assert result["success"] is False
    assert (
        "'docker_volumes' parameter only applies to docker launch method"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_validates_python_venv_path_with_docker():
    """Test that python_venv_path parameter raises error with docker launch method."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should raise validation error: python_venv_path only for python
    result = await session_community_create(
        context,
        session_name="test-invalid",
        launch_method="docker",
        python_venv_path="/path/to/custom/venv",  # Not valid with docker!
    )

    assert result["success"] is False
    assert (
        "'python_venv_path' parameter only applies to python launch method"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_validates_mutually_exclusive_params():
    """Test that programming_language and docker_image cannot both be specified."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should raise validation error: can't specify both
    result = await session_community_create(
        context,
        session_name="test-invalid",
        launch_method="docker",
        programming_language="Python",
        docker_image="ghcr.io/deephaven/server:custom",
    )

    assert result["success"] is False
    assert (
        "Cannot specify both 'programming_language' and 'docker_image'"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_delete_validates_source():
    """Test that session_community_delete only allows deletion of dynamic sessions."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod
    from deephaven_mcp.resource_manager import SystemType

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Create a mock session manager with source="community" (static session from config)
    mock_static_manager = MagicMock()
    mock_static_manager.full_name = "community:community:local"
    mock_static_manager.system_type = SystemType.COMMUNITY
    mock_static_manager.source = "community"  # NOT "dynamic"

    mock_session_registry.get = AsyncMock(return_value=mock_static_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Attempt to delete static session
    result = await session_community_delete(
        context,
        session_name="local",
    )

    # Verify error - cannot delete static sessions
    assert result["success"] is False
    assert "not a dynamically created session" in result["error"]
    assert "source: 'community'" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_delete_allows_dynamic_sessions():
    """Test that session_community_delete allows deletion of dynamic sessions."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod
    from deephaven_mcp.resource_manager import SystemType

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Create a mock dynamic session manager with source="dynamic"
    mock_dynamic_manager = MagicMock()
    mock_dynamic_manager.full_name = "community:dynamic:test-session"
    mock_dynamic_manager.system_type = SystemType.COMMUNITY
    mock_dynamic_manager.source = "dynamic"  # Correct source
    mock_dynamic_manager.close = AsyncMock()

    mock_session_registry.get = AsyncMock(return_value=mock_dynamic_manager)
    mock_session_registry.get_all = AsyncMock(
        return_value=["community:dynamic:test-session"]
    )
    mock_session_registry.remove_session = AsyncMock(return_value=mock_dynamic_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Delete dynamic session
    result = await session_community_delete(
        context,
        session_name="test-session",
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"] == "community:dynamic:test-session"

    # Verify close and remove_session were called
    mock_dynamic_manager.close.assert_called_once()
    mock_session_registry.remove_session.assert_called_once_with(
        "community:dynamic:test-session"
    )


@pytest.mark.asyncio
async def test_session_community_create_explicit_docker_image():
    """Test coverage for line 3830: explicit docker_image parameter override."""
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
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.generate_auth_token",
            return_value="test_token",
        ),
        patch.object(
            mock_launched_session, "wait_until_ready", new=AsyncMock(return_value=True)
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

        # Use explicit docker_image (power user override)
        result = await session_community_create(
            context,
            session_name="test-session",
            docker_image="ghcr.io/deephaven/custom-server:v1.2.3",
        )

        assert result["success"] is True
        # Verify launch_session was called with custom image
        call_kwargs = mock_launch_session.call_args.kwargs
        assert call_kwargs["docker_image"] == "ghcr.io/deephaven/custom-server:v1.2.3"


@pytest.mark.asyncio
async def test_session_community_create_groovy_programming_language():
    """Test coverage for lines 3836-3837: Groovy programming language parameter."""
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
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.generate_auth_token",
            return_value="test_token",
        ),
        patch.object(
            mock_launched_session, "wait_until_ready", new=AsyncMock(return_value=True)
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

        # Use Groovy programming language
        result = await session_community_create(
            context,
            session_name="test-session",
            programming_language="Groovy",
        )

        assert result["success"] is True
        # Verify launch_session was called with Groovy image (slim variant)
        call_kwargs = mock_launch_session.call_args.kwargs
        assert "slim" in call_kwargs["docker_image"]  # Groovy uses server-slim


@pytest.mark.asyncio
async def test_session_community_create_unsupported_programming_language():
    """Test coverage for lines 3839-3843: unsupported programming language error."""
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

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Use unsupported programming language
    result = await session_community_create(
        context,
        session_name="test-session",
        programming_language="JavaScript",  # Invalid!
    )

    assert result["success"] is False
    assert "Unsupported programming_language" in result["error"]
    assert "JavaScript" in result["error"]
    assert "Python" in result["error"] and "Groovy" in result["error"]


@pytest.mark.asyncio
async def test_session_community_create_groovy_from_config_defaults():
    """Test coverage for lines 3849-3850: Groovy as config default."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "programming_language": "Groovy",  # Set Groovy as default
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
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.generate_auth_token",
            return_value="test_token",
        ),
        patch.object(
            mock_launched_session, "wait_until_ready", new=AsyncMock(return_value=True)
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

        # Don't specify programming_language - should use config default (Groovy)
        result = await session_community_create(
            context,
            session_name="test-session",
        )

        assert result["success"] is True
        # Verify launch_session was called with Groovy image from config
        call_kwargs = mock_launch_session.call_args.kwargs
        assert "slim" in call_kwargs["docker_image"]  # Groovy uses slim image


@pytest.mark.asyncio
async def test_session_community_create_invalid_config_programming_language():
    """Test coverage for lines 3853-3857: invalid programming language in config."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "programming_language": "Ruby",  # Invalid in config!
            },
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("not found")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should fail with invalid config language error
    result = await session_community_create(
        context,
        session_name="test-session",
    )

    assert result["success"] is False
    assert "Invalid programming_language in config" in result["error"]
    assert "Ruby" in result["error"]
    assert "Python" in result["error"] and "Groovy" in result["error"]


@pytest.mark.asyncio
async def test_session_community_create_missing_auth_token_env_var():
    """Test that missing auth_token_env_var returns configuration error."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "launch_method": "docker",
                "auth_type": "PSK",
                "auth_token_env_var": "MISSING_ENV_VAR",  # This env var is not set
            },
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("not found")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should return error when env var is not set
    result = await session_community_create(
        context,
        session_name="test-session",
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "MISSING_ENV_VAR" in result["error"]
    assert "not set" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_disabled_by_default():
    """Test that credential retrieval is disabled by default (mode='none')."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Config without security section (defaults to mode='none')
    config = {
        "community": {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {},
            }
        }
    }

    mock_config_manager.get_config = AsyncMock(return_value=config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:dynamic:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Credential retrieval is disabled" in result["error"]
    assert "mode='none'" in result["error"]
    assert "security" in result["error"]
    assert "credential_retrieval_mode" in result["error"]
    assert "deephaven_mcp.json" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_explicit_none():
    """Test that credential retrieval respects explicit 'none' mode."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Config with explicit mode='none'
    config = {"security": {"community": {"credential_retrieval_mode": "none"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:dynamic:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Credential retrieval is disabled" in result["error"]
    assert "mode='none'" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_dynamic_success():
    """Test successful credential retrieval for dynamic session with mode='dynamic_only'."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Config with mode='dynamic_only'
    config = {"security": {"community": {"credential_retrieval_mode": "dynamic_only"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Create a mock DynamicCommunitySessionManager
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.auth_token = "test_auth_token_123"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?psk=test_auth_token_123"
    )
    mock_launched_session.container_id = "test_container_id"

    manager = DynamicCommunitySessionManager(
        name="test-session",
        config={"host": "localhost", "port": 10000},
        launched_session=mock_launched_session,
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:dynamic:test-session"
    )

    assert result["success"] is True
    assert result["connection_url"] == "http://localhost:10000"
    assert (
        result["connection_url_with_auth"]
        == "http://localhost:10000/?psk=test_auth_token_123"
    )
    assert result["auth_token"] == "test_auth_token_123"
    assert result["auth_type"] == "PSK"
    assert "error" not in result
    assert "isError" not in result


@pytest.mark.asyncio
async def test_session_community_credentials_anonymous_auth():
    """Test credential retrieval with anonymous auth (no token)."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "all"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Create a mock session with anonymous auth (no token)
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.auth_token = None
    mock_launched_session.auth_type = "anonymous"
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = "http://localhost:10000"
    mock_launched_session.container_id = "test_container_id"

    manager = DynamicCommunitySessionManager(
        name="test-session",
        config={"host": "localhost", "port": 10000},
        launched_session=mock_launched_session,
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:dynamic:test-session"
    )

    assert result["success"] is True
    assert result["auth_token"] == ""  # Empty string for None
    assert result["auth_type"] == "ANONYMOUS"


@pytest.mark.asyncio
async def test_session_community_credentials_no_config():
    """Test when community config is empty."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Empty config - should default to disabled
    config = {}
    mock_config_manager.get_config = AsyncMock(return_value=config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:dynamic:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Credential retrieval is disabled" in result["error"]
    assert "mode='none'" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_session_not_found():
    """Test when session does not exist."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "all"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Session not found
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Session not found")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:dynamic:nonexistent"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Session 'community:dynamic:nonexistent' not found" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_not_dynamic_session():
    """Test when session is not a DynamicCommunitySessionManager."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "all"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Return a different type of manager (not DynamicCommunitySessionManager)
    mock_manager = MagicMock()
    mock_manager.__class__.__name__ = "StaticCommunitySessionManager"
    mock_session_registry.get = AsyncMock(return_value=mock_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:dynamic:static-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "not a community session" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_static_session():
    """Test credential retrieval for static community session with mode='static_only'."""
    from deephaven_mcp.resource_manager._manager import StaticCommunitySessionManager

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "static_only"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Create a static session manager
    static_config = {
        "server": "http://localhost:10000",
        "auth_token": "static_token_123",
        "auth_type": "PSK",
    }

    manager = StaticCommunitySessionManager(name="local-dev", config=static_config)

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:config:local-dev"
    )

    assert result["success"] is True
    assert result["connection_url"] == "http://localhost:10000"
    assert (
        result["connection_url_with_auth"]
        == "http://localhost:10000/?psk=static_token_123"
    )
    assert result["auth_token"] == "static_token_123"
    assert result["auth_type"] == "PSK"
    assert "error" not in result
    assert "isError" not in result


@pytest.mark.asyncio
async def test_session_community_credentials_static_session_anonymous():
    """Test credential retrieval for static community session with anonymous auth."""
    from deephaven_mcp.resource_manager._manager import StaticCommunitySessionManager

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "all"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Create a static session manager with anonymous auth (no token)
    static_config = {
        "server": "http://localhost:10000",
        "auth_token": "",  # Empty token for anonymous
        "auth_type": "anonymous",
    }

    manager = StaticCommunitySessionManager(name="local-dev-anon", config=static_config)

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:config:local-dev-anon"
    )

    assert result["success"] is True
    assert result["connection_url"] == "http://localhost:10000"
    assert (
        result["connection_url_with_auth"] == "http://localhost:10000"
    )  # No auth query param
    assert result["auth_token"] == ""  # Empty string
    assert result["auth_type"] == "ANONYMOUS"
    assert "error" not in result
    assert "isError" not in result


@pytest.mark.asyncio
async def test_session_community_credentials_invalid_session_id():
    """Test when session_id has invalid format."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "all"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="enterprise:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Invalid session_id" in result["error"]
    assert "community:dynamic:" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_exception_handling():
    """Test exception handling in session_community_credentials."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Make get_config raise an exception
    mock_config_manager.get_config = AsyncMock(
        side_effect=RuntimeError("Unexpected config error")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:dynamic:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Unexpected config error" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_dynamic_only_denies_static():
    """Test that mode='dynamic_only' denies static session credentials."""
    from deephaven_mcp.resource_manager._manager import StaticCommunitySessionManager

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "dynamic_only"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Create a static session manager
    static_config = {
        "server": "http://localhost:10000",
        "auth_token": "static_token_123",
        "auth_type": "PSK",
    }

    manager = StaticCommunitySessionManager(name="local-dev", config=static_config)

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:config:local-dev"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "static sessions is disabled" in result["error"]
    assert "dynamic_only" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_static_only_denies_dynamic():
    """Test that mode='static_only' denies dynamic session credentials."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "static_only"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Create a mock DynamicCommunitySessionManager
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.auth_token = "test_auth_token_123"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?psk=test_auth_token_123"
    )
    mock_launched_session.container_id = "test_container_id"

    manager = DynamicCommunitySessionManager(
        name="test-session",
        config={"host": "localhost", "port": 10000},
        launched_session=mock_launched_session,
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:dynamic:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "dynamic sessions is disabled" in result["error"]
    assert "static_only" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_all_allows_both():
    """Test that mode='all' allows both dynamic and static session credentials."""
    from deephaven_mcp.resource_manager._manager import StaticCommunitySessionManager

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "all"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Test with static session
    static_config = {
        "server": "http://localhost:10000",
        "auth_token": "static_token_123",
        "auth_type": "PSK",
    }

    static_manager = StaticCommunitySessionManager(
        name="local-dev", config=static_config
    )

    mock_session_registry.get = AsyncMock(return_value=static_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:config:local-dev"
    )

    assert result["success"] is True
    assert result["auth_token"] == "static_token_123"


def test_normalize_auth_type_psk_uppercase():
    """Test PSK shorthand normalization - uppercase."""
    result, error = _normalize_auth_type("PSK")
    assert error is None
    assert result == "io.deephaven.authentication.psk.PskAuthenticationHandler"


def test_normalize_auth_type_psk_lowercase():
    """Test PSK shorthand normalization - lowercase."""
    result, error = _normalize_auth_type("psk")
    assert error is None
    assert result == "io.deephaven.authentication.psk.PskAuthenticationHandler"


def test_normalize_auth_type_psk_mixedcase():
    """Test PSK shorthand normalization - mixed case."""
    result, error = _normalize_auth_type("Psk")
    assert error is None
    assert result == "io.deephaven.authentication.psk.PskAuthenticationHandler"


def test_normalize_auth_type_anonymous_uppercase():
    """Test Anonymous shorthand normalization - uppercase."""
    result, error = _normalize_auth_type("ANONYMOUS")
    assert error is None
    assert result == "Anonymous"


def test_normalize_auth_type_anonymous_lowercase():
    """Test Anonymous shorthand normalization - lowercase."""
    result, error = _normalize_auth_type("anonymous")
    assert error is None
    assert result == "Anonymous"


def test_normalize_auth_type_anonymous_proper_case():
    """Test Anonymous shorthand normalization - proper case."""
    result, error = _normalize_auth_type("Anonymous")
    assert error is None
    assert result == "Anonymous"


def test_normalize_auth_type_basic_rejected():
    """Test that Basic auth is rejected for dynamic sessions."""
    result, error = _normalize_auth_type("Basic")
    assert error is not None
    assert "Basic authentication is not supported for dynamic sessions" in error
    assert "requires database setup" in error


def test_normalize_auth_type_basic_lowercase_rejected():
    """Test that Basic auth (lowercase) is rejected."""
    result, error = _normalize_auth_type("basic")
    assert error is not None
    assert "Basic authentication is not supported" in error


def test_normalize_auth_type_basic_uppercase_rejected():
    """Test that Basic auth (uppercase) is rejected."""
    result, error = _normalize_auth_type("BASIC")
    assert error is not None
    assert "Basic authentication is not supported" in error


def test_normalize_auth_type_psk_handler_wrong_case_rejected():
    """Test that the Deephaven PSK handler with incorrect case is rejected."""
    result, error = _normalize_auth_type(
        "IO.DEEPHAVEN.AUTHENTICATION.PSK.PSKAUTHENTICATIONHANDLER"
    )
    assert error is not None
    assert "Deephaven PSK handler with incorrect case" in error
    assert "io.deephaven.authentication.psk.PskAuthenticationHandler" in error


def test_normalize_auth_type_whitespace_rejected():
    """Test that auth_type with whitespace is rejected."""
    result, error = _normalize_auth_type(" PSK")
    assert error is not None
    assert "whitespace" in error


def test_normalize_auth_type_trailing_whitespace_rejected():
    """Test that auth_type with trailing whitespace is rejected."""
    result, error = _normalize_auth_type("PSK ")
    assert error is not None
    assert "whitespace" in error


def test_normalize_auth_type_full_class_name_preserved():
    """Test that correct full class name is preserved."""
    result, error = _normalize_auth_type(
        "io.deephaven.authentication.psk.PskAuthenticationHandler"
    )
    assert error is None
    assert result == "io.deephaven.authentication.psk.PskAuthenticationHandler"


def test_normalize_auth_type_custom_authenticator_preserved():
    """Test that custom authenticator class names are preserved."""
    result, error = _normalize_auth_type("com.example.CustomAuthenticator")
    assert error is None
    assert result == "com.example.CustomAuthenticator"


def test_normalize_auth_type_no_dots_preserved():
    """Test that values without dots (non-class names) are preserved."""
    result, error = _normalize_auth_type("CustomAuth")
    assert error is None
    assert result == "CustomAuth"


def test_normalize_auth_type_anonymous_mixedcase():
    """Test Anonymous shorthand normalization - various mixed cases."""
    result, error = _normalize_auth_type("AnOnYmOuS")
    assert error is None
    assert result == "Anonymous"


def test_normalize_auth_type_uppercase_custom_authenticator_allowed():
    """Test that custom authenticators with uppercase names are allowed."""
    result, error = _normalize_auth_type("COM.MYCOMPANY.CUSTOMAUTH")
    assert error is None
    assert result == "COM.MYCOMPANY.CUSTOMAUTH"


def test_resolve_community_session_parameters_invalid_auth_type():
    """Test _resolve_community_session_parameters with invalid auth_type returns error."""
    # Call with invalid auth_type (Basic is not supported for dynamic sessions)
    resolved_params, error = _resolve_community_session_parameters(
        launch_method=None,
        programming_language=None,
        auth_type="Basic",  # This should trigger validation error
        auth_token=None,
        heap_size_gb=None,
        extra_jvm_args=None,
        environment_vars=None,
        docker_image=None,
        docker_memory_limit_gb=None,
        docker_cpu_limit=None,
        docker_volumes=None,
        python_venv_path=None,
        defaults={},
    )

    # Should return empty dict and error dict
    assert resolved_params == {}
    assert error is not None
    assert error["success"] is False
    assert error["isError"] is True
    assert "Invalid auth_type" in error["error"]
    assert "Basic authentication is not supported" in error["error"]


@pytest.mark.asyncio
async def test_session_community_create_groovy_session_type_in_config():
    """Regression test: Verify programming_language='Groovy' results in session_type='groovy'.

    This test ensures programming_language is properly passed through to the
    session configuration. Previously, the parameter was used for Docker image
    selection but not included in the session config, causing all sessions to
    default to Python.
    """
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
    mock_launched_session.container_id = "test_container"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "test_token"

    # Capture the session_config passed to DynamicCommunitySessionManager
    captured_config = None

    def capture_manager_init(name, config, launched_session):
        nonlocal captured_config
        captured_config = config
        manager = MagicMock()
        manager.full_name = f"community:dynamic:{name}"
        return manager

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
            return_value="test_token",
        ),
        patch.object(
            mock_launched_session, "wait_until_ready", new=AsyncMock(return_value=True)
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.DynamicCommunitySessionManager",
            side_effect=capture_manager_init,
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

        # Create session with Groovy programming language
        result = await session_community_create(
            context,
            session_name="groovy-session",
            programming_language="Groovy",
        )

        assert result["success"] is True

        # CRITICAL: Verify session_config includes session_type='groovy'
        assert captured_config is not None, "Session config was not captured"
        assert (
            "session_type" in captured_config
        ), "session_type missing from session config"
        assert (
            captured_config["session_type"] == "groovy"
        ), f"Expected session_type='groovy', got '{captured_config['session_type']}'"

        # Also verify the Docker image is correct
        call_kwargs = mock_launch_session.call_args.kwargs
        assert (
            "slim" in call_kwargs["docker_image"]
        ), "Groovy should use slim Docker image"


@pytest.mark.asyncio
async def test_session_community_create_python_session_type_in_config():
    """Regression test: Verify programming_language='Python' results in session_type='python'."""
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
    mock_launched_session.container_id = "test_container"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "test_token"

    captured_config = None

    def capture_manager_init(name, config, launched_session):
        nonlocal captured_config
        captured_config = config
        manager = MagicMock()
        manager.full_name = f"community:dynamic:{name}"
        return manager

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
            return_value="test_token",
        ),
        patch.object(
            mock_launched_session, "wait_until_ready", new=AsyncMock(return_value=True)
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.DynamicCommunitySessionManager",
            side_effect=capture_manager_init,
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

        # Create session with Python programming language (explicit)
        result = await session_community_create(
            context,
            session_name="python-session",
            programming_language="Python",
        )

        assert result["success"] is True
        assert captured_config is not None
        assert "session_type" in captured_config
        assert captured_config["session_type"] == "python"


@pytest.mark.asyncio
async def test_session_community_create_default_session_type_in_config():
    """Regression test: Verify omitting programming_language defaults to Python."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},  # No default programming_language
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
    mock_launched_session.container_id = "test_container"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "test_token"

    captured_config = None

    def capture_manager_init(name, config, launched_session):
        nonlocal captured_config
        captured_config = config
        manager = MagicMock()
        manager.full_name = f"community:dynamic:{name}"
        return manager

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
            return_value="test_token",
        ),
        patch.object(
            mock_launched_session, "wait_until_ready", new=AsyncMock(return_value=True)
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._tools.session_community.DynamicCommunitySessionManager",
            side_effect=capture_manager_init,
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

        # Create session WITHOUT specifying programming_language
        result = await session_community_create(
            context,
            session_name="default-session",
        )

        assert result["success"] is True
        assert captured_config is not None
        assert "session_type" in captured_config
        # Should default to Python
        assert captured_config["session_type"] == "python"
