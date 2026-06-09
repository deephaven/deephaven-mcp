"""
Tests for deephaven_mcp.mcp_systems_server._tools.session_community.
"""

import asyncio
import os
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from conftest import (
    MockContext,
    create_mock_instance_tracker,
    stub_session_config,
)

from deephaven_mcp._exceptions import RegistryItemNotFoundError
from deephaven_mcp.client import CommunityClientTimeouts
from deephaven_mcp.config.schema import CommunitySessionCreationDefaults
from deephaven_mcp.mcp_systems_server._tools.session_community import (
    _ANONYMOUS_AUTH_HANDLER,
    _PSK_AUTH_HANDLER,
    _credentials_to_auth_type,
    _register_session_manager,
    _resolve_community_session_parameters,
    session_community_create,
    session_community_credentials,
    session_community_delete,
)
from deephaven_mcp.resource_manager import (
    CommunitySessionRegistry,
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
    PythonLaunchedSession,
    ResourceLivenessStatus,
    SessionOrigin,
    StaticCommunitySessionManager,
    SystemType,
)


@pytest.mark.asyncio
async def test_session_community_create_success():
    """Test successful community session creation."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    # Mock config
    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "launch_method": "docker",
                "heap_size_gb": 4.0,
            },
        }
    }

    full_config = community_config
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    # Stash for conftest's lifespan adapter — exposes the community
    # settings dict to the multiplexed-server tool helpers without
    # rewriting every test's MockContext call site.
    mock_session_registry._community_settings = community_config
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.add_dynamic_session = AsyncMock()
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
                "registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await session_community_create(
            context,
            session_name="test-session",
        )

        # Verify success
        assert result["success"] is True
        assert result["session_id"] == "community:community:test-session"
        assert result["session_name"] == "test-session"
        assert result["port"] == 10000
        assert "connection_url" in result

        # Verify session was added to registry via the dynamic-creation path
        mock_session_registry.add_dynamic_session.assert_called_once()


@pytest.mark.asyncio
async def test_session_community_create_not_configured():
    """Test community session creation when not configured."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    # No session_creation config
    full_config = {}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
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
    """``max_concurrent_sessions: null`` disables the cap (unbounded).

    The previous ``0``-as-disabled sentinel is gone (the schema now
    requires ``int >= 1`` or ``None``); this test pins the new
    semantics: explicit ``None`` skips the count check entirely.
    """
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": None,  # Disabled (unbounded)
            "defaults": {},
        }
    }

    full_config = community_config
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    # Stash for conftest's lifespan adapter — exposes the community
    # settings dict to the multiplexed-server tool helpers without
    # rewriting every test's MockContext call site.
    mock_session_registry._community_settings = community_config
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.add_dynamic_session = AsyncMock()
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
                "registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await session_community_create(
            context,
            session_name="test-session",
        )

        # Should succeed - limit is disabled so no limit check
        assert result["success"] is True
        assert result["session_id"] == "community:community:test-session"
        # count_added_sessions should NOT have been called since limit is disabled
        mock_session_registry.count_added_sessions.assert_not_called()


@pytest.mark.asyncio
async def test_session_community_create_max_sessions_reached():
    """Test community session creation when max sessions reached."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 2,
            "defaults": {},
        }
    }

    full_config = community_config
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    # Stash for conftest's lifespan adapter — exposes the community
    # settings dict to the multiplexed-server tool helpers without
    # rewriting every test's MockContext call site.
    mock_session_registry._community_settings = community_config
    mock_session_registry.count_added_sessions = AsyncMock(return_value=2)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
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
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},
        }
    }

    full_config = community_config
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    # Stash for conftest's lifespan adapter — exposes the community
    # settings dict to the multiplexed-server tool helpers without
    # rewriting every test's MockContext call site.
    mock_session_registry._community_settings = community_config
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
                "registry": mock_session_registry,
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
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    # Create a mock launched session (Docker by default)
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.launch_method = "docker"

    # Create a mock dynamic session manager
    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.full_name = "community:community:test-session"
    mock_manager._name = "test-session"
    mock_manager.system = "community"
    mock_manager.origin = SessionOrigin.DYNAMIC
    mock_manager.system_type = SystemType.COMMUNITY
    mock_manager.launched_session = mock_launched_session
    mock_manager.close = AsyncMock()

    mock_session_registry.get = AsyncMock(return_value=mock_manager)
    mock_session_registry.remove = AsyncMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(
        context,
        session_id="community:community:test-session",
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"] == "community:community:test-session"
    assert result["session_name"] == "test-session"

    # Verify session was closed and removed
    mock_manager.close.assert_called_once()
    mock_session_registry.remove.assert_called_once()


@pytest.mark.asyncio
async def test_session_community_delete_python_session():
    """Test deleting a python-launched session to cover untrack_python_process call."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)
    mock_instance_tracker = create_mock_instance_tracker()

    # Create a mock python-launched session
    mock_launched_session = MagicMock(spec=PythonLaunchedSession)
    mock_launched_session.launch_method = "python"

    # Create a mock python-launched session manager
    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.full_name = "community:community:python-session"
    mock_manager._name = "python-session"
    mock_manager.system = "community"
    mock_manager.origin = SessionOrigin.DYNAMIC
    mock_manager.system_type = SystemType.COMMUNITY
    mock_manager.launched_session = mock_launched_session
    mock_manager.close = AsyncMock()

    mock_session_registry.get = AsyncMock(return_value=mock_manager)
    mock_session_registry.remove = AsyncMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
            "instance_tracker": mock_instance_tracker,
        }
    )

    result = await session_community_delete(
        context,
        session_id="community:community:python-session",
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"] == "community:community:python-session"

    # Verify untrack_python_process was called (line 4197)
    mock_instance_tracker.untrack_python_process.assert_called_once_with(
        "python-session"
    )

    # Verify session was closed and removed
    mock_manager.close.assert_called_once()
    mock_session_registry.remove.assert_called_once()


@pytest.mark.asyncio
async def test_session_community_delete_not_found():
    """Test community session deletion when session not found."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Not found")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(
        context,
        session_id="community:community:nonexistent",
    )

    # Verify error
    assert result["success"] is False
    assert "not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_delete_not_dynamic():
    """Static community sessions cannot be deleted; origin check rejects them."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    # The registry returns a static-origin manager; the origin check rejects deletion.
    # ``__class__`` is patched so the production ``isinstance`` narrowing
    # against :class:`CommunitySessionManager` succeeds.
    mock_manager = MagicMock()
    mock_manager.__class__ = StaticCommunitySessionManager
    mock_manager.full_name = "community:community:test-session"
    mock_manager.system_type = SystemType.COMMUNITY
    mock_manager.system = "community"
    mock_manager.origin = SessionOrigin.STATIC
    mock_session_registry.get = AsyncMock(return_value=mock_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(
        context,
        session_id="community:community:test-session",
    )

    assert result["success"] is False
    assert "Only dynamically created sessions" in result["error"]
    assert "static" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_case_insensitive_params():
    """Test that launch_method, programming_language, and auth_type are case-insensitive."""
    pass  # no longer needed

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    # Mock config with session creation enabled
    mock_config_manager.get_config = AsyncMock(
        return_value={
            "session_creation": {
                "defaults": {},
                "max_concurrent_sessions": 5,
            }
        }
    )
    # Stash for conftest's lifespan adapter — same shape as the
    # ``get_config`` return value so the tool resolves
    # ``session_creation`` correctly.
    mock_session_registry._community_settings = {
        "session_creation": {"defaults": {}, "max_concurrent_sessions": 5}
    }

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    # Test case: Mixed case parameters should be normalized
    # Docker + Python + PSK with various casings
    test_cases = [
        ("Docker", "Python"),  # Title case
        ("DOCKER", "PYTHON"),  # Various cases
        ("docker", "python"),  # Lower + title
        ("PIP", None),  # Pip with anonymous (upper)
        ("Pip", None),  # Pip with anonymous (title)
    ]

    for launch_method, prog_lang in test_cases:
        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "registry": mock_session_registry,
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
    pass  # no longer needed

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "session_creation": {
                "defaults": {},
                "max_concurrent_sessions": 5,
            }
        }
    )
    # Stash for conftest's lifespan adapter — same shape as the
    # ``get_config`` return value so the tool resolves
    # ``session_creation`` correctly.
    mock_session_registry._community_settings = {
        "session_creation": {"defaults": {}, "max_concurrent_sessions": 5}
    }

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
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
    pass  # no longer needed

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "session_creation": {
                "defaults": {},
                "max_concurrent_sessions": 5,
            }
        }
    )
    # Stash for conftest's lifespan adapter — same shape as the
    # ``get_config`` return value so the tool resolves
    # ``session_creation`` correctly.
    mock_session_registry._community_settings = {
        "session_creation": {"defaults": {}, "max_concurrent_sessions": 5}
    }

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
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
    pass  # no longer needed

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "session_creation": {
                "defaults": {},
                "max_concurrent_sessions": 5,
            }
        }
    )
    # Stash for conftest's lifespan adapter — same shape as the
    # ``get_config`` return value so the tool resolves
    # ``session_creation`` correctly.
    mock_session_registry._community_settings = {
        "session_creation": {"defaults": {}, "max_concurrent_sessions": 5}
    }

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
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
    pass  # no longer needed

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "session_creation": {
                "defaults": {},
                "max_concurrent_sessions": 5,
            }
        }
    )
    # Stash for conftest's lifespan adapter — same shape as the
    # ``get_config`` return value so the tool resolves
    # ``session_creation`` correctly.
    mock_session_registry._community_settings = {
        "session_creation": {"defaults": {}, "max_concurrent_sessions": 5}
    }

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
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
    pass  # no longer needed

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "session_creation": {
                "defaults": {},
                "max_concurrent_sessions": 5,
            }
        }
    )
    # Stash for conftest's lifespan adapter — same shape as the
    # ``get_config`` return value so the tool resolves
    # ``session_creation`` correctly.
    mock_session_registry._community_settings = {
        "session_creation": {"defaults": {}, "max_concurrent_sessions": 5}
    }

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
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
    pass  # no longer needed

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "session_creation": {
                "defaults": {},
                "max_concurrent_sessions": 5,
            }
        }
    )
    # Stash for conftest's lifespan adapter — same shape as the
    # ``get_config`` return value so the tool resolves
    # ``session_creation`` correctly.
    mock_session_registry._community_settings = {
        "session_creation": {"defaults": {}, "max_concurrent_sessions": 5}
    }

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
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
    pass  # no longer needed

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "session_creation": {
                "defaults": {},
                "max_concurrent_sessions": 5,
            }
        }
    )
    # Stash for conftest's lifespan adapter — same shape as the
    # ``get_config`` return value so the tool resolves
    # ``session_creation`` correctly.
    mock_session_registry._community_settings = {
        "session_creation": {"defaults": {}, "max_concurrent_sessions": 5}
    }

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
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
async def test_session_community_delete_validates_origin():
    """session_community_delete rejects sessions whose origin is not DYNAMIC."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    # Static-origin manager — deletion must be rejected. ``__class__`` is
    # patched so the production ``isinstance`` narrowing succeeds.
    mock_manager = MagicMock()
    mock_manager.__class__ = StaticCommunitySessionManager
    mock_manager.full_name = "community:community:local"
    mock_manager.system_type = SystemType.COMMUNITY
    mock_manager.system = "community"
    mock_manager.origin = SessionOrigin.STATIC
    mock_session_registry.get = AsyncMock(return_value=mock_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(
        context,
        session_id="community:community:local",
    )

    assert result["success"] is False
    assert "not a dynamically created session" in result["error"]
    assert "origin: 'static'" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_delete_allows_dynamic_sessions():
    """session_community_delete succeeds for a valid dynamic session_id."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    # Create a mock dynamic session manager with source="dynamic".
    # ``__class__`` is patched so the production ``isinstance`` narrowing
    # against :class:`CommunitySessionManager` succeeds.
    mock_dynamic_manager = MagicMock()
    mock_dynamic_manager.__class__ = DynamicCommunitySessionManager
    mock_dynamic_manager.full_name = "community:community:test-session"
    mock_dynamic_manager.system_type = SystemType.COMMUNITY
    mock_dynamic_manager.system = "community"  # community umbrella
    mock_dynamic_manager.origin = SessionOrigin.DYNAMIC
    mock_dynamic_manager.close = AsyncMock()

    mock_session_registry.get = AsyncMock(return_value=mock_dynamic_manager)
    mock_session_registry.remove = AsyncMock(return_value=mock_dynamic_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Delete dynamic session using full session_id
    result = await session_community_delete(
        context,
        session_id="community:community:test-session",
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"] == "community:community:test-session"

    # Verify close and remove were called
    mock_dynamic_manager.close.assert_called_once()
    mock_session_registry.remove.assert_called_once_with(
        "community:community:test-session"
    )


@pytest.mark.asyncio
async def test_session_community_delete_close_failure_continues():
    """session_community_delete logs warning and continues when close() raises."""
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.full_name = "community:community:close-fail"
    mock_manager.system = "community"
    mock_manager.origin = SessionOrigin.DYNAMIC
    mock_manager.system_type = SystemType.COMMUNITY
    mock_manager.launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_manager.close = AsyncMock(side_effect=RuntimeError("close error"))

    mock_session_registry.get = AsyncMock(return_value=mock_manager)
    mock_session_registry.remove = AsyncMock(return_value=mock_manager)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(
        context, session_id="community:community:close-fail"
    )

    # Should succeed despite close failure
    assert result["success"] is True
    assert result["session_id"] == "community:community:close-fail"
    mock_session_registry.remove.assert_called_once()


@pytest.mark.asyncio
async def test_session_community_delete_removal_missing_in_registry():
    """session_community_delete logs warning when remove returns None."""
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.full_name = "community:community:ghost"
    mock_manager.system = "community"
    mock_manager.origin = SessionOrigin.DYNAMIC
    mock_manager.system_type = SystemType.COMMUNITY
    mock_manager.launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_manager.close = AsyncMock()

    mock_session_registry.get = AsyncMock(return_value=mock_manager)
    mock_session_registry.remove = AsyncMock(return_value=None)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(
        context, session_id="community:community:ghost"
    )

    assert result["success"] is True


@pytest.mark.asyncio
async def test_session_community_delete_registry_remove_raises():
    """session_community_delete returns error when remove raises."""
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.full_name = "community:community:boom"
    mock_manager.system = "community"
    mock_manager.origin = SessionOrigin.DYNAMIC
    mock_manager.system_type = SystemType.COMMUNITY
    mock_manager.launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_manager.close = AsyncMock()

    mock_session_registry.get = AsyncMock(return_value=mock_manager)
    mock_session_registry.remove = AsyncMock(
        side_effect=Exception("Simulated registry error")
    )

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(
        context, session_id="community:community:boom"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Simulated registry error" in result["error"]


@pytest.mark.asyncio
async def test_session_community_delete_outer_exception():
    """session_community_delete outer except handler fires when unexpected error occurs."""
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)
    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.full_name = "community:community:unexpected"
    mock_manager.system = "community"
    mock_manager.origin = SessionOrigin.DYNAMIC
    mock_manager.system_type = SystemType.COMMUNITY
    mock_manager.launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_manager.close = AsyncMock()
    mock_session_registry.get = AsyncMock(return_value=mock_manager)
    mock_session_registry.remove = AsyncMock(return_value=mock_manager)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    call_count = {"n": 0}

    def info_side_effect(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise Exception("unexpected log error")
        return None

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.session_community._LOGGER.info",
        side_effect=info_side_effect,
    ):
        result = await session_community_delete(
            context, session_id="community:community:unexpected"
        )

    assert result["success"] is False
    assert result["isError"] is True
    assert "unexpected log error" in result["error"]


@pytest.mark.asyncio
async def test_session_community_delete_invalid_session_id_format():
    """session_community_delete returns error for malformed session_id."""
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(context, session_id="not-a-valid-id")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Invalid session_id format" in result["error"]
    mock_session_registry.get.assert_not_called()


@pytest.mark.asyncio
async def test_session_community_delete_wrong_system_type():
    """session_community_delete returns error when session_id is not community type."""
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(context, session_id="enterprise:prod:s1")

    assert result["success"] is False
    assert result["isError"] is True
    assert "is not a community session" in result["error"]
    mock_session_registry.get.assert_not_called()


@pytest.mark.asyncio
async def test_session_community_create_explicit_docker_image():
    """Test coverage for line 3830: explicit docker_image parameter override."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},
        }
    }

    full_config = community_config
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    # Stash for conftest's lifespan adapter — exposes the community
    # settings dict to the multiplexed-server tool helpers without
    # rewriting every test's MockContext call site.
    mock_session_registry._community_settings = community_config
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.add_dynamic_session = AsyncMock()
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
                "registry": mock_session_registry,
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
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},
        }
    }

    full_config = community_config
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    # Stash for conftest's lifespan adapter — exposes the community
    # settings dict to the multiplexed-server tool helpers without
    # rewriting every test's MockContext call site.
    mock_session_registry._community_settings = community_config
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.add_dynamic_session = AsyncMock()
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
                "registry": mock_session_registry,
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
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},
        }
    }

    full_config = community_config
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    # Stash for conftest's lifespan adapter — exposes the community
    # settings dict to the multiplexed-server tool helpers without
    # rewriting every test's MockContext call site.
    mock_session_registry._community_settings = community_config
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("not found")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
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
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "programming_language": "Groovy",  # Set Groovy as default
            },
        }
    }

    full_config = community_config
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    # Stash for conftest's lifespan adapter — exposes the community
    # settings dict to the multiplexed-server tool helpers without
    # rewriting every test's MockContext call site.
    mock_session_registry._community_settings = community_config
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.add_dynamic_session = AsyncMock()
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
                "registry": mock_session_registry,
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


def test_session_creation_defaults_rejects_invalid_programming_language():
    """Invalid ``programming_language`` is now rejected at config-load time.

    Previously the tool surfaced ``Invalid programming_language in config``
    when the value reached the runtime resolver. Pydantic now validates
    the field as ``Literal["Python", "Groovy"]`` so the bad value never
    leaves the schema layer.
    """
    from pydantic import ValidationError

    from deephaven_mcp.config.schema import CommunitySettings

    with pytest.raises(ValidationError, match="programming_language"):
        CommunitySettings.model_validate(
            {
                "session_creation": {
                    "max_concurrent_sessions": 5,
                    "defaults": {"programming_language": "Ruby"},
                }
            }
        )


# Note: ``test_session_community_create_missing_auth_token_env_var`` was
# removed. The legacy code resolved ``token_env_var`` lazily at session-
# creation time and surfaced ``unset or empty`` errors through this tool.
# After the Pydantic migration, env-var resolution happens eagerly at
# config-load time in :class:`PSKCredentials._resolve`, so a missing
# env var fails the loader before any tool runs. That code path is now
# covered by ``tests/auth/credentials/test__credentials.py``.


@pytest.mark.asyncio
async def test_session_community_credentials_disabled_by_default():
    """Test that credential retrieval is disabled by default (mode='none')."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    # Config without security section (defaults to mode='none')
    config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},
        }
    }

    mock_config_manager.get_config = AsyncMock(return_value=config)
    # Stash for conftest's lifespan adapter (security tests use ``config``
    # rather than ``community_config`` to hold the settings dict).
    mock_session_registry._community_settings = config

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:community:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Credential retrieval is disabled" in result["error"]
    assert "mode='none'" in result["error"]
    assert "security" in result["error"]
    assert "credential_retrieval_mode" in result["error"]
    assert "community/settings.json" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_explicit_none():
    """Test that credential retrieval respects explicit 'none' mode."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    # Config with explicit mode='none'
    config = {"security": {"credential_retrieval_mode": "none"}}

    mock_config_manager.get_config = AsyncMock(return_value=config)
    # Stash for conftest's lifespan adapter (security tests use ``config``
    # rather than ``community_config`` to hold the settings dict).
    mock_session_registry._community_settings = config

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:community:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Credential retrieval is disabled" in result["error"]
    assert "mode='none'" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_dynamic_success():
    """Test successful credential retrieval for dynamic session with mode='dynamic_only'."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    # Config with mode='dynamic_only'
    config = {"security": {"credential_retrieval_mode": "dynamic_only"}}

    mock_config_manager.get_config = AsyncMock(return_value=config)
    # Stash for conftest's lifespan adapter (security tests use ``config``
    # rather than ``community_config`` to hold the settings dict).
    mock_session_registry._community_settings = config

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
        session_config=stub_session_config(),
        launched_session=mock_launched_session,
        timeouts=CommunityClientTimeouts(),
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:community:test-session"
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
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    config = {"security": {"credential_retrieval_mode": "all"}}

    mock_config_manager.get_config = AsyncMock(return_value=config)
    # Stash for conftest's lifespan adapter (security tests use ``config``
    # rather than ``community_config`` to hold the settings dict).
    mock_session_registry._community_settings = config

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
        session_config=stub_session_config(),
        launched_session=mock_launched_session,
        timeouts=CommunityClientTimeouts(),
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:community:test-session"
    )

    assert result["success"] is True
    assert result["auth_token"] == ""  # Empty string for None
    assert result["auth_type"] == "ANONYMOUS"


@pytest.mark.asyncio
async def test_session_community_credentials_no_config():
    """Test when community config is empty."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    # Empty config - should default to disabled
    config = {}
    mock_config_manager.get_config = AsyncMock(return_value=config)
    # Stash for conftest's lifespan adapter (security tests use ``config``
    # rather than ``community_config`` to hold the settings dict).
    mock_session_registry._community_settings = config

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:community:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Credential retrieval is disabled" in result["error"]
    assert "mode='none'" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_session_not_found():
    """Test when session does not exist."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    config = {"security": {"credential_retrieval_mode": "all"}}

    mock_config_manager.get_config = AsyncMock(return_value=config)
    # Stash for conftest's lifespan adapter (security tests use ``config``
    # rather than ``community_config`` to hold the settings dict).
    mock_session_registry._community_settings = config

    # Session not found
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("Session not found")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:community:nonexistent"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Session 'community:community:nonexistent' not found" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_not_dynamic_session():
    """Test when session is not a DynamicCommunitySessionManager."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    config = {"security": {"credential_retrieval_mode": "all"}}

    mock_config_manager.get_config = AsyncMock(return_value=config)
    # Stash for conftest's lifespan adapter (security tests use ``config``
    # rather than ``community_config`` to hold the settings dict).
    mock_session_registry._community_settings = config

    # Return a different type of manager (not DynamicCommunitySessionManager)
    mock_manager = MagicMock()
    mock_manager.__class__.__name__ = "StaticCommunitySessionManager"
    mock_session_registry.get = AsyncMock(return_value=mock_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:community:static-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "not a community session" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_static_session():
    """Test credential retrieval for static community session with mode='static_only'."""
    from deephaven_mcp.resource_manager._manager import StaticCommunitySessionManager

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    config = {"security": {"credential_retrieval_mode": "static_only"}}

    mock_config_manager.get_config = AsyncMock(return_value=config)
    # Stash for conftest's lifespan adapter (security tests use ``config``
    # rather than ``community_config`` to hold the settings dict).
    mock_session_registry._community_settings = config

    # Build a typed static session declaration with PSK auth so the
    # tool reads ``host`` / ``port`` / ``credentials`` directly.
    session_config = stub_session_config(
        name="local-dev",
        host="localhost",
        port=10000,
        auth={"credentials": {"type": "psk", "token": "static_token_123"}},
    )

    manager = StaticCommunitySessionManager(
        name="local-dev",
        session_config=session_config,
        timeouts=CommunityClientTimeouts(),
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:community:local-dev"
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
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    config = {"security": {"credential_retrieval_mode": "all"}}

    mock_config_manager.get_config = AsyncMock(return_value=config)
    # Stash for conftest's lifespan adapter (security tests use ``config``
    # rather than ``community_config`` to hold the settings dict).
    mock_session_registry._community_settings = config

    # Typed static session declaration with anonymous auth.
    session_config = stub_session_config(
        name="local-dev-anon",
        host="localhost",
        port=10000,
    )

    manager = StaticCommunitySessionManager(
        name="local-dev-anon",
        session_config=session_config,
        timeouts=CommunityClientTimeouts(),
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:community:local-dev-anon"
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
async def test_register_session_manager_custom_auth_handler():
    """An auth-type that is neither PSK nor Anonymous routes to a ``custom`` block.

    ``_normalize_auth_type`` does not produce custom handlers today, but
    ``_register_session_manager`` keeps a forward-compatible branch. The
    branch is exercised directly by passing an arbitrary class-name
    string and capturing the typed declaration handed to
    :meth:`CommunitySessionRegistry.add_dynamic_session`.
    """
    mock_launched = MagicMock(spec=DockerLaunchedSession)
    mock_launched.process = MagicMock()
    mock_launched.process.pid = 123

    mock_registry = MagicMock(spec=CommunitySessionRegistry)
    mock_registry.add_dynamic_session = AsyncMock()

    await _register_session_manager(
        session_name="custom-session",
        session_id="community:community:custom-session",
        port=10000,
        programming_language="Python",
        resolved_auth_type="com.example.CustomAuthHandler",
        resolved_auth_token="opaque",
        launched_session=mock_launched,
        session_registry=mock_registry,
        instance_tracker=create_mock_instance_tracker(),
    )

    mock_registry.add_dynamic_session.assert_awaited_once()
    kwargs = mock_registry.add_dynamic_session.call_args.kwargs
    cfg = kwargs["session_config"]

    # Custom auth surfaces as a ``CustomTokenCredentials`` (the
    # declaration's ``custom`` credentials type) with the opaque token.
    from deephaven_mcp.auth.credentials import CustomTokenCredentials

    assert isinstance(cfg.credentials, CustomTokenCredentials)
    assert cfg.credentials.auth_token.get_secret_value() == "opaque"
    assert cfg.credentials.auth_type == "com.example.CustomAuthHandler"


@pytest.mark.asyncio
async def test_register_session_manager_anonymous_auth():
    """``resolved_auth_type='Anonymous'`` produces an anonymous credentials block."""
    mock_launched = MagicMock(spec=DockerLaunchedSession)
    mock_registry = MagicMock(spec=CommunitySessionRegistry)
    mock_registry.add_dynamic_session = AsyncMock()

    await _register_session_manager(
        session_name="anon-session",
        session_id="community:community:anon-session",
        port=10000,
        programming_language="Python",
        resolved_auth_type="Anonymous",
        resolved_auth_token=None,
        launched_session=mock_launched,
        session_registry=mock_registry,
        instance_tracker=create_mock_instance_tracker(),
    )

    from deephaven_mcp.auth.credentials import AnonymousCredentials

    mock_registry.add_dynamic_session.assert_awaited_once()
    kwargs = mock_registry.add_dynamic_session.call_args.kwargs
    assert isinstance(kwargs["session_config"].credentials, AnonymousCredentials)


@pytest.mark.asyncio
async def test_session_community_credentials_static_session_custom_token():
    """Custom-token credentials surface as ``auth_type='CUSTOM'`` with the token."""
    from deephaven_mcp.resource_manager._manager import StaticCommunitySessionManager

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    config = {"security": {"credential_retrieval_mode": "all"}}
    mock_config_manager.get_config = AsyncMock(return_value=config)
    mock_session_registry._community_settings = config

    session_config = stub_session_config(
        name="custom-session",
        host="localhost",
        port=10000,
        auth={
            "credentials": {
                "type": "custom",
                "auth_type": "MyAuth",
                "auth_token": "abc",
            }
        },
    )
    manager = StaticCommunitySessionManager(
        name="custom-session",
        session_config=session_config,
        timeouts=CommunityClientTimeouts(),
    )
    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )
    result = await session_community_credentials(
        context, session_id="community:community:custom-session"
    )

    assert result["success"] is True
    # ``CustomTokenCredentials`` carries its own ``auth_type`` (the
    # custom Java handler class name) which the tool surfaces verbatim.
    assert result["auth_type"] == "MyAuth"
    assert result["auth_token"] == "abc"


@pytest.mark.asyncio
async def test_session_community_credentials_static_session_unsupported_creds():
    """Unsupported credentials (e.g. password) yield a benign default with no token."""
    from deephaven_mcp.resource_manager._manager import StaticCommunitySessionManager

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    config = {"security": {"credential_retrieval_mode": "all"}}
    mock_config_manager.get_config = AsyncMock(return_value=config)
    mock_session_registry._community_settings = config

    # ``PasswordCredentials`` is not produced by the community wire-
    # format schema, so we hand-build a typed declaration that bypasses
    # the JSON validator by constructing the field directly.
    from deephaven_mcp.auth.credentials import PasswordCredentials
    from deephaven_mcp.sessions import CommunitySessionConfig

    session_config = CommunitySessionConfig.model_construct(
        name="pw-session",
        host="localhost",
        port=10000,
        programming_language=None,
        never_timeout=None,
        tls=None,
        credentials=PasswordCredentials(username="u", password="p"),
    )
    manager = StaticCommunitySessionManager(
        name="pw-session",
        session_config=session_config,
        timeouts=CommunityClientTimeouts(),
    )
    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )
    result = await session_community_credentials(
        context, session_id="community:community:pw-session"
    )

    assert result["success"] is True
    assert result["auth_token"] == ""
    assert result["auth_type"] == "PASSWORDCREDENTIALS"


@pytest.mark.asyncio
async def test_session_community_credentials_invalid_session_id():
    """Test when session_id has invalid format."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    config = {"security": {"credential_retrieval_mode": "all"}}

    mock_config_manager.get_config = AsyncMock(return_value=config)
    # Stash for conftest's lifespan adapter (security tests use ``config``
    # rather than ``community_config`` to hold the settings dict).
    mock_session_registry._community_settings = config

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="enterprise:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Invalid session_id" in result["error"]
    assert "community:community:" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_exception_handling():
    """Test exception handling in session_community_credentials.

    The production code now reads community settings as a typed
    :class:`CommunitySettings` model. Trigger an unexpected error by
    making attribute access on ``community.settings.security`` raise so
    the tool exercises its catch-all error path.
    """
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    # Build a multi_config whose community settings access blows up.
    boom_settings = MagicMock()
    type(boom_settings).security = property(
        lambda self: (_ for _ in ()).throw(RuntimeError("Unexpected config error"))
    )
    boom_community = MagicMock()
    boom_community.settings = boom_settings
    multi_config = MagicMock()
    multi_config.community = boom_community

    context = MockContext(
        {
            "registry": mock_session_registry,
            "multi_config": multi_config,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:community:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Unexpected config error" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_dynamic_only_denies_static():
    """Test that mode='dynamic_only' denies static session credentials."""
    from deephaven_mcp.resource_manager._manager import StaticCommunitySessionManager

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    config = {"security": {"credential_retrieval_mode": "dynamic_only"}}

    mock_config_manager.get_config = AsyncMock(return_value=config)
    # Stash for conftest's lifespan adapter (security tests use ``config``
    # rather than ``community_config`` to hold the settings dict).
    mock_session_registry._community_settings = config

    # Create a static session manager (the static_config dict is not
    # used by StaticCommunitySessionManager today; left only for
    # documentation of intent).
    manager = StaticCommunitySessionManager(
        name="local-dev",
        session_config=stub_session_config(name="local-dev"),
        timeouts=CommunityClientTimeouts(),
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:community:local-dev"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "static sessions is disabled" in result["error"]
    assert "dynamic_only" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_static_only_denies_dynamic():
    """Test that mode='static_only' denies dynamic session credentials."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    config = {"security": {"credential_retrieval_mode": "static_only"}}

    mock_config_manager.get_config = AsyncMock(return_value=config)
    # Stash for conftest's lifespan adapter (security tests use ``config``
    # rather than ``community_config`` to hold the settings dict).
    mock_session_registry._community_settings = config

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
        session_config=stub_session_config(),
        launched_session=mock_launched_session,
        timeouts=CommunityClientTimeouts(),
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:community:test-session"
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
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    config = {"security": {"credential_retrieval_mode": "all"}}

    mock_config_manager.get_config = AsyncMock(return_value=config)
    # Stash for conftest's lifespan adapter (security tests use ``config``
    # rather than ``community_config`` to hold the settings dict).
    mock_session_registry._community_settings = config

    # Typed static session declaration with PSK auth so the tool
    # reads ``credentials`` directly.
    session_config = stub_session_config(
        name="local-dev",
        host="localhost",
        port=10000,
        auth={"credentials": {"type": "psk", "token": "static_token_123"}},
    )

    static_manager = StaticCommunitySessionManager(
        name="local-dev",
        session_config=session_config,
        timeouts=CommunityClientTimeouts(),
    )

    mock_session_registry.get = AsyncMock(return_value=static_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(
        context, session_id="community:community:local-dev"
    )

    assert result["success"] is True
    assert result["auth_token"] == "static_token_123"


def test_credentials_to_auth_type_none_defaults_to_psk():
    """None credentials default to the PSK handler (historical default)."""
    result, error = _credentials_to_auth_type(None)
    assert error is None
    assert result == _PSK_AUTH_HANDLER


def test_credentials_to_auth_type_psk():
    """PSKCredentials maps to the PSK handler FQCN."""
    from deephaven_mcp.auth.credentials import PSKCredentials

    result, error = _credentials_to_auth_type(PSKCredentials(token="t"))
    assert error is None
    assert result == _PSK_AUTH_HANDLER


def test_credentials_to_auth_type_anonymous():
    """AnonymousCredentials maps to ``"Anonymous"``."""
    from deephaven_mcp.auth.credentials import AnonymousCredentials

    result, error = _credentials_to_auth_type(AnonymousCredentials())
    assert error is None
    assert result == _ANONYMOUS_AUTH_HANDLER


def test_credentials_to_auth_type_custom_passthrough():
    """CustomTokenCredentials forwards its declared auth_type FQCN verbatim."""
    from deephaven_mcp.auth.credentials import CustomTokenCredentials

    creds = CustomTokenCredentials(
        auth_type="com.example.CustomAuth", auth_token="opaque"
    )
    result, error = _credentials_to_auth_type(creds)
    assert error is None
    assert result == "com.example.CustomAuth"


def test_credentials_to_auth_type_password_rejected():
    """PasswordCredentials are rejected for dynamically-launched workers."""
    from deephaven_mcp.auth.credentials import PasswordCredentials

    creds = PasswordCredentials(username="u", password="p")
    result, error = _credentials_to_auth_type(creds)
    assert result == ""
    assert error is not None
    assert "Basic authentication is not supported" in error
    assert "dynamically-launched" in error


def test_resolve_community_session_parameters_password_credentials_rejected():
    """PasswordCredentials in defaults trigger the dynamic-session rejection."""
    from deephaven_mcp.auth.credentials import PasswordCredentials

    defaults = CommunitySessionCreationDefaults.model_construct(
        launch_method="docker",
        credentials=PasswordCredentials(username="u", password="p"),
        programming_language="Python",
        docker_image=None,
        docker_memory_limit_gb=None,
        docker_cpu_limit=None,
        docker_volumes=None,
        python_venv_path=None,
        heap_size_gb=4.0,
        extra_jvm_args=None,
        environment_vars=None,
        startup_timeout_seconds=60.0,
        startup_check_interval_seconds=2.0,
        startup_retries=3,
    )
    resolved_params, error = _resolve_community_session_parameters(
        launch_method=None,
        programming_language=None,
        auth_token=None,
        heap_size_gb=None,
        extra_jvm_args=None,
        environment_vars=None,
        docker_image=None,
        docker_memory_limit_gb=None,
        docker_cpu_limit=None,
        docker_volumes=None,
        python_venv_path=None,
        defaults=defaults,
    )

    assert resolved_params == {}
    assert error is not None
    assert error["success"] is False
    assert error["isError"] is True
    assert "Basic authentication is not supported" in error["error"]


@pytest.mark.asyncio
async def test_session_community_create_groovy_programming_language_in_config():
    """Regression test: programming_language='Groovy' is forwarded to the session config.

    This test ensures programming_language is properly passed through to the
    session configuration. Previously, the parameter was used for Docker image
    selection but not included in the session config, causing all sessions to
    default to Python.
    """
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},
        }
    }

    full_config = community_config
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    # Stash for conftest's lifespan adapter — exposes the community
    # settings dict to the multiplexed-server tool helpers without
    # rewriting every test's MockContext call site.
    mock_session_registry._community_settings = community_config
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.add_dynamic_session = AsyncMock()
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

    async def capture_add_dynamic(*, name, session_config, launched_session):
        nonlocal captured_config
        captured_config = session_config
        manager = MagicMock()
        manager.full_name = f"community:community:{name}"
        return manager

    mock_session_registry.add_dynamic_session = AsyncMock(
        side_effect=capture_add_dynamic
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
                "registry": mock_session_registry,
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

        # CRITICAL: Verify session_config includes programming_language='Groovy'
        assert captured_config is not None, "Session config was not captured"
        assert (
            captured_config.programming_language is not None
        ), "programming_language missing from session config"
        assert (
            captured_config.programming_language == "Groovy"
        ), f"Expected programming_language='Groovy', got '{captured_config.programming_language}'"

        # Also verify the Docker image is correct
        call_kwargs = mock_launch_session.call_args.kwargs
        assert (
            "slim" in call_kwargs["docker_image"]
        ), "Groovy should use slim Docker image"


@pytest.mark.asyncio
async def test_session_community_create_python_programming_language_in_config():
    """Regression test: programming_language='Python' is forwarded to the session config."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},
        }
    }

    full_config = community_config
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    # Stash for conftest's lifespan adapter — exposes the community
    # settings dict to the multiplexed-server tool helpers without
    # rewriting every test's MockContext call site.
    mock_session_registry._community_settings = community_config
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.add_dynamic_session = AsyncMock()
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

    async def capture_add_dynamic(*, name, session_config, launched_session):
        nonlocal captured_config
        captured_config = session_config
        manager = MagicMock()
        manager.full_name = f"community:community:{name}"
        return manager

    mock_session_registry.add_dynamic_session = AsyncMock(
        side_effect=capture_add_dynamic
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
                "registry": mock_session_registry,
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
        assert captured_config.programming_language is not None
        assert captured_config.programming_language == "Python"


@pytest.mark.asyncio
async def test_session_community_create_default_programming_language_in_config():
    """Regression test: Verify omitting programming_language defaults to Python."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},  # No default programming_language
        }
    }

    full_config = community_config
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    # Stash for conftest's lifespan adapter — exposes the community
    # settings dict to the multiplexed-server tool helpers without
    # rewriting every test's MockContext call site.
    mock_session_registry._community_settings = community_config
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.add_dynamic_session = AsyncMock()
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

    async def capture_add_dynamic(*, name, session_config, launched_session):
        nonlocal captured_config
        captured_config = session_config
        manager = MagicMock()
        manager.full_name = f"community:community:{name}"
        return manager

    mock_session_registry.add_dynamic_session = AsyncMock(
        side_effect=capture_add_dynamic
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
                "registry": mock_session_registry,
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
        assert captured_config.programming_language is not None
        # Should default to Python
        assert captured_config.programming_language == "Python"


def test_register_tools_registers_community_tools():
    """register_tools() registers all DHC-specific session tools."""
    from mcp.server.fastmcp import FastMCP

    from deephaven_mcp.mcp_systems_server._tools.session_community import register_tools

    server = FastMCP("test-community-server")
    register_tools(server)
    tools = server._tool_manager._tools
    assert "session_community_create" in tools
    assert "session_community_delete" in tools
    assert "session_community_credentials" in tools
