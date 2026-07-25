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

from deephaven_mcp._exceptions import (
    CommunityNotConfiguredError,
    InternalError,
    RegistryItemNotFoundError,
    SessionCreationError,
)
from deephaven_mcp.client import CommunityClientTimeouts
from deephaven_mcp.config.schema import CommunitySessionCreationDefaults
from deephaven_mcp.mcp_systems_server._tools.session_community import (
    _ANONYMOUS_AUTH_HANDLER,
    _PSK_AUTH_HANDLER,
    _build_success_response,
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
    LaunchedSession,
    PythonLaunchedSession,
    QualifiedSessionId,
    ResourceLivenessStatus,
    SessionId,
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
    mock_session_registry.add_dynamic_session = AsyncMock(
        return_value=MagicMock(qualified_session_id="community:community:1")
    )
    mock_session_registry.get_all = AsyncMock(return_value=MagicMock(items={}))
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
        assert result["id"] == "community:community:1"
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
    mock_session_registry.add_dynamic_session = AsyncMock(
        return_value=MagicMock(qualified_session_id="community:community:1")
    )
    mock_session_registry.get_all = AsyncMock(return_value=MagicMock(items={}))
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
        assert result["id"] == "community:community:1"
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
    mock_session_registry.get_all = AsyncMock(return_value=MagicMock(items={}))
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
async def test_session_community_delete_no_community_returns_clean_error():
    """On a deployment with no Community sessions, session_community_delete
    surfaces a clean structured error rather than an uncaught/internal error.

    Tools register unconditionally, so a Community tool can be invoked with no
    Community section configured; ``get_community_registry`` raises
    ``CommunityNotConfiguredError``, which the tool's handler converts to
    ``success=False``.
    """
    context = MockContext(
        {
            "registry": MagicMock(),
            "instance_tracker": create_mock_instance_tracker(),
        }
    )
    with patch(
        "deephaven_mcp.mcp_systems_server._tools.session_community.get_community_registry",
        side_effect=CommunityNotConfiguredError(
            "No Community sessions are configured on this server."
        ),
    ):
        result = await session_community_delete(context, id="community:community:1")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Community" in result["error"]


@pytest.mark.asyncio
async def test_session_community_create_invalid_session_name_returns_error():
    """An ill-formed ``session_name`` is rejected before any registry or
    process work.

    The early ``validate_resource_name`` guard exists because the name
    doubles as a Docker container name, Python process tag, and the
    hash input for the :class:`SessionId`; any character outside
    ``[A-Za-z0-9_-]`` would break at least one of those downstream
    surfaces. This test pins the guard so it cannot regress to a
    silent late failure.
    """
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)
    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_create(
        context, session_name="bad name with spaces"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "session_name" in result["error"]
    # Guard fires before we touch the registry.
    mock_session_registry.add_dynamic_session.assert_not_called()


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
    mock_manager.qualified_session_id = "community:community:1"
    mock_manager.name = "test-session"
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
        id="community:community:1",
    )

    # Verify success
    assert result["success"] is True
    assert result["id"] == "community:community:1"
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
    mock_manager.qualified_session_id = "community:community:2"
    mock_manager.name = "python-session"
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
        id="community:community:2",
    )

    # Verify success
    assert result["success"] is True
    assert result["id"] == "community:community:2"

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
        id="community:community:999",
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
    mock_manager.qualified_session_id = "community:community:1"
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
        id="community:community:1",
    )

    assert result["success"] is False
    assert "Only dynamically created sessions" in result["error"]
    assert "static" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_rejects_mixed_case_programming_language():
    """Mixed-case programming_language values are rejected (exact-case closed vocabulary)."""
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
    mock_session_registry._community_settings = {
        "session_creation": {"defaults": {}, "max_concurrent_sessions": 5}
    }
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("not found")
    )

    for prog_lang in ("PYTHON", "python", "groovy", "GROOVY"):
        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await session_community_create(
            context,
            session_name="test-mixed-case-lang",
            launch_method="docker",
            programming_language=prog_lang,
        )

        assert result["success"] is False
        assert f"Invalid programming_language '{prog_lang}'" in result["error"]
        assert "'Python'" in result["error"] and "'Groovy'" in result["error"]
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_rejects_mixed_case_launch_method():
    """Mixed-case launch_method values are rejected (exact-case closed vocabulary)."""
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

    result = await session_community_create(
        context,
        session_name="test-mixed-case",
        launch_method="Docker",
        programming_language="Python",
    )

    assert result["success"] is False
    assert "Invalid launch_method 'Docker'" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_python_launch_success():
    """launch_method="python" succeeds end-to-end and reports a process_id."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},
        }
    }
    mock_config_manager.get_config = AsyncMock(return_value=community_config)
    mock_session_registry._community_settings = community_config
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError("not found")
    )
    mock_session_registry.add_dynamic_session = AsyncMock(
        return_value=MagicMock(qualified_session_id="community:community:py-launch")
    )

    mock_launched_session = MagicMock(spec=PythonLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "python"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?psk=test_token"
    )
    mock_launched_session.process = MagicMock(pid=4242)
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
            session_name="py-launch",
            launch_method="python",
        )

        assert result["success"] is True
        assert result["launch_method"] == "python"
        assert result["process_id"] == 4242
        assert "container_id" not in result
        assert mock_launch_session.call_args.kwargs["launch_method"] == "python"


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
    mock_manager.qualified_session_id = "community:community:10"
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
        id="community:community:10",
    )

    assert result["success"] is False
    assert "not a dynamically created session" in result["error"]
    assert "origin: 'static'" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_delete_allows_dynamic_sessions():
    """session_community_delete succeeds for a valid dynamic id."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    # Create a mock dynamic session manager with source="dynamic".
    # ``__class__`` is patched so the production ``isinstance`` narrowing
    # against :class:`CommunitySessionManager` succeeds.
    mock_dynamic_manager = MagicMock()
    mock_dynamic_manager.__class__ = DynamicCommunitySessionManager
    mock_dynamic_manager.qualified_session_id = "community:community:1"
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

    # Delete dynamic session using full id
    result = await session_community_delete(
        context,
        id="community:community:1",
    )

    # Verify success
    assert result["success"] is True
    assert result["id"] == "community:community:1"

    # Verify close and remove were called
    mock_dynamic_manager.close.assert_called_once()
    mock_session_registry.remove.assert_called_once_with(
        QualifiedSessionId.from_str("community:community:1")
    )


@pytest.mark.asyncio
async def test_session_community_delete_close_failure_continues():
    """session_community_delete logs warning and continues when close() raises."""
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.qualified_session_id = "community:community:11"
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

    result = await session_community_delete(context, id="community:community:11")

    # Should succeed despite close failure
    assert result["success"] is True
    assert result["id"] == "community:community:11"
    mock_session_registry.remove.assert_called_once()


@pytest.mark.asyncio
async def test_session_community_delete_removal_missing_in_registry():
    """session_community_delete logs warning when remove returns None."""
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.qualified_session_id = "community:community:12"
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

    result = await session_community_delete(context, id="community:community:12")

    assert result["success"] is True


@pytest.mark.asyncio
async def test_session_community_delete_registry_remove_raises():
    """session_community_delete returns error when remove raises."""
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.qualified_session_id = "community:community:13"
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

    result = await session_community_delete(context, id="community:community:13")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Simulated registry error" in result["error"]


@pytest.mark.asyncio
async def test_session_community_delete_outer_exception():
    """session_community_delete outer except handler fires when unexpected error occurs."""
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)
    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.qualified_session_id = "community:community:14"
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
        result = await session_community_delete(context, id="community:community:14")

    assert result["success"] is False
    assert result["isError"] is True
    assert "unexpected log error" in result["error"]


@pytest.mark.asyncio
async def test_session_community_delete_invalid_session_id_format():
    """session_community_delete returns error for malformed id."""
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(context, id="not-a-valid-id")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Invalid id format" in result["error"]
    mock_session_registry.get.assert_not_called()


@pytest.mark.asyncio
async def test_session_community_delete_wrong_system_type():
    """session_community_delete returns error when id is not community type."""
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await session_community_delete(context, id="enterprise:prod:1")

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
    mock_session_registry.add_dynamic_session = AsyncMock(
        return_value=MagicMock(qualified_session_id="community:community:1")
    )
    mock_session_registry.get_all = AsyncMock(return_value=MagicMock(items={}))
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
    mock_session_registry.add_dynamic_session = AsyncMock(
        return_value=MagicMock(qualified_session_id="community:community:1")
    )
    mock_session_registry.get_all = AsyncMock(return_value=MagicMock(items={}))
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
    assert "Invalid programming_language" in result["error"]
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
    mock_session_registry.add_dynamic_session = AsyncMock(
        return_value=MagicMock(qualified_session_id="community:community:1")
    )
    mock_session_registry.get_all = AsyncMock(return_value=MagicMock(items={}))
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

    result = await session_community_credentials(context, id="community:community:1")

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

    result = await session_community_credentials(context, id="community:community:1")

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
        session_id=SessionId.from_int(0),
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(context, id="community:community:1")

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
        session_id=SessionId.from_int(0),
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(context, id="community:community:1")

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

    result = await session_community_credentials(context, id="community:community:1")

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

    result = await session_community_credentials(context, id="community:community:999")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Session 'community:community:999' not found" in result["error"]


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
        context, id="community:community:static-session"
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
        session_id=SessionId.from_int(0),
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(context, id="community:community:100")

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
        session_id=SessionId.from_int(0),
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(context, id="community:community:101")

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
    mock_registry.get_all = AsyncMock(return_value=MagicMock(items={}))

    await _register_session_manager(
        session_name="custom-session",
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

    assert isinstance(cfg.auth.credentials, CustomTokenCredentials)
    assert cfg.auth.credentials.auth_token.get_secret_value() == "opaque"
    assert cfg.auth.credentials.auth_type == "com.example.CustomAuthHandler"


@pytest.mark.asyncio
async def test_register_session_manager_anonymous_auth():
    """``resolved_auth_type='Anonymous'`` produces an anonymous credentials block."""
    mock_launched = MagicMock(spec=DockerLaunchedSession)
    mock_registry = MagicMock(spec=CommunitySessionRegistry)
    mock_registry.add_dynamic_session = AsyncMock()
    mock_registry.get_all = AsyncMock(return_value=MagicMock(items={}))

    await _register_session_manager(
        session_name="anon-session",
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
    assert isinstance(kwargs["session_config"].auth.credentials, AnonymousCredentials)


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
        session_id=SessionId.from_int(0),
    )
    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )
    result = await session_community_credentials(
        context, id="community:community:custom-session"
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
    from deephaven_mcp.sessions import AuthConfig, CommunitySessionConfig

    session_config = CommunitySessionConfig.model_construct(
        name="pw-session",
        host="localhost",
        port=10000,
        programming_language=None,
        never_timeout=None,
        tls=None,
        auth=AuthConfig(credentials=PasswordCredentials(username="u", password="p")),
    )
    manager = StaticCommunitySessionManager(
        name="pw-session",
        session_config=session_config,
        timeouts=CommunityClientTimeouts(),
        session_id=SessionId.from_int(0),
    )
    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )
    result = await session_community_credentials(
        context, id="community:community:pw-session"
    )

    assert result["success"] is True
    assert result["auth_token"] == ""
    assert result["auth_type"] == "PASSWORDCREDENTIALS"


@pytest.mark.asyncio
async def test_session_community_credentials_unknown_manager_subtype():
    """A manager that is neither dynamic nor static surfaces the InternalError guard."""
    from deephaven_mcp.resource_manager._manager import CommunitySessionManager

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    config = {"security": {"credential_retrieval_mode": "all"}}
    mock_config_manager.get_config = AsyncMock(return_value=config)
    mock_session_registry._community_settings = config

    # Base-class mock: passes neither DynamicCommunitySessionManager nor
    # StaticCommunitySessionManager isinstance checks.
    manager = MagicMock(spec=CommunitySessionManager)
    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )
    result = await session_community_credentials(context, id="community:community:x")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Unhandled CommunitySessionManager subtype" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_unknown_manager_subtype_dynamic_only():
    """Unknown subtypes hit the InternalError guard before any mode check.

    With mode='dynamic_only', a naive ``is_static = not is_dynamic``
    classification would misreport an unknown subtype as a denied static
    session; the guard must fire instead.
    """
    from deephaven_mcp.resource_manager._manager import CommunitySessionManager

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock(spec=CommunitySessionRegistry)

    config = {"security": {"credential_retrieval_mode": "dynamic_only"}}
    mock_config_manager.get_config = AsyncMock(return_value=config)
    mock_session_registry._community_settings = config

    # Base-class mock: passes neither DynamicCommunitySessionManager nor
    # StaticCommunitySessionManager isinstance checks.
    manager = MagicMock(spec=CommunitySessionManager)
    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )
    result = await session_community_credentials(context, id="community:community:x")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Unhandled CommunitySessionManager subtype" in result["error"]
    assert "static" not in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_invalid_session_id():
    """Test when id has invalid format."""
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

    result = await session_community_credentials(context, id="enterprise:test-session")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Invalid id" in result["error"]
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

    result = await session_community_credentials(context, id="community:community:1")

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
        session_id=SessionId.from_int(0),
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(context, id="community:community:100")

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
        session_id=SessionId.from_int(0),
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(context, id="community:community:1")

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
        session_id=SessionId.from_int(0),
    )

    mock_session_registry.get = AsyncMock(return_value=static_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "registry": mock_session_registry,
        }
    )

    result = await session_community_credentials(context, id="community:community:100")

    assert result["success"] is True
    assert result["auth_token"] == "static_token_123"


def test_credentials_to_auth_type_none_defaults_to_psk():
    """None credentials default to the PSK handler (historical default)."""
    assert _credentials_to_auth_type(None) == _PSK_AUTH_HANDLER


def test_credentials_to_auth_type_psk():
    """PSKCredentials maps to the PSK handler FQCN."""
    from deephaven_mcp.auth.credentials import PSKCredentials

    assert _credentials_to_auth_type(PSKCredentials(token="t")) == _PSK_AUTH_HANDLER


def test_credentials_to_auth_type_anonymous():
    """AnonymousCredentials maps to ``"Anonymous"``."""
    from deephaven_mcp.auth.credentials import AnonymousCredentials

    assert _credentials_to_auth_type(AnonymousCredentials()) == _ANONYMOUS_AUTH_HANDLER


def test_credentials_to_auth_type_custom_passthrough():
    """CustomTokenCredentials forwards its declared auth_type FQCN verbatim."""
    from deephaven_mcp.auth.credentials import CustomTokenCredentials

    creds = CustomTokenCredentials(
        auth_type="com.example.CustomAuth", auth_token="opaque"
    )
    assert _credentials_to_auth_type(creds) == "com.example.CustomAuth"


def test_credentials_to_auth_type_password_rejected():
    """PasswordCredentials are rejected for dynamically-launched workers."""
    from deephaven_mcp.auth.credentials import PasswordCredentials

    creds = PasswordCredentials(username="u", password="p")
    with pytest.raises(
        SessionCreationError, match="Basic authentication is not supported"
    ) as exc_info:
        _credentials_to_auth_type(creds)
    assert "dynamically-launched" in str(exc_info.value)


def test_resolve_community_session_parameters_password_credentials_rejected():
    """PasswordCredentials in defaults trigger the dynamic-session rejection."""
    from deephaven_mcp.auth.credentials import PasswordCredentials
    from deephaven_mcp.sessions import AuthConfig

    defaults = CommunitySessionCreationDefaults.model_construct(
        launch_method="docker",
        auth=AuthConfig(credentials=PasswordCredentials(username="u", password="p")),
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
    with pytest.raises(
        SessionCreationError, match="Basic authentication is not supported"
    ):
        _resolve_community_session_parameters(
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


def test_resolve_community_session_parameters_rejects_unknown_launch_method():
    """An unrecognized launch_method fails fast with a clear error."""
    from deephaven_mcp.auth.credentials import AnonymousCredentials
    from deephaven_mcp.sessions import AuthConfig

    defaults = CommunitySessionCreationDefaults.model_construct(
        launch_method="docker",
        auth=AuthConfig(credentials=AnonymousCredentials()),
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
    with pytest.raises(
        SessionCreationError, match="Invalid launch_method 'podman'"
    ) as exc_info:
        _resolve_community_session_parameters(
            launch_method="podman",
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
    assert "'docker', 'python'" in str(exc_info.value)


def test_build_success_response_docker_session():
    """A DockerLaunchedSession contributes its container_id to the response."""
    launched = MagicMock(spec=DockerLaunchedSession)
    launched.container_id = "abc123"

    result = _build_success_response(
        "community:community:x",
        "x",
        "http://localhost:10000",
        _ANONYMOUS_AUTH_HANDLER,
        "docker",
        10000,
        launched,
    )

    assert result["success"] is True
    assert result["id"] == "community:community:x"
    assert result["launch_method"] == "docker"
    assert result["container_id"] == "abc123"
    assert "process_id" not in result


def test_build_success_response_python_session():
    """A PythonLaunchedSession contributes its process pid to the response."""
    launched = MagicMock(spec=PythonLaunchedSession)
    launched.process = MagicMock(pid=4242)

    result = _build_success_response(
        "community:community:x",
        "x",
        "http://localhost:10000",
        _ANONYMOUS_AUTH_HANDLER,
        "python",
        10000,
        launched,
    )

    assert result["success"] is True
    assert result["launch_method"] == "python"
    assert result["process_id"] == 4242
    assert "container_id" not in result


def test_build_success_response_unknown_session_type_raises():
    """An unrecognized LaunchedSession subtype raises InternalError."""
    with pytest.raises(InternalError, match="Unhandled LaunchedSession subtype"):
        _build_success_response(
            "community:community:x",
            "x",
            "http://localhost:10000",
            _ANONYMOUS_AUTH_HANDLER,
            "docker",
            10000,
            MagicMock(spec=LaunchedSession),
        )


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
    mock_session_registry.add_dynamic_session = AsyncMock(
        return_value=MagicMock(qualified_session_id="community:community:1")
    )
    mock_session_registry.get_all = AsyncMock(return_value=MagicMock(items={}))
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
        manager.qualified_session_id = f"community:community:{name}"
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
    mock_session_registry.add_dynamic_session = AsyncMock(
        return_value=MagicMock(qualified_session_id="community:community:1")
    )
    mock_session_registry.get_all = AsyncMock(return_value=MagicMock(items={}))
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
        manager.qualified_session_id = f"community:community:{name}"
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
    mock_session_registry.add_dynamic_session = AsyncMock(
        return_value=MagicMock(qualified_session_id="community:community:1")
    )
    mock_session_registry.get_all = AsyncMock(return_value=MagicMock(items={}))
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
        manager.qualified_session_id = f"community:community:{name}"
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


def test_credentials_to_auth_type_unrecognized_kind_raises():
    """An unrecognized credentials object raises SessionCreationError.

    Covers the fallthrough branch in ``_credentials_to_auth_type`` for
    credential kinds outside the documented ``CredentialsUnion``.
    """

    class _BogusCreds:
        pass

    with pytest.raises(
        SessionCreationError, match="Unsupported credentials kind"
    ) as exc_info:
        _credentials_to_auth_type(_BogusCreds())  # type: ignore[arg-type]
    assert "_BogusCreds" in str(exc_info.value)


def test_resolve_auth_token_non_psk_returns_none():
    """Only the PSK handler consumes a token; other handlers get ``(None, False)``.

    Covers the early-return branch in ``_resolve_auth_token`` for any
    auth_type that is not the PSK handler FQCN.
    """
    from deephaven_mcp.mcp_systems_server._tools.session_community import (
        _ANONYMOUS_AUTH_HANDLER,
        _resolve_auth_token,
    )

    defaults = CommunitySessionCreationDefaults()
    token, auto = _resolve_auth_token(
        _ANONYMOUS_AUTH_HANDLER, auth_token=None, defaults=defaults
    )
    assert token is None
    assert auto is False


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


def test_docker_image_for_language_maps_every_language():
    """Each ProgrammingLanguage member maps to its configured Docker image."""
    from deephaven_mcp.config.schema import DockerImages
    from deephaven_mcp.mcp_systems_server._tools.session_community import (
        _docker_image_for_language,
    )

    images = DockerImages(python="img-py", groovy="img-groovy")
    assert _docker_image_for_language("Python", images) == "img-py"
    assert _docker_image_for_language("Groovy", images) == "img-groovy"


def test_docker_image_for_language_asserts_on_unknown_language():
    """The assert_never fallthrough raises for values outside the Literal."""
    from deephaven_mcp.config.schema import DockerImages
    from deephaven_mcp.mcp_systems_server._tools.session_community import (
        _docker_image_for_language,
    )

    images = DockerImages()
    with pytest.raises(AssertionError):
        _docker_image_for_language("JavaScript", images)  # type: ignore[arg-type]
    # Suppression justified: deliberately constructing a value the
    # ``Literal`` rejects so the runtime ``assert_never`` branch is
    # covered. Bracketed ``arg-type`` names what is silenced; mypy
    # still flags any *unintentional* misuse at real call sites.


@pytest.mark.asyncio
async def test_session_community_create_input_schema_advertises_closed_vocabularies():
    """The MCP inputSchema advertises the exact enums for launch_method and programming_language.

    Regression guard: if either parameter reverts to a bare ``str``, AI
    agents lose the vocabulary from the tool schema and invalid values
    reach the tool body instead of being rejected at the protocol layer.
    """
    from mcp.server.fastmcp import FastMCP

    from deephaven_mcp.mcp_systems_server._tools.session_community import register_tools

    server = FastMCP("test-community-server")
    register_tools(server)
    (tool,) = [
        t for t in await server.list_tools() if t.name == "session_community_create"
    ]
    props = tool.inputSchema["properties"]

    launch_variants = props["launch_method"]["anyOf"]
    assert {"enum": ["docker", "python"], "type": "string"} in launch_variants

    language_variants = props["programming_language"]["anyOf"]
    assert {"enum": ["Python", "Groovy"], "type": "string"} in language_variants
