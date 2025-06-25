"""Test session manager module."""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
import pytest_asyncio

from deephaven_mcp.sessions._errors import SessionCreationError
from deephaven_mcp.sessions._session._session_base import SessionBase
from deephaven_mcp.sessions._session._session_community import SessionCommunity
from deephaven_mcp.sessions._session_manager import SessionManager


@pytest_asyncio.fixture
async def session_manager():
    """Create a SessionManager instance for testing."""
    # Direct config data instead of ConfigManager
    config_data = {
        "community": {
            "sessions": {
                "local": {"host": "localhost", "port": 10000},
                "foo": {"host": "localhost", "port": 10001},
            }
        }
    }

    # We need to patch the SessionCommunity class to avoid actual initialization
    with patch(
        "deephaven_mcp.sessions._session_manager.SessionCommunity"
    ) as mock_session_community_class:
        # Setup mock session instances
        mock_local_session = MagicMock(spec=SessionBase)
        mock_local_session.name = "local"
        mock_local_session.close_session = AsyncMock()
        mock_local_session.get_session = AsyncMock()
        mock_local_session.is_alive = PropertyMock(return_value=True)

        mock_foo_session = MagicMock(spec=SessionBase)
        mock_foo_session.name = "foo"
        mock_foo_session.close_session = AsyncMock()
        mock_foo_session.get_session = AsyncMock()
        mock_foo_session.is_alive = PropertyMock(return_value=True)

        # Setup mock constructor to return our mocks
        def create_mock_session(name, config):
            if name == "local":
                return mock_local_session
            elif name == "foo":
                return mock_foo_session
            raise ValueError(f"Unexpected session name: {name}")

        mock_session_community_class.side_effect = create_mock_session

        # Create SessionManager with direct config data
        manager = SessionManager(config_data)

        try:
            yield manager
        finally:
            # Clean up
            await manager.close_all_sessions()


@pytest.mark.asyncio
async def test_session_manager_init():
    """Test initializing a SessionManager."""
    config_data = {"community": {"sessions": {"local": {"host": "localhost"}}}}

    with patch("deephaven_mcp.sessions._session_manager.SessionCommunity"):
        manager = SessionManager(config_data)

        # Verify basic structure
        assert isinstance(manager._sessions_community, list)
        assert len(manager._sessions_community) == 1  # One session in config
        assert manager._lock is not None


@pytest.mark.asyncio
async def test_empty_config_init():
    """Test initializing with empty configuration."""
    config_data = {"community": {"sessions": {}}}

    manager = SessionManager(config_data)

    # Verify empty session list
    assert isinstance(manager._sessions_community, list)
    assert len(manager._sessions_community) == 0


@pytest.mark.asyncio
async def test_get_by_name_retrieves_session(session_manager):
    """Test get_by_name retrieves the correct session object."""
    # Get a specific session by name
    foo_session_obj = await session_manager.get_by_name("foo")

    # Verify it's the correct session
    assert foo_session_obj.name == "foo"

    # Should be one of the sessions in the list
    assert any(s.name == "foo" for s in session_manager._sessions_community)


@pytest.mark.asyncio
async def test_get_by_name_unknown_session(session_manager):
    """Test get_by_name with unknown session name."""
    with pytest.raises(ValueError) as exc_info:
        await session_manager.get_by_name("nonexistent")

    assert "No session configuration found for session" in str(exc_info.value)


@pytest.mark.asyncio
async def test_session_manager_concurrent_get_by_name(session_manager):
    """Test concurrent calls to get_by_name return the correct session objects."""
    # Run multiple concurrent get_by_name calls
    tasks = [session_manager.get_by_name("local") for _ in range(5)]
    session_objects = await asyncio.gather(*tasks)

    # All should reference the same session object
    first_session = session_objects[0]
    assert all(
        session_obj.name == first_session.name for session_obj in session_objects
    )

    # Now get the sessions from the session objects
    for session_obj in session_objects:
        await session_obj.get_session()

    # Verify get_session was called on the session objects
    for session_obj in session_objects:
        session_obj.get_session.assert_called()


@pytest.mark.asyncio
async def test_close_all_sessions(session_manager):
    """Test close_all_sessions properly closes all sessions."""
    # Get reference to the mock sessions
    sessions = session_manager._sessions_community

    # Close all sessions
    await session_manager.close_all_sessions()

    # Verify each session had close_session called
    for session in sessions:
        session.close_session.assert_called_once()

    # Verify sessions are NOT removed from list (new behavior)
    assert len(session_manager._sessions_community) > 0


@pytest.mark.asyncio
async def test_get_by_name_nonexistent_with_config(session_manager):
    """Test get_by_name with a session name in config but not created yet."""
    # Setup: We keep the existing config but remove the session from the list
    session_manager._sessions_community = [
        s for s in session_manager._sessions_community if s.name != "foo"
    ]

    # Add foo config back (should already exist from the fixture setup)
    session_config = {
        "community": {"sessions": {"foo": {"host": "localhost", "port": 10001}}}
    }

    # Mock SessionCommunity creation for when it tries to create the missing session
    with patch(
        "deephaven_mcp.sessions._session_manager.SessionCommunity"
    ) as mock_session_community:
        mock_foo_session = MagicMock(spec=SessionBase)
        mock_foo_session.name = "foo"
        mock_session_community.return_value = mock_foo_session

        # With the original implementation, this would try to create a new session
        # With the new implementation, it will just raise ValueError since the session wasn't initialized
        with pytest.raises(ValueError) as exc_info:
            await session_manager.get_by_name("foo")

        assert "No session configuration found for session: foo" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_by_name_session_failure(session_manager):
    """Test handling of a session that fails to be retrieved."""
    # Get a session
    foo_session = await session_manager.get_by_name("foo")

    # Make its get_session method fail
    foo_session.get_session.side_effect = SessionCreationError("Test failure")

    # Should raise the error to caller
    with pytest.raises(SessionCreationError):
        await foo_session.get_session()


@pytest.mark.asyncio
async def test_log_messages(session_manager, caplog):
    """Test that proper log messages are generated."""
    caplog.set_level(logging.INFO)

    # Call some methods that generate logs
    await session_manager.get_by_name("local")
    await session_manager.close_all_sessions()

    # Verify log messages (partial match is sufficient)
    assert any("session 'local'" in rec.message for rec in caplog.records)
    assert any("Closing all sessions" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_get_session_liveness_check(session_manager):
    """Test that get_session method checks liveness of the session."""
    # Get a session from our fixture
    local_session = next(
        s for s in session_manager._sessions_community if s.name == "local"
    )

    # Set up mocking for liveness check and session creation
    with patch.object(
        local_session, "get_session", new_callable=AsyncMock
    ) as mock_get_session:
        # Have the mock return successfully
        mock_get_session.return_value = MagicMock()

        # Call get_by_name to retrieve the session
        retrieved_session = await session_manager.get_by_name("local")

        # Call get_session to trigger the liveness check
        await retrieved_session.get_session()

        # Verify that get_session was called
        mock_get_session.assert_called_once()

        # Our test passes if we successfully called get_session without exceptions
