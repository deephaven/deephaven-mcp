import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydeephaven import Session

from deephaven_mcp import config
from deephaven_mcp._exceptions import InternalError, SessionCreationError
from deephaven_mcp.session_manager import SessionRegistry
from deephaven_mcp.session_manager._session_manager import CommunitySessionManager


# --- Fixtures ---

@pytest.fixture
def mock_config_manager():
    """Fixture for a mock ConfigManager."""
    mock = AsyncMock(spec=config.ConfigManager)
    mock.get_config = AsyncMock(
        return_value={
            "community": {
                "sessions": {
                    "worker1": {"host": "localhost", "port": 10001},
                    "worker2": {"host": "localhost", "port": 10002},
                }
            }
        }
    )
    return mock


@pytest.fixture
def session_registry():
    """Fixture for a SessionRegistry instance."""
    return SessionRegistry()


# --- Test Cases ---

def test_construction(session_registry):
    """Test that SessionRegistry can be constructed."""
    assert isinstance(session_registry, SessionRegistry)
    assert not session_registry._initialized


@pytest.mark.asyncio
async def test_initialize(session_registry, mock_config_manager):
    """Test that initialize() populates session managers correctly."""
    await session_registry.initialize(mock_config_manager)
    assert session_registry._initialized
    assert len(session_registry._sessions) == 2
    assert "worker1" in session_registry._sessions
    assert "worker2" in session_registry._sessions
    assert isinstance(session_registry._sessions["worker1"], CommunitySessionManager)

    # Test idempotency
    await session_registry.initialize(mock_config_manager)
    assert len(session_registry._sessions) == 2


@pytest.mark.asyncio
async def test_methods_raise_before_initialize(session_registry):
    """Test that methods raise InternalError if called before initialization."""
    with pytest.raises(InternalError, match="SessionRegistry not initialized"):
        await session_registry.get("worker1")

    with pytest.raises(InternalError, match="SessionRegistry not initialized"):
        await session_registry.close()


@pytest.mark.asyncio
async def test_get_returns_manager(session_registry, mock_config_manager):
    """Test that get() returns the correct session manager instance."""
    await session_registry.initialize(mock_config_manager)
    manager = await session_registry.get("worker1")
    assert isinstance(manager, CommunitySessionManager)
    assert manager.name == "worker1"


@pytest.mark.asyncio
async def test_get_unknown_raises_key_error(session_registry, mock_config_manager):
    """Test that get() for an unknown worker raises KeyError."""
    await session_registry.initialize(mock_config_manager)
    with pytest.raises(KeyError, match="No session configuration found for worker: unknown_worker"):
        await session_registry.get("unknown_worker")


@pytest.mark.asyncio
async def test_close_calls_close_on_managers(session_registry, mock_config_manager):
    """Test that close() calls close() on each manager but does not clear the registry."""
    await session_registry.initialize(mock_config_manager)

    # Mock the close method of the managers
    manager1 = session_registry._sessions["worker1"]
    manager2 = session_registry._sessions["worker2"]
    manager1.close = AsyncMock()
    manager2.close = AsyncMock()

    await session_registry.close()

    # Verify close was called on each manager
    manager1.close.assert_awaited_once()
    manager2.close.assert_awaited_once()

    # The registry should still be initialized and sessions should be present
    assert session_registry._initialized
    assert "worker1" in session_registry._sessions

    # Getting a manager should still work
    manager = await session_registry.get("worker1")
    assert manager is manager1


@pytest.mark.asyncio
async def test_get_session_from_manager(session_registry, mock_config_manager):
    """Test the full flow of getting a session via the registry and manager."""
    await session_registry.initialize(mock_config_manager)

    mock_session = AsyncMock(spec=Session)
    mock_session.is_alive = True

    # Patch the manager's get method to return our mock session
    with patch(
        "deephaven_mcp.session_manager._session_manager.CommunitySessionManager.get",
        new=AsyncMock(return_value=mock_session),
    ) as mock_manager_get:
        manager = await session_registry.get("worker1")
        session = await manager.get()

        assert session is mock_session
        mock_manager_get.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_session_creation_error(session_registry, mock_config_manager):
    """Test that SessionCreationError from the manager propagates."""
    await session_registry.initialize(mock_config_manager)

    # Patch the manager's get method to raise an error
    with patch(
        "deephaven_mcp.session_manager._session_manager.CommunitySessionManager.get",
        side_effect=SessionCreationError("Failed to connect"),
    ):
        manager = await session_registry.get("worker1")
        with pytest.raises(SessionCreationError, match="Failed to connect"):
            await manager.get()
