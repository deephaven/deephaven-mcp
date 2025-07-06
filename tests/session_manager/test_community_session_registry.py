import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from pydeephaven import Session

from deephaven_mcp import config
from deephaven_mcp._exceptions import InternalError, SessionCreationError
from deephaven_mcp.session_manager import CommunitySessionRegistry
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
def community_session_registry():
    """Fixture for a CommunitySessionRegistry instance."""
    return CommunitySessionRegistry()


# --- Test Cases ---

def test_construction(community_session_registry):
    """Test that CommunitySessionRegistry can be constructed."""
    assert isinstance(community_session_registry, CommunitySessionRegistry)
    assert not community_session_registry._initialized


@pytest.mark.asyncio
async def test_initialize(community_session_registry, mock_config_manager):
    """Test that initialize() populates session managers correctly."""
    await community_session_registry.initialize(mock_config_manager)
    assert community_session_registry._initialized
    assert len(community_session_registry._items) == 2
    assert "worker1" in community_session_registry._items
    assert "worker2" in community_session_registry._items
    assert isinstance(community_session_registry._items["worker1"], CommunitySessionManager)

    # Test idempotency
    await community_session_registry.initialize(mock_config_manager)
    assert len(community_session_registry._items) == 2


@pytest.mark.asyncio
async def test_methods_raise_before_initialize(community_session_registry):
    """Test that methods raise InternalError if called before initialization."""
    with pytest.raises(InternalError, match="CommunitySessionRegistry not initialized"):
        await community_session_registry.get("worker1")

    with pytest.raises(InternalError, match="CommunitySessionRegistry not initialized"):
        await community_session_registry.close()


@pytest.mark.asyncio
async def test_get_returns_manager(community_session_registry, mock_config_manager):
    """Test that get() returns the correct session manager instance."""
    await community_session_registry.initialize(mock_config_manager)
    manager = await community_session_registry.get("worker1")
    assert isinstance(manager, CommunitySessionManager)
    assert manager.name == "worker1"


@pytest.mark.asyncio
async def test_get_unknown_raises_key_error(community_session_registry, mock_config_manager):
    """Test that get() for an unknown worker raises KeyError."""
    await community_session_registry.initialize(mock_config_manager)
    with pytest.raises(KeyError, match="No item found for: unknown_worker"):
        await community_session_registry.get("unknown_worker")


@pytest.mark.asyncio
async def test_close_calls_close_on_managers(community_session_registry, mock_config_manager):
    """Test that close() calls close() on each manager but does not clear the registry."""
    await community_session_registry.initialize(mock_config_manager)

    # Mock the close method of the managers
    manager1 = community_session_registry._items["worker1"]
    manager2 = community_session_registry._items["worker2"]
    manager1.close = AsyncMock()
    manager2.close = AsyncMock()

    await community_session_registry.close()

    # Verify close was called on each manager
    manager1.close.assert_awaited_once()
    manager2.close.assert_awaited_once()

    # The registry should still be initialized and sessions should be present
    assert community_session_registry._initialized
    assert "worker1" in community_session_registry._items

    # Getting a manager should still work
    manager = await community_session_registry.get("worker1")
    assert manager is manager1


@pytest.mark.asyncio
async def test_get_session_from_manager(community_session_registry, mock_config_manager):
    """Test the full flow of getting a session via the registry and manager."""
    await community_session_registry.initialize(mock_config_manager)

    mock_session = AsyncMock(spec=Session)
    mock_session.is_alive = True

    # Patch the manager's get method to return our mock session
    with patch(
        "deephaven_mcp.session_manager._session_manager.CommunitySessionManager.get",
        new=AsyncMock(return_value=mock_session),
    ) as mock_manager_get:
        manager = await community_session_registry.get("worker1")
        session = await manager.get()

        assert session is mock_session
        mock_manager_get.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_session_creation_error(community_session_registry, mock_config_manager):
    """Test that SessionCreationError from the manager propagates."""
    await community_session_registry.initialize(mock_config_manager)

    # Patch the manager's get method to raise an error
    with patch(
        "deephaven_mcp.session_manager._session_manager.CommunitySessionManager.get",
        side_effect=SessionCreationError("Failed to connect"),
    ):
        manager = await community_session_registry.get("worker1")
        with pytest.raises(SessionCreationError, match="Failed to connect"):
            await manager.get()
