"""
Tests for CommunitySessionRegistry in the resource manager module.
"""

from unittest.mock import AsyncMock, patch

import pytest
from pydeephaven import Session

from deephaven_mcp import config
from deephaven_mcp._exceptions import (
    InternalError,
    SessionCreationError,
)
from deephaven_mcp.resource_manager import (
    CommunitySessionRegistry,
    CommunitySessionManager,
)
from deephaven_mcp.resource_manager._registry import MutableSessionRegistry


@pytest.fixture
def mock_community_config_manager():
    """Fixture for a mock ConfigManager with two community session configs."""
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


# --- Construction and isinstance ---


def test_community_registry_construction(community_session_registry):
    """Test that CommunitySessionRegistry can be constructed."""
    assert isinstance(community_session_registry, CommunitySessionRegistry)
    assert not community_session_registry._initialized


def test_community_registry_is_mutable_session_registry():
    """CommunitySessionRegistry is a MutableSessionRegistry."""
    assert isinstance(CommunitySessionRegistry(), MutableSessionRegistry)


# --- initialize / _load_items ---


@pytest.mark.asyncio
async def test_community_registry_initialize(
    community_session_registry, mock_community_config_manager
):
    """initialize() populates session managers from config."""
    await community_session_registry.initialize(mock_community_config_manager)
    assert community_session_registry._initialized
    assert len(community_session_registry._items) == 2
    assert "community:config:worker1" in community_session_registry._items
    assert "community:config:worker2" in community_session_registry._items
    assert isinstance(
        community_session_registry._items["community:config:worker1"], CommunitySessionManager
    )

    # Idempotency: second call should not re-load
    await community_session_registry.initialize(mock_community_config_manager)
    assert len(community_session_registry._items) == 2


@pytest.mark.asyncio
async def test_community_registry_initialize_empty_sessions():
    """initialize() handles a config with no sessions gracefully."""
    empty_config_manager = AsyncMock(spec=config.ConfigManager)
    empty_config_manager.get_config = AsyncMock(return_value={"community": {"sessions": {}}})

    registry = CommunitySessionRegistry()
    await registry.initialize(empty_config_manager)

    assert registry._initialized
    assert len(registry._items) == 0


@pytest.mark.asyncio
async def test_community_registry_initialize_no_community_key():
    """initialize() handles a config with no 'community' key gracefully."""
    no_community_config_manager = AsyncMock(spec=config.ConfigManager)
    no_community_config_manager.get_config = AsyncMock(return_value={})

    registry = CommunitySessionRegistry()
    await registry.initialize(no_community_config_manager)

    assert registry._initialized
    assert len(registry._items) == 0


# --- Methods raise before initialize ---


@pytest.mark.asyncio
async def test_community_registry_methods_raise_before_initialize(
    community_session_registry,
):
    """Methods raise InternalError if called before initialization."""
    with pytest.raises(InternalError, match="CommunitySessionRegistry not initialized"):
        await community_session_registry.get("community:config:worker1")

    with pytest.raises(InternalError, match="CommunitySessionRegistry not initialized"):
        await community_session_registry.close()


# --- get ---


@pytest.mark.asyncio
async def test_community_registry_get_returns_manager(
    community_session_registry, mock_community_config_manager
):
    """get() returns the correct session manager instance."""
    await community_session_registry.initialize(mock_community_config_manager)
    manager = await community_session_registry.get("community:config:worker1")
    assert isinstance(manager, CommunitySessionManager)
    assert manager._name == "worker1"


@pytest.mark.asyncio
async def test_community_registry_get_unknown_raises_registry_item_not_found(
    community_session_registry, mock_community_config_manager
):
    """get() raises RegistryItemNotFoundError for an unknown session name."""
    from deephaven_mcp._exceptions import RegistryItemNotFoundError

    await community_session_registry.initialize(mock_community_config_manager)
    with pytest.raises(
        RegistryItemNotFoundError,
        match="No item with name 'unknown_worker' found in CommunitySessionRegistry",
    ):
        await community_session_registry.get("unknown_worker")


# --- close ---


@pytest.mark.asyncio
async def test_community_registry_close_calls_close_on_managers(
    community_session_registry, mock_community_config_manager
):
    """close() calls close() on each manager and resets state."""
    await community_session_registry.initialize(mock_community_config_manager)

    manager1 = community_session_registry._items["community:config:worker1"]
    manager2 = community_session_registry._items["community:config:worker2"]
    manager1.close = AsyncMock()
    manager2.close = AsyncMock()

    await community_session_registry.close()

    manager1.close.assert_awaited_once()
    manager2.close.assert_awaited_once()

    assert not community_session_registry._initialized
    assert community_session_registry._items == {}


# --- end-to-end session retrieval ---


@pytest.mark.asyncio
async def test_community_registry_get_session_from_manager(
    community_session_registry, mock_community_config_manager
):
    """Full flow: get manager from registry then get session from manager."""
    await community_session_registry.initialize(mock_community_config_manager)

    mock_session = AsyncMock(spec=Session)
    mock_session.is_alive = True

    with patch(
        "deephaven_mcp.resource_manager._manager.CommunitySessionManager.get",
        new=AsyncMock(return_value=mock_session),
    ) as mock_manager_get:
        manager = await community_session_registry.get("community:config:worker1")
        session = await manager.get()

        assert session is mock_session
        mock_manager_get.assert_awaited_once()


@pytest.mark.asyncio
async def test_community_registry_get_session_creation_error(
    community_session_registry, mock_community_config_manager
):
    """SessionCreationError from the manager propagates through the registry."""
    await community_session_registry.initialize(mock_community_config_manager)

    with patch(
        "deephaven_mcp.resource_manager._manager.CommunitySessionManager.get",
        side_effect=SessionCreationError("Failed to connect"),
    ):
        manager = await community_session_registry.get("community:config:worker1")
        with pytest.raises(SessionCreationError, match="Failed to connect"):
            await manager.get()
