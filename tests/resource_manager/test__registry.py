"""
Tests for the registry classes in the session manager module.

This file contains tests for:
1. BaseRegistry - Abstract base class providing generic registry functionality
2. CommunitySessionRegistry - Registry for managing CommunitySessionManager instances
3. CorePlusSessionFactoryRegistry - Registry for managing CorePlusSessionFactoryManager instances
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydeephaven import Session

from deephaven_mcp import config
from deephaven_mcp._exceptions import (
    ConfigurationError,
    InternalError,
    SessionCreationError,
)
from deephaven_mcp.resource_manager import (
    CommunitySessionManager,
    CommunitySessionRegistry,
    CorePlusSessionFactoryRegistry,
    InitializationPhase,
    RegistrySnapshot,
)
from deephaven_mcp.resource_manager._registry import BaseRegistry

# --- RegistrySnapshot Tests ---


def test_snapshot_simple_sets_phase_and_empty_errors():
    """Test that simple() sets SIMPLE phase and empty errors dict."""
    snapshot = RegistrySnapshot.simple(items={"a": 1, "b": 2})
    assert snapshot.items == {"a": 1, "b": 2}
    assert snapshot.initialization_phase == InitializationPhase.SIMPLE
    assert snapshot.initialization_errors == {}


def test_snapshot_simple_empty_items():
    """Test simple() with an empty items dict."""
    snapshot = RegistrySnapshot.simple(items={})
    assert snapshot.items == {}
    assert snapshot.initialization_phase == InitializationPhase.SIMPLE
    assert snapshot.initialization_errors == {}


def test_snapshot_with_initialization_loading_phase():
    """Test with_initialization() during LOADING phase."""
    snapshot = RegistrySnapshot.with_initialization(
        items={"x": 10},
        phase=InitializationPhase.LOADING,
        errors={"src1": "Connection refused"},
    )
    assert snapshot.items == {"x": 10}
    assert snapshot.initialization_phase == InitializationPhase.LOADING
    assert snapshot.initialization_errors == {"src1": "Connection refused"}


def test_snapshot_with_initialization_partial_phase():
    """Test with_initialization() during PARTIAL phase."""
    snapshot = RegistrySnapshot.with_initialization(
        items={},
        phase=InitializationPhase.PARTIAL,
        errors={},
    )
    assert snapshot.initialization_phase == InitializationPhase.PARTIAL
    assert snapshot.initialization_errors == {}


def test_snapshot_with_initialization_completed_with_errors():
    """Test with_initialization() in COMPLETED phase with errors."""
    errors = {"src1": "timeout", "src2": "auth failed"}
    snapshot = RegistrySnapshot.with_initialization(
        items={"a": 1},
        phase=InitializationPhase.COMPLETED,
        errors=errors,
    )
    assert snapshot.initialization_phase == InitializationPhase.COMPLETED
    assert snapshot.initialization_errors == errors


def test_snapshot_direct_construction_requires_all_fields():
    """Test that omitting any field raises TypeError."""
    with pytest.raises(TypeError):
        RegistrySnapshot(items={})  # missing phase and errors
    with pytest.raises(TypeError):
        RegistrySnapshot(
            items={}, initialization_phase=InitializationPhase.COMPLETED
        )  # missing errors


def test_snapshot_frozen_immutability():
    """Test that RegistrySnapshot fields cannot be reassigned."""
    from dataclasses import FrozenInstanceError

    snapshot = RegistrySnapshot.simple(items={"a": 1})
    with pytest.raises(FrozenInstanceError):
        snapshot.items = {}
    with pytest.raises(FrozenInstanceError):
        snapshot.initialization_phase = InitializationPhase.NOT_STARTED
    with pytest.raises(FrozenInstanceError):
        snapshot.initialization_errors = {"x": "y"}


# --- Base Registry Tests ---


class MockItem:
    """A mock item with a close method for testing."""

    def __init__(self, name):
        self.name = name
        self.close = AsyncMock()


class ConcreteRegistry(BaseRegistry[MockItem]):
    """A concrete implementation of BaseRegistry for testing."""

    async def _load_items(self, config_manager: config.ConfigManager) -> None:
        config_data = await config_manager.get_config()
        for name, item_config in config_data.get("items", {}).items():
            self._items[name] = MockItem(name=item_config["name"])


@pytest.fixture
def mock_base_config_manager():
    """Fixture for a mock ConfigManager with item configurations."""
    mock = AsyncMock(spec=config.ConfigManager)
    mock.get_config = AsyncMock(
        return_value={
            "items": {
                "item1": {"name": "alpha"},
                "item2": {"name": "beta"},
            }
        }
    )
    return mock


@pytest.fixture
def registry():
    """Fixture for a ConcreteRegistry instance."""
    return ConcreteRegistry()


def test_construction(registry):
    """Test that the registry is constructed correctly."""
    assert isinstance(registry, BaseRegistry)
    assert not registry._initialized
    assert len(registry._items) == 0


@pytest.mark.asyncio
async def test_initialize(registry, mock_base_config_manager):
    """Test that initialize() loads items and sets the initialized flag."""
    await registry.initialize(mock_base_config_manager)
    assert registry._initialized
    assert len(registry._items) == 2
    assert "item1" in registry._items
    assert registry._items["item1"].name == "alpha"

    # Test idempotency
    await registry.initialize(mock_base_config_manager)
    assert len(registry._items) == 2


@pytest.mark.asyncio
async def test_methods_raise_before_initialize(registry):
    """Test that get() and close() raise InternalError before initialization."""
    with pytest.raises(InternalError, match="ConcreteRegistry not initialized"):
        await registry.get("item1")

    with pytest.raises(InternalError, match="ConcreteRegistry not initialized"):
        await registry.close()


@pytest.mark.asyncio
async def test_get_returns_item(registry, mock_base_config_manager):
    """Test that get() returns the correct item after initialization."""
    await registry.initialize(mock_base_config_manager)
    item = await registry.get("item1")
    assert isinstance(item, MockItem)
    assert item.name == "alpha"


@pytest.mark.asyncio
async def test_get_unknown_raises_registry_item_not_found(
    registry, mock_base_config_manager
):
    """Test that get() raises RegistryItemNotFoundError for an unknown item."""
    from deephaven_mcp._exceptions import RegistryItemNotFoundError

    await registry.initialize(mock_base_config_manager)
    with pytest.raises(
        RegistryItemNotFoundError,
        match="No item with name 'unknown' found in ConcreteRegistry",
    ):
        await registry.get("unknown")


@pytest.mark.asyncio
async def test_get_all_raises_before_initialize(registry):
    """Test that get_all() raises InternalError before initialization."""
    with pytest.raises(InternalError, match="not initialized"):
        await registry.get_all()


@pytest.mark.asyncio
async def test_get_all_returns_snapshot(registry, mock_base_config_manager):
    """Test that get_all() returns a RegistrySnapshot after initialization."""
    await registry.initialize(mock_base_config_manager)
    snapshot = await registry.get_all()

    # Should return a RegistrySnapshot with both configured items
    assert isinstance(snapshot, RegistrySnapshot)
    assert snapshot.initialization_phase == InitializationPhase.SIMPLE
    assert snapshot.initialization_errors == {}
    assert len(snapshot.items) == 2
    assert "item1" in snapshot.items
    assert "item2" in snapshot.items
    assert snapshot.items["item1"].name == "alpha"
    assert snapshot.items["item2"].name == "beta"


@pytest.mark.asyncio
async def test_get_all_returns_copy(registry, mock_base_config_manager):
    """Test that get_all() returns a copy of items, not the original dict."""
    await registry.initialize(mock_base_config_manager)
    snapshot = await registry.get_all()
    assert snapshot.initialization_phase == InitializationPhase.SIMPLE
    assert snapshot.initialization_errors == {}

    # Modify the returned dict
    snapshot.items["new_item"] = MockItem("new")

    # Original registry should be unchanged
    from deephaven_mcp._exceptions import RegistryItemNotFoundError

    with pytest.raises(RegistryItemNotFoundError):
        await registry.get("new_item")

    # Getting all items again should not include our modification
    fresh_snapshot = await registry.get_all()
    assert fresh_snapshot.initialization_phase == InitializationPhase.SIMPLE
    assert fresh_snapshot.initialization_errors == {}
    assert "new_item" not in fresh_snapshot.items
    assert len(fresh_snapshot.items) == 2


@pytest.mark.asyncio
async def test_get_all_empty_registry():
    """Test that get_all() works with an empty registry."""
    # Create a registry with no items configured
    empty_config_manager = AsyncMock(spec=config.ConfigManager)
    empty_config_manager.get_config = AsyncMock(return_value={"items": {}})

    registry = ConcreteRegistry()
    await registry.initialize(empty_config_manager)

    snapshot = await registry.get_all()
    assert isinstance(snapshot, RegistrySnapshot)
    assert snapshot.initialization_phase == InitializationPhase.SIMPLE
    assert snapshot.initialization_errors == {}
    assert len(snapshot.items) == 0


@pytest.mark.asyncio
async def test_close_calls_close_on_items(registry, mock_base_config_manager):
    """Test that close() calls the close() method on all managed items."""
    await registry.initialize(mock_base_config_manager)

    item1 = await registry.get("item1")
    item2 = await registry.get("item2")

    await registry.close()

    item1.close.assert_awaited_once()
    item2.close.assert_awaited_once()

    # close() resets _initialized and clears _items to allow reinitialization
    assert not registry._initialized
    assert registry._items == {}


# --- Community Session Registry Tests ---


@pytest.fixture
def mock_community_config_manager():
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


def test_community_registry_construction(community_session_registry):
    """Test that CommunitySessionRegistry can be constructed."""
    assert isinstance(community_session_registry, CommunitySessionRegistry)
    assert not community_session_registry._initialized


@pytest.mark.asyncio
async def test_community_registry_initialize(
    community_session_registry, mock_community_config_manager
):
    """Test that initialize() populates session managers correctly."""
    await community_session_registry.initialize(mock_community_config_manager)
    assert community_session_registry._initialized
    assert len(community_session_registry._items) == 2
    assert "worker1" in community_session_registry._items
    assert "worker2" in community_session_registry._items
    assert isinstance(
        community_session_registry._items["worker1"], CommunitySessionManager
    )

    # Test idempotency
    await community_session_registry.initialize(mock_community_config_manager)
    assert len(community_session_registry._items) == 2


@pytest.mark.asyncio
async def test_community_registry_methods_raise_before_initialize(
    community_session_registry,
):
    """Test that methods raise InternalError if called before initialization."""
    with pytest.raises(InternalError, match="CommunitySessionRegistry not initialized"):
        await community_session_registry.get("worker1")

    with pytest.raises(InternalError, match="CommunitySessionRegistry not initialized"):
        await community_session_registry.close()


@pytest.mark.asyncio
async def test_community_registry_get_returns_manager(
    community_session_registry, mock_community_config_manager
):
    """Test that get() returns the correct session manager instance."""
    await community_session_registry.initialize(mock_community_config_manager)
    manager = await community_session_registry.get("worker1")
    assert isinstance(manager, CommunitySessionManager)
    assert manager._name == "worker1"


@pytest.mark.asyncio
async def test_community_registry_get_unknown_raises_registry_item_not_found(
    community_session_registry, mock_community_config_manager
):
    """Test that get() for an unknown worker raises RegistryItemNotFoundError."""
    from deephaven_mcp._exceptions import RegistryItemNotFoundError

    await community_session_registry.initialize(mock_community_config_manager)
    with pytest.raises(
        RegistryItemNotFoundError,
        match="No item with name 'unknown_worker' found in CommunitySessionRegistry",
    ):
        await community_session_registry.get("unknown_worker")


@pytest.mark.asyncio
async def test_community_registry_close_calls_close_on_managers(
    community_session_registry, mock_community_config_manager
):
    """Test that close() calls close() on each manager but does not clear the registry."""
    await community_session_registry.initialize(mock_community_config_manager)

    # Mock the close method of the managers
    manager1 = community_session_registry._items["worker1"]
    manager2 = community_session_registry._items["worker2"]
    manager1.close = AsyncMock()
    manager2.close = AsyncMock()

    await community_session_registry.close()

    # Verify close was called on each manager
    manager1.close.assert_awaited_once()
    manager2.close.assert_awaited_once()

    # close() resets _initialized and clears _items to allow reinitialization
    assert not community_session_registry._initialized
    assert community_session_registry._items == {}


@pytest.mark.asyncio
async def test_community_registry_get_session_from_manager(
    community_session_registry, mock_community_config_manager
):
    """Test the full flow of getting a session via the registry and manager."""
    await community_session_registry.initialize(mock_community_config_manager)

    mock_session = AsyncMock(spec=Session)
    mock_session.is_alive = True

    # Patch the manager's get method to return our mock session
    with patch(
        "deephaven_mcp.resource_manager._manager.CommunitySessionManager.get",
        new=AsyncMock(return_value=mock_session),
    ) as mock_manager_get:
        manager = await community_session_registry.get("worker1")
        session = await manager.get()

        assert session is mock_session
        mock_manager_get.assert_awaited_once()


@pytest.mark.asyncio
async def test_community_registry_get_session_creation_error(
    community_session_registry, mock_community_config_manager
):
    """Test that SessionCreationError from the manager propagates."""
    await community_session_registry.initialize(mock_community_config_manager)

    # Patch the manager's get method to raise an error
    with patch(
        "deephaven_mcp.resource_manager._manager.CommunitySessionManager.get",
        side_effect=SessionCreationError("Failed to connect"),
    ):
        manager = await community_session_registry.get("worker1")
        with pytest.raises(SessionCreationError, match="Failed to connect"):
            await manager.get()


# --- CorePlus Session Factory Registry Tests ---


@pytest.fixture
def mock_factory_config_manager():
    """Fixture for a mock ConfigManager."""
    manager = AsyncMock(spec=config.ConfigManager)
    manager.get_config.return_value = {
        "enterprise": {
            "systems": {
                "factory1": {"host": "localhost", "port": 8080},
                "factory2": {"host": "remotehost", "port": 9090},
            }
        }
    }
    return manager


@pytest.mark.asyncio
async def test_factory_registry_creation(mock_factory_config_manager):
    """Test that the registry creates managers for each config entry."""
    with patch(
        "deephaven_mcp.resource_manager._registry.CorePlusSessionFactoryManager"
    ) as mock_manager:
        registry = CorePlusSessionFactoryRegistry()
        await registry.initialize(mock_factory_config_manager)

        assert mock_manager.call_count == 2
        mock_manager.assert_any_call("factory1", {"host": "localhost", "port": 8080})
        mock_manager.assert_any_call("factory2", {"host": "remotehost", "port": 9090})


@pytest.mark.asyncio
async def test_factory_registry_get_nonexistent(mock_factory_config_manager):
    """Test that getting a non-existent manager raises RegistryItemNotFoundError."""
    from deephaven_mcp._exceptions import RegistryItemNotFoundError

    registry = CorePlusSessionFactoryRegistry()
    await registry.initialize(mock_factory_config_manager)

    with pytest.raises(RegistryItemNotFoundError):
        await registry.get("nonexistent")


@pytest.mark.asyncio
async def test_factory_registry_no_systems_key():
    """Test that the registry handles a missing 'systems' key gracefully."""
    manager = AsyncMock(spec=config.ConfigManager)
    manager.get_config.return_value = {"enterprise": {}}
    registry = CorePlusSessionFactoryRegistry()
    await registry.initialize(manager)

    assert len(registry._items) == 0


@pytest.mark.asyncio
async def test_factory_registry_enterprise_not_available_raises_config_error():
    """Test that ConfigurationError is raised when enterprise configs exist but enterprise is not available."""
    manager = AsyncMock(spec=config.ConfigManager)
    manager.get_config.return_value = {
        "enterprise": {
            "systems": {
                "factory1": {"host": "localhost", "port": 8080},
            }
        }
    }

    # Mock is_enterprise_available as False
    with patch(
        "deephaven_mcp.resource_manager._registry.is_enterprise_available", False
    ):
        registry = CorePlusSessionFactoryRegistry()

        with pytest.raises(ConfigurationError) as exc_info:
            await registry.initialize(manager)

        # Verify the error message is helpful
        assert "Enterprise factory configurations" in str(exc_info.value)
        assert "deephaven-coreplus-client" in str(exc_info.value)
        assert "install" in str(exc_info.value).lower()
        assert "Python package" in str(exc_info.value)


