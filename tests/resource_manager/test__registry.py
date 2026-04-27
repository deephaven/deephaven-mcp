"""
Tests for the registry base classes in the resource manager module.

This file contains tests for:
1. BaseRegistry - Abstract base class providing generic registry functionality
2. MutableSessionRegistry - Abstract intermediate class owning mutation API
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deephaven_mcp import config
from deephaven_mcp._exceptions import (
    InternalError,
)
from deephaven_mcp.resource_manager import (
    BaseItemManager,
    InitializationPhase,
    RegistrySnapshot,
    SystemType,
)
from deephaven_mcp.resource_manager._registry import (
    BaseRegistry,
    MutableSessionRegistry,
)

# --- RegistrySnapshot Tests ---


def test_snapshot_simple_sets_phase_and_empty_errors():
    """Test that simple() sets COMPLETED phase and empty errors dict."""
    snapshot = RegistrySnapshot.simple(items={"a": 1, "b": 2})
    assert snapshot.items == {"a": 1, "b": 2}
    assert snapshot.initialization_phase == InitializationPhase.COMPLETED
    assert snapshot.initialization_errors == {}


def test_snapshot_simple_empty_items():
    """Test simple() with an empty items dict."""
    snapshot = RegistrySnapshot.simple(items={})
    assert snapshot.items == {}
    assert snapshot.initialization_phase == InitializationPhase.COMPLETED
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
    assert snapshot.initialization_phase == InitializationPhase.COMPLETED
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
    assert snapshot.initialization_phase == InitializationPhase.COMPLETED
    assert snapshot.initialization_errors == {}

    # Modify the returned dict
    snapshot.items["new_item"] = MockItem("new")

    # Original registry should be unchanged
    from deephaven_mcp._exceptions import RegistryItemNotFoundError

    with pytest.raises(RegistryItemNotFoundError):
        await registry.get("new_item")

    # Getting all items again should not include our modification
    fresh_snapshot = await registry.get_all()
    assert fresh_snapshot.initialization_phase == InitializationPhase.COMPLETED
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
    assert snapshot.initialization_phase == InitializationPhase.COMPLETED
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


@pytest.mark.asyncio
async def test_close_logs_error_when_item_close_raises(
    registry, mock_base_config_manager
):
    """close() logs errors from item.close() but continues closing remaining items."""
    await registry.initialize(mock_base_config_manager)

    item1 = await registry.get("item1")
    item2 = await registry.get("item2")
    item1.close = AsyncMock(side_effect=RuntimeError("network error"))
    item2.close = AsyncMock()

    # Should not raise — errors are logged and swallowed
    await registry.close()

    item1.close.assert_awaited_once()
    item2.close.assert_awaited_once()
    assert not registry._initialized


# ---------------------------------------------------------------------------
# MutableSessionRegistry — mutation methods (via concrete test subclass)
# ---------------------------------------------------------------------------


class _MutableRegistryImpl(MutableSessionRegistry):
    """Minimal concrete subclass of MutableSessionRegistry for testing."""

    async def _load_items(self, config_manager) -> None:  # type: ignore[override]
        pass  # no-op — tests populate _items directly


def _make_initialized_mutable_registry() -> _MutableRegistryImpl:
    """Return a _MutableRegistryImpl that has been initialized (empty)."""
    registry = _MutableRegistryImpl()
    registry._initialized = True
    return registry


def _make_mock_manager(full_name: str) -> MagicMock:
    """Return a BaseItemManager mock with the given full_name and correct system_type/source/name."""
    parts = full_name.split(":")
    mgr = MagicMock(spec=BaseItemManager)
    mgr.full_name = full_name
    mgr.close = AsyncMock()
    mgr.system_type = MagicMock()
    mgr.system_type.value = parts[0] if len(parts) >= 1 else "community"
    mgr.source = parts[1] if len(parts) >= 2 else ""
    mgr.name = parts[2] if len(parts) >= 3 else ""
    return mgr


# --- isinstance checks ---


def test_mutable_registry_is_base_registry():
    """MutableSessionRegistry is a BaseRegistry."""
    assert isinstance(_MutableRegistryImpl(), BaseRegistry)


# --- MutableSessionRegistry.close() clears _added_session_ids ---


@pytest.mark.asyncio
async def test_mutable_registry_close_clears_added_session_ids():
    """close() clears _added_session_ids so the registry is clean for reuse."""
    registry = _make_initialized_mutable_registry()
    mgr = _make_mock_manager("community:default:s1")
    registry._items["community:default:s1"] = mgr
    registry._added_session_ids = {"community:default:s1"}

    await registry.close()

    assert registry._added_session_ids == set()
    assert not registry._initialized


@pytest.mark.asyncio
async def test_mutable_registry_add_session_success():
    """add_session() stores manager and tracks its id."""
    registry = _make_initialized_mutable_registry()
    mgr = _make_mock_manager("community:default:mysession")

    await registry.add_session(mgr)

    assert "community:default:mysession" in registry._items
    assert "community:default:mysession" in registry._added_session_ids


@pytest.mark.asyncio
async def test_mutable_registry_add_session_duplicate_raises():
    """add_session() raises ValueError if session already exists."""
    registry = _make_initialized_mutable_registry()
    mgr = _make_mock_manager("community:default:dup")
    registry._items["community:default:dup"] = mgr

    with pytest.raises(ValueError, match="already exists"):
        await registry.add_session(mgr)


@pytest.mark.asyncio
async def test_mutable_registry_add_session_not_initialized_raises():
    """add_session() raises InternalError if registry not initialized."""
    registry = _MutableRegistryImpl()  # not initialized
    mgr = _make_mock_manager("community:default:s")

    with pytest.raises(InternalError):
        await registry.add_session(mgr)


@pytest.mark.asyncio
async def test_mutable_registry_remove_session_success():
    """remove_session() removes and returns the manager, discards tracking id."""
    registry = _make_initialized_mutable_registry()
    mgr = _make_mock_manager("community:default:tosremove")
    registry._items["community:default:tosremove"] = mgr
    registry._added_session_ids.add("community:default:tosremove")

    result = await registry.remove_session("community:default:tosremove")

    assert result is mgr
    assert "community:default:tosremove" not in registry._items
    assert "community:default:tosremove" not in registry._added_session_ids


@pytest.mark.asyncio
async def test_mutable_registry_remove_session_not_found_returns_none():
    """remove_session() returns None for a non-existent session (idempotent)."""
    registry = _make_initialized_mutable_registry()

    result = await registry.remove_session("community:default:ghost")
    assert result is None


@pytest.mark.asyncio
async def test_mutable_registry_remove_session_not_initialized_raises():
    """remove_session() raises InternalError if registry not initialized."""
    registry = _MutableRegistryImpl()

    with pytest.raises(InternalError):
        await registry.remove_session("community:default:s")


@pytest.mark.asyncio
async def test_mutable_registry_count_added_sessions_zero():
    """count_added_sessions() returns 0 when no sessions tracked."""
    registry = _make_initialized_mutable_registry()

    count = await registry.count_added_sessions(SystemType.COMMUNITY, "default")
    assert count == 0


@pytest.mark.asyncio
async def test_mutable_registry_count_added_sessions_counts_correctly():
    """count_added_sessions() counts matching sessions that are still in _items."""
    registry = _make_initialized_mutable_registry()

    mgr1 = _make_mock_manager("community:default:s1")
    mgr2 = _make_mock_manager("community:default:s2")
    mgr3 = _make_mock_manager("community:other:s3")

    registry._items["community:default:s1"] = mgr1
    registry._items["community:default:s2"] = mgr2
    registry._items["community:other:s3"] = mgr3
    registry._added_session_ids = {
        "community:default:s1",
        "community:default:s2",
        "community:other:s3",
    }

    assert await registry.count_added_sessions(SystemType.COMMUNITY, "default") == 2
    assert await registry.count_added_sessions(SystemType.COMMUNITY, "other") == 1


@pytest.mark.asyncio
async def test_mutable_registry_count_added_sessions_excludes_removed():
    """count_added_sessions() does not count sessions removed from _items."""
    registry = _make_initialized_mutable_registry()

    mgr = _make_mock_manager("community:default:s1")
    registry._items["community:default:s1"] = mgr
    registry._added_session_ids = {"community:default:s1", "community:default:s2"}
    # s2 is tracked but not in _items (removed without cleanup)

    count = await registry.count_added_sessions(SystemType.COMMUNITY, "default")
    assert count == 1


@pytest.mark.asyncio
async def test_mutable_registry_count_added_sessions_not_initialized_raises():
    """count_added_sessions() raises InternalError if registry not initialized."""
    registry = _MutableRegistryImpl()

    with pytest.raises(InternalError):
        await registry.count_added_sessions(SystemType.COMMUNITY, "default")


@pytest.mark.asyncio
async def test_mutable_registry_count_added_sessions_malformed_id_raises():
    """count_added_sessions() raises InternalError for malformed IDs in _added_session_ids."""
    registry = _make_initialized_mutable_registry()
    registry._added_session_ids = {"badkey"}  # no colons — invalid full_name

    with pytest.raises(InternalError, match="Malformed session ID"):
        await registry.count_added_sessions(SystemType.COMMUNITY, "default")
