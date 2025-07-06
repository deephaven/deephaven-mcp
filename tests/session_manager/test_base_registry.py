"""
Tests for the BaseRegistry abstract base class.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from deephaven_mcp import config
from deephaven_mcp._exceptions import InternalError
from deephaven_mcp.session_manager._base_registry import BaseRegistry


# --- Mocks and Fixtures ---

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
def mock_config_manager():
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


# --- Test Cases ---

def test_construction(registry):
    """Test that the registry is constructed correctly."""
    assert isinstance(registry, BaseRegistry)
    assert not registry._initialized
    assert len(registry._items) == 0


@pytest.mark.asyncio
async def test_initialize(registry, mock_config_manager):
    """Test that initialize() loads items and sets the initialized flag."""
    await registry.initialize(mock_config_manager)
    assert registry._initialized
    assert len(registry._items) == 2
    assert "item1" in registry._items
    assert registry._items["item1"].name == "alpha"

    # Test idempotency
    await registry.initialize(mock_config_manager)
    assert len(registry._items) == 2


@pytest.mark.asyncio
async def test_methods_raise_before_initialize(registry):
    """Test that get() and close() raise InternalError before initialization."""
    with pytest.raises(InternalError, match="ConcreteRegistry not initialized"):
        await registry.get("item1")

    with pytest.raises(InternalError, match="ConcreteRegistry not initialized"):
        await registry.close()


@pytest.mark.asyncio
async def test_get_returns_item(registry, mock_config_manager):
    """Test that get() returns the correct item after initialization."""
    await registry.initialize(mock_config_manager)
    item = await registry.get("item1")
    assert isinstance(item, MockItem)
    assert item.name == "alpha"


@pytest.mark.asyncio
async def test_get_unknown_raises_key_error(registry, mock_config_manager):
    """Test that get() raises KeyError for an unknown item."""
    await registry.initialize(mock_config_manager)
    with pytest.raises(KeyError, match="No item found for: unknown_item"):
        await registry.get("unknown_item")


@pytest.mark.asyncio
async def test_close_calls_close_on_items(registry, mock_config_manager):
    """Test that close() calls the close() method on all managed items."""
    await registry.initialize(mock_config_manager)

    item1 = await registry.get("item1")
    item2 = await registry.get("item2")

    await registry.close()

    item1.close.assert_awaited_once()
    item2.close.assert_awaited_once()

    # Ensure registry state is maintained after close
    assert registry._initialized
    assert len(registry._items) == 2
