"""
Unit tests for Session Manager classes.
"""

import asyncio
from unittest.mock import AsyncMock, Mock, MagicMock, call, patch

import pytest

from deephaven_mcp.client import CorePlusSession
from deephaven_mcp._exceptions import InternalError, SessionCreationError
from deephaven_mcp.session_manager import (
    BaseItemManager,
    CommunitySessionManager,
    EnterpriseSessionManager,
    SystemType,
)


# Base Item Manager Tests

class MockItem:
    """A mock item with async methods for testing."""

    def __init__(self):
        self.is_alive = AsyncMock(return_value=True)
        self.close = AsyncMock()


class MockSyncItem:
    """A mock item with a synchronous close method."""

    def __init__(self):
        self.is_alive = AsyncMock(return_value=True)
        self.close = MagicMock()


class ConcreteItemManager(BaseItemManager[MockItem]):
    """A concrete implementation of BaseItemManager for testing."""

    def __init__(self, system_type: SystemType, source: str, name: str):
        super().__init__(system_type, source, name)
        self._create_item_mock = AsyncMock(return_value=MockItem())

    async def _create_item(self) -> MockItem:
        return await self._create_item_mock()

    async def _check_liveness(self, item: MockItem) -> bool:
        return await item.is_alive()


@pytest.mark.asyncio
async def test_properties():
    """Test the basic properties of the manager."""
    manager = ConcreteItemManager(
        system_type=SystemType.COMMUNITY,
        source="test_source",
        name="test_manager",
    )
    assert manager.name == "test_manager"
    assert manager.system_type == SystemType.COMMUNITY
    assert manager.source == "test_source"
    assert manager.full_name == "community:test_source:test_manager"


@pytest.mark.asyncio
async def test_get_lazy_creation():
    """Test that the item is created lazily on the first get call."""
    manager = ConcreteItemManager(SystemType.COMMUNITY, "test-source", "test")
    assert manager._item_cache is None

    # First call should create the item
    item1 = await manager.get()
    assert item1 is not None
    manager._create_item_mock.assert_called_once()
    assert manager._item_cache == item1

    # Second call should return the cached item
    item2 = await manager.get()
    assert item2 == item1
    manager._create_item_mock.assert_called_once()  # Still called only once


@pytest.mark.asyncio
async def test_is_alive():
    """Test the is_alive method."""
    manager = ConcreteItemManager(SystemType.COMMUNITY, "test-source", "test")

    # Not alive if no item is cached
    assert not await manager.is_alive()

    item = await manager.get()
    item.is_alive.return_value = True
    assert await manager.is_alive()
    item.is_alive.assert_called_once()

    item.is_alive.return_value = False
    assert not await manager.is_alive()


@pytest.mark.asyncio
async def test_is_alive_exception():
    """Test that is_alive handles exceptions gracefully."""
    manager = ConcreteItemManager(SystemType.COMMUNITY, "test-source", "test")
    item = await manager.get()
    item.is_alive.side_effect = Exception("Liveness check failed")
    assert not await manager.is_alive()


@pytest.mark.asyncio
async def test_close():
    """Test the close method."""
    manager = ConcreteItemManager(SystemType.COMMUNITY, "test-source", "test")
    item = await manager.get()

    await manager.close()
    item.close.assert_called_once()
    assert manager._item_cache is None

    # Test idempotency
    await manager.close()
    item.close.assert_called_once()  # Still called only once


@pytest.mark.asyncio
async def test_concurrent_get():
    """Test that get is thread-safe and creates only one item."""
    manager = ConcreteItemManager(SystemType.COMMUNITY, "test-source", "test")

    # Simulate concurrent calls to get()
    tasks = [manager.get() for _ in range(10)]
    results = await asyncio.gather(*tasks)

    # Check that the create method was called only once
    manager._create_item_mock.assert_called_once()

    # Check that all results are the same instance
    first_item = results[0]
    for item in results[1:]:
        assert item is first_item


@pytest.mark.asyncio
async def test_close_raises_on_sync_method():
    """Test that close raises InternalError for a synchronous close method."""

    class ConcreteSyncItemManager(BaseItemManager[MockSyncItem]):
        def __init__(self, system_type: SystemType, source: str, name: str):
            super().__init__(system_type, source, name)
            self._create_item_mock = AsyncMock(return_value=MockSyncItem())

        async def _create_item(self) -> MockSyncItem:
            return await self._create_item_mock()

        async def _check_liveness(self, item: MockSyncItem) -> bool:
            # Ensure liveness check passes so close() proceeds
            return True

    manager = ConcreteSyncItemManager(SystemType.COMMUNITY, "test_sync", "test_sync")
    await manager.get()

    with pytest.raises(InternalError, match="is not a coroutine function"):
        await manager.close()


# Session Manager Tests

class TestCommunitySessionManager:
    """Tests for the CommunitySessionManager class."""

    @pytest.mark.asyncio
    @patch("deephaven_mcp.client.CoreSession.from_config")
    async def test_create_item(self, mock_from_config):
        """Test that _create_item correctly calls CoreSession.from_config."""
        mock_from_config.return_value = "mock_session"
        manager = CommunitySessionManager(
            name="test_community",
            config={"host": "localhost"},
        )
        session = await manager._create_item()
        mock_from_config.assert_awaited_once_with({"host": "localhost"})
        assert session == "mock_session"

    @pytest.mark.asyncio
    @patch("deephaven_mcp.client.CoreSession.from_config")
    async def test_create_item_raises_exception(self, mock_from_config):
        """Test that _create_item raises SessionCreationError on failure."""
        mock_from_config.side_effect = Exception("Connection failed")
        manager = CommunitySessionManager(
            name="test_community",
            config={},
        )
        with pytest.raises(SessionCreationError, match="Connection failed"):
            await manager._create_item()

    @pytest.mark.asyncio
    async def test_check_liveness(self):
        """Test that _check_liveness correctly calls the session's is_alive method."""
        manager = CommunitySessionManager(
            name="test_community",
            config={},
        )
        mock_session = AsyncMock()
        mock_session.is_alive.return_value = True
        result = await manager._check_liveness(mock_session)
        mock_session.is_alive.assert_awaited_once()
        assert result is True


class TestEnterpriseSessionManager:
    """Tests for the EnterpriseSessionManager class."""

    @pytest.mark.asyncio
    async def test_create_item_raises_not_implemented(self):
        """Test that _create_item raises NotImplementedError."""
        manager = EnterpriseSessionManager(
            source="test_source",
            name="test_enterprise",
            factory=AsyncMock(),
        )
        with pytest.raises(NotImplementedError):
            await manager._create_item()

    @pytest.mark.asyncio
    async def test_get_raises_not_implemented(self):
        """Test that get() raises NotImplementedError because _create_item is not implemented."""
        manager = EnterpriseSessionManager(
            source="test_source",
            name="test_enterprise",
            factory=AsyncMock(),
        )
        with pytest.raises(NotImplementedError):
            await manager.get()

    @pytest.mark.asyncio
    async def test_close(self):
        """Test that close correctly closes the cached session."""
        manager = EnterpriseSessionManager(
            source="test_source",
            name="test_enterprise",
            factory=AsyncMock(),
        )
        mock_session = AsyncMock(spec=CorePlusSession)

        # Manually set the cached item to bypass get()
        manager._item_cache = mock_session

        # Mock _check_liveness to return True so close proceeds
        with patch.object(
            manager, "_check_liveness", new_callable=AsyncMock, return_value=True
        ) as mock_check:
            await manager.close()
            mock_check.assert_awaited_once_with(mock_session)

        # Verify the session's close method was called
        mock_session.close.assert_awaited_once()
        # Verify the cache is cleared
        assert manager._item_cache is None

    @pytest.mark.asyncio
    async def test_check_liveness(self):
        """Test that _check_liveness correctly calls the session's is_alive method."""
        mock_factory = AsyncMock()
        manager = EnterpriseSessionManager(
            source="test_source",
            name="test_enterprise",
            factory=mock_factory,
        )
        mock_session = AsyncMock(spec=CorePlusSession)
        mock_session.is_alive.return_value = True

        result = await manager._check_liveness(mock_session)

        mock_session.is_alive.assert_awaited_once()
        assert result is True

        # Test when is_alive returns False
        mock_session.is_alive.reset_mock()
        mock_session.is_alive.return_value = False
        assert await manager._check_liveness(mock_session) is False
        mock_session.is_alive.assert_awaited_once()
