from unittest.mock import AsyncMock, patch

import pytest

from deephaven_mcp import client
from deephaven_mcp.session_manager._manager import CorePlusSessionFactoryManager


@pytest.mark.asyncio
@patch("deephaven_mcp.client.CorePlusSessionFactory.from_config", new_callable=AsyncMock)
async def test_create_item(mock_from_config):
    """Test that _create_item correctly calls the factory's from_config method."""
    mock_factory = AsyncMock(spec=client.CorePlusSessionFactory)
    mock_from_config.return_value = mock_factory

    config = {"host": "localhost"}
    manager = CorePlusSessionFactoryManager(name="test_factory", config=config)

    created_factory = await manager._create_item()

    assert created_factory is mock_factory
    mock_from_config.assert_awaited_once_with(config)


@pytest.mark.asyncio
async def test_check_liveness():
    """Test that _check_liveness correctly calls the item's ping method."""
    mock_factory = AsyncMock(spec=client.CorePlusSessionFactory)
    manager = CorePlusSessionFactoryManager(name="test_factory", config={})

    # Test when ping returns True
    mock_factory.ping.return_value = True
    assert await manager._check_liveness(mock_factory) is True
    mock_factory.ping.assert_awaited_once()

    # Test when ping returns False
    mock_factory.ping.reset_mock()
    mock_factory.ping.return_value = False
    assert await manager._check_liveness(mock_factory) is False
    mock_factory.ping.assert_awaited_once()
