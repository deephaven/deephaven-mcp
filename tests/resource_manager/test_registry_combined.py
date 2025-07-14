"""
Tests for CombinedSessionRegistry.

This module contains comprehensive tests for the CombinedSessionRegistry class, which
manages both community and enterprise session registries with proper BaseRegistry inheritance,
controller client caching, and lifecycle management.
"""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from deephaven_mcp._exceptions import DeephavenConnectionError, InternalError
from deephaven_mcp.client import CorePlusControllerClient
from deephaven_mcp.config import ConfigManager
from deephaven_mcp.resource_manager import (
    CommunitySessionRegistry,
    CorePlusSessionFactoryRegistry,
    SystemType,
)
from deephaven_mcp.resource_manager._manager import (
    BaseItemManager,
    CorePlusSessionFactoryManager,
    EnterpriseSessionManager,
)
from deephaven_mcp.resource_manager._registry_combined import CombinedSessionRegistry


@pytest.fixture
def mock_config_manager():
    """Create a mock config manager for testing."""
    config_manager = MagicMock(spec=ConfigManager)
    return config_manager


@pytest.fixture
def combined_registry():
    """Create a CombinedSessionRegistry instance for testing."""
    return CombinedSessionRegistry()


@pytest.fixture
def initialized_registry(combined_registry, mock_config_manager):
    """Create a properly initialized CombinedSessionRegistry instance for testing."""
    # Set up mocks for initialization
    combined_registry._community_registry = MagicMock(spec=CommunitySessionRegistry)
    combined_registry._community_registry.initialize = AsyncMock()

    combined_registry._enterprise_registry = MagicMock(
        spec=CorePlusSessionFactoryRegistry
    )
    combined_registry._enterprise_registry.initialize = AsyncMock()

    # Mark as initialized
    combined_registry._initialized = True

    return combined_registry


class TestCombinedSessionRegistryConstruction:
    """Test CombinedSessionRegistry construction and basic properties."""

    def test_construction(self, combined_registry):
        """Test that CombinedSessionRegistry can be constructed."""
        assert combined_registry is not None
        assert combined_registry._community_registry is None
        assert combined_registry._enterprise_registry is None
        assert combined_registry._controller_clients == {}

    def test_inherits_from_base_registry(self, combined_registry):
        """Test that CombinedSessionRegistry properly inherits from BaseRegistry."""
        # Should have base registry attributes
        assert hasattr(combined_registry, "_lock")
        assert hasattr(combined_registry, "_items")
        assert hasattr(combined_registry, "_initialized")


class TestCombinedSessionRegistryInitialization:
    """Test CombinedSessionRegistry initialization."""

    @pytest.mark.asyncio
    async def test_initialize_success(self, combined_registry, mock_config_manager):
        """Test successful initialization of both registries."""
        # Patch the registries and update_enterprise_sessions
        with (
            patch.object(
                CommunitySessionRegistry,
                "__new__",
                return_value=MagicMock(spec=CommunitySessionRegistry),
            ) as mock_community_cls,
            patch.object(
                CorePlusSessionFactoryRegistry,
                "__new__",
                return_value=MagicMock(spec=CorePlusSessionFactoryRegistry),
            ) as mock_enterprise_cls,
            patch.object(
                combined_registry, "_update_enterprise_sessions", AsyncMock()
            ) as mock_update,
        ):

            # Set up mocks for initialize
            mock_community_registry = mock_community_cls.return_value
            mock_community_registry.initialize = AsyncMock()

            mock_enterprise_registry = mock_enterprise_cls.return_value
            mock_enterprise_registry.initialize = AsyncMock()

            # Call initialize
            await combined_registry.initialize(mock_config_manager)

            # Allow the event loop to process any pending tasks
            await asyncio.sleep(0)

            # Verify correct calls were made
            mock_community_registry.initialize.assert_called_once_with(
                mock_config_manager
            )
            mock_enterprise_registry.initialize.assert_called_once_with(
                mock_config_manager
            )
            mock_update.assert_awaited_once()

            # Verify registry was marked as initialized
            assert combined_registry._initialized is True

    @pytest.mark.asyncio
    async def test_initialize_community_failure(
        self, combined_registry, mock_config_manager
    ):
        """Test handling of community registry initialization failure."""
        # Patch the registries and update_enterprise_sessions
        combined_registry._update_enterprise_sessions = AsyncMock()
        mock_update = combined_registry._update_enterprise_sessions

        with (
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CommunitySessionRegistry"
            ) as mock_community_cls,
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CorePlusSessionFactoryRegistry"
            ) as mock_enterprise_cls,
        ):

            # Set up community registry to fail initialization
            mock_community_registry = mock_community_cls.return_value
            mock_community_registry.initialize = AsyncMock(
                side_effect=Exception("Community initialization failed")
            )

            # Set up enterprise registry
            mock_enterprise_registry = mock_enterprise_cls.return_value
            mock_enterprise_registry.initialize = AsyncMock()

            # Try to initialize the registry (should raise)
            with pytest.raises(Exception, match="Community initialization failed"):
                await combined_registry.initialize(mock_config_manager)

            # Verify registries were created but only community was initialized
            mock_community_registry.initialize.assert_awaited_once_with(
                mock_config_manager
            )
            mock_enterprise_registry.initialize.assert_not_called()
            mock_update.assert_not_awaited()

            # Verify registry was not marked as initialized
            assert combined_registry._initialized is False

    @pytest.mark.asyncio
    async def test_initialize_enterprise_failure(
        self, combined_registry, mock_config_manager
    ):
        """Test handling of enterprise registry initialization failure."""
        # Patch the registries and update_enterprise_sessions
        with (
            patch.object(combined_registry, "_community_registry", None),
            patch.object(combined_registry, "_enterprise_registry", None),
            patch.object(
                combined_registry, "_update_enterprise_sessions", AsyncMock()
            ) as mock_update,
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CommunitySessionRegistry"
            ) as mock_community_cls,
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CorePlusSessionFactoryRegistry"
            ) as mock_enterprise_cls,
        ):

            # Set up community registry
            mock_community_registry = mock_community_cls.return_value
            mock_community_registry.initialize = AsyncMock()

            # Set up enterprise registry to fail initialization
            mock_enterprise_registry = mock_enterprise_cls.return_value
            mock_enterprise_registry.initialize = AsyncMock(
                side_effect=Exception("Enterprise initialization failed")
            )

            # Try to initialize the registry (should raise)
            with pytest.raises(Exception, match="Enterprise initialization failed"):
                await combined_registry.initialize(mock_config_manager)

            # Verify registries were created and initialize was attempted
            mock_community_registry.initialize.assert_awaited_once_with(
                mock_config_manager
            )
            mock_enterprise_registry.initialize.assert_awaited_once_with(
                mock_config_manager
            )

            # Verify enterprise sessions were not updated
            mock_update.assert_not_awaited()

            # Verify registry was not marked as initialized
            assert combined_registry._initialized is False

    @pytest.mark.asyncio
    async def test_double_initialization_warning(
        self, combined_registry, mock_config_manager
    ):
        """Test that initializing an already initialized registry is handled properly."""
        # First initialization
        with (
            patch.object(
                CommunitySessionRegistry,
                "__new__",
                return_value=MagicMock(spec=CommunitySessionRegistry),
            ) as mock_community_cls,
            patch.object(
                CorePlusSessionFactoryRegistry,
                "__new__",
                return_value=MagicMock(spec=CorePlusSessionFactoryRegistry),
            ) as mock_enterprise_cls,
            patch.object(
                combined_registry, "_update_enterprise_sessions", AsyncMock()
            ) as mock_update,
        ):

            mock_community_registry = mock_community_cls.return_value
            mock_community_registry.initialize = AsyncMock()

            mock_enterprise_registry = mock_enterprise_cls.return_value
            mock_enterprise_registry.initialize = AsyncMock()

            await combined_registry.initialize(mock_config_manager)

        # Second initialization should be a no-op
        with patch.object(
            combined_registry, "_update_enterprise_sessions", AsyncMock()
        ) as mock_update:
            with patch(
                "deephaven_mcp.resource_manager._registry_combined._LOGGER.warning"
            ) as mock_warn:
                await combined_registry.initialize(mock_config_manager)

                # Verify warning was logged
                assert mock_warn.called

                # Verify enterprise sessions were not updated again
                mock_update.assert_not_awaited()


class TestCombinedSessionRegistryAccessors:
    """Test CombinedSessionRegistry registry accessor methods."""

    @pytest.mark.asyncio
    async def test_community_registry_not_initialized(self, combined_registry):
        """Test that accessing community registry before initialization raises InternalError."""
        with pytest.raises(InternalError):
            await combined_registry.community_registry()

    @pytest.mark.asyncio
    async def test_enterprise_registry_not_initialized(self, combined_registry):
        """Test that accessing enterprise registry before initialization raises InternalError."""
        with pytest.raises(InternalError):
            await combined_registry.enterprise_registry()

    @pytest.mark.asyncio
    async def test_community_registry_after_initialize(self, initialized_registry):
        """Test getting community registry after initialization."""
        community_registry = await initialized_registry.community_registry()
        assert community_registry == initialized_registry._community_registry

    @pytest.mark.asyncio
    async def test_enterprise_registry_after_initialize(self, initialized_registry):
        """Test getting enterprise registry after initialization."""
        enterprise_registry = await initialized_registry.enterprise_registry()
        assert enterprise_registry == initialized_registry._enterprise_registry


class TestControllerClientCaching:
    """Test controller client caching and lifecycle management."""

    @pytest.mark.asyncio
    async def test_get_or_create_controller_client_new(self, combined_registry):
        """Test creating a new controller client when none exists."""
        # Setup mocks
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory_instance = MagicMock()
        mock_client = MagicMock(spec=CorePlusControllerClient)

        # Mock successful client creation
        mock_factory.get = AsyncMock(return_value=mock_factory_instance)
        mock_factory_instance.create_controller_client = AsyncMock(
            return_value=mock_client
        )
        mock_client.subscribe = AsyncMock()

        # Call the method
        client = await combined_registry._get_or_create_controller_client(
            mock_factory, "test_factory"
        )

        # Verify client was created and cached
        mock_factory.get.assert_awaited_once()
        mock_factory_instance.create_controller_client.assert_awaited_once()
        mock_client.subscribe.assert_awaited_once()
        assert client == mock_client
        assert combined_registry._controller_clients["test_factory"] == mock_client
        assert client == mock_client

    @pytest.mark.asyncio
    async def test_get_or_create_controller_client_reuse_healthy(
        self, combined_registry
    ):
        """Test reusing an existing healthy controller client."""
        # Setup mocks
        mock_client = MagicMock(spec=CorePlusControllerClient)
        mock_client.ping = AsyncMock(return_value=True)

        # First create a client and add to cache
        combined_registry._controller_clients["test_factory"] = mock_client

        # Mock the CorePlusControllerClient import directly
        with patch(
            "deephaven_mcp.resource_manager._registry_combined.CorePlusControllerClient",
            MagicMock(),
        ) as mock_controller_client_cls:
            # Mock client creation for the factory
            mock_controller_client_cls.return_value = MagicMock(
                spec=CorePlusControllerClient
            )

            # Set up the factory
            factory = MagicMock()

            # Call should reuse the client since it's healthy
            client = await combined_registry._get_or_create_controller_client(
                factory, "test_factory"
            )

            # Verify the client was reused not recreated
            mock_controller_client_cls.assert_not_called()
            assert client == mock_client
            mock_client.ping.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_or_create_controller_client_recreate_dead(
        self, combined_registry
    ):
        """Test recreating a controller client when the cached one is dead."""
        # Setup mocks
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory_instance = MagicMock()
        mock_old_client = MagicMock(spec=CorePlusControllerClient)
        mock_new_client = MagicMock(spec=CorePlusControllerClient)

        # Pre-populate cache with a dead client
        combined_registry._controller_clients["test_factory"] = mock_old_client

        # Mock failed health check (False result) and successful recreation
        mock_old_client.ping = AsyncMock(return_value=False)
        mock_old_client.close = AsyncMock()

        mock_factory.get = AsyncMock(return_value=mock_factory_instance)
        mock_factory_instance.create_controller_client = AsyncMock(
            return_value=mock_new_client
        )
        mock_new_client.subscribe = AsyncMock()

        # Call the method
        client = await combined_registry._get_or_create_controller_client(
            mock_factory, "test_factory"
        )

        # Verify old client was closed and new client was created
        mock_old_client.close.assert_awaited_once()
        mock_factory.get.assert_awaited_once()
        mock_factory_instance.create_controller_client.assert_awaited_once()
        mock_new_client.subscribe.assert_awaited_once()
        assert client == mock_new_client
        assert combined_registry._controller_clients["test_factory"] == mock_new_client
        assert client == mock_new_client

    @pytest.mark.asyncio
    async def test_get_or_create_controller_client_close_exception(
        self, combined_registry
    ):
        """Test handling of exceptions when closing a dead controller client."""
        # Setup mocks
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory_instance = MagicMock()
        mock_old_client = MagicMock(spec=CorePlusControllerClient)
        mock_new_client = MagicMock(spec=CorePlusControllerClient)

        # Pre-populate cache with a dead client
        combined_registry._controller_clients["test_factory"] = mock_old_client

        # Mock failed health check (False result) and successful recreation
        mock_old_client.ping = AsyncMock(return_value=False)
        mock_old_client.close = AsyncMock(
            side_effect=RuntimeError("Failed to close client")
        )

        mock_factory.get = AsyncMock(return_value=mock_factory_instance)
        mock_factory_instance.create_controller_client = AsyncMock(
            return_value=mock_new_client
        )
        mock_new_client.subscribe = AsyncMock()

        # Call the method
        client = await combined_registry._get_or_create_controller_client(
            mock_factory, "test_factory"
        )

        # Verify old client was closed and new client was created
        mock_old_client.close.assert_awaited_once()
        mock_factory.get.assert_awaited_once()
        mock_factory_instance.create_controller_client.assert_awaited_once()
        mock_new_client.subscribe.assert_awaited_once()

        # Verify cache was updated
        assert combined_registry._controller_clients["test_factory"] == mock_new_client
        assert client == mock_new_client


class TestEnterpriseSessionUpdate:
    """Test the _update_enterprise_sessions method."""

    @pytest.mark.asyncio
    async def test_update_enterprise_sessions_not_initialized(self, combined_registry):
        """Test that updating enterprise sessions before initialization raises InternalError."""
        with pytest.raises(InternalError):
            await combined_registry._update_enterprise_sessions()

    @pytest.mark.asyncio
    async def test_update_enterprise_sessions_success(self, initialized_registry):
        """Test successful enterprise session update."""
        # Setup mocks
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_client = MagicMock(spec=CorePlusControllerClient)
        mock_session = MagicMock(spec=EnterpriseSessionManager)

        # Mock enterprise registry response
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value={"factory1": mock_factory}
        )

        # Make sure the _items dict exists
        initialized_registry._items = {}

        # Mock controller client
        with patch.object(
            initialized_registry,
            "_get_or_create_controller_client",
            AsyncMock(return_value=mock_client),
        ):
            # Mock session map from controller with proper structure
            mock_session_info = MagicMock()
            mock_session_info.config.pb.name = "session1"
            mock_client.map = AsyncMock(return_value={"session1": mock_session_info})
            mock_client.ping = AsyncMock(return_value=True)

            # Mock session creation
            with patch.object(
                CombinedSessionRegistry,
                "_make_enterprise_session_manager",
                return_value=mock_session,
            ) as mock_make_session:
                # Update sessions
                await initialized_registry._update_enterprise_sessions()

                # Verify session was created and added
                mock_make_session.assert_called_once_with(
                    mock_factory, "factory1", "session1"
                )

                # Verify session was added to items using the session's full_name property
                assert (
                    initialized_registry._items[mock_session.full_name] == mock_session
                )

    @pytest.mark.asyncio
    async def test_update_enterprise_sessions_reuses_existing(
        self, initialized_registry
    ):
        """Test that _update_enterprise_sessions reuses existing session managers (covers line 401)."""
        # Setup mock factory and client
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory.name = "factory1"

        # Mock get_all to return our mock factory
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value={"factory1": mock_factory}
        )

        # Mock controller client
        mock_client = AsyncMock(spec=CorePlusControllerClient)
        mock_client.ping = AsyncMock(return_value=True)

        # Setup existing session manager that will be reused
        session_key = "enterprise/factory1/session1"
        session_manager = AsyncMock(spec=EnterpriseSessionManager)
        session_manager.full_name = session_key

        # Add existing session to registry
        initialized_registry._items = {session_key: session_manager}

        # Create a spy on _make_enterprise_session_manager to track when it's called
        with patch.object(
            initialized_registry,
            "_make_enterprise_session_manager",
            wraps=initialized_registry._make_enterprise_session_manager,
        ) as make_spy:

            # Mock controller client's map() to return sessions including our existing one
            mock_session_info = MagicMock()
            mock_session_info.config.pb.name = "session1"
            mock_client.map = AsyncMock(return_value={"session1": mock_session_info})

            # Create a controller client to be reused
            initialized_registry._controller_clients = {"factory1": mock_client}

            # Run update sessions
            await initialized_registry._update_enterprise_sessions()

            # Verify session is still in registry
            assert session_key in initialized_registry._items
            assert initialized_registry._items[session_key] == session_manager

            # Verify make_session wasn't called for our existing session
            for call in make_spy.mock_calls:
                # Extract the session_name from the call args
                # The order is (factory, factory_name, session_name)
                if len(call[1]) >= 3 and call[1][2] == "session1":
                    assert (
                        False
                    ), "_make_enterprise_session_manager was called for session1 when it should be reused"

            # The method might be called for other sessions, but not for our reused session

    @pytest.mark.asyncio
    async def test_close_stale_sessions_handles_exception(self, initialized_registry):
        """Test that _close_stale_enterprise_sessions removes a manager even if close() fails."""
        # Arrange
        stale_key = "enterprise/factory1/stale_session"
        mock_manager = MagicMock(spec=EnterpriseSessionManager)
        mock_manager.close = AsyncMock(side_effect=RuntimeError("Close failed"))
        initialized_registry._items = {stale_key: mock_manager}

        # Act
        # Directly call the method we want to test
        await initialized_registry._close_stale_enterprise_sessions({stale_key})

        # Assert
        # The manager should be removed from the registry despite the error.
        assert stale_key not in initialized_registry._items
        # The close method should have been called.
        mock_manager.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_stale_sessions_ignores_nonexistent_key(
        self, initialized_registry
    ):
        """Test that _close_stale_enterprise_sessions handles keys that are not in the registry."""
        # Arrange
        non_existent_key = "enterprise/factory1/non_existent_session"
        initialized_registry._items = {}

        # Act
        # Call the method with a key that doesn't exist. It should not raise an error.
        await initialized_registry._close_stale_enterprise_sessions({non_existent_key})

        # Assert
        # No assertion needed, the test passes if no exception is raised.
        pass

    @pytest.mark.asyncio
    async def test_update_enterprise_sessions_removes_stale(self, initialized_registry):
        """Test that update removes stale enterprise sessions."""
        # Setup mocks
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_client = MagicMock(spec=CorePlusControllerClient)
        mock_session = MagicMock(spec=EnterpriseSessionManager)
        mock_old_session = MagicMock(spec=EnterpriseSessionManager)

        # Mock enterprise registry response
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value={"factory1": mock_factory}
        )

        # Add a "stale" session that should be removed
        stale_key = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, "factory1", "old_session"
        )
        initialized_registry._items[stale_key] = mock_old_session
        mock_old_session.close = AsyncMock()

        # Mock controller client
        with patch.object(
            initialized_registry,
            "_get_or_create_controller_client",
            AsyncMock(return_value=mock_client),
        ):
            # Mock session map from controller that doesn't include the stale session
            mock_client.map = AsyncMock(return_value={"session1": {"id": "123"}})


def test_add_new_enterprise_sessions(initialized_registry):
    """Test that new session managers are created for new sessions."""
    # Arrange
    factory = MagicMock(spec=CorePlusSessionFactoryManager)
    factory_name = "factory1"
    new_sessions = {"session1", "session2"}
    initialized_registry._items = {}

    def mock_make_manager_side_effect(factory, factory_name, session_name):
        manager = MagicMock(spec=EnterpriseSessionManager)
        manager.full_name = f"enterprise/{factory_name}/{session_name}"
        return manager

    with patch.object(
        initialized_registry,
        "_make_enterprise_session_manager",
        side_effect=mock_make_manager_side_effect,
    ) as mock_make_manager:
        # Act
        initialized_registry._add_new_enterprise_sessions(
            factory, factory_name, new_sessions
        )

        # Assert
        assert mock_make_manager.call_count == 2
        assert "enterprise/factory1/session1" in initialized_registry._items
        assert "enterprise/factory1/session2" in initialized_registry._items


def test_add_new_enterprise_sessions_skips_existing(initialized_registry):
    """Test that existing session managers are not recreated."""
    # Arrange
    factory = MagicMock(spec=CorePlusSessionFactoryManager)
    factory_name = "factory1"
    # session1 already exists, session2 is new
    sessions = {"session1", "session2"}
    existing_key = "enterprise/factory1/session1"
    initialized_registry._items = {existing_key: MagicMock()}

    def mock_make_manager_side_effect(factory, factory_name, session_name):
        manager = MagicMock(spec=EnterpriseSessionManager)
        manager.full_name = f"enterprise/{factory_name}/{session_name}"
        return manager

    with patch.object(
        initialized_registry,
        "_make_enterprise_session_manager",
        side_effect=mock_make_manager_side_effect,
    ) as mock_make_manager:
        # Act
        initialized_registry._add_new_enterprise_sessions(
            factory, factory_name, sessions
        )

        # Assert
        # The creation method was only called for the new session.
        mock_make_manager.assert_called_once_with(factory, factory_name, "session2")
        assert existing_key in initialized_registry._items
        assert "enterprise/factory1/session2" in initialized_registry._items


@pytest.mark.asyncio
async def test_close_stale_enterprise_sessions(initialized_registry):
    """Test that stale session managers are closed and removed."""
    # Arrange
    stale_key = "enterprise/factory1/stale_session"
    mock_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_manager.close = AsyncMock()
    initialized_registry._items = {stale_key: mock_manager}

    # Act
    await initialized_registry._close_stale_enterprise_sessions({stale_key})

    # Assert
    mock_manager.close.assert_awaited_once()
    assert stale_key not in initialized_registry._items


@pytest.mark.asyncio
async def test_close_stale_enterprise_sessions_handles_exception(initialized_registry):
    """Test that exceptions during close are handled and the manager is still removed."""
    # Arrange
    stale_key = "enterprise/factory1/stale_session"
    mock_manager = MagicMock(spec=EnterpriseSessionManager)
    mock_manager.close = AsyncMock(side_effect=RuntimeError("Close failed"))
    initialized_registry._items = {stale_key: mock_manager}

    # Act
    with patch("logging.Logger.error") as mock_log_error:
        await initialized_registry._close_stale_enterprise_sessions({stale_key})

        # Assert
        mock_manager.close.assert_awaited_once()
        assert stale_key not in initialized_registry._items
        mock_log_error.assert_called_once()


class TestGetAndGetAll:
    """Test get and get_all methods."""

    @pytest.mark.asyncio
    async def test_get_not_initialized(self, combined_registry):
        """Test that get before initialization raises InternalError."""
        with pytest.raises(InternalError):
            await combined_registry.get("community:source:name")

    @pytest.mark.asyncio
    async def test_get_all_not_initialized(self, combined_registry):
        """Test that get_all before initialization raises InternalError."""
        with pytest.raises(InternalError):
            await combined_registry.get_all()

    @pytest.mark.asyncio
    async def test_get_item_not_found(self, initialized_registry):
        """Test that get raises KeyError when item not found."""
        # Set up for _update_enterprise_sessions to be called
        initialized_registry._update_enterprise_sessions = AsyncMock()

        with pytest.raises(KeyError):
            await initialized_registry.get("community:source:nonexistent")

        # Verify enterprise sessions were updated
        initialized_registry._update_enterprise_sessions.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_success(self, initialized_registry):
        """Test successful get after initialization."""
        # Set up for _update_enterprise_sessions to be called
        initialized_registry._update_enterprise_sessions = AsyncMock()

        # Add an item to the registry
        mock_item = MagicMock(spec=BaseItemManager)
        initialized_registry._items["community:source:name"] = mock_item

        # Get the item
        item = await initialized_registry.get("community:source:name")

        # Verify item was returned and enterprise sessions were updated
        assert item == mock_item
        initialized_registry._update_enterprise_sessions.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_all_success(self, initialized_registry):
        """Test successful get_all after initialization."""
        # Set up for _update_enterprise_sessions to be called
        initialized_registry._update_enterprise_sessions = AsyncMock()

        # Add some items to the registry
        mock_item1 = MagicMock(spec=BaseItemManager)
        mock_item2 = MagicMock(spec=BaseItemManager)
        initialized_registry._items["community:source:name1"] = mock_item1
        initialized_registry._items["enterprise:factory:name2"] = mock_item2

        # Get all items
        items = await initialized_registry.get_all()

        # Verify items were returned and enterprise sessions were updated
        assert items == {
            "community:source:name1": mock_item1,
            "enterprise:factory:name2": mock_item2,
        }
        initialized_registry._update_enterprise_sessions.assert_awaited_once()

        # Verify returned dict is a copy
        assert items is not initialized_registry._items


class TestClose:
    """Test close functionality."""

    @pytest.mark.asyncio
    async def test_close_not_initialized(self, combined_registry):
        """Test that close before initialization raises InternalError."""
        with pytest.raises(InternalError):
            await combined_registry.close()

    @pytest.mark.asyncio
    async def test_close_success(self, initialized_registry):
        """Test successful close operation."""
        # Setup mocks
        mock_community_registry = initialized_registry._community_registry
        mock_community_registry.close = AsyncMock()

        mock_enterprise_registry = initialized_registry._enterprise_registry
        mock_enterprise_registry.close = AsyncMock()

        # Add some controller clients
        mock_client1 = MagicMock(spec=CorePlusControllerClient)
        mock_client1.close = AsyncMock()

        mock_client2 = MagicMock(spec=CorePlusControllerClient)
        mock_client2.close = AsyncMock()

        initialized_registry._controller_clients = {
            "factory1": mock_client1,
            "factory2": mock_client2,
        }

        # Close the registry
        await initialized_registry.close()

        # Verify both registries were closed
        mock_community_registry.close.assert_awaited_once()
        mock_enterprise_registry.close.assert_awaited_once()

        # Verify all controller clients were closed
        mock_client1.close.assert_awaited_once()
        mock_client2.close.assert_awaited_once()

        # Verify controller clients dictionary was cleared
        assert initialized_registry._controller_clients == {}

    @pytest.mark.asyncio
    async def test_close_handles_exceptions(self, initialized_registry):
        """Test that close handles exceptions from sub-components."""
        # Setup mocks
        mock_community_registry = initialized_registry._community_registry
        mock_community_registry.close = AsyncMock(
            side_effect=Exception("Community close failed")
        )

        mock_enterprise_registry = initialized_registry._enterprise_registry
        mock_enterprise_registry.close = AsyncMock(
            side_effect=Exception("Enterprise close failed")
        )

        # Add a controller client that also fails to close
        mock_client = MagicMock(spec=CorePlusControllerClient)
        mock_client.close = AsyncMock(side_effect=Exception("Client close failed"))

        initialized_registry._controller_clients = {"factory1": mock_client}

        # Close should complete despite exceptions
        with patch(
            "deephaven_mcp.resource_manager._registry_combined._LOGGER.error"
        ) as mock_error:
            await initialized_registry.close()

            # Verify errors were logged for each component
            assert mock_error.call_count >= 3

        # Verify controller clients dictionary was still cleared
        assert initialized_registry._controller_clients == {}


class TestLoadItems:
    """Test that _load_items correctly raises InternalError."""

    @pytest.mark.asyncio
    async def test_load_items_raises_internal_error(self):
        """Test that _load_items raises InternalError as expected."""
        registry = CombinedSessionRegistry()

        # _load_items should never be called for CombinedSessionRegistry
        with pytest.raises(InternalError):
            await registry._load_items(Mock(spec=ConfigManager))


class TestMakeEnterpriseSessionManager:
    """Test the _make_enterprise_session_manager static method."""

    def test_make_enterprise_session_manager(self):
        """Test that _make_enterprise_session_manager creates correct session manager."""
        # Setup mocks
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory_instance = MagicMock()
        mock_factory.get = AsyncMock(return_value=mock_factory_instance)
        mock_factory_instance.connect_to_persistent_query = AsyncMock()

        # Create session manager
        session_manager = CombinedSessionRegistry._make_enterprise_session_manager(
            mock_factory, "factory1", "session1"
        )

        # Verify session manager was created with correct parameters
        assert isinstance(session_manager, EnterpriseSessionManager)
        assert session_manager._source == "factory1"
        assert session_manager._name == "session1"

        # Verify creation function was set up correctly
        creation_function = session_manager._creation_function
        asyncio.run(creation_function("factory1", "session1"))
        mock_factory.get.assert_awaited_once()
        mock_factory_instance.connect_to_persistent_query.assert_awaited_once_with(
            "session1"
        )
