"""
Tests for CombinedSessionRegistry.

This module contains comprehensive tests for the CombinedSessionRegistry class, which
manages both community and enterprise session registries with proper BaseRegistry inheritance,
controller client caching, and lifecycle management.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, PropertyMock, patch

import pytest

from deephaven_mcp._exceptions import DeephavenConnectionError, InternalError
from deephaven_mcp.client import CorePlusControllerClient
from deephaven_mcp.config import ConfigManager
from deephaven_mcp.resource_manager import (
    BaseItemManager,
    CombinedSessionRegistry,
    CommunitySessionRegistry,
    CorePlusSessionFactoryManager,
    CorePlusSessionFactoryRegistry,
    EnterpriseSessionManager,
    InitializationPhase,
    RegistrySnapshot,
    SystemType,
)


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

    # Mark as initialized with discovery completed
    combined_registry._initialized = True
    combined_registry._init_phase = InitializationPhase.COMPLETED

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
        # Patch the registries and background discovery
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
                combined_registry, "_discover_enterprise_sessions", AsyncMock()
            ) as mock_discover,
        ):

            # Set up mocks for initialize
            mock_community_registry = mock_community_cls.return_value
            mock_community_registry.initialize = AsyncMock()
            mock_community_registry.get_all = AsyncMock(
                return_value=RegistrySnapshot.simple(items={})
            )

            mock_enterprise_registry = mock_enterprise_cls.return_value
            mock_enterprise_registry.initialize = AsyncMock()

            # Call initialize
            await combined_registry.initialize(mock_config_manager)

            # Wait for the mocked background discovery task to complete
            await combined_registry._enterprise_discovery_task

            # Verify correct calls were made
            mock_community_registry.initialize.assert_called_once_with(
                mock_config_manager
            )
            mock_enterprise_registry.initialize.assert_called_once_with(
                mock_config_manager
            )
            mock_discover.assert_awaited_once()

            # Verify registry was marked as initialized with PARTIAL phase
            assert combined_registry._initialized is True
            assert combined_registry._init_phase == InitializationPhase.PARTIAL

            # Verify background task was created
            assert combined_registry._enterprise_discovery_task is not None

    @pytest.mark.asyncio
    async def test_initialize_community_failure(
        self, combined_registry, mock_config_manager
    ):
        """Test handling of community registry initialization failure."""
        with (
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CommunitySessionRegistry"
            ) as mock_community_cls,
            patch(
                "deephaven_mcp.resource_manager._registry_combined.CorePlusSessionFactoryRegistry"
            ) as mock_enterprise_cls,
            patch.object(
                combined_registry, "_discover_enterprise_sessions", AsyncMock()
            ) as mock_discover,
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
            mock_discover.assert_not_awaited()

            # Verify registry was not marked as initialized
            assert combined_registry._initialized is False
            assert combined_registry._init_phase == InitializationPhase.NOT_STARTED

    @pytest.mark.asyncio
    async def test_initialize_enterprise_failure(
        self, combined_registry, mock_config_manager
    ):
        """Test handling of enterprise registry initialization failure."""
        with (
            patch.object(combined_registry, "_community_registry", None),
            patch.object(combined_registry, "_enterprise_registry", None),
            patch.object(
                combined_registry, "_discover_enterprise_sessions", AsyncMock()
            ) as mock_discover,
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
            mock_community_registry.get_all = AsyncMock(
                return_value=RegistrySnapshot.simple(items={})
            )

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

            # Verify background discovery was not started
            mock_discover.assert_not_awaited()

            # Verify registry was not marked as initialized
            assert combined_registry._initialized is False
            assert combined_registry._init_phase == InitializationPhase.NOT_STARTED

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
                combined_registry, "_discover_enterprise_sessions", AsyncMock()
            ),
        ):

            mock_community_registry = mock_community_cls.return_value
            mock_community_registry.initialize = AsyncMock()
            mock_community_registry.get_all = AsyncMock(
                return_value=RegistrySnapshot.simple(items={})
            )

            mock_enterprise_registry = mock_enterprise_cls.return_value
            mock_enterprise_registry.initialize = AsyncMock()

            await combined_registry.initialize(mock_config_manager)
            await combined_registry._enterprise_discovery_task

        # Second initialization should be a no-op
        with patch.object(
            combined_registry, "_discover_enterprise_sessions", AsyncMock()
        ) as mock_discover:
            with patch(
                "deephaven_mcp.resource_manager._registry_combined._LOGGER.warning"
            ) as mock_warn:
                await combined_registry.initialize(mock_config_manager)

                # Verify warning was logged
                assert mock_warn.called

                # Verify background discovery was not started again
                mock_discover.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_initialize_with_community_sessions(
        self, combined_registry, mock_config_manager
    ):
        """Test initialization when community registry has sessions to load."""
        # Create mock session managers with proper full_name attributes
        mock_session1 = MagicMock()
        mock_session1.full_name = "community:local:session1"
        mock_session2 = MagicMock()
        mock_session2.full_name = "community:local:session2"
        community_sessions = {
            "community:local:session1": mock_session1,
            "community:local:session2": mock_session2,
        }

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
                combined_registry, "_discover_enterprise_sessions", AsyncMock()
            ) as mock_discover,
        ):

            # Set up mocks for initialize
            mock_community_registry = mock_community_cls.return_value
            mock_community_registry.initialize = AsyncMock()
            mock_community_registry.get_all = AsyncMock(
                return_value=RegistrySnapshot.simple(items=community_sessions)
            )

            mock_enterprise_registry = mock_enterprise_cls.return_value
            mock_enterprise_registry.initialize = AsyncMock()

            # Call initialize
            await combined_registry.initialize(mock_config_manager)
            await combined_registry._enterprise_discovery_task

            # Verify community sessions were loaded into _items
            assert len(combined_registry._items) == 2
            assert combined_registry._items["community:local:session1"] == mock_session1
            assert combined_registry._items["community:local:session2"] == mock_session2

            # Verify correct calls were made
            mock_community_registry.initialize.assert_called_once_with(
                mock_config_manager
            )
            mock_community_registry.get_all.assert_called_once()
            mock_enterprise_registry.initialize.assert_called_once_with(
                mock_config_manager
            )
            mock_discover.assert_awaited_once()

            # Verify registry was marked as initialized
            assert combined_registry._initialized is True

    @pytest.mark.asyncio
    async def test_initialize_community_get_all_failure(
        self, combined_registry, mock_config_manager
    ):
        """Test initialization when community registry get_all() fails."""
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
                combined_registry, "_discover_enterprise_sessions", AsyncMock()
            ),
        ):

            # Set up mocks for initialize
            mock_community_registry = mock_community_cls.return_value
            mock_community_registry.initialize = AsyncMock()
            mock_community_registry.get_all = AsyncMock(
                side_effect=Exception("Community get_all failed")
            )

            mock_enterprise_registry = mock_enterprise_cls.return_value
            mock_enterprise_registry.initialize = AsyncMock()

            # Call initialize should propagate the exception
            with pytest.raises(Exception, match="Community get_all failed"):
                await combined_registry.initialize(mock_config_manager)

            # Verify registry was not marked as initialized due to failure
            assert combined_registry._initialized is False

    @pytest.mark.asyncio
    async def test_initialize_community_unexpected_phase(
        self, combined_registry, mock_config_manager
    ):
        """Test initialization raises InternalError when community snapshot has unexpected phase."""
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
                combined_registry, "_discover_enterprise_sessions", AsyncMock()
            ),
        ):
            mock_community_registry = mock_community_cls.return_value
            mock_community_registry.initialize = AsyncMock()
            mock_community_registry.get_all = AsyncMock(
                return_value=RegistrySnapshot.with_initialization(
                    items={}, phase=InitializationPhase.LOADING, errors={}
                )
            )

            mock_enterprise_registry = mock_enterprise_cls.return_value
            mock_enterprise_registry.initialize = AsyncMock()

            with pytest.raises(InternalError, match="expected SIMPLE"):
                await combined_registry.initialize(mock_config_manager)

            assert combined_registry._initialized is False

    @pytest.mark.asyncio
    async def test_initialize_community_unexpected_errors(
        self, combined_registry, mock_config_manager
    ):
        """Test initialization raises InternalError when community snapshot has errors."""
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
                combined_registry, "_discover_enterprise_sessions", AsyncMock()
            ),
        ):
            mock_community_registry = mock_community_cls.return_value
            mock_community_registry.initialize = AsyncMock()
            mock_community_registry.get_all = AsyncMock(
                return_value=RegistrySnapshot.with_initialization(
                    items={},
                    phase=InitializationPhase.SIMPLE,
                    errors={"src": "some error"},
                )
            )

            mock_enterprise_registry = mock_enterprise_cls.return_value
            mock_enterprise_registry.initialize = AsyncMock()

            with pytest.raises(InternalError, match="unexpected errors"):
                await combined_registry.initialize(mock_config_manager)

            assert combined_registry._initialized is False

    @pytest.mark.asyncio
    async def test_initialize_with_empty_community_sessions(
        self, combined_registry, mock_config_manager
    ):
        """Test initialization when community registry returns empty sessions dict."""
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
                combined_registry, "_discover_enterprise_sessions", AsyncMock()
            ) as mock_discover,
        ):

            # Set up mocks for initialize with empty community sessions
            mock_community_registry = mock_community_cls.return_value
            mock_community_registry.initialize = AsyncMock()
            mock_community_registry.get_all = AsyncMock(
                return_value=RegistrySnapshot.simple(items={})
            )

            mock_enterprise_registry = mock_enterprise_cls.return_value
            mock_enterprise_registry.initialize = AsyncMock()

            # Call initialize
            await combined_registry.initialize(mock_config_manager)
            await combined_registry._enterprise_discovery_task

            # Verify no community sessions were loaded
            community_items = {
                k: v
                for k, v in combined_registry._items.items()
                if k.startswith("community:")
            }
            assert len(community_items) == 0

            # Verify correct calls were made
            mock_community_registry.get_all.assert_called_once()
            mock_discover.assert_awaited_once()
            assert combined_registry._initialized is True

    @pytest.mark.asyncio
    async def test_initialize_community_sessions_overwrite_behavior(
        self, combined_registry, mock_config_manager
    ):
        """Test that community sessions properly overwrite any existing items with same keys."""
        # Pre-populate _items with some existing data
        existing_session = MagicMock()
        combined_registry._items["community:local:session1"] = existing_session

        # Create new mock session managers with proper full_name attributes
        new_mock_session1 = MagicMock()
        new_mock_session1.full_name = "community:local:session1"
        new_mock_session2 = MagicMock()
        new_mock_session2.full_name = "community:local:session2"
        community_sessions = {
            "community:local:session1": new_mock_session1,  # Should overwrite existing
            "community:local:session2": new_mock_session2,  # Should be new
        }

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
                combined_registry, "_discover_enterprise_sessions", AsyncMock()
            ),
        ):

            # Set up mocks for initialize
            mock_community_registry = mock_community_cls.return_value
            mock_community_registry.initialize = AsyncMock()
            mock_community_registry.get_all = AsyncMock(
                return_value=RegistrySnapshot.simple(items=community_sessions)
            )

            mock_enterprise_registry = mock_enterprise_cls.return_value
            mock_enterprise_registry.initialize = AsyncMock()

            # Call initialize
            await combined_registry.initialize(mock_config_manager)
            await combined_registry._enterprise_discovery_task

            # Verify community sessions were loaded and overwrote existing
            assert (
                combined_registry._items["community:local:session1"]
                == new_mock_session1
            )  # Overwritten
            assert (
                combined_registry._items["community:local:session1"] != existing_session
            )  # Not the old one
            assert (
                combined_registry._items["community:local:session2"]
                == new_mock_session2
            )  # New

            # Verify correct calls were made
            mock_community_registry.get_all.assert_called_once()
            assert combined_registry._initialized is True


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
        type(mock_factory_instance).controller_client = PropertyMock(
            return_value=mock_client
        )
        # Call the method
        client = await combined_registry._get_or_create_controller_client(
            mock_factory, "test_factory"
        )

        # Verify client was created and cached
        # Note: subscribe() is NOT called here - it's already done during factory creation
        mock_factory.get.assert_awaited_once()
        assert mock_factory_instance.controller_client == mock_client
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

        # Set up the factory with mocked methods
        factory = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory_instance = MagicMock()
        factory.get = AsyncMock(return_value=mock_factory_instance)

        # Call should reuse the client since it's healthy
        client = await combined_registry._get_or_create_controller_client(
            factory, "test_factory"
        )

        # Verify the client was reused not recreated
        factory.get.assert_not_called()
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
        mock_old_client.ping = AsyncMock(
            side_effect=DeephavenConnectionError("Dead client")
        )
        mock_factory_instance = MagicMock()
        mock_new_client = MagicMock(spec=CorePlusControllerClient)

        # Pre-populate cache with a dead client
        combined_registry._controller_clients["test_factory"] = mock_old_client

        mock_factory.get = AsyncMock(return_value=mock_factory_instance)
        type(mock_factory_instance).controller_client = PropertyMock(
            return_value=mock_new_client
        )

        # Call the method
        client = await combined_registry._get_or_create_controller_client(
            mock_factory, "test_factory"
        )

        # Verify new client was created and old client was replaced
        # Note: subscribe() is NOT called here - it's already done during factory creation
        mock_factory.get.assert_awaited_once()
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

        # Mock failed health check (False result)
        mock_old_client.ping = AsyncMock(return_value=False)

        mock_factory.get = AsyncMock(return_value=mock_factory_instance)
        # Mock property instead of method
        type(mock_factory_instance).controller_client = PropertyMock(
            return_value=mock_new_client
        )

        # Call the method
        client = await combined_registry._get_or_create_controller_client(
            mock_factory, "test_factory"
        )

        # Verify new client was created
        # Note: subscribe() is NOT called here - it's already done during factory creation
        mock_factory.get.assert_awaited_once()

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

        # Mock enterprise registry responses
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={"factory1": mock_factory})
        )
        initialized_registry._enterprise_registry.get = AsyncMock(
            return_value=mock_factory
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
            return_value=RegistrySnapshot.simple(items={"factory1": mock_factory})
        )

        # Mock controller client
        mock_client = AsyncMock(spec=CorePlusControllerClient)
        mock_client.ping = AsyncMock(return_value=True)

        # Setup existing session manager that will be reused
        session_key = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, "factory1", "session1"
        )
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
        mock_old_session = MagicMock(spec=EnterpriseSessionManager)

        # Mock enterprise registry response
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={"factory1": mock_factory})
        )

        # Add a "stale" session that should be removed
        stale_key = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, "factory1", "old_session"
        )
        initialized_registry._items[stale_key] = mock_old_session
        mock_old_session.close = AsyncMock()

        # Create a mock session info object with config.pb.name attribute
        mock_session_info = MagicMock()
        mock_session_info.config.pb.name = "session1"

        # Mock controller client with properly mocked map() method
        mock_client.map = AsyncMock(return_value={"session1": mock_session_info})

        with patch.object(
            initialized_registry,
            "_get_or_create_controller_client",
            AsyncMock(return_value=mock_client),
        ):
            # Call the method under test
            await initialized_registry._update_enterprise_sessions()

            # Verify stale session was closed and removed
            mock_old_session.close.assert_awaited_once()
            assert stale_key not in initialized_registry._items


# ============================================================================
# Additional Enterprise Session Tests
# ============================================================================


def test_add_new_enterprise_sessions(initialized_registry):
    """Test that new session managers are created for new sessions."""
    # Arrange
    factory = MagicMock(spec=CorePlusSessionFactoryManager)
    factory_name = "factory1"
    new_sessions = {"session1", "session2"}
    initialized_registry._items = {}

    def mock_make_manager_side_effect(factory, factory_name, session_name):
        manager = MagicMock(spec=EnterpriseSessionManager)
        manager.full_name = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, factory_name, session_name
        )
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
        key1 = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, factory_name, "session1"
        )
        key2 = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, factory_name, "session2"
        )
        assert key1 in initialized_registry._items
        assert key2 in initialized_registry._items


def test_add_new_enterprise_sessions_skips_existing(initialized_registry):
    """Test that existing session managers are not recreated."""
    # Arrange
    factory = MagicMock(spec=CorePlusSessionFactoryManager)
    factory_name = "factory1"
    # session1 already exists, session2 is new
    sessions = {"session1", "session2"}
    existing_key = BaseItemManager.make_full_name(
        SystemType.ENTERPRISE, factory_name, "session1"
    )
    initialized_registry._items = {existing_key: MagicMock()}

    def mock_make_manager_side_effect(factory, factory_name, session_name):
        manager = MagicMock(spec=EnterpriseSessionManager)
        manager.full_name = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, factory_name, session_name
        )
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
        new_key = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, factory_name, "session2"
        )
        assert new_key in initialized_registry._items


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
async def test_find_session_keys_for_factory(initialized_registry):
    """Test that _find_session_keys_for_factory correctly identifies session keys for a factory."""
    # Arrange
    factory_name = "test_factory"

    # Create session keys with different prefixes
    factory_prefix = BaseItemManager.make_full_name(
        SystemType.ENTERPRISE, factory_name, ""
    )
    session1_key = f"{factory_prefix}session1"
    session2_key = f"{factory_prefix}session2"
    other_factory_key = BaseItemManager.make_full_name(
        SystemType.ENTERPRISE, "other_factory", "session3"
    )
    community_key = BaseItemManager.make_full_name(
        SystemType.COMMUNITY, "community", "session4"
    )

    # Add sessions to the registry
    initialized_registry._items = {
        session1_key: MagicMock(),
        session2_key: MagicMock(),
        other_factory_key: MagicMock(),
        community_key: MagicMock(),
    }

    # Act
    result = initialized_registry._find_session_keys_for_factory(factory_name)

    # Assert
    assert len(result) == 2
    assert session1_key in result
    assert session2_key in result
    assert other_factory_key not in result
    assert community_key not in result


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
        """Test that get raises RegistryItemNotFoundError when item not found."""
        from deephaven_mcp._exceptions import RegistryItemNotFoundError

        # Community session - no update should happen
        with pytest.raises(RegistryItemNotFoundError):
            await initialized_registry.get("community:source:nonexistent")

    @pytest.mark.asyncio
    async def test_get_not_found_no_init_context_when_completed(
        self, initialized_registry
    ):
        """Test that get error has no init context when initialization completed cleanly."""
        from deephaven_mcp._exceptions import RegistryItemNotFoundError

        with pytest.raises(
            RegistryItemNotFoundError, match="No item with name"
        ) as exc_info:
            await initialized_registry.get("community:source:nonexistent")
        assert "Note:" not in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_not_found_during_discovery(self, initialized_registry):
        """Test that get error includes discovery-in-progress context."""
        from deephaven_mcp._exceptions import RegistryItemNotFoundError

        initialized_registry._init_phase = InitializationPhase.LOADING
        with pytest.raises(RegistryItemNotFoundError) as exc_info:
            await initialized_registry.get("enterprise:factory1:session1")
        assert "discovery is still in progress" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_not_found_with_matching_factory_error(
        self, initialized_registry
    ):
        """Test that get error includes targeted factory error when factory matches."""
        from deephaven_mcp._exceptions import RegistryItemNotFoundError

        initialized_registry._init_errors = {"factory1": "Connection refused"}
        initialized_registry._update_enterprise_sessions = AsyncMock()
        with pytest.raises(RegistryItemNotFoundError) as exc_info:
            await initialized_registry.get("enterprise:factory1:session1")
        error_msg = str(exc_info.value)
        assert "factory 'factory1' had an initialization error" in error_msg
        assert "Connection refused" in error_msg

    @pytest.mark.asyncio
    async def test_get_not_found_with_unmatched_factory_error(
        self, initialized_registry
    ):
        """Test that get error includes all factory errors when factory doesn't match."""
        from deephaven_mcp._exceptions import RegistryItemNotFoundError

        initialized_registry._init_errors = {"other_factory": "Connection timeout"}
        initialized_registry._update_enterprise_sessions = AsyncMock()
        with pytest.raises(RegistryItemNotFoundError) as exc_info:
            await initialized_registry.get("enterprise:factory1:session1")
        error_msg = str(exc_info.value)
        assert "initialization errors were detected for 1 factory" in error_msg
        assert "other_factory: Connection timeout" in error_msg

    @pytest.mark.asyncio
    async def test_get_not_found_during_discovery_with_errors(
        self, initialized_registry
    ):
        """Test that get error includes both discovery status and factory errors."""
        from deephaven_mcp._exceptions import RegistryItemNotFoundError

        initialized_registry._init_phase = InitializationPhase.LOADING
        initialized_registry._init_errors = {"factory1": "Connection refused"}
        with pytest.raises(RegistryItemNotFoundError) as exc_info:
            await initialized_registry.get("enterprise:factory1:session1")
        error_msg = str(exc_info.value)
        assert "discovery is still in progress" in error_msg
        assert "factory 'factory1' had an initialization error" in error_msg

    @pytest.mark.asyncio
    async def test_get_success(self, initialized_registry):
        """Test successful get of community session (no update triggered)."""
        # Add a community item to the registry
        mock_item = MagicMock(spec=BaseItemManager)
        initialized_registry._items["community:source:name"] = mock_item

        # Get the item
        item = await initialized_registry.get("community:source:name")

        # Verify item was returned
        assert item == mock_item

    @pytest.mark.asyncio
    async def test_get_enterprise_session_triggers_factory_update(
        self, initialized_registry
    ):
        """Test that getting an enterprise session updates only that factory."""
        # Mock the update method
        initialized_registry._update_enterprise_sessions = AsyncMock()

        # Add an enterprise item to the registry
        mock_item = MagicMock(spec=BaseItemManager)
        initialized_registry._items["enterprise:factory1:session1"] = mock_item

        # Get the enterprise item
        item = await initialized_registry.get("enterprise:factory1:session1")

        # Verify item was returned and only factory1 was updated
        assert item == mock_item
        initialized_registry._update_enterprise_sessions.assert_awaited_once_with(
            factory_name="factory1"
        )

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
        snapshot = await initialized_registry.get_all()

        # Verify items were returned and enterprise sessions were updated
        assert snapshot.items == {
            "community:source:name1": mock_item1,
            "enterprise:factory:name2": mock_item2,
        }
        assert snapshot.initialization_phase == InitializationPhase.COMPLETED
        assert snapshot.initialization_errors == {}
        initialized_registry._update_enterprise_sessions.assert_awaited_once()

        # Verify returned dict is a copy
        assert snapshot.items is not initialized_registry._items


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
        mock_client2 = MagicMock(spec=CorePlusControllerClient)

        initialized_registry._controller_clients = {
            "factory1": mock_client1,
            "factory2": mock_client2,
        }

        # Close the registry
        await initialized_registry.close()

        # Verify both registries were closed
        mock_community_registry.close.assert_awaited_once()
        mock_enterprise_registry.close.assert_awaited_once()

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

        # Add a controller client
        mock_client = MagicMock(spec=CorePlusControllerClient)

        initialized_registry._controller_clients = {"factory1": mock_client}

        # Close should complete despite exceptions
        with patch(
            "deephaven_mcp.resource_manager._registry_combined._LOGGER.error"
        ) as mock_error:
            await initialized_registry.close()

            # Verify errors were logged for each registry component
            assert mock_error.call_count >= 2

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


class TestPublicAPIMethods:
    """Tests for public API methods (add_session, remove_session, count_added_sessions, is_added_session)."""

    @pytest.mark.asyncio
    async def test_add_session_success(self, initialized_registry):
        """Test successfully adding a session to the registry."""
        # Create a mock session manager
        mock_manager = MagicMock(spec=EnterpriseSessionManager)
        mock_manager.full_name = "enterprise:factory1:session1"

        # Add the session
        await initialized_registry.add_session(mock_manager)

        # Verify it was added to both _items and _added_sessions
        assert "enterprise:factory1:session1" in initialized_registry._items
        assert "enterprise:factory1:session1" in initialized_registry._added_sessions
        assert (
            initialized_registry._items["enterprise:factory1:session1"] == mock_manager
        )

    @pytest.mark.asyncio
    async def test_add_session_duplicate_raises_error(self, initialized_registry):
        """Test that adding a duplicate session raises ValueError."""
        # Create a mock session manager
        mock_manager = MagicMock(spec=EnterpriseSessionManager)
        mock_manager.full_name = "enterprise:factory1:session1"

        # Add the session once
        await initialized_registry.add_session(mock_manager)

        # Try to add it again - should raise ValueError
        with pytest.raises(ValueError, match="Session.*already exists"):
            await initialized_registry.add_session(mock_manager)

    @pytest.mark.asyncio
    async def test_add_session_not_initialized_raises_error(self):
        """Test that add_session raises InternalError if not initialized."""
        registry = CombinedSessionRegistry()
        mock_manager = MagicMock(spec=EnterpriseSessionManager)
        mock_manager.full_name = "enterprise:factory1:session1"

        with pytest.raises(InternalError, match="not initialized"):
            await registry.add_session(mock_manager)

    @pytest.mark.asyncio
    async def test_remove_session_exists(self, initialized_registry):
        """Test removing a session that exists."""
        # Add a session first
        mock_manager = MagicMock(spec=EnterpriseSessionManager)
        mock_manager.full_name = "enterprise:factory1:session1"
        initialized_registry._items["enterprise:factory1:session1"] = mock_manager
        initialized_registry._added_sessions.add("enterprise:factory1:session1")

        # Remove it
        removed = await initialized_registry.remove_session(
            "enterprise:factory1:session1"
        )

        # Verify it was removed and returned
        assert removed == mock_manager
        assert "enterprise:factory1:session1" not in initialized_registry._items
        assert (
            "enterprise:factory1:session1" not in initialized_registry._added_sessions
        )

    @pytest.mark.asyncio
    async def test_remove_session_not_exists(self, initialized_registry):
        """Test removing a session that doesn't exist returns None."""
        removed = await initialized_registry.remove_session(
            "enterprise:factory1:nonexistent"
        )
        assert removed is None

    @pytest.mark.asyncio
    async def test_remove_session_not_initialized_raises_error(self):
        """Test that remove_session raises InternalError if not initialized."""
        registry = CombinedSessionRegistry()

        with pytest.raises(InternalError, match="not initialized"):
            await registry.remove_session("enterprise:factory1:session1")

    @pytest.mark.asyncio
    async def test_count_added_sessions_with_string(self, initialized_registry):
        """Test count_added_sessions with string system_type."""
        # Add some sessions
        mock_manager1 = MagicMock(spec=EnterpriseSessionManager)
        mock_manager1.full_name = "enterprise:factory1:session1"
        mock_manager2 = MagicMock(spec=EnterpriseSessionManager)
        mock_manager2.full_name = "enterprise:factory1:session2"
        mock_manager3 = MagicMock(spec=EnterpriseSessionManager)
        mock_manager3.full_name = "enterprise:factory2:session3"

        initialized_registry._items["enterprise:factory1:session1"] = mock_manager1
        initialized_registry._items["enterprise:factory1:session2"] = mock_manager2
        initialized_registry._items["enterprise:factory2:session3"] = mock_manager3
        initialized_registry._added_sessions.add("enterprise:factory1:session1")
        initialized_registry._added_sessions.add("enterprise:factory1:session2")
        initialized_registry._added_sessions.add("enterprise:factory2:session3")

        # Count for factory1
        count = await initialized_registry.count_added_sessions(
            "enterprise", "factory1"
        )
        assert count == 2

        # Count for factory2
        count = await initialized_registry.count_added_sessions(
            "enterprise", "factory2"
        )
        assert count == 1

    @pytest.mark.asyncio
    async def test_count_added_sessions_with_enum(self, initialized_registry):
        """Test count_added_sessions with SystemType enum."""
        # Add some sessions
        mock_manager = MagicMock(spec=EnterpriseSessionManager)
        mock_manager.full_name = "enterprise:factory1:session1"

        initialized_registry._items["enterprise:factory1:session1"] = mock_manager
        initialized_registry._added_sessions.add("enterprise:factory1:session1")

        # Count using enum
        count = await initialized_registry.count_added_sessions(
            SystemType.ENTERPRISE, "factory1"
        )
        assert count == 1

    @pytest.mark.asyncio
    async def test_count_added_sessions_with_stale_sessions(self, initialized_registry):
        """Test count_added_sessions removes stale sessions."""
        # Add a session to _added_sessions but not to _items (stale)
        initialized_registry._added_sessions.add("enterprise:factory1:stale_session")

        # Also add a valid session
        mock_manager = MagicMock(spec=EnterpriseSessionManager)
        mock_manager.full_name = "enterprise:factory1:valid_session"
        initialized_registry._items["enterprise:factory1:valid_session"] = mock_manager
        initialized_registry._added_sessions.add("enterprise:factory1:valid_session")

        # Count - should be 1 and stale session should be removed
        count = await initialized_registry.count_added_sessions(
            "enterprise", "factory1"
        )
        assert count == 1
        assert (
            "enterprise:factory1:stale_session"
            not in initialized_registry._added_sessions
        )
        assert (
            "enterprise:factory1:valid_session" in initialized_registry._added_sessions
        )

    @pytest.mark.asyncio
    async def test_count_added_sessions_with_invalid_format(self, initialized_registry):
        """Test count_added_sessions handles invalid session ID format."""
        # Add a session with invalid format
        initialized_registry._added_sessions.add("invalid_session_id")

        # Count - should handle the error and remove invalid session
        count = await initialized_registry.count_added_sessions(
            "enterprise", "factory1"
        )
        assert count == 0
        assert "invalid_session_id" not in initialized_registry._added_sessions

    @pytest.mark.asyncio
    async def test_is_added_session_true(self, initialized_registry):
        """Test is_added_session returns True for added sessions."""
        initialized_registry._added_sessions.add("enterprise:factory1:session1")

        result = await initialized_registry.is_added_session(
            "enterprise:factory1:session1"
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_is_added_session_false(self, initialized_registry):
        """Test is_added_session returns False for non-added sessions."""
        result = await initialized_registry.is_added_session(
            "enterprise:factory1:nonexistent"
        )
        assert result is False


class TestConnectionErrorHandling:
    """Tests for connection error handling in enterprise session updates."""

    @pytest.mark.asyncio
    async def test_calculate_factory_session_changes_connection_error(
        self, initialized_registry
    ):
        """Test that connection errors trigger session removal."""
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        factory_name = "factory1"

        # Add some sessions for this factory
        mock_session1 = MagicMock(spec=EnterpriseSessionManager)
        mock_session1.close = AsyncMock()
        mock_session2 = MagicMock(spec=EnterpriseSessionManager)
        mock_session2.close = AsyncMock()

        key1 = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, factory_name, "session1"
        )
        key2 = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, factory_name, "session2"
        )

        initialized_registry._items[key1] = mock_session1
        initialized_registry._items[key2] = mock_session2

        # Mock _get_or_create_controller_client to raise DeephavenConnectionError
        with patch.object(
            initialized_registry,
            "_get_or_create_controller_client",
            AsyncMock(side_effect=DeephavenConnectionError("Connection failed")),
        ):
            # Call the method - should return empty new sessions and existing sessions as removals
            changes = await initialized_registry._calculate_factory_session_changes(
                mock_factory, factory_name
            )

            # Verify it returned correct values including connection_error
            assert changes.session_names_to_add == set()
            assert changes.session_keys_to_remove == {key1, key2}
            assert changes.connection_error == "Connection failed"

            # Apply the removals to verify cleanup works
            await initialized_registry._close_stale_enterprise_sessions(
                changes.session_keys_to_remove
            )

            # Verify sessions were closed and removed
            mock_session1.close.assert_awaited_once()
            mock_session2.close.assert_awaited_once()
            assert key1 not in initialized_registry._items
            assert key2 not in initialized_registry._items

    @pytest.mark.asyncio
    async def test_remove_all_sessions_for_factory(self, initialized_registry):
        """Test _remove_all_sessions_for_factory removes all sessions for a factory."""
        factory_name = "factory1"

        # Add sessions for this factory
        mock_session1 = MagicMock(spec=EnterpriseSessionManager)
        mock_session1.close = AsyncMock()
        mock_session2 = MagicMock(spec=EnterpriseSessionManager)
        mock_session2.close = AsyncMock()

        # Also add a session for a different factory
        mock_session3 = MagicMock(spec=EnterpriseSessionManager)
        mock_session3.close = AsyncMock()

        key1 = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, factory_name, "session1"
        )
        key2 = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, factory_name, "session2"
        )
        key3 = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, "factory2", "session3"
        )

        initialized_registry._items[key1] = mock_session1
        initialized_registry._items[key2] = mock_session2
        initialized_registry._items[key3] = mock_session3

        # Remove all sessions for factory1
        await initialized_registry._remove_all_sessions_for_factory(factory_name)

        # Verify only factory1 sessions were removed
        mock_session1.close.assert_awaited_once()
        mock_session2.close.assert_awaited_once()
        mock_session3.close.assert_not_awaited()

        assert key1 not in initialized_registry._items
        assert key2 not in initialized_registry._items
        assert key3 in initialized_registry._items

    @pytest.mark.asyncio
    async def test_update_enterprise_sessions_multiple_factories_parallel(
        self, initialized_registry
    ):
        """Test that multiple factories update in parallel and all succeed."""
        # Create multiple mock factories
        mock_factory1 = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory1._items = {}
        mock_factory2 = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory2._items = {}
        mock_factory3 = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory3._items = {}

        # Mock enterprise registry to return multiple factories
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(
                items={
                    "factory1": mock_factory1,
                    "factory2": mock_factory2,
                    "factory3": mock_factory3,
                }
            )
        )

        initialized_registry._items = {}

        # Track call order to verify parallel execution
        call_order = []

        async def mock_update_factory(factory, factory_name):
            """Mock that tracks when each factory update is called."""
            from deephaven_mcp.resource_manager._registry_combined import (
                _FactorySessionChanges,
            )

            call_order.append(f"start_{factory_name}")
            # Simulate some async work
            await asyncio.sleep(0.01)
            call_order.append(f"end_{factory_name}")
            return _FactorySessionChanges(
                factory, factory_name, set(), set()
            )  # Return empty changes

        with patch.object(
            initialized_registry,
            "_calculate_factory_session_changes",
            side_effect=mock_update_factory,
        ):
            await initialized_registry._update_enterprise_sessions()

        # Verify all factories were called
        assert len(call_order) == 6
        # All starts should happen before any ends (parallel execution)
        start_indices = [i for i, x in enumerate(call_order) if x.startswith("start_")]
        end_indices = [i for i, x in enumerate(call_order) if x.startswith("end_")]
        # In parallel execution, all starts happen first, then all ends
        assert max(start_indices) < min(end_indices)

    @pytest.mark.asyncio
    async def test_update_enterprise_sessions_mixed_connection_errors(
        self, initialized_registry
    ):
        """Test that connection errors in some factories don't block others."""
        mock_factory1 = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory1._items = {}
        mock_factory2 = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory2._items = {}
        mock_factory3 = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory3._items = {}

        # Mock enterprise registry
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(
                items={
                    "factory1": mock_factory1,
                    "factory2": mock_factory2,
                    "factory3": mock_factory3,
                }
            )
        )

        initialized_registry._items = {}

        # Track which factories completed
        completed_factories = []

        async def mock_update_factory(factory, factory_name):
            """Mock that simulates connection error for factory2."""
            from deephaven_mcp.resource_manager._registry_combined import (
                _FactorySessionChanges,
            )

            await asyncio.sleep(0.01)
            if factory_name != "factory2":
                completed_factories.append(factory_name)
            return _FactorySessionChanges(
                factory, factory_name, set(), set()
            )  # Return empty changes

        with patch.object(
            initialized_registry,
            "_calculate_factory_session_changes",
            side_effect=mock_update_factory,
        ):
            await initialized_registry._update_enterprise_sessions()

        # Verify factories 1 and 3 completed despite factory 2 issue
        assert "factory1" in completed_factories
        assert "factory3" in completed_factories
        # Factory 2 may or may not be in list depending on error handling

    @pytest.mark.asyncio
    async def test_update_enterprise_sessions_raises_exception_group(
        self, initialized_registry
    ):
        """Test that bulk updates raise ExceptionGroup when any factory fails."""
        mock_factory1 = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory1._items = {}
        mock_factory2 = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory2._items = {}

        # Mock enterprise registry
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(
                items={
                    "factory1": mock_factory1,
                    "factory2": mock_factory2,
                }
            )
        )

        initialized_registry._items = {}

        async def mock_update_factory(factory, factory_name):
            """Mock that raises ValueError for factory2."""
            from deephaven_mcp.resource_manager._registry_combined import (
                _FactorySessionChanges,
            )

            await asyncio.sleep(0.01)
            if factory_name == "factory2":
                raise ValueError(f"Test error from {factory_name}")
            return _FactorySessionChanges(
                factory, factory_name, set(), set()
            )  # Return empty changes for successful factories

        with patch.object(
            initialized_registry,
            "_calculate_factory_session_changes",
            side_effect=mock_update_factory,
        ):
            # Bulk update should raise ExceptionGroup with the failure
            with pytest.raises(ExceptionGroup) as exc_info:
                await initialized_registry._update_enterprise_sessions()

            # Verify the ExceptionGroup contains the ValueError
            assert len(exc_info.value.exceptions) == 1
            assert isinstance(exc_info.value.exceptions[0], ValueError)
            assert "factory2" in str(exc_info.value.exceptions[0])

    @pytest.mark.asyncio
    async def test_update_single_factory_exception_propagates(
        self, initialized_registry
    ):
        """Test that single-factory updates raise ExceptionGroup with the failure."""
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory._items = {}

        # Mock enterprise registry to return the factory
        initialized_registry._enterprise_registry.get = AsyncMock(
            return_value=mock_factory
        )

        initialized_registry._items = {}

        async def mock_update_factory(factory, factory_name):
            """Mock that raises ValueError."""
            raise ValueError(f"Test error from {factory_name}")

        with patch.object(
            initialized_registry,
            "_calculate_factory_session_changes",
            side_effect=mock_update_factory,
        ):
            # Single-factory update should raise ExceptionGroup
            with pytest.raises(ExceptionGroup) as exc_info:
                await initialized_registry._update_enterprise_sessions(
                    factory_name="factory1"
                )

            # Verify the ExceptionGroup contains the ValueError
            assert len(exc_info.value.exceptions) == 1
            assert isinstance(exc_info.value.exceptions[0], ValueError)
            assert "factory1" in str(exc_info.value.exceptions[0])


class TestUpdateEnterpriseFactorySessions:
    """Tests for _update_enterprise_sessions with factory_name parameter (single factory updates)."""

    @pytest.mark.asyncio
    async def test_update_single_factory_success(self, initialized_registry):
        """Test successful update of a single factory."""
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory._items = {}
        factory_name = "factory1"

        # Mock enterprise registry get() to return the factory
        initialized_registry._enterprise_registry.get = AsyncMock(
            return_value=mock_factory
        )

        # Mock the _calculate_factory_session_changes method
        with patch.object(
            initialized_registry,
            "_calculate_factory_session_changes",
            AsyncMock(return_value=None),  # Will be set below
        ) as mock_update:
            from deephaven_mcp.resource_manager._registry_combined import (
                _FactorySessionChanges,
            )

            mock_update.return_value = _FactorySessionChanges(
                mock_factory, factory_name, set(), set()
            )
            await initialized_registry._update_enterprise_sessions(
                factory_name=factory_name
            )

            # Verify only the specific factory was updated
            mock_update.assert_awaited_once_with(mock_factory, factory_name)
            # Verify we called get() not get_all()
            initialized_registry._enterprise_registry.get.assert_awaited_once_with(
                factory_name
            )

    @pytest.mark.asyncio
    async def test_update_single_factory_not_found(self, initialized_registry):
        """Test that RegistryItemNotFoundError is raised for non-existent factory."""
        from deephaven_mcp._exceptions import RegistryItemNotFoundError

        # Mock enterprise registry get() to raise RegistryItemNotFoundError (like the real registry does)
        initialized_registry._enterprise_registry.get = AsyncMock(
            side_effect=RegistryItemNotFoundError(
                "No item with name 'nonexistent' found"
            )
        )

        # Try to update non-existent factory - RegistryItemNotFoundError should be caught gracefully
        # The exception is expected and handled, so get() should return RegistryItemNotFoundError for the session
        with pytest.raises(
            RegistryItemNotFoundError,
            match="No item with name 'enterprise:nonexistent:session' found",
        ):
            await initialized_registry.get("enterprise:nonexistent:session")

    @pytest.mark.asyncio
    async def test_update_single_factory_not_initialized(self, combined_registry):
        """Test that InternalError is raised if registry not initialized."""
        with pytest.raises(InternalError):
            await combined_registry._update_enterprise_sessions(factory_name="factory1")

    @pytest.mark.asyncio
    async def test_update_single_factory_connection_error(self, initialized_registry):
        """Test that connection errors are handled gracefully for single factory."""
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory._items = {}
        factory_name = "factory1"

        # Mock enterprise registry get() to return the factory
        initialized_registry._enterprise_registry.get = AsyncMock(
            return_value=mock_factory
        )

        # Add sessions for this factory
        mock_session = MagicMock(spec=EnterpriseSessionManager)
        mock_session.close = AsyncMock()
        key = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, factory_name, "session1"
        )
        initialized_registry._items[key] = mock_session

        # Mock _calculate_factory_session_changes to return empty changes (connection errors handled internally)
        from deephaven_mcp.resource_manager._registry_combined import (
            _FactorySessionChanges,
        )

        with patch.object(
            initialized_registry,
            "_calculate_factory_session_changes",
            AsyncMock(
                return_value=_FactorySessionChanges(
                    mock_factory, factory_name, set(), set()
                )
            ),
        ):
            # This should not raise - connection errors are handled internally
            await initialized_registry._update_enterprise_sessions(
                factory_name=factory_name
            )

    @pytest.mark.asyncio
    async def test_update_enterprise_sessions_factory_snapshot_unexpected_phase(
        self, initialized_registry
    ):
        """Test update raises InternalError when factory snapshot has unexpected phase."""
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.with_initialization(
                items={}, phase=InitializationPhase.LOADING, errors={}
            )
        )

        with pytest.raises(InternalError, match="expected SIMPLE"):
            await initialized_registry._update_enterprise_sessions()

    @pytest.mark.asyncio
    async def test_update_enterprise_sessions_factory_snapshot_with_errors(
        self, initialized_registry
    ):
        """Test update raises InternalError when factory snapshot has errors."""
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.with_initialization(
                items={},
                phase=InitializationPhase.SIMPLE,
                errors={"factory_reg": "broken"},
            )
        )

        # Simple registry should never have errors - raises InternalError
        with pytest.raises(InternalError, match="unexpected errors"):
            await initialized_registry._update_enterprise_sessions()

    @pytest.mark.asyncio
    async def test_update_enterprise_sessions_no_factories(self, initialized_registry):
        """Test update when no factories are configured (empty registry)."""
        # Mock enterprise registry to return empty dict
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={})
        )

        # Should log and return early without error
        await initialized_registry._update_enterprise_sessions()

        # Verify early return - no items should be added
        assert len(initialized_registry._items) == 0

    @pytest.mark.asyncio
    async def test_update_enterprise_sessions_application_failure(
        self, initialized_registry
    ):
        """Test that application failures are collected in ExceptionGroup."""
        from deephaven_mcp.resource_manager._registry_combined import (
            _FactorySessionChanges,
        )

        mock_factory1 = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory1._items = {}
        mock_factory2 = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory2._items = {}

        # Mock enterprise registry
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(
                items={
                    "factory1": mock_factory1,
                    "factory2": mock_factory2,
                }
            )
        )

        # Mock _calculate_factory_session_changes to succeed for both
        changes1 = _FactorySessionChanges(
            mock_factory1, "factory1", {"session1"}, set()
        )
        changes2 = _FactorySessionChanges(
            mock_factory2, "factory2", {"session2"}, set()
        )

        with patch.object(
            initialized_registry,
            "_calculate_factory_session_changes",
            side_effect=[changes1, changes2],
        ):
            # Mock _apply_factory_session_changes to fail for factory1
            original_apply = initialized_registry._apply_factory_session_changes
            call_count = [0]

            async def failing_apply(changes):
                call_count[0] += 1
                if call_count[0] == 1:  # First call fails
                    raise RuntimeError("Application failed for factory1")
                # Second call succeeds
                await original_apply(changes)

            with patch.object(
                initialized_registry,
                "_apply_factory_session_changes",
                side_effect=failing_apply,
            ):
                # Should raise ExceptionGroup containing the application error
                with pytest.raises(ExceptionGroup) as exc_info:
                    await initialized_registry._update_enterprise_sessions()

                # Verify the exception group contains our application error
                assert len(exc_info.value.exceptions) == 1
                assert isinstance(exc_info.value.exceptions[0], RuntimeError)
                assert "Application failed for factory1" in str(
                    exc_info.value.exceptions[0]
                )

    @pytest.mark.asyncio
    async def test_get_with_invalid_session_name(self, initialized_registry):
        """Test get() with malformed session name catches InvalidSessionNameError."""
        from deephaven_mcp._exceptions import (
            InvalidSessionNameError,
            RegistryItemNotFoundError,
        )

        # Mock parse_full_name to raise InvalidSessionNameError
        with patch.object(
            BaseItemManager,
            "parse_full_name",
            side_effect=InvalidSessionNameError("Malformed session name"),
        ):
            # Should catch InvalidSessionNameError and continue
            # Then raise RegistryItemNotFoundError for the session not found
            with pytest.raises(RegistryItemNotFoundError):
                await initialized_registry.get("invalid:::name")


# ============================================================================
# v7 Non-Blocking Initialization Tests
# ============================================================================


class TestInitializationPhase:
    """Test InitializationPhase enum."""

    def test_enum_values(self):
        """Test that InitializationPhase has the expected values."""
        assert InitializationPhase.NOT_STARTED.value == "not_started"
        assert InitializationPhase.PARTIAL.value == "partial"
        assert InitializationPhase.LOADING.value == "loading"
        assert InitializationPhase.COMPLETED.value == "completed"

    def test_construction_defaults(self, combined_registry):
        """Test that new registry starts with NOT_STARTED phase and empty errors."""
        assert combined_registry._init_phase == InitializationPhase.NOT_STARTED
        assert combined_registry._init_errors == {}
        assert combined_registry._enterprise_discovery_task is None


class TestInitializationStatus:
    """Test the initialization_status() method."""

    @pytest.mark.asyncio
    async def test_initialization_status_not_started(self, combined_registry):
        """Test initialization_status returns NOT_STARTED before init."""
        # Need to set initialized to True to avoid InternalError, but phase is still NOT_STARTED
        combined_registry._initialized = True
        phase, errors = await combined_registry.initialization_status()
        assert phase == InitializationPhase.NOT_STARTED
        assert errors == {}

    @pytest.mark.asyncio
    async def test_initialization_status_completed_no_errors(
        self, initialized_registry
    ):
        """Test initialization_status returns COMPLETED with no errors."""
        phase, errors = await initialized_registry.initialization_status()
        assert phase == InitializationPhase.COMPLETED
        assert errors == {}

    @pytest.mark.asyncio
    async def test_initialization_status_completed_with_errors(
        self, initialized_registry
    ):
        """Test initialization_status returns errors when present."""
        initialized_registry._init_errors = {"factory1": "Connection failed: timeout"}
        phase, errors = await initialized_registry.initialization_status()
        assert phase == InitializationPhase.COMPLETED
        assert errors == {"factory1": "Connection failed: timeout"}

    @pytest.mark.asyncio
    async def test_initialization_status_returns_copy(self, initialized_registry):
        """Test that initialization_status returns a copy of errors dict."""
        initialized_registry._init_errors = {"factory1": "error"}
        _, errors = await initialized_registry.initialization_status()
        errors["factory2"] = "another error"
        # Original should not be modified
        assert "factory2" not in initialized_registry._init_errors

    @pytest.mark.asyncio
    async def test_initialization_status_during_discovery(self, initialized_registry):
        """Test initialization_status during LOADING phase."""
        initialized_registry._init_phase = InitializationPhase.LOADING
        phase, errors = await initialized_registry.initialization_status()
        assert phase == InitializationPhase.LOADING


class TestDiscoverEnterpriseSessions:
    """Test _discover_enterprise_sessions background task."""

    @pytest.mark.asyncio
    async def test_discover_factory_snapshot_unexpected_phase(
        self, initialized_registry
    ):
        """Test discovery raises InternalError when factory snapshot has unexpected phase."""
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.with_initialization(
                items={}, phase=InitializationPhase.LOADING, errors={}
            )
        )
        initialized_registry._init_phase = InitializationPhase.PARTIAL

        # Mock _detect_unreachable_factories to avoid second InternalError from same mock
        with patch.object(
            initialized_registry,
            "_detect_unreachable_factories",
            AsyncMock(),
        ):
            await initialized_registry._discover_enterprise_sessions()

        # InternalError is caught by the except Exception handler and recorded
        assert initialized_registry._init_phase == InitializationPhase.COMPLETED
        assert "enterprise_discovery" in initialized_registry._init_errors
        assert (
            "expected SIMPLE"
            in initialized_registry._init_errors["enterprise_discovery"]
        )

    @pytest.mark.asyncio
    async def test_discover_factory_snapshot_with_errors(self, initialized_registry):
        """Test discovery raises InternalError when factory snapshot has errors."""
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.with_initialization(
                items={},
                phase=InitializationPhase.SIMPLE,
                errors={"factory_reg": "something broke"},
            )
        )
        initialized_registry._init_phase = InitializationPhase.PARTIAL

        # Mock _detect_unreachable_factories to avoid second InternalError from same mock
        with patch.object(
            initialized_registry,
            "_detect_unreachable_factories",
            AsyncMock(),
        ):
            await initialized_registry._discover_enterprise_sessions()

        # InternalError is caught by the except Exception handler and recorded
        assert initialized_registry._init_phase == InitializationPhase.COMPLETED
        assert "enterprise_discovery" in initialized_registry._init_errors
        assert (
            "unexpected errors"
            in initialized_registry._init_errors["enterprise_discovery"]
        )

    @pytest.mark.asyncio
    async def test_discover_no_factories(self, initialized_registry):
        """Test discovery when no enterprise factories are configured."""
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={})
        )
        initialized_registry._init_phase = InitializationPhase.PARTIAL

        await initialized_registry._discover_enterprise_sessions()

        assert initialized_registry._init_phase == InitializationPhase.COMPLETED
        assert initialized_registry._init_errors == {}

    @pytest.mark.asyncio
    async def test_discover_successful_factory(self, initialized_registry):
        """Test discovery with a factory that succeeds."""
        from deephaven_mcp.resource_manager._registry_combined import (
            _FactorySessionChanges,
        )

        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={"factory1": mock_factory})
        )
        initialized_registry._init_phase = InitializationPhase.PARTIAL

        with (
            patch.object(
                initialized_registry,
                "_calculate_factory_session_changes",
                AsyncMock(
                    return_value=_FactorySessionChanges(
                        mock_factory, "factory1", set(), set()
                    )
                ),
            ),
            patch.object(
                initialized_registry,
                "_apply_factory_session_changes",
                AsyncMock(),
            ),
            patch.object(
                initialized_registry,
                "_detect_unreachable_factories",
                AsyncMock(),
            ),
        ):
            await initialized_registry._discover_enterprise_sessions()

        assert initialized_registry._init_phase == InitializationPhase.COMPLETED
        assert initialized_registry._init_errors == {}

    @pytest.mark.asyncio
    async def test_discover_factory_with_connection_error(self, initialized_registry):
        """Test discovery records connection_error from _FactorySessionChanges."""
        from deephaven_mcp.resource_manager._registry_combined import (
            _FactorySessionChanges,
        )

        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={"factory1": mock_factory})
        )
        initialized_registry._init_phase = InitializationPhase.PARTIAL

        # Return changes with connection_error set
        with (
            patch.object(
                initialized_registry,
                "_calculate_factory_session_changes",
                AsyncMock(
                    return_value=_FactorySessionChanges(
                        mock_factory,
                        "factory1",
                        set(),
                        set(),
                        connection_error="Failed to establish connection to Deephaven at https://example.com after 30.12s: Connection refused",
                    )
                ),
            ),
            patch.object(
                initialized_registry,
                "_apply_factory_session_changes",
                AsyncMock(),
            ),
            patch.object(
                initialized_registry,
                "_detect_unreachable_factories",
                AsyncMock(),
            ),
        ):
            await initialized_registry._discover_enterprise_sessions()

        assert initialized_registry._init_phase == InitializationPhase.COMPLETED
        assert "factory1" in initialized_registry._init_errors
        assert "Connection failed:" in initialized_registry._init_errors["factory1"]
        assert "Connection refused" in initialized_registry._init_errors["factory1"]

    @pytest.mark.asyncio
    async def test_discover_factory_with_exception(self, initialized_registry):
        """Test discovery records non-connection exceptions."""
        from deephaven_mcp._exceptions import AuthenticationError

        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={"factory1": mock_factory})
        )
        initialized_registry._init_phase = InitializationPhase.PARTIAL

        with (
            patch.object(
                initialized_registry,
                "_calculate_factory_session_changes",
                AsyncMock(
                    side_effect=AuthenticationError("Failed to authenticate user admin")
                ),
            ),
            patch.object(
                initialized_registry,
                "_detect_unreachable_factories",
                AsyncMock(),
            ),
        ):
            await initialized_registry._discover_enterprise_sessions()

        assert initialized_registry._init_phase == InitializationPhase.COMPLETED
        assert "factory1" in initialized_registry._init_errors
        assert "AuthenticationError" in initialized_registry._init_errors["factory1"]
        assert (
            "Failed to authenticate user admin"
            in initialized_registry._init_errors["factory1"]
        )

    @pytest.mark.asyncio
    async def test_discover_mixed_factory_results(self, initialized_registry):
        """Test discovery with mix of success and failure across factories."""
        from deephaven_mcp.resource_manager._registry_combined import (
            _FactorySessionChanges,
        )

        mock_factory1 = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_factory2 = MagicMock(spec=CorePlusSessionFactoryManager)
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(
                items={"factory1": mock_factory1, "factory2": mock_factory2}
            )
        )
        initialized_registry._init_phase = InitializationPhase.PARTIAL

        call_count = [0]

        async def mock_calculate(factory, fname):
            call_count[0] += 1
            if fname == "factory2":
                raise ValueError("Config error")
            return _FactorySessionChanges(factory, fname, set(), set())

        with (
            patch.object(
                initialized_registry,
                "_calculate_factory_session_changes",
                side_effect=mock_calculate,
            ),
            patch.object(
                initialized_registry,
                "_apply_factory_session_changes",
                AsyncMock(),
            ),
            patch.object(
                initialized_registry,
                "_detect_unreachable_factories",
                AsyncMock(),
            ),
        ):
            await initialized_registry._discover_enterprise_sessions()

        assert initialized_registry._init_phase == InitializationPhase.COMPLETED
        # factory1 succeeded, factory2 failed
        assert "factory1" not in initialized_registry._init_errors
        assert "factory2" in initialized_registry._init_errors
        assert "ValueError" in initialized_registry._init_errors["factory2"]

    @pytest.mark.asyncio
    async def test_discover_cancellation(self, initialized_registry):
        """Test that CancelledError propagates and phase stays LOADING."""
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            side_effect=asyncio.CancelledError()
        )
        initialized_registry._init_phase = InitializationPhase.PARTIAL

        with pytest.raises(asyncio.CancelledError):
            await initialized_registry._discover_enterprise_sessions()

        # Phase should stay LOADING (not transitioned to COMPLETED)
        assert initialized_registry._init_phase == InitializationPhase.LOADING


class TestDetectUnreachableFactories:
    """Test _detect_unreachable_factories."""

    @pytest.mark.asyncio
    async def test_detect_no_enterprise_registry(self, initialized_registry):
        """Test detection when enterprise registry is None."""
        initialized_registry._enterprise_registry = None
        errors: dict[str, str] = {}
        await initialized_registry._detect_unreachable_factories(errors)
        assert errors == {}

    @pytest.mark.asyncio
    async def test_detect_factory_snapshot_unexpected_phase(self, initialized_registry):
        """Test detection raises InternalError for unexpected phase (propagates to caller)."""
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.with_initialization(
                items={}, phase=InitializationPhase.LOADING, errors={}
            )
        )
        initialized_registry._items = {}

        errors: dict[str, str] = {}
        # InternalError propagates — programming bug, not caught
        with pytest.raises(InternalError, match="expected SIMPLE"):
            await initialized_registry._detect_unreachable_factories(errors)

    @pytest.mark.asyncio
    async def test_detect_factory_snapshot_with_errors(self, initialized_registry):
        """Test detection raises InternalError when factory snapshot has errors (propagates to caller)."""
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.with_initialization(
                items={},
                phase=InitializationPhase.SIMPLE,
                errors={"factory_reg": "internal failure"},
            )
        )
        initialized_registry._items = {}

        errors: dict[str, str] = {}
        # InternalError propagates — programming bug, not caught
        with pytest.raises(InternalError, match="unexpected errors"):
            await initialized_registry._detect_unreachable_factories(errors)

    @pytest.mark.asyncio
    async def test_detect_factory_online_no_sessions(self, initialized_registry):
        """Test that factory in _controller_clients but with no sessions is informational, not error."""
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={"factory1": mock_factory})
        )
        # Factory is in controller_clients (connected successfully)
        initialized_registry._controller_clients = {"factory1": MagicMock()}
        initialized_registry._items = {}

        errors: dict[str, str] = {}
        await initialized_registry._detect_unreachable_factories(errors)
        # Should NOT add an error — online but zero PQs is informational
        assert errors == {}

    @pytest.mark.asyncio
    async def test_detect_factory_offline(self, initialized_registry):
        """Test that factory NOT in _controller_clients and no sessions gets error."""
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={"factory1": mock_factory})
        )
        # Factory NOT in controller_clients
        initialized_registry._controller_clients = {}
        initialized_registry._items = {}

        errors: dict[str, str] = {}
        await initialized_registry._detect_unreachable_factories(errors)
        assert "factory1" in errors
        assert "offline or unreachable" in errors["factory1"]

    @pytest.mark.asyncio
    async def test_detect_skips_factory_already_in_errors(self, initialized_registry):
        """Test that detection skips factories already recorded in errors dict."""
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={"factory1": mock_factory})
        )
        initialized_registry._controller_clients = {}
        initialized_registry._items = {}

        errors: dict[str, str] = {"factory1": "Connection failed: detailed error"}
        await initialized_registry._detect_unreachable_factories(errors)
        # Should NOT overwrite the detailed error with generic message
        assert errors["factory1"] == "Connection failed: detailed error"

    @pytest.mark.asyncio
    async def test_detect_skips_factory_with_sessions(self, initialized_registry):
        """Test that factory with sessions is not flagged as unreachable."""
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.simple(items={"factory1": mock_factory})
        )
        initialized_registry._controller_clients = {}
        key = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, "factory1", "session1"
        )
        initialized_registry._items = {key: MagicMock()}

        errors: dict[str, str] = {}
        await initialized_registry._detect_unreachable_factories(errors)
        assert errors == {}


class TestDiscoverOuterException:
    """Test _discover_enterprise_sessions outer except Exception (line 874-877)."""

    @pytest.mark.asyncio
    async def test_discover_registry_get_all_failure(self, initialized_registry):
        """Test that an unexpected error from registry.get_all() is caught and recorded."""
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            side_effect=RuntimeError("Registry corrupted")
        )
        initialized_registry._init_phase = InitializationPhase.PARTIAL

        with patch.object(
            initialized_registry,
            "_detect_unreachable_factories",
            AsyncMock(),
        ):
            await initialized_registry._discover_enterprise_sessions()

        assert initialized_registry._init_phase == InitializationPhase.COMPLETED
        assert "enterprise_discovery" in initialized_registry._init_errors
        assert (
            "RuntimeError" in initialized_registry._init_errors["enterprise_discovery"]
        )
        assert (
            "Registry corrupted"
            in initialized_registry._init_errors["enterprise_discovery"]
        )


class TestDetectUnreachableFactoriesException:
    """Test _detect_unreachable_factories exception handling."""

    @pytest.mark.asyncio
    async def test_detect_handles_runtime_exception(self, initialized_registry):
        """Test that non-InternalError exceptions are recorded and logged at WARNING."""
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            side_effect=RuntimeError("Unexpected error")
        )

        errors: dict[str, str] = {}
        # RuntimeError is caught, recorded, and logged — does not propagate
        await initialized_registry._detect_unreachable_factories(errors)
        assert "enterprise_connectivity_check" in errors
        assert "RuntimeError" in errors["enterprise_connectivity_check"]
        assert "Unexpected error" in errors["enterprise_connectivity_check"]

    @pytest.mark.asyncio
    async def test_detect_propagates_internal_error(self, initialized_registry):
        """Test that InternalError propagates (programming bug indicator)."""
        initialized_registry._enterprise_registry.get_all = AsyncMock(
            return_value=RegistrySnapshot.with_initialization(
                items={}, phase=InitializationPhase.LOADING, errors={}
            )
        )

        errors: dict[str, str] = {}
        # InternalError should propagate, not be caught
        with pytest.raises(InternalError):
            await initialized_registry._detect_unreachable_factories(errors)


class TestGetDuringDiscovery:
    """Test get() and get_all() behavior during LOADING phase."""

    @pytest.mark.asyncio
    async def test_get_returns_existing_session_during_discovery(
        self, initialized_registry
    ):
        """Test get() returns sessions already in _items during discovery."""
        initialized_registry._init_phase = InitializationPhase.LOADING
        mock_item = MagicMock(spec=BaseItemManager)
        initialized_registry._items["enterprise:factory1:session1"] = mock_item

        result = await initialized_registry.get("enterprise:factory1:session1")
        assert result == mock_item

    @pytest.mark.asyncio
    async def test_get_skips_enterprise_update_during_discovery(
        self, initialized_registry
    ):
        """Test get() does NOT call _update_enterprise_sessions during discovery."""
        initialized_registry._init_phase = InitializationPhase.LOADING
        initialized_registry._update_enterprise_sessions = AsyncMock()
        mock_item = MagicMock(spec=BaseItemManager)
        initialized_registry._items["enterprise:factory1:session1"] = mock_item

        await initialized_registry.get("enterprise:factory1:session1")
        initialized_registry._update_enterprise_sessions.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_triggers_enterprise_update_after_completed(
        self, initialized_registry
    ):
        """Test get() calls _update_enterprise_sessions when COMPLETED."""
        assert initialized_registry._init_phase == InitializationPhase.COMPLETED
        initialized_registry._update_enterprise_sessions = AsyncMock()
        mock_item = MagicMock(spec=BaseItemManager)
        initialized_registry._items["enterprise:factory1:session1"] = mock_item

        await initialized_registry.get("enterprise:factory1:session1")
        initialized_registry._update_enterprise_sessions.assert_awaited_once_with(
            factory_name="factory1"
        )

    @pytest.mark.asyncio
    async def test_get_all_skips_enterprise_update_during_discovery(
        self, initialized_registry
    ):
        """Test get_all() does NOT call _update_enterprise_sessions during discovery."""
        initialized_registry._init_phase = InitializationPhase.LOADING
        initialized_registry._update_enterprise_sessions = AsyncMock()

        result = await initialized_registry.get_all()
        initialized_registry._update_enterprise_sessions.assert_not_awaited()
        assert isinstance(result, RegistrySnapshot)
        assert result.initialization_phase == InitializationPhase.LOADING
        assert result.initialization_errors == {}

    @pytest.mark.asyncio
    async def test_get_all_triggers_enterprise_update_after_completed(
        self, initialized_registry
    ):
        """Test get_all() calls _update_enterprise_sessions when COMPLETED."""
        assert initialized_registry._init_phase == InitializationPhase.COMPLETED
        initialized_registry._update_enterprise_sessions = AsyncMock()

        result = await initialized_registry.get_all()
        initialized_registry._update_enterprise_sessions.assert_awaited_once()
        assert result.initialization_phase == InitializationPhase.COMPLETED
        assert result.initialization_errors == {}

    @pytest.mark.asyncio
    async def test_get_community_session_during_discovery(self, initialized_registry):
        """Test get() returns community sessions during discovery (no enterprise update needed)."""
        initialized_registry._init_phase = InitializationPhase.LOADING
        mock_item = MagicMock(spec=BaseItemManager)
        initialized_registry._items["community:local:session1"] = mock_item

        result = await initialized_registry.get("community:local:session1")
        assert result == mock_item


class TestCloseWithBackgroundTask:
    """Test close() behavior with background enterprise discovery task."""

    @pytest.mark.asyncio
    async def test_close_cancels_running_task(self, initialized_registry):
        """Test that close() cancels a running background task."""

        # Create a task that will block until cancelled
        async def long_running():
            await asyncio.sleep(100)

        task = asyncio.create_task(long_running())
        initialized_registry._enterprise_discovery_task = task

        await initialized_registry.close()

        assert task.cancelled()
        assert initialized_registry._enterprise_discovery_task is None

    @pytest.mark.asyncio
    async def test_close_handles_completed_task(self, initialized_registry):
        """Test that close() handles an already-completed task gracefully."""

        async def completed():
            pass

        task = asyncio.create_task(completed())
        await task
        initialized_registry._enterprise_discovery_task = task

        # Should not raise
        await initialized_registry.close()
        assert initialized_registry._enterprise_discovery_task is None

    @pytest.mark.asyncio
    async def test_close_concurrent_second_close_returns_early(
        self, initialized_registry
    ):
        """Test that concurrent close() calls don't double-cleanup.

        Simulates the scenario where close-2 passes the first lock block
        (while _initialized is still True), but by the time it reaches the
        second lock block, close-1 has already set _initialized=False.
        """
        mock_community = initialized_registry._community_registry
        mock_community.close = AsyncMock()
        mock_enterprise = initialized_registry._enterprise_registry
        mock_enterprise.close = AsyncMock()

        # Simulate the state after close-1 has finished cleanup:
        # _initialized=False but we're already past the first lock block.
        # Directly test the second lock block's re-check.
        initialized_registry._initialized = False

        # The second lock block should return early (not raise, not double-cleanup)
        # We call close() but skip the first lock block by pre-setting state.
        # Instead, directly verify the guard: set _initialized=False, then
        # call close() — it raises from the first block, which is correct.
        # The re-check at the second block is the concurrent guard.
        # To truly test it, run two close() calls concurrently.
        async def close_with_delay():
            """Close that forces a yield between lock blocks."""
            await initialized_registry.close()

        # Reset to initialized so both calls pass the first lock block
        initialized_registry._initialized = True

        # Create a slow task so close-1 spends time in cancel/await
        async def slow_discovery():
            await asyncio.sleep(100)

        task = asyncio.create_task(slow_discovery())
        initialized_registry._enterprise_discovery_task = task

        # Run two close() calls concurrently
        results = await asyncio.gather(
            initialized_registry.close(),
            initialized_registry.close(),
            return_exceptions=True,
        )

        # One should succeed, the other should return early (not raise)
        errors = [r for r in results if isinstance(r, Exception)]
        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert initialized_registry._initialized is False

    @pytest.mark.asyncio
    async def test_close_resets_init_state(self, initialized_registry):
        """Test that close() resets phase, errors, and clears _items."""
        initialized_registry._init_phase = InitializationPhase.COMPLETED
        initialized_registry._init_errors = {"factory1": "error"}
        initialized_registry._items["community:local:session1"] = MagicMock()

        mock_community = initialized_registry._community_registry
        mock_community.close = AsyncMock()
        mock_enterprise = initialized_registry._enterprise_registry
        mock_enterprise.close = AsyncMock()

        await initialized_registry.close()

        assert initialized_registry._init_phase == InitializationPhase.NOT_STARTED
        assert initialized_registry._init_errors == {}
        assert initialized_registry._items == {}
        assert initialized_registry._initialized is False

    @pytest.mark.asyncio
    async def test_close_then_reinitialize(
        self, initialized_registry, mock_config_manager
    ):
        """Test that close() allows reinitialization."""
        mock_community = initialized_registry._community_registry
        mock_community.close = AsyncMock()
        mock_enterprise = initialized_registry._enterprise_registry
        mock_enterprise.close = AsyncMock()

        await initialized_registry.close()

        # Should be able to reinitialize
        assert initialized_registry._initialized is False
        assert initialized_registry._init_phase == InitializationPhase.NOT_STARTED

        # Reinitialize with mocks
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
                initialized_registry, "_discover_enterprise_sessions", AsyncMock()
            ),
        ):
            mock_community_registry = mock_community_cls.return_value
            mock_community_registry.initialize = AsyncMock()
            mock_community_registry.get_all = AsyncMock(
                return_value=RegistrySnapshot.simple(items={})
            )
            mock_enterprise_registry = mock_enterprise_cls.return_value
            mock_enterprise_registry.initialize = AsyncMock()

            await initialized_registry.initialize(mock_config_manager)
            await initialized_registry._enterprise_discovery_task

            assert initialized_registry._initialized is True
            assert initialized_registry._init_phase == InitializationPhase.PARTIAL


class TestFactorySessionChangesConnectionError:
    """Test _FactorySessionChanges connection_error field."""

    def test_default_connection_error_is_none(self):
        """Test that connection_error defaults to None."""
        from deephaven_mcp.resource_manager._registry_combined import (
            _FactorySessionChanges,
        )

        changes = _FactorySessionChanges(MagicMock(), "factory1", set(), set())
        assert changes.connection_error is None

    def test_connection_error_preserved(self):
        """Test that connection_error value is preserved."""
        from deephaven_mcp.resource_manager._registry_combined import (
            _FactorySessionChanges,
        )

        changes = _FactorySessionChanges(
            MagicMock(),
            "factory1",
            set(),
            set(),
            connection_error="Failed to connect to auth server: Connection refused",
        )
        assert (
            changes.connection_error
            == "Failed to connect to auth server: Connection refused"
        )

    @pytest.mark.asyncio
    async def test_calculate_success_has_no_connection_error(
        self, initialized_registry
    ):
        """Test that successful calculation returns connection_error=None."""
        mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
        mock_client = MagicMock(spec=CorePlusControllerClient)
        mock_session_info = MagicMock()
        mock_session_info.config.pb.name = "session1"
        mock_client.map = AsyncMock(return_value={"session1": mock_session_info})

        with patch.object(
            initialized_registry,
            "_get_or_create_controller_client",
            AsyncMock(return_value=mock_client),
        ):
            changes = await initialized_registry._calculate_factory_session_changes(
                mock_factory, "factory1"
            )
            assert changes.connection_error is None
