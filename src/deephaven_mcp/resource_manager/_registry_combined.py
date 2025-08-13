"""
Combined registry for managing both community and enterprise session resources.

This module provides the `CombinedSessionRegistry` class that unifies management of both
community sessions and multiple enterprise (CorePlus) session factory registries
with proper async locking, caching, and lifecycle management.

Key Classes:
    CombinedSessionRegistry: Unified registry managing community sessions and
        enterprise session factories with their associated controller clients.

Features:
    - Unified API for accessing both community and enterprise sessions
    - Thread-safe operations with asyncio locking for concurrent access
    - Automatic caching and lifecycle management of controller clients
    - Smart controller client recreation if connections die
    - Efficient session tracking with separate storage for different registry types
    - Enterprise session population via controller client integration
    - Graceful error handling and resource cleanup

Architecture:
    The combined registry maintains:
    - A single CommunitySessionRegistry for community sessions
    - A CorePlusSessionFactoryRegistry for enterprise session factories
    - A cache of controller clients for enterprise registries
    - A unified sessions dictionary tracking all available sessions across both types

Usage:
    Create a CombinedSessionRegistry, initialize it with a ConfigManager, and use it
    to access and manage all session resources. The registry handles all the complexities
    of maintaining separate registry types while presenting a unified interface:

    ```python
    registry = CombinedSessionRegistry()
    await registry.initialize(config_manager)
    sessions = await registry.get_all()  # Gets all sessions across community and enterprise
    await registry.close()  # Properly closes all resources including cached controller clients
    ```
"""

import logging
import sys
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from typing_extensions import override  # pragma: no cover
elif sys.version_info >= (3, 12):
    from typing import override  # pragma: no cover
else:
    from typing_extensions import override  # pragma: no cover

from deephaven_mcp._exceptions import DeephavenConnectionError, InternalError
from deephaven_mcp.client import CorePlusControllerClient, CorePlusSession
from deephaven_mcp.config import ConfigManager

from ._manager import BaseItemManager, EnterpriseSessionManager, SystemType
from ._registry import (
    BaseRegistry,
    CommunitySessionRegistry,
    CorePlusSessionFactoryManager,
    CorePlusSessionFactoryRegistry,
)

_LOGGER = logging.getLogger(__name__)


class CombinedSessionRegistry(BaseRegistry[BaseItemManager]):
    """
    A unified registry for managing both community and enterprise session resources.

    This registry provides a centralized management system for all session resources,
    including both community (local) sessions and enterprise (CorePlus) sessions across
    multiple factories. It manages the full lifecycle of these resources with proper
    caching, health checking, and cleanup.

    Architecture:
        - A single CommunitySessionRegistry for local community sessions
        - A CorePlusSessionFactoryRegistry for enterprise session factories
        - A cache of controller clients for efficient enterprise session management
        - A unified sessions dictionary tracking all available sessions
        - Intelligent enterprise session discovery via controller client integration

    Key Features:
        - Unified API for managing heterogeneous session resources
        - Smart controller client caching with automatic health checking
        - Efficient session resource reuse and cleanup
        - Thread-safe operations with proper asyncio locking
        - Graceful error handling and resource lifecycle management
        - Support for dynamic session discovery from enterprise controllers

    Usage:
        The registry must be initialized before use and properly closed when no longer needed:
        ```python
        registry = CombinedSessionRegistry()
        await registry.initialize(config_manager)

        # Get all available sessions
        all_sessions = await registry.get_all()

        # Get a specific session
        session = await registry.get("enterprise:factory1:session1")

        # Close the registry when done
        await registry.close()
        ```

    Thread Safety:
        All methods in this class are designed to be coroutine-safe and can be
        called concurrently from multiple tasks. Internal synchronization ensures
        consistent state.
    """

    @staticmethod
    def _make_enterprise_session_manager(
        factory: CorePlusSessionFactoryManager, factory_name: str, session_name: str
    ) -> EnterpriseSessionManager:
        """Create an EnterpriseSessionManager for a specific session.

        This method creates a new EnterpriseSessionManager that wraps a session connection
        from the specified factory. It provides a closure over the factory that uses the
        factory's connect_to_persistent_query method to establish the session connection.

        The closure pattern used here captures the factory reference in the creation_function,
        allowing the EnterpriseSessionManager to lazily initialize the connection only when
        needed. This approach ensures efficient resource usage by deferring the actual
        connection establishment until the session is accessed.

        The resulting EnterpriseSessionManager will handle lifecycle management for the
        session, including lazy initialization and proper cleanup.

        Args:
            factory: The CorePlusSessionFactoryManager instance that will create the session.
            factory_name: The string identifier for the factory (used as the session's 'source').
            session_name: The name of the persistent query session to connect to.

        Returns:
            EnterpriseSessionManager: A new manager that provides access to the enterprise session.

        Concurrency:
            This method is coroutine-safe and can be called concurrently.
        """

        async def creation_function(source: str, name: str) -> CorePlusSession:
            factory_instance = await factory.get()
            return await factory_instance.connect_to_persistent_query(name)

        return EnterpriseSessionManager(
            source=factory_name,
            name=session_name,
            creation_function=creation_function,
        )

    def __init__(self) -> None:
        """
        Initialize the combined session registry.

        Creates a new registry instance with separate storage for community and enterprise
        registries, and initializes the controller client cache. This constructor does not
        perform any I/O operations or connect to any resources - the registry must be
        explicitly initialized with the `initialize()` method before use.

        Thread Safety:
            The constructor itself is thread-safe, and the resulting registry provides
            thread safety through asyncio locks for all operations.
        """
        super().__init__()
        # Separate storage for different registry types
        self._community_registry: CommunitySessionRegistry | None = None
        self._enterprise_registry: CorePlusSessionFactoryRegistry | None = None
        # Dictionary to store controller clients for each factory
        self._controller_clients: dict[str, CorePlusControllerClient] = {}

    async def initialize(self, config_manager: ConfigManager) -> None:
        """
        Initialize community and enterprise registries from configuration.

        This method discovers and initializes both community session registries
        and enterprise session factory registries based on the provided
        configuration manager. It performs the following steps:

        1. Creates and initializes the community session registry
        2. Creates and initializes the enterprise session factory registry
        3. Updates enterprise sessions by querying all available factories

        The initialization process is thread-safe and idempotent - calling this method
        multiple times will only perform the initialization once.

        Args:
            config_manager: The configuration manager containing session
                and factory configurations for both community and enterprise environments.

        Raises:
            Exception: Any exceptions from underlying registry initializations will
                be propagated to the caller.

        Thread Safety:
            This method is coroutine-safe and can be called concurrently.
            Internal synchronization ensures proper initialization.
        """
        async with self._lock:
            if self._initialized:  # Follow base registry pattern
                _LOGGER.warning("[%s] already initialized", self.__class__.__name__)
                return

            _LOGGER.info("[%s] initializing...", self.__class__.__name__)

            # Initialize community session registry
            self._community_registry = CommunitySessionRegistry()
            await self._community_registry.initialize(config_manager)
            _LOGGER.debug(
                "[%s] initialized community session registry", self.__class__.__name__
            )

            # Initialize enterprise session factory registry
            self._enterprise_registry = CorePlusSessionFactoryRegistry()
            await self._enterprise_registry.initialize(config_manager)
            _LOGGER.debug(
                "[%s] initialized enterprise session factory registry",
                self.__class__.__name__,
            )

            # Load static community sessions into _items
            _LOGGER.debug("[%s] loading community sessions", self.__class__.__name__)
            community_sessions = await self._community_registry.get_all()
            _LOGGER.debug(
                "[%s] loading %d community sessions",
                self.__class__.__name__,
                len(community_sessions),
            )

            for name, session in community_sessions.items():
                _LOGGER.debug(
                    "[%s] loading community session '%s'", self.__class__.__name__, name
                )
                self._items[name] = session

            _LOGGER.debug(
                "[%s] loaded %d community sessions",
                self.__class__.__name__,
                len(community_sessions),
            )

            # Update enterprise sessions from controller clients
            await self._update_enterprise_sessions()
            _LOGGER.debug(
                "[%s] populated enterprise sessions from controllers",
                self.__class__.__name__,
            )

            # Mark as initialized only after all steps complete successfully
            self._initialized = True
            _LOGGER.info("[%s] initialization complete", self.__class__.__name__)

    @override
    async def _load_items(self, config_manager: ConfigManager) -> None:
        """Raise an error as this method should not be called directly."""
        raise InternalError(
            "CombinedSessionRegistry does not support _load_items; use initialize() to set up sub-registries."
        )

    async def community_registry(self) -> CommunitySessionRegistry:
        """Get access to the community session registry.

        This method provides direct access to the underlying CommunitySessionRegistry
        instance, allowing specialized operations on community sessions that might not
        be available through the combined registry interface.

        The community registry manages session connections to local Deephaven Community
        Edition instances. It handles session creation, tracking, and lifecycle management
        for these connections.

        Returns:
            CommunitySessionRegistry: The community session registry instance for
                direct manipulation of community sessions.

        Raises:
            InternalError: If the combined registry has not been initialized.

        Thread Safety:
            This method is coroutine-safe and can be called concurrently.
            It acquires the registry lock to ensure thread safety.
        """
        async with self._lock:
            if not self._initialized:
                raise InternalError(
                    f"{self.__class__.__name__} not initialized. Call 'await initialize()' after construction."
                )
            # We know this is initialized at this point, so it's safe to cast
            return cast(CommunitySessionRegistry, self._community_registry)

    async def enterprise_registry(self) -> CorePlusSessionFactoryRegistry:
        """Get access to the enterprise session factory registry.

        This method provides direct access to the underlying CorePlusSessionFactoryRegistry
        instance, allowing specialized operations on enterprise session factories that might
        not be available through the combined registry interface.

        The enterprise registry manages connections to Deephaven Enterprise Edition CorePlus
        session factories. These factories create and manage enterprise sessions through
        controller clients that are cached by this combined registry.

        Returns:
            CorePlusSessionFactoryRegistry: The enterprise session factory registry instance
                for direct manipulation of enterprise session factories.

        Raises:
            InternalError: If the combined registry has not been initialized.

        Thread Safety:
            This method is coroutine-safe and can be called concurrently.
            It acquires the registry lock to ensure thread safety.
        """
        async with self._lock:
            if not self._initialized:
                raise InternalError(
                    f"{self.__class__.__name__} not initialized. Call 'await initialize()' after construction."
                )
            # We know this is initialized at this point, so it's safe to cast
            return cast(CorePlusSessionFactoryRegistry, self._enterprise_registry)

    async def _get_or_create_controller_client(
        self, factory: CorePlusSessionFactoryManager, factory_name: str
    ) -> CorePlusControllerClient:
        """Get a cached controller client or create a new one with health checking.

        This method implements intelligent caching of controller clients to optimize
        resource usage and improve performance. It follows this logic:

        1. Check if a cached controller client exists for the factory
        2. If a cached client exists, verify its health by attempting a ping() call
        3. If the cached client is healthy, return it
        4. If the cached client is dead or no cached client exists, create a new one
        5. Subscribe the new client to receive updates and cache it for future use

        This approach ensures efficient reuse of connections while maintaining reliability
        through automatic recreation of failed clients. The health check verifies that the
        client can still communicate with the controller before reusing it.

        Args:
            factory: The CorePlusSessionFactoryManager instance used to create controller clients
                if needed.
            factory_name: The name of the factory, used as a key in the controller client cache
                and for logging purposes.

        Returns:
            CorePlusControllerClient: A healthy controller client for the factory, either from
                cache or newly created.

        Raises:
            Exception: Any exception during controller client creation or health checking is
                logged but not propagated, as this method will attempt recovery by creating
                a new client.
        """
        # Check if we have a cached controller client
        if factory_name in self._controller_clients:
            try:
                # Check if the client is still alive
                client = self._controller_clients[factory_name]
                # We'll consider a successful ping() call (returns True) as proof of liveness
                ping_result = await client.ping()
                if not ping_result:
                    raise DeephavenConnectionError(
                        "Controller client ping returned False, indicating authentication issue"
                    )
                _LOGGER.debug(
                    "[%s] using cached controller client for factory '%s'",
                    self.__class__.__name__,
                    factory_name,
                )
                return client
            except Exception as e:
                # If there's any error, close the old client and create a new one
                _LOGGER.warning(
                    "[%s] controller client for factory '%s' is dead: %s. Creating a new one.",
                    self.__class__.__name__,
                    factory_name,
                    e,
                )
                try:
                    await self._controller_clients[factory_name].close()
                except Exception as close_e:
                    _LOGGER.warning(
                        "[%s] error closing dead controller client: %s",
                        self.__class__.__name__,
                        close_e,
                    )
                # Remove the dead client from cache
                self._controller_clients.pop(factory_name, None)

        # Create a new controller client
        _LOGGER.debug(
            "[%s] creating new controller client for factory '%s'",
            self.__class__.__name__,
            factory_name,
        )
        factory_instance = await factory.get()
        client = await factory_instance.create_controller_client()
        await client.subscribe()
        _LOGGER.debug(
            "[%s] subscribed to controller for factory '%s'",
            self.__class__.__name__,
            factory_name,
        )

        # Cache the client
        self._controller_clients[factory_name] = client
        return client

    def _add_new_enterprise_sessions(
        self,
        factory: CorePlusSessionFactoryManager,
        factory_name: str,
        session_names: set[str],
    ) -> None:
        """Create and add new enterprise session managers to the registry.

        This method creates EnterpriseSessionManager instances for each session name
        and adds them to the registry's internal storage. Each session manager is
        created with a closure that connects to the persistent query session through
        the factory.

        Session keys are constructed using BaseItemManager.make_full_name with the format:
        SystemType.ENTERPRISE:factory_name:session_name
        This ensures consistent key formatting throughout the registry for storage,
        retrieval, and existence checks. The colon-separated format is used across
        all registry operations.

        Args:
            factory: The CorePlusSessionFactoryManager to create sessions from.
            factory_name: The name of the factory (used as the session source).
            session_names: Set of session names to create managers for.
        """
        for session_name in session_names:
            key = BaseItemManager.make_full_name(
                SystemType.ENTERPRISE, factory_name, session_name
            )
            if key not in self._items:
                session_manager = self._make_enterprise_session_manager(
                    factory, factory_name, session_name
                )
                self._items[session_manager.full_name] = session_manager
                _LOGGER.debug(
                    "[%s] created and stored EnterpriseSessionManager for '%s'",
                    self.__class__.__name__,
                    session_manager.full_name,
                )

    async def _close_stale_enterprise_sessions(self, stale_keys: set[str]) -> None:
        """Close and remove stale enterprise session managers from the registry.

        This method handles cleanup of session managers that are no longer available
        on the enterprise controller. It removes them from the registry first to
        prevent further access, then attempts to close them gracefully.

        Args:
            stale_keys: Set of fully qualified session keys to close and remove.
        """
        for key in stale_keys:
            # Remove the manager from the registry first. This ensures that even if
            # closing fails, the stale manager is no longer available.
            manager = self._items.pop(key, None)
            if not manager:
                continue

            await manager.close()

    def _find_session_keys_for_factory(self, factory_name: str) -> set[str]:
        """Find all session keys associated with a specific factory.

        Args:
            factory_name: The name of the factory to find sessions for.

        Returns:
            set[str]: A set of session keys for the specified factory.
        """
        prefix = BaseItemManager.make_full_name(SystemType.ENTERPRISE, factory_name, "")
        return {k for k in self._items if k.startswith(prefix)}

    async def _remove_all_sessions_for_factory(self, factory_name: str) -> None:
        """Remove all sessions for a specific factory when the system is offline.

        This method finds all session keys associated with the given factory and
        closes/removes them from the registry.

        Args:
            factory_name: The name of the factory to remove sessions for.
        """
        _LOGGER.warning(
            "[%s] removing all sessions for offline factory '%s'",
            self.__class__.__name__,
            factory_name,
        )

        # Find all sessions for this factory and remove them
        keys_to_remove = self._find_session_keys_for_factory(factory_name)
        await self._close_stale_enterprise_sessions(keys_to_remove)

        _LOGGER.info(
            "[%s] removed %d sessions for offline factory '%s'",
            self.__class__.__name__,
            len(keys_to_remove),
            factory_name,
        )

    async def _update_sessions_for_factory(
        self, factory: CorePlusSessionFactoryManager, factory_name: str
    ) -> None:
        """
        Update the sessions for a single enterprise factory.

        This method attempts to connect to the factory's controller client to retrieve
        the current list of available sessions. It then synchronizes the registry by:
        - Adding new sessions that are present on the controller but not in the registry.
        - Removing stale sessions that are no longer present on the controller.

        If a DeephavenConnectionError occurs (e.g., the system is offline or unreachable),
        all sessions for that factory will be removed from the registry and closed.
        Only connection-related exceptions trigger this removal; all other exceptions
        are propagated to the caller for visibility and debugging.

        Args:
            factory: The CorePlusSessionFactoryManager to update sessions for.
            factory_name: The name of the factory being updated.
        """
        _LOGGER.info(
            "[%s] updating enterprise sessions for factory '%s'",
            self.__class__.__name__,
            factory_name,
        )

        try:
            # These two operations can fail if the system is offline
            controller_client = await self._get_or_create_controller_client(
                factory, factory_name
            )
            session_info = await controller_client.map()
        except DeephavenConnectionError as e:
            _LOGGER.warning(
                "[%s] failed to connect to factory '%s': %s",
                self.__class__.__name__,
                factory_name,
                e,
            )
            # If we can't connect to the factory, remove all sessions for it
            await self._remove_all_sessions_for_factory(factory_name)
            return

        # If we successfully connected, proceed with normal session update
        session_names_from_controller = [
            si.config.pb.name for si in session_info.values()
        ]
        _LOGGER.debug(
            "[%s] factory '%s' reports %d sessions: %s",
            self.__class__.__name__,
            factory_name,
            len(session_names_from_controller),
            session_names_from_controller,
        )

        existing_keys = self._find_session_keys_for_factory(factory_name)
        _LOGGER.debug(
            "[%s] factory '%s' has %d existing sessions in registry",
            self.__class__.__name__,
            factory_name,
            len(existing_keys),
        )

        controller_keys = {
            BaseItemManager.make_full_name(SystemType.ENTERPRISE, factory_name, name)
            for name in session_names_from_controller
        }

        new_session_names = {
            name
            for name in session_names_from_controller
            if BaseItemManager.make_full_name(SystemType.ENTERPRISE, factory_name, name)
            not in existing_keys
        }
        if new_session_names:
            _LOGGER.debug(
                "[%s] factory '%s' adding %d new sessions: %s",
                self.__class__.__name__,
                factory_name,
                len(new_session_names),
                list(new_session_names),
            )
        self._add_new_enterprise_sessions(factory, factory_name, new_session_names)

        stale_keys = existing_keys - controller_keys
        if stale_keys:
            _LOGGER.debug(
                "[%s] factory '%s' removing %d stale sessions: %s",
                self.__class__.__name__,
                factory_name,
                len(stale_keys),
                list(stale_keys),
            )
        await self._close_stale_enterprise_sessions(stale_keys)

        _LOGGER.info(
            "[%s] enterprise session update complete for factory '%s'",
            self.__class__.__name__,
            factory_name,
        )

    async def _update_enterprise_sessions(self) -> None:
        """Update enterprise sessions by querying all factories and syncing sessions.

        This method iterates through all registered enterprise factories and updates
        their sessions by querying their controller clients. It ensures the registry
        has the most current view of available enterprise sessions.

        Raises:
            InternalError: If the registry has not been initialized.
            Exception: Any exception from factory session updates.
        """
        _LOGGER.info("[%s] Updating enterprise sessions", self.__class__.__name__)
        self._check_initialized()

        _LOGGER.debug("[%s] Getting all factories", self.__class__.__name__)
        # We know this is initialized at this point, so it's safe to cast
        factories = await cast(
            CorePlusSessionFactoryRegistry, self._enterprise_registry
        ).get_all()
        _LOGGER.debug("[%s] Got %d factories", self.__class__.__name__, len(factories))

        for factory_name, factory in factories.items():
            _LOGGER.debug(
                "[%s] Updating sessions for factory '%s'",
                self.__class__.__name__,
                factory_name,
            )
            await self._update_sessions_for_factory(factory, factory_name)

        _LOGGER.info("[%s] Updated enterprise sessions", self.__class__.__name__)

    @override
    async def get(self, name: str) -> BaseItemManager:
        """Retrieve a specific session manager from the registry by its fully qualified name.

        This method provides access to any session manager (community or enterprise)
        by its fully qualified name. Before retrieving the item, it updates the enterprise
        sessions to ensure that the registry has the latest information about available
        enterprise sessions.

        The name must be a fully qualified name in the format:
        - For community sessions: "community:<source>:<name>"
        - For enterprise sessions: "enterprise:<factory_name>:<session_name>"

        Args:
            name: The fully qualified name of the session manager to retrieve.

        Returns:
            BaseItemManager: The session manager corresponding to the given name.
                This could be either a CommunitySessionManager or an EnterpriseSessionManager.

        Raises:
            InternalError: If the registry has not been initialized.
            KeyError: If no session manager with the given name is found in the registry.
            Exception: If any error occurs while updating enterprise sessions.

        Thread Safety:
            This method is coroutine-safe and can be called concurrently.
            It acquires the registry lock to ensure thread safety.
        """
        async with self._lock:

            # Check initialization and raise KeyError if item not found
            # (avoid calling super().get() which would try to acquire the lock again)
            self._check_initialized()

            # Update enterprise sessions before retrieving (lock is already held)
            # This also checks initialization status
            await self._update_enterprise_sessions()

            if name not in self._items:
                raise KeyError(
                    f"No item with name '{name}' found in {self.__class__.__name__}"
                )

            return self._items[name]

    @override
    async def get_all(self) -> dict[str, BaseItemManager]:
        """Retrieve all session managers from both community and enterprise registries.

        This method returns a unified view of all available sessions across both
        community and enterprise registries. Before returning the results, it updates
        the enterprise sessions to ensure that the most current state is available.

        The returned dictionary is a copy, so modifications to it will not affect
        the registry's internal state. The keys in the dictionary are fully qualified
        names, and the values are the corresponding session manager instances.

        Returns:
            dict[str, BaseItemManager]: A dictionary containing all registered session managers,
                with fully qualified names as keys and manager instances as values.

        Raises:
            InternalError: If the registry has not been initialized.
            Exception: If any error occurs while updating enterprise sessions.

        Thread Safety:
            This method is coroutine-safe and can be called concurrently.
            It acquires the registry lock to ensure thread safety.
        """
        async with self._lock:
            # Check initialization first
            self._check_initialized()

            # Update enterprise sessions before retrieving (lock is already held)
            # This also checks initialization status
            _LOGGER.info(
                "[%s] Updating enterprise sessions before retrieving",
                self.__class__.__name__,
            )
            await self._update_enterprise_sessions()

            _LOGGER.info("[%s] Returning all sessions", self.__class__.__name__)
            return self._items.copy()

    @override
    async def close(self) -> None:
        """Close the registry and release all resources managed by it.

        This method performs an orderly shutdown of all resources managed by this registry:

        1. Closes the community session registry and all its managed sessions
        2. Closes the enterprise session factory registry and all its managed factories
        3. Closes all cached controller clients

        The method handles errors during closure gracefully, ensuring that all resources
        are attempted to be closed even if some failures occur. Each closure operation
        is performed independently, and errors in one will not prevent attempts to close
        other resources.

        After this method completes successfully, the registry should not be used again.
        A new registry should be created and initialized if needed.

        Raises:
            InternalError: If the registry has not been initialized.
            Exception: Any exceptions from closing operations are logged but not propagated.

        Thread Safety:
            This method is coroutine-safe and can be called concurrently.
            It acquires the registry lock to ensure thread safety during closure.
        """
        async with self._lock:
            if not self._initialized:
                raise InternalError(
                    f"{self.__class__.__name__} not initialized. Call 'await initialize()' after construction."
                )

            _LOGGER.info("[%s] closing...", self.__class__.__name__)

            # Close community registry
            if self._community_registry is not None:
                try:
                    await self._community_registry.close()
                    _LOGGER.debug(
                        "[%s] closed community registry", self.__class__.__name__
                    )
                except Exception as e:
                    _LOGGER.error(
                        "[%s] error closing community registry: %s",
                        self.__class__.__name__,
                        e,
                    )

            # Close enterprise registry
            if self._enterprise_registry is not None:
                try:
                    await self._enterprise_registry.close()
                    _LOGGER.debug(
                        "[%s] closed enterprise registry", self.__class__.__name__
                    )
                except Exception as e:
                    _LOGGER.error(
                        "[%s] error closing enterprise registry: %s",
                        self.__class__.__name__,
                        e,
                    )

            # Close all controller clients
            for factory_name, client in list(self._controller_clients.items()):
                try:
                    await client.close()
                    _LOGGER.debug(
                        "[%s] closed controller client for factory '%s'",
                        self.__class__.__name__,
                        factory_name,
                    )
                except Exception as e:
                    _LOGGER.error(
                        "[%s] error closing controller client for factory '%s': %s",
                        self.__class__.__name__,
                        factory_name,
                        e,
                    )

            # Clear the controller clients dictionary
            self._controller_clients.clear()

            _LOGGER.info("[%s] closed", self.__class__.__name__)
