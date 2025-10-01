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
    - Session tracking for MCP-created sessions with counting capabilities
    - Enterprise session discovery via controller clients
    - Graceful error handling and resource cleanup
    - Public API methods for adding/removing sessions with proper validation

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
    await registry.close()  # Properly closes all resources and manages resource cleanup
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
        - Intelligent enterprise session discovery via controller clients

    Key Features:
        - Unified API for managing heterogeneous session resources
        - Session tracking with counting capabilities for MCP-created sessions
        - Controller client health monitoring and management
        - Efficient session resource reuse and cleanup
        - Thread-safe operations with proper asyncio locking
        - Graceful error handling and resource lifecycle management
        - Support for dynamic session discovery from enterprise controllers
        - Public API for adding/removing sessions with validation

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

        Creates a new registry that provides unified management of both community
        and enterprise session resources. The registry establishes the foundational
        architecture for handling heterogeneous session types with a single,
        consistent interface.

        Architecture Initialized:
            - Community session registry (None until initialize() is called)
            - Enterprise session factory registry (None until initialize() is called)
            - Controller client cache for efficient enterprise session management
            - Session tracking set for MCP-created session counting
            - Thread-safe locking mechanism for concurrent access

        State After Construction:
            The registry is created but not functional until initialize() is called
            with a ConfigManager. All internal registries are None and no sessions
            are available until proper initialization occurs.

        Initialization Required:
            After construction, you MUST call initialize(config_manager) before
            using any registry methods. Attempting to use the registry before
            initialization will raise InternalError exceptions.

        Session Tracking:
            The registry maintains a tracking set (_added_sessions) that records
            sessions explicitly added via add_session(). This enables counting
            of MCP-created sessions separate from discovered sessions.

        Thread Safety:
            The constructor initializes thread-safe components including an asyncio
            lock that ensures all registry operations can be safely called from
            multiple concurrent tasks.

        Example:
            ```python
            # Create registry (not yet functional)
            registry = CombinedSessionRegistry()

            # Initialize with configuration (now functional)
            await registry.initialize(config_manager)

            # Now ready for use
            sessions = await registry.get_all()
            ```
        """
        super().__init__()
        # Separate storage for different registry types
        self._community_registry: CommunitySessionRegistry | None = None
        self._enterprise_registry: CorePlusSessionFactoryRegistry | None = None
        # Dictionary to store controller clients for each factory
        self._controller_clients: dict[str, CorePlusControllerClient] = {}
        # Track sessions added to this registry instance
        # Format: {session_id1, session_id2, ...}
        self._added_sessions: set[str] = set()

    async def initialize(self, config_manager: ConfigManager) -> None:
        """
        Initialize community and enterprise registries from configuration.

        This method discovers and initializes both community session registries
        and enterprise session factory registries based on the provided
        configuration manager. It performs the following steps:

        1. Creates and initializes the community session registry
        2. Creates and initializes the enterprise session factory registry
        3. Loads static community sessions into the registry
        4. Updates enterprise sessions by querying all available factories

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
                # Use the session's full_name (which is properly encoded) as the key
                self._items[session.full_name] = session

            _LOGGER.debug(
                "[%s] loaded %d community sessions",
                self.__class__.__name__,
                len(community_sessions),
            )

            # Mark as initialized before updating enterprise sessions since they check initialization
            self._initialized = True

            # Update enterprise sessions from controller clients
            await self._update_enterprise_sessions()
            _LOGGER.debug(
                "[%s] populated enterprise sessions from controllers",
                self.__class__.__name__,
            )

            _LOGGER.info("[%s] initialization complete", self.__class__.__name__)

    @override
    async def _load_items(self, config_manager: ConfigManager) -> None:
        """Raise an error as this method should not be called directly.

        Args:
            config_manager: The configuration manager (unused in this implementation).

        Raises:
            InternalError: Always raised to indicate this method should not be used.
        """
        raise InternalError(
            "CombinedSessionRegistry does not support _load_items; use initialize() to set up sub-registries."
        )

    async def community_registry(self) -> CommunitySessionRegistry:
        """Get direct access to the community session registry.

        This method provides access to the underlying CommunitySessionRegistry instance,
        allowing specialized operations on community sessions that might not be available
        through the combined registry's unified interface.

        The community registry manages session connections to local Deephaven Community
        Edition instances, handling session creation, configuration-based initialization,
        and lifecycle management for local or containerized Deephaven deployments.

        Use this method when you need community-specific functionality such as:
        - Direct access to community session configuration
        - Community-specific health checking or diagnostics
        - Advanced community session management operations

        Returns:
            CommunitySessionRegistry: The community session registry instance that
                manages all community sessions loaded from configuration.

        Raises:
            InternalError: If the combined registry has not been initialized via initialize().

        Thread Safety:
            This method is coroutine-safe and can be called concurrently.
            It acquires the registry lock to ensure thread safety.

        Example:
            ```python
            # Get direct access to community registry
            community_reg = await combined_registry.community_registry()

            # Perform community-specific operations
            community_sessions = await community_reg.get_all()
            for session_id, manager in community_sessions.items():
                print(f"Community session: {session_id}")
            ```
        """
        async with self._lock:
            if not self._initialized:
                raise InternalError(
                    f"{self.__class__.__name__} not initialized. Call 'await initialize()' after construction."
                )
            # We know this is initialized at this point, so it's safe to cast
            return cast(CommunitySessionRegistry, self._community_registry)

    async def enterprise_registry(self) -> CorePlusSessionFactoryRegistry:
        """Get direct access to the enterprise session factory registry.

        This method provides access to the underlying CorePlusSessionFactoryRegistry
        instance, allowing specialized operations on enterprise session factories that
        might not be available through the combined registry's unified interface.

        The enterprise registry manages CorePlusSessionFactory instances that connect
        to Deephaven Enterprise Edition deployments. These factories are responsible
        for creating enterprise sessions through controller clients, handling advanced
        authentication, and managing enterprise-specific features.

        Use this method when you need enterprise-specific functionality such as:
        - Direct factory management and configuration
        - Enterprise-specific authentication handling
        - Advanced controller client operations
        - Factory-level health monitoring and diagnostics

        Returns:
            CorePlusSessionFactoryRegistry: The enterprise session factory registry
                instance that manages all enterprise factories loaded from configuration.

        Raises:
            InternalError: If the combined registry has not been initialized via initialize().

        Thread Safety:
            This method is coroutine-safe and can be called concurrently.
            It acquires the registry lock to ensure thread safety.

        Example:
            ```python
            # Get direct access to enterprise registry
            enterprise_reg = await combined_registry.enterprise_registry()

            # Perform enterprise-specific operations
            factories = await enterprise_reg.get_all()
            for factory_name, factory_manager in factories.items():
                print(f"Enterprise factory: {factory_name}")
                # Get the actual factory instance
                factory = await factory_manager.get()
                # Check factory health
                health = await factory.ping()
                print(f"  Health: {'OK' if health else 'FAILED'}")
            ```
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
        5. Cache the new client for future use

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
                    "[%s] controller client for factory '%s' is dead: %s. Releasing reference to dead controller client.",
                    self.__class__.__name__,
                    factory_name,
                    e,
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
        client = factory_instance.controller_client

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
        prevent further access, then attempts to close the session managers gracefully.

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

        This method finds all session keys associated with the given factory,
        removes them from the registry, and properly cleans up the session resources.

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
        - Adding new sessions that are present on the controller but not in the registry
        - Removing stale sessions that are no longer present on the controller

        The synchronization process ensures the registry accurately reflects the current
        state of sessions available on the enterprise controller, providing a consistent
        view for clients accessing the registry.

        If a DeephavenConnectionError occurs (e.g., the system is offline or unreachable),
        all sessions for that factory will be removed from the registry and their resources
        cleaned up. Only connection-related exceptions trigger this removal; all other
        exceptions are propagated to the caller for visibility and debugging.

        Args:
            factory (CorePlusSessionFactoryManager): The factory manager to update sessions for.
            factory_name (str): The name of the factory being updated, used for logging and
                session identification.

        Raises:
            Exception: Any non-connection exceptions during session discovery are propagated
                to allow proper error handling by the caller.
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
        enterprise sessions from all controller clients.

        The registry will automatically discover and add new enterprise sessions if they
        have been created since the last update, ensuring you get access to the most
        current session state.

        Name Format:
            The name must be a fully qualified identifier following the format:
                "<system_type>:<source>:<name>"

            Examples:
            - Community sessions: "community:local_config:worker-1"
            - Enterprise sessions: "enterprise:prod-factory:analytics-session"

        Args:
            name (str): The fully qualified name of the session manager to retrieve.
                Must follow the format "<system_type>:<source>:<name>".

        Returns:
            BaseItemManager: The session manager corresponding to the given name.
                This will be either a CommunitySessionManager or EnterpriseSessionManager
                depending on the session type.

        Raises:
            InternalError: If the registry has not been initialized via initialize().
            KeyError: If no session manager with the given name is found in the registry
                after updating enterprise sessions.
            Exception: If any error occurs while updating enterprise sessions from controllers.

        Thread Safety:
            This method is coroutine-safe and can be called concurrently.
            It acquires the registry lock to ensure thread safety.

        Example:
            ```python
            # Get a community session
            community_mgr = await registry.get("community:local_config:worker-1")

            # Get an enterprise session
            enterprise_mgr = await registry.get("enterprise:prod-factory:analytics-session")
            ```
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
        the enterprise sessions by querying all controller clients to ensure that
        the most current session state is available, including any newly created
        sessions that may have been added since the last update.

        The returned dictionary includes:
        - All community sessions loaded from configuration during initialization
        - All enterprise sessions discovered from controller clients
        - Both tracked sessions (added via add_session) and discovered sessions

        The returned dictionary is a copy, so modifications to it will not affect
        the registry's internal state. This ensures safe iteration and manipulation
        without affecting the registry's consistency.

        Returns:
            dict[str, BaseItemManager]: A dictionary mapping fully qualified session
                names to their corresponding session manager instances. Keys follow
                the format "<system_type>:<source>:<name>" and values are either
                CommunitySessionManager or EnterpriseSessionManager instances.

        Raises:
            InternalError: If the registry has not been initialized via initialize().
            Exception: If any error occurs while updating enterprise sessions from
                controller clients (network errors, authentication failures, etc.).

        Thread Safety:
            This method is coroutine-safe and can be called concurrently.
            It acquires the registry lock to ensure thread safety during both
            enterprise session updates and registry access.

        Example:
            ```python
            # Get all available sessions
            all_sessions = await registry.get_all()

            # Iterate through all sessions
            for session_id, manager in all_sessions.items():
                print(f"Found session: {session_id}")
                if session_id.startswith("enterprise:"):
                    print("  Type: Enterprise session")
                elif session_id.startswith("community:"):
                    print("  Type: Community session")

            # Filter for specific system
            prod_sessions = {
                k: v for k, v in all_sessions.items()
                if ":prod-factory:" in k
            }
            ```
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

    async def add_session(self, manager: BaseItemManager) -> None:
        """Add a session manager to the registry with tracking.

        This method adds a session manager to the registry and marks it as tracked for
        counting purposes. This is typically used by the MCP server to register sessions
        that it has created, allowing for proper session lifecycle management and counting.

        The operation is not idempotent - attempting to add a session that already exists
        will raise a ValueError to catch programming errors early. This fail-fast behavior
        helps identify duplicate session creation attempts.

        The session will be tracked in the registry's internal session tracking set,
        which is used by count_added_sessions() to determine how many sessions were
        created by the MCP server that still exist.

        Args:
            manager (BaseItemManager): The session manager to add to the registry.
                Must have a unique full_name that doesn't already exist in the registry.

        Raises:
            ValueError: If a session with the same full_name already exists in the registry.
                The error message will include the conflicting session ID.
            InternalError: If the registry has not been initialized via initialize().

        Thread Safety:
            This method is coroutine-safe and can be called concurrently.
            It acquires the registry lock to ensure thread safety.

        Example:
            ```python
            # Add a newly created session to the registry
            session_manager = EnterpriseSessionManager(
                source="prod-factory",
                name="analytics-session",
                creation_function=creation_func
            )
            await registry.add_session(session_manager)
            ```
        """
        async with self._lock:
            # Check initialization first
            self._check_initialized()

            session_id = manager.full_name
            if session_id in self._items:
                raise ValueError(f"Session '{session_id}' already exists in registry")

            self._items[session_id] = manager
            self._added_sessions.add(session_id)

            _LOGGER.debug(
                "[%s] added session '%s' to registry",
                self.__class__.__name__,
                session_id,
            )

    async def remove_session(self, session_id: str) -> BaseItemManager | None:
        """Remove and return a session manager from the registry with tracking cleanup.

        This method removes a session manager from the registry and also removes it
        from the internal tracking set used by count_added_sessions(). This ensures
        proper cleanup of sessions that were previously added via add_session().

        The operation is idempotent - calling it multiple times with the same
        session_id is safe and will return None for subsequent calls. This design
        makes it safe to use in cleanup code that might be called multiple times.

        Args:
            session_id (str): The fully qualified session identifier to remove, in the
                format "<system_type>:<source>:<name>" (e.g., "enterprise:factory1:session1").

        Returns:
            BaseItemManager | None: The removed session manager if it existed in the
                registry, or None if no session with the given ID was found.

        Note:
            This method does not raise an exception if the session is not found,
            as sessions may be removed by external systems, timeout naturally, or
            be removed by automatic cleanup processes. Callers should check the
            return value if they need to know whether the session existed.

        Raises:
            InternalError: If the registry has not been initialized via initialize().

        Thread Safety:
            This method is coroutine-safe and can be called concurrently.
            It acquires the registry lock to ensure thread safety during both
            registry and tracking cleanup.

        Example:
            ```python
            # Remove a session and check if it existed
            removed = await registry.remove_session("enterprise:factory1:session1")
            if removed is not None:
                print(f"Removed session: {removed.full_name}")
                # Optionally close the removed session
                await removed.close()
            else:
                print("Session was not found in registry")
            ```
        """
        async with self._lock:
            # Check initialization first
            self._check_initialized()

            removed_manager = self._items.pop(session_id, None)
            if removed_manager is not None:
                # Update tracking for all added sessions
                self._added_sessions.discard(session_id)

                _LOGGER.debug(
                    "[%s] removed session '%s' from registry",
                    self.__class__.__name__,
                    session_id,
                )
            return removed_manager

    async def count_added_sessions(
        self, system_type: str | SystemType, system_name: str
    ) -> int:
        """Count sessions added to this registry for a specific system that still exist.

        This method provides the functionality previously handled by the global
        _created_sessions tracking in the MCP server. It counts only sessions that
        were explicitly added via add_session() and filters for the specified system.

        Args:
            system_type (str | SystemType): The system type to filter by. Can be either
                a string ("enterprise", "community") or a SystemType enum value
                (SystemType.ENTERPRISE, SystemType.COMMUNITY). Enum values are converted
                to their string representation automatically.
            system_name (str): The system name to check (e.g., "prod-system", "dev-env").

        Returns:
            int: Number of sessions we have added that still exist in the registry.

        Raises:
            InternalError: If the registry has not been initialized via initialize().

        Thread Safety:
            This method is thread-safe and acquires the registry lock.

        Example:
            ```python
            # Count enterprise sessions using string
            count = await registry.count_added_sessions("enterprise", "prod-system")

            # Count enterprise sessions using enum
            count = await registry.count_added_sessions(SystemType.ENTERPRISE, "prod-system")

            # Count community sessions
            count = await registry.count_added_sessions(SystemType.COMMUNITY, "local-dev")
            ```
        """
        # Convert SystemType enum to string if needed
        type_str = (
            system_type.value if isinstance(system_type, SystemType) else system_type
        )

        existing_count = 0
        stale_sessions = []

        # Use lock to ensure consistent state during counting
        async with self._lock:
            # Check initialization first
            self._check_initialized()

            # Filter sessions that match the system type and name
            for session_id in self._added_sessions:
                try:
                    s_type, s_source, s_name = BaseItemManager.parse_full_name(
                        session_id
                    )
                    if s_type == type_str and s_source == system_name:
                        if session_id in self._items:
                            existing_count += 1
                        else:
                            # Session no longer exists, mark for cleanup
                            stale_sessions.append(session_id)
                except ValueError:
                    # Invalid session ID format, mark for cleanup
                    stale_sessions.append(session_id)

            # Clean up stale sessions
            for session_id in stale_sessions:
                self._added_sessions.discard(session_id)

        return existing_count

    async def is_added_session(self, session_id: str) -> bool:
        """Check if a session is tracked as added to this registry.

        This method determines whether a session was explicitly added to this registry
        via the add_session() method. This is different from checking if a session
        exists in the registry, as sessions can exist through discovery (enterprise)
        or initialization (community) without being explicitly added.

        Sessions tracked by this method are counted by count_added_sessions() and
        represent sessions that were created through the MCP server's session
        creation process.

        Args:
            session_id (str): The fully qualified session ID to check, in the format
                "<system_type>:<source>:<name>" (e.g., "enterprise:factory1:session1").

        Returns:
            bool: True if the session was explicitly added via add_session(),
                False if it was never added or has been removed.

        Raises:
            InternalError: If the registry has not been initialized via initialize().

        Thread Safety:
            This method is thread-safe and acquires the registry lock to ensure
            consistent reads from the tracking set.

        Example:
            ```python
            # Check if a session was added by the MCP server
            if await registry.is_added_session("enterprise:factory1:session1"):
                print("Session was created by MCP server")
            else:
                print("Session was discovered or doesn't exist")
            ```
        """
        async with self._lock:
            # Check initialization first
            self._check_initialized()
            return session_id in self._added_sessions

    @override
    async def close(self) -> None:
        """Close the registry and release all resources managed by it.

        This method performs an orderly shutdown of all resources managed by this registry:

        1. Closes the community session registry and all its managed sessions
        2. Closes the enterprise session factory registry and all its managed factories
        3. Properly manages the shutdown of cached controller clients
        4. Resets the initialization flag to allow reinitialization
        5. Clears session tracking state

        The method handles errors during closure gracefully, ensuring that all resources
        are attempted to be closed even if some failures occur. Each closure operation
        is performed independently, and errors in one will not prevent attempts to close
        other resources.

        After this method completes successfully, the registry can be reinitialized
        by calling initialize() again with a new configuration.

        Raises:
            InternalError: If the registry has not been initialized.

        Note:
            Exceptions from closing sub-registries are logged but not propagated to
            ensure all cleanup operations are attempted.

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

            # Log that we're releasing controller clients
            # (Note: CorePlusControllerClient doesn't have a close() method; clients are managed by the CorePlus system)
            for factory_name, _ in list(self._controller_clients.items()):
                _LOGGER.debug(
                    "[%s] releasing controller client for factory '%s'",
                    self.__class__.__name__,
                    factory_name,
                )

            # Clear the controller clients dictionary
            self._controller_clients.clear()

            # Reset initialization flag to allow reinitialization
            self._initialized = False

            # Clear our session tracking
            session_count = len(self._added_sessions)
            self._added_sessions.clear()
            _LOGGER.debug(
                "[%s] cleared session tracking (%d sessions)",
                self.__class__.__name__,
                session_count,
            )

            _LOGGER.info("[%s] closed", self.__class__.__name__)
