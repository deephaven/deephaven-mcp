"""
Base classes and enums for Deephaven MCP session and factory management.

This module provides the core async/coroutine-safe building blocks for managing sessions and factories
in both community and enterprise Deephaven deployments. It defines generic, extensible manager types
that ensure safe, lazy initialization and lifecycle management for backend resources.

The managers use lazy initialization patterns with asyncio.Lock for thread safety, automatic resource
lifecycle management, and consistent error handling. They are designed to be used in registries that
manage collections of sessions or factories.

Key Classes:
    AsyncClosable: Protocol defining the async close() interface for managed resources.
    SystemType: Enum for backend system type (COMMUNITY, ENTERPRISE).
    BaseItemManager: Generic async manager for a single lazily-initialized item, providing
        locking, caching, liveness checking, and proper async cleanup.
    CommunitySessionManager: Manages a single CoreSession (community) with configuration-based
        lazy initialization, caching, and liveness monitoring.
    EnterpriseSessionManager: Manages a single CorePlusSession (enterprise) via a provided
        creation function that takes source and name parameters.
    CorePlusSessionFactoryManager: Manages a single CorePlusSessionFactory (enterprise),
        with liveness determined via ping() method calls.

Features:
    - Lazy initialization: Resources are created only when first accessed
    - Thread safety: All operations protected by asyncio.Lock
    - Automatic cleanup: Proper async resource disposal via close() methods
    - Liveness monitoring: Built-in health checking for managed resources
    - Error handling: Consistent exception wrapping and logging

All managers are designed to be coroutine-safe and are suitable for use in async applications.
"""

import asyncio
import enum
import logging
import sys
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Generic, Protocol, TypeVar

if TYPE_CHECKING:
    from typing_extensions import override  # pragma: no cover
elif sys.version_info >= (3, 12):
    from typing import override  # pragma: no cover
else:
    from typing_extensions import override  # pragma: no cover

from deephaven_mcp._exceptions import InternalError, SessionCreationError
from deephaven_mcp.client import (
    CorePlusSession,
    CorePlusSessionFactory,
    CoreSession,
)

_LOGGER = logging.getLogger(__name__)


class AsyncClosable(Protocol):
    """Protocol for types that provide an asynchronous close() method.

    Any resource or session type used with BaseItemManager must implement this protocol
    to ensure proper async cleanup. This allows mypy and other type checkers to verify
    that the managed item supports async close(), which is required for safe resource
    management in async contexts.

    This protocol is used as a type bound for the generic TypeVar T in BaseItemManager,
    ensuring that all managed items can be properly closed when no longer needed.

    Examples:
        CoreSession, CorePlusSession, and CorePlusSessionFactory all implement this
        protocol by providing an async close() method.
    """

    async def close(self) -> None:
        """Close the underlying resource or session.
        
        This method should perform all necessary cleanup operations, such as
        closing network connections, releasing resources, and notifying any
        dependent systems of the shutdown.
        
        Raises:
            Exception: May raise exceptions during cleanup, which should be
                handled by the caller.
        """
        ...  # pragma: no cover


T = TypeVar("T", bound=AsyncClosable)


class SystemType(str, enum.Enum):
    """Enum representing the types of Deephaven backend systems.
    
    This enum is used to categorize the different types of Deephaven deployments
    that can be managed by the session managers. Each type has different
    authentication requirements, capabilities, and management approaches.
    
    Attributes:
        COMMUNITY: Free, open-source Deephaven Community deployment.
            Typically runs locally or in simple containerized environments.
        ENTERPRISE: Commercial Deephaven Enterprise deployment with advanced
            features like authentication, scaling, and enterprise integrations.
    """

    COMMUNITY = "community"
    ENTERPRISE = "enterprise"


class BaseItemManager(Generic[T], ABC):
    """
    A generic, async, coroutine-safe base class for managing a single, lazily-initialized item.

    This abstract base class provides the foundation for managing any type of resource
    that implements the AsyncClosable protocol. It ensures thread-safe access patterns,
    proper resource lifecycle management, and consistent error handling across different
    types of managed resources.

    Core Features:
        - Lazy initialization: Items are created only when first accessed via get()
        - Caching: Once created, items are cached and reused for subsequent calls
        - Thread safety: All operations are protected by an asyncio.Lock
        - Liveness monitoring: Built-in support for checking if managed items are alive
        - Proper cleanup: Automatic resource disposal with async close() support
        - Consistent interface: Common API for all resource management operations

    Type Parameters:
        T: The type of item being managed, must implement AsyncClosable protocol

    Thread Safety:
        All public methods are coroutine-safe and can be called concurrently
        from multiple tasks without risk of race conditions.

    Usage Pattern:
        Subclasses must implement _create_item() and _check_liveness() methods
        to define how items are created and their health is monitored.
    """

    def __init__(self, system_type: SystemType, source: str, name: str):
        """
        Initialize the manager with system identification and configuration.

        Sets up the manager with the necessary metadata for resource identification
        and initializes the internal state for lazy loading and thread safety.

        Args:
            system_type: The type of Deephaven system (COMMUNITY or ENTERPRISE).
                This determines the management approach and capabilities.
            source: The configuration source identifier (e.g., file path, URL, or
                configuration key). Used for grouping and identification.
            name: The unique name of this manager instance within its source.
                Used for identification, logging, and debugging.
        """
        self._system_type = system_type
        self._source = source
        self._name = name
        self._item_cache: T | None = None
        self._lock = asyncio.Lock()

    @property
    def system_type(self) -> SystemType:
        """The type of Deephaven system this manager connects to.
        
        Returns:
            SystemType: Either COMMUNITY or ENTERPRISE, indicating the backend type.
        """
        return self._system_type

    @property
    def source(self) -> str:
        """The configuration source identifier for this manager.
        
        This property provides the source identifier used to group related managers
        or trace back to their configuration origin.
        
        Returns:
            str: The source identifier (e.g., file path, URL, or config key).
        """
        return self._source

    @property
    def name(self) -> str:
        """The unique name of this manager instance.
        
        This name is used for identification within the source context, logging,
        debugging, and creating fully qualified identifiers.
        
        Returns:
            str: The manager's unique name within its source.
        """
        return self._name

    @property
    def full_name(self) -> str:
        """A fully qualified name combining system type, source, and name.
        
        This property creates a unique identifier that can be used for logging,
        debugging, and distinguishing between different manager instances across
        the entire system.
        
        Returns:
            str: A colon-separated string in the format "system_type:source:name".
        """
        return f"{self.system_type.value}:{self.source}:{self.name}"

    @abstractmethod
    async def _create_item(self) -> T:
        """Create and return a new instance of the managed item.
        
        This abstract method must be implemented by subclasses to define how
        the specific type of managed item is created. It is called only when
        a new item needs to be created (lazy initialization).
        
        Returns:
            T: A newly created instance of the managed item type.
            
        Raises:
            Exception: May raise various exceptions depending on the specific
                implementation and the type of resource being created.
        """
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    async def _check_liveness(self, item: T) -> bool:
        """Check if the managed item is still alive and functional.
        
        This abstract method must be implemented by subclasses to define how
        to determine if the managed item is still healthy and usable. It is
        called by the get() method to validate cached items.
        
        Args:
            item: The managed item instance to check for liveness.
            
        Returns:
            bool: True if the item is alive and functional, False otherwise.
            
        Raises:
            Exception: May raise exceptions during the liveness check, which
                should be handled appropriately by the caller.
        """
        raise NotImplementedError  # pragma: no cover

    async def get(self) -> T:
        """Get the managed item, creating it lazily if it doesn't exist.
        
        This method implements the lazy initialization pattern with thread safety.
        On the first call, it creates the item using _create_item(). Subsequent
        calls return the cached item without recreating it.
        
        Returns:
            T: The managed item instance, either from cache or newly created.
            
        Raises:
            Exception: May raise any exception from the _create_item() implementation,
                such as connection errors, authentication failures, or configuration issues.
        
        Thread Safety:
            This method is coroutine-safe and can be called concurrently from multiple
            tasks without race conditions.
        """
        async with self._lock:
            # Double-checked locking pattern
            if self._item_cache:
                return self._item_cache

            _LOGGER.info(
                "[%s] Creating new item for '%s'...",
                self.__class__.__name__,
                self._name,
            )
            self._item_cache = await self._create_item()
            return self._item_cache

    async def is_alive(self) -> bool:
        """Check if the cached item exists and is still alive.
        
        This method first checks if an item is cached, then delegates to the
        subclass-specific _check_liveness() method to determine if the item
        is still functional. If no item is cached, returns False immediately.
        
        Returns:
            bool: True if an item is cached and passes the liveness check,
                False if no item is cached or the liveness check fails.
                
        Thread Safety:
            This method is coroutine-safe and handles exceptions from the
            liveness check gracefully by logging warnings and returning False.
        
        Note:
            This method never raises exceptions - liveness check failures are
            logged as warnings and the method returns False.
        """
        async with self._lock:
            if not self._item_cache:
                return False
            try:
                return await self._check_liveness(self._item_cache)
            except Exception as e:
                _LOGGER.warning(
                    "[%s] Liveness check failed for '%s': %s",
                    self.__class__.__name__,
                    self._name,
                    e,
                )
                return False

    async def close(self) -> None:
        """Close the underlying managed item and clear the cache.

        This method performs an orderly async shutdown of the managed resource (such as a session or connection),
        if one exists and is alive. It acquires the internal lock to ensure thread/coroutine safety, checks liveness,
        and then calls the async close() method on the item. After closing, the item is removed from the cache.

        This method is safe to call multiple times and is idempotent: if no item is cached or the item is already dead,
        it does nothing. Use this to proactively release resources before shutdown or reconfiguration.
        """
        async with self._lock:
            if not self._item_cache:
                return

            is_alive = await self._check_liveness(self._item_cache)

            if not is_alive:
                self._item_cache = None
                return

            if asyncio.iscoroutinefunction(self._item_cache.close):
                await self._item_cache.close()
            else:
                raise InternalError(
                    f"Item '{self._name}' of type {type(self._item_cache).__name__} has a 'close' attribute that is not a coroutine function."
                )

            self._item_cache = None


class CommunitySessionManager(BaseItemManager[CoreSession]):
    """
    An async, coroutine-safe manager for a single Deephaven Community session.

    This specialized manager handles the complete lifecycle of a `CoreSession` for
    Deephaven Community deployments. It provides lazy initialization, caching,
    liveness monitoring, and proper cleanup for community sessions.

    Key Features:
        - Configuration-based session creation from a dictionary
        - Automatic connection management to Community servers
        - Session health monitoring via the session's is_alive() method
        - Thread-safe operations for concurrent access
        - Proper async cleanup and resource disposal

    The manager creates `CoreSession` instances on-demand using the provided
    configuration dictionary, which typically contains server connection details,
    authentication parameters, and other session-specific settings.

    Architecture:
        This manager is designed to be used within a `CommunitySessionRegistry`
        that manages collections of sessions for different workers or use cases.
        However, it can also be used standalone for single-session scenarios.

    Session Lifecycle:
        1. Initialize manager with name and configuration
        2. First get() call creates and caches the CoreSession
        3. Subsequent get() calls return the cached session
        4. Liveness checks ensure session health
        5. close() properly disposes of the session when done

    See Also:
        - `BaseItemManager`: The generic base class providing core lifecycle and concurrency logic.
        - `CoreSession`: The async session wrapper for Deephaven Community sessions.
        - `CommunitySessionRegistry`: The registry that manages multiple manager instances.
    """

    def __init__(self, name: str, config: dict[str, Any]):
        """Initialize the manager with a name and session configuration.

        Sets up the manager for a specific Community session with the provided
        configuration. The configuration will be used later during lazy session
        creation when get() is first called.

        Args:
            name: The unique name of this manager instance, used for identification,
                logging, and debugging purposes.
            config: The configuration dictionary containing all parameters needed
                to create a CoreSession, such as server URL, authentication details,
                and connection settings.
        """
        super().__init__(
            system_type=SystemType.COMMUNITY,
            source="community",
            name=name,
        )
        self._config = config

    @override
    async def _create_item(self) -> CoreSession:
        """Create a new CoreSession from the stored configuration.
        
        This method is called during lazy initialization to create a new
        CoreSession instance using the configuration provided during construction.
        It handles the async session creation process and wraps any exceptions
        in a SessionCreationError for consistent error handling.
        
        Returns:
            CoreSession: A newly created and connected CoreSession instance.
            
        Raises:
            SessionCreationError: If session creation fails due to connection
                issues, authentication problems, or invalid configuration.
        """
        try:
            return await CoreSession.from_config(self._config)
        except Exception as e:
            # TODO: what exception strategy?
            raise SessionCreationError(
                f"Failed to create session for community worker {self._name}: {e}"
            ) from e

    @override
    async def _check_liveness(self, item: CoreSession) -> bool:
        """Check if the CoreSession is still alive and responsive.
        
        This method delegates to the session's own is_alive() method to
        determine if the session is still connected and functional.
        
        Args:
            item: The CoreSession instance to check for liveness.
            
        Returns:
            bool: True if the session is alive and responsive, False otherwise.
        """
        return await item.is_alive()


class EnterpriseSessionManager(BaseItemManager[CorePlusSession]):
    """
    An async, coroutine-safe manager for a single Deephaven Enterprise session.

    This specialized manager handles the complete lifecycle of a `CorePlusSession` for
    Deephaven Enterprise deployments. Unlike the community version, it uses a flexible
    creation function approach that allows customizable session creation logic.

    Key Features:
        - Function-based session creation for maximum flexibility
        - Support for complex Enterprise authentication flows
        - Session health monitoring via the session's is_alive() method
        - Thread-safe operations for concurrent access
        - Proper async cleanup and resource disposal

    Creation Function Pattern:
        The manager accepts a creation function during initialization that takes
        `source` and `name` parameters and returns an awaitable `CorePlusSession`.
        This pattern allows callers to customize the session creation process
        while maintaining the manager's lifecycle management benefits.

    Architecture:
        This design decouples the session creation logic from the manager,
        allowing different creation strategies (e.g., different authentication
        methods, connection pooling, or factory patterns) to be used with the
        same management infrastructure.

    Session Lifecycle:
        1. Initialize manager with source, name, and creation function
        2. First get() call invokes the creation function to create and cache the session
        3. Subsequent get() calls return the cached session
        4. Liveness checks ensure session health
        5. close() properly disposes of the session when done

    Error Handling:
        Exceptions from the creation function are wrapped in `SessionCreationError`
        for consistent error handling across the application.

    See Also:
        - `BaseItemManager`: The generic base class providing core lifecycle and concurrency logic.
        - `CorePlusSession`: The async session wrapper for Deephaven Enterprise sessions.
        - `CorePlusSessionFactory`: A common implementation of session creation logic.
    """

    def __init__(
        self,
        source: str,
        name: str,
        creation_function: Callable[[str, str], Awaitable["CorePlusSession"]],
    ):
        """Initialize the manager with source, name, and session creation function.

        Sets up the manager to use the provided creation function for lazy session
        initialization. The creation function will be called with source and name
        parameters when a session needs to be created.

        Args:
            source: The configuration source identifier (e.g., file path, URL, or
                configuration key). This will be passed to the creation function.
            name: The unique name of this manager instance, used for identification
                and passed to the creation function.
            creation_function: A callable that takes (source: str, name: str) as
                parameters and returns an awaitable CorePlusSession. This function
                should handle all session creation logic including authentication,
                connection establishment, and any required configuration.

        Note:
            The creation function is expected to be properly configured and
            ready to create sessions. The manager does not validate the creation
            function during initialization - validation occurs during first use.
        """
        super().__init__(system_type=SystemType.ENTERPRISE, source=source, name=name)
        self._creation_function = creation_function

    @override
    async def _create_item(self) -> CorePlusSession:
        """Create a new CorePlusSession using the provided creation function.
        
        This method invokes the creation function provided during initialization,
        passing the source and name parameters. It handles the async session creation
        process and wraps any exceptions in a SessionCreationError for consistent
        error handling.
        
        Returns:
            CorePlusSession: A newly created and connected CorePlusSession instance.
            
        Raises:
            SessionCreationError: If session creation fails due to any reason,
                including connection issues, authentication problems, or creation
                function failures. The original exception is preserved as the cause.
        """
        try:
            return await self._creation_function(self._source, self._name)
        except Exception as e:
            raise SessionCreationError(
                f"Failed to create enterprise session for {self._name}: {e}"
            ) from e

    @override
    async def _check_liveness(self, item: CorePlusSession) -> bool:
        """Check if the CorePlusSession is still alive and responsive.
        
        This method delegates to the session's own is_alive() method to
        determine if the session is still connected and functional.
        
        Args:
            item: The CorePlusSession instance to check for liveness.
            
        Returns:
            bool: True if the session is alive and responsive, False otherwise.
        """
        return await item.is_alive()


class CorePlusSessionFactoryManager(BaseItemManager[CorePlusSessionFactory]):
    """
    An async, coroutine-safe manager for a single `CorePlusSessionFactory`.

    This manager is a critical component of the Deephaven Enterprise session architecture.
    Instead of managing sessions directly, it manages the lifecycle of a
    `CorePlusSessionFactory`, which serves as a factory for creating `CorePlusSession`
    instances with consistent configuration and authentication.

    Key Features:
        - Configuration-based factory creation from a dictionary
        - Lazy initialization with thread-safe caching
        - Health monitoring via the factory's ping() method
        - Proper async cleanup and resource disposal
        - Integration with registry management patterns

    Architecture:
        This manager sits between raw configuration and session creation, providing
        a managed factory that can be shared across multiple session creation requests.
        The factory handles authentication, connection pooling, and other Enterprise-specific
        features that need to be shared across sessions.

    Factory Lifecycle:
        1. Initialize manager with name and configuration
        2. First get() call creates and caches the CorePlusSessionFactory
        3. Subsequent get() calls return the cached factory
        4. Liveness checks use ping() to verify factory connectivity
        5. close() properly disposes of the factory when done

    Liveness Monitoring:
        Unlike sessions which use is_alive(), factories use a ping() method to
        confirm connectivity and readiness. This allows verification of the
        underlying connection without creating a full session.

    Registry Integration:
        This manager is typically managed by a `CorePlusSessionFactoryRegistry`
        that handles collections of factory managers for different configurations
        or deployment targets.

    See Also:
        - `BaseItemManager`: The generic base class providing core lifecycle and concurrency logic.
        - `CorePlusSessionFactory`: The factory this manager creates and manages.
        - `CorePlusSessionFactoryRegistry`: The registry that manages multiple factory managers.
    """

    def __init__(self, name: str, config: dict[str, Any]):
        """Initialize the manager with a name and factory configuration.

        Sets up the manager for a specific Enterprise factory with the provided
        configuration. The configuration will be used later during lazy factory
        creation when get() is first called.

        Args:
            name: The unique name of this manager instance, used for identification,
                logging, and debugging purposes.
            config: The configuration dictionary containing all parameters needed
                to create a CorePlusSessionFactory, such as server URLs, authentication
                credentials, connection settings, and factory-specific options.
        """
        super().__init__(
            system_type=SystemType.ENTERPRISE,
            source="factory",
            name=name,
        )
        self._config = config

    @override
    async def _create_item(self) -> CorePlusSessionFactory:
        """Create a new CorePlusSessionFactory from the stored configuration.
        
        This method is called during lazy initialization to create a new
        CorePlusSessionFactory instance using the configuration provided during
        construction. It handles the async factory creation process.
        
        Returns:
            CorePlusSessionFactory: A newly created and configured factory instance
                ready to create CorePlusSession instances.
                
        Raises:
            Exception: May raise various exceptions during factory creation,
                such as connection errors, authentication failures, or
                invalid configuration parameters.
        """
        return await CorePlusSessionFactory.from_config(self._config)

    @override
    async def _check_liveness(self, item: CorePlusSessionFactory) -> bool:
        """Check if the CorePlusSessionFactory is still alive and responsive.
        
        This method uses the factory's ping() method to verify connectivity
        and readiness without creating a full session. This is more lightweight
        than session-based liveness checks.
        
        Args:
            item: The CorePlusSessionFactory instance to check for liveness.
            
        Returns:
            bool: True if the factory responds to ping and is ready to create
                sessions, False otherwise.
        """
        return await item.ping()
