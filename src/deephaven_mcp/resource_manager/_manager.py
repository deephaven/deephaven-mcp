"""
Async resource managers for Deephaven MCP session and factory lifecycle management.

This module provides thread-safe, async resource managers that handle the complete lifecycle
of Deephaven sessions and factories across both Community and Enterprise deployments. The managers
implement lazy initialization, caching, health monitoring, and proper cleanup patterns for
long-lived backend resources.

Core Architecture:
    The module is built around a generic BaseItemManager that provides common lifecycle
    management patterns, with specialized concrete implementations for different resource types.
    All managers use asyncio.Lock for thread safety and implement consistent error handling
    and logging patterns.

Manager Types:
    CommunitySessionManager: Manages CoreSession instances for Community deployments
        using configuration-based initialization.
    EnterpriseSessionManager: Manages CorePlusSession instances for Enterprise deployments
        using flexible creation functions.
    CorePlusSessionFactoryManager: Manages CorePlusSessionFactory instances that serve
        as factories for creating Enterprise sessions.

Key Features:
    - Lazy Initialization: Resources created only when first accessed, reducing overhead
    - Thread Safety: All operations protected by asyncio.Lock for concurrent access
    - Dual Liveness Checking: Support for both cached item checks and provisioning checks
    - Comprehensive Logging: Detailed operational logging for debugging and monitoring
    - Exception Safety: Consistent error handling with proper exception wrapping
    - Resource Cleanup: Automatic disposal of resources with proper async cleanup

Resource Lifecycle:
    1. Manager initialization with configuration or creation functions
    2. Lazy resource creation on first access via get() method
    3. Cached resource reuse for subsequent accesses
    4. Health monitoring via liveness_status() with dual modes
    5. Proper cleanup and disposal via close() method

Liveness Monitoring:
    All managers support dual-mode liveness checking:
    - Cached Mode (default): Check if cached resource is alive
    - Provisioning Mode: Ensure resource exists (create if needed) and check liveness

Usage Pattern:
    ```python
    # Create manager
    manager = CommunitySessionManager("worker1", config)

    # Get resource (lazy initialization)
    session = await manager.get()

    # Check health (cached mode)
    status, detail = await manager.liveness_status()

    # Check provisioning capability
    status, detail = await manager.liveness_status(ensure_item=True)

    # Clean up
    await manager.close()
    ```

Key Classes:
    AsyncClosable: Protocol defining async close() interface for managed resources
    ResourceLivenessStatus: Enum representing resource health states
    SystemType: Enum for Deephaven deployment types (COMMUNITY, ENTERPRISE)
    BaseItemManager: Generic base class providing core lifecycle management
    CommunitySessionManager: Concrete manager for Community sessions
    EnterpriseSessionManager: Concrete manager for Enterprise sessions
    CorePlusSessionFactoryManager: Concrete manager for Enterprise session factories

Thread Safety:
    All managers are fully coroutine-safe and designed for concurrent access in
    async applications. Internal locking ensures race-condition-free operations.
"""

import asyncio
import enum
import logging
import sys
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

if TYPE_CHECKING:
    from typing_extensions import override  # pragma: no cover
elif sys.version_info >= (3, 12):
    from typing import override  # pragma: no cover
else:
    from typing_extensions import override  # pragma: no cover

from deephaven_mcp._exceptions import (
    AuthenticationError,
    ConfigurationError,
    SessionCreationError,
)
from deephaven_mcp.client import (
    CorePlusSession,
    CorePlusSessionFactory,
    CoreSession,
)

_LOGGER = logging.getLogger(__name__)


class AsyncClosable(Protocol):
    """Protocol defining the async close() interface for managed resources.

    This protocol establishes the contract that all resources managed by BaseItemManager
    must support asynchronous cleanup operations. It serves as a type constraint ensuring
    that managed resources can be properly disposed of when no longer needed.

    The protocol is used as a type bound for the generic TypeVar T in BaseItemManager,
    providing compile-time verification that managed items support the required cleanup
    interface. This enables safe resource management patterns in async contexts.

    Implementation Requirements:
        Classes implementing this protocol must provide an async close() method that:
        - Performs complete resource cleanup (connections, files, etc.)
        - Can be safely called multiple times (idempotent)
        - Handles cleanup failures gracefully
        - Releases all held resources

    Compatible Types:
        The following Deephaven client types implement this protocol:
        - CoreSession: Community session cleanup
        - CorePlusSession: Enterprise session cleanup
        - CorePlusSessionFactory: Factory resource cleanup

    Usage in Type Hints:
        ```python
        T = TypeVar("T", bound=AsyncClosable)

        class Manager(Generic[T]):
            async def cleanup(self, item: T) -> None:
                await item.close()  # Type checker validates this is available
        ```

    See Also:
        BaseItemManager: The generic manager that uses this protocol constraint
    """

    async def close(self) -> None:
        """Close the underlying resource and perform cleanup operations.

        This method should perform complete resource cleanup including closing
        network connections, releasing file handles, freeing memory, and notifying
        dependent systems of the shutdown. Implementations should be idempotent,
        meaning multiple calls should be safe and not cause errors.

        Best Practices:
            - Make the method idempotent (safe to call multiple times)
            - Handle partial cleanup failures gracefully
            - Release all held resources (connections, files, memory)
            - Avoid blocking operations in the cleanup path
            - Log cleanup failures but don't raise unless critical

        Raises:
            Exception: May raise exceptions during cleanup operations. Callers
                should handle these exceptions appropriately, typically by logging
                the error and continuing with other cleanup operations.

        Example:
            ```python
            async def close(self) -> None:
                try:
                    if self._connection:
                        await self._connection.close()
                        self._connection = None
                except Exception as e:
                    logger.warning(f"Failed to close connection: {e}")
                    # Continue with other cleanup...
            ```
        """
        raise NotImplementedError  # pragma: no cover


T = TypeVar("T", bound=AsyncClosable)


class ResourceLivenessStatus(enum.Enum):
    """Enum representing the health and availability status of managed resources.

    This enum provides a standardized way to categorize the operational status of
    Deephaven sessions, factories, and other managed resources. It enables consistent
    status reporting across different resource types and helps with automated
    decision-making in resource management workflows.

    Status Categories:
        The enum covers the full spectrum of resource states from healthy operation
        to various failure modes, allowing precise classification of issues for
        debugging and monitoring purposes.

    Usage Context:
        This enum is returned by liveness_status() methods across all resource managers
        and is used by registries to make decisions about resource cleanup, replacement,
        or continued use.

    Values:
        ONLINE: Resource is healthy, responsive, and ready for operational use.
            Indicates successful connectivity and passing health checks.

        OFFLINE: Resource is unavailable, unresponsive, or has failed health checks.
            May indicate network issues, service downtime, or resource termination.

        UNAUTHORIZED: Resource access failed due to authentication or authorization issues.
            Indicates invalid credentials, expired tokens, or insufficient permissions.

        MISCONFIGURED: Resource cannot be used due to invalid or incomplete configuration.
            Indicates configuration errors, missing parameters, or incompatible settings.

        UNKNOWN: Resource status could not be determined due to unexpected errors.
            Indicates exceptions during status checking that prevent classification.

    String Representation:
        The enum provides lowercase string representations via __str__() for logging
        and display purposes (e.g., "online", "offline", "unauthorized").

    Example:
        ```python
        status, detail = await manager.liveness_status()
        if status == ResourceLivenessStatus.ONLINE:
            # Resource is ready for use
            resource = await manager.get()
        elif status == ResourceLivenessStatus.UNAUTHORIZED:
            # Handle authentication issues
            logger.warning(f"Auth failed: {detail}")
        ```
    """

    ONLINE = 1
    OFFLINE = 2
    UNAUTHORIZED = 3
    MISCONFIGURED = 4
    UNKNOWN = 5

    def __str__(self) -> str:
        """Return the uppercase name of the resource liveness status."""
        return self.name


class SystemType(str, enum.Enum):
    """Enum representing different types of Deephaven backend deployment architectures.

    This enum categorizes the distinct Deephaven deployment models that require
    different management approaches, authentication mechanisms, and client libraries.
    It enables resource managers to adapt their behavior based on the target
    deployment type.

    Deployment Characteristics:
        Each system type has unique operational characteristics that affect how
        sessions are created, authenticated, and managed. The enum enables
        polymorphic behavior across different deployment architectures.

    String Inheritance:
        This enum inherits from str, making instances directly usable as string
        values in configuration, logging, and serialization contexts without
        explicit conversion.

    Values:
        COMMUNITY: Open-source Deephaven Community Edition deployments.
            - Simplified authentication (typically no auth or basic auth)
            - Uses CoreSession and related community client libraries
            - Suitable for development, testing, and simple production use
            - Typically deployed locally, in containers, or simple cloud setups
            - Configuration-based session creation

        ENTERPRISE: Commercial Deephaven Enterprise Edition deployments.
            - Advanced authentication (SSO, LDAP, OAuth, etc.)
            - Uses CorePlusSession and Enterprise client libraries
            - Enhanced security, scalability, and enterprise integrations
            - Multi-tenant capabilities and advanced resource management
            - Factory-based session creation with sophisticated provisioning

    Usage in Resource Managers:
        The system type determines which client libraries and authentication
        mechanisms are used during resource creation and management.

    Example:
        ```python
        if manager.system_type == SystemType.COMMUNITY:
            # Use community-specific configuration and libraries
            session = await CoreSession.from_config(config)
        elif manager.system_type == SystemType.ENTERPRISE:
            # Use enterprise-specific factories and authentication
            session = await factory.create_session(source, name)
        ```
    """

    COMMUNITY = "community"
    ENTERPRISE = "enterprise"

    def __str__(self) -> str:
        """Return the uppercase name of the system type."""
        return self.name


class BaseItemManager(Generic[T], ABC):
    """Generic async resource manager providing lazy initialization and lifecycle management.

    This abstract base class establishes a comprehensive framework for managing single
    Deephaven resources (sessions, factories, etc.) with thread-safe operations, lazy
    initialization, health monitoring, and proper cleanup patterns. It serves as the
    foundation for all concrete resource managers in the system.

    Design Philosophy:
        The manager follows the "lazy initialization" pattern where expensive resources
        are created only when first accessed, then cached for reuse. This approach
        minimizes startup overhead and allows for efficient resource utilization.

    Core Capabilities:
        - **Lazy Loading**: Resources created on-demand during first access
        - **Thread Safety**: Full coroutine safety with asyncio.Lock protection
        - **Dual Liveness Modes**: Support for cached-only and provisioning health checks
        - **Exception Safety**: Comprehensive error handling with consistent logging patterns
        - **Resource Cleanup**: Automatic disposal with idempotent close operations
        - **Comprehensive Logging**: Detailed operational logging for debugging and monitoring

    Lifecycle Management:
        1. **Initialization**: Manager created with identification metadata
        2. **Lazy Creation**: Resource created on first get() call
        3. **Caching**: Subsequent get() calls return cached resource
        4. **Health Monitoring**: liveness_status() provides dual-mode health checking
        5. **Cleanup**: close() disposes of resource and resets state

    Liveness Checking Modes:
        - **Cached Mode** (default): Check health of existing cached resource
        - **Provisioning Mode**: Ensure resource exists (create if needed) and check health

    Thread Safety Guarantees:
        All public methods are fully coroutine-safe and can be called concurrently
        from multiple async tasks without race conditions. Internal operations use
        asyncio.Lock with careful lock ordering to prevent deadlocks.

    Type Parameters:
        T: The type of resource being managed. Must implement the AsyncClosable protocol
           to ensure proper cleanup capabilities.

    Abstract Methods:
        Concrete subclasses must implement:
        - _create_item(): Create and return a new resource instance
        - _check_liveness(item): Check health of a specific resource instance

    Error Handling:
        The manager provides consistent exception handling patterns:
        - Resource creation failures are wrapped with appropriate exception types
        - Liveness check failures are categorized using ResourceLivenessStatus enum
        - Cleanup failures are logged but don't prevent other operations

    Usage Pattern:
        ```python
        class MyResourceManager(BaseItemManager[MyResource]):
            async def _create_item(self) -> MyResource:
                return await MyResource.create(self._config)

            async def _check_liveness(self, item: MyResource) -> tuple[ResourceLivenessStatus, str | None]:
                if await item.is_alive():
                    return (ResourceLivenessStatus.ONLINE, None)
                return (ResourceLivenessStatus.OFFLINE, "Resource not responding")

        # Usage
        manager = MyResourceManager(SystemType.COMMUNITY, "config.yaml", "worker1")
        resource = await manager.get()  # Lazy creation
        status, detail = await manager.liveness_status()  # Health check
        await manager.close()  # Cleanup
        ```

    See Also:
        CommunitySessionManager: Concrete implementation for Community sessions
        EnterpriseSessionManager: Concrete implementation for Enterprise sessions
        CorePlusSessionFactoryManager: Concrete implementation for Enterprise factories
    """

    @staticmethod
    def make_full_name(system_type: "SystemType", source: str, name: str) -> str:
        """Construct the canonical full name identifier for managed resources.

        This utility method creates standardized, unique identifiers for managed resources
        by combining the system type, source, and name into a colon-separated string.
        These identifiers are used throughout the system for resource identification,
        registry keys, logging, and debugging.

        Identifier Format:
            The format follows the pattern: "system_type:source:name"
            - system_type: "community" or "enterprise"
            - source: Configuration source (file path, URL, config key, etc.)
            - name: Unique name within the source context

        Consistency:
            This method should be used for ALL resource identifier construction to
            ensure consistency across registries, logging, and other subsystems.
            Using this method prevents identifier format inconsistencies.

        Args:
            system_type: The Deephaven deployment type (COMMUNITY or ENTERPRISE).
                Determines which client libraries and authentication mechanisms are used.
            source: The configuration source identifier that groups related resources.
                Examples: "config.yaml", "https://api.example.com/config", "env-vars"
            name: The unique name of the specific resource within its source context.
                Must be unique within the same system_type and source combination.

        Returns:
            str: A colon-separated identifier string in the exact format
                "system_type:source:name". This string is safe for use as
                dictionary keys, file names, and logging contexts.

        Example:
            ```python
            # Create identifier for a community session
            full_name = BaseItemManager.make_full_name(
                SystemType.COMMUNITY, "local-config.yaml", "worker-1"
            )
            # Result: "community:local-config.yaml:worker-1"

            # Create identifier for an enterprise factory
            full_name = BaseItemManager.make_full_name(
                SystemType.ENTERPRISE, "prod-env", "factory-east-1"
            )
            # Result: "enterprise:prod-env:factory-east-1"
            ```
        """
        return f"{system_type.value}:{source}:{name}"

    @staticmethod
    def parse_full_name(full_name: str) -> tuple[str, str, str]:
        """Parse a full name identifier into its components.
        
        This method is the inverse of make_full_name() and parses identifiers
        created by that method back into their constituent parts.
        
        Args:
            full_name: Full name in format "system_type:source:name"
            
        Returns:
            tuple[str, str, str]: (system_type, source, name)
            
        Raises:
            ValueError: If full_name is not in the expected format
            
        Example:
            ```python
            system_type, source, name = BaseItemManager.parse_full_name(
                "enterprise:prod-env:session-1"
            )
            # Result: ("enterprise", "prod-env", "session-1")
            ```
        """
        parts = full_name.split(":", 2)
        if len(parts) != 3 or not all(part for part in parts):
            raise ValueError(
                f"Invalid full_name format: '{full_name}'. "
                f"Expected format: 'system_type:source:name'"
            )
        return parts[0], parts[1], parts[2]

    def __init__(self, system_type: SystemType, source: str, name: str):
        """Initialize the resource manager with identification metadata and internal state.

        Creates a new manager instance with the specified identification parameters and
        initializes all internal state required for lazy loading, thread safety, and
        resource management. The manager is ready for use immediately after construction,
        but the actual managed resource won't be created until first access.

        Initialization Process:
            1. Store identification metadata (system_type, source, name)
            2. Initialize empty resource cache (lazy loading)
            3. Create asyncio.Lock for thread safety
            4. Generate canonical full name identifier
            5. Log manager creation for debugging and monitoring

        Thread Safety:
            The constructor is thread-safe and the resulting manager instance is
            fully prepared for concurrent access from multiple async tasks.

        Args:
            system_type: The Deephaven deployment type (COMMUNITY or ENTERPRISE).
                This determines which client libraries, authentication mechanisms,
                and management approaches will be used by concrete implementations.
            source: The configuration source identifier used for grouping and organization.
                Examples: "config.yaml", "production.env", "https://config-api/v1"
                This helps organize related resources and provides context for debugging.
            name: The unique name of this specific manager within its source context.
                Must be unique within the same system_type and source combination.
                Used for identification, logging, and resource tracking.

        Post-Initialization State:
            After construction, the manager has:
            - Empty resource cache (_item_cache = None)
            - Initialized asyncio.Lock for thread safety
            - Logged creation message for operational visibility
            - Ready to handle get(), liveness_status(), and close() operations

        Example:
            ```python
            # Create a manager for a community session
            manager = CommunitySessionManager(
                SystemType.COMMUNITY,
                "local-config.yaml",
                "worker-1"
            )
            # Manager is ready, but resource not yet created

            # First access triggers lazy creation
            session = await manager.get()
            ```
        """
        self._system_type = system_type
        self._source = source
        self._name = name
        self._item_cache: T | None = None
        self._lock = asyncio.Lock()

        full_name = self.make_full_name(system_type, source, name)
        _LOGGER.info(
            "[%s] Initialized manager for '%s'", self.__class__.__name__, full_name
        )

    @abstractmethod
    async def _create_item(self) -> T:
        """Create and return a new instance of the managed resource.

        This abstract method defines the resource creation logic that concrete
        subclasses must implement. It is called during lazy initialization when
        a resource is first requested via get() or when liveness_status() is
        called with ensure_item=True and no cached resource exists.

        Implementation Requirements:
            Concrete implementations must:
            - Create a fully initialized and ready-to-use resource instance
            - Handle all necessary configuration, authentication, and setup
            - Return a resource that implements the AsyncClosable protocol
            - Perform any required connectivity or validation checks
            - Be idempotent and safe to call multiple times (though caching prevents this)

        Error Handling:
            Implementations should let exceptions bubble up to the caller, where
            they will be caught and wrapped with appropriate context by the
            calling liveness_status() method. Common exceptions include:
            - ConfigurationError: Invalid or missing configuration
            - AuthenticationError: Failed authentication or authorization
            - SessionCreationError: Resource creation failures
            - NetworkError: Connection or communication failures

        Thread Safety:
            This method is always called within the manager's asyncio.Lock context,
            so implementations don't need to provide their own synchronization.
            However, they should avoid blocking operations that could cause deadlocks.

        Performance Considerations:
            This method may be called infrequently (only during lazy initialization),
            so implementations can prioritize correctness and reliability over
            performance. However, excessively slow creation can impact user experience.

        Returns:
            T: A newly created, fully initialized resource instance ready for use.
                The resource must implement AsyncClosable and be in a healthy,
                operational state.

        Raises:
            ConfigurationError: Invalid, missing, or incompatible configuration parameters
            AuthenticationError: Failed authentication or insufficient permissions
            SessionCreationError: Resource creation failed due to system issues
            Exception: Other implementation-specific errors during resource creation

        Example Implementation:
            ```python
            async def _create_item(self) -> CoreSession:
                try:
                    session = await CoreSession.from_config(self._config)
                    # Validate the session is working
                    await session.is_alive()
                    return session
                except Exception as e:
                    # Let exceptions bubble up for proper handling
                    raise
            ```

        See Also:
            get(): The public method that triggers lazy creation
            liveness_status(): Method that may trigger creation with ensure_item=True
        """
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    async def _check_liveness(
        self, item: T
    ) -> tuple[ResourceLivenessStatus, str | None]:
        """Check the health and operational status of a managed resource.

        This abstract method defines the liveness checking logic that concrete
        subclasses must implement. It determines whether a specific resource
        instance is still healthy, responsive, and ready for operational use.
        The method is called by liveness_status() to validate cached resources.

        Implementation Requirements:
            Concrete implementations must:
            - Perform appropriate health checks for the specific resource type
            - Return accurate status classifications using ResourceLivenessStatus
            - Provide meaningful detail messages for non-ONLINE statuses
            - Complete checks efficiently to avoid blocking operations
            - Handle edge cases gracefully (disconnections, timeouts, etc.)

        Status Classification Guidelines:
            - ONLINE: Resource is healthy and ready for use
            - OFFLINE: Resource is unresponsive or has failed health checks
            - UNAUTHORIZED: Authentication or authorization failures
            - MISCONFIGURED: Configuration errors preventing operation
            - UNKNOWN: Unable to determine status due to unexpected errors

        Exception Handling:
            This method should NOT handle exceptions internally. Exceptions should
            bubble up to the calling liveness_status() method, which provides
            centralized exception handling and logging. The caller will catch
            exceptions and convert them to appropriate ResourceLivenessStatus values.

        Performance Considerations:
            - Keep health checks lightweight and fast when possible
            - Avoid long-running operations that could block other operations
            - Consider implementing timeouts for network-based checks
            - Balance thoroughness with performance for frequently called checks

        Thread Safety:
            This method is always called within the manager's asyncio.Lock context,
            ensuring thread-safe access to the resource instance. Implementations
            don't need additional synchronization.

        Args:
            item: The managed resource instance to check for liveness.
                This is guaranteed to be non-None and of the correct type T.

        Returns:
            tuple[ResourceLivenessStatus, str | None]: A tuple containing:
                - ResourceLivenessStatus: The health status classification
                - str | None: Optional detail message explaining the status,
                  particularly useful for non-ONLINE statuses to aid debugging

        Raises:
            Exception: May raise various exceptions during health checking.
                Common exceptions include network errors, authentication failures,
                or resource-specific errors. These will be caught and handled
                by the calling liveness_status() method.

        Example Implementation:
            ```python
            async def _check_liveness(self, item: CoreSession) -> tuple[ResourceLivenessStatus, str | None]:
                # Let exceptions bubble up to caller
                is_alive = await item.is_alive()

                if is_alive:
                    return (ResourceLivenessStatus.ONLINE, None)
                else:
                    return (ResourceLivenessStatus.OFFLINE, "Session is_alive() returned False")
            ```

        See Also:
            liveness_status(): The public method that calls this implementation
            ResourceLivenessStatus: Enum defining possible status values
        """
        raise NotImplementedError  # pragma: no cover

    @property
    def system_type(self) -> SystemType:
        """The Deephaven deployment type that this manager targets.

        This property indicates which type of Deephaven backend system the manager
        is configured to work with, determining the client libraries, authentication
        mechanisms, and management approaches used by the concrete implementation.

        Usage:
            The system type is used by registries and other components to group
            managers by deployment type and make decisions about resource allocation
            and management strategies.

        Returns:
            SystemType: The deployment type, either:
                - SystemType.COMMUNITY: For open-source Community deployments
                - SystemType.ENTERPRISE: For commercial Enterprise deployments
        """
        return self._system_type

    @property
    def source(self) -> str:
        """The configuration source identifier for this manager.

        This property provides the source identifier that groups related managers
        and traces back to their configuration origin. It's used for organization,
        debugging, and identifying which configuration provided the manager's settings.

        Common Source Types:
            - File paths: "config.yaml", "/etc/deephaven/production.conf"
            - Environment names: "production", "staging", "development"
            - URLs: "https://config-api.example.com/v1/deephaven"
            - Configuration keys: "kubernetes-configmap", "vault-secrets"

        Usage:
            Sources are used to:
            - Group related managers in registries
            - Provide context in logging and debugging
            - Support configuration reloading and updates
            - Enable hierarchical configuration management

        Returns:
            str: The source identifier string as provided during manager creation.
        """
        return self._source

    @property
    def name(self) -> str:
        """The unique name of this manager instance within its source context.

        This property provides the specific name that uniquely identifies this
        manager among other managers from the same source. It's used for
        identification, logging, debugging, and creating fully qualified identifiers.

        Uniqueness Scope:
            The name must be unique within the combination of system_type and source,
            but can be reused across different sources or system types.

        Common Naming Patterns:
            - Service names: "worker-1", "api-server", "data-processor"
            - Functional names: "primary", "backup", "analytics"
            - Environment-specific: "prod-east", "staging-west", "dev-local"

        Usage:
            Names are used to:
            - Create unique full identifiers via make_full_name()
            - Provide specific context in logging messages
            - Enable targeted resource management operations
            - Support debugging and monitoring of specific instances

        Returns:
            str: The unique name string as provided during manager creation.
        """
        return self._name

    @property
    def full_name(self) -> str:
        """The fully qualified, globally unique identifier for this manager.

        This property provides a canonical identifier that uniquely identifies this
        manager instance across the entire system by combining the system type,
        source, and name into a colon-separated string. These identifiers are used
        extensively for logging, debugging, registry keys, and system-wide identification.

        Identifier Format:
            The format follows the standard pattern: "system_type:source:name"
            Examples:
            - "community:config.yaml:worker-1"
            - "enterprise:production-env:api-server"
            - "community:local-dev:analytics-session"

        Global Uniqueness:
            The full name is guaranteed to be unique across all managers in the system,
            as long as the combination of (system_type, source, name) is unique.
            This enables safe use as dictionary keys, file names, and identifiers.

        Usage Contexts:
            - **Logging**: Provides clear context in log messages
            - **Registry Keys**: Used as unique keys in manager registries
            - **Debugging**: Helps identify specific manager instances
            - **Monitoring**: Enables tracking of individual manager metrics
            - **Configuration**: Maps to specific configuration sections

        Implementation:
            This property delegates to the static make_full_name() method to ensure
            consistency across all identifier creation in the system.

        Returns:
            str: A colon-separated identifier string in the exact format
                "system_type:source:name" that uniquely identifies this manager
                across the entire system.

        Example:
            ```python
            manager = CommunitySessionManager(
                SystemType.COMMUNITY, "config.yaml", "worker-1"
            )
            print(manager.full_name)  # "community:config.yaml:worker-1"
            ```

        See Also:
            make_full_name(): The static method that implements the identifier format
        """
        return self.make_full_name(self.system_type, self.source, self.name)

    @property
    def split_name(self) -> tuple[str, str, str]:
        """Split this manager's full name into its constituent components.
        
        This property provides convenient access to the individual components
        that make up this manager's full_name identifier.
        
        Returns:
            tuple[str, str, str]: (system_type, source, name) components
            
        Example:
            ```python
            def creation_func(source: str, name: str):
                # Session creation logic
                pass
                
            manager = EnterpriseSessionManager(
                source="prod-env", 
                name="session-1", 
                creation_function=creation_func
            )
            system_type, source, name = manager.split_name
            # Result: ("enterprise", "prod-env", "session-1")
            ```
            
        See Also:
            full_name: The combined identifier string
            parse_full_name(): Static method for parsing arbitrary full names
        """
        return self.system_type.value, self.source, self.name

    async def _get_unlocked(self) -> T:
        """Get the managed resource without acquiring the synchronization lock.

        This private method provides non-locking access to the managed resource,
        implementing lazy initialization when no cached resource exists. It assumes
        the caller has already acquired self._lock to ensure thread-safe operation.

        Lazy Initialization Pattern:
            - If a resource is cached, returns it immediately (cache hit)
            - If no resource is cached, creates a new one via _create_item() (cache miss)
            - Caches the newly created resource for future requests
            - Provides comprehensive logging for debugging and monitoring

        Lock Safety:
            This method MUST be called while holding self._lock. It is designed to be
            used by other methods that need resource access within their critical sections,
            avoiding the overhead and potential deadlock issues of nested lock acquisition.

        Usage Context:
            Called by:
            - liveness_status() when ensure_item=True and no cached resource exists
            - Other internal methods that need lock-free resource access
            - Should NOT be called directly by external code

        Performance Characteristics:
            - Cache hits are very fast (simple attribute access)
            - Cache misses involve resource creation overhead
            - Comprehensive logging helps with performance monitoring

        Error Propagation:
            This method does not handle exceptions from resource creation. All exceptions
            from _create_item() bubble up to the caller, where they can be handled
            appropriately based on the calling context.

        Returns:
            T: The managed resource instance, either:
                - An existing cached resource (immediate return)
                - A newly created and cached resource (after successful creation)

        Raises:
            Exception: Any exception raised by the _create_item() implementation,
                including but not limited to:
                - ConfigurationError: Invalid or missing configuration
                - AuthenticationError: Authentication or authorization failures
                - SessionCreationError: Resource creation failures
                - NetworkError: Connectivity or communication issues

        Thread Safety:
            This method is NOT thread-safe by itself. The caller MUST hold self._lock
            before calling this method to ensure proper synchronization.

        See Also:
            get(): The public, thread-safe method that acquires the lock and calls this
            liveness_status(): Another caller that uses this for resource access
        """
        if self._item_cache:
            _LOGGER.debug(
                "[%s] Cache hit for '%s'", self.__class__.__name__, self.full_name
            )
            return self._item_cache

        _LOGGER.info(
            "[%s] Cache miss - creating new item for '%s'...",
            self.__class__.__name__,
            self.full_name,
        )
        self._item_cache = await self._create_item()
        _LOGGER.info(
            "[%s] Successfully created and cached new item for '%s'",
            self.__class__.__name__,
            self.full_name,
        )
        return self._item_cache

    async def get(self) -> T:
        """Get the managed resource, using lazy initialization with full thread safety.

        This is the primary public method for accessing managed resources. It implements
        a lazy initialization pattern where resources are created only when first requested,
        then cached for subsequent accesses. The method provides full thread safety for
        concurrent access from multiple asyncio tasks.

        Lazy Initialization Behavior:
            - **First Call**: Creates a new resource via _create_item() and caches it
            - **Subsequent Calls**: Returns the cached resource immediately
            - **Thread Safety**: Uses asyncio.Lock to prevent race conditions
            - **Performance**: Cache hits are very fast, creation only happens once

        Resource Lifecycle:
            Once a resource is created and cached, it remains available until:
            - The manager is explicitly closed via close()
            - The application shuts down and resources are cleaned up
            - An error occurs that invalidates the cached resource

        Error Handling:
            Resource creation errors are propagated directly to the caller without
            modification. This allows application code to handle specific error types
            appropriately (e.g., retry logic, fallback strategies, user notification).

        Usage Patterns:
            ```python
            # Basic usage - get a resource
            resource = await manager.get()

            # Safe concurrent access
            async def worker(manager):
                resource = await manager.get()  # Thread-safe
                # Use resource...

            # Multiple concurrent workers
            await asyncio.gather(
                worker(manager),
                worker(manager),  # Same cached resource
                worker(manager)
            )
            ```

        Performance Considerations:
            - First call may be slow due to resource creation (network, auth, etc.)
            - Subsequent calls are very fast (cached access)
            - Lock contention is minimal for cache hits
            - Consider calling early in application startup for predictable performance

        Returns:
            T: The managed resource instance, guaranteed to be:
                - Fully initialized and ready for use
                - The same instance across all calls (cached)
                - Implementing the AsyncClosable protocol

        Raises:
            ConfigurationError: Invalid, missing, or incompatible configuration
            AuthenticationError: Authentication or authorization failures
            SessionCreationError: Resource creation failed due to system issues
            Exception: Other resource-specific creation errors from _create_item()

        Thread Safety:
            This method is fully thread-safe and coroutine-safe. Multiple concurrent
            calls will not create duplicate resources or cause race conditions.
            The first caller creates the resource while others wait.

        See Also:
            _create_item(): The abstract method that creates new resources
            liveness_status(): Check resource health without necessarily creating it
            close(): Clean up and invalidate the cached resource
        """
        _LOGGER.debug(
            "[%s] Getting managed item for '%s'",
            self.__class__.__name__,
            self.full_name,
        )
        async with self._lock:
            result = await self._get_unlocked()
            _LOGGER.debug(
                "[%s] Successfully retrieved managed item for '%s'",
                self.__class__.__name__,
                self.full_name,
            )
            return result

    async def _liveness_status_unlocked(
        self, ensure_item: bool = False
    ) -> tuple[ResourceLivenessStatus, str | None]:
        """Check resource liveness without acquiring the synchronization lock.

        This private method provides non-locking access to liveness checking functionality,
        implementing the same dual-mode liveness checking as the public liveness_status()
        method. It assumes the caller has already acquired self._lock for thread safety.

        Dual Liveness Check Modes:
            The method supports two distinct liveness checking scenarios:

            **Mode 1: Manager Capability Check (ensure_item=True)**
            - Question: "Can this manager provide a working resource?"
            - Behavior: Creates resource if none cached, then checks its health
            - Use Case: Pre-flight checks, resource provisioning validation

            **Mode 2: Cached Resource Check (ensure_item=False, default)**
            - Question: "Is the currently cached resource alive?"
            - Behavior: Only checks cached resource, returns OFFLINE if none exists
            - Use Case: Health monitoring, periodic status checks

        Exception Handling Strategy:
            This method implements centralized exception handling that converts various
            error types into appropriate ResourceLivenessStatus values:
            - AuthenticationError → UNAUTHORIZED
            - ConfigurationError → MISCONFIGURED
            - SessionCreationError → OFFLINE (if connection failure) or MISCONFIGURED (if config issue)
            - Other exceptions → UNKNOWN (with warning log)

        Lock Safety:
            This method MUST be called while holding self._lock. It delegates to other
            non-locking methods (_get_unlocked, _check_liveness) to avoid nested lock
            acquisition that could cause deadlocks.

        Usage Context:
            Called by:
            - liveness_status(): The public thread-safe wrapper method
            - Other internal methods needing lock-free liveness checking
            - Should NOT be called directly by external code

        Performance Characteristics:
            - Mode 1 (ensure_item=True): May be slow due to resource creation
            - Mode 2 (ensure_item=False): Fast for cached resources, immediate for none
            - Exception handling adds minimal overhead
            - Error logging provides debugging context

        Args:
            ensure_item: Controls the liveness checking mode:
                - False (default): Only check cached resource, return OFFLINE if none
                - True: Ensure resource exists (create if needed) before checking

        Returns:
            tuple[ResourceLivenessStatus, str | None]: A tuple containing:
                - ResourceLivenessStatus: The health status classification
                - str | None: Optional detail message explaining non-ONLINE statuses,
                  particularly useful for debugging and error reporting

        Thread Safety:
            This method is NOT thread-safe by itself. The caller MUST hold self._lock
            before calling this method to ensure proper synchronization.

        Logging:
            - Warning logs for unexpected exceptions with full context
            - No logging for successful operations (handled by calling methods)
            - Error details included in return value for caller processing

        See Also:
            liveness_status(): The public, thread-safe wrapper for this method
            _check_liveness(): The abstract method that performs actual health checks
            _get_unlocked(): Method used to get/create resources in ensure_item mode
        """
        try:
            if ensure_item:
                # Mode 1: "Can this manager provide a working item?"
                # Get or create the item, then check its liveness
                item = await self._get_unlocked()
                return await self._check_liveness(item)
            else:
                # Mode 2: "Is the cached item alive?"
                # Only check cached item, return OFFLINE if none cached
                if not self._item_cache:
                    return (ResourceLivenessStatus.OFFLINE, "No item cached")
                return await self._check_liveness(self._item_cache)
        except AuthenticationError as e:
            return (ResourceLivenessStatus.UNAUTHORIZED, str(e))
        except ConfigurationError as e:
            return (ResourceLivenessStatus.MISCONFIGURED, str(e))
        except SessionCreationError as e:
            # Distinguish between connection failures and actual configuration errors
            error_msg = str(e).lower()
            connection_failure_indicators = [
                "connection refused",
                "connection timed out",
                "connection failed",
                "failed to connect",
                "unable to connect",
                "network is unreachable",
                "host is unreachable",
                "no route to host",
                "connection reset",
                "connection aborted",
                "server not running",
                "service unavailable",
                "name or service not known",
                "nodename nor servname provided",
                "temporary failure in name resolution",
            ]

            # Check if this is a connection failure rather than a config issue
            if any(
                indicator in error_msg for indicator in connection_failure_indicators
            ):
                return (ResourceLivenessStatus.OFFLINE, str(e))
            else:
                return (ResourceLivenessStatus.MISCONFIGURED, str(e))
        except Exception as e:
            _LOGGER.warning(
                "[%s] Liveness check failed for %s: %s",
                self.__class__.__name__,
                self.full_name,
                e,
            )
            return (ResourceLivenessStatus.UNKNOWN, str(e))

    async def liveness_status(
        self, ensure_item: bool = False
    ) -> tuple[ResourceLivenessStatus, str | None]:
        """Check the health and operational status of the managed resource.

        This is the primary public method for checking resource liveness with full thread
        safety. It provides two distinct checking modes to address different operational
        needs, from lightweight monitoring to comprehensive capability validation.

        Dual Liveness Check Modes:
            This method supports two fundamentally different approaches to liveness checking:

            **Mode 1: Cached Resource Monitoring (ensure_item=False, default)**
            - Purpose: "Is my cached resource currently healthy?"
            - Behavior: Only checks existing cached resource, no resource creation
            - Performance: Very fast, minimal overhead
            - Returns: OFFLINE if no resource is cached
            - Use Cases:
              * Periodic health monitoring
              * Status dashboards and alerts
              * Quick health checks before using cached resources
              * Resource cleanup decisions

            **Mode 2: Manager Capability Validation (ensure_item=True)**
            - Purpose: "Can this manager provide a working resource right now?"
            - Behavior: Ensures resource exists (creates if needed), then checks health
            - Performance: May be slow due to resource creation overhead
            - Returns: Actual resource health after ensuring availability
            - Use Cases:
              * Pre-flight checks before important operations
              * Resource provisioning validation
              * System readiness verification
              * Troubleshooting connectivity issues

        Status Classification:
            The method returns ResourceLivenessStatus values with these meanings:
            - **ONLINE**: Resource is healthy and ready for operational use
            - **OFFLINE**: Resource is unresponsive, failed health checks, or not cached
            - **UNAUTHORIZED**: Authentication or authorization failures prevent access
            - **MISCONFIGURED**: Configuration errors prevent proper resource operation
            - **UNKNOWN**: Unexpected errors occurred during status determination

        Error Handling:
            This method provides comprehensive error handling that converts exceptions
            into appropriate status classifications rather than propagating them.
            This makes it safe for monitoring and status checking without exception handling.

        Performance Characteristics:
            - **ensure_item=False**: Typically completes in microseconds
            - **ensure_item=True**: May take seconds due to network operations
            - Thread safety adds minimal overhead via asyncio.Lock
            - Comprehensive logging aids performance monitoring

        Usage Patterns:
            ```python
            # Quick health check of cached resource
            status, detail = await manager.liveness_status()
            if status == ResourceLivenessStatus.ONLINE:
                resource = await manager.get()  # Safe to use

            # Comprehensive capability check
            status, detail = await manager.liveness_status(ensure_item=True)
            if status != ResourceLivenessStatus.ONLINE:
                logger.error(f"Manager unavailable: {detail}")
                return  # Handle the error appropriately

            # Monitoring loop
            async def monitor_resources():
                while True:
                    status, detail = await manager.liveness_status()
                    if status != ResourceLivenessStatus.ONLINE:
                        alert_ops_team(f"Resource {manager.full_name}: {status.name} - {detail}")
                    await asyncio.sleep(30)
            ```

        Args:
            ensure_item: Controls the liveness checking mode:
                - False (default): Quick check of cached resource only
                - True: Comprehensive check ensuring resource availability first

        Returns:
            tuple[ResourceLivenessStatus, str | None]: A tuple containing:
                - ResourceLivenessStatus: The health status classification
                - str | None: Human-readable detail message explaining the status,
                  particularly valuable for non-ONLINE statuses to aid debugging
                  and operational response

        Thread Safety:
            This method is fully thread-safe and coroutine-safe. Multiple concurrent
            calls are properly serialized to ensure consistent state observation.
            The ensure_item=True mode prevents duplicate resource creation.

        Logging:
            - Debug-level entry/exit logging for performance monitoring
            - Info-level result logging with mode and status details
            - Warning-level error logging handled by internal methods
            - All logs include manager class name and full_name for context

        See Also:
            ResourceLivenessStatus: Enum defining possible status return values
            get(): Method to actually retrieve the managed resource
            _check_liveness(): Abstract method that concrete classes implement
            is_alive(): Simplified boolean health check wrapper
        """
        mode = "provisioning" if ensure_item else "cached-only"
        _LOGGER.debug(
            "[%s] Checking liveness status (%s mode) for '%s'",
            self.__class__.__name__,
            mode,
            self.full_name,
        )

        async with self._lock:
            status, detail = await self._liveness_status_unlocked(ensure_item)
            _LOGGER.info(
                "[%s] Liveness check (%s mode) for '%s': %s%s",
                self.__class__.__name__,
                mode,
                self.full_name,
                status.name,
                f" ({detail})" if detail else "",
            )
            return status, detail

    async def _is_alive_unlocked(self) -> bool:
        """Check if the cached resource is alive without acquiring the synchronization lock.

        This private method provides a simplified boolean health check for the cached
        resource without lock acquisition. It assumes the caller has already acquired
        self._lock and delegates to _liveness_status_unlocked for the actual health check.

        Simplified Health Check:
            This method provides a boolean interface to the more comprehensive liveness
            checking functionality, returning True only if the resource status is
            ResourceLivenessStatus.ONLINE, False for any other status.

        Lock Safety:
            This method MUST be called while holding self._lock. It is designed for use
            within critical sections where simplified health checking is needed without
            the complexity of status detail messages.

        Usage Context:
            Called by:
            - close(): To check if a resource needs cleanup before closing
            - is_alive(): The public thread-safe wrapper method
            - Other internal methods needing simple boolean health checks
            - Should NOT be called directly by external code

        Performance:
            Very fast operation that delegates to _liveness_status_unlocked() and
            performs a simple enum comparison. The performance characteristics depend
            on the default cached-only mode of _liveness_status_unlocked().

        Returns:
            bool: True if the cached resource is ONLINE and ready for use,
                  False for any other status (OFFLINE, UNAUTHORIZED, MISCONFIGURED, UNKNOWN)
                  or if no resource is cached.

        Thread Safety:
            This method is NOT thread-safe by itself. The caller MUST hold self._lock
            before calling this method to ensure proper synchronization.

        See Also:
            is_alive(): The public, thread-safe wrapper for this method
            _liveness_status_unlocked(): The underlying method that provides detailed status
        """
        status, _ = await self._liveness_status_unlocked()
        return status == ResourceLivenessStatus.ONLINE

    async def is_alive(self) -> bool:
        """Check if the cached resource is currently alive and ready for use.

        This is a convenience method that provides a simple boolean interface to resource
        health checking with full thread safety. It returns True only if the cached resource
        is in the ONLINE state, making it ideal for quick health checks and conditional logic.

        Simplified Health Check:
            This method abstracts away the complexity of ResourceLivenessStatus values,
            providing a straightforward True/False answer to "Is my cached resource healthy?"
            It only returns True for ONLINE status, treating all other statuses as "not alive".

        Cached Resource Only:
            This method only checks cached resources (equivalent to liveness_status(ensure_item=False)).
            If no resource is cached, it returns False. It does not trigger resource creation.

        Performance Characteristics:
            - Very fast operation for cached resources
            - Immediate False return if no resource is cached
            - Full thread safety with minimal overhead
            - Suitable for frequent health checking

        Common Usage Patterns:
            ```python
            # Quick health check before using resource
            if await manager.is_alive():
                resource = await manager.get()
                # Use resource...
            else:
                # Handle unhealthy resource
                logger.warning(f"Resource {manager.full_name} is not alive")

            # Conditional resource cleanup
            if await manager.is_alive():
                await manager.close()  # Clean shutdown

            # Health monitoring with simple boolean logic
            healthy_managers = []
            for manager in all_managers:
                if await manager.is_alive():
                    healthy_managers.append(manager)
            ```

        Comparison with liveness_status():
            - is_alive(): Simple boolean, fast, no detail messages
            - liveness_status(): Detailed status with explanatory messages, more comprehensive

            Use is_alive() for:
            - Quick conditional checks
            - Boolean logic and filtering
            - Frequent monitoring loops

            Use liveness_status() for:
            - Detailed health analysis
            - Error reporting and debugging
            - Status dashboards and diagnostics

        Returns:
            bool: True if the cached resource is ONLINE and operational,
                  False for any other condition (no cached resource, non-ONLINE status)

        Thread Safety:
            This method is fully thread-safe and coroutine-safe. Multiple concurrent
            calls are properly serialized to ensure consistent state observation.

        See Also:
            liveness_status(): More detailed health checking with status explanations
            get(): Method to retrieve the managed resource
            close(): Method to clean up resources when they're no longer needed
        """
        async with self._lock:
            return await self._is_alive_unlocked()

    async def close(self) -> None:
        """Clean up and release the managed resource with comprehensive error handling.

        This method performs graceful shutdown of the managed resource by attempting
        to close the cached resource (if it exists and is responsive) and clearing
        the internal cache. It implements robust error handling to ensure cleanup
        proceeds even when individual operations fail.

        Cleanup Process:
            The method follows a multi-step cleanup process:
            1. **Liveness Check**: Verify if the cached resource is still responsive
            2. **Conditional Close**: Close the resource only if it's alive and responsive
            3. **Fallback Close**: If liveness check fails, attempt close anyway
            4. **Cache Clearing**: Always clear the cache regardless of close results
            5. **Comprehensive Logging**: Log all steps for debugging and monitoring

        Error Handling Strategy:
            This method uses a layered error handling approach:
            - **Liveness Failures**: Log warning, attempt close anyway
            - **Close Failures**: Log warning, continue with cache clearing
            - **Always Complete**: Cache is always cleared regardless of errors
            - **No Exception Propagation**: All exceptions are caught and logged

        Resource State Management:
            After this method completes:
            - The internal cache is guaranteed to be cleared (set to None)
            - Future get() calls will create a new resource instance
            - The manager returns to its initial uninitialized state
            - Any existing resource references become independent of the manager

        Liveness-Based Closing:
            The method performs a liveness check before attempting to close:
            - **Alive Resources**: Closed normally with proper AsyncClosable protocol
            - **Unresponsive Resources**: Close attempted but may be unreliable
            - **No Cached Resource**: No action needed, cache cleared immediately

        Idempotent Operation:
            This method is safe to call multiple times:
            - First call: Performs actual cleanup if resource exists
            - Subsequent calls: No-op with debug logging, no errors
            - Always safe: No side effects or state corruption

        Usage Patterns:
            ```python
            # Explicit cleanup in application shutdown
            async def shutdown():
                for manager in all_managers:
                    await manager.close()

            # Context manager pattern (if implemented)
            async with manager:
                resource = await manager.get()
                # Use resource...
            # manager.close() called automatically

            # Error recovery - reset manager state
            try:
                resource = await manager.get()
                # Resource operation fails...
            except Exception:
                await manager.close()  # Reset for retry
            ```

        Performance Considerations:
            - **Fast Path**: No cached resource results in immediate return
            - **Network Operations**: Closing remote resources may be slow
            - **Error Resilience**: Failed operations don't block overall cleanup
            - **Lock Contention**: Full synchronization ensures clean state transitions

        Logging Behavior:
            This method provides comprehensive logging at multiple levels:
            - **Debug**: Entry/exit, cache state, liveness results
            - **Info**: Successful close operations and completion
            - **Warning**: Liveness failures, close failures with context
            - **All logs**: Include manager class name and full_name for context

        Thread Safety:
            This method is fully thread-safe and coroutine-safe. The entire cleanup
            process is performed within a single critical section to ensure atomic
            state transitions and prevent race conditions with other operations.

        Exception Safety:
            This method never propagates exceptions to the caller. All errors are
            caught, logged with appropriate detail, and cleanup continues. This makes
            it safe to use in shutdown code, error handlers, and cleanup routines.

        See Also:
            get(): Method to retrieve resources (will create new after close)
            is_alive(): Method to check resource health before closing
            AsyncClosable: Protocol that managed resources must implement
        """
        _LOGGER.debug(
            "[%s] Starting close operation for '%s'",
            self.__class__.__name__,
            self.full_name,
        )

        async with self._lock:
            if self._item_cache:
                _LOGGER.debug(
                    "[%s] Found cached item for '%s', checking liveness before close",
                    self.__class__.__name__,
                    self.full_name,
                )
                try:
                    # Check liveness using the unlocked method since we already hold the lock
                    if await self._is_alive_unlocked():
                        _LOGGER.info(
                            "[%s] Closing live item for '%s'",
                            self.__class__.__name__,
                            self.full_name,
                        )
                        await self._item_cache.close()
                        _LOGGER.info(
                            "[%s] Successfully closed item for '%s'",
                            self.__class__.__name__,
                            self.full_name,
                        )
                    else:
                        _LOGGER.debug(
                            "[%s] Item for '%s' is not alive, skipping close",
                            self.__class__.__name__,
                            self.full_name,
                        )
                except Exception as e:
                    # If liveness check fails, still try to close the item
                    _LOGGER.warning(
                        "[%s] Liveness check failed during close for %s: %s",
                        self.__class__.__name__,
                        self.full_name,
                        e,
                    )
                    try:
                        _LOGGER.info(
                            "[%s] Attempting to close item despite liveness check failure for '%s'",
                            self.__class__.__name__,
                            self.full_name,
                        )
                        await self._item_cache.close()
                        _LOGGER.info(
                            "[%s] Successfully closed item for '%s' despite earlier liveness failure",
                            self.__class__.__name__,
                            self.full_name,
                        )
                    except Exception as close_e:
                        # Log close failures but continue cleanup
                        _LOGGER.warning(
                            "[%s] Failed to close item for %s: %s",
                            self.__class__.__name__,
                            self.full_name,
                            close_e,
                        )
            else:
                _LOGGER.debug(
                    "[%s] No cached item to close for '%s'",
                    self.__class__.__name__,
                    self.full_name,
                )

            self._item_cache = None
            _LOGGER.debug(
                "[%s] Cleared cache for '%s', close operation complete",
                self.__class__.__name__,
                self.full_name,
            )


class CommunitySessionManager(BaseItemManager[CoreSession]):
    """Manages the complete lifecycle of a Deephaven Community session.

    This specialized resource manager handles the creation, caching, health monitoring,
    and cleanup of CoreSession instances for Deephaven Community deployments. It extends
    BaseItemManager to provide Community-specific session management with full thread
    safety and comprehensive error handling.

    Core Capabilities:
        **Session Management**:
        - Lazy initialization: Sessions created only when first requested
        - Intelligent caching: Single session instance reused across requests
        - Health monitoring: Regular liveness checks via session.is_alive()
        - Graceful cleanup: Proper session disposal with error handling

        **Configuration-Driven**:
        - Dictionary-based configuration for flexible session setup
        - Support for all CoreSession configuration parameters
        - Server URL, authentication, and connection parameter handling
        - Environment-specific configuration support

        **Thread Safety**:
        - Full asyncio concurrency support for multi-task environments
        - Race condition prevention during session creation
        - Atomic operations for cache management and cleanup
        - Safe concurrent access from multiple coroutines

        **Error Resilience**:
        - Comprehensive exception handling during session creation
        - Liveness check failure recovery with fallback strategies
        - Cleanup operations that complete even when sessions are unresponsive
        - Detailed logging for debugging and operational monitoring

    Session Lifecycle Management:
        The manager implements a complete session lifecycle with these phases:

        1. **Initialization**: Manager created with name and configuration
        2. **Lazy Creation**: First get() call triggers CoreSession creation
        3. **Active Usage**: Cached session served for subsequent requests
        4. **Health Monitoring**: Periodic liveness checks ensure session validity
        5. **Graceful Shutdown**: close() properly disposes of session resources

    Configuration Requirements:
        The configuration dictionary must contain parameters suitable for CoreSession.from_config():
        - **server**: Deephaven Community server URL (required)
        - **auth_type**: Authentication method (basic, anonymous, etc.)
        - **username/password**: Credentials if using basic auth
        - **session_type**: Session configuration type
        - **extra**: Additional session-specific parameters

        Example configuration:
        ```python
        config = {
            "server": "http://localhost:10000",
            "auth_type": "anonymous",
            "session_type": "python"
        }
        ```

    Integration Patterns:
        **Registry Integration**:
        Typically used within CommunitySessionRegistry for managing multiple sessions:
        ```python
        registry = CommunitySessionRegistry()
        manager = CommunitySessionManager("worker-1", config)
        registry.add_manager(manager)
        ```

        **Standalone Usage**:
        Can be used independently for single-session applications:
        ```python
        manager = CommunitySessionManager("main-session", config)
        session = await manager.get()
        # Use session for Deephaven operations...
        await manager.close()
        ```

        **Health Monitoring**:
        Regular health checks for operational monitoring:
        ```python
        async def monitor_session(manager):
            status, detail = await manager.liveness_status()
            if status != ResourceLivenessStatus.ONLINE:
                alert_operations(f"Session {manager.full_name}: {detail}")
        ```

    Performance Characteristics:
        - **Session Creation**: May be slow (network handshake, authentication)
        - **Cached Access**: Very fast once session is established
        - **Health Checks**: Moderate cost (network round-trip to server)
        - **Memory Usage**: Single session instance per manager
        - **Concurrency**: Full asyncio support with minimal lock contention

    Error Handling:
        The manager handles various error scenarios gracefully:
        - **Configuration Errors**: Invalid parameters mapped to MISCONFIGURED status
        - **Network Failures**: Connection issues mapped to OFFLINE status
        - **Authentication Failures**: Auth problems mapped to UNAUTHORIZED status
        - **Session Errors**: Runtime issues handled with appropriate status mapping
        - **Cleanup Errors**: Close failures logged but don't prevent state cleanup

    Type Parameters:
        T = CoreSession: The specific session type managed by this implementation

    Thread Safety:
        All public methods are fully thread-safe and can be called concurrently
        from multiple asyncio tasks without synchronization concerns.

    See Also:
        BaseItemManager[T]: Generic base class providing core lifecycle management
        CoreSession: The Deephaven Community session type being managed
        CommunitySessionRegistry: Registry for managing multiple session managers
        SystemType.COMMUNITY: The system type constant for Community deployments
    """

    def __init__(self, name: str, config: dict[str, Any]):
        """Initialize a new Community session manager with configuration.

        Creates a new manager instance for handling a Deephaven Community session
        with the specified name and configuration parameters. The manager is initialized
        in an uninitialized state - no actual session is created until the first
        get() call is made (lazy initialization).

        Manager Identity:
            The manager is configured with:
            - **system_type**: Set to SystemType.COMMUNITY for Community deployments
            - **source**: Set to "community" to identify the configuration source
            - **name**: The unique identifier for this specific manager instance
            - **full_name**: Computed as "community.{name}" for global uniqueness

        Configuration Storage:
            The provided configuration dictionary is stored internally and used
            later during lazy session creation. The configuration is not validated
            at construction time - validation occurs during the first get() call
            when CoreSession.from_config() is invoked.

        Configuration Requirements:
            The config dictionary should contain parameters suitable for CoreSession.from_config():
            - **server** (required): Deephaven Community server URL
            - **auth_type**: Authentication method ("anonymous", "basic", etc.)
            - **username**: Username for basic authentication (if applicable)
            - **password**: Password for basic authentication (if applicable)
            - **session_type**: Type of session to create ("python", "groovy", etc.)
            - **extra**: Additional session-specific parameters
            - **use_tls**: Whether to use TLS/SSL for connections
            - **tls_root_certs**: Custom TLS certificates (if needed)

        Usage Examples:
            ```python
            # Anonymous Community session
            config = {
                "server": "http://localhost:10000",
                "auth_type": "anonymous",
                "session_type": "python"
            }
            manager = CommunitySessionManager("worker-1", config)

            # Authenticated Community session
            config = {
                "server": "https://deephaven.example.com:10000",
                "auth_type": "basic",
                "username": "user",
                "password": "pass",
                "session_type": "python",
                "use_tls": True
            }
            manager = CommunitySessionManager("secure-session", config)
            ```

        Manager State After Construction:
            - **Ready for use**: Manager is fully initialized and ready for get() calls
            - **No session created**: Actual CoreSession creation is deferred until needed
            - **Configuration stored**: Parameters are cached for later session creation
            - **Thread-safe**: Manager can be safely used from multiple asyncio tasks

        Args:
            name: Unique identifier for this manager instance within its registry.
                Used for logging, debugging, and creating the full_name identifier.
                Should be a descriptive name like "worker-1", "main-session", etc.
            config: Configuration dictionary containing all parameters needed for
                CoreSession creation. Must include at minimum a "server" parameter.
                Additional parameters depend on authentication and session requirements.

        Thread Safety:
            This constructor is thread-safe and can be called from any asyncio task.
            All initialization is synchronous and does not involve network operations.

        See Also:
            CoreSession.from_config(): The method used to create sessions from configuration
            SystemType.COMMUNITY: The system type constant used for Community deployments
            BaseItemManager.__init__(): The parent constructor that handles common initialization
        """
        super().__init__(
            system_type=SystemType.COMMUNITY,
            source="community",
            name=name,
        )
        self._config = config

    @override
    async def _create_item(self) -> CoreSession:
        """Create and initialize a new Deephaven Community session from configuration.

        This method implements the abstract _create_item() method from BaseItemManager
        to provide Community-specific session creation. It is called automatically
        during lazy initialization when get() is first invoked on an uninitialized
        manager.

        Session Creation Process:
            The method performs these steps:
            1. **Delegate to CoreSession**: Uses CoreSession.from_config() for actual creation
            2. **Network Handshake**: Establishes connection to the Deephaven Community server
            3. **Authentication**: Performs authentication if credentials are provided
            4. **Session Initialization**: Completes session setup and readiness checks
            5. **Error Handling**: Wraps failures in SessionCreationError with context

        Configuration Validation:
            The stored configuration is validated during this call:
            - **Server URL**: Must be reachable and running Deephaven Community
            - **Authentication**: Credentials must be valid if authentication is required
            - **Session Type**: Must be supported by the target server
            - **Network Settings**: TLS and connection parameters must be correct

        Performance Characteristics:
            This method involves network operations and may be slow:
            - **Network Latency**: Depends on distance to Deephaven server
            - **Authentication Time**: Additional delay for credential verification
            - **Session Initialization**: Server-side session setup overhead
            - **Typical Duration**: 100ms to several seconds depending on conditions

        Error Scenarios:
            Various failure modes are handled and wrapped in SessionCreationError:
            - **Connection Refused**: Server unreachable or not running
            - **Authentication Failed**: Invalid credentials or authorization issues
            - **Configuration Error**: Missing required parameters or invalid values
            - **Network Timeout**: Server too slow to respond or network issues
            - **Protocol Error**: Incompatible client/server versions
            - **Resource Exhaustion**: Server unable to create new sessions

        Exception Mapping:
            All underlying exceptions are caught and re-raised as SessionCreationError:
            - **Preserves Cause**: Original exception available via __cause__ attribute
            - **Adds Context**: Error message includes manager name and configuration context
            - **Consistent Interface**: All callers receive uniform exception type
            - **Detailed Logging**: Full error details logged for debugging

        Thread Safety:
            This method is fully thread-safe and can be called concurrently,
            though the BaseItemManager ensures only one creation attempt occurs
            per manager instance at a time.

        Returns:
            CoreSession: A fully initialized and connected Deephaven Community session
                ready for use. The session will have completed authentication and
                initialization, and its is_alive() method should return True.

        Raises:
            SessionCreationError: If session creation fails for any reason. The error
                message will include context about the failure, and the original
                exception will be available via the __cause__ attribute.

        Implementation Notes:
            This method is marked with @override to indicate it implements the abstract
            method from BaseItemManager. It must not be called directly - use get()
            instead to ensure proper caching and error handling.

        See Also:
            CoreSession.from_config(): The underlying method used for session creation
            BaseItemManager.get(): The public method that triggers lazy initialization
            SessionCreationError: The exception type raised on creation failures
        """
        try:
            _LOGGER.info(
                "[%s] Creating community session for %s",
                self.__class__.__name__,
                self.full_name,
            )
            return await CoreSession.from_config(self._config)
        except Exception as e:
            _LOGGER.error(
                "[%s] Failed to create community session for %s: %s",
                self.__class__.__name__,
                self.full_name,
                e,
            )
            raise SessionCreationError(
                f"Failed to create session for community worker {self._name}: {e}"
            ) from e

    @override
    async def _check_liveness(
        self, item: CoreSession
    ) -> tuple[ResourceLivenessStatus, str | None]:
        """Assess the health and responsiveness of a Deephaven Community session.

        This method implements the abstract _check_liveness() method from BaseItemManager
        to provide Community-specific session health checking. It evaluates whether
        the provided CoreSession is still connected, authenticated, and capable of
        processing requests.

        Health Check Process:
            The method performs a simple but effective health assessment:
            1. **Delegate to CoreSession**: Calls the session's is_alive() method
            2. **Network Round-Trip**: The is_alive() call typically involves server communication
            3. **Status Classification**: Maps boolean result to ResourceLivenessStatus
            4. **Detail Generation**: Provides explanatory message for non-ONLINE states

        Session Health Criteria:
            A Community session is considered ONLINE when:
            - **Connection Active**: Network connection to server is established
            - **Authentication Valid**: Session credentials are still accepted
            - **Server Responsive**: Server responds to health check requests
            - **Protocol Functional**: Session can execute basic operations

            A session is considered OFFLINE when:
            - **Connection Lost**: Network connection has been dropped
            - **Authentication Expired**: Session credentials are no longer valid
            - **Server Unreachable**: Server is down or unreachable
            - **Protocol Error**: Session is in an unusable state

        Performance Characteristics:
            This method involves network communication and timing varies:
            - **Local Server**: Typically 1-10ms for health checks
            - **Remote Server**: 10-100ms+ depending on network latency
            - **Server Load**: Response time affected by server utilization
            - **Network Issues**: May timeout or fail on connectivity problems

        Error Handling Strategy:
            This method is designed to be exception-transparent:
            - **No Exception Catching**: All exceptions propagate to caller
            - **Caller Responsibility**: BaseItemManager.liveness_status() handles exceptions
            - **Exception Mapping**: Caller maps exceptions to appropriate status codes
            - **Consistent Interface**: Simple delegation pattern for maintainability

        Status Mapping:
            The method maps CoreSession health to ResourceLivenessStatus:
            - **True → ONLINE**: Session is healthy and ready for use
            - **False → OFFLINE**: Session is unhealthy with explanatory detail message

            Note: This method only returns ONLINE or OFFLINE. Other status values
            (UNAUTHORIZED, MISCONFIGURED, UNKNOWN) are handled by the exception
            handling in the calling liveness_status() method.

        Thread Safety:
            This method is fully thread-safe and can be called concurrently.
            The underlying CoreSession.is_alive() method handles its own synchronization.

        Usage Context:
            This method is called automatically by BaseItemManager.liveness_status()
            and should not be called directly by external code. It represents the
            Community-specific implementation of the abstract health checking contract.

        Args:
            item: The CoreSession instance to evaluate for health and responsiveness.
                Must be a valid CoreSession that was previously created by this manager.
                The session may be in any state (healthy, unhealthy, disconnected).

        Returns:
            tuple[ResourceLivenessStatus, str | None]: A tuple containing:
                - ResourceLivenessStatus: Either ONLINE (healthy) or OFFLINE (unhealthy)
                - str | None: Detail message explaining the status, None for ONLINE,
                  descriptive message for OFFLINE states

        Implementation Notes:
            This method is marked with @override to indicate it implements the abstract
            method from BaseItemManager. The implementation is intentionally simple
            to maintain reliability and debuggability.

        See Also:
            CoreSession.is_alive(): The underlying method used for health assessment
            BaseItemManager.liveness_status(): The public method that calls this implementation
            ResourceLivenessStatus: The enumeration of possible health states
        """
        alive = await item.is_alive()

        if alive:
            return (ResourceLivenessStatus.ONLINE, None)
        else:
            return (ResourceLivenessStatus.OFFLINE, "Session not alive")


class EnterpriseSessionManager(BaseItemManager[CorePlusSession]):
    """Manages the complete lifecycle of a Deephaven Enterprise session with customizable creation.

    This specialized resource manager handles CorePlusSession instances for Deephaven Enterprise
    deployments using a flexible function-based creation approach. Unlike CommunitySessionManager's
    configuration-driven approach, this manager uses injectable creation functions to support
    complex Enterprise authentication flows and diverse session creation strategies.

    Core Architecture:
        **Function-Based Creation**:
        - Injectable creation function for maximum flexibility
        - Decoupled session creation logic from lifecycle management
        - Support for complex authentication flows and custom protocols
        - Enables factory patterns, connection pooling, and advanced creation strategies

        **Enterprise-Specific Features**:
        - Support for CorePlusSession with Enterprise-only capabilities
        - Complex authentication handling (SAML, OAuth, custom protocols)
        - Multi-tenant and workspace-aware session management
        - Advanced security and compliance features

        **Lifecycle Management**:
        - Lazy initialization with custom creation functions
        - Intelligent caching of expensive Enterprise sessions
        - Health monitoring via CorePlusSession.is_alive()
        - Graceful cleanup with comprehensive error handling

        **Thread Safety**:
        - Full asyncio concurrency support for Enterprise workloads
        - Race condition prevention during complex session creation
        - Atomic operations for cache management and cleanup
        - Safe concurrent access from multiple coroutines and tasks

    Creation Function Pattern:
        The manager uses dependency injection for session creation:

        **Function Signature**:
        ```python
        async def creation_function(source: str, name: str) -> CorePlusSession:
            # Custom creation logic here
            return session
        ```

        **Flexibility Benefits**:
        - **Authentication Strategies**: Support for any Enterprise auth method
        - **Configuration Sources**: Database, vault, config service, etc.
        - **Factory Integration**: Compatible with session factory patterns
        - **Testing Support**: Easy mocking and testing with custom functions
        - **Environment Adaptation**: Different creation logic per environment

    Integration Patterns:
        **Factory Integration**:
        ```python
        factory = CorePlusSessionFactory(config)
        creation_func = lambda src, name: factory.create_session(src, name)
        manager = EnterpriseSessionManager("enterprise", "worker-1", creation_func)
        ```

        **Custom Authentication**:
        ```python
        async def saml_session_creator(source: str, name: str) -> CorePlusSession:
            token = await saml_auth.get_token()
            return await CorePlusSession.from_token(server_url, token)

        manager = EnterpriseSessionManager("saml", "user-123", saml_session_creator)
        ```

        **Registry Integration**:
        ```python
        registry = EnterpriseSessionRegistry()
        manager = EnterpriseSessionManager("enterprise", "session-1", creation_func)
        registry.add_manager(manager)
        ```

        **Health Monitoring**:
        ```python
        async def monitor_enterprise_session(manager):
            status, detail = await manager.liveness_status(ensure_item=True)
            if status != ResourceLivenessStatus.ONLINE:
                alert_enterprise_ops(f"Enterprise session {manager.full_name}: {detail}")
        ```

    Performance Characteristics:
        - **Session Creation**: Variable (depends on creation function complexity)
        - **Authentication**: Can be slow for Enterprise protocols (SAML, OAuth)
        - **Cached Access**: Very fast once session is established and cached
        - **Health Checks**: Moderate cost (Enterprise servers may be slower)
        - **Memory Usage**: Single CorePlusSession instance per manager
        - **Concurrency**: Full asyncio support with Enterprise-grade synchronization

    Error Handling:
        The manager provides comprehensive error handling for Enterprise scenarios:
        - **Creation Failures**: Custom function exceptions wrapped in SessionCreationError
        - **Authentication Errors**: Enterprise auth failures mapped to UNAUTHORIZED
        - **Configuration Issues**: Missing/invalid parameters mapped to MISCONFIGURED
        - **Network Problems**: Enterprise connectivity issues mapped to OFFLINE
        - **Permission Errors**: Access control failures handled gracefully
        - **Cleanup Errors**: Enterprise session disposal failures logged but don't block cleanup

    Enterprise Use Cases:
        - **Multi-Tenant Applications**: Different sessions per tenant or workspace
        - **Complex Authentication**: SAML, OAuth, custom Enterprise protocols
        - **Factory Integration**: Working with CorePlusSessionFactory instances
        - **Dynamic Configuration**: Runtime-determined session creation parameters
        - **Testing and Development**: Mock sessions and test doubles
        - **High-Performance Workloads**: Enterprise-grade session management

    Comparison with CommunitySessionManager:
        | Feature | CommunitySessionManager | EnterpriseSessionManager |
        |---------|------------------------|-------------------------|
        | Creation | Configuration dict | Injectable function |
        | Session Type | CoreSession | CorePlusSession |
        | Flexibility | Limited to config | Full customization |
        | Use Case | Simple Community | Complex Enterprise |
        | Authentication | Basic/Anonymous | Any Enterprise method |
        | Integration | Direct config | Factory/function patterns |

    Type Parameters:
        T = CorePlusSession: The specific Enterprise session type managed by this implementation

    Thread Safety:
        All public methods are fully thread-safe and can be called concurrently
        from multiple asyncio tasks without synchronization concerns.

    See Also:
        BaseItemManager[T]: Generic base class providing core lifecycle management
        CorePlusSession: The Deephaven Enterprise session type being managed
        CorePlusSessionFactory: Common factory for creating Enterprise sessions
        SystemType.ENTERPRISE: The system type constant for Enterprise deployments
        CommunitySessionManager: The simpler configuration-based Community manager
    """

    def __init__(
        self,
        source: str,
        name: str,
        creation_function: Callable[[str, str], Awaitable["CorePlusSession"]],
    ):
        """Initialize a new Enterprise session manager with injectable creation logic.

        Creates a new manager instance for handling Deephaven Enterprise sessions
        using a flexible, function-based creation approach. The manager is initialized
        in an uninitialized state - no actual session is created until the first
        get() call triggers the provided creation function.

        Manager Identity:
            The manager is configured with:
            - **system_type**: Set to SystemType.ENTERPRISE for Enterprise deployments
            - **source**: The configuration source identifier provided by caller
            - **name**: The unique identifier for this specific manager instance
            - **full_name**: Computed as "{source}.{name}" for global uniqueness

        Function-Based Creation:
            Unlike CommunitySessionManager's config-dict approach, this manager uses
            dependency injection with a creation function. This provides maximum
            flexibility for Enterprise scenarios where session creation may involve:
            - Complex authentication protocols (SAML, OAuth, custom)
            - Dynamic configuration retrieval from databases or vaults
            - Factory pattern integration with CorePlusSessionFactory
            - Custom Enterprise-specific logic and workflows

        Creation Function Contract:
            The provided function must conform to this signature and behavior:
            ```python
            async def creation_function(source: str, name: str) -> CorePlusSession:
                # Function receives the same source and name passed to constructor
                # Function must return a fully initialized CorePlusSession
                # Function may perform any required authentication, configuration
                # Function should raise exceptions for creation failures
                return session
            ```

        Deferred Validation:
            The creation function is stored but not validated at construction time:
            - **No Early Validation**: Function is not called during __init__
            - **Lazy Validation**: First get() call will validate function behavior
            - **Error Deferral**: Creation failures are handled during actual use
            - **Testing Friendly**: Allows mock functions and test doubles

        Integration Examples:
            **Factory Integration**:
            ```python
            factory = CorePlusSessionFactory(config)
            manager = EnterpriseSessionManager(
                "enterprise", "worker-1",
                lambda src, name: factory.create_session(src, name)
            )
            ```

            **Custom Authentication**:
            ```python
            async def saml_creator(source: str, name: str) -> CorePlusSession:
                token = await saml_provider.authenticate(name)
                return await CorePlusSession.from_token(server_url, token)

            manager = EnterpriseSessionManager("saml", "user-123", saml_creator)
            ```

            **Configuration Service**:
            ```python
            async def config_service_creator(source: str, name: str) -> CorePlusSession:
                config = await config_service.get_session_config(source, name)
                return await CorePlusSession.from_config(config)

            manager = EnterpriseSessionManager("config-svc", "session-1", config_service_creator)
            ```

        Manager State After Construction:
            - **Ready for use**: Manager is fully initialized and ready for get() calls
            - **No session created**: Actual CorePlusSession creation is deferred until needed
            - **Function stored**: Creation function is cached for later invocation
            - **Thread-safe**: Manager can be safely used from multiple asyncio tasks

        Args:
            source: Configuration source identifier that will be passed to the creation
                function. This can be any string that helps the creation function locate
                or identify the appropriate configuration (e.g., "enterprise-config",
                "/path/to/config", "vault://secrets/sessions", "database://session-configs").
            name: Unique identifier for this manager instance within its registry.
                Used for logging, debugging, and creating the full_name identifier.
                Also passed to creation function for session identification.
            creation_function: Async callable that creates CorePlusSession instances.
                Must take (source: str, name: str) parameters and return CorePlusSession.
                Should handle all aspects of session creation including authentication,
                configuration retrieval, and connection establishment.

        Thread Safety:
            This constructor is thread-safe and can be called from any asyncio task.
            All initialization is synchronous and does not involve network operations.

        See Also:
            CorePlusSession: The Enterprise session type that creation functions must return
            CorePlusSessionFactory: Common factory implementation for Enterprise sessions
            SystemType.ENTERPRISE: The system type constant used for Enterprise deployments
            BaseItemManager.__init__(): The parent constructor that handles common initialization
        """
        super().__init__(system_type=SystemType.ENTERPRISE, source=source, name=name)
        self._creation_function = creation_function

    @override
    async def _create_item(self) -> CorePlusSession:
        """Create and initialize a new Deephaven Enterprise session using the injected creation function.

        This method implements the abstract _create_item() method from BaseItemManager
        to provide Enterprise-specific session creation using the injectable creation
        function pattern. It is called automatically during lazy initialization when
        get() is first invoked on an uninitialized manager.

        Function-Based Creation Process:
            The method performs these steps:
            1. **Invoke Creation Function**: Calls the provided creation function with source and name
            2. **Function Execution**: The creation function handles all Enterprise-specific logic
            3. **Authentication & Setup**: Function performs auth, config retrieval, connection setup
            4. **Session Initialization**: Function returns fully initialized CorePlusSession
            5. **Error Handling**: Wraps any creation function failures in SessionCreationError

        Creation Function Flexibility:
            The injected function provides maximum flexibility for Enterprise scenarios:
            - **Authentication Methods**: SAML, OAuth, custom Enterprise protocols
            - **Configuration Sources**: Databases, vaults, config services, files
            - **Factory Integration**: CorePlusSessionFactory or custom factory patterns
            - **Dynamic Logic**: Runtime-determined creation parameters and strategies
            - **Environment Adaptation**: Different creation logic per deployment environment

        Performance Characteristics:
            Performance depends entirely on the provided creation function:
            - **Simple Functions**: Fast creation for basic Enterprise setups
            - **Complex Auth**: Slower for SAML/OAuth with multiple round trips
            - **Config Retrieval**: Variable depending on configuration source performance
            - **Network Operations**: May involve multiple network calls for Enterprise setup
            - **Typical Duration**: Highly variable from 100ms to several seconds

        Error Scenarios and Handling:
            Various failure modes are wrapped in SessionCreationError:
            - **Function Exceptions**: Any exception from the creation function
            - **Authentication Failures**: Enterprise auth protocol failures
            - **Configuration Errors**: Missing or invalid configuration data
            - **Network Issues**: Connectivity problems during session establishment
            - **Permission Errors**: Access control or authorization failures
            - **Resource Exhaustion**: Enterprise server unable to create sessions

        Exception Wrapping Strategy:
            All creation function exceptions are caught and re-raised as SessionCreationError:
            - **Preserves Cause**: Original exception available via __cause__ attribute
            - **Adds Context**: Error message includes manager identity and Enterprise context
            - **Consistent Interface**: All callers receive uniform exception type
            - **Detailed Logging**: Full error details logged for Enterprise troubleshooting

        Function Parameter Passing:
            The creation function is called with the exact parameters from construction:
            - **source**: The configuration source identifier from __init__
            - **name**: The manager name from __init__
            - **No Modification**: Parameters are passed through unchanged
            - **Function Responsibility**: Creation function interprets parameters as needed

        Thread Safety:
            This method is fully thread-safe and can be called concurrently,
            though the BaseItemManager ensures only one creation attempt occurs
            per manager instance at a time.

        Returns:
            CorePlusSession: A fully initialized and connected Deephaven Enterprise session
                ready for use. The session will have completed all required authentication
                and initialization, and its is_alive() method should return True.

        Raises:
            SessionCreationError: If the creation function fails for any reason. The error
                message will include context about the failure, and the original
                exception will be available via the __cause__ attribute.

        Implementation Notes:
            This method is marked with @override to indicate it implements the abstract
            method from BaseItemManager. It must not be called directly - use get()
            instead to ensure proper caching and error handling.

        See Also:
            BaseItemManager.get(): The public method that triggers lazy initialization
            SessionCreationError: The exception type raised on creation failures
            CorePlusSession: The Enterprise session type returned by creation functions
            CorePlusSessionFactory: Common factory that can be used with this manager
        """
        try:
            _LOGGER.info(
                "[%s] Creating enterprise session for %s using creation function",
                self.__class__.__name__,
                self.full_name,
            )
            return await self._creation_function(self._source, self._name)
        except Exception as e:
            _LOGGER.error(
                "[%s] Failed to create enterprise session for %s: %s",
                self.__class__.__name__,
                self.full_name,
                e,
            )
            raise SessionCreationError(
                f"Failed to create enterprise session for {self._name}: {e}"
            ) from e

    @override
    async def _check_liveness(
        self, item: CorePlusSession
    ) -> tuple[ResourceLivenessStatus, str | None]:
        """Evaluate the health and responsiveness of a Deephaven Enterprise session.

        This method implements the abstract _check_liveness() method from BaseItemManager
        to provide Enterprise-specific session health checking. It delegates to the
        CorePlusSession.is_alive() method to determine if the Enterprise session is
        still connected, authenticated, and functional.

        Enterprise Session Health Assessment:
            The method performs a comprehensive health check of the Enterprise session:
            - **Connection Status**: Verifies the underlying network connection is active
            - **Authentication State**: Checks that Enterprise credentials are still valid
            - **Server Responsiveness**: Confirms the Enterprise server is responding
            - **Session Validity**: Ensures the session is still recognized by the server
            - **Protocol Health**: Validates the Enterprise protocol is functioning correctly

        CorePlusSession Integration:
            This method leverages the CorePlusSession's built-in health checking:
            - **Delegates to is_alive()**: Uses the session's native health check method
            - **Enterprise-Specific Logic**: CorePlusSession handles Enterprise-specific checks
            - **Async Operation**: Supports Enterprise servers that may have higher latency
            - **Comprehensive Validation**: Enterprise sessions perform more thorough validation

        Health Check Scenarios:
            A CorePlusSession is considered ONLINE when:
            - **Connection Active**: Network connection to Enterprise server is established
            - **Authentication Valid**: Enterprise credentials (tokens, certificates) are current
            - **Server Responsive**: Enterprise server responds to health check requests
            - **Session Active**: Server recognizes and accepts the session
            - **Protocol Functional**: Enterprise protocol layer is operating correctly

            A CorePlusSession is considered OFFLINE when:
            - **Connection Lost**: Network connection has been dropped or is unstable
            - **Authentication Expired**: Enterprise credentials have expired or been revoked
            - **Server Unreachable**: Enterprise server is down, overloaded, or unreachable
            - **Session Expired**: Server has terminated or forgotten the session
            - **Protocol Error**: Enterprise protocol is in an unusable or error state

        Performance Characteristics:
            This method involves network communication with Enterprise servers:
            - **Enterprise Servers**: Typically 10-100ms+ for health checks
            - **Complex Auth**: Additional overhead for Enterprise credential validation
            - **Network Latency**: Affected by distance to Enterprise infrastructure
            - **Server Load**: Enterprise servers may have higher response times
            - **Security Overhead**: Enterprise security protocols add processing time

        Error Handling Strategy:
            This method is designed to be exception-transparent:
            - **No Exception Catching**: All exceptions propagate to caller
            - **Caller Responsibility**: BaseItemManager.liveness_status() handles exceptions
            - **Exception Mapping**: Caller maps exceptions to appropriate status codes
            - **Consistent Interface**: Simple delegation pattern for maintainability

        Status Mapping:
            The method maps CorePlusSession health to ResourceLivenessStatus:
            - **True → ONLINE**: Enterprise session is healthy and ready for use
            - **False → OFFLINE**: Enterprise session is unhealthy with explanatory detail message

            Note: This method only returns ONLINE or OFFLINE. Other status values
            (UNAUTHORIZED, MISCONFIGURED, UNKNOWN) are handled by the exception
            handling in the calling liveness_status() method.

        Enterprise vs Community Differences:
            Enterprise session health checking differs from Community sessions:
            - **More Complex**: Enterprise sessions have additional validation layers
            - **Higher Latency**: Enterprise servers may be geographically distributed
            - **Security Overhead**: Enterprise protocols include additional security checks
            - **Credential Validation**: Enterprise sessions validate complex credentials
            - **Multi-Tenant Checks**: Enterprise sessions may validate tenant/workspace status

        Thread Safety:
            This method is fully thread-safe and can be called concurrently.
            The underlying CorePlusSession.is_alive() method handles its own synchronization.

        Usage Context:
            This method is called automatically by BaseItemManager.liveness_status()
            and should not be called directly by external code. It represents the
            Enterprise-specific implementation of the abstract health checking contract.

        Args:
            item: The CorePlusSession instance to evaluate for health and responsiveness.
                Must be a valid CorePlusSession that was previously created by this manager.
                The session may be in any state (healthy, unhealthy, disconnected).

        Returns:
            tuple[ResourceLivenessStatus, str | None]: A tuple containing:
                - ResourceLivenessStatus: Either ONLINE (healthy) or OFFLINE (unhealthy)
                - str | None: Detail message explaining the status, None for ONLINE,
                  descriptive message for OFFLINE

        Implementation Notes:
            This method is marked with @override to indicate it implements the abstract
            method from BaseItemManager. It follows the same pattern as other session
            manager implementations but handles Enterprise-specific session types.

        See Also:
            BaseItemManager.liveness_status(): The public method that calls this implementation
            CorePlusSession.is_alive(): The Enterprise session health check method
            ResourceLivenessStatus: The enumeration of possible health states
            CommunitySessionManager._check_liveness(): The Community equivalent method
        """
        alive = await item.is_alive()

        if alive:
            return (ResourceLivenessStatus.ONLINE, None)
        else:
            return (ResourceLivenessStatus.OFFLINE, "Session not alive")


class CorePlusSessionFactoryManager(BaseItemManager[CorePlusSessionFactory]):
    """Manages the lifecycle of a Deephaven Enterprise session factory with configuration-driven creation.

    This manager is a foundational component of the Deephaven Enterprise session architecture,
    providing lifecycle management for CorePlusSessionFactory instances. Rather than managing
    individual sessions, it manages the factory that creates sessions, enabling consistent
    configuration, authentication, and connection pooling across multiple session creation requests.

    Core Architecture:
        **Factory-Level Management**:
        - Manages CorePlusSessionFactory instances rather than individual sessions
        - Provides shared configuration and authentication across multiple sessions
        - Enables connection pooling and resource sharing at the factory level
        - Supports Enterprise-wide factory configuration and management

        **Configuration-Driven Creation**:
        - Uses dictionary-based configuration for factory creation
        - Supports complex Enterprise configuration with nested parameters
        - Validates configuration during factory creation process
        - Enables dynamic factory configuration from external sources

        **Lifecycle Management**:
        - Lazy initialization with thread-safe caching of expensive factories
        - Health monitoring via factory ping() method for lightweight checks
        - Graceful cleanup with comprehensive resource disposal
        - Integration with registry patterns for multi-factory management

        **Enterprise Integration**:
        - Designed for Enterprise-scale deployments with multiple configurations
        - Supports complex authentication and connection strategies
        - Enables centralized management of factory configurations
        - Facilitates factory sharing across application components

    Factory vs Session Management:
        This manager operates at a higher abstraction level than session managers:

        **Factory Management (This Class)**:
        - Manages CorePlusSessionFactory instances
        - Configuration-driven creation with complex parameter support
        - Health checks via lightweight ping() operations
        - Shared across multiple session creation requests
        - Optimized for Enterprise-scale factory lifecycle management

        **Session Management (EnterpriseSessionManager)**:
        - Manages individual CorePlusSession instances
        - Function-based creation with injectable logic
        - Health checks via session is_alive() operations
        - One-to-one mapping between manager and session
        - Optimized for individual session lifecycle management

    Configuration Architecture:
        The manager accepts rich configuration dictionaries that define:
        - **Server Configuration**: URLs, ports, connection parameters
        - **Authentication Settings**: Credentials, tokens, certificates, auth protocols
        - **Factory Options**: Connection pooling, timeout settings, retry policies
        - **Enterprise Features**: Multi-tenancy, workspace configuration, security settings
        - **Performance Tuning**: Connection limits, caching strategies, optimization flags

    Integration Patterns:
        **Registry Integration**:
        ```python
        registry = CorePlusSessionFactoryRegistry()
        manager = CorePlusSessionFactoryManager("prod-factory", config)
        registry.add_manager(manager)
        factory = await registry.get_factory("prod-factory")
        ```

        **Factory-Based Session Creation**:
        ```python
        factory_manager = CorePlusSessionFactoryManager("enterprise", config)
        factory = await factory_manager.get()
        session = await factory.create_session(source="app", name="worker-1")
        ```

        **Multi-Configuration Support**:
        ```python
        configs = {
            "prod": {"url": "prod-server", "auth": prod_auth},
            "dev": {"url": "dev-server", "auth": dev_auth},
            "test": {"url": "test-server", "auth": test_auth}
        }
        managers = {
            env: CorePlusSessionFactoryManager(env, config)
            for env, config in configs.items()
        }
        ```

        **Health Monitoring**:
        ```python
        async def monitor_factory_health(manager):
            status, detail = await manager.liveness_status(ensure_item=True)
            if status != ResourceLivenessStatus.ONLINE:
                alert_ops(f"Factory {manager.full_name} health issue: {detail}")
        ```

    Performance Characteristics:
        - **Factory Creation**: Expensive operation involving authentication and connection setup
        - **Factory Caching**: Very fast access once factory is created and cached
        - **Health Checks**: Lightweight ping operations (faster than full session checks)
        - **Memory Usage**: Single CorePlusSessionFactory instance per manager
        - **Connection Pooling**: Factory handles connection reuse across sessions
        - **Concurrency**: Full asyncio support with Enterprise-grade synchronization

    Health Monitoring:
        Factory health is monitored via the ping() method rather than is_alive():
        - **Lightweight Operation**: Ping is faster than full session health checks
        - **Connection Validation**: Verifies underlying connection without session overhead
        - **Server Responsiveness**: Confirms Enterprise server is responding
        - **Authentication Check**: Validates that factory credentials are still valid
        - **Resource Availability**: Ensures factory can create new sessions

    Error Handling:
        The manager provides comprehensive error handling for Enterprise factory scenarios:
        - **Configuration Errors**: Invalid or missing configuration parameters
        - **Authentication Failures**: Enterprise credential validation failures
        - **Connection Issues**: Network connectivity problems to Enterprise servers
        - **Resource Exhaustion**: Enterprise server unable to support more factories
        - **Permission Errors**: Access control failures for factory creation
        - **Cleanup Errors**: Factory disposal failures logged but don't block cleanup

    Enterprise Use Cases:
        - **Multi-Environment Deployments**: Different factories for prod/dev/test
        - **Connection Pooling**: Shared connection resources across sessions
        - **Centralized Configuration**: Factory-level configuration management
        - **Authentication Sharing**: Reuse authentication across multiple sessions
        - **Resource Optimization**: Shared factories reduce connection overhead
        - **Monitoring and Observability**: Factory-level health and performance monitoring

    Comparison with Session Managers:
        | Feature | CorePlusSessionFactoryManager | EnterpriseSessionManager |
        |---------|------------------------------|-------------------------|
        | Manages | CorePlusSessionFactory | CorePlusSession |
        | Creation | Configuration dict | Injectable function |
        | Health Check | ping() | is_alive() |
        | Use Case | Factory lifecycle | Session lifecycle |
        | Performance | Expensive creation, fast reuse | Variable per session |
        | Sharing | Shared across sessions | One-to-one mapping |

    Type Parameters:
        T = CorePlusSessionFactory: The specific Enterprise factory type managed by this implementation

    Thread Safety:
        All public methods are fully thread-safe and can be called concurrently
        from multiple asyncio tasks without synchronization concerns.

    See Also:
        BaseItemManager[T]: Generic base class providing core lifecycle management
        CorePlusSessionFactory: The Deephaven Enterprise factory type being managed
        CorePlusSessionFactoryRegistry: Registry for managing multiple factory managers
        EnterpriseSessionManager: Session-level manager that can use factories
        SystemType.ENTERPRISE: The system type constant for Enterprise deployments
    """

    def __init__(self, name: str, config: dict[str, Any]):
        """Initialize a new Enterprise session factory manager with configuration-driven creation.

        Creates a new manager instance for handling Deephaven Enterprise session factories
        using a configuration dictionary approach. The manager is initialized in an
        uninitialized state - no actual factory is created until the first get() call
        triggers the factory creation process using the provided configuration.

        Manager Identity and Configuration:
            The manager is configured with:
            - **system_type**: Set to SystemType.ENTERPRISE for Enterprise factory management
            - **source**: Set to "factory" to indicate this manages factory instances
            - **name**: The unique identifier for this specific factory manager instance
            - **full_name**: Computed as "factory.{name}" for global uniqueness
            - **config**: The complete configuration dictionary for factory creation

        Configuration-Driven Architecture:
            Unlike EnterpriseSessionManager's function-based approach, this manager uses
            a rich configuration dictionary that supports:
            - **Server Configuration**: URLs, ports, connection parameters, timeouts
            - **Authentication Settings**: Credentials, tokens, certificates, auth protocols
            - **Factory Options**: Connection pooling, caching, retry policies
            - **Enterprise Features**: Multi-tenancy, workspace settings, security options
            - **Performance Tuning**: Connection limits, optimization flags, resource limits

        Configuration Dictionary Structure:
            The config dictionary supports comprehensive Enterprise factory settings:
            ```python
            config = {
                # Server connection settings
                "url": "https://enterprise.deephaven.io",
                "port": 8443,
                "timeout": 30.0,

                # Authentication configuration
                "auth": {
                    "type": "saml",
                    "saml_config": {...},
                    "credentials": {...}
                },

                # Factory-specific options
                "factory_options": {
                    "connection_pool_size": 10,
                    "cache_sessions": True,
                    "retry_policy": {...}
                },

                # Enterprise features
                "enterprise": {
                    "multi_tenant": True,
                    "workspace_config": {...},
                    "security_settings": {...}
                }
            }
            ```

        Deferred Factory Creation:
            The factory creation is deferred until actual use:
            - **No Early Creation**: Factory is not created during __init__
            - **Lazy Initialization**: First get() call triggers factory creation
            - **Configuration Validation**: Config validation occurs during factory creation
            - **Error Deferral**: Configuration errors are handled during actual use
            - **Testing Friendly**: Allows configuration validation without network operations

        Factory Manager Patterns:
            **Single Factory Management**:
            ```python
            config = load_enterprise_config("production")
            manager = CorePlusSessionFactoryManager("prod-factory", config)
            factory = await manager.get()  # Creates factory on first access
            session = await factory.create_session("app", "worker-1")
            ```

            **Multi-Environment Support**:
            ```python
            environments = {"prod": prod_config, "dev": dev_config, "test": test_config}
            managers = {
                env: CorePlusSessionFactoryManager(f"{env}-factory", config)
                for env, config in environments.items()
            }
            ```

            **Registry Integration**:
            ```python
            registry = CorePlusSessionFactoryRegistry()
            for env, config in configurations.items():
                manager = CorePlusSessionFactoryManager(f"{env}-factory", config)
                registry.add_manager(manager)
            ```

            **Health Monitoring Setup**:
            ```python
            manager = CorePlusSessionFactoryManager("enterprise", config)

            async def monitor_factory():
                status, detail = await manager.liveness_status(ensure_item=True)
                if status != ResourceLivenessStatus.ONLINE:
                    alert_ops(f"Factory {manager.full_name} issue: {detail}")
            ```

        Configuration Validation Strategy:
            Configuration validation is deferred to factory creation time:
            - **No Constructor Validation**: Config is stored but not validated during __init__
            - **Lazy Validation**: Configuration is validated when factory is first created
            - **Comprehensive Checking**: Factory creation validates all config parameters
            - **Error Context**: Validation errors include manager identity and config context
            - **Flexible Configuration**: Allows dynamic config loading and modification

        Manager State After Construction:
            - **Ready for use**: Manager is fully initialized and ready for get() calls
            - **No factory created**: Actual CorePlusSessionFactory creation is deferred
            - **Configuration stored**: Config dictionary is cached for factory creation
            - **Thread-safe**: Manager can be safely used from multiple asyncio tasks
            - **Registry-ready**: Manager can be immediately added to registries

        Args:
            name: Unique identifier for this factory manager instance. Used for logging,
                debugging, registry management, and creating the full_name identifier.
                Should be descriptive and unique within its registry context
                (e.g., "prod-factory", "dev-east-factory", "test-factory").
            config: Comprehensive configuration dictionary containing all parameters
                needed to create a CorePlusSessionFactory. Must include server connection
                details, authentication settings, and factory options. The exact structure
                depends on the CorePlusSessionFactory requirements and Enterprise deployment.

        Thread Safety:
            This constructor is thread-safe and can be called from any asyncio task.
            All initialization is synchronous and does not involve network operations
            or factory creation.

        See Also:
            CorePlusSessionFactory: The Enterprise factory type that will be created from config
            CorePlusSessionFactoryRegistry: Registry for managing multiple factory managers
            SystemType.ENTERPRISE: The system type constant used for Enterprise deployments
            BaseItemManager.__init__(): The parent constructor that handles common initialization
        """
        super().__init__(
            system_type=SystemType.ENTERPRISE,
            source="factory",
            name=name,
        )
        self._config = config

    @override
    async def _create_item(self) -> CorePlusSessionFactory:
        """Create and initialize a new Deephaven Enterprise session factory from stored configuration.

        This method implements the abstract _create_item() method from BaseItemManager
        to provide Enterprise-specific factory creation using the configuration dictionary
        approach. It is called automatically during lazy initialization when get() is
        first invoked on an uninitialized manager.

        Configuration-Driven Creation Process:
            The method performs comprehensive factory creation:
            1. **Configuration Validation**: Validates the stored config dictionary structure
            2. **Authentication Setup**: Configures Enterprise authentication from config
            3. **Connection Establishment**: Establishes connections to Enterprise servers
            4. **Factory Initialization**: Creates and initializes the CorePlusSessionFactory
            5. **Readiness Verification**: Confirms factory is ready to create sessions

        Factory Creation Complexity:
            Enterprise factory creation involves multiple steps:
            - **Server Connection**: Establishing secure connections to Enterprise infrastructure
            - **Authentication Protocol**: Implementing complex Enterprise auth (SAML, OAuth, etc.)
            - **Configuration Parsing**: Processing nested configuration parameters
            - **Resource Allocation**: Setting up connection pools and resource management
            - **Validation Checks**: Ensuring all factory components are properly configured

        Configuration Processing:
            The stored configuration dictionary is processed comprehensively:
            - **Server Settings**: URL, port, timeout, and connection parameters
            - **Authentication Config**: Credentials, tokens, certificates, and auth protocols
            - **Factory Options**: Connection pooling, caching, retry policies
            - **Enterprise Features**: Multi-tenancy, workspace, and security settings
            - **Performance Tuning**: Resource limits, optimization flags, and tuning parameters

        Performance Characteristics:
            Factory creation is an expensive operation involving:
            - **Network Operations**: Multiple round trips to Enterprise servers
            - **Authentication Overhead**: Complex Enterprise auth protocol handshakes
            - **Configuration Validation**: Comprehensive parameter validation and setup
            - **Resource Allocation**: Connection pool setup and resource initialization
            - **Typical Duration**: Can range from 1-10 seconds depending on complexity

        Error Scenarios and Handling:
            Various failure modes can occur during factory creation:
            - **Configuration Errors**: Invalid, missing, or malformed configuration parameters
            - **Authentication Failures**: Invalid credentials, expired tokens, or auth protocol errors
            - **Network Issues**: Connectivity problems to Enterprise servers
            - **Permission Errors**: Insufficient privileges for factory creation
            - **Resource Exhaustion**: Enterprise server unable to allocate factory resources
            - **Version Compatibility**: Enterprise server version incompatibility issues

        Enterprise Authentication Integration:
            Factory creation handles complex Enterprise authentication:
            - **SAML Integration**: Full SAML authentication protocol support
            - **OAuth Flows**: OAuth 2.0 and OpenID Connect integration
            - **Certificate-Based**: X.509 certificate authentication
            - **Custom Protocols**: Support for Enterprise-specific auth mechanisms
            - **Token Management**: Secure token storage and refresh capabilities

        Factory Capabilities After Creation:
            The created factory provides comprehensive session creation capabilities:
            - **Session Creation**: Ability to create multiple CorePlusSession instances
            - **Authentication Sharing**: Reuse authentication across created sessions
            - **Connection Pooling**: Efficient connection reuse for session creation
            - **Configuration Consistency**: All sessions created with consistent config
            - **Health Monitoring**: Factory-level health monitoring via ping() method

        Thread Safety:
            This method is fully thread-safe and can be called concurrently,
            though the BaseItemManager ensures only one creation attempt occurs
            per manager instance at a time.

        Returns:
            CorePlusSessionFactory: A fully initialized and configured Deephaven Enterprise
                session factory ready for creating CorePlusSession instances. The factory
                will have completed all authentication, connection setup, and configuration
                validation, and its ping() method should return success.

        Raises:
            Exception: Various exceptions can be raised during factory creation:
                - **ConfigurationError**: Invalid or incomplete configuration parameters
                - **AuthenticationError**: Authentication setup or credential validation failures
                - **ConnectionError**: Network connectivity issues to Enterprise servers
                - **PermissionError**: Insufficient privileges for factory creation
                - **ResourceError**: Enterprise server resource allocation failures
                - **CompatibilityError**: Version or protocol compatibility issues

        Implementation Notes:
            This method is marked with @override to indicate it implements the abstract
            method from BaseItemManager. It must not be called directly - use get()
            instead to ensure proper caching, error handling, and lifecycle management.

        See Also:
            BaseItemManager.get(): The public method that triggers lazy initialization
            CorePlusSessionFactory: The Enterprise factory type created by this method
            CorePlusSessionFactory.ping(): Health check method for created factories
            EnterpriseSessionManager._create_item(): Session-level creation counterpart
        """
        return await CorePlusSessionFactory.from_config(self._config)

    @override
    async def _check_liveness(
        self, item: CorePlusSessionFactory
    ) -> tuple[ResourceLivenessStatus, str | None]:
        """Verify Enterprise session factory health and responsiveness through lightweight ping operation.

        This method implements the abstract _check_liveness() method from BaseItemManager
        to provide Enterprise-specific factory health checking using the factory's ping()
        method. It performs a lightweight connectivity test without creating full sessions
        or consuming significant Enterprise server resources.

        Factory Health Assessment Strategy:
            The method uses CorePlusSessionFactory's ping() method for health verification:
            - **Lightweight Check**: Minimal overhead ping operation vs. full session creation
            - **Network Verification**: Confirms connectivity to Enterprise infrastructure
            - **Authentication Status**: Validates that factory authentication remains valid
            - **Server Responsiveness**: Ensures Enterprise servers are responding properly
            - **Resource Availability**: Confirms factory can still access server resources

        Enterprise Factory Ping Operation:
            The ping() method performs comprehensive health checking:
            - **Connection Status**: Verifies network connections to Enterprise servers
            - **Authentication Health**: Confirms authentication tokens/credentials are valid
            - **Server Response**: Ensures Enterprise servers respond to health requests
            - **Resource Access**: Validates factory can access required Enterprise resources
            - **Performance Check**: Measures response time for basic operations

        Health Check Performance Characteristics:
            Factory liveness checking is designed for efficiency:
            - **Fast Operation**: Typically completes in 100-500ms
            - **Minimal Resources**: Uses minimal network bandwidth and server resources
            - **Non-Intrusive**: Does not affect ongoing factory operations or sessions
            - **Concurrent Safe**: Can be called while factory is creating sessions
            - **Reliable Indicator**: Accurately reflects factory operational status

        Liveness Status Interpretation:
            The method returns detailed health status information:
            - **ONLINE**: Factory ping() returned True, indicating full operational health
            - **OFFLINE**: Factory ping() returned False, indicating connectivity/health issues
            - **Detail Messages**: When offline, provides "Ping returned False" explanation

        Common Offline Scenarios:
            Various conditions can cause a factory to report as offline:
            - **Network Issues**: Connectivity problems to Enterprise servers
            - **Authentication Expiry**: Expired tokens, certificates, or credentials
            - **Server Maintenance**: Enterprise servers undergoing maintenance or restart
            - **Resource Exhaustion**: Enterprise server resource limits reached
            - **Configuration Changes**: Server-side configuration changes affecting factory
            - **Version Incompatibility**: Enterprise server version changes breaking compatibility

        Error Handling Architecture:
            Exception handling follows the established pattern:
            - **Exception Transparency**: This method does not catch exceptions
            - **Caller Responsibility**: The liveness_status() method handles all exceptions
            - **Centralized Handling**: All resource managers use consistent exception handling
            - **Detailed Logging**: Exceptions are logged with full context by liveness_status()
            - **Clean Error Propagation**: Exceptions bubble up with proper context

        Integration with Resource Management:
            This method integrates with the broader resource management system:
            - **Lifecycle Management**: Used by close() to verify factory state before cleanup
            - **Health Monitoring**: Called by monitoring systems to assess factory health
            - **Registry Operations**: Used by registries for factory health assessment
            - **Debugging Support**: Provides detailed health information for troubleshooting
            - **Automatic Recovery**: Health status used for automatic factory recreation

        Factory vs. Session Health Checking:
            **Factory-Level Health** (this method):
            - Tests factory's ability to create sessions
            - Verifies infrastructure connectivity
            - Checks authentication validity
            - Minimal resource consumption

            **Session-Level Health** (EnterpriseSessionManager):
            - Tests individual session responsiveness
            - Verifies session-specific operations
            - Checks query execution capability
            - Higher resource consumption

        Usage in Factory Lifecycle:
            **Regular Health Monitoring**:
            ```python
            manager = CorePlusSessionFactoryManager("prod", config)
            factory = await manager.get()

            # Regular health checking
            status, detail = await manager.liveness_status()
            if status != ResourceLivenessStatus.ONLINE:
                logger.warning(f"Factory {manager.full_name} health issue: {detail}")
            ```

            **Pre-Session Creation Verification**:
            ```python
            # Verify factory health before creating sessions
            if await manager.is_alive():
                factory = await manager.get()
                session = await factory.create_session("app", "worker")
            else:
                logger.error("Factory not responsive, cannot create session")
            ```

            **Cleanup Verification**:
            ```python
            # Verify factory state during cleanup
            if await manager.is_alive():
                logger.info(f"Factory {manager.full_name} responsive during cleanup")
            await manager.close()  # Safe cleanup
            ```

        Thread Safety and Concurrency:
            This method is fully thread-safe and supports concurrent operations:
            - **Concurrent Pings**: Multiple ping operations can run simultaneously
            - **Non-Blocking**: Does not block other factory operations
            - **Session Creation Safe**: Can run while factory creates sessions
            - **Registry Safe**: Safe to call from registry health monitoring

        Args:
            item: The CorePlusSessionFactory instance to check for liveness and health.
                Must be a valid factory instance previously created by _create_item().
                The factory's ping() method will be called to assess health status.

        Returns:
            tuple[ResourceLivenessStatus, str | None]: A tuple containing:
                - **ResourceLivenessStatus**: ONLINE if factory ping() returns True,
                  OFFLINE if ping() returns False
                - **str | None**: Detail message providing additional context:
                  - None when status is ONLINE (no additional detail needed)
                  - "Ping returned False" when status is OFFLINE

        Exception Handling:
            This method does not catch exceptions - they are handled by liveness_status():
            - **Ping Exceptions**: Network, authentication, or server errors during ping
            - **Protocol Errors**: Enterprise protocol or communication errors
            - **Resource Errors**: Server resource exhaustion or allocation failures

        Implementation Notes:
            This method is marked with @override to indicate it implements the abstract
            method from BaseItemManager. It follows the established pattern of exception
            transparency, allowing liveness_status() to provide centralized exception handling.

        See Also:
            BaseItemManager._check_liveness(): The abstract method this implements
            CorePlusSessionFactory.ping(): The factory health check method used
            BaseItemManager.liveness_status(): The public method that handles exceptions
            EnterpriseSessionManager._check_liveness(): Session-level health checking counterpart
            ResourceLivenessStatus: The enum values returned by this method
        """
        alive = await item.ping()

        if alive:
            return (ResourceLivenessStatus.ONLINE, None)
        else:
            return (ResourceLivenessStatus.OFFLINE, "Ping returned False")
