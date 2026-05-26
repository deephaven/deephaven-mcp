"""Async resource managers for Deephaven MCP session and factory lifecycle management.

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
    CommunitySessionManager: Base class managing CoreSession instances for Community
        deployments; in practice one of the two concrete subclasses below is used.
    StaticCommunitySessionManager: Manages CoreSession instances connecting to
        pre-configured Community servers declared in per-session files under
        ``$DH_MCP_CONFIG_DIR/community/sessions/``.
    DynamicCommunitySessionManager: Manages CoreSession instances for on-demand
        servers launched by the MCP server itself (Docker / Python).
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
    # Create manager (use StaticCommunitySessionManager for configured servers)
    manager = StaticCommunitySessionManager("worker1", config)

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
    CommunitySessionManager: Concrete manager for Community sessions (typically used
        via its Static/Dynamic subclasses rather than instantiated directly)
    StaticCommunitySessionManager: Manager for configured (pre-existing) Community servers
    DynamicCommunitySessionManager: Manager for on-demand launched Community servers
    EnterpriseSessionManager: Concrete manager for Enterprise sessions
    CorePlusSessionFactoryManager: Concrete manager for Enterprise session factories

Thread Safety:
    All managers are fully coroutine-safe and designed for concurrent access in
    async applications. Internal locking ensures race-condition-free operations.
"""

import asyncio
import enum
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, ClassVar, Protocol, override

from deephaven_mcp._exceptions import (
    AuthenticationError,
    ConfigurationError,
    DeephavenConnectionError,
    InvalidSessionNameError,
    SessionCreationError,
)
from deephaven_mcp._taxonomy import SessionOrigin, SystemType
from deephaven_mcp.auth.credentials import Credentials
from deephaven_mcp.client import (
    CommunityClientTimeouts,
    CorePlusSession,
    CorePlusSessionFactory,
    CoreSession,
    EnterpriseClientTimeouts,
)
from deephaven_mcp.sessions import CommunitySessionConfig, EnterpriseSystemConfig

from ._launcher import (
    DockerLaunchedSession,
    PythonLaunchedSession,
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


class BaseItemManager[T: AsyncClosable](ABC):
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
                return await MyResource.create(self._session_config)

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

    evicts_on_idle: ClassVar[bool] = False
    """Whether the registry should also remove this manager from ``_items`` on idle eviction.

    Default ``False`` — for managers that hold only a cached client (no
    external process), the idle sweeper just calls :meth:`close` to drop
    the cached item; the manager stays in the registry so a subsequent
    :meth:`get` lazily reconnects.

    Subclasses that own an external resource which cannot be transparently
    reopened (e.g. a launched Docker container or Python subprocess) should
    override this to ``True``.  The sweeper will then remove the entry
    from the registry (and from any added-session tracking) after closing
    it, since the underlying resource is gone and a later :meth:`get`
    would fail.
    """

    @staticmethod
    def make_full_name(system_type: "SystemType", system: str, name: str) -> str:
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
            system_type (SystemType): The Deephaven deployment type (COMMUNITY or ENTERPRISE).
                Determines which client libraries and authentication mechanisms are used.
            source (str): The configuration source identifier that groups related resources.
                Examples: "config.yaml", "https://api.example.com/config", "env-vars"
            name (str): The unique name of the specific resource within its source context.
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
        return f"{system_type.value}:{system}:{name}"

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
            InvalidSessionNameError: If full_name is not in the expected format

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
            raise InvalidSessionNameError(
                f"Invalid full_name format: '{full_name}'. "
                f"Expected format: 'system_type:source:name'"
            )
        return parts[0], parts[1], parts[2]

    def __init__(
        self,
        system_type: SystemType,
        system: str,
        name: str,
    ):
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
            system: The system identifier this manager belongs to. ``"community"``
                for the community umbrella; an enterprise ``system_name`` for
                enterprise managers. Matches the ``name`` field returned by the
                ``list_systems`` MCP tool.
            name: The unique name of this manager within its ``system_type`` +
                ``system`` namespace. Used for identification, logging, and
                resource tracking.

        Classification metadata that is not common to every manager kind
        lives on the relevant subclass instead of this base:

        - ``origin`` (``SessionOrigin.STATIC`` vs ``SessionOrigin.DYNAMIC``) is a
          community-session-only concept and lives on
          :class:`CommunitySessionManager`.
        - Session-vs-factory disambiguation is expressed by the class
          hierarchy itself (see :class:`CorePlusSessionFactoryManager`);
          callers that need to filter factories use ``isinstance``.

        State invariants:
            All three mutable state slots below are read and written only
            under ``self._lock``.

            - ``_item_cache`` (``T | None``): the lazily created resource,
              or ``None`` when not yet created / after :meth:`close` /
              after idle eviction. Set to non-``None`` only by
              :meth:`_get_unlocked` (cache miss path).
            - ``_last_accessed`` (``float | None``): monotonic timestamp
              of the most recent :meth:`get`. ``None`` means "never
              accessed since construction or last close / idle-eviction"
              and is therefore not eligible for idle eviction. Whenever
              ``_item_cache`` is non-``None``, ``_last_accessed`` is also
              non-``None`` (set together in :meth:`_get_unlocked`).
            - ``_lock`` (``asyncio.Lock``): serializes reads and writes
              of the two slots above.

            After :meth:`close` or idle eviction, the next :meth:`get`
            repopulates the cache via :meth:`_create_item`.

        Post-Initialization State:
            After construction, the manager has:
            - Empty resource cache (_item_cache = None)
            - No last-accessed timestamp (_last_accessed = None)
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
        self._system = system
        self._name = name
        self._item_cache: T | None = None
        self._last_accessed: float | None = None
        self._lock = asyncio.Lock()

        full_name = self.make_full_name(system_type, system, name)
        _LOGGER.info(
            f"[{self.__class__.__name__}] Initialized manager for '{full_name}'"
        )

    async def maybe_close_if_idle(self, timeout_seconds: float, now: float) -> bool:
        """Close the cached item if it has been idle past ``timeout_seconds``.

        Called by :class:`~deephaven_mcp.resource_manager.Evictor`.  A
        manager whose :attr:`_last_accessed` is ``None`` (never accessed
        since construction or last close) is not eligible for eviction.

        Two-phase pattern:

        1. Under ``self._lock``: re-check idleness.  Return ``False``
           early when the manager is within the timeout, has never been
           accessed, or has no cached item.
        2. Outside the lock: ``await self.close()`` — polymorphic close,
           which clears the cache (and, for subclasses that own external
           resources, releases them; e.g.
           :class:`DynamicCommunitySessionManager.close` stops the
           launched process).

        Intentional non-atomicity: the manager's lock is released
        between the idleness re-check and the ``await self.close()``
        call.  A concurrent :meth:`get` racing in during that window
        may observe the still-cached item and hand it to a caller
        immediately before close runs.  The caller's next use will then
        surface a clear closed-resource error from the underlying
        client.

        Args:
            timeout_seconds (float): Idle threshold.  An item is eligible
                for closure if ``now - self._last_accessed > timeout_seconds``.
            now (float): A ``time.monotonic()`` reading taken by the
                sweeper, used to keep the eligibility check consistent
                across all managers in a sweep pass.

        Returns:
            bool: ``True`` if :meth:`close` was issued for this manager.
                The caller (the Evictor) reads
                :attr:`evicts_on_idle` to decide whether to remove the
                manager from the registry.
                ``False`` if the manager was within the timeout, had
                never been accessed, or had no cached item to close.
        """
        async with self._lock:
            last = self._last_accessed
            if last is None or (now - last) <= timeout_seconds:
                return False
            if self._item_cache is None:
                # Nothing cached; just reset the timer so we don't
                # repeatedly log "eviction" for the same idle slot.
                self._last_accessed = None
                return False
            idle_for = now - last

        _LOGGER.info(
            f"[{self.__class__.__name__}] Closing idle item for "
            f"'{self.full_name}' (idle for {idle_for:.1f}s)"
        )
        await self.close()
        return True

    async def _close_captured_item(self, item: T) -> None:
        """Close ``item`` and swallow exceptions, logging at WARNING.

        Caller must **not** hold ``self._lock`` — this method runs the
        ``await item.close()`` outside the manager's critical section so
        that network/IPC I/O during cleanup does not block other
        operations on this manager.  Used by :meth:`close` after it has
        captured the cached item ref under the lock and reset the cache
        state.

        Never raises.

        Args:
            item: The previously-cached item to close.  Must be a stable
                local reference captured under ``self._lock`` before
                ``self._item_cache`` was reset to ``None``.
        """
        try:
            await item.close()
        except Exception as e:
            _LOGGER.warning(
                f"[{self.__class__.__name__}] Error closing item for "
                f"'{self.full_name}': {e}"
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
                    session = await CoreSession.from_session_config(
                        self._session_config
                    )
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
    def system(self) -> str:
        """The system identifier this manager belongs to.

        Matches the ``name`` field returned by the ``list_systems`` MCP tool:

        - ``"community"`` for any community manager (the umbrella system).
        - The enterprise ``system_name`` for any enterprise manager (sessions
          and factories alike).

        Returns:
            str: The system identifier string.
        """
        return self._system

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
            - "community:community:worker-1"
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
            print(manager.full_name)  # "community:community:worker-1"
            ```

        See Also:
            make_full_name(): The static method that implements the identifier format
        """
        return self.make_full_name(self.system_type, self.system, self.name)

    @property
    def split_name(self) -> tuple[str, str, str]:
        """Split this manager's full name into its constituent components.

        This property provides convenient access to the individual components
        that make up this manager's full_name identifier.

        Returns:
            tuple[str, str, str]: (system_type, system, name) components

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
        return self.system_type.value, self.system, self.name

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
            This method is NOT thread-safe by itself. The caller MUST hold ``self._lock``
            before calling this method. Both ``self._item_cache`` and
            ``self._last_accessed`` are read and written here; future refactors must
            keep the timestamp update inside the same critical section as the cache
            mutation, since :meth:`maybe_close_if_idle` reads
            ``self._last_accessed`` under the same lock.

        See Also:
            get(): The public, thread-safe method that acquires the lock and calls this
            liveness_status(): Another caller that uses this for resource access
        """
        if self._item_cache:
            _LOGGER.debug(
                f"[{self.__class__.__name__}] Cache hit for '{self.full_name}'"
            )
            self._last_accessed = time.monotonic()
            return self._item_cache

        _LOGGER.info(
            f"[{self.__class__.__name__}] Cache miss - creating new item for '{self.full_name}'..."
        )
        self._item_cache = await self._create_item()
        self._last_accessed = time.monotonic()
        _LOGGER.info(
            f"[{self.__class__.__name__}] Successfully created and cached new item for '{self.full_name}'"
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
            f"[{self.__class__.__name__}] Getting managed item for '{self.full_name}'"
        )
        async with self._lock:
            result = await self._get_unlocked()
            _LOGGER.debug(
                f"[{self.__class__.__name__}] Successfully retrieved managed item for '{self.full_name}'"
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
                f"[{self.__class__.__name__}] Liveness check failed for {self.full_name}: {e}"
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
            f"[{self.__class__.__name__}] Checking liveness status ({mode} mode) for '{self.full_name}'"
        )

        async with self._lock:
            status, detail = await self._liveness_status_unlocked(ensure_item)
            detail_suffix = f" ({detail})" if detail else ""
            _LOGGER.info(
                f"[{self.__class__.__name__}] Liveness check ({mode} mode) for '{self.full_name}': {status.value}{detail_suffix}"
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
        """Close the cached resource and reset the manager for reuse.

        Two-phase pattern (matches :meth:`maybe_close_if_idle`):

        1. Under ``self._lock``: capture the cached item ref and clear
           ``_item_cache`` / ``_last_accessed`` atomically.  Any concurrent
           :meth:`get` after this point sees a fresh slot and lazily
           re-creates.
        2. Outside the lock: hand the captured item to
           :meth:`_close_captured_item`, which performs the actual close
           with full error suppression.

        Subclasses that own external resources (e.g. a launched process)
        extend ``close()`` with their own teardown — that logic runs in
        the subclass after this base call returns.

        Idempotent: safe to call multiple times.  Never raises.
        """
        _LOGGER.debug(
            f"[{self.__class__.__name__}] Starting close operation for '{self.full_name}'"
        )

        async with self._lock:
            item_to_close = self._item_cache
            self._item_cache = None
            self._last_accessed = None

        if item_to_close is None:
            _LOGGER.debug(
                f"[{self.__class__.__name__}] No cached item to close for '{self.full_name}'"
            )
            return

        _LOGGER.info(f"[{self.__class__.__name__}] Closing item for '{self.full_name}'")
        await self._close_captured_item(item_to_close)
        _LOGGER.debug(
            f"[{self.__class__.__name__}] Close operation complete for '{self.full_name}'"
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
        The manager takes a fully-validated
        :class:`~deephaven_mcp.sessions.CommunitySessionConfig`
        declaration. Its required and optional fields are documented
        on that class; the manager itself simply forwards the
        declaration to :meth:`CoreSession.from_session_config`.

        Example construction:
        ```python
        from deephaven_mcp.sessions import CommunitySessionConfig

        session_config = CommunitySessionConfig.model_validate(
            {
                "name": "worker-1",
                "host": "localhost",
                "port": 10000,
                "auth": {"credentials": {"type": "anonymous"}},
            }
        )
        manager = StaticCommunitySessionManager("worker-1", session_config)
        ```

    Integration Patterns:
        **Registry Integration**:
        Typically used within CommunitySessionRegistry for managing multiple sessions:
        ```python
        registry = CommunitySessionRegistry({"worker-1": session_config})
        await registry.initialize()
        ```

        **Standalone Usage**:
        Can be used independently for single-session applications:
        ```python
        manager = StaticCommunitySessionManager("main-session", session_config)
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

    def __init__(
        self,
        name: str,
        session_config: CommunitySessionConfig,
        origin: SessionOrigin,
        timeouts: CommunityClientTimeouts,
    ):
        """Initialize a new Community session manager with configuration.

        Creates a new manager instance for handling a Deephaven Community session
        with the specified name and configuration parameters. The manager is initialized
        in an uninitialized state - no actual session is created until the first
        get() call is made (lazy initialization).

        Manager Identity:
            The manager is configured with:
            - **system_type**: Set to SystemType.COMMUNITY for Community deployments
            - **system**: Always ``"community"`` (the umbrella system name); the static-vs-dynamic distinction lives in :attr:`origin`, not in the id
            - **name**: The unique identifier for this specific manager instance
            - **full_name**: Computed as ``"community:community:{name}"`` for global uniqueness

        Configuration Storage:
            The provided session declaration is stored internally and used
            later during lazy session creation. The declaration is already
            validated by :class:`CommunitySessionConfig.model_validate`;
            the manager performs no additional validation at construction
            time.

        Configuration Requirements:
            The ``session_config`` argument is a validated
            :class:`~deephaven_mcp.sessions.CommunitySessionConfig` that
            carries the connection target (``host`` / ``port``), the
            optional TLS block, and the pre-resolved
            :class:`~deephaven_mcp.auth.credentials.Credentials`. All
            fields are validated up front; no additional checks happen
            here.

        Usage Examples:
            ```python
            from deephaven_mcp.sessions import CommunitySessionConfig

            # Anonymous community session
            anon = CommunitySessionConfig.model_validate(
                {
                    "name": "worker-1",
                    "host": "localhost",
                    "port": 10000,
                    "programming_language": "Python",
                    "auth": {"credentials": {"type": "anonymous"}},
                }
            )
            manager = StaticCommunitySessionManager("worker-1", anon)

            # PSK-authenticated community session
            psk = CommunitySessionConfig.model_validate(
                {
                    "name": "secure-session",
                    "host": "deephaven.example.com",
                    "port": 10000,
                    "programming_language": "Python",
                    "tls": {},
                    "auth": {
                        "credentials": {"type": "psk", "token": "..."},
                    },
                }
            )
            manager = StaticCommunitySessionManager("secure-session", psk)
            ```

        Manager State After Construction:
            - **Ready for use**: Manager is fully initialized and ready for get() calls
            - **No session created**: Actual CoreSession creation is deferred until needed
            - **Declaration stored**: Typed config is cached for later session creation
            - **Thread-safe**: Manager can be safely used from multiple asyncio tasks

        Args:
            name: Unique identifier for this manager instance within its registry.
                Used for logging, debugging, and creating the full_name identifier.
                Should be a descriptive name like "worker-1", "main-session", etc.
            session_config: Validated
                :class:`~deephaven_mcp.sessions.CommunitySessionConfig`
                describing how to connect to the session.
            origin: Where this session came from. ``SessionOrigin.STATIC`` for
                sessions declared in ``community/sessions/*.json``;
                ``SessionOrigin.DYNAMIC`` for sessions created at runtime by
                MCP tools.

        Thread Safety:
            This constructor is thread-safe and can be called from any asyncio task.
            All initialization is synchronous and does not involve network operations.

        See Also:
            CoreSession.from_session_config(): The method used to create sessions
                from a typed :class:`CommunitySessionConfig`.
            SystemType.COMMUNITY: The system type constant used for Community deployments
            BaseItemManager.__init__(): The parent constructor that handles common initialization
        """
        super().__init__(
            system_type=SystemType.COMMUNITY,
            system=SystemType.COMMUNITY.value,
            name=name,
        )
        self._origin = origin
        self._session_config = session_config
        self._timeouts = timeouts

    @property
    def origin(self) -> SessionOrigin:
        """How this community session was created.

        ``SessionOrigin.STATIC`` for community sessions declared in
        ``community/sessions/*.json``; ``SessionOrigin.DYNAMIC`` for community
        sessions created at runtime via ``session_community_create``.
        """
        return self._origin

    @override
    async def _create_item(self) -> CoreSession:
        """Create and initialize a new Deephaven Community session from configuration.

        This method implements the abstract _create_item() method from BaseItemManager
        to provide Community-specific session creation. It is called automatically
        during lazy initialization when get() is first invoked on an uninitialized
        manager.

        Session Creation Process:
            The method performs these steps:
            1. **Delegate to CoreSession**: Uses CoreSession.from_session_config() for actual creation
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
            CoreSession.from_session_config(): The underlying method used for session creation
            BaseItemManager.get(): The public method that triggers lazy initialization
            SessionCreationError: The exception type raised on creation failures
        """
        try:
            _LOGGER.info(
                f"[{self.__class__.__name__}] Creating community session for {self.full_name}"
            )
            return await CoreSession.from_session_config(
                self._session_config, self._timeouts
            )
        except Exception as e:
            _LOGGER.error(
                f"[{self.__class__.__name__}] Failed to create community session for {self.full_name}: {e}"
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


class StaticCommunitySessionManager(CommunitySessionManager):
    """Manages a statically configured Deephaven Community session.

    This class extends CommunitySessionManager for sessions defined in configuration files.
    These sessions connect to pre-existing Deephaven servers that are managed externally
    (e.g., servers started manually or by other processes).

    Key Characteristics:
        - **Origin**: Automatically set to ``SessionOrigin.STATIC`` to distinguish from dynamic sessions
        - **Server Lifecycle**: Does NOT manage server startup/shutdown (server must exist)
        - **Configuration**: Loaded from per-session files under ``$DH_MCP_CONFIG_DIR/community/sessions/``
        - **Full Name Format**: ``"community:community:{name}"`` (the middle segment is the umbrella system name; the static/dynamic distinction lives in :attr:`origin`, not in the id)

    Usage:
        Typically created by CommunitySessionRegistry when loading configuration:
        ```python
        manager = StaticCommunitySessionManager("local-dev", {
            "server": "http://localhost:10000",
            "auth_type": "anonymous"
        })
        session = await manager.get()
        ```

    See Also:
        DynamicCommunitySessionManager: For runtime-created sessions with lifecycle management
        CommunitySessionRegistry: Registry that creates these managers from configuration
    """

    @override
    def __init__(
        self,
        name: str,
        session_config: CommunitySessionConfig,
        timeouts: CommunityClientTimeouts,
    ):
        """Initialize a StaticCommunitySessionManager for a configuration-based session.

        Args:
            name (str): Unique identifier for this manager instance within the registry.
                Used to construct full_name as ``"community:community:{name}"``.
                The static-vs-dynamic distinction lives in :attr:`origin`, not in the id.
            session_config (CommunitySessionConfig): Validated session
                declaration (host, port, credentials, etc.).
            timeouts (CommunityClientTimeouts): Community client-layer timeout
                configuration forwarded to
                :meth:`CoreSession.from_session_config` when the
                underlying session is lazily created.

        Note:
            The origin parameter is automatically set to ``SessionOrigin.STATIC``
            - callers do not need to specify it. This distinguishes static
            sessions from dynamic ones.
        """
        # Static community session: declared in community/sessions/*.json.
        super().__init__(name, session_config, SessionOrigin.STATIC, timeouts)


class DynamicCommunitySessionManager(CommunitySessionManager):
    """Manages a dynamically created Deephaven Community session.

    This class extends CommunitySessionManager to add full lifecycle management for
    sessions that are launched on-demand via Docker containers or Python-based servers.
    Unlike static sessions, this manager controls server startup, monitoring, and shutdown.

    Key Characteristics:
        - **Origin**: Automatically set to ``SessionOrigin.DYNAMIC`` to distinguish from static sessions
        - **Server Lifecycle**: DOES manage server startup/shutdown (via LaunchedSession)
        - **Launch Methods**: Supports Docker containers or Python-based deephaven-server
        - **Full Name Format**: ``"community:community:{name}"`` (the middle segment is the umbrella system name; the static/dynamic distinction lives in :attr:`origin`, not in the id)
        - **Created By**: MCP tools like session_community_create

    Additional Properties:
        This class provides convenient properties that delegate to the launched_session:
        - connection_url: Base HTTP URL for the session
        - connection_url_with_auth: URL with authentication token included
        - port: Port number the session is listening on
        - container_id: Docker container ID (for Docker launches)
        - process_id: Process ID (for python launches)

    Lifecycle Management:
        The launched_session handles:
        - Starting the Docker container or python process
        - Waiting for the server to be ready
        - Stopping the container/process on close()
        - Health monitoring via wait_until_ready()

    Usage:
        Typically created by MCP tools during session_community_create:
        ```python
        launched = await launch_session(
            launch_method="docker",
            session_name="my-session",
            port=10000,
            auth_token="secret",
            heap_size_gb=4,
            extra_jvm_args=[],
            environment_vars={},
            docker_image="ghcr.io/deephaven/server:latest",
        )
        manager = DynamicCommunitySessionManager(
            name="my-session",
            config={"server": "http://localhost:10000", "auth_token": "secret"},
            launched_session=launched
        )
        ```

    Attributes:
        launched_session (DockerLaunchedSession | PythonLaunchedSession): The launched session
            that manages server lifecycle.

    See Also:
        StaticCommunitySessionManager: For pre-existing servers from configuration
        LaunchedSession: Base class for Docker/python session launchers
        launch_session: Factory function that creates launched sessions
    """

    evicts_on_idle: ClassVar[bool] = True
    """Dynamic sessions are removed from the registry on idle eviction.

    The launched Docker container / Python subprocess is stopped by this
    class's :meth:`close` override (called from the manager's
    :meth:`maybe_close_if_idle` during a sweep), so a subsequent
    :meth:`get` could not transparently reconnect; keeping the manager
    in the registry would leave a stale entry that errors on access and
    consumes a ``max_concurrent_sessions`` slot. The Evictor therefore
    drops the entry entirely after closing.
    """

    @override
    def __init__(
        self,
        name: str,
        session_config: CommunitySessionConfig,
        launched_session: DockerLaunchedSession | PythonLaunchedSession,
        timeouts: CommunityClientTimeouts,
    ):
        """Initialize a DynamicCommunitySessionManager for a runtime-created session.

        Args:
            name (str): Unique identifier for this manager instance within the registry.
                Used to construct full_name as ``"community:community:{name}"``.
                The static-vs-dynamic distinction lives in :attr:`origin`, not in the id.
            session_config (CommunitySessionConfig): Validated session
                declaration whose ``host`` / ``port`` / ``credentials``
                match the launched session.
            launched_session (DockerLaunchedSession | PythonLaunchedSession): The launched
                session that provides server lifecycle management.

        Note:
            The origin parameter is automatically set to ``SessionOrigin.DYNAMIC``
            - callers do not need to specify it. This distinguishes dynamic
            sessions from static ones and enables proper cleanup and
            validation in deletion operations.
        """
        # Dynamic community session: created at runtime via
        # session_community_create.
        super().__init__(name, session_config, SessionOrigin.DYNAMIC, timeouts)
        self.launched_session = launched_session
        self._is_stopped: bool = False

        _LOGGER.debug(
            f"[DynamicCommunitySessionManager] Created manager for '{name}' "
            f"(port: {launched_session.port}, method: {launched_session.launch_method})"
        )

    @override
    async def _create_item(self) -> CoreSession:
        """Create a new CoreSession, refusing once the manager has been stopped.

        Called under ``self._lock`` from
        :meth:`BaseItemManager._get_unlocked` on a cache miss.  Reading
        :attr:`_is_stopped` here is lock-protected because
        :meth:`close` sets the flag under the same lock that captures
        and clears the cache.

        Returns:
            CoreSession: A new session connected to the launched process.

        Raises:
            SessionCreationError: When :meth:`close` has stopped the
                launched process.  Recreating would connect to a port
                that is no longer listening.
            SessionCreationError: When the underlying ``CoreSession``
                creation fails for any other reason (re-raised by
                :meth:`CommunitySessionManager._create_item`).
        """
        if self._is_stopped:
            raise SessionCreationError(
                f"Cannot create session for '{self.full_name}': "
                f"the dynamic session has been stopped."
            )
        return await super()._create_item()

    @override
    async def _get_unlocked(self) -> CoreSession:
        """Return the cached session, refusing once the manager has been stopped.

        Overrides :meth:`BaseItemManager._get_unlocked` to consult
        :attr:`_is_stopped` **before** returning a cached item.  Without
        this gate, a caller racing :meth:`close` could acquire
        ``self._lock`` between the close's cache-clear and the underlying
        process teardown and receive a session reference that is about
        to become unusable.

        Must be called while holding ``self._lock``; see
        :meth:`BaseItemManager._get_unlocked` for the contract.

        Returns:
            CoreSession: A live cached session, or a freshly created
                session on cache miss.

        Raises:
            SessionCreationError: When :meth:`close` has stopped the
                manager.  Raised regardless of whether the cache slot
                is populated.
        """
        if self._is_stopped:
            raise SessionCreationError(
                f"Cannot return session for '{self.full_name}': "
                f"the dynamic session has been stopped."
            )
        return await super()._get_unlocked()

    @property
    def connection_url(self) -> str:
        """Get the base connection URL for this session.

        Returns:
            str: The base URL without authentication token (e.g., "http://localhost:10000").
        """
        return self.launched_session.connection_url

    @property
    def connection_url_with_auth(self) -> str:
        """Get the connection URL with authentication token (if applicable).

        Returns:
            str: The complete URL with auth token parameter if PSK auth is used,
                otherwise the base URL.
        """
        return self.launched_session.connection_url_with_auth

    @property
    def port(self) -> int:
        """Get the port the session is listening on.

        Returns:
            int: The TCP port number where the Deephaven server is accessible.
        """
        return self.launched_session.port

    @property
    def launch_method(self) -> str:
        """Get the launch method used (docker or python).

        Returns:
            str: Either "docker" or "python" indicating how the session was launched.
        """
        return self.launched_session.launch_method

    @property
    def container_id(self) -> str | None:
        """Get the Docker container ID (if launched via Docker).

        Returns:
            str | None: The Docker container ID if launch_method is "docker", otherwise None.
        """
        if isinstance(self.launched_session, DockerLaunchedSession):
            return self.launched_session.container_id
        return None

    @property
    def process_id(self) -> int | None:
        """Get the process ID (if launched via python).

        Returns:
            int | None: The system process ID if launch_method is "python", otherwise None.
        """
        if isinstance(self.launched_session, PythonLaunchedSession):
            return self.launched_session.process.pid
        return None

    @override
    async def close(self) -> None:
        """Close the session and stop the underlying process/container.

        This method:
        1. Under ``self._lock``: capture the cached ``CoreSession`` ref,
           clear ``_item_cache`` / ``_last_accessed``, and set
           ``_is_stopped=True`` in one critical section.  Combining all
           three mutations under a single lock acquisition prevents a
           concurrent :meth:`get` from observing a cleared cache yet
           still passing the ``_is_stopped`` check, or from returning
           a still-cached session that is about to be torn down.
        2. Outside the lock: close the captured CoreSession (if any).
        3. Outside the lock: stop the launched process/container.

        Errors during cleanup are logged but don't prevent the cleanup from completing.
        """
        _LOGGER.info(
            f"[DynamicCommunitySessionManager] Closing dynamic session '{self.full_name}'"
        )

        # Single critical section: capture, clear, flip together.  Any
        # concurrent get() that wins self._lock after this block sees
        # _is_stopped=True (raises) or, if it won the lock before this
        # block, returns a cached session that is closed promptly below.
        async with self._lock:
            session_to_close = self._item_cache
            self._item_cache = None
            self._last_accessed = None
            self._is_stopped = True

        # Close the captured session outside the lock.
        # ``_close_captured_item`` swallows and logs its own exceptions
        # (see its docstring "Never raises."), so no outer try/except.
        if session_to_close is not None:
            await self._close_captured_item(session_to_close)

        # Then, stop the launched session
        try:
            _LOGGER.debug(
                f"[DynamicCommunitySessionManager] Stopping {self.launch_method} "
                f"session '{self.full_name}'"
            )
            await self.launched_session.stop()
            _LOGGER.info(
                f"[DynamicCommunitySessionManager] Successfully stopped {self.launch_method} "
                f"session '{self.full_name}'"
            )
        except Exception as e:
            _LOGGER.error(
                f"[DynamicCommunitySessionManager] Error stopping {self.launch_method} "
                f"session '{self.full_name}': {e}",
                exc_info=True,
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert session information to a dictionary for API responses.

        Returns:
            dict[str, Any]: Session information including connection details.

        Note:
            Does NOT include connection_url_with_auth or auth_token for security.
            Use session_community_credentials MCP tool if credentials are needed.
        """
        base_dict = {
            "session_id": self.full_name,
            "session_type": "COMMUNITY",
            "source": "dynamic",
            "name": self._name,
            "connection_url": self.connection_url,
            "auth_type": self.launched_session.auth_type.upper(),  # "PSK" or "ANONYMOUS"
            "launch_method": self.launch_method,
            "port": self.port,
        }

        # Add launch-method-specific details
        if self.launch_method == "docker":
            base_dict["container_id"] = self.container_id
        elif self.launch_method == "python":
            base_dict["process_id"] = self.process_id

        return base_dict


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
        system: str,
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
            - **full_name**: Computed as "{system_type}:{source}:{name}" for global uniqueness

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
        super().__init__(
            system_type=SystemType.ENTERPRISE,
            system=system,
            name=name,
        )
        self._creation_function = creation_function

    @override
    async def _create_item(self) -> CorePlusSession:
        """Create a Deephaven Core+ session using the injected creation function.

        This method implements the abstract _create_item() from BaseItemManager to provide
        Core+ (Enterprise) specific session creation using the injectable creation function
        pattern. Called automatically during lazy initialization when get() is first invoked.

        Implementation:
            1. Logs creation attempt (INFO level)
            2. Calls self._creation_function(self._source, self._name)
            3. Returns the CorePlusSession instance created by the function
            4. Catches any exceptions and wraps them in SessionCreationError with logging

        The creation function is responsible for all session creation logic including
        authentication, configuration retrieval, and connection establishment.

        Args:
            None (uses self._creation_function, self._source, and self._name from __init__)

        Returns:
            CorePlusSession: Initialized Core+ session ready for use. The session's
                is_alive() method should return True.

        Raises:
            SessionCreationError: If the creation function fails for any reason.
                Original exception is preserved via __cause__ attribute and logged.

        Notes:
            - This method is marked @override to implement BaseItemManager abstract method
            - Do not call directly - use get() for proper caching and error handling
            - Creation function is called with exact (source, name) parameters from __init__
            - All exceptions from creation function are wrapped in SessionCreationError

        See Also:
            BaseItemManager.get(): Public method triggering lazy initialization
            CorePlusSession: The Core+ session type returned by creation functions
            CorePlusSessionFactory: Common factory usable with this manager
        """
        try:
            _LOGGER.info(
                f"[{self.__class__.__name__}] Creating enterprise session for {self.full_name} using creation function"
            )
            return await self._creation_function(self._system, self._name)
        except Exception as e:
            _LOGGER.error(
                f"[{self.__class__.__name__}] Failed to create enterprise session for {self.full_name}: {e}"
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

    Configuration and Credentials Architecture:
        The manager takes two separate constructor inputs:

        - **system_config** (:class:`~deephaven_mcp.sessions.EnterpriseSystemConfig`):
          Typed declaration carrying the ``connection_json_url`` and
          other non-secret connection parameters consumed by
          :meth:`CorePlusSessionFactory.from_credentials`. Auth material
          is NOT stored here — see ``creds`` instead.
        - **creds** (:class:`~deephaven_mcp.auth.credentials.Credentials`):
          Authentication material separated from ``config`` so that (a) a
          single config can be reused with different identities and
          (b) secrets never end up inside config dicts that may be logged or
          persisted.

    Integration Patterns:
        **Factory-Based Session Creation**:
        ```python
        factory_manager = CorePlusSessionFactoryManager("enterprise", config, creds)
        factory = await factory_manager.get()
        session = await factory.create_session(source="app", name="worker-1")
        ```

        **Multi-Configuration Support**:
        ```python
        configs = {
            "prod": {"connection_json_url": "https://prod/iris/connection.json"},
            "dev": {"connection_json_url": "https://dev/iris/connection.json"},
            "test": {"connection_json_url": "https://test/iris/connection.json"},
        }
        managers = {
            env: CorePlusSessionFactoryManager(env, config, creds)
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
        EnterpriseSessionManager: Session-level manager that can use factories
        SystemType.ENTERPRISE: The system type constant for Enterprise deployments
    """

    def __init__(
        self,
        name: str,
        system_config: EnterpriseSystemConfig,
        creds: Credentials,
        timeouts: EnterpriseClientTimeouts,
    ):
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
            - **full_name**: Computed as "enterprise:factory:{name}" for global uniqueness
            - **system_config**: Typed :class:`EnterpriseSystemConfig` declaration
              (connection URL, timeouts, etc.).
            - **creds**: Authentication material, passed separately from
              ``system_config``.

        Separation of Config and Credentials:
            Unlike EnterpriseSessionManager's function-based approach, this manager
            takes a typed system declaration plus a
            :class:`~deephaven_mcp.auth.credentials.Credentials` object. Keeping
            the two inputs separate means a single ``system_config`` can be reused
            with different identities (password, private key, etc.) and prevents
            secrets from ending up in dicts that may be logged, serialised,
            or persisted.

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
            system_config = multi_config.enterprise.systems["production"]
            creds = PasswordCredentials(username="alice", password="...")
            manager = CorePlusSessionFactoryManager(
                "prod-factory", system_config, creds
            )
            factory = await manager.get()  # Creates factory on first access
            session = await factory.create_session("app", "worker-1")
            ```

            **Multi-Environment Support**:
            ```python
            environments = multi_config.enterprise.systems
            managers = {
                env: CorePlusSessionFactoryManager(f"{env}-factory", sys_cfg, creds)
                for env, sys_cfg in environments.items()
            }
            ```

            **Health Monitoring Setup**:
            ```python
            manager = CorePlusSessionFactoryManager(
                "enterprise", system_config, creds
            )

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
            - **Typed declaration stored**: ``EnterpriseSystemConfig`` is cached for factory creation
            - **Thread-safe**: Manager can be safely used from multiple asyncio tasks
            - **Registry-ready**: Manager can be immediately added to registries

        Args:
            name: Unique identifier for this factory manager instance. Used for logging,
                debugging, registry management, and creating the full_name identifier.
                Should be descriptive and unique within its registry context
                (e.g., "prod-factory", "dev-east-factory", "test-factory").
            system_config: Validated
                :class:`~deephaven_mcp.sessions.EnterpriseSystemConfig`
                declaration passed to
                :meth:`CorePlusSessionFactory.from_credentials`. Carries
                ``connection_json_url`` and timeout settings; does NOT
                carry authentication material -- that is supplied via
                ``creds``.
            creds: Authentication material
                (:class:`~deephaven_mcp.auth.credentials.Credentials`) forwarded
                to :meth:`CorePlusSessionFactory.from_credentials` when the
                factory is lazily created. Must be a type the enterprise
                factory supports (:class:`PasswordCredentials` or
                :class:`PrivateKeyCredentials`); :class:`PSKCredentials` is
                rejected at factory-creation time.

        Thread Safety:
            This constructor is thread-safe and can be called from any asyncio task.
            All initialization is synchronous and does not involve network operations
            or factory creation.

        See Also:
            CorePlusSessionFactory: The Enterprise factory type that will be created from config
            SystemType.ENTERPRISE: The system type constant used for Enterprise deployments
            BaseItemManager.__init__(): The parent constructor that handles common initialization
        """
        # The factory belongs to the enterprise system identified by ``name``
        # (the system_name); we record that as ``system`` and use a sentinel
        # ``"factory"`` as the manager's own ``name``. Factories are not
        # listed alongside sessions; the listing filter uses ``isinstance``
        # against :class:`CorePlusSessionFactoryManager` to drop them.
        super().__init__(
            system_type=SystemType.ENTERPRISE,
            system=name,
            name="factory",
        )
        self._system_config = system_config
        self._creds = creds
        self._timeouts = timeouts

    @override
    async def _create_item(self) -> CorePlusSessionFactory:
        """Create and initialize a Deephaven Core+ session factory from configuration.

        This method implements the abstract _create_item() from BaseItemManager to provide
        Core+ (Enterprise) specific factory creation. It is called automatically during
        lazy initialization when get() is first invoked.

        Implementation:
            1. Reads the effective ``session_connect_timeout_seconds`` from
               :class:`EnterpriseClientTimeouts` (its field already carries the
               package-wide default when ``enterprise/settings.json`` omits
               the value).
            2. Calls ``CorePlusSessionFactory.from_credentials(system_config, creds)`` with a timeout wrapper.
            3. Logs creation progress (DEBUG) and completion (INFO).
            4. Handles timeout errors with appropriate logging and exception.

        Timeout Behavior:
            The configurable timeout prevents indefinite hanging when connecting to
            unreachable or slow Core+ systems. If the timeout expires, a
            DeephavenConnectionError is raised with a descriptive message.

        Args:
            None (uses ``self._system_config`` stored declaration)

        Returns:
            CorePlusSessionFactory: Initialized Core+ session factory ready to create
                CorePlusSession instances. The factory's ping() method can be used to
                verify health.

        Raises:
            DeephavenConnectionError: If connection times out after the configured duration.
                Includes timeout value and troubleshooting guidance in the error message.
            AuthenticationError: If Core+ authentication fails (raised by from_credentials).
            ConfigurationError: If configuration is invalid (raised by from_credentials).
            Exception: Other errors from CorePlusSessionFactory.from_credentials().

        Notes:
            - This method is marked @override to implement BaseItemManager abstract method.
            - Do not call directly - use get() for proper caching and error handling.
            - Timeout can be configured via
              ``enterprise/settings.json: timeouts.client.session_connect_timeout_seconds``;
              the field carries the schema default declared on
              :class:`~deephaven_mcp.client._timeouts.EnterpriseClientTimeouts`
              when omitted from the JSON.

        See Also:
            BaseItemManager.get(): Public method triggering lazy initialization
            CorePlusSessionFactory.from_credentials(): Underlying factory creation method
            EnterpriseSessionManager._create_item(): Session-level creation counterpart
        """
        # Effective connection timeout (project-wide default is filled
        # in at validation time when ``enterprise/settings.json`` omits
        # the field).
        timeout = self._timeouts.session_connect_timeout_seconds

        _LOGGER.debug(
            f"[{self.__class__.__name__}] Creating enterprise factory for '{self.full_name}' (timeout: {timeout}s)"
        )

        # Wrap factory creation with timeout to prevent hanging on unreachable systems
        try:
            factory = await asyncio.wait_for(
                CorePlusSessionFactory.from_credentials(
                    self._system_config, self._creds, self._timeouts
                ),
                timeout=timeout,
            )
            _LOGGER.info(
                f"[{self.__class__.__name__}] Successfully created enterprise factory for '{self.full_name}'"
            )
            return factory
        except TimeoutError as e:
            _LOGGER.error(
                f"[{self.__class__.__name__}] Connection to enterprise system '{self.full_name}' timed out after {timeout} seconds. "
                f"Increase enterprise/settings.json: timeouts.client.session_connect_timeout_seconds."
            )
            raise DeephavenConnectionError(
                f"Connection to enterprise system timed out after {timeout} seconds. "
                f"Check connection_json_url and network connectivity. To allow more time, "
                f"increase enterprise/settings.json: timeouts.client.session_connect_timeout_seconds."
            ) from e

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
