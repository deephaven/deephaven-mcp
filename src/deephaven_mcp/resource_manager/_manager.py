"""
Base classes and enums for Deephaven MCP session and factory management.

This module provides the core async/coroutine-safe building blocks for managing sessions and factories
in both community and enterprise Deephaven deployments. It defines generic, extensible manager types
that ensure safe, lazy initialization and lifecycle management for backend resources.

Key Classes:
    SystemType: Enum for backend system type (COMMUNITY, ENTERPRISE).
    BaseItemManager: Generic async manager for a single lazily-initialized item, with locking, caching, liveness, and close logic.
    CommunitySessionManager: Manages a single CoreSession (community) with lazy init, caching, and liveness.
    EnterpriseSessionManager: Manages a single CorePlusSession (enterprise) via a CorePlusSessionFactory.
    CorePlusSessionFactoryManager: Manages a single CorePlusSessionFactory (enterprise), with liveness via ping().

All managers are designed to be coroutine-safe and are suitable for use in async applications.
"""

import asyncio
import enum
import logging
import sys
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

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
    """

    async def close(self) -> None:
        """Close the underlying resource or session."""
        ...  # pragma: no cover


T = TypeVar("T", bound=AsyncClosable)


class SystemType(str, enum.Enum):
    """Enum representing the types of backend systems."""

    COMMUNITY = "community"
    ENTERPRISE = "enterprise"


class BaseItemManager(Generic[T], ABC):
    """
    A generic, async, coroutine-safe base class for managing a single, lazily-initialized item.

    This class provides the core boilerplate for:
    - Lazy initialization on the first `get` call.
    - Caching the item for subsequent calls.
    - Thread-safe operations using an asyncio.Lock.
    - A common interface for getting, checking liveness, and closing the item.
    """

    def __init__(self, system_type: SystemType, source: str, name: str):
        """
        Initialize the manager.

        Args:
            system_type: The system type (e.g., COMMUNITY, ENTERPRISE).
            source: The configuration source name (e.g., a file path or URL).
            name: The name of the manager instance.
        """
        self._system_type = system_type
        self._source = source
        self._name = name
        self._item_cache: T | None = None
        self._lock = asyncio.Lock()

    @property
    def system_type(self) -> SystemType:
        """The type of system this manager connects to."""
        return self._system_type

    @property
    def source(self) -> str:
        """The source of the item, used for grouping or identification."""
        return self._source

    @property
    def name(self) -> str:
        """The name of the manager, used for identification and logging."""
        return self._name

    @property
    def full_name(self) -> str:
        """A fully qualified name for the manager instance."""
        return f"{self.system_type.value}:{self.source}:{self.name}"

    @abstractmethod
    async def _create_item(self) -> T:
        """Abstract method to create the managed item."""
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    async def _check_liveness(self, item: T) -> bool:
        """Abstract method to check the liveness of the managed item."""
        raise NotImplementedError  # pragma: no cover

    async def get(self) -> T:
        """Get the managed item, creating it if it doesn't exist."""
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
        """Check if the underlying item is alive."""
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
    An async, coroutine-safe manager for a single Deephaven community session.

    This class, built on the `BaseItemManager` foundation, handles the lazy
    initialization, caching, and lifecycle of a `CoreSession`. It guarantees
    that only one session instance is created per manager and provides a
    coroutine-safe `get()` method for access.

    The underlying `CoreSession` is created on-demand from a configuration
    dictionary passed during the manager's instantiation. Session liveness is
    determined by the session's own `is_alive()` method.

    While it can be used standalone, this manager is primarily designed to be
    held and managed by a `CommunitySessionRegistry`, which oversees a
    collection of these managers for different workers.

    See Also:
        - `BaseItemManager`: The generic base class providing core lifecycle and concurrency logic.
        - `CoreSession`: The async session wrapper for standard Deephaven sessions.
        - `CommunitySessionRegistry`: The registry that manages multiple `CommunitySessionManager` instances.
    """

    def __init__(self, name: str, config: dict[str, Any]):
        """Initialize the manager with a name and configuration.

        Args:
            name: The name of the manager instance, used for identification.
            config: The configuration dictionary used to create the CoreSession.
        """
        super().__init__(
            system_type=SystemType.COMMUNITY,
            source="community",
            name=name,
        )
        self._config = config

    @override
    async def _create_item(self) -> CoreSession:
        """Create the CoreSession from the config."""
        try:
            return await CoreSession.from_config(self._config)
        except Exception as e:
            # TODO: what exception strategy?
            raise SessionCreationError(
                f"Failed to create session for community worker {self._name}: {e}"
            ) from e

    @override
    async def _check_liveness(self, item: CoreSession) -> bool:
        """Check the liveness of the session."""
        return await item.is_alive()


class EnterpriseSessionManager(BaseItemManager[CorePlusSession]):
    """
    An async, coroutine-safe manager for a single Deephaven enterprise session.

    This manager, inheriting from `BaseItemManager`, is responsible for the
    complete lifecycle of a `CorePlusSession`. Its role is to abstract the
    process of obtaining a session from a `CorePlusSessionFactory`.

    The manager is given a factory instance during construction and uses it to
    create session objects on demand. It does not own or manage the lifecycle
    of the factory itself; that is the responsibility of the caller, typically
    a registry that manages a pool of factories.

    Liveness of the managed session is checked via the session's `is_alive()` method.

    See Also:
        - `BaseItemManager`: The generic base class providing core lifecycle and concurrency logic.
        - `CorePlusSession`: The async session wrapper for enterprise Deephaven sessions.
        - `CorePlusSessionFactory`: The factory responsible for creating `CorePlusSession` instances.
    """

    def __init__(
        self,
        source: str,
        name: str,
        factory: CorePlusSessionFactory,
    ):
        """Initialize the manager with a name and a session factory.

        Args:
            source: The configuration source name (e.g., a file path or URL).
            name: The name of the manager instance, used for identification.
            factory: The CorePlusSessionFactory used to create sessions.

        Note:
            The factory instance is expected to be properly configured and
            ready to create sessions. The manager does not validate the factory
            instance during initialization.
        """
        super().__init__(system_type=SystemType.ENTERPRISE, source=source, name=name)
        self._factory = factory

    @override
    async def _create_item(self) -> CorePlusSession:
        """Create the CorePlusSession from the provided factory."""
        # TODO: implement
        raise NotImplementedError
        # try:
        #     session = self._factory.connect_to_persistent_query()
        #     return CorePlusSession(session)
        #     return await self._factory.get_session()
        # except Exception as e:
        #     raise SessionCreationError(
        #         f"Failed to create enterprise session for {self._name}: {e}"
        #     ) from e

    @override
    async def _check_liveness(self, item: CorePlusSession) -> bool:
        """Check the liveness of the session."""
        return await item.is_alive()


class CorePlusSessionFactoryManager(BaseItemManager[CorePlusSessionFactory]):
    """
    An async, coroutine-safe manager for a single `CorePlusSessionFactory`.

    This manager is a critical component of the enterprise session architecture.
    Instead of managing a session directly, it manages the lifecycle of a
    `CorePlusSessionFactory`. This factory is then used by other components
    (like an `EnterpriseSessionManager`) to create `CorePlusSession` instances.

    Built on `BaseItemManager`, it handles the lazy, thread-safe creation of the
    factory from a configuration dictionary. It is typically managed by a
    `CorePlusSessionFactoryRegistry`.

    Liveness for the factory is not determined by an `is_alive` method, but
    rather by calling the factory's `ping()` method, which confirms connectivity
    and readiness.

    See Also:
        - `BaseItemManager`: The generic base class providing core lifecycle and concurrency logic.
        - `CorePlusSessionFactory`: The factory this manager creates and manages.
        - `CorePlusSessionFactoryRegistry`: The registry that manages multiple factory managers.
    """

    def __init__(self, name: str, config: dict[str, Any]):
        """Initialize the manager with a name and configuration.

        Args:
            name: The name of the manager instance, used for identification.
            config: The configuration dictionary used to create the CorePlusSessionFactory.
        """
        super().__init__(
            system_type=SystemType.ENTERPRISE,
            source="factory",
            name=name,
        )
        self._config = config

    @override
    async def _create_item(self) -> CorePlusSessionFactory:
        """Create the CorePlusSessionFactory from the config."""
        return await CorePlusSessionFactory.from_config(self._config)

    @override
    async def _check_liveness(self, item: CorePlusSessionFactory) -> bool:
        """Check the liveness of the factory by pinging it."""
        return await item.ping()
