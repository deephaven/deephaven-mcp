"""
Async, coroutine-safe registries for Deephaven MCP resource management.

This module provides a generic, reusable foundation for managing collections of objects (such as session or factory managers)
in a coroutine-safe, async environment. It defines the abstract `BaseRegistry` and concrete registry implementations for
community and enterprise session/factory managers.

Key Classes:
    BaseRegistry: Abstract, generic, coroutine-safe registry base class. Handles item caching, async initialization, locking, and closure.
    CommunitySessionRegistry: Registry for managing CommunitySessionManager instances. Discovers and loads community sessions from config.
    CorePlusSessionFactoryRegistry: Registry for managing CorePlusSessionFactoryManager instances. Discovers and loads enterprise factories from config.

Features:
    - Abstract interface for all registry implementations (subclass and implement `_load_items`).
    - Coroutine-safe: All methods use `asyncio.Lock` for safe concurrent access.
    - Generic: Can be subclassed to manage any object type, not just sessions.
    - Lifecycle management: Robust `initialize` and `close` methods for resource control.

Usage:
    Subclass `BaseRegistry` and implement the `_load_items` method to define how items are loaded from configuration.
    Use the provided concrete registries for most Deephaven MCP session/factory management scenarios.
"""

import abc
import asyncio
import enum
import logging
import sys
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from typing_extensions import override  # pragma: no cover
elif sys.version_info >= (3, 12):
    from typing import override  # pragma: no cover
else:
    from typing_extensions import override  # pragma: no cover

from deephaven_mcp import config
from deephaven_mcp._exceptions import (
    ConfigurationError,
    InternalError,
    MissingEnterprisePackageError,
    RegistryItemNotFoundError,
)
from deephaven_mcp.client import is_enterprise_available

from ._manager import (
    AsyncClosable,
    CommunitySessionManager,
    CorePlusSessionFactoryManager,
    StaticCommunitySessionManager,
)

_LOGGER = logging.getLogger(__name__)

T = TypeVar("T", bound=AsyncClosable)


class InitializationPhase(enum.Enum):
    """Lifecycle phase of a registry's initialization.

    Simple registries (e.g. CommunitySessionRegistry, CorePlusSessionFactoryRegistry)
    always return ``SIMPLE`` — they have no initialization lifecycle.

    Complex registries (e.g. CombinedSessionRegistry) progress through phases:
    NOT_STARTED → PARTIAL → LOADING → COMPLETED (or FAILED).
    """

    SIMPLE = "simple"
    """Registry has no initialization lifecycle — always fully available.
    Used by simple registries like CommunitySessionRegistry and
    CorePlusSessionFactoryRegistry."""

    NOT_STARTED = "not_started"
    """Registry has not been initialized yet."""

    PARTIAL = "partial"
    """Core items are loaded and the registry is usable, but background
    initialization work has not yet started."""

    LOADING = "loading"
    """Background initialization is actively running.  Items may become
    available progressively as each unit of work completes."""

    COMPLETED = "completed"
    """All initialization has finished (successfully or with errors).
    On-demand updates resume in ``get()`` / ``get_all()``.  Only used by
    complex registries with an initialization lifecycle."""

    FAILED = "failed"
    """Initialization failed critically.  The registry may have partial data."""


@dataclass(frozen=True)
class RegistrySnapshot(Generic[T]):
    """Atomic snapshot of registry items and initialization state.

    Returned by :meth:`BaseRegistry.get_all` to provide a consistent view
    captured under a single lock acquisition.

    All fields are required — use the class methods :meth:`simple` and
    :meth:`with_initialization` for convenient construction.

    Attributes:
        items: Copy of the registry items dictionary mapping names to their
            corresponding manager instances.
        initialization_phase: Current lifecycle phase of the registry.
        initialization_errors: Errors recorded during background
            initialization.  Maps source names to error descriptions.
            Empty dict means no errors.
    """

    items: dict[str, T]
    initialization_phase: InitializationPhase
    initialization_errors: dict[str, str]

    @classmethod
    def simple(cls, items: dict[str, T]) -> "RegistrySnapshot[T]":
        """Create a snapshot for a registry without initialization lifecycle.

        Intended for simple registries (CommunitySessionRegistry,
        CorePlusSessionFactoryRegistry) that are always fully available.

        Args:
            items: Copy of the registry items dictionary.

        Returns:
            A snapshot with phase SIMPLE and no initialization errors.
        """
        return cls(
            items=items,
            initialization_phase=InitializationPhase.SIMPLE,
            initialization_errors={},
        )

    @classmethod
    def with_initialization(
        cls,
        items: dict[str, T],
        phase: InitializationPhase,
        errors: dict[str, str],
    ) -> "RegistrySnapshot[T]":
        """Create a snapshot that includes initialization state.

        Intended for CombinedSessionRegistry, which tracks enterprise
        discovery progress and per-factory errors.

        Args:
            items: Copy of the registry items dictionary.
            phase: Current initialization lifecycle phase.
            errors: Per-factory error descriptions from enterprise discovery.

        Returns:
            A snapshot with the given initialization state.
        """
        return cls(
            items=items,
            initialization_phase=phase,
            initialization_errors=errors,
        )


class BaseRegistry(abc.ABC, Generic[T]):
    """
    Generic, async, coroutine-safe abstract base class for a registry of items.

    This class provides a skeletal implementation for managing a dictionary of items, including initialization, retrieval, and closure. It is designed to be subclassed to create specific types of registries.

    See Also:
        - `CommunitySessionRegistry`: A concrete implementation for managing community sessions.
        - `CorePlusSessionFactoryRegistry`: A concrete implementation for managing enterprise factories.
    """

    def __init__(self) -> None:
        """Initialize the BaseRegistry.

        This constructor sets up the internal state for the registry, including
        the item dictionary, an asyncio lock for safe concurrent access, and
        an initialization flag.

        It's important to note that the registry is not fully operational after
        the constructor is called. The `initialize()` method must be called and
        awaited to load the configured items before the registry can be used.
        """
        self._items: dict[str, T] = {}
        self._lock = asyncio.Lock()
        self._initialized = False
        _LOGGER.info(
            f"[{self.__class__.__name__}] created (must call and await initialize() after construction)"
        )

    def _check_initialized(self) -> None:
        """Check if the registry is initialized and raise an error if not.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        if not self._initialized:
            _LOGGER.error(
                f"[{self.__class__.__name__}] Not initialized. Call 'await initialize()' after construction."
            )
            raise InternalError(
                f"{self.__class__.__name__} not initialized. Call 'await initialize()' after construction."
            )

    @abc.abstractmethod
    async def _load_items(self, config_manager: config.ConfigManager) -> None:
        """
        Abstract method to load items into the registry.

        Subclasses must implement this method to populate the `_items` dictionary.

        Args:
            config_manager: The configuration manager to use for loading item configurations.
        """
        pass  # pragma: no cover

    async def initialize(self, config_manager: config.ConfigManager) -> None:
        """
        Initialize the registry by loading all configured items.

        This method is idempotent and ensures that initialization is only performed once.

        Args:
            config_manager: The configuration manager to use for loading item configurations.
        """
        async with self._lock:
            if self._initialized:
                return

            _LOGGER.info(f"[{self.__class__.__name__}] initializing...")
            await self._load_items(config_manager)
            self._initialized = True
            _LOGGER.info(
                f"[{self.__class__.__name__}] initialized with {len(self._items)} items"
            )

    async def get(self, name: str) -> T:
        """
        Retrieve an item from the registry by its name.

        Args:
            name: The name of the item to retrieve.

        Returns:
            The item corresponding to the given name.

        Raises:
            InternalError: If the registry has not been initialized.
            RegistryItemNotFoundError: If no item with the given name exists in the registry.
        """
        async with self._lock:
            self._check_initialized()

            if name not in self._items:
                raise RegistryItemNotFoundError(
                    f"No item with name '{name}' found in {self.__class__.__name__}"
                )

            return self._items[name]

    async def get_all(self) -> RegistrySnapshot[T]:
        """
        Retrieve all items from the registry as an atomic snapshot.

        Returns:
            RegistrySnapshot[T]: An atomic snapshot containing:

                - **items** — ``dict[str, T]`` copy of all registered items.
                - **initialization_phase** — the current
                  :class:`InitializationPhase` lifecycle value.
                  Always ``SIMPLE`` for simple registries.
                - **initialization_errors** — ``dict[str, str]`` mapping
                  source names to error descriptions.  Always empty for
                  simple registries.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        async with self._lock:
            self._check_initialized()

            # Return a copy to avoid external modification
            return RegistrySnapshot.simple(items=self._items.copy())

    async def close(self) -> None:
        """
        Close all managed items in the registry and reset state for reinitialization.

        This method iterates through all items and calls their `close` method,
        then resets `_initialized` and clears `_items` so the registry can be
        reinitialized via `initialize()` if needed.

        Note:
            This method is intended as a terminal shutdown operation. It holds
            ``self._lock`` for the duration of closing all items, which includes
            network calls. It is not safe to call concurrently with other operations.
        """
        async with self._lock:
            self._check_initialized()

            start_time = time.time()
            _LOGGER.info(f"[{self.__class__.__name__}] closing all items...")
            num_items = len(self._items)

            for item in self._items.values():
                await item.close()

            self._items.clear()
            self._initialized = False

            _LOGGER.info(
                f"[{self.__class__.__name__}] closed all items. Processed {num_items} items in {time.time() - start_time:.2f}s"
            )


class CommunitySessionRegistry(BaseRegistry[CommunitySessionManager]):
    """
    A registry for managing `CommunitySessionManager` instances.

    This class discovers and loads community session configurations from the
    `community.sessions` path in the application's configuration data.
    """

    @override
    async def _load_items(self, config_manager: config.ConfigManager) -> None:
        """
        Load session configurations and create CommunitySessionManager instances.

        Args:
            config_manager: The configuration manager to use for loading session configurations.
        """
        config_data = await config_manager.get_config()
        community_sessions_config = config_data.get("community", {}).get("sessions", {})

        _LOGGER.info(
            f"[{self.__class__.__name__}] Found {len(community_sessions_config)} community session configurations to load."
        )

        for session_name, session_config in community_sessions_config.items():
            _LOGGER.info(
                f"[{self.__class__.__name__}] Loading session configuration for '{session_name}'..."
            )
            self._items[session_name] = StaticCommunitySessionManager(
                session_name, session_config
            )


class CorePlusSessionFactoryRegistry(BaseRegistry[CorePlusSessionFactoryManager]):
    """
    A registry for managing `CorePlusSessionFactoryManager` instances.

    This class discovers and loads enterprise factory configurations from the
    `enterprise.factories` path in the application's configuration data.
    """

    @override
    async def _load_items(self, config_manager: config.ConfigManager) -> None:
        """
        Load factory configurations and create CorePlusSessionFactoryManager instances.

        Args:
            config_manager: The configuration manager to use for loading factory configurations.
        """
        config_data = await config_manager.get_config()
        factories_config = config_data.get("enterprise", {}).get("systems", {})

        if not is_enterprise_available and factories_config:
            raise ConfigurationError(
                "Enterprise factory configurations were found in your config, but the required "
                "Python package 'deephaven-coreplus-client' is not installed. "
                "Please install the deephaven-coreplus-client package to use Deephaven Enterprise (DHE) features, "
                "or remove the enterprise factory configurations from your config file."
            ) from MissingEnterprisePackageError()

        _LOGGER.info(
            f"[{self.__class__.__name__}] Found {len(factories_config)} core+ factory configurations to load."
        )

        for factory_name, factory_config in factories_config.items():
            _LOGGER.info(
                f"[{self.__class__.__name__}] Loading factory configuration for '{factory_name}'..."
            )
            self._items[factory_name] = CorePlusSessionFactoryManager(
                factory_name, factory_config
            )
