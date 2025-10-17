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
import logging
import sys
import time
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
)
from deephaven_mcp.client import is_enterprise_available

from ._manager import CommunitySessionManager, CorePlusSessionFactoryManager

_LOGGER = logging.getLogger(__name__)

T = TypeVar("T")


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
            "[%s] created (must call and await initialize() after construction)",
            self.__class__.__name__,
        )

    def _check_initialized(self) -> None:
        """Check if the registry is initialized and raise an error if not.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        if not self._initialized:
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

            _LOGGER.info("[%s] initializing...", self.__class__.__name__)
            await self._load_items(config_manager)
            self._initialized = True
            _LOGGER.info(
                "[%s] initialized with %d items",
                self.__class__.__name__,
                len(self._items),
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
            KeyError: If no item with the given name exists in the registry.
        """
        async with self._lock:
            self._check_initialized()

            if name not in self._items:
                raise KeyError(
                    f"No item with name '{name}' found in {self.__class__.__name__}"
                )

            return self._items[name]

    async def get_all(self) -> dict[str, T]:
        """
        Retrieve all items from the registry.

        Returns:
            A copy of the items dictionary containing all registered items.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        async with self._lock:
            self._check_initialized()

            # Return a copy to avoid external modification
            return self._items.copy()

    async def close(self) -> None:
        """
        Close all managed items in the registry.

        This method iterates through all items and calls their `close` method.

        Raises:
            InternalError: If any item in the registry does not have a compliant
                async `close` method.
        """
        async with self._lock:
            self._check_initialized()

            start_time = time.time()
            _LOGGER.info("[%s] closing all items...", self.__class__.__name__)
            num_items = len(self._items)

            for item in self._items.values():
                if hasattr(item, "close") and asyncio.iscoroutinefunction(item.close):
                    await item.close()
                else:
                    _LOGGER.error(
                        f"Item {item} does not have a close method or the method is not a coroutine function."
                    )
                    raise InternalError(
                        f"Item {item} does not have a close method or the method is not a coroutine function."
                    )

            _LOGGER.info(
                "[%s] close command sent to all items. Processed %d items in %.2fs",
                self.__class__.__name__,
                num_items,
                time.time() - start_time,
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
            "[%s] Found %d community session configurations to load.",
            self.__class__.__name__,
            len(community_sessions_config),
        )

        for session_name, session_config in community_sessions_config.items():
            _LOGGER.info(
                "[%s] Loading session configuration for '%s'...",
                self.__class__.__name__,
                session_name,
            )
            self._items[session_name] = CommunitySessionManager(
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
                "deephaven-coreplus-client package is not available. "
                "Please install deephaven-coreplus-client to use Deephaven Enterprise (DHE) features, "
                "or remove the enterprise factory configurations from your config file."
            ) from MissingEnterprisePackageError()

        _LOGGER.info(
            "[%s] Found %d core+ factory configurations to load.",
            self.__class__.__name__,
            len(factories_config),
        )

        for factory_name, factory_config in factories_config.items():
            _LOGGER.info(
                "[%s] Loading factory configuration for '%s'...",
                self.__class__.__name__,
                factory_name,
            )
            self._items[factory_name] = CorePlusSessionFactoryManager(
                factory_name, factory_config
            )
