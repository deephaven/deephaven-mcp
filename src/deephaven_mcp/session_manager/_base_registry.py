"""
Provides a generic, async, and coroutine-safe base registry for managing objects.

This module defines the `BaseRegistry`, an abstract base class (ABC) that offers a standardized, reusable foundation for creating different types of registries. It includes core functionalities such as item caching, asynchronous initialization, and resource management, all designed to be coroutine-safe.

Key Features:
    - Abstract Base Class: Defines a clear and consistent interface for all registry implementations.
    - Coroutine-Safe: All operations are protected by an `asyncio.Lock` to ensure safe concurrent access.
    - Generic Design: Can be subclassed to manage any type of object, not just session managers.
    - Lifecycle Management: Includes `initialize` and `close` methods for robust lifecycle control.

Subclassing `BaseRegistry`:
    To create a new registry, you must subclass `BaseRegistry` and implement the `_load_items` abstract method. This method should contain the specific logic for loading and creating the items to be managed by the registry.
"""

import abc
import asyncio
import logging
import time
from typing import TypeVar, Generic

from deephaven_mcp import config
from deephaven_mcp._exceptions import InternalError

_LOGGER = logging.getLogger(__name__)

T = TypeVar("T")


class BaseRegistry(abc.ABC, Generic[T]):
    """
    Generic, async, coroutine-safe abstract base class for a registry of items.

    This class provides a skeletal implementation for managing a dictionary of items, including initialization, retrieval, and closure. It is designed to be subclassed to create specific types of registries.
    """

    def __init__(self):
        self._items: dict[str, T] = {}
        self._lock = asyncio.Lock()
        self._initialized = False
        _LOGGER.info("[%s] created (must call and await initialize() after construction)", self.__class__.__name__)

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
        Initializes the registry by loading all items.

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
        Retrieves an item from the registry by name.

        Args:
            name: The name of the item to retrieve.

        Returns:
            The item corresponding to the given name.

        Raises:
            InternalError: If the registry has not been initialized.
            KeyError: If the item is not found in the registry.
        """
        async with self._lock:
            if not self._initialized:
                raise InternalError(
                    f"{self.__class__.__name__} not initialized. Call 'await initialize()' after construction."
                )
            if name not in self._items:
                raise KeyError(f"No item found for: {name}")
            return self._items[name]

    async def close(self) -> None:
        """
        Closes all items in the registry.

        This method iterates through all items and calls their `close` method if it exists.
        """
        async with self._lock:
            if not self._initialized:
                raise InternalError(
                    f"{self.__class__.__name__} not initialized. Call 'await initialize()' after construction."
                )

            start_time = time.time()
            _LOGGER.info("[%s] closing all items...", self.__class__.__name__)
            num_items = len(self._items)

            for item in self._items.values():
                if hasattr(item, "close") and asyncio.iscoroutinefunction(item.close):
                    await item.close()

            _LOGGER.info(
                "[%s] close command sent to all items. Processed %d items in %.2fs",
                self.__class__.__name__,
                num_items,
                time.time() - start_time,
            )
