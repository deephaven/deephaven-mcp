"""Async, coroutine-safe registries for Deephaven resource management.

This module provides a generic, reusable foundation for managing named collections
of async-closable objects in a coroutine-safe environment.

Key Classes:
    BaseRegistry: Abstract, generic, coroutine-safe registry base class.
    MutableSessionRegistry: Extends BaseRegistry with tracked mutation support
        (add/count of dynamically created sessions).  Removal is exposed on
        :class:`BaseRegistry` itself via :meth:`remove`.

Features:
    - Abstract interface for all registry implementations (subclass and implement `_load_items`).
    - Coroutine-safe: All methods use `asyncio.Lock` for safe concurrent access.
    - Generic: Can be subclassed to manage any object type, not just sessions.
    - Lifecycle management: Robust `initialize` and `close` methods for resource control.

Eviction:
    Idle eviction is the responsibility of a separate
    :class:`~deephaven_mcp.resource_manager.Evictor`, not the registry.  The
    Evictor uses the registry's existing public collection API
    (:meth:`get_all` for iteration and :meth:`remove` for identity-checked
    removal); the registry itself has no sweep loop, no timing state, and
    no eviction-specific methods.

Usage:
    Subclass `BaseRegistry` and implement `_load_items` to define how items are loaded.
    Subclass `MutableSessionRegistry` when callers also need to add items
    dynamically after initialization.
"""

import abc
import asyncio
import enum
import logging
import time
from dataclasses import dataclass
from typing import override

from deephaven_mcp import config
from deephaven_mcp._exceptions import (
    InternalError,
    InvalidSessionNameError,
    RegistryItemNotFoundError,
)

from ._manager import (
    AsyncClosable,
    BaseItemManager,
    SystemType,
)

_LOGGER = logging.getLogger(__name__)


class InitializationPhase(enum.Enum):
    """Lifecycle phase of a registry's initialization.

    Registries that load synchronously (e.g. CommunitySessionRegistry) return
    ``COMPLETED`` immediately after ``initialize()`` finishes — they have no
    background initialization lifecycle.

    Complex registries (e.g. EnterpriseSessionRegistry) progress through phases:
    NOT_STARTED → PARTIAL → LOADING → COMPLETED (or FAILED).
    """

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
    Used by all registries — both synchronous (always) and complex
    (after background work completes)."""

    FAILED = "failed"
    """Initialization failed critically.  The registry may have partial data."""


@dataclass(frozen=True)
class RegistrySnapshot[T: AsyncClosable]:
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
        """Create a snapshot for a registry that loads synchronously.

        For registries that are always fully available after ``initialize()``
        completes (no background work, no per-source errors).

        Args:
            items (dict[str, T]): Copy of the registry items dictionary.

        Returns:
            A snapshot with phase COMPLETED and no initialization errors.
        """
        return cls(
            items=items,
            initialization_phase=InitializationPhase.COMPLETED,
            initialization_errors={},
        )

    @classmethod
    def with_initialization(
        cls,
        items: dict[str, T],
        phase: InitializationPhase,
        errors: dict[str, str],
    ) -> "RegistrySnapshot[T]":
        """Create a snapshot that includes initialization lifecycle state.

        For registries that perform background discovery after ``initialize()``
        returns and track per-source errors during that process.

        Args:
            items (dict[str, T]): Copy of the registry items dictionary.
            phase (InitializationPhase): Current initialization lifecycle phase.
            errors (dict[str, str]): Per-source error descriptions recorded during initialization.

        Returns:
            A snapshot with the given initialization state.
        """
        return cls(
            items=items,
            initialization_phase=phase,
            initialization_errors=errors,
        )


class BaseRegistry[T: AsyncClosable](abc.ABC):
    """Generic, async, coroutine-safe abstract base class for a named item registry.

    Manages a ``dict[str, T]`` of async-closable items.  Subclasses implement
    ``_load_items`` to populate the dict at initialization time; this class
    handles locking, idempotent initialization, retrieval, removal, and
    shutdown.

    Subclasses that need to track dynamically-added items separately from
    those loaded by ``_load_items`` should extend ``MutableSessionRegistry``.

    Idle eviction is handled by an external
    :class:`~deephaven_mcp.resource_manager.Evictor` that uses only public
    methods on the registry (:meth:`get_all` for iteration,
    :meth:`remove` for identity-checked removal).
    """

    def __init__(self) -> None:
        """Set up internal state.  ``await initialize()`` must be called before use."""
        self._items: dict[str, T] = {}
        self._lock = asyncio.Lock()
        self._initialized = False
        _LOGGER.info(
            f"[{self.__class__.__name__}] created "
            f"(must call and await initialize() after construction)"
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
        """Populate ``_items`` from the given configuration.

        Called by ``initialize()`` under ``self._lock``.  Subclasses must
        implement this to define how items are loaded.

        Args:
            config_manager (config.ConfigManager): Source of configuration data.
        """
        pass  # pragma: no cover

    async def initialize(self, config_manager: config.ConfigManager) -> None:
        """Initialize the registry by loading items from configuration.

        Idempotent — subsequent calls return immediately if already initialized.

        Args:
            config_manager (config.ConfigManager): Source of configuration data.
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
        """Retrieve an item by name.

        Args:
            name (str): Key of the item to retrieve.

        Returns:
            The item registered under *name*.

        Raises:
            InternalError: If the registry has not been initialized.
            RegistryItemNotFoundError: If no item with the given name exists.
        """
        async with self._lock:
            self._check_initialized()

            if name not in self._items:
                raise RegistryItemNotFoundError(
                    f"No item with name '{name}' found in {self.__class__.__name__}"
                )

            return self._items[name]

    async def get_all(self) -> RegistrySnapshot[T]:
        """Retrieve all items as an atomic snapshot.

        Returns:
            RegistrySnapshot[T]: Snapshot with ``items``, ``initialization_phase``
            (always ``COMPLETED`` for this base implementation), and
            ``initialization_errors`` (always empty for this base implementation).
            Subclasses that perform background initialization override this to
            return richer phase/error information.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        async with self._lock:
            self._check_initialized()

            # Return a copy to avoid external modification
            return RegistrySnapshot.simple(items=self._items.copy())

    async def remove(self, name: str, *, expected: T | None = None) -> T | None:
        """Remove and return the item registered under ``name``.

        Symmetric public verb to :meth:`get`.  Used by both routine
        callers (e.g. ``session_*_delete`` tools) and the
        :class:`~deephaven_mcp.resource_manager.Evictor` sweep loop.

        When ``expected`` is provided, the removal is **identity-checked**:
        the item is dropped only when ``self._items[name] is expected``.
        When ``expected`` is ``None``, the removal is unconditional (the
        existing item under ``name``, whatever it is, is dropped).

        Subclasses extend the post-removal behavior via :meth:`_on_removed`,
        which is invoked under ``self._lock`` immediately after the pop.

        Args:
            name (str): Key of the item to remove.
            expected (T | None): If non-``None``, only remove when the
                current entry is exactly this identity.

        Returns:
            T | None: The removed item, or ``None`` when no item is
                registered under ``name`` (or when ``expected`` was
                provided and the current entry did not match).

        Raises:
            InternalError: If the registry has not been initialized.
        """
        async with self._lock:
            self._check_initialized()
            current = self._items.get(name)
            if current is None:
                return None
            if expected is not None and current is not expected:
                return None
            removed = self._items.pop(name, None)
            self._on_removed(name)
            _LOGGER.debug(f"[{self.__class__.__name__}] removed item '{name}'")
            return removed

    def _on_removed(self, _name: str) -> None:
        """Subclass hook invoked under ``self._lock`` after :meth:`remove` pops an item.

        Default is a no-op.  Subclasses that maintain additional tracking
        structures (e.g.
        :attr:`MutableSessionRegistry._added_session_ids`) override this
        to keep them atomically consistent with ``_items``.  The parameter
        is prefixed with ``_`` here because the base body does not use
        it; overrides typically rename it to ``name``.
        """
        return None

    async def _close_items(self, items: list[T]) -> None:
        """Close a list of managed items, logging any errors.

        Called outside ``self._lock`` so that network I/O during close does not
        block other coroutines.  Subclasses may override this to add extra
        teardown steps alongside item closing.

        Args:
            items (list[T]): Items to close.  Each is closed in sequence; errors are
                logged and do not abort the remaining closures.
        """
        for item in items:
            try:
                await item.close()
            except Exception as e:
                _LOGGER.error(
                    f"[{self.__class__.__name__}] error closing item"
                    f" '{getattr(item, 'full_name', repr(item))}': {e}"
                )

    async def close(self) -> None:
        """Close all managed items and reset state for reinitialization.

        Captures items under ``self._lock``, resets state, then closes items
        **outside** the lock via :meth:`_close_items` so that network I/O
        during close does not block other coroutines.

        After this call the registry can be reinitialized via ``initialize()``.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        async with self._lock:
            self._check_initialized()
            start_time = time.time()
            _LOGGER.info(f"[{self.__class__.__name__}] closing all items...")
            num_items = len(self._items)
            items_to_close = list(self._items.values())
            self._items.clear()
            self._initialized = False

        await self._close_items(items_to_close)
        _LOGGER.info(
            f"[{self.__class__.__name__}] closed all items."
            f" Processed {num_items} items in {time.time() - start_time:.2f}s"
        )


class MutableSessionRegistry(BaseRegistry[BaseItemManager]):
    """Abstract registry that supports dynamic mutation after initialization.

    Extends ``BaseRegistry`` with ``_added_session_ids`` and two mutation
    methods (``add_session``, ``count_added_sessions``) that track items
    added after the initial ``_load_items`` call.  ``_load_items`` is
    still abstract — subclasses define how items are loaded from config.
    Removal uses the inherited :meth:`BaseRegistry.remove`; the
    :meth:`_on_removed` override below keeps ``_added_session_ids`` in
    sync with ``_items``.

    See Also:
        - `CommunitySessionRegistry`: Concrete subclass for community sessions.
        - `EnterpriseSessionRegistry`: Concrete subclass for enterprise sessions.
    """

    def __init__(self) -> None:
        """Initialize the registry.  Call ``await initialize()`` before use."""
        super().__init__()
        self._added_session_ids: set[str] = set()

    @override
    def _on_removed(self, name: str) -> None:
        """Discard the removed key from the added-session tracking set.

        Frees the corresponding ``max_concurrent_sessions`` slot — see
        :meth:`count_added_sessions`.  Called under ``self._lock`` from
        :meth:`BaseRegistry.remove`, so the pop from ``_items`` and the
        discard from ``_added_session_ids`` are observed atomically.
        """
        self._added_session_ids.discard(name)

    @override
    async def close(self) -> None:
        """Close all managed items and clear mutation-tracking state.

        Delegates item closure and ``_initialized`` reset to
        ``BaseRegistry.close()``, then clears ``_added_session_ids`` so the
        registry is clean for reinitialization.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        await super().close()
        # super().close() sets _initialized=False under self._lock, preventing
        # any concurrent mutation call from entering after this point.
        self._added_session_ids.clear()

    async def add_session(self, manager: BaseItemManager) -> None:
        """Add a dynamically created session to the registry and mark it as added.

        Args:
            manager (BaseItemManager): Session manager to add.  Its ``full_name`` must not already
                exist in the registry.

        Raises:
            ValueError: If a session with the same ``full_name`` already exists.
            InternalError: If the registry has not been initialized.
        """
        async with self._lock:
            self._check_initialized()
            session_id = manager.full_name
            if session_id in self._items:
                raise ValueError(f"Session '{session_id}' already exists in registry")
            self._items[session_id] = manager
            self._added_session_ids.add(session_id)
            _LOGGER.debug(f"[{self.__class__.__name__}] added session '{session_id}'")

    async def count_added_sessions(
        self, system_type: SystemType, system_name: str
    ) -> int:
        """Count dynamically added sessions for a specific system that still exist.

        Only counts sessions that were added via ``add_session()`` (not config-loaded
        sessions) and that are still present in the registry.

        Args:
            system_type (SystemType): Session type to filter by (e.g. ``SystemType.COMMUNITY``).
            system_name (str): Source/system name to filter by.

        Returns:
            int: Count of matching dynamically added sessions still in the registry.

        Raises:
            InternalError: If the registry has not been initialized, or if a malformed
                session ID is found in the internal tracking set.
        """
        async with self._lock:
            self._check_initialized()
            count = 0
            for sid in self._added_session_ids:
                try:
                    s_type, s_source, _ = BaseItemManager.parse_full_name(sid)
                except InvalidSessionNameError as e:
                    raise InternalError(
                        f"Malformed session ID {sid!r} found in _added_session_ids: {e}"
                    ) from e
                if (
                    s_type == system_type.value
                    and s_source == system_name
                    and sid in self._items
                ):
                    count += 1
            return count
