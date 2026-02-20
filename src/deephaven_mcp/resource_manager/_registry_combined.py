"""
Combined registry for managing both community and enterprise session resources.

This module provides the ``CombinedSessionRegistry`` class, which extends
``BaseRegistry`` to unify management of community sessions (loaded from config)
and enterprise sessions (discovered from controller clients).

Architecture
------------
``CombinedSessionRegistry`` inherits the ``_items`` dict, ``_lock``, and
``_initialized`` flag from ``BaseRegistry``.  It adds:

- ``_community_registry`` / ``_enterprise_registry`` — sub-registries.
- ``_controller_clients`` — per-factory controller client cache.
- ``_added_session_ids`` — tracks sessions explicitly added via
  ``add_session()`` for MCP-created session counting.
- ``_phase`` / ``_errors`` — enterprise discovery lifecycle state.
- ``_discovery_task`` — background task for initial enterprise discovery.
- ``_refresh_lock`` — serializes concurrent enterprise refresh operations.

Locking contract (two rules, no exceptions)
-------------------------------------------
``self._lock``    — protects all mutable state; held only for fast operations.
``_refresh_lock`` — serializes enterprise refresh; **never held simultaneously
                    with** ``self._lock``.

Enterprise refresh is a four-phase operation:

1. **Snapshot**: call ``_enterprise_registry.get_all()`` outside the lock,
   then acquire ``self._lock`` briefly to read cached clients.
2. **Query**    (no lock): network I/O per factory via ``_fetch_factory_pqs``.
3. **Apply**    (``self._lock``): mutate ``_items``/caches, collect managers
   to close.
4. **Close**    (no lock): close stale managers outside the lock.
"""

import asyncio
import logging
import sys
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing_extensions import override  # pragma: no cover
elif sys.version_info >= (3, 12):
    from typing import override  # pragma: no cover
else:
    from typing_extensions import override  # pragma: no cover

from deephaven_mcp._exceptions import (
    InternalError,
    InvalidSessionNameError,
    RegistryItemNotFoundError,
)
from deephaven_mcp.client import CorePlusControllerClient, CorePlusSession
from deephaven_mcp.config import ConfigManager

from ._manager import BaseItemManager, EnterpriseSessionManager, SystemType
from ._registry import (
    BaseRegistry,
    CommunitySessionRegistry,
    CorePlusSessionFactoryManager,
    CorePlusSessionFactoryRegistry,
    InitializationPhase,
    RegistrySnapshot,
)

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level result types
# ---------------------------------------------------------------------------


@dataclass
class _FactorySnapshot:
    """State captured under ``self._lock`` for one factory before querying.

    Attributes:
        factory_name (str): Unique name of the enterprise factory.
        factory_manager (CorePlusSessionFactoryManager): Manager used to obtain
            a connected factory instance and its controller client.
        client (CorePlusControllerClient | None): Cached controller client from
            the previous refresh cycle, or ``None`` if this is the first query.
    """

    factory_name: str
    factory_manager: CorePlusSessionFactoryManager
    client: CorePlusControllerClient | None


@dataclass
class _FactoryQueryResult:
    """Successful result from querying one factory's controller.

    Attributes:
        factory_name (str): Unique name of the enterprise factory.
        new_client (CorePlusControllerClient): The live client used for this
            query — either the cached client (if ping succeeded) or a freshly
            created one (if the cached client was dead or absent).
        query_names (set[str]): Names of all persistent queries currently
            reported by the controller.
    """

    factory_name: str
    new_client: CorePlusControllerClient
    query_names: set[str]


@dataclass
class _FactoryQueryError:
    """Failed result from querying one factory's controller.

    Attributes:
        factory_name (str): Unique name of the enterprise factory.
        new_client (CorePlusControllerClient | None): A freshly created client
            if one was obtained before the failure (e.g. ``map()`` failed after
            a successful connect), or ``None`` if no new client was created
            (e.g. the cached client failed ping and reconnect also failed).
        error (str): Human-readable error description in ``"ExcType: message"``
            format.
    """

    factory_name: str
    new_client: CorePlusControllerClient | None
    error: str


# ---------------------------------------------------------------------------
# Module-level pure I/O function — no shared state
# ---------------------------------------------------------------------------


class _PingFailed(Exception):
    """Sentinel raised when a controller client ping returns False."""


async def _fetch_factory_pqs(
    snapshot: _FactorySnapshot,
) -> _FactoryQueryResult | _FactoryQueryError:
    """Query one enterprise factory's controller for its current PQ list.

    Pure I/O function — accesses no shared registry state.

    Algorithm:
        1. If no cached client, create one via ``factory_manager.get()``.
        2. If cached client exists, ping to verify liveness; recreate if dead.
        3. Call ``map()`` to get the current PQ list.

    Args:
        snapshot: Per-factory state captured in Phase 1.

    Returns:
        ``_FactoryQueryResult`` on success, ``_FactoryQueryError`` on failure.
    """
    name = snapshot.factory_name
    client = snapshot.client
    new_client: CorePlusControllerClient | None = None

    try:
        if client is None:
            factory_instance = await snapshot.factory_manager.get()
            client = factory_instance.controller_client
            new_client = client
            _LOGGER.debug(f"[_fetch_factory_pqs] created new client for '{name}'")
        else:
            try:
                ping_ok = await client.ping()
                if not ping_ok:
                    raise _PingFailed("ping returned False")
            except Exception as ping_err:
                _LOGGER.warning(
                    f"[_fetch_factory_pqs] cached client for '{name}' dead ({ping_err}); recreating"
                )
                factory_instance = await snapshot.factory_manager.get()
                client = factory_instance.controller_client
                new_client = client

        query_map = await client.map()
        query_names = {info.config.pb.name for info in query_map.values()}
        _LOGGER.debug(f"[_fetch_factory_pqs] factory '{name}': {len(query_names)} PQs")
        return _FactoryQueryResult(
            factory_name=name,
            new_client=client,
            query_names=query_names,
        )

    except Exception as e:
        _LOGGER.warning(f"[_fetch_factory_pqs] factory '{name}' failed: {e}")
        return _FactoryQueryError(
            factory_name=name,
            new_client=new_client,
            error=f"{type(e).__name__}: {e}",
        )


class CombinedSessionRegistry(BaseRegistry[BaseItemManager]):
    """Unified registry for community and enterprise session resources.

    Extends ``BaseRegistry[BaseItemManager]`` to manage both community sessions
    (loaded synchronously from config) and enterprise sessions (discovered
    asynchronously from controller clients).

    See module docstring for the full locking contract.

    Usage::

        registry = CombinedSessionRegistry()
        await registry.initialize(config_manager)
        snapshot = await registry.get_all()
        session_mgr = await registry.get("enterprise:prod:my-session")
        await registry.close()
    """

    @staticmethod
    def _make_enterprise_session_manager(
        factory: CorePlusSessionFactoryManager,
        factory_name: str,
        session_name: str,
    ) -> EnterpriseSessionManager:
        """Create an ``EnterpriseSessionManager`` that lazily connects to a PQ.

        Args:
            factory: Factory manager used to obtain a connected factory instance.
            factory_name: Source name for the session (e.g. ``"prod-system"``).
            session_name: PQ name to connect to.

        Returns:
            An ``EnterpriseSessionManager`` whose creation function calls
            ``factory.get()`` then ``connect_to_persistent_query(session_name)``
            on first use.
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
        """Initialize the registry.  Call ``await initialize()`` before use."""
        super().__init__()
        self._community_registry: CommunitySessionRegistry | None = None
        self._enterprise_registry: CorePlusSessionFactoryRegistry | None = None
        self._controller_clients: dict[str, CorePlusControllerClient] = {}
        self._added_session_ids: set[str] = set()
        self._phase: InitializationPhase = InitializationPhase.NOT_STARTED
        self._errors: dict[str, str] = {}
        self._discovery_task: asyncio.Task[None] | None = None
        self._refresh_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # BaseRegistry overrides — lifecycle
    # ------------------------------------------------------------------

    @override
    async def _load_items(self, config_manager: ConfigManager) -> None:
        """Load community sessions into ``self._items``.

        Called by ``super().initialize()`` while holding ``self._lock``.
        Creates and initializes both sub-registries, then copies community
        sessions into ``self._items``.

        Args:
            config_manager: Configuration source for both sub-registries.

        Raises:
            InternalError: If the community registry returns an unexpected
                snapshot phase or errors (indicates a programming bug).
        """
        self._community_registry = CommunitySessionRegistry()
        await self._community_registry.initialize(config_manager)

        self._enterprise_registry = CorePlusSessionFactoryRegistry()
        await self._enterprise_registry.initialize(config_manager)

        community_snapshot = await self._community_registry.get_all()
        if community_snapshot.initialization_phase != InitializationPhase.SIMPLE:
            raise InternalError(
                f"Community registry returned unexpected phase "
                f"{community_snapshot.initialization_phase.value!r} (expected SIMPLE)"
            )
        if community_snapshot.initialization_errors:
            raise InternalError(
                f"Community registry returned unexpected errors: "
                f"{community_snapshot.initialization_errors}"
            )

        for session in community_snapshot.items.values():
            self._items[session.full_name] = session

        self._phase = InitializationPhase.PARTIAL
        _LOGGER.info(f"[{self.__class__.__name__}] loaded {len(community_snapshot.items)} community sessions")

    @override
    async def initialize(self, config_manager: ConfigManager) -> None:
        """Initialize the registry and start background enterprise discovery.

        Phase 1 (under ``self._lock``): calls ``super().initialize()`` which
        calls ``_load_items`` — loads community sessions and sub-registries.

        Phase 2 (background task): discovers enterprise sessions from all
        configured factories in parallel.

        Idempotent — if ``initialize()`` has already been called, subsequent calls
        return immediately without restarting discovery.

        Args:
            config_manager: Configuration source.
        """
        await super().initialize(config_manager)

        async with self._lock:
            if self._discovery_task is not None:
                return
            self._discovery_task = asyncio.create_task(
                self._discover_enterprise_sessions()
            )

    @override
    async def close(self) -> None:
        """Shut down the registry and release all resources.

        Shutdown sequence (all steps are safe against concurrent callers):

        1. Under ``self._lock``: verify initialized, set ``_initialized=False``
           (gates all other operations immediately), grab the discovery task
           reference, and null out sub-registry refs so ``_snapshot_factory_state``
           sees ``None`` on its next lock-free read.
        2. Acquire ``_refresh_lock`` as a barrier — waits for any in-flight
           ``_sync_enterprise_sessions`` to finish before proceeding.
        3. Cancel and await the background discovery task (outside lock).
        4. Close sub-registries using the local refs captured in step 1.
        5. Under ``self._lock``: clear remaining mutable state and ``_items``.

        After this call the registry can be reinitialized via ``initialize()``.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        # Step 1: atomically gate all concurrent callers and grab refs.
        async with self._lock:
            self._check_initialized()
            self._initialized = False
            task = self._discovery_task
            self._discovery_task = None
            community = self._community_registry
            enterprise = self._enterprise_registry
            self._community_registry = None
            self._enterprise_registry = None

        # Step 2: barrier — wait for any in-flight _sync_enterprise_sessions.
        # _sync_enterprise_sessions holds _refresh_lock for its entire duration,
        # so acquiring it here guarantees no sync is mutating state when we proceed.
        async with self._refresh_lock:
            pass

        # Step 3: cancel the background task (outside lock to avoid deadlock).
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            _LOGGER.info(f"[{self.__class__.__name__}] cancelled background enterprise discovery")

        # Step 4: close sub-registries via local refs captured under the lock.
        if community is not None:
            try:
                await community.close()
            except Exception as e:
                _LOGGER.error(f"[{self.__class__.__name__}] error closing community registry: {e}")

        if enterprise is not None:
            try:
                await enterprise.close()
            except Exception as e:
                _LOGGER.error(f"[{self.__class__.__name__}] error closing enterprise registry: {e}")

        # Step 5: clear remaining state and collect items to close.
        # We do not call super().close() because _initialized is already False
        # (super().close() would fail _check_initialized()). Close _items inline.
        async with self._lock:
            self._controller_clients.clear()
            self._added_session_ids.clear()
            self._phase = InitializationPhase.NOT_STARTED
            self._errors = {}
            items_to_close = list(self._items.values())
            self._items.clear()

        # Close items outside the lock — close() may involve network I/O.
        for item in items_to_close:
            try:
                await item.close()
            except Exception as e:
                _LOGGER.error(f"[{self.__class__.__name__}] error closing item '{item.full_name}': {e}")

    # ------------------------------------------------------------------
    # BaseRegistry overrides — read interface
    # ------------------------------------------------------------------

    @override
    async def get(self, name: str) -> BaseItemManager:
        """Return the session manager for *name*, refreshing enterprise data if needed.

        For enterprise session names, triggers an on-demand refresh of the
        relevant factory before looking up the item.  For community sessions,
        no refresh is needed.

        Refresh only runs after initial discovery completes (``COMPLETED``
        phase); during ``LOADING`` or ``PARTIAL`` the background task is the
        sole writer and on-demand refresh is skipped.

        Args:
            name (str): Fully qualified session name in ``"type:source:name"``
                format (e.g. ``"enterprise:prod:my-pq"`` or
                ``"community:local:default"``).

        Returns:
            BaseItemManager: The session manager for *name*.

        Raises:
            InternalError: If the registry has not been initialized.
            InvalidSessionNameError: If *name* is not in ``type:source:name`` format.
            RegistryItemNotFoundError: If no session with *name* exists.
        """
        self._check_initialized()

        system_type, source, _ = BaseItemManager.parse_full_name(name)
        is_enterprise = system_type == SystemType.ENTERPRISE

        if is_enterprise:
            async with self._lock:
                phase = self._phase
            if phase == InitializationPhase.COMPLETED:
                await self._sync_enterprise_sessions([source])

        async with self._lock:
            self._check_initialized()
            if name not in self._items:
                raise RegistryItemNotFoundError(self._build_not_found_message(name))
            return self._items[name]

    @override
    async def get_all(self) -> RegistrySnapshot[BaseItemManager]:
        """Return an atomic snapshot of all sessions, refreshing enterprise data if needed.

        Triggers an on-demand refresh of all enterprise factories before
        returning the snapshot.  Refresh only runs after initial discovery
        completes (``COMPLETED`` phase); during ``LOADING`` or ``PARTIAL`` the
        snapshot reflects whatever sessions have been discovered so far.

        Returns:
            RegistrySnapshot[BaseItemManager]: Snapshot containing ``items``
                (all currently known sessions), ``initialization_phase``, and
                ``initialization_errors``.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        self._check_initialized()

        async with self._lock:
            phase = self._phase
            enterprise_registry = self._enterprise_registry

        if phase == InitializationPhase.COMPLETED and enterprise_registry is not None:
            factory_snapshot = await enterprise_registry.get_all()
            factory_names = list(factory_snapshot.items.keys())
            if factory_names:
                await self._sync_enterprise_sessions(factory_names)

        async with self._lock:
            self._check_initialized()
            return RegistrySnapshot.with_initialization(
                items=self._items.copy(),
                phase=self._phase,
                errors=self._errors.copy(),
            )

    # ------------------------------------------------------------------
    # Mutation interface
    # ------------------------------------------------------------------

    async def add_session(self, manager: BaseItemManager) -> None:
        """Add a session manager and mark it as MCP-created.

        The session is tracked in ``_added_session_ids`` so it is counted by
        ``count_added_sessions()`` and identified by ``is_added_session()``.

        Args:
            manager (BaseItemManager): Session manager to add.  Its
                ``full_name`` must not already exist in the registry.

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

    async def remove_session(self, session_id: str) -> BaseItemManager | None:
        """Remove a session manager from the registry.

        Idempotent — returns ``None`` if the session does not exist.  Also
        removes *session_id* from ``_added_session_ids`` if present.

        Args:
            session_id (str): Fully qualified session identifier in
                ``"type:source:name"`` format.

        Returns:
            BaseItemManager | None: The removed manager, or ``None`` if not
                found.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        async with self._lock:
            self._check_initialized()
            manager = self._items.pop(session_id, None)
            if manager is not None:
                self._added_session_ids.discard(session_id)
                _LOGGER.debug(f"[{self.__class__.__name__}] removed session '{session_id}'")
            return manager

    async def count_added_sessions(
        self, system_type: SystemType, system_name: str
    ) -> int:
        """Count MCP-created sessions for a specific system that still exist.

        Only counts sessions that are both tracked in ``_added_session_ids``
        *and* still present in ``_items`` (i.e. not yet removed).

        Args:
            system_type (SystemType): Session type to filter by (e.g.
                ``SystemType.ENTERPRISE`` or ``SystemType.COMMUNITY``).
            system_name (str): Source/system name to filter by (e.g.
                ``"prod-system"`` for enterprise, ``"dynamic"`` for community).

        Returns:
            int: Count of matching sessions that exist in the registry.

        Raises:
            InternalError: If the registry has not been initialized, or if a
                malformed session ID is found in the internal tracking set
                (indicates a programming bug).
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
                if s_type == system_type.value and s_source == system_name and sid in self._items:
                    count += 1
            return count

    async def is_added_session(self, session_id: str) -> bool:
        """Return ``True`` if *session_id* was added via ``add_session()``.

        Args:
            session_id (str): Fully qualified session identifier to check.

        Returns:
            bool: ``True`` if the session was added via ``add_session()`` and
                has not been removed, ``False`` otherwise.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        async with self._lock:
            self._check_initialized()
            return session_id in self._added_session_ids

    # ------------------------------------------------------------------
    # Private — enterprise refresh (four single-responsibility methods)
    # ------------------------------------------------------------------

    async def _sync_enterprise_sessions(self, factory_names: list[str]) -> None:
        """Refresh enterprise sessions for the given factories.

        Serialized by ``_refresh_lock`` so concurrent callers queue rather than
        duplicate work.  ``self._lock`` and ``_refresh_lock`` are never held
        simultaneously — ``self._lock`` is acquired briefly inside this method
        for fast state operations only.

        Phases:
            1. Snapshot state (``self._lock``, fast).
            2. Query each factory (no lock, network I/O, parallel).
            3. Apply results (``self._lock``, fast).
            4. Close stale managers (no lock).

        Args:
            factory_names: Factory names to refresh.
        """
        _LOGGER.debug(f"[{self.__class__.__name__}] refreshing {len(factory_names)} factory(ies): {factory_names}")
        async with self._refresh_lock:
            snapshots = await self._snapshot_factory_state(factory_names)

            # Factories that disappeared from the enterprise registry produce no
            # snapshot.  Synthesize a _FactoryQueryError for each so that
            # _apply_results removes any stale sessions they left behind.
            snapshot_names = {s.factory_name for s in snapshots}
            missing_errors: list[_FactoryQueryResult | _FactoryQueryError] = [
                _FactoryQueryError(
                    factory_name=name,
                    new_client=None,
                    error="factory no longer present in enterprise registry",
                )
                for name in factory_names
                if name not in snapshot_names
            ]

            raw = await asyncio.gather(
                *(_fetch_factory_pqs(s) for s in snapshots),
                return_exceptions=False,
            )
            results: list[_FactoryQueryResult | _FactoryQueryError] = list(raw) + missing_errors

            async with self._lock:
                managers_to_close = self._apply_results(results, snapshots)

        for manager in managers_to_close:
            try:
                await manager.close()
            except Exception as e:
                _LOGGER.warning(f"[{self.__class__.__name__}] error closing stale session '{manager.full_name}': {e}")

    async def _snapshot_factory_state(
        self, factory_names: list[str]
    ) -> list[_FactorySnapshot]:
        """Capture per-factory state needed for querying.

        Acquires ``self._lock`` briefly to atomically snapshot both
        ``_enterprise_registry`` and ``_controller_clients``, then calls
        ``get(name)`` on the enterprise registry for each factory outside
        the lock.  Factories that have disappeared are silently skipped.

        If ``_enterprise_registry`` is ``None`` (registry is closing), returns
        an empty list immediately.

        Args:
            factory_names (list[str]): Factory names to snapshot.

        Returns:
            list[_FactorySnapshot]: One snapshot per factory found in the
                enterprise registry.  Factories not found are silently skipped.
        """
        async with self._lock:
            enterprise_registry = self._enterprise_registry
            clients_snapshot = self._controller_clients.copy()

        if enterprise_registry is None:
            return []

        snapshots: list[_FactorySnapshot] = []
        for name in factory_names:
            try:
                factory_manager = await enterprise_registry.get(name)
            except RegistryItemNotFoundError:
                continue
            snapshots.append(
                _FactorySnapshot(
                    factory_name=name,
                    factory_manager=factory_manager,
                    client=clients_snapshot.get(name),
                )
            )
        return snapshots

    def _get_factory_keys(self, factory_name: str) -> set[str]:
        """Return all ``_items`` keys belonging to *factory_name*.

        Filters on both ``system_type == ENTERPRISE`` and ``source == factory_name``
        so community sessions with the same source name are excluded.

        Synchronous — no ``await``.  Must be called under ``self._lock``.

        Args:
            factory_name: The enterprise factory name to match.

        Returns:
            Set of full-name keys in ``_items`` that belong to this factory.

        Raises:
            InternalError: If any key in ``_items`` is malformed (indicates
                a programming bug — keys must always be valid full names).
        """
        keys: set[str] = set()
        for key in self._items:
            try:
                system_type, source, _ = BaseItemManager.parse_full_name(key)
            except InvalidSessionNameError as e:
                raise InternalError(
                    f"Malformed key {key!r} found in _items: {e}"
                ) from e
            if system_type == SystemType.ENTERPRISE.value and source == factory_name:
                keys.add(key)
        return keys

    def _remove_factory_sessions_by_keys(
        self, keys: set[str]
    ) -> list[BaseItemManager]:
        """Remove a specific set of session keys from ``_items``.

        Synchronous — no ``await``.  Must be called under ``self._lock``.
        Keeps ``_added_session_ids`` consistent with ``_items``.

        Args:
            keys: Full-name keys to remove.  Keys not present in ``_items``
                are silently ignored.

        Returns:
            Removed managers; caller must close them outside the lock.
        """
        managers_to_close: list[BaseItemManager] = []
        for key in keys:
            mgr = self._items.pop(key, None)
            if mgr is not None:
                self._added_session_ids.discard(key)
                managers_to_close.append(mgr)
        return managers_to_close

    def _remove_factory_sessions(
        self, factory_name: str
    ) -> list[BaseItemManager]:
        """Remove all sessions for *factory_name* from ``_items``.

        Synchronous — no ``await``.  Must be called under ``self._lock``.

        Args:
            factory_name: The enterprise factory whose sessions should be removed.

        Returns:
            Removed managers; caller must close them outside the lock.

        Raises:
            InternalError: If any key in ``_items`` is malformed (indicates
                a programming bug — keys must always be valid full names).
        """
        return self._remove_factory_sessions_by_keys(self._get_factory_keys(factory_name))

    def _apply_factory_success(
        self,
        result: _FactoryQueryResult,
        factory_manager: CorePlusSessionFactoryManager,
    ) -> list[BaseItemManager]:
        """Reconcile ``_items`` with a successful controller query for one factory.

        Synchronous — no ``await``.  Must be called under ``self._lock``.

        - Caches the live client returned by the query.
        - Adds sessions the controller reports that we do not yet have.
        - Removes sessions we have that the controller no longer reports.
        - Clears any previous error recorded for this factory.

        Args:
            result: Successful query result containing the current PQ names.
            factory_manager: Factory manager used to create new session managers.
                Must not be ``None`` — a ``_FactoryQueryResult`` is only produced
                by ``_fetch_factory_pqs``, which always has a corresponding
                snapshot and therefore a valid factory manager.

        Returns:
            Managers removed as stale; caller must close them outside the lock.
        """
        name = result.factory_name

        # Cache the live client returned by this successful query.
        self._controller_clients[name] = result.new_client

        # All keys currently in _items that belong to this factory.
        existing_keys = self._get_factory_keys(name)

        # Full set of keys the controller currently reports.
        controller_keys = {
            BaseItemManager.make_full_name(SystemType.ENTERPRISE, name, n)
            for n in result.query_names
        }

        # Set-difference gives us exactly what to add and what to remove.
        keys_to_add = controller_keys - existing_keys
        keys_to_remove = existing_keys - controller_keys

        # Add sessions the controller knows about that we don't have yet.
        for full_key in keys_to_add:
            _, _, session_name = BaseItemManager.parse_full_name(full_key)
            mgr = self._make_enterprise_session_manager(factory_manager, name, session_name)
            self._items[mgr.full_name] = mgr

        # Remove stale sessions and collect them for closing.
        managers_to_close = self._remove_factory_sessions_by_keys(keys_to_remove)

        # Clear any previous error for this factory — query succeeded.
        self._errors.pop(name, None)

        if keys_to_add:
            _LOGGER.debug(f"[{self.__class__.__name__}] factory '{name}': added {len(keys_to_add)} sessions")
        if keys_to_remove:
            _LOGGER.debug(f"[{self.__class__.__name__}] factory '{name}': removed {len(keys_to_remove)} stale sessions")

        return managers_to_close

    def _apply_factory_error(
        self,
        result: _FactoryQueryError,
    ) -> list[BaseItemManager]:
        """Record a failed controller query and remove all sessions for the factory.

        Synchronous — no ``await``.  Must be called under ``self._lock``.

        - Updates ``_controller_clients``: caches the new client if one was
          created before the failure, or evicts the dead cached client so
          the next refresh creates a fresh connection.
        - Records the error in ``_errors`` for surfacing via ``get_all()``.
        - Removes all sessions for the factory from ``_items``.

        Args:
            result: Failed query result containing the factory name, error
                message, and optionally a newly created client.

        Returns:
            Managers removed; caller must close them outside the lock.
        """
        name = result.factory_name

        # Update the client cache.  If a new client was created before the
        # failure (e.g., map() failed after a fresh connect), cache it so the
        # next refresh can reuse it.  If no new client was created, evict the
        # dead cached client so the next refresh creates a fresh connection.
        if result.new_client is not None:
            self._controller_clients[name] = result.new_client
        else:
            self._controller_clients.pop(name, None)

        # Record the error so callers can surface it via get_all().
        self._errors[name] = result.error

        managers_to_close = self._remove_factory_sessions(name)

        _LOGGER.warning(f"[{self.__class__.__name__}] factory '{name}' query failed: {result.error}")

        return managers_to_close

    def _apply_results(
        self,
        results: list[_FactoryQueryResult | _FactoryQueryError],
        snapshots: list[_FactorySnapshot],
    ) -> list[BaseItemManager]:
        """Apply query results to registry state.

        Synchronous — no ``await``.  Must be called under ``self._lock``.
        Dispatches each result to ``_apply_factory_success`` or
        ``_apply_factory_error`` and collects managers to close.

        Args:
            results: One result per factory — includes both real query results
                from ``_fetch_factory_pqs`` and synthesized ``_FactoryQueryError``
                entries for factories that disappeared between snapshot and apply.
            snapshots: Snapshots from Phase 1 (one per factory that existed at
                snapshot time).  Used to look up ``factory_manager`` for
                ``_FactoryQueryResult`` entries; synthesized error entries have
                no corresponding snapshot.

        Returns:
            Managers that should be closed by the caller (outside the lock).
        """
        factory_mgr_by_name = {s.factory_name: s.factory_manager for s in snapshots}
        managers_to_close: list[BaseItemManager] = []

        for result in results:
            if isinstance(result, _FactoryQueryResult):
                factory_manager = factory_mgr_by_name.get(result.factory_name)
                if factory_manager is None:
                    raise InternalError(
                        f"No snapshot found for successful result from factory "
                        f"'{result.factory_name}'; this indicates a programming bug"
                    )
                managers_to_close += self._apply_factory_success(result, factory_manager)
            elif isinstance(result, _FactoryQueryError):
                managers_to_close += self._apply_factory_error(result)
            else:
                raise InternalError(
                    f"Unexpected result type {type(result).__name__!r} for factory '{result.factory_name}'"
                )

        return managers_to_close

    # ------------------------------------------------------------------
    # Private — background discovery task
    # ------------------------------------------------------------------

    async def _discover_enterprise_sessions(self) -> None:
        """One-shot background task: discover enterprise sessions at startup.

        Sets ``_phase`` to ``LOADING``, calls ``_sync_enterprise_sessions`` for
        all configured factories, then sets ``_phase`` to ``COMPLETED``.

        On ``CancelledError`` (from ``close()``), sets ``_phase`` to ``FAILED``
        and re-raises.
        """
        start = time.monotonic()
        _LOGGER.info(f"[{self.__class__.__name__}] starting enterprise session discovery")

        try:
            # Set LOADING and read _enterprise_registry atomically under the lock.
            # close() may null out _enterprise_registry concurrently, so both
            # operations must be in the same critical section.
            async with self._lock:
                self._phase = InitializationPhase.LOADING
                enterprise_registry = self._enterprise_registry
            if enterprise_registry is not None:
                factory_snapshot = await enterprise_registry.get_all()
                factory_names = list(factory_snapshot.items.keys())
                if factory_names:
                    await self._sync_enterprise_sessions(factory_names)

            elapsed = time.monotonic() - start
            _LOGGER.info(f"[{self.__class__.__name__}] enterprise discovery completed in {elapsed:.2f}s")

            async with self._lock:
                self._phase = InitializationPhase.COMPLETED
                if self._errors:
                    _LOGGER.warning(f"[{self.__class__.__name__}] discovery completed with errors: {self._errors}")

        except asyncio.CancelledError:
            async with self._lock:
                self._phase = InitializationPhase.FAILED
            _LOGGER.info(f"[{self.__class__.__name__}] enterprise discovery cancelled (shutdown)")
            raise

        except Exception as e:
            elapsed = time.monotonic() - start
            _LOGGER.error(
                f"[{self.__class__.__name__}] enterprise discovery failed in {elapsed:.2f}s: {e}",
                exc_info=True,
            )
            async with self._lock:
                self._errors["enterprise_discovery"] = f"{type(e).__name__}: {e}"
                self._phase = InitializationPhase.COMPLETED

    # ------------------------------------------------------------------
    # Private — error message helper
    # ------------------------------------------------------------------

    def _build_not_found_message(self, name: str) -> str:
        """Build a ``RegistryItemNotFoundError`` message with context.

        Must be called while holding ``self._lock``.

        Args:
            name: The fully qualified session name that was not found.
                Must be a valid ``type:source:name`` string — callers are
                responsible for validating via ``parse_full_name`` before
                calling this method.

        Returns:
            Error message string.

        Raises:
            InternalError: If *name* is not in ``type:source:name`` format
                (indicates a programming bug in the caller).
        """
        msg = f"No item with name '{name}' found in {self.__class__.__name__}"
        notes: list[str] = []

        if self._phase == InitializationPhase.LOADING:
            notes.append(
                "enterprise session discovery is still in progress — "
                "the session may appear shortly"
            )
        elif self._phase in (
            InitializationPhase.NOT_STARTED,
            InitializationPhase.PARTIAL,
            InitializationPhase.FAILED,
        ):
            notes.append(
                f"enterprise session discovery has not completed "
                f"(phase: {self._phase.value})"
            )

        if self._errors:
            try:
                _, factory_name, _ = BaseItemManager.parse_full_name(name)
            except InvalidSessionNameError as e:
                raise InternalError(
                    f"_build_not_found_message called with malformed name {name!r}: {e}"
                ) from e

            if factory_name in self._errors:
                notes.append(
                    f"factory '{factory_name}' had an error: "
                    f"{self._errors[factory_name]}"
                )
            else:
                notes.append(
                    f"initialization errors were detected for "
                    f"{len(self._errors)} factory(ies): "
                    + "; ".join(f"{k}: {v}" for k, v in self._errors.items())
                )

        if notes:
            msg += " Note: " + "; ".join(notes) + "."
        return msg
