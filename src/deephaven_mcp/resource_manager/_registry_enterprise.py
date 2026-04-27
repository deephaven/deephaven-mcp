"""Enterprise session registry for the DHE MCP server.

This module provides ``EnterpriseSessionRegistry``, a purpose-built registry for the
DHE server that manages exactly one enterprise system (factory) per instance.

Architecture
------------
``EnterpriseSessionRegistry`` inherits the ``_items`` dict, ``_lock``, and
``_initialized`` flag from ``BaseRegistry``.  It adds:

- ``_factory_manager`` — the single ``CorePlusSessionFactoryManager`` for this instance.
- ``_controller_client`` — cached controller client for the factory.
- ``_added_session_ids`` — tracks sessions added via ``add_session()`` for counting.
- ``_phase`` / ``_errors`` — enterprise discovery lifecycle state.
- ``_discovery_task`` — background task for initial enterprise discovery.
- ``_refresh_lock`` — serializes concurrent enterprise refresh operations.

Locking contract (strict ordering, no exceptions)
--------------------------------------------------
``self._lock``    — protects all mutable state; held only for fast operations.
``_refresh_lock`` — serializes enterprise refresh.

Lock ordering rule: ``_refresh_lock`` is always the **outer** lock.
It is permitted to acquire ``self._lock`` briefly while already holding
``_refresh_lock`` (phases 1 and 3 of the refresh do exactly this).
The reverse — acquiring ``_refresh_lock`` while holding ``self._lock`` —
is **never** allowed, as it would risk deadlock.

Enterprise refresh is a four-phase operation:

1. **Snapshot**: acquire ``self._lock`` briefly to read the factory manager and
   cached client.
2. **Query**    (no lock): network I/O via ``_fetch_factory_pqs``.
3. **Apply**    (``self._lock``): mutate ``_items``/caches, collect managers to close.
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
    RegistryItemNotFoundError,
)
from deephaven_mcp.client import CorePlusControllerClient, CorePlusSession
from deephaven_mcp.config import ConfigManager

from ._manager import (
    BaseItemManager,
    CorePlusSessionFactoryManager,
    EnterpriseSessionManager,
    SystemType,
)
from ._registry import InitializationPhase, MutableSessionRegistry, RegistrySnapshot

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level result types for the factory query pipeline
# ---------------------------------------------------------------------------


@dataclass
class _FactorySnapshot:
    """State captured under ``self._lock`` for the factory before querying.

    Attributes:
        factory_manager (CorePlusSessionFactoryManager): Manager used to obtain
            a connected factory instance and its controller client.
        client (CorePlusControllerClient | None): Cached controller client from
            the previous refresh cycle, or ``None`` if this is the first query.
    """

    factory_manager: CorePlusSessionFactoryManager
    client: CorePlusControllerClient | None


@dataclass
class _FactoryQueryResult:
    """Successful result from querying the factory's controller.

    Attributes:
        new_client (CorePlusControllerClient): The live client used for this
            query — either the cached client (if ping succeeded) or a freshly
            created one (if the cached client was dead or absent).
        query_names (set[str]): Names of all persistent queries currently
            reported by the controller.
    """

    new_client: CorePlusControllerClient
    query_names: set[str]


@dataclass
class _FactoryQueryError:
    """Failed result from querying the factory's controller.

    Attributes:
        new_client (CorePlusControllerClient | None): A freshly created client
            if one was obtained before the failure, or ``None`` if no new client
            was created.
        error (str): Human-readable error description in ``"ExcType: message"``
            format.
    """

    new_client: CorePlusControllerClient | None
    error: str


# ---------------------------------------------------------------------------
# Module-level pure I/O function — no shared state
# ---------------------------------------------------------------------------


async def _fetch_factory_pqs(
    snapshot: _FactorySnapshot,
) -> _FactoryQueryResult | _FactoryQueryError:
    """Query the enterprise factory's controller for its current PQ list.

    Pure I/O function — accesses no shared registry state.

    Algorithm:
        1. If no cached client, create one via ``factory_manager.get()``.
        2. If cached client exists, ping to verify liveness; recreate if dead.
        3. Call ``map()`` to get the current PQ list.

    Args:
        snapshot (_FactorySnapshot): Factory state captured in Phase 1.

    Returns:
        ``_FactoryQueryResult`` on success, ``_FactoryQueryError`` on failure.
    """
    client = snapshot.client
    new_client: CorePlusControllerClient | None = None

    try:
        if client is None:
            _LOGGER.debug("[_fetch_factory_pqs] no cached client, creating")
            t0 = time.monotonic()
            factory_instance = await snapshot.factory_manager.get()
            client = factory_instance.controller_client
            new_client = client
            _LOGGER.debug(
                f"[_fetch_factory_pqs] client created in {time.monotonic()-t0:.2f}s"
            )
        else:
            try:
                _LOGGER.debug("[_fetch_factory_pqs] pinging cached client")
                t0 = time.monotonic()
                ping_ok = await client.ping()
                _LOGGER.debug(
                    f"[_fetch_factory_pqs] ping={'ok' if ping_ok else 'False'} in {time.monotonic()-t0:.2f}s"
                )
                if not ping_ok:
                    raise RuntimeError("ping() returned False")
            except Exception as ping_err:
                _LOGGER.warning(
                    f"[_fetch_factory_pqs] cached controller client failed liveness check "
                    f"({type(ping_err).__name__}: {ping_err}); discarding and recreating"
                )
                t0 = time.monotonic()
                factory_instance = await snapshot.factory_manager.get()
                client = factory_instance.controller_client
                new_client = client
                _LOGGER.debug(
                    f"[_fetch_factory_pqs] client recreated in {time.monotonic()-t0:.2f}s"
                )

        _LOGGER.debug("[_fetch_factory_pqs] calling map()")
        t0 = time.monotonic()
        query_map = await client.map()
        _LOGGER.debug(
            f"[_fetch_factory_pqs] map() returned {len(query_map)} entries in {time.monotonic()-t0:.2f}s"
        )
        query_names = {info.config.pb.name for info in query_map.values()}
        _LOGGER.debug(f"[_fetch_factory_pqs] {len(query_names)} PQs")
        return _FactoryQueryResult(
            new_client=client,
            query_names=query_names,
        )

    except Exception as e:
        _LOGGER.warning(f"[_fetch_factory_pqs] factory query failed: {e}")
        return _FactoryQueryError(
            new_client=new_client,
            error=f"{type(e).__name__}: {e}",
        )


class EnterpriseSessionRegistry(MutableSessionRegistry):
    """Purpose-built registry for the DHE MCP server.

    Manages exactly one enterprise system (configured via ``system_name``).  Discovers
    enterprise PQ sessions asynchronously from the controller client and supports
    mutation methods for MCP-created sessions.

    See module docstring for the full locking contract.

    Usage::

        registry = EnterpriseSessionRegistry()
        await registry.initialize(config_manager)  # config_manager returns flat DHE config
        session_mgr = await registry.get("enterprise:system:my-pq")
        factory = registry.factory_manager
        await registry.close()
    """

    @staticmethod
    def _make_enterprise_session_manager(
        factory: CorePlusSessionFactoryManager,
        session_name: str,
        system_name: str,
    ) -> EnterpriseSessionManager:
        """Create an ``EnterpriseSessionManager`` that lazily connects to a PQ.

        Args:
            factory (CorePlusSessionFactoryManager): Factory manager used to
                obtain a connected factory instance.
            session_name (str): PQ name to connect to.
            system_name (str): Enterprise system name used as the session source.

        Returns:
            An ``EnterpriseSessionManager`` whose creation function calls
            ``factory.get()`` then ``connect_to_persistent_query(session_name)``.
        """

        async def creation_function(source: str, name: str) -> CorePlusSession:
            factory_instance = await factory.get()
            return await factory_instance.connect_to_persistent_query(name)

        return EnterpriseSessionManager(
            source=system_name,
            name=session_name,
            creation_function=creation_function,
        )

    def __init__(self) -> None:
        """Initialize the registry.  Call ``await initialize()`` before use."""
        super().__init__()
        self._system_name: str = ""
        self._factory_manager: CorePlusSessionFactoryManager | None = None
        self._controller_client: CorePlusControllerClient | None = None
        self._phase: InitializationPhase = InitializationPhase.NOT_STARTED
        self._error: str | None = None
        self._discovery_task: asyncio.Task[None] | None = None
        self._refresh_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public property
    # ------------------------------------------------------------------

    @property
    def system_name(self) -> str:
        """Return the enterprise system name.

        This value comes from the ``system_name`` field in the flat DHE config and
        appears as the source segment in all enterprise session identifiers
        (e.g. ``"enterprise:<system_name>:<pq-name>"``).

        Returns:
            str: The configured system name.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        self._check_initialized()
        return self._system_name

    @property
    def factory_manager(self) -> CorePlusSessionFactoryManager:
        """Return the enterprise factory manager.

        Returns:
            CorePlusSessionFactoryManager: The single factory manager for this DHE server.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        self._check_initialized()
        if self._factory_manager is None:
            raise InternalError(
                f"{self.__class__.__name__} factory manager is not available"
            )
        return self._factory_manager

    # ------------------------------------------------------------------
    # BaseRegistry overrides — lifecycle
    # ------------------------------------------------------------------

    @override
    async def _load_items(self, config_manager: ConfigManager) -> None:
        """Create the single enterprise factory manager from flat config.

        Called by ``super().initialize()`` while holding ``self._lock``.

        Args:
            config_manager (ConfigManager): Must be an ``EnterpriseServerConfigManager``; its
                ``get_config()`` returns the flat enterprise system config dict.
        """
        flat_config = await config_manager.get_config()
        self._system_name = flat_config["system_name"]
        self._factory_manager = CorePlusSessionFactoryManager(
            self._system_name, flat_config
        )
        self._phase = InitializationPhase.PARTIAL
        _LOGGER.info(
            f"[{self.__class__.__name__}] factory manager created for '{self._system_name}'"
        )

    @override
    async def initialize(self, config_manager: ConfigManager) -> None:
        """Initialize the registry and start background enterprise discovery.

        Phase 1 (under ``self._lock``): calls ``super().initialize()`` which
        calls ``_load_items`` — creates the factory manager.

        Phase 2 (background task): discovers enterprise PQ sessions from the
        factory controller.

        Idempotent — subsequent calls return immediately without restarting discovery.

        Args:
            config_manager (ConfigManager): Configuration source.
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

        Shutdown sequence:

        1. Under ``self._lock``: verify initialized, set ``_initialized=False``,
           grab the discovery task reference and factory manager, null them out.
        2. Acquire ``_refresh_lock`` as a barrier — waits for any in-flight
           ``_sync_enterprise_sessions`` to finish before proceeding.
        3. Cancel and await the background discovery task (outside lock).
        4. Close the factory manager using the local ref captured in step 1.
        5. Under ``self._lock``: clear remaining mutable state and ``_items``.
        6. Close remaining session managers (outside lock) via ``_close_items``.

        After this call the registry can be reinitialized via ``initialize()``.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        # Step 1: gate all concurrent callers and grab refs.
        async with self._lock:
            self._check_initialized()
            self._initialized = False
            task = self._discovery_task
            self._discovery_task = None
            factory = self._factory_manager
            self._factory_manager = None

        # Step 2: barrier — wait for any in-flight _sync_enterprise_sessions.
        async with self._refresh_lock:
            pass

        # Step 3: cancel the background task (outside lock to avoid deadlock).
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            _LOGGER.info(
                f"[{self.__class__.__name__}] cancelled background enterprise discovery"
            )

        # Step 4: close factory manager via local ref captured under the lock.
        if factory is not None:
            try:
                await factory.close()
            except Exception as e:
                _LOGGER.error(
                    f"[{self.__class__.__name__}] error closing factory manager: {e}"
                )

        # Step 5: clear remaining state and collect items to close.
        async with self._lock:
            self._controller_client = None
            self._added_session_ids.clear()
            self._phase = InitializationPhase.NOT_STARTED
            self._error = None
            items_to_close = list(self._items.values())
            self._items.clear()

        # Step 6: close items outside the lock via the inherited hook.
        # NOTE: super().close() is NOT called here. The _refresh_lock barrier
        # (step 2) must be acquired after _initialized is set to False (step 1)
        # and before _items is cleared (step 5). That ordering cannot be
        # preserved if super().close() controls both the lock acquisition and
        # the _check_initialized() gate.
        await self._close_items(items_to_close)

        _LOGGER.info(f"[{self.__class__.__name__}] closed")

    # ------------------------------------------------------------------
    # BaseRegistry overrides — read interface
    # ------------------------------------------------------------------

    async def _check_and_sync(self) -> None:
        """Verify initialized and trigger an enterprise sync if in COMPLETED phase.

        Shared preamble for :meth:`get` and :meth:`get_all`.  Callers must
        re-check ``_check_initialized()`` under ``self._lock`` after this returns,
        since a concurrent ``close()`` could have run during the sync.
        """
        self._check_initialized()
        async with self._lock:
            phase = self._phase
        if phase == InitializationPhase.COMPLETED:
            await self._sync_enterprise_sessions()

    @override
    async def get(self, name: str) -> BaseItemManager:
        """Return the session manager for *name*, refreshing enterprise data if needed.

        Triggers an on-demand refresh before looking up the item once initial
        discovery completes (``COMPLETED`` phase).  During ``LOADING`` or
        ``PARTIAL`` the background task is the sole writer and refresh is skipped.

        Args:
            name (str): Fully qualified session name in ``"type:source:name"``
                format (e.g. ``"enterprise:system:my-pq"``).

        Returns:
            BaseItemManager: The session manager for *name*.

        Raises:
            InternalError: If the registry has not been initialized.
            InvalidSessionNameError: If *name* is not in ``type:source:name`` format.
            RegistryItemNotFoundError: If no session with *name* exists.
        """
        _LOGGER.debug(
            f"[{self.__class__.__name__}:get] enterprise sync starting for '{name}'"
        )
        await self._check_and_sync()
        _LOGGER.debug(
            f"[{self.__class__.__name__}:get] enterprise sync complete for '{name}'"
        )

        async with self._lock:
            self._check_initialized()
            if name not in self._items:
                raise RegistryItemNotFoundError(self._build_not_found_message(name))
            return self._items[name]

    @override
    async def get_all(self) -> RegistrySnapshot[BaseItemManager]:
        """Return an atomic snapshot of all sessions, refreshing enterprise data if needed.

        Returns:
            RegistrySnapshot[BaseItemManager]: Snapshot containing ``items``,
                ``initialization_phase``, and ``initialization_errors``.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        await self._check_and_sync()

        async with self._lock:
            self._check_initialized()
            return RegistrySnapshot.with_initialization(
                items=self._items.copy(),
                phase=self._phase,
                errors={"error": self._error} if self._error else {},
            )

    # ------------------------------------------------------------------
    # Private — enterprise refresh (four single-responsibility phases)
    # ------------------------------------------------------------------

    async def _sync_enterprise_sessions(self) -> None:
        """Refresh enterprise sessions for the single configured factory.

        Serialized by ``_refresh_lock`` so concurrent callers queue rather than
        duplicate work.

        Phases:
            1. Snapshot state (``self._lock``, fast).
            2. Query factory (no lock, network I/O).
            3. Apply results (``self._lock``, fast).
            4. Close stale managers (no lock).
        """
        _LOGGER.debug(
            f"[{self.__class__.__name__}:_sync_enterprise_sessions] waiting for _refresh_lock"
        )
        async with self._refresh_lock:
            _LOGGER.debug(
                f"[{self.__class__.__name__}:_sync_enterprise_sessions] acquired _refresh_lock"
            )
            snapshot = await self._snapshot_factory_state()
            if snapshot is None:
                return

            t0 = time.monotonic()
            result = await _fetch_factory_pqs(snapshot)
            _LOGGER.debug(
                f"[{self.__class__.__name__}:_sync_enterprise_sessions] factory query completed in {time.monotonic()-t0:.2f}s"
            )

            async with self._lock:
                managers_to_close = self._apply_result(result, snapshot.factory_manager)

        for manager in managers_to_close:
            try:
                await manager.close()
            except Exception as e:
                _LOGGER.warning(
                    f"[{self.__class__.__name__}] error closing stale session '{manager.full_name}': {e}"
                )

    async def _snapshot_factory_state(self) -> _FactorySnapshot | None:
        """Capture factory state needed for querying.

        Acquires ``self._lock`` briefly to atomically snapshot ``_factory_manager``
        and ``_controller_client``.  Returns ``None`` if factory is not available
        (registry is closing).

        Returns:
            ``_FactorySnapshot`` if factory is available, ``None`` otherwise.
        """
        async with self._lock:
            factory_manager = self._factory_manager
            client = self._controller_client
        if factory_manager is None:
            return None
        return _FactorySnapshot(factory_manager=factory_manager, client=client)

    def _apply_result(
        self,
        result: _FactoryQueryResult | _FactoryQueryError,
        factory_manager: CorePlusSessionFactoryManager,
    ) -> list[BaseItemManager]:
        """Apply a query result to registry state.

        Synchronous — no ``await``.  Must be called under ``self._lock``.

        Args:
            result (_FactoryQueryResult | _FactoryQueryError): Query result from ``_fetch_factory_pqs``.
            factory_manager (CorePlusSessionFactoryManager): Factory manager for creating new session managers.

        Returns:
            Managers that should be closed by the caller (outside the lock).
        """
        if isinstance(result, _FactoryQueryResult):
            return self._apply_factory_success(result, factory_manager)
        elif isinstance(result, _FactoryQueryError):
            return self._apply_factory_error(result)
        else:
            raise InternalError(f"Unexpected result type {type(result).__name__!r}")

    def _remove_sessions_by_keys(self, keys: set[str]) -> list[BaseItemManager]:
        """Remove a specific set of session keys from ``_items``.

        Synchronous — no ``await``.  Must be called under ``self._lock``.
        Keeps ``_added_session_ids`` consistent with ``_items``.

        Args:
            keys (set[str]): Full-name keys to remove.

        Returns:
            list[BaseItemManager]: Removed managers; caller must close them.
        """
        managers_to_close: list[BaseItemManager] = []
        for key in keys:
            mgr = self._items.pop(key, None)
            if mgr is not None:
                self._added_session_ids.discard(key)
                managers_to_close.append(mgr)
        return managers_to_close

    def _apply_factory_success(
        self,
        result: _FactoryQueryResult,
        factory_manager: CorePlusSessionFactoryManager,
    ) -> list[BaseItemManager]:
        """Reconcile ``_items`` with a successful controller query.

        Synchronous — no ``await``.  Must be called under ``self._lock``.

        - Caches the live client returned by the query.
        - Adds PQ sessions the controller reports that we do not yet have.
        - Removes sessions we have that the controller no longer reports.
        - Clears any previous error.

        Args:
            result (_FactoryQueryResult): Successful query result.
            factory_manager (CorePlusSessionFactoryManager): Factory manager for
                creating new session managers.

        Returns:
            list[BaseItemManager]: Managers removed as stale; caller must close them.
        """
        self._controller_client = result.new_client

        existing_keys = set(self._items.keys())
        controller_keys = {
            BaseItemManager.make_full_name(SystemType.ENTERPRISE, self._system_name, n)
            for n in result.query_names
        }

        keys_to_add = controller_keys - existing_keys
        keys_to_remove = existing_keys - controller_keys

        for full_key in keys_to_add:
            _, _, session_name = BaseItemManager.parse_full_name(full_key)
            mgr = self._make_enterprise_session_manager(
                factory_manager, session_name, self._system_name
            )
            self._items[mgr.full_name] = mgr

        managers_to_close = self._remove_sessions_by_keys(keys_to_remove)

        self._error = None

        if keys_to_add:
            _LOGGER.debug(
                f"[{self.__class__.__name__}] added {len(keys_to_add)} sessions"
            )
        if keys_to_remove:
            _LOGGER.debug(
                f"[{self.__class__.__name__}] removed {len(keys_to_remove)} stale sessions"
            )

        return managers_to_close

    def _apply_factory_error(
        self,
        result: _FactoryQueryError,
    ) -> list[BaseItemManager]:
        """Record a failed controller query and remove all enterprise sessions.

        Synchronous — no ``await``.  Must be called under ``self._lock``.

        - Updates ``_controller_client``.
        - Records the error in ``_error``.
        - Removes all enterprise sessions from ``_items``.

        Args:
            result (_FactoryQueryError): Failed query result.

        Returns:
            list[BaseItemManager]: Removed managers; caller must close them.
        """
        if result.new_client is not None:
            self._controller_client = result.new_client
        else:
            self._controller_client = None

        self._error = result.error

        managers_to_close = self._remove_sessions_by_keys(set(self._items.keys()))

        _LOGGER.warning(
            f"[{self.__class__.__name__}] factory query failed: {result.error}"
        )

        return managers_to_close

    # ------------------------------------------------------------------
    # Private — background discovery task
    # ------------------------------------------------------------------

    async def _discover_enterprise_sessions(self) -> None:
        """One-shot background task: discover enterprise sessions at startup.

        Sets ``_phase`` to ``LOADING``, calls ``_sync_enterprise_sessions``,
        then sets ``_phase`` to ``COMPLETED``.

        On ``CancelledError`` (from ``close()``), sets ``_phase`` to ``FAILED``
        and re-raises.
        """
        start = time.monotonic()
        _LOGGER.info(
            f"[{self.__class__.__name__}] starting enterprise session discovery"
        )

        try:
            async with self._lock:
                self._phase = InitializationPhase.LOADING

            await self._sync_enterprise_sessions()

            elapsed = time.monotonic() - start
            _LOGGER.info(
                f"[{self.__class__.__name__}] enterprise discovery completed in {elapsed:.2f}s"
            )

            async with self._lock:
                self._phase = InitializationPhase.COMPLETED
                if self._error:
                    _LOGGER.warning(
                        f"[{self.__class__.__name__}] discovery completed with error: {self._error}"
                    )

        except asyncio.CancelledError:
            async with self._lock:
                self._phase = InitializationPhase.FAILED
            _LOGGER.info(
                f"[{self.__class__.__name__}] enterprise discovery cancelled (shutdown)"
            )
            raise

        except Exception as e:
            elapsed = time.monotonic() - start
            _LOGGER.error(
                f"[{self.__class__.__name__}] enterprise discovery failed in {elapsed:.2f}s: {e}",
                exc_info=True,
            )
            async with self._lock:
                self._error = f"{type(e).__name__}: {e}"
                self._phase = InitializationPhase.COMPLETED

    # ------------------------------------------------------------------
    # Private — error message helper
    # ------------------------------------------------------------------

    def _build_not_found_message(self, name: str) -> str:
        """Build a ``RegistryItemNotFoundError`` message with context.

        Must be called while holding ``self._lock``.

        Args:
            name (str): The fully qualified session name that was not found.

        Returns:
            str: Error message string.
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

        if self._error:
            notes.append(f"factory error: {self._error}")

        if notes:
            msg += " Note: " + "; ".join(notes) + "."
        return msg
