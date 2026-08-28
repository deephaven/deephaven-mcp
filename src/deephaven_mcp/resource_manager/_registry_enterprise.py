"""Enterprise session registry for one Deephaven Enterprise system.

This module provides :class:`EnterpriseSessionRegistry`, a purpose-built
registry that manages exactly one enterprise system (factory) per
instance. Configuration and credentials are pre-resolved by the
multi-system config manager and supplied to the registry at construction
time; the registry no longer participates in any per-request credential
flow.

Lifecycle
---------
The registry is created with an :class:`~deephaven_mcp.sessions.EnterpriseSystemConfig`
that already carries the resolved :class:`~deephaven_mcp.auth.credentials.Credentials`.
:meth:`initialize` builds the :class:`CorePlusSessionFactoryManager`,
transitions to :attr:`InitializationPhase.PARTIAL`, and launches the
background discovery task that populates the registry from the
controller client.

Architecture
------------
``EnterpriseSessionRegistry`` inherits the ``_items`` dict, ``_lock``, and
``_initialized`` flag from ``BaseRegistry``, and ``_added_session_ids``
(tracking sessions added via ``add_session()`` for counting) from
``MutableSessionRegistry``.  It adds:

- ``_factory_manager`` — the single ``CorePlusSessionFactoryManager`` for this instance.
- ``_controller_client`` — cached controller client for the factory.
- ``_phase`` / ``_error`` — enterprise discovery lifecycle state.
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
import time
from dataclasses import dataclass
from typing import override

from deephaven_mcp._exception_utils import exception_summary
from deephaven_mcp._exceptions import (
    InternalError,
    RegistryItemNotFoundError,
)
from deephaven_mcp._taxonomy import SessionOrigin
from deephaven_mcp.client import (
    WEB_CLIENT_DATA_PQ,
    CorePlusControllerClient,
    CorePlusQuerySerial,
    CorePlusSession,
    EnterpriseClientTimeouts,
)
from deephaven_mcp.sessions import EnterpriseSystemConfig

from ._manager import (
    BaseItemManager,
    CorePlusSessionFactoryManager,
    EnterpriseSessionManager,
    SessionManager,
    SystemType,
)
from ._registry import (
    InitializationPhase,
    MutableSessionRegistry,
    RegistrySnapshot,
)
from ._session_id import QualifiedSessionId, SessionId

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
        query_map (dict[CorePlusQuerySerial, str]): Mapping from PQ serial to
            display name for every persistent query currently reported by the
            controller.
    """

    new_client: CorePlusControllerClient
    query_map: dict[CorePlusQuerySerial, str]


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
        raw_map = await client.map()
        _LOGGER.debug(
            f"[_fetch_factory_pqs] map() returned {len(raw_map)} entries in {time.monotonic()-t0:.2f}s"
        )
        query_map = {serial: info.config.pb.name for serial, info in raw_map.items()}
        _LOGGER.debug(f"[_fetch_factory_pqs] {len(query_map)} PQs")
        return _FactoryQueryResult(
            new_client=client,
            query_map=query_map,
        )

    except Exception as e:
        _LOGGER.warning(f"[_fetch_factory_pqs] factory query failed: {e!r}")
        return _FactoryQueryError(
            new_client=new_client,
            error=exception_summary(e),
        )


class EnterpriseSessionRegistry(MutableSessionRegistry):
    """Purpose-built registry for the DHE MCP server.

    Manages exactly one enterprise system (configured via ``system_name``).  Discovers
    enterprise PQ sessions asynchronously from the controller client and supports
    mutation methods for MCP-created sessions.

    See module docstring for the full locking contract.

    Usage::

        registry = EnterpriseSessionRegistry(system_config)
        await registry.initialize()
        session_mgr = await registry.get("enterprise:my-system:my-pq")
        factory = registry.factory_manager
        await registry.close()
    """

    @staticmethod
    def _make_enterprise_session_manager(
        factory: CorePlusSessionFactoryManager,
        serial: CorePlusQuerySerial,
        display_name: str,
        system_name: str,
    ) -> EnterpriseSessionManager:
        """Create an ``EnterpriseSessionManager`` that lazily connects to a PQ.

        Args:
            factory (CorePlusSessionFactoryManager): Factory manager used to
                obtain a connected factory instance.
            serial (CorePlusQuerySerial): PQ serial assigned by the controller;
                becomes the manager's :class:`SessionId`.
            display_name (str): PQ display name from the controller. Travels
                through as :attr:`BaseItemManager.name` for human output and
                does not participate in the id.
            system_name (str): Enterprise system name; becomes the system
                segment of the manager's :attr:`qualified_session_id`.

        Returns:
            An ``EnterpriseSessionManager`` whose creation function calls
            ``factory.get()`` then ``connect_to_persistent_query(serial=serial)``.
        """

        async def creation_function(source: str, name: str) -> CorePlusSession:
            factory_instance = await factory.get()
            return await factory_instance.connect_to_persistent_query(serial=serial)

        return EnterpriseSessionManager(
            system=system_name,
            session_id=SessionId.from_int(serial),
            name=display_name,
            creation_function=creation_function,
            origin=SessionOrigin.DISCOVERED,
        )

    def __init__(
        self,
        system_config: EnterpriseSystemConfig,
        timeouts: EnterpriseClientTimeouts,
    ) -> None:
        """Capture the validated, credential-bound enterprise configuration.

        Args:
            system_config (EnterpriseSystemConfig): A validated enterprise
                system declaration. The registry stores three fields
                directly: the system name (used as the system segment
                in session IDs), the typed system config (forwarded to
                :class:`CorePlusSessionFactoryManager`), and the
                pre-resolved credentials (also forwarded to the factory
                manager). The config carries no per-request state, so
                the registry never re-reads credentials at run time.
            timeouts (EnterpriseClientTimeouts): Enterprise client-layer
                timeout configuration forwarded to the
                :class:`CorePlusSessionFactoryManager` this registry
                constructs (and from there to the underlying
                :class:`CorePlusSessionFactory`).
        """
        super().__init__()
        self._system_name: str = system_config.name
        self._system_config: EnterpriseSystemConfig = system_config
        self._creds = system_config.auth.credentials
        self._timeouts = timeouts
        self._factory_manager: CorePlusSessionFactoryManager | None = None
        self._controller_client: CorePlusControllerClient | None = None
        self._web_client_data_session: CorePlusSession | None = None
        self._web_client_data_lock = asyncio.Lock()
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

        This value comes from the ``system_name`` field in the DHE system config and
        appears as the system segment in all enterprise session identifiers
        (e.g. ``"enterprise:<system_name>:<session_id>"`` where ``session_id`` is
        the controller-assigned PQ serial).

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
            CorePlusSessionFactoryManager: The single factory manager
                for this enterprise system, created during
                :meth:`initialize`.

        Raises:
            InternalError: If the registry has not been initialized.
        """
        self._check_initialized()
        if self._factory_manager is None:
            raise InternalError(
                f"{self.__class__.__name__} factory manager is not available; "
                "initialize() did not run to completion."
            )
        return self._factory_manager

    # ------------------------------------------------------------------
    # WebClientData session
    # ------------------------------------------------------------------

    async def web_client_data_session(self) -> CorePlusSession:
        """Return a live session connected to this system's WebClientData PQ.

        The session is created on first use and cached. A cached session is
        returned only after a liveness check passes; a dead session is
        discarded and replaced. Because the registry owns exactly one factory
        manager, and therefore one credential set, the returned session is
        authenticated as this system's configured principal without the caller
        supplying credentials.

        Returns:
            CorePlusSession: A live session on the ``WebClientData`` persistent
                query, usable for any system-scoped read such as the catalog.

        Raises:
            InternalError: If the registry has not been initialized, or was
                closed while this call waited for the cache lock.
            Exception: Any exception raised while connecting to the
                ``WebClientData`` persistent query propagates unchanged; a
                system where that PQ is not running cannot serve these tables.
        """
        async with self._web_client_data_lock:
            # Resolved inside the lock: a close() that lands while this call
            # waits clears the manager, and this then fails rather than
            # connecting a session nothing will ever close.
            factory_manager = self.factory_manager

            cached = self._web_client_data_session
            if cached is not None:
                try:
                    # Bounded: an unbounded probe would hold the lock and stall
                    # every catalog request for this system.
                    if await asyncio.wait_for(
                        cached.is_alive(),
                        timeout=self._timeouts.quick_operation_timeout_seconds,
                    ):
                        return cached
                    raise RuntimeError("is_alive() returned False")
                except Exception as e:
                    _LOGGER.warning(
                        f"[{self.__class__.__name__}] cached '{WEB_CLIENT_DATA_PQ}' session "
                        f"failed liveness check ({exception_summary(e)}); reconnecting"
                    )
                    self._web_client_data_session = None
                    await self._close_web_client_data_session(cached)

            _LOGGER.debug(
                f"[{self.__class__.__name__}] connecting to '{WEB_CLIENT_DATA_PQ}' "
                f"for system '{self._system_name}'"
            )
            factory_instance = await factory_manager.get()
            session = await factory_instance.connect_to_persistent_query(
                name=WEB_CLIENT_DATA_PQ
            )
            self._web_client_data_session = session
            _LOGGER.info(
                f"[{self.__class__.__name__}] connected to '{WEB_CLIENT_DATA_PQ}' "
                f"for system '{self._system_name}'"
            )
            return session

    async def effective_user(self) -> str:
        """Return the identity this system's factory authenticated as.

        Read from the authentication token, so it reflects the operate-as
        identity for every credential kind. Per-user reads through the
        ``WebClientData`` widget must name this identity.

        Returns:
            str: The effective user for this system.

        Raises:
            InternalError: If the registry has not been initialized, or if the
                controller client has authenticated but reports no user.
            Exception: Any exception raised while reaching the controller
                propagates unchanged.
        """
        factory_instance = await self.factory_manager.get()
        controller = factory_instance.controller_client
        user = controller.effective_user
        if user is None:
            # The controller authenticates lazily; a ping forces the token.
            await controller.ping()
            user = controller.effective_user
        if user is None:
            raise InternalError(
                f"Controller client for enterprise system "
                f"'{self._system_name}' reports no effective user after "
                f"authenticating; cannot perform a per-user read."
            )
        return user

    async def _close_web_client_data_session(self, session: CorePlusSession) -> None:
        """Close a WebClientData session, logging and swallowing any failure.

        Args:
            session (CorePlusSession): The session to close.
        """
        try:
            await session.close()
        except Exception as e:
            _LOGGER.warning(
                f"[{self.__class__.__name__}] error closing '{WEB_CLIENT_DATA_PQ}' "
                f"session: {exception_summary(e)}"
            )

    # ------------------------------------------------------------------
    # BaseRegistry overrides — lifecycle
    # ------------------------------------------------------------------

    @override
    async def _load_items(self) -> None:
        """Build the factory manager and launch background discovery.

        Called by ``super().initialize()`` while holding ``self._lock``.
        The pre-resolved configuration and credentials captured in
        :meth:`__init__` are forwarded directly to a
        :class:`CorePlusSessionFactoryManager`; the registry then
        transitions to :attr:`InitializationPhase.PARTIAL` and starts
        a background task to populate ``_items`` from the controller
        client.
        """
        self._factory_manager = CorePlusSessionFactoryManager(
            self._system_name,
            self._system_config,
            self._creds,
            timeouts=self._timeouts,
        )
        self._phase = InitializationPhase.PARTIAL
        self._discovery_task = asyncio.create_task(self._discover_enterprise_sessions())
        _LOGGER.info(
            f"[{self.__class__.__name__}] factory bound for "
            f"'{self._system_name}'; discovery started"
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
        4. Close the cached WebClientData session (drained in step 1b), then
           the factory manager, using the local refs captured earlier.
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

        # Step 1b: drain the WebClientData cache under its own lock, so an
        # in-flight connect completes and is handed over here rather than
        # storing a session after shutdown.
        async with self._web_client_data_lock:
            web_client_data = self._web_client_data_session
            self._web_client_data_session = None

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
                f"[{self.__class__.__name__}] canceled background enterprise discovery"
            )

        # Step 4: close factory manager via local ref captured under the lock.
        if web_client_data is not None:
            await self._close_web_client_data_session(web_client_data)

        if factory is not None:
            try:
                await factory.close()
            except Exception as e:
                _LOGGER.error(
                    f"[{self.__class__.__name__}] error closing factory manager: {e}"
                )

        # Step 5: clear remaining state and collect items to close. The
        # _config / _creds / _system_name fields are kept so that the
        # registry can be re-initialized in place via initialize() if
        # needed; only the per-run discovery state is reset.
        async with self._lock:
            self._controller_client = None
            self._added_session_ids.clear()
            self._phase = InitializationPhase.NOT_STARTED
            self._error = None
            items_to_close = dict(self._items)
            self._items.clear()

        # Step 6: close items outside the lock via the inherited hook.
        # Inlines the steps from super().close() so the discovery-task
        # cancel and factory-manager close can run between the
        # _initialized=False flip and _items.clear().
        await self._close_items(items_to_close)

        _LOGGER.info(f"[{self.__class__.__name__}] closed")

    # ------------------------------------------------------------------
    # BaseRegistry overrides — read interface
    # ------------------------------------------------------------------

    async def _check_and_sync(self) -> None:
        """Verify initialized and trigger a sync if in COMPLETED phase.

        Shared preamble for :meth:`get` and :meth:`get_all`. Callers must
        re-check ``_check_initialized()`` under ``self._lock`` after this
        returns, since a concurrent ``close()`` could have run during the
        sync.
        """
        self._check_initialized()
        async with self._lock:
            phase = self._phase
        if phase == InitializationPhase.COMPLETED:
            await self._sync_enterprise_sessions()

    @override
    async def get(self, name: QualifiedSessionId) -> SessionManager:
        """Return the session manager for *name*, refreshing enterprise data if needed.

        Triggers an on-demand refresh before looking up the item once initial
        discovery completes (``COMPLETED`` phase).  During ``LOADING`` or
        ``PARTIAL`` the background task is the sole writer and refresh is skipped.

        Args:
            name (str): Fully qualified session name in ``"type:source:name"``
                format (e.g. ``"enterprise:system:my-pq"``).

        Returns:
            SessionManager: The session manager for *name*.

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
    async def get_all(self) -> RegistrySnapshot[SessionManager]:
        """Return an atomic snapshot of all sessions, refreshing enterprise data if needed.

        Returns:
            RegistrySnapshot[SessionManager]: Snapshot containing ``items``,
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
                    f"[{self.__class__.__name__}] error closing stale session '{manager.qualified_session_id}': {e}"
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

        On success, reconciles ``_items`` to match the controller's
        report (delegated to :meth:`_apply_factory_success`).  On error,
        records the failure and replaces ``_controller_client`` but
        leaves ``_items`` untouched — the controller gave us no
        information about which sessions are still alive, so the safest
        move is to keep the last known state until the next successful
        refresh reconciles. Wiping on every transient blip would be
        too disruptive to in-flight tool calls.

        Args:
            result (_FactoryQueryResult | _FactoryQueryError): Query result from ``_fetch_factory_pqs``.
            factory_manager (CorePlusSessionFactoryManager): Factory manager for creating new session managers.

        Returns:
            Managers that should be closed by the caller (outside the
                lock). Empty on the error path (nothing was removed).
        """
        if isinstance(result, _FactoryQueryResult):
            return self._apply_factory_success(result, factory_manager)
        if isinstance(result, _FactoryQueryError):
            self._controller_client = result.new_client
            self._error = result.error
            _LOGGER.warning(
                f"[{self.__class__.__name__}] factory query failed: {result.error}"
            )
            return []
        raise InternalError(f"Unexpected result type {type(result).__name__!r}")

    def _remove_sessions_by_keys(
        self, keys: set[QualifiedSessionId]
    ) -> list[BaseItemManager]:
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
        """Reconcile ``_items`` to match the controller's report.

        Synchronous — no ``await``.  Must be called under ``self._lock``.

        The controller is the source of truth for what PQs exist; this
        method makes ``_items`` mirror the controller's reported set:

        - Caches the live client returned by the query.
        - Adds PQ sessions the controller reports that we do not yet have.
        - Removes sessions we have that the controller no longer reports.
        - Clears any previous error.

        ``add_session`` is a cache-warming optimization, not a protection
        contract: a session added between this method's snapshot and the
        controller's response can be wiped here. That is acceptable —
        the session itself still exists on the controller (the create
        call returned), and the next refresh re-discovers it. Recovery
        is bounded by one refresh interval and requires no manual
        intervention.

        Invariant: a session key reappearing after removal is installed
        with a freshly constructed manager instance — never by mutating
        an existing manager in place.  The Evictor's identity-checked
        :meth:`BaseRegistry.remove` relies on this to safely race with
        controller-driven re-adds.

        Args:
            result (_FactoryQueryResult): Successful query result.
            factory_manager (CorePlusSessionFactoryManager): Factory manager for
                creating new session managers.

        Returns:
            list[BaseItemManager]: Managers removed as stale; caller must close them.
        """
        self._controller_client = result.new_client

        existing_keys = set(self._items.keys())
        # Map controller serial -> full registry key for the live PQ set.
        controller_key_by_serial: dict[CorePlusQuerySerial, QualifiedSessionId] = {
            serial: QualifiedSessionId(
                SystemType.ENTERPRISE, self._system_name, SessionId.from_int(serial)
            )
            for serial in result.query_map
        }
        controller_keys = set(controller_key_by_serial.values())

        keys_to_remove = existing_keys - controller_keys
        serials_to_add = [
            serial
            for serial, key in controller_key_by_serial.items()
            if key not in existing_keys
        ]

        for serial in serials_to_add:
            display_name = result.query_map[serial]
            # Always a new manager instance — never reuse an existing one
            # (see method docstring invariant).
            mgr = self._make_enterprise_session_manager(
                factory_manager, serial, display_name, self._system_name
            )
            self._items[mgr.qualified_session_id] = mgr

        managers_to_close = self._remove_sessions_by_keys(keys_to_remove)

        self._error = None

        if serials_to_add:
            _LOGGER.debug(
                f"[{self.__class__.__name__}] added {len(serials_to_add)} sessions"
            )
        if keys_to_remove:
            _LOGGER.debug(
                f"[{self.__class__.__name__}] removed {len(keys_to_remove)} stale sessions"
            )

        return managers_to_close

    # ------------------------------------------------------------------
    # Private — background discovery task
    # ------------------------------------------------------------------

    async def _discover_enterprise_sessions(self) -> None:
        """One-shot background task: discover enterprise sessions at startup.

        Outcomes:

        - **Success**: sets ``_phase`` to ``LOADING``, runs
          ``_sync_enterprise_sessions``, then sets ``_phase`` to ``COMPLETED``.
        - **CancelledError** (from ``close()``): sets ``_phase`` to
          ``FAILED`` and re-raises so the awaiter in ``close()`` observes
          the cancellation.
        - **Other Exception**: records the error on ``_error`` and still
          sets ``_phase`` to ``COMPLETED`` (with ``exc_info`` logged).
          Leaving the phase at ``LOADING`` would permanently block every
          ``get`` / ``get_all`` call, so a soft-failure policy is used:
          the registry is considered "done trying" and subsequent reads
          see the error via ``_error``.
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
                f"[{self.__class__.__name__}] enterprise discovery canceled (shutdown)"
            )
            raise

        except Exception as e:
            elapsed = time.monotonic() - start
            _LOGGER.error(
                f"[{self.__class__.__name__}] enterprise discovery failed in {elapsed:.2f}s: {e}",
                exc_info=True,
            )
            async with self._lock:
                self._error = exception_summary(e)
                self._phase = InitializationPhase.COMPLETED

    # ------------------------------------------------------------------
    # Private — error message helper
    # ------------------------------------------------------------------

    def _build_not_found_message(self, name: QualifiedSessionId) -> str:
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
