"""Per-session registry manager with idle-TTL expiry.

This module owns the lifecycle of one ``BaseRegistry`` per MCP session.  Its
job is to:

* Create a registry for a session on first access (running ``initialize()``
  exactly once per session, even under concurrent access).
* Hand the same registry back to subsequent callers for that session.
* Close idle registries after a configurable timeout.
* Close a session on demand, or close everything on shutdown.

Concurrency model
-----------------
All public methods must be called from a single ``asyncio`` event loop.  The
class does not provide cross-thread safety; callers running on multiple OS
threads must either marshal calls onto one loop or run separate manager
instances per loop.  At the deployment level, scale beyond one event loop's
capacity by running multiple processes behind a session-sticky load balancer
on the MCP session id.

Internals
---------
A single ``dict[str, _SessionEntry]`` is the only session-keyed structure.
Each entry owns a per-session :class:`asyncio.Lock`, the registry instance
(or ``None`` while uninitialized), the last-used timestamp, and a
``closed`` :class:`asyncio.Event` used to signal close-during-init.

The per-entry lock serializes initialization vs. close vs. eviction for one
session, which makes single-flight initialization an emergent property of the
lock rather than a hand-written future protocol.  A second tiny lock,
``_map_lock``, guards only ``self._entries`` mutations and is never held
across an ``await``.

Entry locking invariants
------------------------
Fields on :class:`_SessionEntry` fall into two groups:

* **State fields** -- ``last_used`` and ``registry``.  Writes always occur
  under ``entry.lock``.  Reads almost always occur under ``entry.lock``;
  the one exception is ``_sweep_expired``'s candidate-snapshot listcomp,
  which reads ``last_used`` under ``_map_lock`` only and is itself
  await-free.  ``registry`` is never read without ``entry.lock``.
* **Signaling field** -- ``closed`` (an :class:`asyncio.Event`).  Goes
  not-set -> set exactly once.  Safe to ``set()`` without any lock by
  design; this is *required* for ``close_session`` / ``_close_all`` to
  signal an in-flight ``initialize()`` to abort, since the owner holds
  ``entry.lock`` for the entire duration of ``await initialize()`` and
  any attempt to take ``entry.lock`` from the close path would deadlock.
  ``set()`` may also be called while already holding ``entry.lock``
  (e.g. from the init-failure path); both are safe.  Reads (``is_set()``)
  are safe without any lock; authoritative checks occur under
  ``entry.lock`` where the value is stable for the duration of the
  critical section.

Reads under ``_map_lock`` (without ``entry.lock``) of ``last_used`` and
``closed.is_set()`` are sound because every ``_map_lock`` critical section
in this module is await-free, so no concurrent coroutine can mutate the
fields within it.
"""

import asyncio
import logging
import time

from deephaven_mcp.config import (
    DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS,
    ConfigManager,
)
from deephaven_mcp.resource_manager import BaseRegistry

_LOGGER = logging.getLogger(__name__)

__all__ = ["SessionClosedDuringInitError", "SessionRegistryManager"]


class SessionClosedDuringInitError(RuntimeError):
    """Raised when a session is closed while its registry is still initializing.

    Occurs when :meth:`SessionRegistryManager.close_session` (or shutdown via
    :meth:`SessionRegistryManager.stop`) is invoked for a session whose
    ``initialize()`` call has not yet completed.  The owning coroutine that
    was running ``initialize()`` receives this exception and the freshly
    created registry is closed before being exposed to anyone.
    """


class _SessionEntry[R: BaseRegistry]:
    """State for one session.

    See the module docstring's "Entry locking invariants" section for the
    full per-field locking rules; the summary on each attribute below
    follows that contract.

    Attributes:
        lock (asyncio.Lock): Per-session lock that serializes initialization,
            close, and eviction for this entry, and protects the state
            fields ``registry`` and ``last_used``.  See those fields'
            individual docstrings for the precise read/write rules
            (including the single ``_map_lock``-only read of ``last_used``
            in the sweeper).
        last_used (float): Monotonic timestamp (``time.monotonic()``) of the
            most recent access.  Written under ``self.lock`` by
            :meth:`SessionRegistryManager.get_or_create_registry`.  Read
            under ``self.lock`` by that same method and by the per-entry
            sweep recheck; also read under ``_map_lock`` (without
            ``self.lock``) by the candidate-snapshot listcomp in
            :meth:`SessionRegistryManager._sweep_expired`, whose critical
            section is await-free.
        registry (R | None): The initialized registry, or ``None`` until
            :meth:`BaseRegistry.initialize` has succeeded.  Always accessed
            under ``self.lock``.
        closed (asyncio.Event): One-way signal that this entry has been
            retired (close requested, init failed, or evicted).  Goes
            not-set -> set exactly once; never reset.  ``set()`` may be
            called with or without ``self.lock`` held; calling it without
            the lock is required from ``close_session`` and
            ``_close_all`` so they can signal an in-flight
            ``initialize()`` owner (which holds ``self.lock``) to abort
            without deadlocking.  Once ``is_set()`` returns True, the
            entry is dead -- a fresh entry is created on the next access
            for the same session id.
    """

    __slots__ = ("lock", "last_used", "registry", "closed")

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.last_used = time.monotonic()
        self.registry: R | None = None
        self.closed = asyncio.Event()


class SessionRegistryManager[R: BaseRegistry]:
    """Manage per-session Deephaven registries with idle-TTL expiry.

    Instantiate once per server, call :meth:`start` during lifespan startup,
    and call :meth:`stop` during lifespan shutdown.

    Args:
        registry_class (type[R]): Concrete
            :class:`~deephaven_mcp.resource_manager.BaseRegistry` subclass used
            to create a new per-session registry on first access (e.g.
            ``CommunitySessionRegistry`` or ``EnterpriseSessionRegistry``).
            Called as ``registry_class()`` with no arguments.
        idle_timeout_seconds (float): Seconds of inactivity after which a
            session's registry is closed by the TTL sweeper.  Defaults to
            :data:`~deephaven_mcp.config.DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS`
            (3600.0, i.e. 1 hour).
        sweep_interval_seconds (float): How often (in seconds) the TTL sweeper
            wakes to check for expired sessions.  Defaults to ``60.0``
            (1 minute).
    """

    def __init__(
        self,
        registry_class: type[R],
        idle_timeout_seconds: float = DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS,
        sweep_interval_seconds: float = 60.0,
    ) -> None:
        self._registry_class = registry_class
        self._idle_timeout = idle_timeout_seconds
        self._sweep_interval = sweep_interval_seconds
        self._entries: dict[str, _SessionEntry[R]] = {}
        self._map_lock = asyncio.Lock()
        self._sweeper_task: asyncio.Task[None] | None = None
        _LOGGER.info(
            f"[SessionRegistryManager] created "
            f"(idle_timeout={idle_timeout_seconds}s, sweep_interval={sweep_interval_seconds}s)"
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Launch the TTL sweeper background task.

        Behavior:

        * If no sweeper task has ever been started, creates and schedules one.
        * If a sweeper task exists and is still running, logs a warning and
          returns without starting another (idempotent fast-path).
        * If a previous sweeper task exists but has finished (cancelled,
          completed, or raised), creates and schedules a new one.

        Should be called once during server lifespan startup before any
        sessions are accessed.
        """
        if self._sweeper_task is not None and not self._sweeper_task.done():
            _LOGGER.warning(
                "[SessionRegistryManager] start() called while sweeper already running; ignoring."
            )
            return
        _LOGGER.info("[SessionRegistryManager] Starting TTL sweeper.")
        self._sweeper_task = asyncio.create_task(self._sweep_loop())

    async def stop(self) -> None:
        """Cancel the sweeper and close all registries.

        Cancels the sweeper task (if any) and awaits its completion,
        absorbing :class:`asyncio.CancelledError` and logging any other
        exception that surfaces from the cancelled task.  Then calls
        :meth:`_close_all` to close every per-session registry.

        Idempotent: safe to call without a prior :meth:`start`, and safe to
        call repeatedly.
        """
        _LOGGER.info("[SessionRegistryManager] Stopping.")
        task = self._sweeper_task
        self._sweeper_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                _LOGGER.exception(
                    "[SessionRegistryManager] Error awaiting cancelled sweeper task"
                )
        await self._close_all()
        _LOGGER.info("[SessionRegistryManager] Stopped.")

    # ------------------------------------------------------------------
    # Public session API
    # ------------------------------------------------------------------

    async def get_or_create_registry(
        self, session_id: str, config_manager: ConfigManager
    ) -> R:
        """Return the registry for a session, creating it on first access.

        Concurrent first-access calls for the same ``session_id`` are
        serialized through the per-session lock so that ``initialize()`` runs
        exactly once; later callers acquire the per-session lock, refresh
        ``last_used``, and return the already-initialized registry without
        re-initializing.

        On every call, ``last_used`` is updated to ``time.monotonic()``
        before the registry reference is returned, keeping the session alive
        with respect to the TTL sweeper.

        Args:
            session_id (str): Opaque per-client session identifier.
            config_manager (ConfigManager): Configuration source passed to
                :meth:`BaseRegistry.initialize` if this is the first access
                for ``session_id``.  Ignored on subsequent accesses.

        Returns:
            R: The registry instance for the given session.

        Raises:
            SessionClosedDuringInitError: If :meth:`close_session` (or
                shutdown via :meth:`stop` / :meth:`_close_all`) ran while
                ``initialize()`` was in progress for this session.  The
                freshly created registry is closed before the exception is
                raised, so callers never observe a half-live registry.
            Exception: Any exception raised by ``initialize()`` is
                propagated after the half-initialized registry is closed.
                The entry is removed from the manager so a subsequent call
                with the same ``session_id`` retries with a fresh entry.
        """
        # Loop bound: at most one extra iteration when a fresh entry is found
        # already-closed immediately after acquiring its lock (close raced us).
        while True:
            async with self._map_lock:
                entry = self._entries.get(session_id)
                if entry is None or entry.closed.is_set():
                    entry = _SessionEntry[R]()
                    self._entries[session_id] = entry

            async with entry.lock:
                if entry.closed.is_set():
                    # close_session ran between map_lock release and entry.lock
                    # acquire; create a fresh entry on the next iteration.
                    continue

                entry.last_used = time.monotonic()

                if entry.registry is not None:
                    return entry.registry

                _LOGGER.info(
                    f"[SessionRegistryManager] Creating registry for session '{session_id}'"
                )
                registry = self._registry_class()
                try:
                    await registry.initialize(config_manager)
                except BaseException:
                    entry.closed.set()
                    async with self._map_lock:
                        if self._entries.get(session_id) is entry:
                            del self._entries[session_id]
                    try:
                        await registry.close()
                    except Exception:
                        _LOGGER.exception(
                            f"[SessionRegistryManager] Error closing failed registry for session '{session_id}'"
                        )
                    raise

                if entry.closed.is_set():
                    # close_session() / _close_all() ran while we were inside
                    # await registry.initialize(); discard the new registry.
                    try:
                        await registry.close()
                    except Exception:
                        _LOGGER.exception(
                            f"[SessionRegistryManager] Error closing registry discarded after close-during-init for session '{session_id}'"
                        )
                    raise SessionClosedDuringInitError(
                        f"Session '{session_id}' was closed during initialization"
                    )

                entry.registry = registry
                _LOGGER.info(
                    f"[SessionRegistryManager] Registry ready for session '{session_id}'"
                )
                return registry

    async def close_session(self, session_id: str) -> None:
        """Close and remove the registry for one session.

        Idempotent: a no-op if ``session_id`` has no entry.  If a registry
        initialization is currently in progress for this session, the
        entry's ``closed`` event is set immediately (under ``_map_lock``);
        the owning :meth:`get_or_create_registry` call observes the signal
        after ``initialize()`` returns, closes the new registry, and raises
        :class:`SessionClosedDuringInitError`.

        After signalling, this method awaits ``entry.lock`` to wait out any
        in-flight init owner, then closes the registry if one was
        successfully populated.  Errors from ``registry.close()`` are
        logged and swallowed -- this method never raises.

        Args:
            session_id (str): The session identifier to close.
        """
        async with self._map_lock:
            entry = self._entries.pop(session_id, None)
            if entry is not None:
                # Signal the in-flight initialize() owner (if any) to discard
                # its registry.  Event.set() is intentionally called without
                # entry.lock; taking the lock here would deadlock against the
                # owner, which holds it across await registry.initialize().
                entry.closed.set()
        if entry is None:
            return
        # Wait for any in-flight init or other holder of the entry lock to
        # finish, then close the registry if one was successfully populated.
        async with entry.lock:
            registry, entry.registry = entry.registry, None
        if registry is not None:
            _LOGGER.info(
                f"[SessionRegistryManager] Closing registry for session '{session_id}'"
            )
            try:
                await registry.close()
            except Exception:
                _LOGGER.exception(
                    f"[SessionRegistryManager] Error closing registry for session '{session_id}'"
                )
            _LOGGER.info(
                f"[SessionRegistryManager] Registry closed for session '{session_id}'"
            )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _close_all(self) -> None:
        """Mark every entry closed and close every populated registry.

        Called by :meth:`stop` after the sweeper task is cancelled.  Sets
        ``entry.closed`` on every entry under ``_map_lock`` so any in-flight
        :meth:`BaseRegistry.initialize` calls discover the signal when they
        return and discard their fresh registries.  Then, for each entry,
        awaits ``entry.lock`` (waiting out any in-flight init owner) and
        closes the populated registry if one is present.  Per-registry
        close errors are logged and swallowed so a single failure does not
        prevent other registries from being closed.
        """
        async with self._map_lock:
            entries = list(self._entries.items())
            for _, entry in entries:
                # Signal any in-flight initialize() owners to abort; see the
                # corresponding comment in close_session().
                entry.closed.set()
            self._entries.clear()
        if not entries:
            return
        _LOGGER.info(
            f"[SessionRegistryManager] Closing all registries ({len(entries)} session(s))."
        )
        for session_id, entry in entries:
            async with entry.lock:
                registry, entry.registry = entry.registry, None
            if registry is not None:
                try:
                    await registry.close()
                except Exception:
                    _LOGGER.exception(
                        f"[SessionRegistryManager] Error closing registry for session '{session_id}' during shutdown"
                    )

    async def _sweep_loop(self) -> None:
        """Background loop that runs :meth:`_sweep_expired` on an interval.

        Sleeps for ``self._sweep_interval`` seconds between each sweep.
        :class:`Exception` raised by a single sweep is caught and logged;
        the loop continues.  :class:`asyncio.CancelledError` is logged and
        re-raised, terminating the loop (this is how :meth:`stop` shuts
        the sweeper down).
        """
        _LOGGER.info(
            f"[SessionRegistryManager] Sweeper loop started (interval={self._sweep_interval}s)"
        )
        try:
            while True:
                await asyncio.sleep(self._sweep_interval)
                try:
                    await self._sweep_expired()
                except Exception:
                    _LOGGER.exception(
                        "[SessionRegistryManager] Error in TTL sweep; continuing."
                    )
        except asyncio.CancelledError:
            _LOGGER.info("[SessionRegistryManager] Sweeper loop cancelled.")
            raise

    async def _sweep_expired(self) -> None:
        """Close every session whose ``last_used`` is older than the TTL.

        Two-phase to avoid evicting a session that races with a fresh use:

        1. Under ``_map_lock`` (await-free), snapshot the list of entries
           whose ``last_used`` is older than ``self._idle_timeout``.
        2. For each candidate, acquire ``entry.lock`` and re-check both
           ``entry.closed.is_set()`` (skip if already retired by another
           path) and ``time.monotonic() - entry.last_used`` against the
           timeout (skip if the entry was used since the snapshot).  If
           still expired, mark ``entry.closed``, remove the entry from the
           map (only if it is still the same entry), capture the registry
           reference, and close it outside the per-entry lock.

        Errors from ``registry.close()`` are logged and swallowed.
        """
        now = time.monotonic()
        timeout = self._idle_timeout
        async with self._map_lock:
            candidates = [
                (sid, entry)
                for sid, entry in self._entries.items()
                if now - entry.last_used > timeout
            ]
        for session_id, entry in candidates:
            async with entry.lock:
                if entry.closed.is_set():
                    continue
                if time.monotonic() - entry.last_used <= timeout:
                    # Session was used since the snapshot; skip.
                    continue
                entry.closed.set()
                async with self._map_lock:
                    if self._entries.get(session_id) is entry:
                        del self._entries[session_id]
                registry, entry.registry = entry.registry, None
            if registry is not None:
                _LOGGER.info(
                    f"[SessionRegistryManager] Closing idle registry for session '{session_id}'"
                )
                try:
                    await registry.close()
                except Exception:
                    _LOGGER.exception(
                        f"[SessionRegistryManager] Error closing idle registry for session '{session_id}'"
                    )
