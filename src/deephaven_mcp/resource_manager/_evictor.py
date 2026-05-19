"""Per-registry idle-eviction sweeper.

Provides :class:`Evictor`, a coordinator object that runs a periodic
background sweep over one :class:`BaseRegistry`, asking each managed
item whether it has been idle past a configured timeout and removing
items whose :attr:`BaseItemManager.evicts_on_idle` is ``True`` from the
registry after closure.

The Evictor holds a reference to its registry and uses only the
registry's existing public collection API (:meth:`BaseRegistry.get_all`
for iteration and :meth:`BaseRegistry.remove` for identity-checked
removal); no registry methods are exposed solely for the Evictor.

Lifecycle:

- Construct: ``evictor = Evictor(registry, idle_timeout_seconds=...)``
- ``await evictor.start()`` — launches the background sweep loop.
- ``await evictor.stop()`` — cancels and awaits the loop; idempotent,
  safe to call multiple times and safe to call without a prior ``start``.

Concurrency:

- ``_sweeper_task`` is read and written under ``self._lock``.
- The sweep loop snapshots the registry's items under the registry's
  own lock (via :meth:`BaseRegistry.get_all`), then iterates candidates
  outside any lock; per-item idleness is checked under each manager's
  lock inside :meth:`BaseItemManager.maybe_close_if_idle`.
- Final drop happens via :meth:`BaseRegistry.remove(name, expected=manager)`
  — identity-checked, so a same-key re-add between the snapshot and the
  removal does not evict the new entry.
"""

import asyncio
import logging
import time

from .._exceptions import InternalError
from ._manager import BaseItemManager
from ._registry import BaseRegistry

_LOGGER = logging.getLogger(__name__)

__all__ = ["Evictor"]


class Evictor:
    """Idle-eviction sweep coordinator for one :class:`BaseRegistry`.

    One Evictor instance per registry.  Holds the timing parameters and
    the background sweeper task lifecycle; defers the per-item decisions
    to :meth:`BaseItemManager.maybe_close_if_idle` and reads
    :attr:`BaseItemManager.evicts_on_idle` to decide whether to remove
    the manager from the registry after closing it.

    Args:
        registry (BaseRegistry): The registry to sweep.
        idle_timeout_seconds (float | None): Seconds of inactivity after
            which a managed item is closed.  ``None`` disables the
            sweeper entirely (useful for tests and opt-out configs).
        sweep_interval_seconds (float | None): How often (in seconds) the
            sweep loop wakes to check for idle items.  ``None`` (the
            default) disables the sweeper entirely — same as
            ``idle_timeout_seconds=None``.  Callers that want the sweeper
            to run must pass non-``None`` values for both parameters.
    """

    def __init__(
        self,
        registry: BaseRegistry,
        *,
        idle_timeout_seconds: float | None = None,
        sweep_interval_seconds: float | None = None,
    ) -> None:
        self._registry = registry
        self._idle_timeout: float | None = idle_timeout_seconds
        self._sweep_interval: float | None = sweep_interval_seconds
        self._sweeper_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        _LOGGER.debug(
            f"[Evictor] created for {registry.__class__.__name__} "
            f"(idle_timeout={idle_timeout_seconds}, sweep_interval={sweep_interval_seconds})"
        )

    async def start(self) -> None:
        """Launch the background sweep loop.

        No-op when either ``idle_timeout_seconds`` or
        ``sweep_interval_seconds`` is ``None`` (the sweeper requires both
        to run), or when the sweeper task already exists and is still
        running.
        """
        async with self._lock:
            if self._idle_timeout is None or self._sweep_interval is None:
                _LOGGER.info(
                    f"[Evictor] Sweeper disabled for "
                    f"{self._registry.__class__.__name__} "
                    f"(idle_timeout={self._idle_timeout}, "
                    f"sweep_interval={self._sweep_interval}); "
                    f"both must be non-None to start"
                )
                return
            if self._sweeper_task is not None and not self._sweeper_task.done():
                _LOGGER.info(
                    f"[Evictor] start() is a no-op for "
                    f"{self._registry.__class__.__name__}: sweeper task already running"
                )
                return
            _LOGGER.info(
                f"[Evictor] Starting idle sweeper for "
                f"{self._registry.__class__.__name__} "
                f"(timeout={self._idle_timeout}s, interval={self._sweep_interval}s)"
            )
            self._sweeper_task = asyncio.create_task(self._sweep_loop())

    async def stop(self) -> None:
        """Cancel and await the sweeper task.

        Acquires ``self._lock`` briefly to atomically capture and clear
        ``self._sweeper_task``, then cancels and awaits outside the lock.

        Idempotent: safe to call when no sweeper was ever started, safe
        to call repeatedly, and safe to call concurrently with a
        :meth:`start` that has not yet scheduled the task.
        """
        async with self._lock:
            task = self._sweeper_task
            self._sweeper_task = None
        if task is None:
            _LOGGER.debug(
                f"[Evictor] stop() is a no-op for "
                f"{self._registry.__class__.__name__}: no sweeper task to cancel"
            )
            return
        if task.done():
            _LOGGER.debug(
                f"[Evictor] stop() found sweeper task for "
                f"{self._registry.__class__.__name__} already finished; "
                f"nothing to cancel"
            )
            return
        _LOGGER.info(
            f"[Evictor] Stopping idle sweeper for "
            f"{self._registry.__class__.__name__}"
        )
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            _LOGGER.exception(
                f"[Evictor] Sweeper task for "
                f"{self._registry.__class__.__name__} raised a non-CancelledError "
                f"exception while being cancelled; this indicates a bug in the "
                f"sweep loop's CancelledError handling"
            )
        _LOGGER.info(
            f"[Evictor] Idle sweeper stopped for "
            f"{self._registry.__class__.__name__}"
        )

    async def _sweep_loop(self) -> None:
        """Run :meth:`_sweep_once` periodically until cancelled.

        A single sweep exception is caught and logged; the loop
        continues.  ``asyncio.CancelledError`` is re-raised so the
        awaiter in :meth:`stop` observes the cancellation.

        Raises :class:`InternalError` if invoked with
        ``_sweep_interval`` set to ``None``.
        """
        if self._sweep_interval is None:
            raise InternalError(
                "[Evictor] _sweep_loop entered with _sweep_interval is None; "
                "start() must guarantee a non-None value before launching the "
                "sweep task"
            )
        _LOGGER.info(
            f"[Evictor] Sweep loop started for "
            f"{self._registry.__class__.__name__} (interval={self._sweep_interval}s)"
        )
        try:
            while True:
                await asyncio.sleep(self._sweep_interval)
                try:
                    await self._sweep_once()
                except Exception:
                    _LOGGER.exception("[Evictor] sweep failed; continuing")
        except asyncio.CancelledError:
            _LOGGER.info(
                f"[Evictor] Sweep loop cancelled for "
                f"{self._registry.__class__.__name__}"
            )
            raise

    async def _sweep_once(self) -> None:
        """One eviction pass over the registry's items.

        1. Snapshot ``(name, manager)`` pairs via the registry's public
           :meth:`BaseRegistry.get_all` (the registry takes its own lock
           during the snapshot).
        2. For each pair, call
           :meth:`BaseItemManager.maybe_close_if_idle` — the manager's
           own lock serializes idle-check + close against concurrent
           :meth:`BaseItemManager.get` calls.
        3. Managers whose :attr:`BaseItemManager.evicts_on_idle` is
           ``True`` and that were just closed are removed from the
           registry via :meth:`BaseRegistry.remove(name, expected=manager)`
           — identity-checked, so a same-key re-add between the snapshot
           and the removal does not evict the new entry.
        """
        if self._idle_timeout is None:
            raise InternalError(
                "[Evictor] _sweep_once invoked with _idle_timeout is None; "
                "start() must guarantee a non-None value before launching the "
                "sweep task"
            )
        timeout = self._idle_timeout
        now = time.monotonic()

        snapshot = await self._registry.get_all()
        total = len(snapshot.items)
        closed_count = 0
        removed_count = 0
        skipped_count = 0

        for name, manager in snapshot.items.items():
            if not isinstance(manager, BaseItemManager):
                # Defensive: the registry's generic type only constrains items
                # to AsyncClosable.  Items that are not BaseItemManager don't
                # expose maybe_close_if_idle / evicts_on_idle and can't be
                # processed by the sweep.  In normal use this never fires;
                # log at WARNING so a misconfiguration is visible.
                _LOGGER.warning(
                    f"[Evictor] Skipping {name!r}: item is not a "
                    f"BaseItemManager (type={type(manager).__name__})"
                )
                skipped_count += 1
                continue
            try:
                closed = await manager.maybe_close_if_idle(timeout, now)
            except Exception:
                _LOGGER.exception(f"[Evictor] eviction failed for {name!r}")
                continue
            if not closed:
                continue
            closed_count += 1
            if not manager.evicts_on_idle:
                continue
            try:
                removed = await self._registry.remove(name, expected=manager)
            except Exception:
                _LOGGER.exception(f"[Evictor] removal failed for {name!r}")
                continue
            if removed is not None:
                removed_count += 1
                _LOGGER.info(
                    f"[Evictor] Removed evicted manager {name!r} from "
                    f"{self._registry.__class__.__name__}"
                )
            else:
                # Identity-checked remove returned None — the registry
                # entry under this key is no longer the manager we evicted
                # (e.g. the entry was concurrently replaced).  Not an error;
                # log at DEBUG so an audit can still see the race.
                _LOGGER.debug(
                    f"[Evictor] Skipped removal of {name!r}: registry entry "
                    f"is no longer the evicted manager (concurrent replacement)"
                )

        _LOGGER.debug(
            f"[Evictor] Sweep complete on {self._registry.__class__.__name__}: "
            f"{total} item(s) scanned, {closed_count} closed, "
            f"{removed_count} removed, {skipped_count} skipped"
        )
