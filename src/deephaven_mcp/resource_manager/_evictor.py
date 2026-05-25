"""Per-registry idle-eviction sweeper.

Provides :class:`Evictor`, a coordinator object that runs a periodic
background sweep over one :class:`BaseRegistry`, asking each managed
item whether it has been idle past a configured timeout and removing
items whose :attr:`BaseItemManager.evicts_on_idle` is ``True`` from the
registry after closure.

The Evictor holds a reference to its registry and uses only the
registry's existing public collection API
(:meth:`BaseRegistry.snapshot_items` for cheap iteration and
:meth:`BaseRegistry.remove` for identity-checked removal); no registry
methods are exposed solely for the Evictor.

Lifecycle:

- Construct: ``evictor = Evictor(registry, idle_timeout_seconds=...)``
- ``await evictor.start()`` — launches the background sweep loop.
- ``await evictor.stop()`` — cancels and awaits the loop; idempotent,
  safe to call multiple times and safe to call without a prior ``start``.

Concurrency:

- ``_sweeper_task`` is read and written under ``self._lock``.
- The sweep loop snapshots the registry's items under the registry's
  own lock (via :meth:`BaseRegistry.snapshot_items`), then iterates
  candidates outside any lock; per-item idleness is checked under each
  manager's lock inside :meth:`BaseItemManager.maybe_close_if_idle`.
  ``snapshot_items`` performs no network I/O or refresh, so the sweep
  never forces enterprise registries to re-query their controller
  purely for the sake of eviction.
- Final drop happens via :meth:`BaseRegistry.remove(name, expected=manager)`
  — identity-checked, so a same-key re-add between the snapshot and the
  removal does not evict the new entry.
"""

import asyncio
import logging
import time
from typing import Annotated

from pydantic import Field

from .._exceptions import InternalError
from .._pydantic import StrictSchema
from ._manager import BaseItemManager
from ._registry import BaseRegistry

_LOGGER = logging.getLogger(__name__)

__all__ = ["Evictor", "EvictionTimeouts"]


class EvictionTimeouts(StrictSchema):
    """Durations the MCP-side idle-session eviction sweeper applies.

    Drives the per-section eviction sweeper that removes MCP-cached
    sessions which have been unused for ``session_idle_timeout_seconds``.
    Independent of any worker-side timeout
    (e.g. ``EnterpriseSessionCreationDefaults.auto_delete_timeout``).

    Shared between community and enterprise — both sides have the same
    eviction-sweeper contract.
    """

    session_idle_timeout_seconds: Annotated[float, Field(gt=0)] = 3600.0
    """How long (seconds) an idle MCP-cached session is retained
    before the eviction sweeper removes it."""

    sweep_interval_seconds: Annotated[float, Field(gt=0)] = 60.0
    """Cadence (seconds) at which the idle-eviction sweep runs.
    Drives how quickly idle sessions are reclaimed once
    ``session_idle_timeout_seconds`` is exceeded."""


class Evictor:
    """Idle-eviction sweep coordinator for one :class:`BaseRegistry`.

    One Evictor instance per registry.  Holds the timing parameters and
    the background sweeper task lifecycle; defers the per-item decisions
    to :meth:`BaseItemManager.maybe_close_if_idle` and reads
    :attr:`BaseItemManager.evicts_on_idle` to decide whether to remove
    the manager from the registry after closing it.

    Args:
        registry (BaseRegistry): The registry to sweep.
        timeouts (EvictionTimeouts | None): Sweep timing. ``None``
            disables the sweeper entirely (useful for tests and opt-out
            configs). When non-``None``, both fields are guaranteed
            positive by :class:`EvictionTimeouts` validation.
    """

    def __init__(
        self,
        registry: BaseRegistry,
        timeouts: EvictionTimeouts | None = None,
    ) -> None:
        self._registry = registry
        self._timeouts: EvictionTimeouts | None = timeouts
        self._sweeper_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        _LOGGER.debug(
            f"[Evictor] created for {registry.__class__.__name__} "
            f"(timeouts={timeouts!r})"
        )

    async def start(self) -> None:
        """Launch the background sweep loop.

        No-op when ``timeouts`` is ``None`` (the sweeper is disabled),
        or when the sweeper task already exists and is still running.
        """
        async with self._lock:
            if self._timeouts is None:
                _LOGGER.info(
                    f"[Evictor] Sweeper disabled for "
                    f"{self._registry.__class__.__name__} "
                    f"(timeouts=None)"
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
                f"(timeout={self._timeouts.session_idle_timeout_seconds}s, "
                f"interval={self._timeouts.sweep_interval_seconds}s)"
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

        Raises :class:`InternalError` if invoked with ``_timeouts`` set
        to ``None`` (start() must gate this branch).
        """
        if self._timeouts is None:
            raise InternalError(
                "[Evictor] _sweep_loop entered with _timeouts is None; "
                "start() must guarantee a non-None value before launching the "
                "sweep task"
            )
        sweep_interval = self._timeouts.sweep_interval_seconds
        _LOGGER.info(
            f"[Evictor] Sweep loop started for "
            f"{self._registry.__class__.__name__} (interval={sweep_interval}s)"
        )
        try:
            while True:
                await asyncio.sleep(sweep_interval)
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
           :meth:`BaseRegistry.snapshot_items` (the registry takes its
           own lock during the snapshot, but performs no refresh or
           network I/O).
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
        if self._timeouts is None:
            raise InternalError(
                "[Evictor] _sweep_once invoked with _timeouts is None; "
                "start() must guarantee a non-None value before launching the "
                "sweep task"
            )
        timeout = self._timeouts.session_idle_timeout_seconds
        now = time.monotonic()

        items = await self._registry.snapshot_items()
        total = len(items)
        closed_count = 0
        removed_count = 0
        skipped_count = 0

        for name, manager in items.items():
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
