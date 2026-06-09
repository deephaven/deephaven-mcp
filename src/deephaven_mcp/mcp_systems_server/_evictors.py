"""Per-child evictor pool for the multiplexed systems server.

The systems server attaches one :class:`Evictor` per child registry —
the community child (when present) plus one per enterprise system —
each parameterized by that section's own ``timeouts.eviction`` block.
This module owns the *pool*: the atomic-startup-with-rollback discipline
on :meth:`EvictorPool.start` and the concurrent-stop-with-error-isolation
discipline on :meth:`EvictorPool.stop`.

The pool exposes explicit ``start()`` / ``stop()`` lifecycle methods,
paired by the lifespan via
:meth:`contextlib.AsyncExitStack.push_async_callback`.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator

from deephaven_mcp.mcp_systems_server.config import ConfigTree
from deephaven_mcp.resource_manager import (
    BaseRegistry,
    EvictionTimeouts,
    Evictor,
    MultiSystemRegistry,
)

_LOGGER = logging.getLogger(__name__)

__all__ = ["EvictorPool"]


class EvictorPool:
    """Explicit-lifecycle owner of one :class:`Evictor` per child registry.

    Lifecycle:

    - :meth:`start` builds and starts evictors in declaration order
      (community first when present, then enterprise systems in
      ``multi_config.enterprise.systems`` iteration order). Startup is
      **atomic**: on a mid-loop failure, every already-started evictor
      is stopped (best-effort) before the original exception is
      re-raised. Callers therefore see either a fully-started pool or
      no surviving state to clean up.
    - :meth:`stop` stops every evictor concurrently. Per-evictor
      ``stop()`` failures are logged at ERROR but never re-raised:
      shutdown must continue regardless. An empty pool (no community,
      no enterprise) is a clean no-op.

    Each evictor reads its umbrella section's
    ``settings.timeouts.eviction`` block. Enterprise evictors share one
    :class:`EvictionTimeouts` across every system in the section;
    callers that want different timers per enterprise system today must
    construct a ``ConfigTree`` whose enterprise settings carry
    the values they want — there is no parallel scalar override path.
    """

    def __init__(
        self,
        registry: MultiSystemRegistry,
        multi_config: ConfigTree,
    ) -> None:
        """Capture the registry and configuration.

        The lifespan is responsible for ensuring ``registry`` was
        constructed from ``multi_config``; divergence is a
        registry-construction bug, not a pool-level concern.
        """
        self._registry = registry
        self._multi_config = multi_config
        self._evictors: list[Evictor] = []

    @property
    def evictors(self) -> list[Evictor]:
        """Currently-started evictors, in startup order (copy)."""
        return list(self._evictors)

    async def start(self) -> None:
        """Build and start one evictor per child registry, atomically.

        Pair every successful :meth:`start` with a :meth:`stop`.
        On failure :meth:`start` performs its own rollback before
        re-raising; the caller does not need to call :meth:`stop`
        in that case (and doing so is a clean no-op).

        Raises:
            Exception: Any exception raised by :meth:`Evictor.start`
                is re-raised after partial cleanup of
                already-started evictors.
        """
        try:
            for child, timeouts in self._eviction_targets():
                ev = Evictor(child, timeouts)
                await ev.start()
                self._evictors.append(ev)
                _LOGGER.info(
                    f"[_evictors:EvictorPool] Started evictor for "
                    f"{child.__class__.__name__} "
                    f"(idle={timeouts.session_idle_timeout_seconds}, "
                    f"sweep={timeouts.sweep_interval_seconds})"
                )
        except BaseException as exc:
            _LOGGER.error(
                f"[_evictors:EvictorPool] Partial-startup failure: {exc!r}; "
                f"rolling back {len(self._evictors)} already-started evictor(s)",
                exc_info=True,
            )
            for ev in reversed(self._evictors):
                try:
                    await ev.stop()
                except Exception:
                    _LOGGER.exception(
                        f"[_evictors:EvictorPool] Error stopping {ev!r} during rollback"
                    )
            self._evictors = []
            raise

    async def stop(self) -> None:
        """Stop every started evictor concurrently, logging per-evictor failures.

        An empty pool is a clean no-op. ``stop()`` failures are
        logged at ERROR but never re-raised — shutdown continues
        regardless. Calling :meth:`stop` when the pool was never
        started (or rolled back during :meth:`start`) is a no-op.
        """
        evictors = self._evictors
        if not evictors:
            return
        results = await asyncio.gather(
            *(ev.stop() for ev in evictors),
            return_exceptions=True,
        )
        for ev, result in zip(evictors, results, strict=True):
            if isinstance(result, BaseException):
                _LOGGER.error(
                    "[_evictors:EvictorPool] Error stopping evictor %r: %r",
                    ev,
                    result,
                    exc_info=result,
                )
        # Clear the list so re-use of the pool starts clean.
        self._evictors = []

    def _eviction_targets(
        self,
    ) -> Iterator[tuple[BaseRegistry, EvictionTimeouts]]:
        """Yield ``(child_registry, EvictionTimeouts)`` pairs to supervise.

        Community contributes 0 or 1 pairs (when both registry and
        config carry community); enterprise contributes 0..N pairs
        (one per system in declaration order).
        """
        community_child = self._registry.community
        community_cfg = self._multi_config.community
        if community_child is not None and community_cfg is not None:
            yield community_child, community_cfg.settings.timeouts.eviction
        enterprise_cfg = self._multi_config.enterprise
        if enterprise_cfg is not None:
            timeouts = enterprise_cfg.settings.timeouts.eviction
            for child in self._registry.enterprise_systems.values():
                yield child, timeouts
