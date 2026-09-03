"""Background healing for a wedged Deephaven Enterprise controller subscription.

A Core+ controller client can get stuck in ``SubState.SUBSCRIBING``: the vendor
client keeps the subscription object alive but never completes it, so every
controller-backed call blocks until its own timeout. The only recovery is to
recreate the whole session factory.

:class:`ControllerHealer` owns that recovery for one factory. It runs a
background loop that watches the factory's controller subscription and, while
wedged, recreates the factory on a capped exponential backoff until a fresh
controller subscribes cleanly. Callers never block on the recovery: they ask
the healer for an immediate status message and fail fast with
:data:`~deephaven_mcp.client.CONTROLLER_SUBSCRIBING_ERROR_CODE`.

The healer holds no factory and no cache. Every operation that touches the
cached factory is delegated to a :class:`HealableFactorySource` -- in
production, the owning
:class:`~deephaven_mcp.resource_manager.CorePlusSessionFactoryManager`.

Concurrency:
    The healer holds no lock: every state change is straight-line synchronous
    code that the single-threaded asyncio event loop runs without interleaving.
    A recreate attempt is the one operation that spans an ``await``, and it
    re-arms the deadline on the outage record it captured before starting -- so
    a pass superseded mid-rebuild, by a caller reporting the controller
    healthy, updates only its own detached record and cannot revive a closed
    outage.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Protocol

from deephaven_mcp.client import (
    CONTROLLER_SUBSCRIBING_ERROR_CODE,
    EnterpriseClientTimeouts,
)

from ._session_id import QualifiedSessionId

_LOGGER = logging.getLogger(__name__)

_POLL_SECONDS = 5.0
"""How often the healing loop wakes to re-read the subscription state.

Deliberately not configurable: it only bounds how quickly a change is noticed.
Recreates are paced by the outage's own deadline, so this is a resolution limit
on ``controller_resubscribe_backoff_initial_seconds``, not a part of it.
"""


@dataclass
class _Outage:
    """One uninterrupted stretch during which the controller subscription is wedged."""

    since: float
    """``time.monotonic()`` reading at which the outage was first observed."""

    next_recreate: float
    """``time.monotonic()`` deadline for the next recreate attempt."""

    attempts: int = 0
    """Recreate attempts made so far during this outage."""


class HealableFactorySource(Protocol):
    """The factory-cache operations a :class:`ControllerHealer` drives.

    Implemented by
    :class:`~deephaven_mcp.resource_manager.CorePlusSessionFactoryManager`,
    which owns the cached factory and the lock guarding it. Every member is
    safe to call without holding any healer state.
    """

    @property
    def qualified_session_id(self) -> QualifiedSessionId:
        """Identifier of the managed factory, used in log and status messages."""

    @property
    def system(self) -> str:
        """Enterprise system name, named by the status message's remediation hint."""

    async def peek_controller_poisoned(self) -> bool | None:
        """Report the cached controller's poison state without creating a factory.

        Returns:
            bool | None: ``None`` when no factory is cached; otherwise whether
                the cached factory's controller subscription is wedged in
                ``SUBSCRIBING``. ``None`` is ambiguous on its own -- it means
                either "idle, nothing to heal" or "the last recreate failed
                mid-outage" -- and is disambiguated by the healer's outage
                state.
        """

    async def rebuild_factory(self) -> None:
        """Discard the cached factory if still wedged, then create a replacement.

        Never raises: a failed creation is logged and leaves the cache empty,
        which the healer treats as a continuing outage.
        """


class ControllerHealer:
    """Recreates one enterprise factory while its controller subscription is wedged.

    Tracks a single :class:`_Outage` and, for its duration, owns factory
    creation on behalf of the source; the outage also feeds the status message
    callers receive instead of blocking.

    The background loop wakes every :data:`_POLL_SECONDS`, or immediately on
    :meth:`request_reconnect`, and re-reads the current state before deciding
    anything -- it never acts on a decision made before it slept. Recreates are
    paced by ``_Outage.next_recreate``, armed when the outage opens and re-armed
    after every attempt, so the configured backoff is honored no matter who
    detected the wedge, and the countdown the status message reports is the one
    the loop actually waits.
    """

    def __init__(
        self,
        source: HealableFactorySource,
        timeouts: EnterpriseClientTimeouts,
    ) -> None:
        """Create a healer for one factory source.

        The loop does not run until :meth:`start` is called.

        Args:
            source (HealableFactorySource): Owner of the cached factory, whose
                controller subscription this healer watches and whose factory it
                recreates.
            timeouts (EnterpriseClientTimeouts): Supplies
                ``controller_resubscribe_backoff_initial_seconds`` and
                ``controller_resubscribe_backoff_max_seconds``.
        """
        self._source = source
        self._timeouts = timeouts
        self._task: asyncio.Task[None] | None = None
        self._requested = asyncio.Event()
        self._outage: _Outage | None = None

    @property
    def outage_active(self) -> bool:
        """Whether an outage is currently being tracked."""
        return self._outage is not None

    def _backoff_seconds(self, attempts: int) -> float:
        """Return the delay before the next recreate attempt.

        Doubles ``controller_resubscribe_backoff_initial_seconds`` once per
        recreate attempt already made in the current outage, saturating at
        ``controller_resubscribe_backoff_max_seconds``.

        Args:
            attempts (int): Recreate attempts already made in this outage. Zero
                yields the configured initial delay.

        Returns:
            float: Seconds to wait. Never exceeds the configured maximum, which
                also means a maximum below the initial value pins every delay to
                the maximum.
        """
        maximum = self._timeouts.controller_resubscribe_backoff_max_seconds
        delay = self._timeouts.controller_resubscribe_backoff_initial_seconds
        # Saturating rather than exponent-capped: reaching the maximum is what
        # bounds the doubling, so a tiny initial value still gets there.
        for _ in range(attempts):
            if delay >= maximum:
                break
            delay *= 2.0
        return min(delay, maximum)

    def _open_outage(self, now: float) -> _Outage:
        """Start tracking an outage, scheduling its first recreate.

        Args:
            now (float): A ``time.monotonic()`` reading taken by the caller.

        Returns:
            _Outage: The newly opened outage, also stored on the healer.
        """
        self._outage = _Outage(
            since=now, next_recreate=now + self._backoff_seconds(attempts=0)
        )
        return self._outage

    def _status_message(self, outage: _Outage, now: float) -> str:
        """Build the "still subscribing" status message for an outage.

        Args:
            outage (_Outage): The outage to describe.
            now (float): A ``time.monotonic()`` reading taken by the caller.

        Returns:
            str: A message prefixed with
                :data:`CONTROLLER_SUBSCRIBING_ERROR_CODE` describing how long
                the subscription has been wedged, how many recreate attempts
                have been made, the countdown to the next recreate, and how to
                force an immediate reconnect.
        """
        return (
            f"[{CONTROLLER_SUBSCRIBING_ERROR_CODE}] Controller subscription for "
            f"'{self._source.qualified_session_id}' is still initializing "
            f"(waited {now - outage.since:.0f}s, {outage.attempts} recreate "
            f"attempt(s) so far); the next automatic recreate is in "
            f"~{max(0.0, outage.next_recreate - now):.0f}s. Retry this call "
            f"shortly, or call the enterprise_controller_reconnect tool with "
            f"system='{self._source.system}' to force an immediate reconnect."
        )

    def note_healthy(self) -> None:
        """Record that the controller subscription is healthy, ending any outage."""
        self._outage = None

    def note_wedged(self, now: float) -> str:
        """Record that the controller subscription is wedged and describe the outage.

        Opens an outage if none is in progress; one already in progress keeps
        its start time, attempt count, and recreate deadline, so the reported
        wait keeps growing and the countdown stays the one the loop is waiting.

        Args:
            now (float): A ``time.monotonic()`` reading taken by the caller.

        Returns:
            str: The status message from :meth:`_status_message`.
        """
        outage = self._outage if self._outage is not None else self._open_outage(now)
        return self._status_message(outage, now)

    def healing_status_message(self, now: float) -> str | None:
        """Return the outage status message, or ``None`` when nothing is wedged.

        Lets a caller that cannot observe the controller directly -- because the
        cache is momentarily empty between a discard and its rebuild -- still
        distinguish "the healer owns creation right now" from "idle".

        Args:
            now (float): A ``time.monotonic()`` reading taken by the caller.

        Returns:
            str | None: The message from :meth:`_status_message` while an outage
                is in progress; ``None`` otherwise.
        """
        outage = self._outage
        return self._status_message(outage, now) if outage is not None else None

    def start(self) -> None:
        """Launch the background healing loop.

        Idempotent: a no-op when the loop is already running. Requires a running
        event loop.
        """
        if self._task is not None and not self._task.done():
            _LOGGER.debug(
                f"[ControllerHealer:start] Already running for "
                f"'{self._source.qualified_session_id}'"
            )
            return
        self._task = asyncio.create_task(self._loop())
        _LOGGER.info(
            f"[ControllerHealer:start] Started subscription healer for "
            f"'{self._source.qualified_session_id}'"
        )

    async def stop(self) -> None:
        """Cancel and await the background healing loop, then clear outage state.

        The outage is cleared because nothing is left to act on it: a stale
        outage surviving into a restarted healer would make the source keep
        failing fast against a cache the stopped loop never refilled.

        Idempotent: safe to call when the loop was never started or already
        stopped. Never raises.
        """
        task = self._task
        self._task = None
        if task is not None and not task.done():
            _LOGGER.info(
                f"[ControllerHealer:stop] Stopping subscription healer for "
                f"'{self._source.qualified_session_id}'"
            )
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._outage = None
        self._requested.clear()

    async def request_reconnect(self) -> bool:
        """Ask the loop to run a recreate pass immediately.

        Signals the loop rather than recreating inline, so this returns without
        waiting for the (potentially minutes-long) factory rebuild. The signal
        is a single flag, so repeated requests coalesce into one pending pass
        rather than queuing a backlog of attempts.

        The signal is sent only when a pass would actually do something: the
        cached controller is wedged, or an outage is in progress with the cache
        momentarily empty. A healthy controller is left alone, and a source with
        nothing cached and no outage has nothing to reconnect. Nothing is queued
        for later in those cases; the loop polls often enough to notice a wedge
        on its own.

        Returns:
            bool: ``True`` when a running loop was signaled and its next pass
                will attempt a recreate; ``False`` when no attempt was
                requested, because nothing is wedged or no loop is running.
        """
        poisoned = await self._source.peek_controller_poisoned()
        running = self._task is not None and not self._task.done()
        # ``None`` (nothing cached) is only actionable mid-outage, when the
        # healer is between discarding a wedged factory and rebuilding it.
        heal_would_act = poisoned is True or (poisoned is None and self.outage_active)
        actionable = running and heal_would_act
        if actionable:
            self._requested.set()
        _LOGGER.info(
            f"[ControllerHealer:request_reconnect] Reconnect requested for "
            f"'{self._source.qualified_session_id}' (healer_running={running}, "
            f"actionable={actionable})"
        )
        return actionable

    async def _wait_for_next_pass(self) -> bool:
        """Wait one poll interval, returning early if a reconnect was requested.

        Consumes the request, so requests raised before the pass starts all
        coalesce into it.

        Returns:
            bool: ``True`` when :meth:`request_reconnect` cut the wait short,
                ``False`` when the full interval elapsed.
        """
        try:
            await asyncio.wait_for(self._requested.wait(), timeout=_POLL_SECONDS)
        except TimeoutError:
            return False
        self._requested.clear()
        return True

    async def _loop(self) -> None:
        """Run a healing pass every poll interval until canceled.

        :meth:`request_reconnect` cuts the wait short. A pass that raises is
        logged and the loop continues.
        """
        _LOGGER.debug(
            f"[ControllerHealer:_loop] Entered for "
            f"'{self._source.qualified_session_id}'"
        )
        try:
            while True:
                forced = await self._wait_for_next_pass()
                try:
                    await self.heal_once(forced=forced)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    _LOGGER.exception(
                        f"[ControllerHealer:_loop] Heal pass failed for "
                        f"'{self._source.qualified_session_id}'; continuing"
                    )
        except asyncio.CancelledError:
            _LOGGER.debug(
                f"[ControllerHealer:_loop] Canceled for "
                f"'{self._source.qualified_session_id}'"
            )
            raise

    async def heal_once(self, forced: bool = False) -> None:
        """Run one healing pass: recreate the factory iff the subscription is wedged.

        A pass recreates when the cached controller is wedged, or when nothing
        is cached *while an outage is in progress* -- the latter is how a
        recreate that failed outright (leaving the cache empty) keeps the
        outage, and therefore the backoff escalation, alive. With no outage in
        progress an empty cache simply means idle, and the pass does nothing.

        The pass that first *detects* a wedge only opens the outage; the
        recreate waits out the deadline that opening armed. A forced pass skips
        that wait.

        Args:
            forced (bool): Whether this pass was triggered by
                :meth:`request_reconnect` rather than by the backoff elapsing.
                A forced pass recreates on detection instead of waiting out the
                backoff; it still never tears down a healthy controller.
        """
        poisoned = await self._source.peek_controller_poisoned()
        if poisoned is False:
            self._outage = None
            return

        outage = self._outage
        if outage is None:
            if poisoned is None:
                return
            outage = self._open_outage(time.monotonic())
            if not forced:
                return
        elif not forced and time.monotonic() < outage.next_recreate:
            return

        outage.attempts += 1
        _LOGGER.warning(
            f"[ControllerHealer:heal_once] Controller subscription for "
            f"'{self._source.qualified_session_id}' unavailable; recreating "
            f"factory (attempt {outage.attempts}, forced={forced})"
        )
        await self._source.rebuild_factory()
        outage.next_recreate = time.monotonic() + self._backoff_seconds(outage.attempts)
