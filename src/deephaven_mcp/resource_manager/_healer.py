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
    The healer holds no lock. Every read and write of outage state happens in
    straight-line synchronous code, which the single-threaded asyncio event
    loop runs to completion without interleaving. Introducing an ``await``
    between a read and its dependent write would break that property and
    require a lock.
"""

import asyncio
import logging
import time
from typing import Protocol

from deephaven_mcp.client import (
    CONTROLLER_SUBSCRIBING_ERROR_CODE,
    EnterpriseClientTimeouts,
)

from ._session_id import QualifiedSessionId

_LOGGER = logging.getLogger(__name__)

_HEALTHY_POLL_SECONDS = 5.0
"""Cadence at which the healer checks a healthy factory for a wedge.

Deliberately not configurable: it only bounds how quickly a wedge is noticed,
and the check is a lock-guarded attribute read with no I/O. Keeping it separate
from ``controller_resubscribe_backoff_initial_seconds`` is what gives that
tunable phase-independent meaning -- the backoff clock starts when the outage
is detected, not when the poll loop happens to tick.
"""


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

    Tracks a single *outage* -- an uninterrupted stretch during which the
    controller subscription is wedged -- and, for its duration, owns factory
    creation on behalf of the source. The outage carries the timestamp it began,
    the number of recreate attempts made, and the deadline for the next one, all
    of which feed the status message that callers receive instead of blocking.

    The background loop runs two cadences so the configured backoff has
    phase-independent meaning:

    - **No outage**: poll every :data:`_HEALTHY_POLL_SECONDS` purely to notice a
      wedge, publishing no next-recreate deadline because none is scheduled.
    - **Outage in progress**: wait until the outage's next-recreate deadline,
      which :meth:`healing_status_message` reports as an accurate countdown.

    That deadline is armed the moment the outage opens -- by the loop's own
    poll or by a foreground caller reporting a wedge through
    :meth:`note_wedged` -- and a non-forced pass recreates only once it has
    arrived. The first recreate therefore lands
    ``controller_resubscribe_backoff_initial_seconds`` after detection
    regardless of where in the poll cycle the wedge occurred, and regardless of
    who detected it.
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
        self._subscribing_since: float | None = None
        self._attempts = 0
        self._next_recreate_monotonic: float | None = None

    @property
    def outage_active(self) -> bool:
        """Whether an outage is currently being tracked."""
        return self._subscribing_since is not None

    def _reset_outage(self) -> None:
        """Clear all outage state, returning the loop to its healthy cadence."""
        self._subscribing_since = None
        self._attempts = 0
        self._next_recreate_monotonic = None

    def _backoff_seconds(self) -> float:
        """Return the delay before the next recreate attempt.

        Doubles ``controller_resubscribe_backoff_initial_seconds`` once per
        recreate attempt already made in the current outage, capped at
        ``controller_resubscribe_backoff_max_seconds``. With zero attempts the
        delay is the initial value, so the first recreate of an outage waits
        exactly that long.

        Returns:
            float: Seconds to wait. Never exceeds the configured maximum, which
                also means a maximum below the initial value pins every delay to
                the maximum.
        """
        initial = self._timeouts.controller_resubscribe_backoff_initial_seconds
        maximum = self._timeouts.controller_resubscribe_backoff_max_seconds
        # Bound the exponent so a long outage cannot overflow the shift.
        exponent = min(self._attempts, 32)
        return min(initial * float(2**exponent), maximum)

    def _arm_backoff(self, now: float) -> float:
        """Schedule the outage's next recreate one backoff delay from ``now``.

        Args:
            now (float): A ``time.monotonic()`` reading taken by the caller.

        Returns:
            float: The monotonic deadline that was published.
        """
        deadline = now + self._backoff_seconds()
        self._next_recreate_monotonic = deadline
        return deadline

    def _recreate_due(self, now: float) -> bool:
        """Whether the current outage's scheduled recreate deadline has arrived.

        Args:
            now (float): A ``time.monotonic()`` reading taken by the caller.

        Returns:
            bool: ``True`` once the published deadline has passed; ``False``
                while it is still pending or when none is scheduled.
        """
        deadline = self._next_recreate_monotonic
        return deadline is not None and now >= deadline

    def _status_message(self, now: float) -> str:
        """Build the "still subscribing" status message for the current outage.

        Args:
            now (float): A ``time.monotonic()`` reading taken by the caller.

        Returns:
            str: A message prefixed with
                :data:`CONTROLLER_SUBSCRIBING_ERROR_CODE` describing how long
                the subscription has been wedged, how many recreate attempts
                have been made, the countdown to the next recreate, and how to
                force an immediate reconnect.
        """
        waited = now - self._subscribing_since if self._subscribing_since else 0.0
        if self._next_recreate_monotonic is not None:
            next_in = max(0.0, self._next_recreate_monotonic - now)
        else:
            next_in = self._backoff_seconds()
        return (
            f"[{CONTROLLER_SUBSCRIBING_ERROR_CODE}] Controller subscription for "
            f"'{self._source.qualified_session_id}' is still initializing "
            f"(waited {waited:.0f}s, {self._attempts} recreate "
            f"attempt(s) so far); the next automatic recreate is in "
            f"~{next_in:.0f}s. Retry this call shortly, or call the "
            f"enterprise_controller_reconnect tool with "
            f"system='{self._source.system}' to force an immediate reconnect."
        )

    def note_healthy(self) -> None:
        """Record that the controller subscription is healthy, ending any outage."""
        self._reset_outage()

    def note_wedged(self, now: float) -> str:
        """Record that the controller subscription is wedged and describe the outage.

        Opens an outage if none is in progress, arming its first recreate one
        full ``controller_resubscribe_backoff_initial_seconds`` from ``now`` so
        the delay the returned message advertises is the delay the loop
        actually waits. An outage already in progress keeps its original start
        time and deadline, so the reported wait keeps growing.

        Args:
            now (float): A ``time.monotonic()`` reading taken by the caller.

        Returns:
            str: The status message from :meth:`_status_message`.
        """
        if self._subscribing_since is None:
            self._subscribing_since = now
            self._arm_backoff(now)
        return self._status_message(now)

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
        return self._status_message(now) if self.outage_active else None

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
        self._reset_outage()

    async def request_reconnect(self) -> bool:
        """Ask the loop to run a recreate pass immediately.

        Signals the loop rather than recreating inline, so this returns without
        waiting for the (potentially minutes-long) factory rebuild and
        concurrent requests collapse into a single attempt.

        The nudge is sent only when a pass would actually do something: the
        cached controller is wedged, or an outage is in progress with the cache
        momentarily empty. A healthy controller is left alone, and a source with
        nothing cached and no outage has nothing to reconnect. Nothing is queued
        for later in those cases; the loop polls often enough to notice a wedge
        on its own.

        Returns:
            bool: ``True`` when a running loop was nudged and its next pass will
                attempt a recreate; ``False`` when no attempt was started,
                because nothing is wedged or no loop is running.
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

    async def _wait_for_next_pass(self, delay: float) -> bool:
        """Wait ``delay`` seconds, returning early if a reconnect was requested.

        Args:
            delay (float): Seconds to wait before the next healing pass.

        Returns:
            bool: ``True`` when :meth:`request_reconnect` cut the wait short,
                ``False`` when the full delay elapsed.
        """
        try:
            await asyncio.wait_for(self._requested.wait(), timeout=delay)
        except TimeoutError:
            return False
        self._requested.clear()
        return True

    async def _loop(self) -> None:
        """Run healing passes on the healthy or backoff cadence until canceled.

        Schedules each pass per the two cadences described on the class,
        publishing the next-recreate deadline only while an outage is in
        progress. :meth:`request_reconnect` cuts any wait short. A pass that
        raises is logged and the loop continues.
        """
        _LOGGER.debug(
            f"[ControllerHealer:_loop] Entered for "
            f"'{self._source.qualified_session_id}'"
        )
        try:
            while True:
                now = time.monotonic()
                if self._subscribing_since is None:
                    delay = _HEALTHY_POLL_SECONDS
                    self._next_recreate_monotonic = None
                else:
                    # An outage opened by a foreground caller mid-wait already
                    # armed its deadline; honor it instead of restarting the
                    # clock from this tick.
                    deadline = self._next_recreate_monotonic
                    if deadline is None:
                        deadline = self._arm_backoff(now)
                    delay = max(0.0, deadline - now)
                forced = await self._wait_for_next_pass(delay)
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

        The pass that first *detects* a wedge only opens the outage and arms
        its backoff; a non-forced pass then recreates only once that deadline
        has arrived, so the configured initial delay is honored whether the
        outage was opened by this loop or by a foreground caller through
        :meth:`note_wedged` mid-wait. A forced pass skips the wait entirely.

        Args:
            forced (bool): Whether this pass was triggered by
                :meth:`request_reconnect` rather than by the backoff elapsing.
                A forced pass recreates on detection instead of waiting out the
                backoff; it still never tears down a healthy controller.
        """
        poisoned = await self._source.peek_controller_poisoned()

        if poisoned is False:
            self._reset_outage()
            return
        if poisoned is None and not self.outage_active:
            return

        now = time.monotonic()
        if not self.outage_active:
            self._subscribing_since = now
            self._arm_backoff(now)
            if not forced:
                return
        elif not forced and not self._recreate_due(now):
            return

        self._attempts += 1
        # Re-armed by the loop once the rebuild returns, so the escalated
        # backoff is measured from the end of this attempt.
        self._next_recreate_monotonic = None

        _LOGGER.warning(
            f"[ControllerHealer:heal_once] Controller subscription for "
            f"'{self._source.qualified_session_id}' unavailable; recreating "
            f"factory (attempt {self._attempts}, forced={forced})"
        )
        await self._source.rebuild_factory()
