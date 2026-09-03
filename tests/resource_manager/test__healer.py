"""Tests for deephaven_mcp.resource_manager._healer."""

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from deephaven_mcp._taxonomy import SystemType
from deephaven_mcp.client import (
    CONTROLLER_SUBSCRIBING_ERROR_CODE,
    EnterpriseClientTimeouts,
)
from deephaven_mcp.resource_manager._healer import ControllerHealer
from deephaven_mcp.resource_manager._session_id import QualifiedSessionId, SessionId

_HEALER_MODULE = "deephaven_mcp.resource_manager._healer"


class _FakeSource:
    """Minimal HealableFactorySource, standing in for the factory manager.

    The healer never touches a factory or a cache directly, so the whole
    manager-side surface it drives is these two coroutines plus two identity
    fields.
    """

    def __init__(self, poisoned: bool | None = None, system: str = "test_factory"):
        self.poisoned = poisoned
        self.system = system
        self.qualified_session_id = QualifiedSessionId(
            SystemType.ENTERPRISE, system, SessionId.from_int(0)
        )
        self.rebuild_calls = 0
        self.rebuild_hook = None

    async def peek_controller_poisoned(self) -> bool | None:
        return self.poisoned

    async def rebuild_factory(self) -> None:
        self.rebuild_calls += 1
        if self.rebuild_hook is not None:
            await self.rebuild_hook(self)


def _timeouts(initial: float = 30.0, maximum: float = 300.0):
    return EnterpriseClientTimeouts.model_validate(
        {
            "controller_resubscribe_backoff_initial_seconds": initial,
            "controller_resubscribe_backoff_max_seconds": maximum,
        }
    )


def _healer(poisoned: bool | None = None, **timeout_kwargs):
    source = _FakeSource(poisoned=poisoned)
    return ControllerHealer(source, _timeouts(**timeout_kwargs)), source


class TestOutageState:
    """Outage bookkeeping, backoff, and status rendering.

    All synchronous: the healer keeps no lock and no I/O on these paths, so
    they need no event loop to exercise.
    """

    def test_new_healer_has_no_outage(self):
        """A freshly constructed healer is idle."""
        healer, _ = _healer()
        assert healer.outage_active is False
        assert healer.healing_status_message(time.monotonic()) is None

    def test_note_wedged_opens_outage_and_returns_message(self):
        """The first wedged report opens the outage and describes it."""
        healer, _ = _healer()

        msg = healer.note_wedged(time.monotonic())

        assert healer.outage_active is True
        assert CONTROLLER_SUBSCRIBING_ERROR_CODE in msg
        assert "0 recreate attempt" in msg
        # The message names the escape hatch and the system to pass to it.
        assert "enterprise_controller_reconnect" in msg
        assert "system='test_factory'" in msg

    def test_note_wedged_arms_the_first_recreate(self):
        """Opening an outage starts the backoff clock at detection.

        Regression guard: a foreground caller can open the outage while the
        loop is mid-wait on its healthy cadence. Arming here is what keeps the
        first recreate one full initial backoff away from *detection* rather
        than from the loop's next tick.
        """
        healer, _ = _healer(initial=30.0)
        now = time.monotonic()

        healer.note_wedged(now)

        assert healer._next_recreate_monotonic == now + 30.0

    def test_note_wedged_preserves_original_start(self):
        """A second wedged report keeps the original outage-start timestamp."""
        healer, _ = _healer()

        healer.note_wedged(time.monotonic())
        first_since = healer._subscribing_since
        first_deadline = healer._next_recreate_monotonic
        healer.note_wedged(time.monotonic())

        assert healer._subscribing_since == first_since
        assert healer._next_recreate_monotonic == first_deadline

    def test_note_wedged_reports_growing_wait(self):
        """The reported wait grows with the time since the outage opened."""
        healer, _ = _healer()
        now = time.monotonic()
        healer.note_wedged(now)

        assert "waited 42s" in healer.note_wedged(now + 42.0)

    def test_note_healthy_clears_outage(self):
        """A healthy report clears the start time, attempts, and deadline."""
        healer, _ = _healer()
        healer.note_wedged(time.monotonic())
        healer._attempts = 4
        healer._next_recreate_monotonic = time.monotonic() + 10.0

        healer.note_healthy()

        assert healer.outage_active is False
        assert healer._attempts == 0
        assert healer._next_recreate_monotonic is None

    def test_healing_status_message_reports_scheduled_countdown(self):
        """A published deadline is reported as the countdown to the next recreate."""
        healer, _ = _healer()
        now = time.monotonic()
        healer.note_wedged(now)
        healer._attempts = 2
        healer._next_recreate_monotonic = now + 25.0

        msg = healer.healing_status_message(now)

        assert "next automatic recreate is in ~25s" in msg
        assert "2 recreate attempt" in msg

    def test_healing_status_message_falls_back_to_backoff(self):
        """With no deadline published the countdown falls back to the backoff.

        The deadline is momentarily unpublished between an attempt starting and
        the loop re-arming it after the rebuild returns.
        """
        healer, _ = _healer(initial=30.0)
        now = time.monotonic()
        healer.note_wedged(now)
        healer._next_recreate_monotonic = None

        assert "next automatic recreate is in ~30s" in healer.healing_status_message(
            now
        )

    def test_healing_status_message_never_reports_a_past_deadline(self):
        """An elapsed deadline is reported as zero, not as a negative countdown."""
        healer, _ = _healer()
        now = time.monotonic()
        healer.note_wedged(now)
        healer._next_recreate_monotonic = now - 5.0

        assert "in ~0s" in healer.healing_status_message(now)

    def test_backoff_doubles_and_caps(self):
        """The delay doubles per attempt and is capped at the configured maximum."""
        healer, _ = _healer(initial=10.0, maximum=60.0)

        healer._attempts = 0
        assert healer._backoff_seconds() == 10.0
        healer._attempts = 1
        assert healer._backoff_seconds() == 20.0
        healer._attempts = 2
        assert healer._backoff_seconds() == 40.0
        # Doubling would give 80.0; the cap wins.
        healer._attempts = 3
        assert healer._backoff_seconds() == 60.0
        # A very long outage stays at the cap without overflowing.
        healer._attempts = 10_000
        assert healer._backoff_seconds() == 60.0

    def test_backoff_max_below_initial_clamps(self):
        """A maximum below the initial delay pins every delay to the maximum."""
        healer, _ = _healer(initial=30.0, maximum=5.0)
        assert healer._backoff_seconds() == 5.0


@pytest.mark.asyncio
class TestHealOnce:
    """One healing pass, across every combination of poison state and outage."""

    async def test_healthy_ends_the_outage(self):
        """A healthy peek clears outage state and does not rebuild."""
        healer, source = _healer(poisoned=False)
        healer.note_wedged(time.monotonic())
        healer._attempts = 3

        await healer.heal_once()

        assert healer.outage_active is False
        assert healer._attempts == 0
        assert source.rebuild_calls == 0

    async def test_idle_with_no_cache_does_nothing(self):
        """Nothing cached and no outage is idle, not a wedge."""
        healer, source = _healer(poisoned=None)

        await healer.heal_once()

        assert source.rebuild_calls == 0
        assert healer.outage_active is False
        assert healer._attempts == 0

    async def test_detection_opens_outage_without_rebuilding(self):
        """The pass that first sees a wedge only opens the outage.

        The rebuild waits for the backoff armed at detection, so
        ``controller_resubscribe_backoff_initial_seconds`` is honored
        regardless of where in the poll cycle the wedge was noticed.
        """
        healer, source = _healer(poisoned=True)

        await healer.heal_once()

        assert healer.outage_active is True
        assert healer._attempts == 0
        assert healer._next_recreate_monotonic is not None
        assert source.rebuild_calls == 0

    async def test_pending_backoff_defers_the_rebuild(self):
        """A non-forced pass waits out the deadline armed when the outage opened.

        Regression guard: a foreground caller opens the outage through
        ``note_wedged`` while the loop is mid-wait on its 5s healthy cadence.
        Rebuilding on the very next tick would make the first attempt land
        within that poll rather than after the configured initial backoff.
        """
        healer, source = _healer(poisoned=True, initial=3600.0, maximum=3600.0)
        healer.note_wedged(time.monotonic())

        await healer.heal_once()

        assert source.rebuild_calls == 0
        assert healer._attempts == 0
        assert healer.outage_active is True

    async def test_forced_pass_ignores_a_pending_backoff(self):
        """A reconnect request rebuilds without waiting out the armed deadline."""
        healer, source = _healer(poisoned=True, initial=3600.0, maximum=3600.0)
        healer.note_wedged(time.monotonic())

        await healer.heal_once(forced=True)

        assert source.rebuild_calls == 1
        assert healer._attempts == 1

    async def test_forced_rebuilds_on_detection(self):
        """A forced pass rebuilds immediately instead of waiting out the backoff."""
        healer, source = _healer(poisoned=True)

        await healer.heal_once(forced=True)

        assert source.rebuild_calls == 1
        assert healer._attempts == 1
        assert healer.outage_active is True

    async def test_wedged_mid_outage_rebuilds_and_counts(self):
        """With the armed deadline elapsed, a wedged peek rebuilds and counts it."""
        healer, source = _healer(poisoned=True)
        healer.note_wedged(time.monotonic())
        healer._next_recreate_monotonic = time.monotonic() - 1.0

        await healer.heal_once()

        assert source.rebuild_calls == 1
        assert healer._attempts == 1
        # Cleared so the loop re-arms the escalated backoff from the attempt's end.
        assert healer._next_recreate_monotonic is None

    async def test_empty_cache_mid_outage_keeps_escalating(self):
        """A rebuild that left the cache empty keeps the outage and keeps retrying.

        Regression guard: treating the empty cache as "idle" would reset the
        attempt counter every pass, so the backoff could never escalate in the
        exact case it exists for — a controller that is entirely unreachable.
        """
        healer, source = _healer(poisoned=None)
        healer.note_wedged(time.monotonic())
        healer._attempts = 2
        healer._next_recreate_monotonic = time.monotonic() - 1.0

        await healer.heal_once()

        assert source.rebuild_calls == 1
        assert healer._attempts == 3
        assert healer.outage_active is True


@pytest.mark.asyncio
class TestRequestReconnect:
    """The reconnect nudge and the wait it short-circuits."""

    async def test_not_actionable_without_a_running_loop(self):
        """A wedged controller with no loop running has nothing to nudge."""
        healer, _ = _healer(poisoned=True)

        assert await healer.request_reconnect() is False
        assert not healer._requested.is_set()

    async def test_actionable_when_wedged(self):
        """A wedged controller with a running loop is nudged."""
        healer, _ = _healer(poisoned=True)
        healer.heal_once = AsyncMock()
        healer.start()
        try:
            assert await healer.request_reconnect() is True
            assert healer._requested.is_set()
        finally:
            await healer.stop()

    async def test_not_actionable_when_nothing_cached_and_no_outage(self):
        """With no factory and no outage there is nothing for the loop to do.

        Reporting True here would tell the caller an attempt is under way when
        the next pass is a guaranteed no-op.
        """
        healer, _ = _healer(poisoned=None)
        healer.heal_once = AsyncMock()
        healer.start()
        try:
            assert await healer.request_reconnect() is False
            # Nothing is queued: a claim of deferred recording would be a lie,
            # since _wait_for_next_pass consumes the event on the very next tick.
            assert not healer._requested.is_set()
        finally:
            await healer.stop()

    async def test_not_actionable_for_a_healthy_controller(self):
        """A healthy controller is left alone rather than reported as reconnecting.

        ``heal_once`` returns without rebuilding for a healthy controller, so
        claiming an attempt started would contradict the documented
        no-op-on-healthy behavior.
        """
        healer, _ = _healer(poisoned=False)
        healer.heal_once = AsyncMock()
        healer.start()
        try:
            assert await healer.request_reconnect() is False
            assert not healer._requested.is_set()
        finally:
            await healer.stop()

    async def test_actionable_mid_outage_without_cache(self):
        """An outage whose last rebuild failed is still actionable."""
        healer, _ = _healer(poisoned=None)
        healer.heal_once = AsyncMock()
        healer.note_wedged(time.monotonic())
        healer.start()
        try:
            assert await healer.request_reconnect() is True
        finally:
            await healer.stop()

    async def test_wait_for_next_pass_returns_false_on_timeout(self):
        """The full delay elapsing is reported as a non-forced pass."""
        healer, _ = _healer()
        assert await healer._wait_for_next_pass(0.01) is False

    async def test_wait_for_next_pass_short_circuits_on_request(self):
        """A pending reconnect request cuts the wait short and is consumed."""
        healer, _ = _healer()
        healer._requested.set()

        # Would block for an hour if the request were not honored.
        assert await healer._wait_for_next_pass(3600) is True
        assert not healer._requested.is_set()

    async def test_request_wakes_the_loop_immediately(self):
        """A reconnect request runs a pass without waiting out the backoff."""
        # A wedged controller plus an open outage, so the nudge is actionable
        # and the loop waits on the backoff rather than the healthy poll. The
        # hour-long backoff means only the nudge can trigger a pass in time.
        healer, _ = _healer(poisoned=True, initial=3600.0, maximum=3600.0)
        healer.note_wedged(time.monotonic())
        forced_calls = []
        healed = asyncio.Event()

        async def _record_heal_once(forced=False):
            forced_calls.append(forced)
            healed.set()

        healer.heal_once = _record_heal_once
        healer.start()
        try:
            await healer.request_reconnect()
            await asyncio.wait_for(healed.wait(), timeout=1.0)
        finally:
            await healer.stop()

        assert forced_calls == [True]


@pytest.mark.asyncio
class TestLoopLifecycle:
    """Starting, stopping, and the loop's two cadences."""

    async def test_start_and_stop_are_idempotent(self):
        """start launches one task; stop cancels it; both are idempotent."""
        healer, _ = _healer()
        healer.heal_once = AsyncMock()

        healer.start()
        task = healer._task
        assert task is not None and not task.done()
        # Second start is a no-op — same task.
        healer.start()
        assert healer._task is task

        await healer.stop()
        assert healer._task is None
        assert task.done()
        # Second stop is a no-op.
        await healer.stop()

    async def test_stop_without_start_is_a_noop(self):
        """Stopping a healer that never ran does nothing and never raises."""
        healer, _ = _healer()
        await healer.stop()
        assert healer._task is None

    async def test_stop_with_a_finished_task_is_a_noop(self):
        """A loop that already exited needs no cancellation."""
        healer, _ = _healer()

        async def _done():
            return None

        task = asyncio.create_task(_done())
        await task
        healer._task = task

        await healer.stop()

        assert healer._task is None

    async def test_loop_invokes_heal_once(self):
        """The loop runs a pass each tick until canceled."""
        healer, _ = _healer()
        called = asyncio.Event()

        async def _fake_heal_once(forced=False):
            called.set()

        healer.heal_once = _fake_heal_once

        with patch(f"{_HEALER_MODULE}._HEALTHY_POLL_SECONDS", 0.001):
            healer.start()
            await asyncio.wait_for(called.wait(), timeout=1.0)
            await healer.stop()

    async def test_loop_polls_without_scheduling_while_healthy(self):
        """With no outage open the loop polls and publishes no recreate deadline.

        The countdown reported to callers must not advertise a recreate that is
        not scheduled, and the healthy cadence must not consume the configured
        backoff.
        """
        healer, _ = _healer(initial=3600.0, maximum=3600.0)
        polled = asyncio.Event()

        async def _fake_heal_once(forced=False):
            polled.set()

        healer.heal_once = _fake_heal_once

        with patch(f"{_HEALER_MODULE}._HEALTHY_POLL_SECONDS", 0.001):
            healer.start()
            try:
                # A 3600s backoff would never tick; the healthy cadence does.
                await asyncio.wait_for(polled.wait(), timeout=1.0)
                assert healer._next_recreate_monotonic is None
            finally:
                await healer.stop()

    async def test_loop_honors_the_deadline_armed_at_detection(self):
        """The loop waits out the deadline the outage armed, not a fresh one.

        Restarting the clock from the loop's own tick would push the first
        recreate a full backoff past the tick that noticed the wedge, on top of
        the wait already served since detection.
        """
        healer, _ = _healer(initial=3600.0, maximum=3600.0)
        healer.note_wedged(time.monotonic())
        armed = healer._next_recreate_monotonic
        healer.heal_once = AsyncMock()

        healer.start()
        try:
            # Yield so the loop body runs up to its wait.
            await asyncio.sleep(0)
            assert healer._next_recreate_monotonic == armed
        finally:
            await healer.stop()

    async def test_loop_arms_a_deadline_for_an_unscheduled_outage(self):
        """An outage with no deadline (mid-rebuild) gets one on the next tick."""
        healer, _ = _healer(initial=3600.0, maximum=3600.0)
        healer.note_wedged(time.monotonic())
        healer._next_recreate_monotonic = None
        healer.heal_once = AsyncMock()

        healer.start()
        try:
            await asyncio.sleep(0)
            assert healer._next_recreate_monotonic is not None
        finally:
            await healer.stop()

    async def test_stop_clears_outage_state(self):
        """Stopping clears the outage so a restarted healer starts clean.

        Regression guard: nothing is left to act on a stale outage, so a source
        reused after ``stop`` would otherwise keep failing fast against a cache
        the stopped loop never refilled.
        """
        healer, _ = _healer(poisoned=True)
        healer.heal_once = AsyncMock()
        healer.start()
        healer.note_wedged(time.monotonic())
        healer._attempts = 3

        await healer.stop()

        assert healer.outage_active is False
        assert healer._attempts == 0
        assert healer._next_recreate_monotonic is None

    async def test_stop_without_start_clears_outage_state(self):
        """The no-task path clears the outage too."""
        healer, _ = _healer()
        healer.note_wedged(time.monotonic())

        await healer.stop()

        assert healer.outage_active is False

    async def test_loop_continues_after_a_failed_pass(self):
        """An exception in one pass is logged and the loop keeps running."""
        healer, _ = _healer()
        calls = []
        second_call = asyncio.Event()

        async def _flaky_heal_once(forced=False):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("transient")
            second_call.set()

        healer.heal_once = _flaky_heal_once

        with patch(f"{_HEALER_MODULE}._HEALTHY_POLL_SECONDS", 0.001):
            healer.start()
            await asyncio.wait_for(second_call.wait(), timeout=1.0)
            await healer.stop()
        assert len(calls) >= 2

    async def test_loop_propagates_cancel_during_a_pass(self):
        """Canceling while heal_once runs propagates through the inner guard."""
        healer, _ = _healer()
        in_heal = asyncio.Event()

        async def _blocking_heal_once(forced=False):
            in_heal.set()
            await asyncio.sleep(3600)

        healer.heal_once = _blocking_heal_once

        with patch(f"{_HEALER_MODULE}._HEALTHY_POLL_SECONDS", 0.001):
            healer.start()
            await asyncio.wait_for(in_heal.wait(), timeout=1.0)
            # Cancels the task while it is suspended inside heal_once.
            await healer.stop()
        assert healer._task is None
