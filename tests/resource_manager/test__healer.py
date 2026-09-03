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

    async def peek_controller_poisoned(self) -> bool | None:
        return self.poisoned

    async def rebuild_factory(self) -> None:
        self.rebuild_calls += 1


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


def _wedge(healer, *, attempts: int = 0, due: bool = False):
    """Open an outage and put its recreate deadline in the past or the future."""
    now = time.monotonic()
    healer.note_wedged(now)
    healer._outage.attempts = attempts
    healer._outage.next_recreate = now - 1.0 if due else now + 3600.0
    return healer._outage


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

        A foreground caller can open the outage between the loop's polls;
        arming here keeps the first recreate one full initial backoff away from
        detection rather than from the loop's next tick.
        """
        healer, _ = _healer(initial=30.0)
        now = time.monotonic()

        healer.note_wedged(now)

        assert healer._outage.next_recreate == now + 30.0

    def test_note_wedged_preserves_the_outage_in_progress(self):
        """A second wedged report keeps the original start time and deadline."""
        healer, _ = _healer()

        healer.note_wedged(time.monotonic())
        first = healer._outage
        healer.note_wedged(time.monotonic())

        assert healer._outage is first

    def test_note_wedged_reports_growing_wait(self):
        """The reported wait grows with the time since the outage opened."""
        healer, _ = _healer()
        now = time.monotonic()
        healer.note_wedged(now)

        assert "waited 42s" in healer.note_wedged(now + 42.0)

    def test_note_healthy_clears_outage(self):
        """A healthy report drops the outage wholesale."""
        healer, _ = _healer()
        _wedge(healer, attempts=4)

        healer.note_healthy()

        assert healer.outage_active is False
        assert healer.healing_status_message(time.monotonic()) is None

    def test_healing_status_message_reports_scheduled_countdown(self):
        """The armed deadline is reported as the countdown to the next recreate."""
        healer, _ = _healer()
        now = time.monotonic()
        healer.note_wedged(now)
        healer._outage.attempts = 2
        healer._outage.next_recreate = now + 25.0

        msg = healer.healing_status_message(now)

        assert "next automatic recreate is in ~25s" in msg
        assert "2 recreate attempt" in msg

    def test_healing_status_message_never_reports_a_past_deadline(self):
        """An elapsed deadline is reported as zero, not as a negative countdown."""
        healer, _ = _healer()
        now = time.monotonic()
        healer.note_wedged(now)
        healer._outage.next_recreate = now - 5.0

        assert "in ~0s" in healer.healing_status_message(now)

    def test_backoff_doubles_and_caps(self):
        """The delay doubles per attempt and is capped at the configured maximum."""
        healer, _ = _healer(initial=10.0, maximum=60.0)

        assert healer._backoff_seconds(0) == 10.0
        assert healer._backoff_seconds(1) == 20.0
        assert healer._backoff_seconds(2) == 40.0
        # Doubling would give 80.0; the cap wins.
        assert healer._backoff_seconds(3) == 60.0
        # A very long outage stays at the cap without overflowing.
        assert healer._backoff_seconds(10_000) == 60.0

    def test_backoff_max_below_initial_clamps(self):
        """A maximum below the initial delay pins every delay to the maximum."""
        healer, _ = _healer(initial=30.0, maximum=5.0)
        assert healer._backoff_seconds(0) == 5.0


@pytest.mark.asyncio
class TestHealOnce:
    """One healing pass, across every combination of poison state and outage."""

    async def test_healthy_ends_the_outage(self):
        """A healthy peek clears outage state and does not rebuild."""
        healer, source = _healer(poisoned=False)
        _wedge(healer, attempts=3, due=True)

        await healer.heal_once()

        assert healer.outage_active is False
        assert source.rebuild_calls == 0

    async def test_idle_with_no_cache_does_nothing(self):
        """Nothing cached and no outage is idle, not a wedge."""
        healer, source = _healer(poisoned=None)

        await healer.heal_once()

        assert source.rebuild_calls == 0
        assert healer.outage_active is False

    async def test_detection_opens_outage_without_rebuilding(self):
        """The pass that first sees a wedge only opens the outage.

        The rebuild waits for the deadline that opening armed, so
        ``controller_resubscribe_backoff_initial_seconds`` is honored no matter
        when in the poll cycle the wedge was noticed.
        """
        healer, source = _healer(poisoned=True)

        await healer.heal_once()

        assert healer.outage_active is True
        assert healer._outage.attempts == 0
        assert healer._outage.next_recreate > time.monotonic()
        assert source.rebuild_calls == 0

    async def test_forced_rebuilds_on_detection(self):
        """A forced pass rebuilds immediately instead of waiting out the backoff."""
        healer, source = _healer(poisoned=True)

        await healer.heal_once(forced=True)

        assert source.rebuild_calls == 1
        assert healer._outage.attempts == 1

    async def test_pending_backoff_defers_the_rebuild(self):
        """A non-forced pass waits out the deadline armed when the outage opened."""
        healer, source = _healer(poisoned=True)
        _wedge(healer)

        await healer.heal_once()

        assert source.rebuild_calls == 0
        assert healer._outage.attempts == 0

    async def test_forced_pass_ignores_a_pending_backoff(self):
        """A reconnect request rebuilds without waiting out the armed deadline."""
        healer, source = _healer(poisoned=True)
        _wedge(healer)

        await healer.heal_once(forced=True)

        assert source.rebuild_calls == 1
        assert healer._outage.attempts == 1

    async def test_elapsed_backoff_rebuilds_and_escalates(self):
        """A due deadline rebuilds, counts the attempt, and re-arms escalated."""
        healer, source = _healer(poisoned=True, initial=10.0, maximum=1000.0)
        _wedge(healer, attempts=2, due=True)

        await healer.heal_once()

        assert source.rebuild_calls == 1
        assert healer._outage.attempts == 3
        # Re-armed from the end of the attempt at the escalated delay (10 * 2**3).
        assert healer._outage.next_recreate == pytest.approx(
            time.monotonic() + 80.0, abs=1.0
        )

    async def test_empty_cache_mid_outage_keeps_escalating(self):
        """A rebuild that left the cache empty keeps the outage and keeps retrying.

        Regression guard: treating the empty cache as "idle" would reset the
        attempt counter every pass, so the backoff could never escalate in the
        exact case it exists for — a controller that is entirely unreachable.
        """
        healer, source = _healer(poisoned=None)
        _wedge(healer, attempts=2, due=True)

        await healer.heal_once()

        assert source.rebuild_calls == 1
        assert healer._outage.attempts == 3
        assert healer.outage_active is True


@pytest.mark.asyncio
class TestRequestReconnect:
    """The reconnect signal and the wait it short-circuits."""

    async def test_not_actionable_without_a_running_loop(self):
        """A wedged controller with no loop running has nothing to signal."""
        healer, _ = _healer(poisoned=True)

        assert await healer.request_reconnect() is False
        assert not healer._requested.is_set()

    async def test_actionable_when_wedged(self):
        """A wedged controller with a running loop is signaled."""
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
        _wedge(healer)
        healer.start()
        try:
            assert await healer.request_reconnect() is True
        finally:
            await healer.stop()

    async def test_wait_for_next_pass_returns_false_on_timeout(self):
        """The full interval elapsing is reported as a non-forced pass."""
        healer, _ = _healer()
        with patch(f"{_HEALER_MODULE}._POLL_SECONDS", 0.01):
            assert await healer._wait_for_next_pass() is False

    async def test_wait_for_next_pass_short_circuits_on_request(self):
        """A pending reconnect request cuts the wait short."""
        healer, _ = _healer()
        healer._requested.set()

        # Would block for the full poll interval if the request were ignored.
        assert await healer._wait_for_next_pass() is True

    async def test_request_wakes_the_loop_immediately(self):
        """A reconnect request runs a pass without waiting out the poll interval."""
        healer, _ = _healer(poisoned=True)
        _wedge(healer)
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

    async def test_repeated_requests_coalesce_into_one_pass(self):
        """Many requests raised before a pass starts produce exactly one pass.

        The signal is a single flag, so a burst of reconnect calls cannot queue
        a backlog of recreate attempts behind each other.
        """
        healer, _ = _healer(poisoned=True)
        _wedge(healer)
        forced_calls = []
        healed = asyncio.Event()

        async def _record_heal_once(forced=False):
            forced_calls.append(forced)
            healed.set()

        healer.heal_once = _record_heal_once
        healer.start()
        try:
            for _ in range(5):
                assert await healer.request_reconnect() is True
            await asyncio.wait_for(healed.wait(), timeout=1.0)
            # Long enough for a queued second pass to have run, if one existed.
            await asyncio.sleep(0.05)

            assert forced_calls == [True]
            assert not healer._requested.is_set()
        finally:
            await healer.stop()


@pytest.mark.asyncio
class TestLoopLifecycle:
    """Starting, stopping, and the loop's poll cadence."""

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

    async def test_stop_clears_outage_and_pending_request(self):
        """Stopping leaves no state for a restarted healer to act on.

        Regression guard: nothing is left to heal a stale outage, so a source
        reused after ``stop`` would otherwise keep failing fast against a cache
        the stopped loop never refilled.
        """
        healer, _ = _healer(poisoned=True)
        _wedge(healer, attempts=3)
        healer._requested.set()

        await healer.stop()

        assert healer.outage_active is False
        assert not healer._requested.is_set()

    async def test_loop_invokes_heal_once(self):
        """The loop runs a pass each tick until canceled."""
        healer, _ = _healer()
        called = asyncio.Event()

        async def _fake_heal_once(forced=False):
            called.set()

        healer.heal_once = _fake_heal_once

        with patch(f"{_HEALER_MODULE}._POLL_SECONDS", 0.001):
            healer.start()
            await asyncio.wait_for(called.wait(), timeout=1.0)
            await healer.stop()

    async def test_loop_polls_regardless_of_the_recreate_deadline(self):
        """Every tick re-reads the state; a far-off deadline does not park the loop.

        Regression guard: the loop used to sleep until the outage's own
        deadline, so state changed by a foreground caller during that sleep —
        an outage ending and reopening, for instance — went unseen for up to
        ``controller_resubscribe_backoff_max_seconds``.
        """
        healer, _ = _healer(initial=3600.0, maximum=3600.0)
        _wedge(healer)
        polled = asyncio.Event()

        async def _fake_heal_once(forced=False):
            polled.set()

        healer.heal_once = _fake_heal_once

        with patch(f"{_HEALER_MODULE}._POLL_SECONDS", 0.001):
            healer.start()
            try:
                await asyncio.wait_for(polled.wait(), timeout=1.0)
            finally:
                await healer.stop()

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

        with patch(f"{_HEALER_MODULE}._POLL_SECONDS", 0.001):
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

        with patch(f"{_HEALER_MODULE}._POLL_SECONDS", 0.001):
            healer.start()
            await asyncio.wait_for(in_heal.wait(), timeout=1.0)
            # Cancels the task while it is suspended inside heal_once.
            await healer.stop()
        assert healer._task is None
