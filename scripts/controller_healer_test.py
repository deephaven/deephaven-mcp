"""Exercise the enterprise controller subscription healer against a real DHE system.

Standalone diagnostic harness. It does not modify any MCP server behavior; it
only calls the same public APIs an MCP tool calls.

The run shows four things:
    1. A working subscription  -- authenticates by --system and lists PQs.
    2. The poison              -- genuinely wedges the controller subscription.
    3. The retry messages      -- the healer recreating on escalating backoff,
                                  each retry followed by a PQ call that still
                                  fails fast (attempt count and countdown climb).
    4. The reconnect           -- the controller recovering, proven by a PQ list.

To make step 3 show *several* attempts, the harness re-wedges each freshly
built factory --retries times, then steps aside so the healer wins.

Poison methods (--poison):
    resubscribe  (default, REAL) Closes the controller's gRPC channel and then
                 calls the vendor's own ControllerClient.subscribe() -- the
                 same call the vendor response thread makes when a subscription
                 stream drops. It sets sub_state=SUBSCRIBING and then fails
                 inside _do_subscription against the closed channel, leaving a
                 genuinely wedged client. Nothing is faked, and it fails fast
                 so there is no 120s vendor timeout to wait out.
    flag         Assigns sub_state=SUBSCRIBING directly (simulated). Use only
                 if resubscribe misbehaves on your build.
    none         Poisons nothing; polls until the controller wedges on its own.
                 Use when reproducing a real outage by stopping the DHE
                 controller yourself.

Usage:
    uv run scripts/controller_healer_test.py --system ent
    uv run scripts/controller_healer_test.py --system ent --retries 5
    uv run scripts/controller_healer_test.py --system ent -v
    uv run scripts/controller_healer_test.py --system ent --poison none

Arguments:
    --system SYSTEM        REQUIRED. Enterprise system name to connect to.
    --config-dir PATH      Configuration directory (default: normal resolution,
                           honoring DH_AI_DATA_DIR).
    --retries N            Healer recreate attempts to force before letting it
                           succeed (default: 3).
    --poison MODE          resubscribe (default) | flag | none
    --backoff-initial SEC  Healer initial backoff (default: 2.0; shipped: 30.0).
    --backoff-max SEC      Healer backoff ceiling (default: 8.0; shipped: 300.0).
    --no-force-reconnect   Wait out the first backoff instead of signaling the
                           healer the way enterprise_controller_reconnect does.
    --recovery-timeout SEC Seconds to wait for recovery (default: 120).
    --poison-timeout SEC   With --poison none, seconds to wait for an external
                           outage to wedge the controller (default: 300).
    -v, --verbose          Full DEBUG logging from every component.

Requirements:
    - A reachable, configured DHE enterprise system (VPN if applicable).
    - Credentials in the configuration tree that can authenticate to it.
"""

import argparse
import asyncio
import logging
import sys
import threading
import time
from pathlib import Path

import grpc
from deephaven_enterprise.client.controller import SubState
from pydantic import ValidationError

from deephaven_mcp._exceptions import DeephavenConnectionError
from deephaven_mcp.config.tree import ConfigTreeLoader
from deephaven_mcp.resource_manager import CorePlusSessionFactoryManager

_STEP = 0


def _step(message: str) -> None:
    """Print a numbered progress banner."""
    global _STEP
    _STEP += 1
    print(f"\n=== [{_STEP}] {message} ===", flush=True)


def _ok(message: str) -> None:
    print(f"    PASS  {message}", flush=True)


def _info(message: str) -> None:
    print(f"    ....  {message}", flush=True)


def _fail(message: str) -> None:
    print(f"    FAIL  {message}", flush=True)


def _cached_factory(manager: CorePlusSessionFactoryManager) -> object | None:
    """Return the manager's cached factory without triggering creation."""
    return manager._item_cache  # noqa: SLF001 - diagnostic harness


async def _list_pqs(manager: CorePlusSessionFactoryManager) -> int:
    """Do a real controller-backed PQ listing, the way a PQ tool call does.

    Returns:
        int: Number of persistent queries the controller reported.
    """
    controller = await manager.get_controller_client()
    query_map = await controller.map()
    return len(query_map)


def _wedge_factory(factory: object) -> bool:
    """Wedge one factory's controller through the vendor re-subscribe path.

    Closes the gRPC channel and then calls the vendor
    ``ControllerClient.subscribe()`` -- the same call the vendor response
    thread makes when a subscription stream drops. ``subscribe()`` sets
    ``sub_state = SUBSCRIBING`` and then fails inside ``_do_subscription``
    because the channel is gone, leaving the client genuinely wedged.

    Returns:
        bool: True if the controller ended up in the SUBSCRIBING state.
    """
    vendor = factory.controller_client.wrapped  # type: ignore[attr-defined]
    vendor.channel.close()
    try:
        vendor.subscribe()
    except Exception as exc:  # noqa: BLE001 - the failure is the point
        logging.getLogger(__name__).debug("vendor subscribe failed: %r", exc)
    return bool(vendor.sub_state is SubState.SUBSCRIBING)


def _flag_factory(factory: object) -> bool:
    """Wedge one factory's controller by assigning ``sub_state`` (simulated)."""
    factory.controller_client.wrapped.sub_state = SubState.SUBSCRIBING  # type: ignore[attr-defined]
    return True


async def _probe_failure(manager: CorePlusSessionFactoryManager) -> bool:
    """Show that a PQ call still fails fast while the outage persists.

    Called after each forced healer retry so the climbing attempt count and
    backoff countdown in the status message are visible.

    Returns:
        bool: True if the call failed as expected.
    """
    started = time.monotonic()
    try:
        await _list_pqs(manager)
    except DeephavenConnectionError as exc:
        elapsed = (time.monotonic() - started) * 1000
        # The "how to force a reconnect" tail is already shown once in step 2.
        head = str(exc).split(". Retry this call shortly", 1)[0]
        _info(f"pq_list failed in {elapsed:.0f}ms -- {head}")
        return True
    _fail("pq_list unexpectedly succeeded while the controller was wedged")
    return False


async def _repoison_watcher(
    manager: CorePlusSessionFactoryManager,
    wedge: "object",
    remaining: int,
    stop: asyncio.Event,
) -> int:
    """Re-wedge each newly built factory so the healer has to retry.

    The healer recreates the factory on every pass; a fresh factory subscribes
    cleanly and would recover on the first attempt. To surface several retry
    messages (and the escalating backoff), this watcher re-wedges each new
    factory until ``remaining`` is exhausted, then leaves the next one healthy.

    Args:
        manager: The manager being healed.
        wedge: Callable taking a factory and returning whether it wedged.
        remaining: How many further factories to wedge.
        stop: Set to end the watcher early.

    Returns:
        int: How many additional factories were wedged.
    """
    seen: set[int] = set()
    wedged = 0
    while remaining > 0 and not stop.is_set():
        await asyncio.sleep(0.2)
        factory = _cached_factory(manager)
        if factory is None or id(factory) in seen:
            continue
        if factory.controller_client.is_poisoned:  # type: ignore[attr-defined]
            continue
        seen.add(id(factory))
        if wedge(factory):  # type: ignore[operator]
            wedged += 1
            remaining -= 1
            await _probe_failure(manager)
    return wedged


async def _await_state(
    manager: CorePlusSessionFactoryManager, poisoned: bool, timeout: float
) -> bool:
    """Poll the cached controller until it reaches the requested poison state.

    Observes passively -- it never calls ``get()``, so the harness cannot
    accidentally do the healer's work for it.

    Args:
        manager: The manager holding the cached factory.
        poisoned: Target state to wait for.
        timeout: Seconds to wait before giving up.

    Returns:
        bool: True if the state was reached within the timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        factory = _cached_factory(manager)
        if factory is not None:
            if factory.controller_client.is_poisoned is poisoned:  # type: ignore[attr-defined]
                return True
        await asyncio.sleep(0.25)
    return False


async def _build_manager(
    args: argparse.Namespace,
) -> CorePlusSessionFactoryManager:
    """Load configuration and build a factory manager for ``--system``.

    Raises:
        SystemExit: If enterprise is unconfigured or the system is unknown.
    """
    config_dir = Path(args.config_dir) if args.config_dir else None
    loader = ConfigTreeLoader(config_dir)
    config = await loader.initialize()

    enterprise = config.enterprise
    if enterprise is None or not enterprise.systems:
        sys.exit("No enterprise systems are configured in this config tree.")
    if args.system not in enterprise.systems:
        available = ", ".join(sorted(enterprise.systems)) or "(none)"
        sys.exit(f"Unknown system {args.system!r}. Configured systems: {available}")

    system_config = enterprise.systems[args.system]
    base_timeouts = enterprise.settings.timeouts.client
    # argparse values are untrusted, so re-validate rather than model_copy --
    # otherwise --backoff-initial 0 would bypass the schema's Field(gt=0) and
    # make the healer spin.
    try:
        timeouts = type(base_timeouts).model_validate(
            base_timeouts.model_dump()
            | {
                "controller_resubscribe_backoff_initial_seconds": args.backoff_initial,
                "controller_resubscribe_backoff_max_seconds": args.backoff_max,
            }
        )
    except ValidationError as exc:
        sys.exit(f"Invalid backoff values: {exc}")
    return CorePlusSessionFactoryManager(
        system_config.name,
        system_config,
        system_config.auth.credentials,
        timeouts=timeouts,
    )


async def _first_poison(
    args: argparse.Namespace, manager: CorePlusSessionFactoryManager, wedge: object
) -> bool:
    """Wedge the controller for the first time. Returns success."""
    if args.poison == "none":
        _info("Stop the DHE controller now; the vendor needs up to 120s to wedge.")
        if not await _await_state(manager, poisoned=True, timeout=args.poison_timeout):
            _fail("controller never became wedged; nothing to heal")
            return False
        _ok("controller reported itself wedged")
        return True

    factory = _cached_factory(manager)
    if factory is None or not wedge(factory):  # type: ignore[operator]
        _fail("could not wedge the controller; try --poison flag")
        return False
    _ok(f"controller wedged ({args.poison})")
    return True


async def _show_instant_failure(manager: CorePlusSessionFactoryManager) -> bool:
    """Assert a controller-backed call now fails fast with the right message."""
    started = time.monotonic()
    try:
        await _list_pqs(manager)
    except DeephavenConnectionError as exc:
        elapsed = (time.monotonic() - started) * 1000
        message = str(exc)
        _ok(f"pq_list-equivalent failed in {elapsed:.0f}ms (no blocking)")
        print(f"    -> {message}", flush=True)
        return (
            "CONTROLLER_SUBSCRIBING" in message
            and "enterprise_controller_reconnect" in message
        )
    _fail("call unexpectedly succeeded; the controller was not wedged")
    return False


async def _run(args: argparse.Namespace, manager: CorePlusSessionFactoryManager) -> int:
    """Drive the healer scenario. Returns a process exit code."""
    wedge = _flag_factory if args.poison == "flag" else _wedge_factory
    stop_watcher = asyncio.Event()
    watcher: asyncio.Task[int] | None = None
    await manager.start_healer()

    try:
        _step(f"Working subscription on {args.system!r}")
        started = time.monotonic()
        try:
            count = await _list_pqs(manager)
        except DeephavenConnectionError as exc:
            _fail(f"could not reach the system: {exc}")
            _info("Check VPN / that the system is up, then re-run.")
            return 1
        _ok(f"listed {count} persistent queries in {time.monotonic() - started:.2f}s")

        _step("Poison the controller subscription")
        if not await _first_poison(args, manager, wedge):
            return 1
        if not await _show_instant_failure(manager):
            _fail("error message did not carry the expected code / tool name")
            return 1

        _step(f"Healer retries (forcing {args.retries} attempt(s), then recovery)")
        if args.poison != "none" and args.retries > 1:
            watcher = asyncio.create_task(
                _repoison_watcher(manager, wedge, args.retries - 1, stop_watcher)
            )
        if args.force_reconnect:
            requested = await manager.request_reconnect()
            _info(f"enterprise_controller_reconnect signaled (healer={requested})")
        else:
            _info(f"waiting out the first backoff (~{args.backoff_initial:.0f}s)")

        started = time.monotonic()
        if not await _await_state(
            manager, poisoned=False, timeout=args.recovery_timeout
        ):
            _fail(f"still wedged after {args.recovery_timeout:.0f}s")
            return 1
        stop_watcher.set()
        _ok(f"controller reconnected after {time.monotonic() - started:.1f}s")

        _step("Confirm the reconnect")
        count = await _list_pqs(manager)
        _ok(f"listed {count} persistent queries")
        return 0
    finally:
        stop_watcher.set()
        if watcher is not None:
            await watcher
        await manager.stop_healer()
        await manager.close()


async def _main(args: argparse.Namespace) -> int:
    """Build the manager and run the scenario inside one event loop."""
    manager = await _build_manager(args)
    return await _run(args, manager)


class _QuietHarnessNoise(logging.Filter):
    """Drop log lines the harness causes or already prints itself."""

    _DROP = (
        # Caused by the harness closing the channel to wedge the controller.
        "Error closing item for",
        # The harness prints its own timed, compact version of this.
        ":get_controller_client]",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return not any(fragment in message for fragment in self._DROP)


def _silence_vendor_thread_tracebacks() -> None:
    """Stop dead vendor response threads from printing gRPC tracebacks.

    Wedging the controller cancels its subscription stream, so the vendor
    response thread dies with a CANCELED ``RpcError``. Python prints that
    through ``threading.excepthook`` rather than logging, so it has to be
    suppressed here rather than by setting a log level.
    """
    original = threading.excepthook

    def hook(args: threading.ExceptHookArgs) -> None:
        if args.exc_type is not None and issubclass(args.exc_type, grpc.RpcError):
            return
        original(args)

    threading.excepthook = hook


def _configure_logging(verbose: bool) -> None:
    """Show the healer's own messages and little else."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.WARNING,
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    if verbose:
        return
    _silence_vendor_thread_tracebacks()
    for handler in logging.getLogger().handlers:
        handler.addFilter(_QuietHarnessNoise())
    # The healer logs its retries at WARNING; everything below is noise here.
    for noisy in (
        "deephaven_mcp.config",
        "deephaven_mcp.client",
        "deephaven_enterprise",
        "deephaven_enterprise.controller",
        "httpx",
        "httpcore",
        "urllib3",
    ):
        logging.getLogger(noisy).setLevel(logging.CRITICAL)


def main() -> None:
    """Parse arguments and run the harness."""
    parser = argparse.ArgumentParser(
        description="Exercise the enterprise controller subscription healer.",
    )
    parser.add_argument("--system", required=True, help="Enterprise system name.")
    parser.add_argument("--config-dir", default=None, help="Configuration directory.")
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Healer recreate attempts to force before letting it succeed.",
    )
    parser.add_argument(
        "--poison",
        choices=("resubscribe", "flag", "none"),
        default="resubscribe",
        help="How to wedge the controller subscription.",
    )
    parser.add_argument("--backoff-initial", type=float, default=2.0)
    parser.add_argument("--backoff-max", type=float, default=8.0)
    parser.add_argument(
        "--no-force-reconnect",
        dest="force_reconnect",
        action="store_false",
        help="Wait out the backoff instead of signaling the healer.",
    )
    parser.add_argument("--recovery-timeout", type=float, default=120.0)
    parser.add_argument("--poison-timeout", type=float, default=300.0)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    _configure_logging(args.verbose)
    sys.exit(asyncio.run(_main(args)))


if __name__ == "__main__":
    main()
