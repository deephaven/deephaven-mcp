"""
HTTP Stress Test Script for Production Deployments

This script is designed to stress-test HTTP endpoints of a Deephaven MCP deployment.
It creates multiple concurrent connections and sends requests to the specified HTTP endpoint,
reporting errors and response times. The script is useful for validating the stability and
performance of production or staging deployments under load.

Usage Example:
    uv run ./scripts/mcp_docs_stress_http.py \
        --concurrency 10 \
        --requests-per-conn 100 \
        --url "https://your-production-url/mcp" \
        --max-errors 5 \
        --rps 100 \
        --max-response-time 2

Arguments:
    --concurrency         Number of concurrent connections (default: 100)
    --requests-per-conn   Number of requests per connection (default: 100)
    --url                 Target HTTP endpoint URL (streamable-http or SSE)
    --max-errors          Maximum number of errors before stopping the test (default: 5)
    --rps                 Requests per second limit per connection (default: 0, no limit)
    --max-response-time   Maximum allowed response time in seconds (default: 1)

Output:
    - Logs warnings and errors for slow responses, bad status codes, or exceptions.
    - Prints only the reason string for any error encountered.
    - Prints "PASSED" if the test completes without exceeding the error threshold,
      or "FAILED" with the reason if the error threshold is reached or another fatal error occurs.

Requirements:
    - Python 3.8+
    - aiohttp
    - aiolimiter
    - uv (for running in a virtual environment, optional)

This script is intended for use by engineers or SREs validating MCP deployments.
"""

import argparse
import asyncio
import logging
import time

import aiohttp
from aiolimiter import AsyncLimiter

parser = argparse.ArgumentParser(
    description="Stress test the /sse endpoint with concurrent connections."
)
parser.add_argument(
    "--concurrency",
    type=int,
    default=100,
    help="Number of concurrent connections (default: 100)",
)
parser.add_argument(
    "--requests-per-conn",
    type=int,
    default=100,
    help="Number of requests per connection (default: 100)",
)
parser.add_argument(
    "--url",
    type=str,
    default="http://localhost:8001/mcp",
    help="HTTP endpoint URL (default: http://localhost:8001/mcp)",
)
parser.add_argument(
    "--max-errors",
    type=int,
    default=5,
    help="Maximum number of errors before stopping (default: 5)",
)
parser.add_argument(
    "--rps",
    type=float,
    default=10000,
    help="Requests per second limit per connection (default: 0, no limit)",
)
parser.add_argument(
    "--max-response-time",
    type=float,
    default=1,
    help="Maximum allowed response time in seconds (default: 0, no check)",
)
args = parser.parse_args()

CONCURRENCY = args.concurrency
REQUESTS_PER_CONN = args.requests_per_conn
HTTP_URL = args.url
MAX_ERRORS = args.max_errors
RPS = args.rps
MAX_RESPONSE_TIME = args.max_response_time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_LOGGER = logging.getLogger(__name__)

# Global rate limiter (shared across all tasks)
limiter = AsyncLimiter(max_rate=RPS, time_period=1) if RPS > 0 else None


class StressTestState:
    """
    Thread-safe state management for stress test execution.

    Tracks error counts, success counts, and attempt counts across multiple
    concurrent connections. Provides early termination when error threshold
    is exceeded.

    Args:
        max_errors (int): Maximum number of errors before stopping the test.
    """

    def __init__(self, max_errors):
        """Initialize stress test state with error threshold."""
        self.error_count = 0
        self.success_count = 0
        self.attempt_count = 0
        self.error_lock = asyncio.Lock()
        self.success_lock = asyncio.Lock()
        self.attempt_lock = asyncio.Lock()
        self.stop_event = asyncio.Event()
        self.max_errors = max_errors

    @property
    def should_stop(self):
        """Check if the test should stop due to error threshold being reached."""
        return self.stop_event.is_set()

    async def increment_error(self):
        """
        Thread-safely increment error count and check if threshold is exceeded.

        Returns:
            bool: True if error threshold reached and test should stop, False otherwise.
        """
        async with self.error_lock:
            self.error_count += 1
            if self.error_count >= self.max_errors:
                self.stop_event.set()
                return True
        return False

    async def increment_success(self):
        """Thread-safely increment success count."""
        async with self.success_lock:
            self.success_count += 1

    async def increment_attempt(self):
        """Thread-safely increment attempt count."""
        async with self.attempt_lock:
            self.attempt_count += 1


async def http_client(session, idx, state: "StressTestState"):
    """
    Execute HTTP requests for a single concurrent connection.

    Sends multiple requests to the HTTP endpoint, handling SSE streaming responses
    and tracking response times. Stops early if the global error threshold is reached.

    Args:
        session (aiohttp.ClientSession): HTTP session for making requests.
        idx (int): Connection index for logging identification.
        state (StressTestState): Shared state for tracking test progress.
    """
    for i in range(REQUESTS_PER_CONN):
        if state.should_stop:
            break
        await state.increment_attempt()
        try:
            start_time = time.monotonic()
            async with session.get(HTTP_URL, timeout=10) as resp:
                if resp.status != 200:
                    _LOGGER.warning(
                        f"[Conn {idx}] Unexpected HTTP status: {resp.status}"
                    )
                    continue
                async for line in resp.content:
                    # SSE streams send data line by line
                    if line:
                        response_time = time.monotonic() - start_time
                        if MAX_RESPONSE_TIME > 0 and response_time > MAX_RESPONSE_TIME:
                            _LOGGER.warning(
                                f"[Conn {idx}] Response time {response_time:.3f}s exceeded max {MAX_RESPONSE_TIME}s"
                            )
                        # Uncomment below to print all lines
                        # print(f"[Conn {idx}] {line.decode().strip()}")
                        await state.increment_success()
                        break  # Only read the first event for stress
        except Exception as e:
            _LOGGER.error(
                f"[Conn {idx}:{i}] HTTP request failed: {e.args[0] if e.args else str(e)}",
                exc_info=True,
            )
            if await state.increment_error():
                break
        if limiter:
            async with limiter:
                pass  # Limit the rate globally across all tasks


async def main():
    """
    Execute the main stress test by creating concurrent HTTP clients.

    Creates multiple concurrent connections and waits for all to complete.
    Raises RuntimeError if the error threshold is exceeded.

    Raises:
        RuntimeError: If error count exceeds the configured maximum.
    """
    state = StressTestState(MAX_ERRORS)
    async with aiohttp.ClientSession() as session:
        tasks = [http_client(session, i, state) for i in range(CONCURRENCY)]
        await asyncio.gather(*tasks)
    if state.error_count >= state.max_errors:
        raise RuntimeError(
            f"Error threshold reached: {state.error_count} errors (max allowed: {state.max_errors}), {state.success_count} successes out of {state.attempt_count} attempts"
        )


if __name__ == "__main__":
    try:
        _LOGGER.info(
            f"Starting HTTP stress test: {CONCURRENCY} concurrent connections, {REQUESTS_PER_CONN} requests each to {HTTP_URL}"
        )
        _LOGGER.info(
            f"Configuration: max_errors={MAX_ERRORS}, rps={RPS}, max_response_time={MAX_RESPONSE_TIME}s"
        )
        asyncio.run(main())
        print("Complete... PASSED")
    except Exception as e:
        _LOGGER.error(f"Stress test failed: {e}")
        print(f"Complete... FAILED: {e}")
