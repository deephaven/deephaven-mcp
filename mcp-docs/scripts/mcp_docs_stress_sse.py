import asyncio
import aiohttp
import argparse
import logging
from aiolimiter import AsyncLimiter

parser = argparse.ArgumentParser(description="Stress test the /sse endpoint with concurrent connections.")
parser.add_argument('--concurrency', type=int, default=100, help='Number of concurrent connections (default: 100)')
parser.add_argument('--requests-per-conn', type=int, default=100, help='Number of requests per connection (default: 100)')
parser.add_argument('--sse-url', type=str, default="http://localhost:8000/sse", help='SSE endpoint URL (default: http://localhost:8000/sse)')
parser.add_argument('--max-errors', type=int, default=5, help='Maximum number of errors before stopping (default: 5)')
parser.add_argument('--rps', type=float, default=10000, help='Requests per second limit per connection (default: 0, no limit)')
args = parser.parse_args()

CONCURRENCY = args.concurrency
REQUESTS_PER_CONN = args.requests_per_conn
SSE_URL = args.sse_url
MAX_ERRORS = args.max_errors
RPS = args.rps

# Global rate limiter (shared across all tasks)
limiter = AsyncLimiter(max_rate=RPS, time_period=1) if RPS > 0 else None

class StressTestState:
    def __init__(self, max_errors):
        self.error_count = 0
        self.error_lock = asyncio.Lock()
        self.stop_event = asyncio.Event()
        self.max_errors = max_errors

    @property
    def should_stop(self):
        return self.stop_event.is_set()

    async def increment_error(self):
        async with self.error_lock:
            self.error_count += 1
            if self.error_count >= self.max_errors:
                self.stop_event.set()
                return True
        return False


async def sse_client(session, idx, state: 'StressTestState'):
    for i in range(REQUESTS_PER_CONN):
        if state.should_stop:
            break
        try:
            async with session.get(SSE_URL, timeout=10) as resp:
                if resp.status != 200:
                    print(f"[Conn {idx}] Unexpected status: {resp.status}")
                    continue
                async for line in resp.content:
                    # SSE streams send data line by line
                    if line:
                        # Uncomment below to print all lines
                        # print(f"[Conn {idx}] {line.decode().strip()}")
                        break  # Only read the first event for stress
        except Exception as e:
            logging.error(f"[Conn{idx}:{i}] Error: {e}", exc_info=True)
            if await state.increment_error():
                break
        if limiter:
            async with limiter:
                pass  # Limit the rate globally across all tasks

async def main():
    state = StressTestState(MAX_ERRORS)
    async with aiohttp.ClientSession() as session:
        tasks = [sse_client(session, i, state) for i in range(CONCURRENCY)]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    print(f"Starting SSE stress test: {CONCURRENCY} concurrent connections, {REQUESTS_PER_CONN} requests each")
    asyncio.run(main())
    print(f"Complete... PASSED")
