#!/usr/bin/env python3
"""
In-process concurrent load test for the Deephaven MCP docs ``docs_chat`` tool.

This script imports the ``docs_chat`` tool function directly (no MCP transport)
and drives many concurrent calls against it, then reports per-request timing,
success rates, throughput, and response-length statistics. ``docs_chat`` creates
its own Inkeep-backed client per request from ``INKEEP_API_KEY``, so the only
prerequisite is that the key is set in the environment (or a ``.env`` file).

For an HTTP-transport stress test against a deployed endpoint, use
``scripts/mcp_docs_stress_http.py`` instead.

Usage:
    # Set INKEEP_API_KEY (in the environment or a .env file), then:
    python scripts/mcp_docs_stress_test.py

Defaults:
- Query: "Write a query to join quotes onto trades."
- Iterations: 100 concurrent requests

Output:
- Real-time logging of progress.
- Summary statistics (success rate, response times, throughput) to the console.
- Detailed per-request metrics written to ``stress_test_results.json``.
"""

import asyncio
import json
import os
import statistics
import sys
import time
from typing import Any

# Load environment variables from .env file
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    print(
        "Warning: python-dotenv not installed. Environment variables must be set manually."
    )
    print("Install with: pip install python-dotenv")

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

try:
    from deephaven_mcp.mcp_docs_server._mcp import docs_chat
except (ImportError, RuntimeError) as e:
    # ``docs_chat``'s module reads INKEEP_API_KEY at import time and raises
    # RuntimeError if it is unset, so a missing key surfaces here as an import
    # failure rather than a per-request error.
    print(f"Error: Could not import docs_chat: {e}")
    print(
        "Make sure you're running from the project root, dependencies are "
        "installed, and INKEEP_API_KEY is set."
    )
    sys.exit(1)


async def run_single_query(query: str, iteration: int) -> dict[str, Any]:
    """
    Run a single docs_chat query and measure timing and success.

    Args:
        query: The documentation query to send to the docs_chat tool.
        iteration: The iteration number for tracking and logging.

    Returns:
        Dict containing success status, timing, response length, and error info.
    """
    start_time = time.time()

    try:
        # docs_chat ignores the context for client creation (it builds its own
        # Inkeep client per request from INKEEP_API_KEY); pass an empty context.
        result = await docs_chat(
            context={},
            prompt=query,
            history=None,
            deephaven_core_version=None,
            deephaven_enterprise_version=None,
            programming_language=None,
        )

        end_time = time.time()
        duration_ms = (end_time - start_time) * 1000

        success = bool(result.get("success"))
        response = result.get("response", "") or ""
        return {
            "iteration": iteration,
            "success": success,
            "duration_ms": duration_ms,
            "response_length": len(response),
            "error": None if success else result.get("error"),
        }

    except Exception as e:
        end_time = time.time()
        duration_ms = (end_time - start_time) * 1000

        return {
            "iteration": iteration,
            "success": False,
            "duration_ms": duration_ms,
            "response_length": 0,
            "error": str(e),
        }


async def stress_test_docs_chat(
    query: str, num_iterations: int = 100
) -> dict[str, Any]:
    """
    Run comprehensive stress test with specified number of concurrent iterations.

    This function executes the core stress test by running multiple concurrent
    requests and collecting detailed performance metrics.

    Args:
        query: The documentation query to test with.
        num_iterations: Number of concurrent requests to execute.

    Returns:
        Dict containing comprehensive test results and statistics.
    """
    print(f"Starting stress test with {num_iterations} iterations...")
    print(f"Query: {query}")
    print("-" * 70)

    start_time = time.time()

    # Run all queries concurrently.
    tasks = [run_single_query(query, i + 1) for i in range(num_iterations)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    end_time = time.time()
    total_duration = end_time - start_time

    # Process results and separate successes from failures
    successful_results = []
    failed_results = []

    for result in results:
        if isinstance(result, Exception):
            failed_results.append(
                {"success": False, "error": str(result), "duration_ms": 0}
            )
        elif result["success"]:
            successful_results.append(result)
        else:
            failed_results.append(result)

    # Calculate comprehensive statistics
    total_requests = len(results)
    successful_count = len(successful_results)
    failed_count = len(failed_results)
    success_rate = (
        (successful_count / total_requests) * 100 if total_requests > 0 else 0
    )
    requests_per_second = total_requests / total_duration if total_duration > 0 else 0

    # Response time statistics (for successful requests only)
    response_times = []
    response_lengths = []

    if successful_results:
        response_times = [r["duration_ms"] for r in successful_results]
        response_lengths = [r["response_length"] for r in successful_results]

    # Compile comprehensive results
    test_results = {
        "test_config": {
            "query": query,
            "iterations": num_iterations,
            "total_duration_seconds": total_duration,
        },
        "summary": {
            "total_requests": total_requests,
            "successful": successful_count,
            "failed": failed_count,
            "success_rate_percent": success_rate,
            "requests_per_second": requests_per_second,
        },
        "response_time_stats": {},
        "response_length_stats": {},
        "errors": [r["error"] for r in failed_results if r.get("error")],
        "detailed_results": results,
    }

    # Add response time statistics if we have successful requests
    if response_times:
        test_results["response_time_stats"] = {
            "min_ms": min(response_times),
            "max_ms": max(response_times),
            "mean_ms": statistics.mean(response_times),
            "median_ms": statistics.median(response_times),
            "std_dev_ms": (
                statistics.stdev(response_times) if len(response_times) > 1 else 0
            ),
        }

    # Add response length statistics if we have successful requests
    if response_lengths:
        test_results["response_length_stats"] = {
            "min_chars": min(response_lengths),
            "max_chars": max(response_lengths),
            "mean_chars": statistics.mean(response_lengths),
            "median_chars": statistics.median(response_lengths),
        }

    # Print summary to console
    print(f"\nStress Test Results:")
    print(f"Total Requests: {total_requests}")
    print(f"Successful: {successful_count}")
    print(f"Failed: {failed_count}")
    print(f"Success Rate: {success_rate:.1f}%")
    print(f"Total Duration: {total_duration:.2f} seconds")
    print(f"Requests/Second: {requests_per_second:.2f}")

    if response_times:
        print(f"\nResponse Time Statistics:")
        print(f"  Min: {min(response_times):.1f} ms")
        print(f"  Max: {max(response_times):.1f} ms")
        print(f"  Mean: {statistics.mean(response_times):.1f} ms")
        print(f"  Median: {statistics.median(response_times):.1f} ms")
        print(
            f"  Std Dev: {statistics.stdev(response_times) if len(response_times) > 1 else 0:.1f} ms"
        )

    if response_lengths:
        print(f"\nResponse Length Statistics:")
        print(f"  Min: {min(response_lengths)} chars")
        print(f"  Max: {max(response_lengths)} chars")
        print(f"  Mean: {statistics.mean(response_lengths):.0f} chars")
        print(f"  Median: {statistics.median(response_lengths):.0f} chars")

    # Show first few errors if any occurred
    if failed_results:
        print(f"\nFirst {min(5, len(failed_results))} Errors:")
        for i, result in enumerate(failed_results[:5], 1):
            print(f"  {i}. {result.get('error', 'Unknown error')}")

    return test_results


async def main():
    """
    Main function to execute the stress test.

    Configures and runs the stress test and saves results to a JSON file.
    """
    # Test configuration
    query = "Write a query to join quotes onto trades."
    iterations = 100

    try:
        print("=" * 70)
        print("MCP DOCS SERVER STRESS TEST")
        print("=" * 70)
        print("Concurrent in-process load test of the docs_chat tool.")
        print("=" * 70)

        results = await stress_test_docs_chat(query, iterations)

        # Save detailed results to JSON file
        output_file = "stress_test_results.json"
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2)

        print(f"\nDetailed results saved to: {output_file}")

        # Provide interpretation of results
        success_rate = results["summary"]["success_rate_percent"]
        if success_rate == 100.0:
            print(f"\n✅ EXCELLENT: 100% success rate")
        elif success_rate >= 95.0:
            print(
                f"\n✅ GOOD: {success_rate:.1f}% success rate - minor issues detected"
            )
        elif success_rate >= 80.0:
            print(
                f"\n⚠️  WARNING: {success_rate:.1f}% success rate - significant issues detected"
            )
        else:
            print(
                f"\n❌ CRITICAL: {success_rate:.1f}% success rate - major problems detected"
            )

        if results["summary"]["failed"] > 0:
            print(f"   Review errors in {output_file} for detailed failure analysis")

    except Exception as e:
        print(f"Stress test failed with error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
