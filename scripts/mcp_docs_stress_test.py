#!/usr/bin/env python3
"""
MCP Docs Server Stress Test Script

This script performs comprehensive stress testing of the Deephaven MCP docs server
to validate performance, stability, and error handling under concurrent load.

OVERVIEW
========
The script runs concurrent requests against the docs_chat tool to:
- Validate that timeout fixes prevent "Truncated response body" errors
- Measure response times and throughput under load
- Test resource cleanup and connection management
- Generate detailed performance metrics and error reports

BACKGROUND
==========
This script was created to validate fixes for critical timeout issues that were
causing "Truncated response body" errors during high-volume usage of the MCP docs
server. The original issues were caused by:
- HTTP connection leaks in the OpenAI client
- Missing lifecycle management in the MCP server
- Lack of proper resource cleanup
- Connection pool exhaustion under concurrent load

FIXES VALIDATED
===============
This stress test validates the following architectural improvements:
1. OpenAI client connection cleanup with proper close() method
2. FastMCP lifecycle management with @asynccontextmanager
3. Dependency injection pattern for resource management
4. Advanced HTTP client configuration (timeouts, connection limits, retries)
5. Proper error handling and logging throughout the request pipeline

USAGE
=====
Prerequisites:
- Set INKEEP_API_KEY environment variable (or use .env file)
- Run from the project root directory
- Ensure the MCP server dependencies are installed

Basic usage:
    python scripts/mcp_docs_stress_test.py

The script will:
- Load environment variables from .env file if available
- Run 100 concurrent requests to the docs_chat tool
- Measure response times, success rates, and error patterns
- Save detailed results to stress_test_results.json
- Print summary statistics to the console

CONFIGURATION
=============
Default settings:
- Query: "Write a query to join quotes onto trades."
- Iterations: 100 concurrent requests
- Model: inkeep-context-expert (Inkeep API)
- Base URL: https://api.inkeep.com/v1

To modify test parameters, edit the main() function or create a custom version.

OUTPUT
======
The script generates:
1. Real-time logging of request progress with timestamps
2. Summary statistics (success rate, response times, throughput)
3. Detailed JSON results file with per-request metrics
4. Error analysis for any failed requests

PERFORMANCE EXPECTATIONS
========================
With the timeout fixes in place, expected results:
- 100% success rate (no "Truncated response body" errors)
- Response times: 15-180 seconds per request (depends on query complexity)
- Throughput: 0.5-2.0 requests/second (limited by API rate limits)
- Zero connection leaks or resource exhaustion errors

TROUBLESHOOTING
===============
Common issues:
- "INKEEP_API_KEY environment variable must be set" → Set API key in .env file
- "Invalid model" error → Verify model name matches server configuration
- Import errors → Run from project root with proper Python environment
- Connection timeouts → Check network connectivity and API key validity

DEVELOPMENT NOTES
=================
This script uses the new dependency injection architecture introduced to fix
the timeout issues. It creates a proper context with the OpenAI client and
passes it to the docs_chat function, matching the production MCP server pattern.

The script includes proper resource cleanup to prevent connection leaks and
uses the same OpenAI client configuration as the production server for
accurate testing results.

For more information on the timeout fixes and architectural changes, see:
- src/deephaven_mcp/openai.py (OpenAI client with connection management)
- src/deephaven_mcp/mcp_docs_server/_mcp.py (MCP server with lifecycle management)
- tests/mcp_docs_server/ (comprehensive unit tests with 100% coverage)

Author: Deephaven MCP Development Team
Created: 2025-01-23 (during timeout issue resolution)
Last Updated: 2025-01-23
"""

import asyncio
import json
import os
import statistics
import sys
import time
from typing import Any, Dict, List

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
    from deephaven_mcp.openai import OpenAIClient
except ImportError as e:
    print(f"Error: Could not import required modules: {e}")
    print(
        "Make sure you're running from the project root and dependencies are installed."
    )
    sys.exit(1)

# Global client instance for the stress test
_client = None


async def get_client():
    """
    Get or create the OpenAI client instance.

    Creates a singleton OpenAI client configured to match the production
    MCP docs server settings. Uses the same model, base URL, and advanced
    HTTP client configuration that was implemented to fix timeout issues.

    Returns:
        OpenAIClient: Configured client instance for Inkeep API communication.

    Raises:
        RuntimeError: If INKEEP_API_KEY environment variable is not set.
    """
    global _client
    if _client is None:
        api_key = os.getenv("INKEEP_API_KEY")
        if not api_key:
            raise RuntimeError(
                "INKEEP_API_KEY environment variable must be set. "
                "Add it to your .env file or set it in your environment."
            )

        _client = OpenAIClient(
            api_key=api_key,
            base_url="https://api.inkeep.com/v1",
            model="inkeep-context-expert",
        )
    return _client


async def cleanup_client():
    """
    Clean up the OpenAI client and close HTTP connections.

    This is critical for preventing connection leaks that were causing
    the original "Truncated response body" errors. The cleanup ensures
    all HTTP connections are properly closed.
    """
    global _client
    if _client:
        await _client.close()
        _client = None


async def run_single_query(query: str, iteration: int) -> Dict[str, Any]:
    """
    Run a single docs_chat query and measure timing and success.

    This function replicates the exact calling pattern used by MCP clients,
    including the dependency injection context that was implemented to fix
    the architectural inconsistencies that contributed to timeout issues.

    Args:
        query: The documentation query to send to the docs_chat tool.
        iteration: The iteration number for tracking and logging.

    Returns:
        Dict containing success status, timing, response length, and error info.
    """
    start_time = time.time()

    try:
        # Get the OpenAI client and create context (matches production pattern)
        client = await get_client()
        context = {"inkeep_client": client}

        # Call the docs_chat function with proper dependency injection
        result = await docs_chat(
            context=context,
            prompt=query,
            history=None,
            deephaven_core_version=None,
            deephaven_enterprise_version=None,
            programming_language=None,
        )

        end_time = time.time()
        duration_ms = (end_time - start_time) * 1000

        return {
            "iteration": iteration,
            "success": True,
            "duration_ms": duration_ms,
            "response_length": len(result) if result else 0,
            "error": None,
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
) -> Dict[str, Any]:
    """
    Run comprehensive stress test with specified number of concurrent iterations.

    This function executes the core stress test by running multiple concurrent
    requests and collecting detailed performance metrics. The concurrent execution
    pattern replicates the conditions that originally caused timeout issues.

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

    # Run all queries concurrently (this pattern exposed the original timeout issues)
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

    Configures and runs the stress test, saves results to JSON file,
    and ensures proper cleanup of resources.
    """
    # Test configuration
    query = "Write a query to join quotes onto trades."
    iterations = 100

    try:
        print("=" * 70)
        print("MCP DOCS SERVER STRESS TEST")
        print("=" * 70)
        print(f"This test validates the timeout fixes implemented to prevent")
        print(f"'Truncated response body' errors during concurrent usage.")
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
            print(
                "\n✅ EXCELLENT: 100% success rate - timeout fixes are working perfectly!"
            )
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
    finally:
        # Critical: Clean up the OpenAI client to prevent connection leaks
        await cleanup_client()

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
