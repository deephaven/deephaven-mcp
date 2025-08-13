#!/usr/bin/env python3
"""
Test script to verify MCP docs server resilience against unsupported HTTP methods.

This script tests that the server gracefully handles unsupported HTTP methods
(like HEAD requests) without crashing, returning proper error responses instead.
"""

import asyncio
import logging
import os
import signal
import subprocess
import sys
import time
from contextlib import asynccontextmanager

import httpx

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Server configuration
SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8001
SERVER_URL = f"http://{SERVER_HOST}:{SERVER_PORT}"


@asynccontextmanager
async def run_test_server():
    """Context manager to start and stop the MCP docs server for testing."""
    # Set required environment variable
    env = os.environ.copy()
    env["INKEEP_API_KEY"] = "test-key-for-resilience-testing"
    env["MCP_DOCS_HOST"] = SERVER_HOST
    env["MCP_DOCS_PORT"] = str(SERVER_PORT)
    
    # Start the server
    logger.info(f"Starting MCP docs server on {SERVER_URL}")
    process = subprocess.Popen([
        sys.executable, "-m", "deephaven_mcp.mcp_docs_server.main",
        "-t", "streamable-http"
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Wait for server to start
    max_wait = 10  # seconds
    start_time = time.time()
    server_ready = False
    
    while time.time() - start_time < max_wait:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{SERVER_URL}/health")
                if response.status_code == 200:
                    server_ready = True
                    logger.info("Server is ready")
                    break
        except (httpx.ConnectError, httpx.RequestError):
            pass
        await asyncio.sleep(0.5)
    
    if not server_ready:
        process.terminate()
        process.wait()
        raise RuntimeError("Server failed to start within timeout")
    
    try:
        yield process
    finally:
        # Clean shutdown
        logger.info("Shutting down server")
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


async def test_unsupported_methods():
    """Test various unsupported HTTP methods to ensure server resilience."""
    test_methods = ["HEAD", "PUT", "DELETE", "PATCH", "OPTIONS"]
    test_paths = ["/mcp", "/health", "/nonexistent"]
    
    results = []
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        for method in test_methods:
            for path in test_paths:
                url = f"{SERVER_URL}{path}"
                logger.info(f"Testing {method} {url}")
                
                try:
                    response = await client.request(method, url)
                    result = {
                        "method": method,
                        "path": path,
                        "status_code": response.status_code,
                        "success": True,
                        "error": None
                    }
                    logger.info(f"  → {response.status_code} (OK - server handled gracefully)")
                except Exception as e:
                    result = {
                        "method": method,
                        "path": path,
                        "status_code": None,
                        "success": False,
                        "error": str(e)
                    }
                    logger.error(f"  → Exception: {e} (BAD - server may have crashed)")
                
                results.append(result)
                
                # Small delay between requests
                await asyncio.sleep(0.1)
    
    return results


async def test_server_still_responsive():
    """Test that the server is still responsive after unsupported method requests."""
    logger.info("Testing server responsiveness after unsupported method tests")
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Test health endpoint
            response = await client.get(f"{SERVER_URL}/health")
            if response.status_code == 200:
                logger.info("  → Health endpoint still responsive ✓")
                return True
            else:
                logger.error(f"  → Health endpoint returned {response.status_code} ✗")
                return False
    except Exception as e:
        logger.error(f"  → Health endpoint failed: {e} ✗")
        return False


async def main():
    """Main test function."""
    logger.info("Starting MCP docs server resilience test")
    
    async with run_test_server() as server_process:
        # Test unsupported HTTP methods
        logger.info("\n=== Testing Unsupported HTTP Methods ===")
        results = await test_unsupported_methods()
        
        # Check if server is still responsive
        logger.info("\n=== Testing Server Responsiveness ===")
        server_responsive = await test_server_still_responsive()
        
        # Analyze results
        logger.info("\n=== Test Results ===")
        total_tests = len(results)
        successful_tests = sum(1 for r in results if r["success"])
        failed_tests = total_tests - successful_tests
        
        logger.info(f"Total tests: {total_tests}")
        logger.info(f"Successful (graceful handling): {successful_tests}")
        logger.info(f"Failed (exceptions/crashes): {failed_tests}")
        logger.info(f"Server still responsive: {'Yes' if server_responsive else 'No'}")
        
        # Print detailed results for failures
        if failed_tests > 0:
            logger.info("\nFailed tests:")
            for result in results:
                if not result["success"]:
                    logger.info(f"  {result['method']} {result['path']}: {result['error']}")
        
        # Overall assessment
        if failed_tests == 0 and server_responsive:
            logger.info("\n✅ SUCCESS: Server is resilient to unsupported HTTP methods")
            return 0
        else:
            logger.error("\n❌ FAILURE: Server is not fully resilient")
            return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed with exception: {e}")
        sys.exit(1)
