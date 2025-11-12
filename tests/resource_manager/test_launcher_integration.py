"""
Integration tests for Docker and pip launchers.

These tests actually run Docker containers and pip processes, so they:
- Require Docker to be installed and running
- Require 'deephaven-server' to be pip-installable (from PyPI or local)
- Are slower than unit tests
- Are marked with @pytest.mark.integration and skipped by default

Run with: uv run pytest -m integration -s

IMPORTANT: The -s flag is REQUIRED to disable pytest output capture.
Pytest's output capturing interferes with subprocess pipes, causing
deephaven processes to abort. Always use -s when running integration tests.

Prerequisites:
- Docker must be installed and running
- Install deephaven for pip tests: pip install deephaven-server

Note: These tests verify that our command-line construction and process
management actually work with real Docker/pip processes, not just mocks.
"""

import asyncio
import logging
import os
import shutil
import signal
import sys
import threading
import time
from pathlib import Path

import pytest

_LOGGER = logging.getLogger(__name__)

# Global lock to prevent port allocation race conditions in parallel tests
_PORT_ALLOCATION_LOCK = threading.Lock()

from deephaven_mcp.resource_manager import (
    DockerLaunchedSession,
    PythonLaunchedSession,
    find_available_port,
)
from deephaven_mcp.resource_manager._instance_tracker import (
    InstanceTracker,
    cleanup_orphaned_resources,
)
from deephaven_mcp.resource_manager._launcher import _find_deephaven_executable


# Helper to check if deephaven command is available
def is_deephaven_available() -> bool:
    """Check if deephaven command is available in the same venv as current Python."""
    deephaven_cmd = _find_deephaven_executable(None)

    # If it returned an absolute path, it exists
    if Path(deephaven_cmd).is_absolute():
        return True

    # Otherwise it's "deephaven" - check if it's in PATH
    return shutil.which(deephaven_cmd) is not None


def find_available_port_locked() -> int:
    """
    Thread-safe port allocation for integration tests.

    Uses a lock to prevent multiple tests from getting the same port when
    run in parallel. This solves the TOCTOU race condition in find_available_port()
    where multiple tests call it simultaneously and get the same port.
    """
    with _PORT_ALLOCATION_LOCK:
        port = find_available_port()
        # Small delay to ensure the port is not immediately reused by OS
        time.sleep(2)
        return port


@pytest.mark.integration
class TestDockerLauncherIntegration:
    """Integration tests for Docker launcher with real containers."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(300)  # 5 minute timeout
    async def test_docker_launch_and_cleanup(self):
        """Test actual Docker container launch, health check, and cleanup."""
        session = None
        port = find_available_port_locked()
        _LOGGER.info(f"[Integration Test] Allocated port {port} for Docker test")
        try:
            # Launch a real Docker container
            _LOGGER.info(
                f"[Integration Test] Launching Docker container on port {port}"
            )
            session = await DockerLaunchedSession.launch(
                session_name="integration-test",
                port=port,
                auth_token="test-token-123",
                heap_size_gb=2,
                extra_jvm_args=[],
                environment_vars={},
                docker_image="ghcr.io/deephaven/server:latest",
                docker_memory_limit_gb=4.0,
                docker_cpu_limit=None,
                docker_volumes=[],
                instance_id=None,
            )

            # Verify container was created
            assert session is not None
            assert session.container_id
            assert len(session.container_id) > 0
            assert session.port == port
            assert session.launch_method == "docker"

            # Wait for session to be ready (health check)
            # This verifies the container actually starts and responds
            _LOGGER.info(
                f"[Integration Test] Container {session.container_id[:12]} started, waiting for health check"
            )
            ready = await session.wait_until_ready(
                timeout_seconds=60, check_interval_seconds=2
            )
            assert (
                ready
            ), f"Docker container {session.container_id[:12]} failed to become ready within 60s"
            _LOGGER.info(
                f"[Integration Test] Container {session.container_id[:12]} is ready"
            )

            # Verify connection URL is correct
            assert session.connection_url == f"http://localhost:{port}"
            assert "authToken=test-token-123" in session.connection_url_with_auth

        finally:
            # Clean up - stop and remove container
            if session:
                _LOGGER.info(
                    f"[Integration Test] Cleaning up container {session.container_id[:12]}"
                )
                await session.stop()
                _LOGGER.info(f"[Integration Test] Container cleanup complete")

    @pytest.mark.asyncio
    @pytest.mark.timeout(300)  # 5 minute timeout
    async def test_docker_launch_with_instance_label(self):
        """Test Docker container gets labeled with instance ID for orphan tracking."""
        session = None
        instance_id = "test-integration-instance-123"
        port = find_available_port_locked()

        try:
            # Launch with instance_id
            session = await DockerLaunchedSession.launch(
                session_name="integration-label-test",
                port=port,
                auth_token="test-token-456",
                heap_size_gb=2,
                extra_jvm_args=[],
                environment_vars={},
                docker_image="ghcr.io/deephaven/server:latest",
                docker_memory_limit_gb=None,
                docker_cpu_limit=None,
                docker_volumes=[],
                instance_id=instance_id,
            )

            # Wait for session to be ready (health check)
            # This verifies the container actually starts and responds
            ready = await session.wait_until_ready(
                timeout_seconds=60, check_interval_seconds=2
            )
            assert (
                ready
            ), f"Docker container {session.container_id[:12]} failed to become ready within 60s"

            # Verify container has the label by checking with docker inspect
            process = await asyncio.create_subprocess_exec(
                "docker",
                "inspect",
                "--format",
                '{{index .Config.Labels "deephaven-mcp-server-instance"}}',
                session.container_id,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                label_value = stdout.decode().strip()
                assert (
                    label_value == instance_id
                ), f"Expected label '{instance_id}', got '{label_value}'"
            else:
                pytest.fail(f"Failed to inspect container: {stderr.decode()}")

        finally:
            if session:
                await session.stop()


@pytest.mark.integration
class TestPythonLauncherIntegration:
    """Integration tests for python launcher with real processes."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(300)  # 5 minute timeout (pip takes longer to start)
    @pytest.mark.skipif(
        not is_deephaven_available(), reason="deephaven command not in PATH"
    )
    @pytest.mark.parametrize(
        "python_venv_path,test_suffix",
        [
            (None, "default-venv"),
            (str(Path(sys.executable).parent.parent), "custom-venv"),
        ],
        ids=["default_venv", "custom_venv"],
    )
    async def test_python_launch_and_cleanup(self, python_venv_path, test_suffix):
        """Test actual python process launch, health check, and cleanup.

        Tests both default venv (None) and explicit custom venv path.

        Prerequisites:
        - deephaven-server must be installed: pip install deephaven-server
        """
        session = None
        port = find_available_port_locked()
        try:
            # Launch a real python process
            session = await PythonLaunchedSession.launch(
                session_name=f"integration-python-{test_suffix}",
                port=port,
                auth_token=f"test-token-{test_suffix}",
                heap_size_gb=2,
                extra_jvm_args=[],
                environment_vars={},
                python_venv_path=python_venv_path,
            )

            # Verify process was created
            assert session is not None
            assert session.process is not None
            assert session.process.pid > 0
            assert session.port == port
            assert session.launch_method == "python"

            # Check if process is still running before health check
            _LOGGER.info(
                f"[Integration Test] Process PID {session.process.pid} launched, checking status"
            )
            # Give the process a moment to start or fail
            await asyncio.sleep(0.5)
            if session.process.returncode is not None:
                # Process crashed immediately - capture error output
                stderr_data = ""
                stdout_data = ""
                try:
                    if session.process.stderr:
                        stderr_bytes = await asyncio.wait_for(
                            session.process.stderr.read(), timeout=1.0
                        )
                        stderr_data = stderr_bytes.decode() if stderr_bytes else ""
                    if session.process.stdout:
                        stdout_bytes = await asyncio.wait_for(
                            session.process.stdout.read(), timeout=1.0
                        )
                        stdout_data = stdout_bytes.decode() if stdout_bytes else ""
                except Exception as e:
                    stderr_data = f"Could not read output: {e}"
                pytest.fail(
                    f"Process PID {session.process.pid} exited immediately with code {session.process.returncode}\n"
                    f"=== FULL STDOUT ===\n{stdout_data}\n"
                    f"=== FULL STDERR ===\n{stderr_data}"
                )

            # Wait for session to be ready (health check)
            # This verifies the process actually starts and responds
            # Use longer timeout for pip tests - JVM startup can be slow under load
            _LOGGER.info(
                f"[Integration Test] Starting health check for PID {session.process.pid}"
            )
            ready = await session.wait_until_ready(
                timeout_seconds=240, check_interval_seconds=5
            )

            # If health check failed, capture stderr for debugging
            if not ready:
                _LOGGER.error(
                    f"[Integration Test] Health check failed! Process returncode: {session.process.returncode}"
                )
                stderr_output = ""
                stdout_output = ""

                # Try to get any output from the process - read everything available
                if session.process.stderr:
                    try:
                        stderr_bytes = await asyncio.wait_for(
                            session.process.stderr.read(),  # Read all available data
                            timeout=2.0,
                        )
                        stderr_output = stderr_bytes.decode()
                    except Exception as e:
                        stderr_output = f"Could not read stderr: {e}"

                if session.process.stdout:
                    try:
                        stdout_bytes = await asyncio.wait_for(
                            session.process.stdout.read(),  # Read all available data
                            timeout=2.0,
                        )
                        stdout_output = stdout_bytes.decode()
                    except Exception as e:
                        stdout_output = f"Could not read stdout: {e}"

                pytest.fail(
                    f"Session failed to become ready within 240s.\n"
                    f"Process returncode: {session.process.returncode}\n"
                    f"=== FULL STDOUT ===\n{stdout_output}\n"
                    f"=== FULL STDERR ===\n{stderr_output}"
                )

            # Verify connection URL is correct
            assert session.connection_url == f"http://localhost:{port}"
            assert f"authToken=test-token-{test_suffix}" in session.connection_url_with_auth

            # Verify process is actually running
            assert session.process.returncode is None

        finally:
            # Clean up - terminate process
            if session:
                await session.stop()
                # Give it a moment to clean up
                await asyncio.sleep(1)


@pytest.mark.integration
class TestOrphanCleanupIntegration:
    """Integration tests for cleanup of orphaned Docker/pip processes."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(180)
    async def test_cleanup_orphaned_docker_container(self, tmp_path):
        """Test that orphaned Docker containers are actually cleaned up."""
        container_id = None
        instance_id = "orphan-test-instance-456"
        port = find_available_port_locked()

        try:
            # Create instance tracker and register
            instances_dir = tmp_path / ".deephaven-mcp" / "instances"
            instances_dir.mkdir(parents=True, exist_ok=True)

            # Create a real Docker container with our label
            process = await asyncio.create_subprocess_exec(
                "docker",
                "run",
                "-d",
                "--rm",
                "-p",
                f"{port}:10000",
                "--label",
                f"deephaven-mcp-server-instance={instance_id}",
                "ghcr.io/deephaven/server:latest",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                pytest.fail(f"Failed to start test container: {stderr.decode()}")

            container_id = stdout.decode().strip()
            assert container_id

            # Create fake instance metadata with dead PID
            instance_file = instances_dir / f"{instance_id}.json"
            import json

            instance_file.write_text(
                json.dumps(
                    {
                        "instance_id": instance_id,
                        "pid": 99999999,  # Fake dead PID
                        "started_at": "2025-01-01T00:00:00Z",
                        "python_processes": {},
                    }
                )
            )

            # Temporarily override Path.home() to use tmp_path
            import deephaven_mcp.resource_manager._instance_tracker as tracker_mod

            original_home = Path.home

            def mock_home():
                return tmp_path

            Path.home = staticmethod(mock_home)

            try:
                # Run cleanup - should find and stop the container
                await cleanup_orphaned_resources()

                # Wait a moment for Docker to process the stop
                await asyncio.sleep(2)

                # Verify container was stopped
                check_process = await asyncio.create_subprocess_exec(
                    "docker",
                    "ps",
                    "-q",
                    "--filter",
                    f"id={container_id}",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, _ = await check_process.communicate()

                # Container should not be in running containers list
                running_containers = stdout.decode().strip()
                assert (
                    not running_containers
                ), f"Container {container_id} still running after cleanup"

                # Instance metadata should be removed
                assert not instance_file.exists()

            finally:
                Path.home = original_home

        finally:
            # Cleanup - make sure container is stopped even if test fails
            if container_id:
                try:
                    stop_process = await asyncio.create_subprocess_exec(
                        "docker",
                        "stop",
                        container_id,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    await stop_process.communicate()
                except Exception:
                    pass  # Best effort cleanup

    @pytest.mark.asyncio
    @pytest.mark.timeout(300)  # 5 minute timeout (python launch takes longer to start)
    @pytest.mark.skipif(
        not is_deephaven_available(), reason="deephaven command not in PATH"
    )
    async def test_cleanup_orphaned_python_process(self, tmp_path):
        """Test that orphaned python processes are actually cleaned up.

        Prerequisites:
        - deephaven-server must be installed: pip install deephaven-server
        """
        process = None
        instance_id = "orphan-pip-test-instance-789"
        port = find_available_port_locked()

        try:
            # Start a real pip process
            process = await asyncio.create_subprocess_exec(
                "deephaven",
                "server",
                "--port",
                str(port),
                "--jvm-args",
                "-Xmx2g",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            # Wait a moment for process to start
            await asyncio.sleep(2)

            # Verify process is running
            assert process.returncode is None

            # Create instance metadata directory
            instances_dir = tmp_path / ".deephaven-mcp" / "instances"
            instances_dir.mkdir(parents=True, exist_ok=True)

            # Create fake instance metadata with dead server PID but live python process
            instance_file = instances_dir / f"{instance_id}.json"
            import json

            instance_file.write_text(
                json.dumps(
                    {
                        "instance_id": instance_id,
                        "pid": 99999999,  # Fake dead server PID
                        "started_at": "2025-01-01T00:00:00Z",
                        "python_processes": {
                            "test-session": process.pid  # Real python process PID
                        },
                    }
                )
            )

            # Temporarily override Path.home() to use tmp_path
            original_home = Path.home

            def mock_home():
                return tmp_path

            Path.home = staticmethod(mock_home)

            try:
                # Run cleanup - should find and kill the process
                await cleanup_orphaned_resources()

                # Wait for process to be terminated
                await asyncio.sleep(2)

                # Verify process was killed
                try:
                    os.kill(process.pid, 0)
                    pytest.fail(f"Process {process.pid} still running after cleanup")
                except OSError:
                    pass  # Good - process is dead

                # Instance metadata should be removed
                assert not instance_file.exists()

            finally:
                Path.home = original_home

        finally:
            # Cleanup - make sure process is killed even if test fails
            if process and process.returncode is None:
                try:
                    process.kill()
                    await process.wait()
                except Exception:
                    pass  # Best effort cleanup


@pytest.mark.integration
class TestInstanceTrackerIntegration:
    """Integration tests for instance tracker with real processes."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(180)
    @pytest.mark.skipif(
        not is_deephaven_available(), reason="deephaven command not in PATH"
    )
    async def test_track_and_untrack_real_python_process(self, tmp_path):
        """Test tracking a real python process through its lifecycle.

        Prerequisites:
        - deephaven-server must be installed: pip install deephaven-server
        """
        process = None
        tracker = None
        port = find_available_port_locked()

        # Temporarily override Path.home() to use tmp_path
        original_home = Path.home

        def mock_home():
            return tmp_path

        Path.home = staticmethod(mock_home)

        try:
            # Create and register instance tracker
            tracker = await InstanceTracker.create_and_register()

            # Start a real pip process
            process = await asyncio.create_subprocess_exec(
                "deephaven",
                "server",
                "--port",
                str(port),
                "--jvm-args",
                "-Xmx2g",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            # Wait a moment for process to start
            await asyncio.sleep(2)
            assert process.returncode is None

            # Track the process
            await tracker.track_python_process("test-session", process.pid)

            # Verify process is tracked in metadata
            import json

            instance_file = (
                tmp_path
                / ".deephaven-mcp"
                / "instances"
                / f"{tracker.instance_id}.json"
            )
            data = json.loads(instance_file.read_text())
            assert "test-session" in data["python_processes"]
            assert data["python_processes"]["test-session"] == process.pid

            # Untrack the process
            await tracker.untrack_python_process("test-session")

            # Verify process is no longer tracked
            data = json.loads(instance_file.read_text())
            assert "test-session" not in data["python_processes"]

            # Process should still be running (we just untracked it)
            assert process.returncode is None

        finally:
            Path.home = original_home

            # Cleanup
            if tracker:
                await tracker.unregister()
            if process and process.returncode is None:
                try:
                    process.kill()
                    await process.wait()
                except Exception:
                    pass
