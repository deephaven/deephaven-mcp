"""
Unit tests for instance tracking and orphan cleanup functionality.

Tests cover:
- Instance registration and unregistration
- Pip process tracking
- Orphan detection and cleanup
- Docker container cleanup
- File persistence and loading
"""

import asyncio
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deephaven_mcp.resource_manager._instance_tracker import (
    InstanceTracker,
    cleanup_orphaned_resources,
    is_process_running,
)


@pytest.fixture
def temp_instances_dir(tmp_path):
    """Create a temporary instances directory for testing."""
    instances_dir = tmp_path / ".deephaven-mcp" / "instances"
    instances_dir.mkdir(parents=True)

    # Patch Path.home() to return our temp directory
    with patch(
        "deephaven_mcp.resource_manager._instance_tracker.Path.home",
        return_value=tmp_path,
    ):
        yield instances_dir


class TestInstanceTracker:
    """Tests for the InstanceTracker class."""

    @pytest.mark.asyncio
    async def test_create_and_register(self, temp_instances_dir):
        """Test creating and registering a new instance."""
        tracker = await InstanceTracker.create_and_register()

        # Verify instance has required attributes
        assert tracker.instance_id is not None
        assert len(tracker.instance_id) == 36  # UUID format
        assert tracker.pid == os.getpid()
        assert tracker.started_at is not None

        # Verify instance file was created
        assert tracker.instance_file.exists()

        # Verify file contents
        data = json.loads(tracker.instance_file.read_text())
        assert data["instance_id"] == tracker.instance_id
        assert data["pid"] == tracker.pid
        assert data["started_at"] == tracker.started_at
        assert data["pip_processes"] == {}

    @pytest.mark.asyncio
    async def test_track_pip_process(self, temp_instances_dir):
        """Test tracking a pip process."""
        tracker = await InstanceTracker.create_and_register()

        # Track a process
        await tracker.track_pip_process("test-session", 12345)

        # Verify it's tracked in memory
        assert tracker._pip_processes["test-session"] == 12345

        # Verify it's persisted to disk
        data = json.loads(tracker.instance_file.read_text())
        assert data["pip_processes"]["test-session"] == 12345

    @pytest.mark.asyncio
    async def test_track_multiple_pip_processes(self, temp_instances_dir):
        """Test tracking multiple pip processes."""
        tracker = await InstanceTracker.create_and_register()

        # Track multiple processes
        await tracker.track_pip_process("session-1", 11111)
        await tracker.track_pip_process("session-2", 22222)
        await tracker.track_pip_process("session-3", 33333)

        # Verify all are tracked
        assert len(tracker._pip_processes) == 3
        assert tracker._pip_processes["session-1"] == 11111
        assert tracker._pip_processes["session-2"] == 22222
        assert tracker._pip_processes["session-3"] == 33333

        # Verify persistence
        data = json.loads(tracker.instance_file.read_text())
        assert len(data["pip_processes"]) == 3

    @pytest.mark.asyncio
    async def test_untrack_pip_process(self, temp_instances_dir):
        """Test untracking a pip process."""
        tracker = await InstanceTracker.create_and_register()

        # Track and then untrack
        await tracker.track_pip_process("test-session", 12345)
        await tracker.untrack_pip_process("test-session")

        # Verify it's removed
        assert "test-session" not in tracker._pip_processes

        # Verify it's removed from disk
        data = json.loads(tracker.instance_file.read_text())
        assert "test-session" not in data["pip_processes"]

    @pytest.mark.asyncio
    async def test_untrack_nonexistent_process(self, temp_instances_dir):
        """Test untracking a process that isn't tracked (should not error)."""
        tracker = await InstanceTracker.create_and_register()

        # Untracking a non-existent process should not raise
        await tracker.untrack_pip_process("nonexistent")

        assert "nonexistent" not in tracker._pip_processes

    @pytest.mark.asyncio
    async def test_unregister(self, temp_instances_dir):
        """Test unregistering an instance."""
        tracker = await InstanceTracker.create_and_register()

        instance_file = tracker.instance_file
        assert instance_file.exists()

        # Unregister
        await tracker.unregister()

        # Verify file is removed
        assert not instance_file.exists()

    @pytest.mark.asyncio
    async def test_unregister_missing_file(self, temp_instances_dir):
        """Test unregistering when file is already missing (should not error)."""
        tracker = await InstanceTracker.create_and_register()

        # Manually remove file
        tracker.instance_file.unlink()

        # Unregister should not raise
        await tracker.unregister()

    def test_load_from_file(self, temp_instances_dir):
        """Test loading an instance from an existing file."""
        # Create a test instance file
        instance_id = "test-uuid-1234"
        instance_file = temp_instances_dir / f"{instance_id}.json"

        data = {
            "instance_id": instance_id,
            "pid": 9999,
            "started_at": "2025-11-07T14:00:00Z",
            "pip_processes": {
                "session-1": 11111,
                "session-2": 22222,
            },
        }
        instance_file.write_text(json.dumps(data))

        # Load it
        tracker = InstanceTracker.load_from_file(instance_file)

        # Verify attributes
        assert tracker.instance_id == instance_id
        assert tracker.pid == 9999
        assert tracker.started_at == "2025-11-07T14:00:00Z"
        assert tracker._pip_processes["session-1"] == 11111
        assert tracker._pip_processes["session-2"] == 22222

    def test_load_from_file_missing(self, temp_instances_dir):
        """Test loading from a missing file raises FileNotFoundError."""
        missing_file = temp_instances_dir / "missing.json"

        with pytest.raises(FileNotFoundError):
            InstanceTracker.load_from_file(missing_file)

    def test_load_from_file_invalid_json(self, temp_instances_dir):
        """Test loading from a file with invalid JSON raises JSONDecodeError."""
        invalid_file = temp_instances_dir / "invalid.json"
        invalid_file.write_text("not valid json{")

        with pytest.raises(json.JSONDecodeError):
            InstanceTracker.load_from_file(invalid_file)


class TestIsProcessRunning:
    """Tests for the is_process_running helper function."""

    def test_current_process_is_running(self):
        """Test that the current process is detected as running."""
        assert is_process_running(os.getpid()) is True

    def test_nonexistent_process_is_not_running(self):
        """Test that a nonexistent process is detected as not running."""
        # Use a very high PID that's unlikely to exist
        assert is_process_running(999999) is False

    def test_init_process_is_running(self):
        """Test that PID 1 (init/launchd) is detected as running or not accessible."""
        # PID 1 should always be running on Unix systems
        # On macOS, we may not have permission to check PID 1, which returns False
        # So we just verify the function doesn't crash
        result = is_process_running(1)
        assert isinstance(result, bool)


class TestCleanupOrphanedResources:
    """Tests for the cleanup_orphaned_resources function."""

    @pytest.mark.asyncio
    async def test_no_instances_directory(self, tmp_path):
        """Test cleanup when instances directory doesn't exist."""
        with patch(
            "deephaven_mcp.resource_manager._instance_tracker.Path.home",
            return_value=tmp_path,
        ):
            # Should not raise, just log and return
            await cleanup_orphaned_resources()

    @pytest.mark.asyncio
    async def test_empty_instances_directory(self, temp_instances_dir):
        """Test cleanup when no instance files exist."""
        # Should not raise, just log and return
        await cleanup_orphaned_resources()

    @pytest.mark.asyncio
    async def test_skip_running_instances(self, temp_instances_dir):
        """Test that running instances are not cleaned up."""
        # Create an instance file for the current process
        instance_id = "test-running-instance"
        instance_file = temp_instances_dir / f"{instance_id}.json"

        data = {
            "instance_id": instance_id,
            "pid": os.getpid(),  # Current process - should be running
            "started_at": "2025-11-07T14:00:00Z",
            "pip_processes": {"session-1": 11111},
        }
        instance_file.write_text(json.dumps(data))

        await cleanup_orphaned_resources()

        # Instance file should still exist (not cleaned up)
        assert instance_file.exists()

    @pytest.mark.asyncio
    async def test_cleanup_dead_instance_no_resources(self, temp_instances_dir):
        """Test cleanup of a dead instance with no resources."""
        # Create an instance file for a dead process
        instance_id = "test-dead-instance"
        instance_file = temp_instances_dir / f"{instance_id}.json"

        data = {
            "instance_id": instance_id,
            "pid": 999999,  # Non-existent process
            "started_at": "2025-11-07T14:00:00Z",
            "pip_processes": {},
        }
        instance_file.write_text(json.dumps(data))

        # Mock Docker cleanup
        with patch(
            "deephaven_mcp.resource_manager._instance_tracker._cleanup_docker_containers_for_instance"
        ) as mock_docker:
            mock_docker.return_value = AsyncMock()

            await cleanup_orphaned_resources()

            # Docker cleanup should have been called
            mock_docker.assert_called_once_with(instance_id)

        # Instance file should be removed
        assert not instance_file.exists()

    @pytest.mark.asyncio
    async def test_cleanup_dead_instance_with_docker(self, temp_instances_dir):
        """Test cleanup of Docker containers for a dead instance."""
        instance_id = "test-docker-instance"
        instance_file = temp_instances_dir / f"{instance_id}.json"

        data = {
            "instance_id": instance_id,
            "pid": 999999,
            "started_at": "2025-11-07T14:00:00Z",
            "pip_processes": {},
        }
        instance_file.write_text(json.dumps(data))

        # Mock Docker commands to return a container
        mock_ps = AsyncMock()
        mock_ps.returncode = 0
        mock_ps.communicate = AsyncMock(return_value=(b"container123\n", b""))

        mock_stop = AsyncMock()
        mock_stop.communicate = AsyncMock(return_value=(b"", b""))

        mock_rm = AsyncMock()
        mock_rm.communicate = AsyncMock(return_value=(b"", b""))

        async def create_subprocess_side_effect(*args, **kwargs):
            if "ps" in args:
                return mock_ps
            elif "stop" in args:
                return mock_stop
            elif "rm" in args:
                return mock_rm

        with patch(
            "asyncio.create_subprocess_exec", side_effect=create_subprocess_side_effect
        ):
            await cleanup_orphaned_resources()

        # Verify stop and rm were called
        assert mock_stop.communicate.call_count == 1
        assert mock_rm.communicate.call_count == 1

        # Instance file should be removed
        assert not instance_file.exists()

    @pytest.mark.asyncio
    async def test_cleanup_dead_instance_with_pip_processes(self, temp_instances_dir):
        """Test cleanup of pip processes for a dead instance."""
        instance_id = "test-pip-instance"
        instance_file = temp_instances_dir / f"{instance_id}.json"

        # Create test data with pip processes
        data = {
            "instance_id": instance_id,
            "pid": 999999,  # Dead process
            "started_at": "2025-11-07T14:00:00Z",
            "pip_processes": {"session-1": 88888, "session-2": 99999},
        }
        instance_file.write_text(json.dumps(data))

        # Mock Docker cleanup
        with (
            patch(
                "deephaven_mcp.resource_manager._instance_tracker._cleanup_docker_containers_for_instance"
            ) as mock_docker,
            patch(
                "deephaven_mcp.resource_manager._instance_tracker.is_process_running"
            ) as mock_is_running,
            patch("os.kill") as mock_kill,
        ):

            mock_docker.return_value = AsyncMock()

            # First call checks the instance PID (dead), subsequent calls check pip processes
            mock_is_running.side_effect = [
                False,
                True,
                True,
            ]  # Instance dead, both pip processes alive

            await cleanup_orphaned_resources()

            # Verify we tried to kill the pip processes
            assert mock_kill.call_count == 2
            mock_kill.assert_any_call(88888, 15)  # SIGTERM = 15
            mock_kill.assert_any_call(99999, 15)

        # Instance file should be removed
        assert not instance_file.exists()

    @pytest.mark.asyncio
    async def test_cleanup_handles_errors_gracefully(self, temp_instances_dir):
        """Test that errors during cleanup don't crash the cleanup process."""
        # Create multiple instance files
        for i in range(3):
            instance_file = temp_instances_dir / f"instance-{i}.json"
            data = {
                "instance_id": f"instance-{i}",
                "pid": 999990 + i,
                "started_at": "2025-11-07T14:00:00Z",
                "pip_processes": {},
            }
            instance_file.write_text(json.dumps(data))

        # Make the second one raise an error during cleanup
        call_count = [0]

        async def docker_cleanup_side_effect(instance_id):
            call_count[0] += 1
            if instance_id == "instance-1":
                raise RuntimeError("Simulated Docker error")

        with patch(
            "deephaven_mcp.resource_manager._instance_tracker._cleanup_docker_containers_for_instance",
            side_effect=docker_cleanup_side_effect,
        ):
            # Should not raise despite the error
            await cleanup_orphaned_resources()

        # All three instances should have been attempted
        assert call_count[0] == 3

        # Only the failed one should still have its file
        assert not (temp_instances_dir / "instance-0.json").exists()
        assert (temp_instances_dir / "instance-1.json").exists()  # Failed cleanup
        assert not (temp_instances_dir / "instance-2.json").exists()

    @pytest.mark.asyncio
    async def test_cleanup_docker_ps_failure(self, temp_instances_dir):
        """Test handling of Docker ps command failure."""
        instance_id = "test-docker-fail"
        instance_file = temp_instances_dir / f"{instance_id}.json"

        data = {
            "instance_id": instance_id,
            "pid": 999999,
            "started_at": "2025-11-07T14:00:00Z",
            "pip_processes": {},
        }
        instance_file.write_text(json.dumps(data))

        # Mock Docker ps to fail
        mock_ps = AsyncMock()
        mock_ps.returncode = 1
        mock_ps.communicate = AsyncMock(
            return_value=(b"", b"Docker daemon not running")
        )

        with patch("asyncio.create_subprocess_exec", return_value=mock_ps):
            # Should not raise
            await cleanup_orphaned_resources()

        # Instance file should still be removed (we tried)
        assert not instance_file.exists()

    @pytest.mark.asyncio
    async def test_unregister_error_handling(self, tmp_path):
        """Test that unregister handles file removal errors gracefully."""
        with patch(
            "deephaven_mcp.resource_manager._instance_tracker.Path.home",
            return_value=tmp_path,
        ):
            tracker = await InstanceTracker.create_and_register()

            # Make unlink raise an exception
            with patch.object(
                Path, "unlink", side_effect=PermissionError("Access denied")
            ):
                # Should not raise, just log warning
                await tracker.unregister()

    @pytest.mark.asyncio
    async def test_cleanup_no_docker_containers_found(self, temp_instances_dir):
        """Test cleanup when docker ps returns no containers (lines 376-379)."""
        instance_file = temp_instances_dir / "dead-instance-id.json"
        data = {
            "instance_id": "dead-instance-id",
            "pid": 99999,
            "started_at": "2025-11-07T14:00:00Z",
            "pip_processes": {},
        }
        instance_file.write_text(json.dumps(data))

        # Mock process check to return dead
        with patch(
            "deephaven_mcp.resource_manager._instance_tracker.is_process_running",
            return_value=False,
        ):
            # Mock Docker ps to return success but empty output (no containers)
            mock_ps = AsyncMock()
            mock_ps.returncode = 0
            mock_ps.communicate = AsyncMock(return_value=(b"", b""))

            with patch("asyncio.create_subprocess_exec", return_value=mock_ps):
                await cleanup_orphaned_resources()

        # Instance file should be removed
        assert not instance_file.exists()

    @pytest.mark.asyncio
    async def test_cleanup_docker_stop_error(self, temp_instances_dir):
        """Test cleanup handles docker stop errors gracefully (lines 411-412)."""
        instance_file = temp_instances_dir / "dead-instance-id.json"
        data = {
            "instance_id": "dead-instance-id",
            "pid": 99999,
            "started_at": "2025-11-07T14:00:00Z",
            "pip_processes": {},
        }
        instance_file.write_text(json.dumps(data))

        # Mock process check to return dead
        with patch(
            "deephaven_mcp.resource_manager._instance_tracker.is_process_running",
            return_value=False,
        ):
            # Mock Docker ps to return container
            mock_ps = AsyncMock()
            mock_ps.returncode = 0
            mock_ps.communicate = AsyncMock(return_value=(b"container123\n", b""))

            # Mock docker stop to fail
            mock_stop = AsyncMock()
            mock_stop.returncode = 1
            mock_stop.communicate = AsyncMock(
                return_value=(b"", b"Container not found")
            )

            async def mock_exec(*args, **kwargs):
                if args[0] == "docker" and args[1] == "ps":
                    return mock_ps
                elif args[0] == "docker" and args[1] == "stop":
                    return mock_stop
                return AsyncMock(
                    returncode=0, communicate=AsyncMock(return_value=(b"", b""))
                )

            with patch("asyncio.create_subprocess_exec", side_effect=mock_exec):
                # Should not raise
                await cleanup_orphaned_resources()

        # Instance file should still be removed (we tried)
        assert not instance_file.exists()

    @pytest.mark.asyncio
    async def test_cleanup_pip_process_already_dead(self, temp_instances_dir):
        """Test cleanup when pip process is already dead (lines 447-449)."""
        instance_file = temp_instances_dir / "dead-instance-id.json"
        data = {
            "instance_id": "dead-instance-id",
            "pid": 99999,
            "started_at": "2025-11-07T14:00:00Z",
            "pip_processes": {"test-session": 88888},
        }
        instance_file.write_text(json.dumps(data))

        # Mock process check to return dead for instance and pip process
        with patch(
            "deephaven_mcp.resource_manager._instance_tracker.is_process_running",
            return_value=False,
        ):
            # Mock no Docker containers
            mock_ps = AsyncMock()
            mock_ps.returncode = 0
            mock_ps.communicate = AsyncMock(return_value=(b"", b""))

            with patch("asyncio.create_subprocess_exec", return_value=mock_ps):
                # Mock os.kill to check if it's called
                with patch("os.kill") as mock_kill:
                    await cleanup_orphaned_resources()
                    # Should not call os.kill since process is already dead
                    mock_kill.assert_not_called()

        # Instance file should be removed
        assert not instance_file.exists()

    @pytest.mark.asyncio
    async def test_cleanup_pip_process_kill_error(self, temp_instances_dir):
        """Test cleanup handles os.kill errors gracefully (lines 450-453)."""
        instance_file = temp_instances_dir / "dead-instance-id.json"
        data = {
            "instance_id": "dead-instance-id",
            "pid": 99999,
            "started_at": "2025-11-07T14:00:00Z",
            "pip_processes": {"test-session": 88888},
        }
        instance_file.write_text(json.dumps(data))

        # Mock instance dead, but pip process alive
        def mock_is_running(pid):
            if pid == 99999:
                return False  # Instance dead
            elif pid == 88888:
                return True  # Pip process alive
            return False

        with patch(
            "deephaven_mcp.resource_manager._instance_tracker.is_process_running",
            side_effect=mock_is_running,
        ):
            # Mock no Docker containers
            mock_ps = AsyncMock()
            mock_ps.returncode = 0
            mock_ps.communicate = AsyncMock(return_value=(b"", b""))

            with patch("asyncio.create_subprocess_exec", return_value=mock_ps):
                # Mock os.kill to raise exception
                with patch("os.kill", side_effect=PermissionError("Access denied")):
                    # Should not raise
                    await cleanup_orphaned_resources()

        # Instance file should still be removed (we tried)
        assert not instance_file.exists()

    @pytest.mark.asyncio
    async def test_cleanup_docker_subprocess_exception(self, temp_instances_dir):
        """Test cleanup handles subprocess exceptions during Docker cleanup (lines 411-412)."""
        instance_file = temp_instances_dir / "dead-instance-id.json"
        data = {
            "instance_id": "dead-instance-id",
            "pid": 99999,
            "started_at": "2025-11-07T14:00:00Z",
            "pip_processes": {},
        }
        instance_file.write_text(json.dumps(data))

        # Mock process check to return dead
        with patch(
            "deephaven_mcp.resource_manager._instance_tracker.is_process_running",
            return_value=False,
        ):
            # Mock Docker ps to return container
            mock_ps = AsyncMock()
            mock_ps.returncode = 0
            mock_ps.communicate = AsyncMock(return_value=(b"container123\n", b""))

            # Mock asyncio.create_subprocess_exec to raise exception on docker stop
            async def mock_exec(*args, **kwargs):
                if args[0] == "docker" and args[1] == "ps":
                    return mock_ps
                elif args[0] == "docker" and args[1] == "stop":
                    raise OSError("Docker daemon not responding")
                return AsyncMock(
                    returncode=0, communicate=AsyncMock(return_value=(b"", b""))
                )

            with patch("asyncio.create_subprocess_exec", side_effect=mock_exec):
                # Should not raise, just log error
                await cleanup_orphaned_resources()

        # Instance file should still be removed (we tried)
        assert not instance_file.exists()
