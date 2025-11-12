"""
Instance tracking and orphaned resource cleanup for MCP server instances.

This module provides functionality to track running MCP server instances and clean up
orphaned Docker containers and python processes that may be left behind when a server
is terminated with SIGKILL or crashes unexpectedly.

Key Concepts:
    - Each MCP server instance is assigned a unique UUID on startup
    - Instance metadata (UUID, PID, start time, python processes) is persisted to disk
    - Docker containers are labeled with the instance UUID for identification
    - Python processes are tracked in the instance metadata file
    - On startup, dead instances are detected and their orphaned resources are cleaned up

Architecture:
    - Instance metadata stored in: ~/.deephaven-mcp/instances/{uuid}.json
    - Docker containers labeled with: deephaven-mcp-server-instance={uuid}
    - Python processes tracked in instance file: {"python_processes": {"session": pid}}

Usage:
    # On server startup
    instance = await InstanceTracker.create_and_register()
    await cleanup_orphaned_resources()

    # During operation
    await instance.track_python_process("my-session", 12345)
    await instance.untrack_python_process("my-session")

    # On server shutdown
    await instance.unregister()

Thread Safety:
    - All operations are coroutine-safe
    - File operations use atomic writes where possible
    - Multiple server instances can safely coexist
"""

import asyncio
import json
import logging
import os
import signal
import uuid
from datetime import datetime
from pathlib import Path

_LOGGER = logging.getLogger(__name__)


class InstanceTracker:
    """
    Tracks a single MCP server instance and its associated resources.

    This class manages the lifecycle of an MCP server instance, including:
    - Generating and persisting a unique instance identifier
    - Tracking python-launched session processes
    - Registering/unregistering the instance on startup/shutdown
    - Providing the instance ID for Docker container labeling

    Attributes:
        instance_id (str): Unique UUID for this server instance.
        pid (int): Process ID of this server instance.
        started_at (str): ISO 8601 timestamp of when this instance started.
        instance_file (Path): Path to the instance metadata file.
    """

    def __init__(self, instance_id: str, pid: int, started_at: str):
        """
        Initialize an InstanceTracker.

        This constructor should not be called directly. Use create_and_register()
        or load_from_file() factory methods instead.

        Args:
            instance_id (str): Unique UUID for this instance.
            pid (int): Process ID of this server instance.
            started_at (str): ISO 8601 timestamp of when the instance started.
        """
        self.instance_id = instance_id
        self.pid = pid
        self.started_at = started_at
        self._python_processes: dict[str, int] = {}

        # Ensure instances directory exists
        instances_dir = Path.home() / ".deephaven-mcp" / "instances"
        instances_dir.mkdir(parents=True, exist_ok=True)

        self.instance_file = instances_dir / f"{instance_id}.json"

    @classmethod
    async def create_and_register(cls) -> "InstanceTracker":
        """
        Create a new instance tracker and register it.

        This factory method creates a new instance with a unique UUID and immediately
        persists it to disk. Call this on MCP server startup.

        Returns:
            InstanceTracker: A new registered instance tracker.

        Example:
            ```python
            instance = await InstanceTracker.create_and_register()
            _LOGGER.info(f"Server instance: {instance.instance_id}")
            ```
        """
        instance_id = str(uuid.uuid4())
        pid = os.getpid()
        started_at = datetime.now().isoformat()

        tracker = cls(instance_id, pid, started_at)
        await tracker._save()

        _LOGGER.info(
            f"[InstanceTracker] Registered new instance {instance_id} (PID: {pid})"
        )

        return tracker

    @classmethod
    def load_from_file(cls, instance_file: Path) -> "InstanceTracker":
        """
        Load an existing instance tracker from a metadata file.

        This is used during orphan cleanup to load information about other
        (potentially dead) server instances.

        Args:
            instance_file (Path): Path to the instance metadata JSON file.

        Returns:
            InstanceTracker: Instance tracker loaded from the file.

        Raises:
            FileNotFoundError: If the instance file does not exist.
            json.JSONDecodeError: If the file contains invalid JSON.
            KeyError: If required fields (instance_id, pid, started_at) are missing from the JSON.
        """
        data = json.loads(instance_file.read_text())

        tracker = cls(
            instance_id=data["instance_id"],
            pid=data["pid"],
            started_at=data["started_at"],
        )
        tracker._python_processes = data.get("python_processes", {})

        return tracker

    async def track_python_process(self, session_name: str, pid: int) -> None:
        """
        Track a new python-launched session process.

        Adds the process to the instance metadata so it can be cleaned up
        if the server crashes or is killed.

        Args:
            session_name (str): Name of the session.
            pid (int): Process ID of the python-launched deephaven-server process.

        Example:
            ```python
            # After launching a python session
            await instance.track_python_process("my-session", 12345)
            ```
        """
        self._python_processes[session_name] = pid
        await self._save()

        _LOGGER.debug(
            f"[InstanceTracker] Tracking python process for session '{session_name}' (PID: {pid})"
        )

    async def untrack_python_process(self, session_name: str) -> None:
        """
        Stop tracking a python-launched session process.

        Removes the process from the instance metadata, typically called when
        the session is stopped normally.

        Args:
            session_name (str): Name of the session to stop tracking.

        Example:
            ```python
            # After stopping a python session
            await instance.untrack_python_process("my-session")
            ```
        """
        if session_name in self._python_processes:
            del self._python_processes[session_name]
            await self._save()

            _LOGGER.debug(
                f"[InstanceTracker] Stopped tracking python process for session '{session_name}'"
            )

    async def unregister(self) -> None:
        """
        Unregister this instance and remove its metadata file.

        Call this on normal server shutdown to clean up instance tracking.
        This prevents the cleanup logic from attempting to clean up resources
        for a server that shut down normally.

        Example:
            ```python
            # In app_lifespan finally block
            await instance.unregister()
            ```
        """
        try:
            self.instance_file.unlink(missing_ok=True)
            _LOGGER.info(f"[InstanceTracker] Unregistered instance {self.instance_id}")
        except Exception as e:
            _LOGGER.warning(
                f"[InstanceTracker] Error unregistering instance {self.instance_id}: {e}"
            )

    async def _save(self) -> None:
        """
        Save instance metadata to disk using atomic write.

        Persists the current state of the instance tracker, including tracked
        python processes, to the instance metadata file. Uses atomic write
        (temp file + rename) to ensure the file is never left in a corrupted
        state if the write is interrupted.
        """
        data = {
            "instance_id": self.instance_id,
            "pid": self.pid,
            "started_at": self.started_at,
            "python_processes": self._python_processes,
        }

        # Atomic write using temporary file + rename
        temp_file = self.instance_file.with_suffix(".tmp")
        temp_file.write_text(json.dumps(data, indent=2))
        temp_file.replace(self.instance_file)


def is_process_running(pid: int) -> bool:
    """
    Check if a process with the given PID is currently running.

    Uses os.kill(pid, 0) which sends signal 0 to check process existence
    without actually sending a signal. This is the standard Unix way to
    check if a process is alive.

    Args:
        pid (int): Process ID to check.

    Returns:
        bool: True if the process is running, False otherwise.

    Note:
        Returns False if the process doesn't exist OR if we don't have
        permission to check its status (e.g., process owned by another user).
    """
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


async def cleanup_orphaned_resources() -> None:
    """
    Clean up orphaned Docker containers and python processes from dead server instances.

    This function should be called on MCP server startup. It:
    1. Scans the instances directory for registered server instances
    2. Checks if each instance's process is still running
    3. For dead instances, cleans up their orphaned resources:
       - Stops and removes Docker containers (via instance label)
       - Kills python processes (from instance metadata)
    4. Removes instance metadata files for dead instances

    The cleanup is safe for concurrent server instances - only resources from
    dead servers are cleaned up. Running servers are left untouched.

    This handles the SIGKILL case where a server is forcibly terminated without
    a chance to clean up its resources in the finally block.

    Example:
        ```python
        # In app_lifespan, before yielding context
        await cleanup_orphaned_resources()
        ```

    Note:
        Errors during cleanup are logged but don't raise exceptions, ensuring
        that server startup continues even if cleanup partially fails.
    """
    instances_dir = Path.home() / ".deephaven-mcp" / "instances"

    if not instances_dir.exists():
        _LOGGER.debug(
            "[InstanceTracker] No instances directory, skipping orphan cleanup"
        )
        return

    instance_files = list(instances_dir.glob("*.json"))

    if not instance_files:
        _LOGGER.debug(
            "[InstanceTracker] No instance files found, skipping orphan cleanup"
        )
        return

    _LOGGER.info(
        f"[InstanceTracker] Checking {len(instance_files)} instance(s) for orphaned resources..."
    )

    for instance_file in instance_files:
        try:
            tracker = InstanceTracker.load_from_file(instance_file)

            # Check if this instance's process is still running
            if is_process_running(tracker.pid):
                _LOGGER.debug(
                    f"[InstanceTracker] Instance {tracker.instance_id} still running (PID {tracker.pid}), skipping"
                )
                continue

            # Process is dead - clean up its orphaned resources
            _LOGGER.warning(
                f"[InstanceTracker] Found dead instance {tracker.instance_id} (PID {tracker.pid}), cleaning up orphans..."
            )

            # Clean up Docker containers
            await _cleanup_docker_containers_for_instance(tracker.instance_id)

            # Clean up python processes
            await _cleanup_python_processes_for_instance(tracker)

            # Remove instance metadata file
            instance_file.unlink(missing_ok=True)
            _LOGGER.info(
                f"[InstanceTracker] Cleaned up orphaned resources for instance {tracker.instance_id}"
            )

        except Exception as e:
            _LOGGER.error(
                f"[InstanceTracker] Error cleaning up instance {instance_file.name}: {e}",
                exc_info=True,
            )


async def _cleanup_docker_containers_for_instance(instance_id: str) -> None:
    """
    Clean up Docker containers for a specific server instance.

    Finds all Docker containers labeled with the instance ID and stops/removes them.
    Uses the 'deephaven-mcp-server-instance' label to identify containers belonging
    to the dead instance. Attempts graceful stop via 'docker stop' before removing
    with 'docker rm'.

    Args:
        instance_id (str): The instance UUID to clean up containers for.

    Note:
        Errors during cleanup are logged but do not raise exceptions. This ensures
        that failure to clean up one container doesn't prevent cleanup of others
        or block server startup.
    """
    try:
        # Find containers with this instance ID label
        process = await asyncio.create_subprocess_exec(
            "docker",
            "ps",
            "-a",
            "--filter",
            f"label=deephaven-mcp-server-instance={instance_id}",
            "--format",
            "{{.ID}}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            _LOGGER.warning(
                f"[InstanceTracker] Docker ps command failed: {stderr.decode()}"
            )
            return

        container_ids = [
            cid.strip() for cid in stdout.decode().strip().split("\n") if cid.strip()
        ]

        if not container_ids:
            _LOGGER.debug(
                f"[InstanceTracker] No Docker containers found for instance {instance_id}"
            )
            return

        _LOGGER.info(
            f"[InstanceTracker] Found {len(container_ids)} orphaned container(s) for instance {instance_id}"
        )

        # Stop and remove each container
        for container_id in container_ids:
            _LOGGER.info(
                f"[InstanceTracker] Stopping orphaned container {container_id[:12]}..."
            )

            # Try to stop gracefully
            stop_process = await asyncio.create_subprocess_exec(
                "docker",
                "stop",
                container_id,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await stop_process.communicate()

            # Remove the container
            rm_process = await asyncio.create_subprocess_exec(
                "docker",
                "rm",
                container_id,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await rm_process.communicate()

            _LOGGER.info(
                f"[InstanceTracker] Cleaned up orphaned container {container_id[:12]}"
            )

    except Exception as e:
        _LOGGER.error(
            f"[InstanceTracker] Error cleaning up Docker containers for instance {instance_id}: {e}",
            exc_info=True,
        )


async def _cleanup_python_processes_for_instance(tracker: InstanceTracker) -> None:
    """
    Clean up python processes for a specific server instance.

    Terminates all python processes tracked in the instance metadata. Sends SIGTERM
    to each tracked process after verifying it's still running. Processes that
    are already dead are logged and skipped.

    Args:
        tracker (InstanceTracker): The instance tracker with python process information.

    Note:
        Errors during process termination (e.g., permission denied, process already
        exited) are logged as warnings but do not raise exceptions. This ensures
        that cleanup continues for all processes even if some fail.
    """
    python_processes = tracker._python_processes

    if not python_processes:
        _LOGGER.debug(
            f"[InstanceTracker] No python processes found for instance {tracker.instance_id}"
        )
        return

    _LOGGER.info(
        f"[InstanceTracker] Found {len(python_processes)} orphaned python process(es) for instance {tracker.instance_id}"
    )

    for session_name, pid in python_processes.items():
        try:
            if is_process_running(pid):
                _LOGGER.info(
                    f"[InstanceTracker] Killing orphaned python process {pid} (session: {session_name})"
                )
                os.kill(pid, signal.SIGTERM)
            else:
                _LOGGER.debug(
                    f"[InstanceTracker] Python process {pid} (session: {session_name}) already dead"
                )
        except Exception as e:
            _LOGGER.warning(
                f"[InstanceTracker] Error killing python process {pid} (session: {session_name}): {e}"
            )
