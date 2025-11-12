"""Session launcher for dynamically creating Deephaven Community sessions.

This module provides session classes for starting Deephaven Community sessions via:
- **Docker containers** (DockerLaunchedSession) - Launches Deephaven in isolated containers
- **Python launch method** (PythonLaunchedSession) - Launches Deephaven as local processes using pip-installed deephaven-server

Design Pattern:
- Sessions own their complete lifecycle (launch + stop)
- Abstract base class (LaunchedSession) defines the session interface
- Concrete subclasses implement launch() as classmethod factory and stop() as instance method
- Idempotent stop() methods allow safe multiple calls without side effects

Key Features:
- **Runtime validation** of session parameters (ports, auth, resources)
- **Process lifecycle management** with graceful shutdown and forced termination fallback
- **Health checking** via wait_until_ready() with configurable timeouts and retries
- **Graceful cleanup** with proper resource release
- **Authentication support** via JVM system properties (PSK or anonymous)

Typical Usage:
    # Launch a Docker session
    session = await DockerLaunchedSession.launch(
        session_name="my-session",
        port=10000,
        auth_token="secret",
        heap_size_gb=4,
        extra_jvm_args=[],
        environment_vars={},
        docker_image="ghcr.io/deephaven/server:latest",
        docker_memory_limit_gb=8.0,
        docker_cpu_limit=None,
        docker_volumes=[],  # Empty list for no volumes, or ["host:container:ro"]
    )

    # Wait for it to be ready
    if await session.wait_until_ready():
        print(f"Session ready at {session.connection_url}")

    # Use the session...

    # Clean up
    await session.stop()

Or use the convenience function:
    session = await launch_session(
        launch_method="docker",
        session_name="my-session",
        port=10000,
        auth_token="secret",
        heap_size_gb=4,
        extra_jvm_args=[],
        environment_vars={},
        docker_image="ghcr.io/deephaven/server:latest",
    )
"""

import asyncio
import logging
import os
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal

import aiohttp

from deephaven_mcp._exceptions import SessionLaunchError

_LOGGER = logging.getLogger(__name__)


def _redact_auth_token_from_command(cmd: list[str], auth_token: str | None) -> str:
    """Redact authentication token from command list for safe logging.
    
    Args:
        cmd: Command as list of arguments.
        auth_token: PSK authentication token to redact, or None.
        
    Returns:
        str: Command string with auth token replaced by [REDACTED] if present.
    """
    cmd_str = " ".join(cmd)
    if auth_token:
        cmd_str = cmd_str.replace(auth_token, "[REDACTED]")
    return cmd_str


def _build_jvm_args(
    heap_size_gb: int,
    extra_jvm_args: list[str],
    auth_token: str | None,
) -> list[str]:
    """Build JVM arguments with authentication configuration.
    
    This is a shared helper used by both Docker and Python launch methods to ensure
    consistent JVM configuration across launch methods.
    
    Args:
        heap_size_gb: JVM heap size in gigabytes (e.g., 4 for -Xmx4g).
        extra_jvm_args: Additional JVM arguments to append.
        auth_token: PSK authentication token, or None for anonymous auth.
        
    Returns:
        list[str]: Complete list of JVM arguments including heap size, extra args, and auth config.
    """
    jvm_args = [f"-Xmx{heap_size_gb}g"]
    jvm_args.extend(extra_jvm_args)

    if auth_token:
        jvm_args.append(f"-Dauthentication.psk={auth_token}")
    else:
        jvm_args.append(
            "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"
        )

    return jvm_args


def _find_deephaven_executable() -> str:
    """
    Find the deephaven executable in the current Python venv (private helper).

    This ensures we use the deephaven version from the same venv as the MCP server,
    avoiding version mismatch issues and segfaults.

    Returns:
        str: Path to deephaven executable (absolute path from venv, or "deephaven" for PATH fallback).

    Note:
        This is a private helper function. Use PythonLaunchedSession.launch() for public API.
    """
    # Check in the same venv as the current Python
    python_executable = Path(sys.executable)
    deephaven_executable = python_executable.parent / "deephaven"

    if deephaven_executable.exists():
        return str(deephaven_executable)

    # Fall back to PATH
    _LOGGER.warning(
        f"deephaven not found in venv at {deephaven_executable}, falling back to PATH"
    )
    return "deephaven"


class LaunchedSession(ABC):
    """
    Base class for a launched Deephaven session.

    This abstract class defines the interface for sessions that have been launched
    and are managing their own lifecycle. Subclasses implement launch() as a class
    method factory and stop() for cleanup.

    Attributes:
        launch_method (Literal["docker", "python"]): How the session was launched.
        host (str): The host the session is listening on (typically "localhost").
        port (int): The port the session is listening on.
        auth_type (Literal["anonymous", "psk"]): Authentication type.
        auth_token (str | None): Authentication token for PSK auth, or None for anonymous.
    """

    def __init__(
        self,
        launch_method: Literal["docker", "python"],
        host: str,
        port: int,
        auth_type: Literal["anonymous", "psk"],
        auth_token: str | None,
    ):
        """Initialize a LaunchedSession instance.

        This constructor performs runtime validation of all parameters to ensure
        consistency between authentication settings and validates literal types that
        are only checked statically by type checkers.

        Args:
            launch_method (Literal["docker", "python"]): How the session was launched.
                Must be exactly "docker" or "python" (runtime validated).
            host (str): The host the session is listening on (typically "localhost").
            port (int): The port the session is listening on.
            auth_type (Literal["anonymous", "psk"]): Authentication type.
                Must be exactly "anonymous" or "psk" (runtime validated).
            auth_token (str | None): Authentication token for PSK auth, or None for anonymous.
                Required when auth_type="psk", must be None when auth_type="anonymous".

        Raises:
            ValueError: If parameters have invalid values or are inconsistent:
                - launch_method not in ("docker", "python")
                - auth_type not in ("anonymous", "psk")
                - auth_type="psk" but auth_token is None/empty
                - auth_type="anonymous" but auth_token is provided
        """
        # Validate launch_method (runtime check, Literal is only static)
        if launch_method not in ("docker", "python"):
            raise ValueError(
                f"launch_method must be 'docker' or 'python', got '{launch_method}'"
            )

        # Validate auth_type (runtime check, Literal is only static)
        if auth_type not in ("anonymous", "psk"):
            raise ValueError(
                f"auth_type must be 'anonymous' or 'psk', got '{auth_type}'"
            )

        # Validate consistency between auth_type and auth_token
        if auth_type == "psk" and not auth_token:
            raise ValueError("auth_token is required when auth_type is 'psk'")

        if auth_type == "anonymous" and auth_token:
            raise ValueError(
                "auth_token should not be provided when auth_type is 'anonymous'"
            )

        self.launch_method = launch_method
        self.host = host
        self.port = port
        self.auth_type = auth_type
        self.auth_token = auth_token

    @property
    def connection_url(self) -> str:
        """Get the base connection URL for this session without authentication.

        This URL can be used for anonymous connections or when authentication will
        be provided through other means (e.g., separate headers or tokens).

        Returns:
            str: The HTTP URL (e.g., "http://localhost:10000") that can be used to
                connect to the Deephaven server. This URL does not include authentication
                parameters - use connection_url_with_auth for URLs with PSK tokens included.

        Example:
            >>> session.connection_url
            'http://localhost:10000'
        """
        return f"http://{self.host}:{self.port}"

    @property
    def connection_url_with_auth(self) -> str:
        """Get the connection URL with authentication token included (if applicable).

        For PSK authentication, this appends the auth token as a query parameter.
        For anonymous authentication, this returns the base URL without modifications.

        Returns:
            str: For PSK auth: URL with ?authToken=<token> appended (e.g.,
                "http://localhost:10000/?authToken=abc123").
                For anonymous auth: Base URL without auth parameters (e.g.,
                "http://localhost:10000").

        Note:
            For PSK auth, auth_token is guaranteed to be present due to __init__ validation,
            so this property will never return a malformed URL.

        Example:
            >>> # PSK authentication
            >>> session.auth_type
            'psk'
            >>> session.connection_url_with_auth
            'http://localhost:10000/?authToken=secret123'
            >>>
            >>> # Anonymous authentication
            >>> session.auth_type
            'anonymous'
            >>> session.connection_url_with_auth
            'http://localhost:10000'
        """
        if self.auth_type == "psk":
            # auth_token is guaranteed to exist for PSK (validated in __init__)
            return f"{self.connection_url}/?authToken={self.auth_token}"
        return self.connection_url

    @abstractmethod
    async def stop(self) -> None:
        """
        Stop this session and clean up all associated resources.

        Concrete implementations must be idempotent - calling this method multiple
        times should be safe and subsequent calls after the first should be no-ops.

        Raises:
            SessionLaunchError: If stop fails due to errors terminating the underlying
                process/container or cleaning up resources.
        """
        pass  # pragma: no cover

    async def wait_until_ready(
        self,
        timeout_seconds: float = 60,
        check_interval_seconds: float = 2,
        max_retries: int = 3,
    ) -> bool:
        """
        Wait for this session to become ready by polling its HTTP health endpoint.

        A session is considered "ready" when its HTTP server responds with any of the
        following status codes:
        - 200 (OK) - Server is fully operational
        - 404 (Not Found) - Server is running but endpoint not found (still means server is up)
        - 401 (Unauthorized) - Server is running but requires authentication
        - 403 (Forbidden) - Server is running but access is forbidden

        These status codes all indicate the server is running and accepting connections,
        even if authentication or specific routing hasn't been fully configured yet.

        This method implements a polling strategy with retries:
        1. Makes up to max_retries connection attempts per check interval
        2. Waits 0.5 seconds between retry attempts (within a check interval)
        3. Waits check_interval_seconds between check intervals
        4. Continues until session is ready or timeout_seconds is reached

        Args:
            timeout_seconds (float): Maximum time in seconds to wait for session to be ready.
                Default: 60 seconds. If the timeout is reached, returns False.
            check_interval_seconds (float): Time in seconds between health check attempts.
                Default: 2 seconds. Actual wait may be shorter if approaching timeout.
            max_retries (int): Number of connection attempts per check interval before waiting
                for the next interval. Default: 3 attempts. Each failed attempt waits 0.5s
                before retry.

        Returns:
            bool: True if session became ready within the timeout period, False if the
                timeout was reached without the session becoming ready.

        Raises:
            SessionLaunchError: If an unexpected error occurs during health checking
                (not connection errors, which are retried, but unexpected exceptions like
                programming errors or system failures).

        Example:
            >>> session = await DockerLaunchedSession.launch(...)
            >>> # Wait up to 60 seconds with default settings
            >>> if await session.wait_until_ready():
            ...     print(f"Session ready at {session.connection_url}")
            ... else:
            ...     print("Session failed to start within timeout")
            ...     await session.stop()  # Clean up failed session
        """
        _LOGGER.info(
            f"[_launcher:LaunchedSession] Waiting for session on port {self.port} "
            f"(timeout: {timeout_seconds}s, interval: {check_interval_seconds}s, retries: {max_retries})"
        )

        start_time = asyncio.get_event_loop().time()
        check_count = 0

        while True:
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout_seconds:
                _LOGGER.warning(
                    f"[_launcher:LaunchedSession] Timeout after {elapsed:.1f}s "
                    f"({check_count} checks)"
                )
                return False

            check_count += 1
            _LOGGER.debug(
                f"[_launcher:LaunchedSession] Health check #{check_count} "
                f"(elapsed: {elapsed:.1f}s)"
            )

            # For python sessions, check if process has crashed
            if hasattr(self, "process") and self.process.returncode is not None:
                _LOGGER.error(
                    f"[_launcher:LaunchedSession] Process terminated during health check "
                    f"with exit code {self.process.returncode}"
                )
                return False

            # Try to connect with retries
            for attempt in range(max_retries):
                try:
                    async with aiohttp.ClientSession() as client:
                        # Try to connect to the Deephaven server
                        # Use a simple GET to the root path - Deephaven should respond
                        async with client.get(
                            self.connection_url,
                            timeout=aiohttp.ClientTimeout(total=5),
                        ) as response:
                            # Any response (even 404) means the server is up
                            if response.status in (200, 404, 401, 403):
                                _LOGGER.info(
                                    f"[_launcher:LaunchedSession] Session ready on port {self.port} "
                                    f"after {elapsed:.1f}s ({check_count} checks, attempt {attempt + 1})"
                                )
                                return True
                            else:
                                _LOGGER.debug(
                                    f"[_launcher:LaunchedSession] Unexpected status {response.status}, "
                                    f"attempt {attempt + 1}/{max_retries}"
                                )

                except (TimeoutError, aiohttp.ClientError) as e:
                    _LOGGER.debug(
                        f"[_launcher:LaunchedSession] Connection failed (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    if attempt < max_retries - 1:
                        # Brief backoff before retry
                        await asyncio.sleep(0.5)
                    continue

                except Exception as e:
                    _LOGGER.error(
                        f"[_launcher:LaunchedSession] Unexpected error during health check: {e}"
                    )
                    raise SessionLaunchError(f"Health check failed: {e}") from e

            # Wait before next check interval
            remaining_time = timeout_seconds - (
                asyncio.get_event_loop().time() - start_time
            )
            if remaining_time > 0:
                wait_time = min(check_interval_seconds, remaining_time)
                await asyncio.sleep(wait_time)


class DockerLaunchedSession(LaunchedSession):
    """A Deephaven session launched via Docker.

    This class extends LaunchedSession to manage Deephaven sessions running in Docker
    containers. It handles container lifecycle (launch and stop) and provides
    Docker-specific attributes.

    Attributes:
        launch_method (Literal["docker"]): Always "docker" for this class.
        host (str): The host the session is listening on (inherited from LaunchedSession).
        port (int): The port the session is listening on (inherited from LaunchedSession).
        auth_type (Literal["anonymous", "psk"]): Authentication type (inherited from LaunchedSession).
        auth_token (str | None): Authentication token for PSK auth (inherited from LaunchedSession).
        container_id (str): Docker container ID for this session.
        _stopped (bool): Internal flag tracking whether stop() has been called (for idempotency).
    """

    def __init__(
        self,
        host: str,
        port: int,
        auth_type: Literal["anonymous", "psk"],
        auth_token: str | None,
        container_id: str,
    ):
        """Initialize a DockerLaunchedSession.

        Args:
            host (str): The host the session is listening on.
            port (int): The port the session is listening on.
            auth_type (Literal["anonymous", "psk"]): Authentication type.
            auth_token (str | None): Authentication token for PSK auth, or None for anonymous.
            container_id (str): Docker container ID (must be non-empty).

        Raises:
            ValueError: If container_id is None or empty string.
            ValueError: If auth_type/auth_token are inconsistent (inherited from LaunchedSession).
        """
        super().__init__("docker", host, port, auth_type, auth_token)

        # Validate container_id
        if not container_id:
            raise ValueError("container_id must be a non-empty string")

        self.container_id = container_id
        self._stopped = False  # Track if stop() has been called for idempotency

    @classmethod
    async def launch(
        cls,
        session_name: str,
        port: int,
        auth_token: str | None,
        heap_size_gb: int,
        extra_jvm_args: list[str],
        environment_vars: dict[str, str],
        docker_image: str,
        docker_memory_limit_gb: float | None,
        docker_cpu_limit: float | None,
        docker_volumes: list[str],
        instance_id: str | None = None,
    ) -> "DockerLaunchedSession":
        """
        Launch a Deephaven session via Docker.

        This method starts a Deephaven server in a Docker container with the specified
        configuration. The container uses port mapping to expose the Deephaven server.

        Requirements:
            - Docker must be installed and the Docker daemon must be running
            - The specified docker_image must be available (pulled or built locally)
            - The specified port must be available on the host

        Args:
            session_name (str): Name for the Docker container (will be prefixed with "deephaven-mcp-").
            port (int): Port to bind the session to.
            auth_token (str | None): Authentication token (PSK) for the session, or None for anonymous.
            heap_size_gb (int): JVM heap size in gigabytes (integer only, e.g., 2 for -Xmx2g).
            extra_jvm_args (list[str]): Additional JVM arguments (empty list for none).
            environment_vars (dict[str, str]): Environment variables to set (empty dict for none).
            docker_image (str): Docker image to use (e.g., "ghcr.io/deephaven/server:latest").
            docker_memory_limit_gb (float | None): Container memory limit in GB, or None for no limit.
            docker_cpu_limit (float | None): Container CPU limit in cores, or None for no limit.
            docker_volumes (list[str]): Volume mounts in format ["host:container:mode"] (empty list for none).
            instance_id (str | None): MCP server instance ID for tracking orphaned containers.
                If provided, the container will be labeled for cleanup on server crash/SIGKILL.

        Returns:
            DockerLaunchedSession: The launched Docker session.

        Raises:
            SessionLaunchError: If launch fails (e.g., Docker not available, image not found,
                port already in use, or container fails to start).
        """
        _LOGGER.info(
            f"[_launcher:DockerLaunchedSession] Launching Docker session '{session_name}' on port {port}"
        )

        # Build JVM arguments with authentication
        jvm_args = _build_jvm_args(heap_size_gb, extra_jvm_args, auth_token)

        # Prepare environment variables
        env_vars = environment_vars.copy()
        env_vars["START_OPTS"] = " ".join(jvm_args)

        # Build docker command with all parameters including environment variables
        cmd = cls._build_docker_command(
            session_name,
            port,
            instance_id,
            docker_memory_limit_gb,
            docker_cpu_limit,
            docker_volumes,
            env_vars,
            docker_image,
        )

        # Log command with PSK redacted for security
        _LOGGER.debug(
            f"[_launcher:DockerLaunchedSession] Docker command: {_redact_auth_token_from_command(cmd, auth_token)}"
        )

        container_id = await cls._launch_container(cmd)

        _LOGGER.info(
            f"[_launcher:DockerLaunchedSession] Successfully launched container {container_id[:12]}"
        )

        return cls(
            host="localhost",
            port=port,
            auth_type="psk" if auth_token else "anonymous",
            auth_token=auth_token,
            container_id=container_id,
        )

    @classmethod
    def _build_docker_command(
        cls,
        session_name: str,
        port: int,
        instance_id: str | None,
        docker_memory_limit_gb: float | None,
        docker_cpu_limit: float | None,
        docker_volumes: list[str],
        environment_vars: dict[str, str],
        docker_image: str,
    ) -> list[str]:
        """Build the Docker command with resource limits, volumes, and environment variables.
        
        Args:
            session_name: Name for the Docker container (will be prefixed with "deephaven-mcp-").
            port: Host port to map to container's port 10000.
            instance_id: MCP server instance ID for labeling containers (for orphan cleanup), or None.
            docker_memory_limit_gb: Container memory limit in GB, or None for no limit.
            docker_cpu_limit: Container CPU limit in cores, or None for no limit.
            docker_volumes: Volume mounts in format ["host:container:mode"].
            environment_vars: Environment variables to set in the container.
            docker_image: Docker image to use.
            
        Returns:
            list[str]: Complete docker run command as list of arguments.
        """
        cmd = [
            "docker",
            "run",
            "--rm",  # Remove container when stopped
            "--detach",  # Run in background
            "--name",
            f"deephaven-mcp-{session_name}",
            "-p",
            f"{port}:10000",  # Map host port to container's port 10000
        ]

        # Add instance tracking label for orphan cleanup
        if instance_id:
            cmd.extend(["--label", f"deephaven-mcp-server-instance={instance_id}"])
            _LOGGER.debug(
                f"[_launcher:DockerLaunchedSession] Labeling container with instance ID: {instance_id}"
            )

        # Add resource limits if specified
        if docker_memory_limit_gb is not None:
            memory_bytes = int(docker_memory_limit_gb * 1024 * 1024 * 1024)
            cmd.extend(["--memory", f"{memory_bytes}"])
            _LOGGER.debug(
                f"[_launcher:DockerLaunchedSession] Setting memory limit: {docker_memory_limit_gb}GB"
            )

        if docker_cpu_limit is not None:
            cmd.extend(["--cpus", str(docker_cpu_limit)])
            _LOGGER.debug(
                f"[_launcher:DockerLaunchedSession] Setting CPU limit: {docker_cpu_limit} cores"
            )

        # Add volume mounts
        if docker_volumes:
            for volume in docker_volumes:
                cmd.extend(["-v", volume])
                _LOGGER.debug(
                    f"[_launcher:DockerLaunchedSession] Adding volume mount: {volume}"
                )

        # Add environment variables
        for key, value in environment_vars.items():
            cmd.extend(["-e", f"{key}={value}"])

        cmd.append(docker_image)

        return cmd

    @classmethod
    async def _launch_container(cls, cmd: list[str]) -> str:
        """Launch the Docker container and handle errors.
        
        Args:
            cmd: Complete docker run command as list of arguments.
            
        Returns:
            str: Container ID of the launched container.
            
        Raises:
            SessionLaunchError: If docker command fails or returns empty container ID.
        """
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                raise SessionLaunchError(
                    f"Docker launch failed with return code {process.returncode}: {error_msg}"
                )

            container_id = stdout.decode().strip()
            if not container_id:
                error_msg = stderr.decode() if stderr else "No error output"
                raise SessionLaunchError(
                    f"Docker launch succeeded but returned empty container ID. Error: {error_msg}"
                )

            return container_id

        except Exception as e:
            raise SessionLaunchError(f"Failed to launch Docker container: {e}") from e

    async def stop(self) -> None:
        """Stop this Docker container.

        This method is idempotent - calling it multiple times is safe.
        Subsequent calls after the first will be no-ops.

        Raises:
            SessionLaunchError: If stop fails.
        """
        # Idempotent: if already stopped, do nothing
        if self._stopped:
            _LOGGER.debug(
                f"[_launcher:DockerLaunchedSession] Container {self.container_id[:12]} already stopped, skipping"
            )
            return

        _LOGGER.info(
            f"[_launcher:DockerLaunchedSession] Stopping container {self.container_id[:12]}"
        )

        try:
            process = await asyncio.create_subprocess_exec(
                "docker",
                "stop",
                self.container_id,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                _LOGGER.warning(
                    f"[_launcher:DockerLaunchedSession] Docker stop failed: {error_msg}"
                )
                # Try force kill
                _LOGGER.info(
                    f"[_launcher:DockerLaunchedSession] Attempting force kill of container {self.container_id[:12]}"
                )
                kill_process = await asyncio.create_subprocess_exec(
                    "docker",
                    "kill",
                    self.container_id,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await kill_process.communicate()

            _LOGGER.info(
                f"[_launcher:DockerLaunchedSession] Successfully stopped container {self.container_id[:12]}"
            )

            # Mark as stopped for idempotency
            self._stopped = True

        except Exception as e:
            raise SessionLaunchError(f"Failed to stop Docker container: {e}") from e


class PythonLaunchedSession(LaunchedSession):
    """A Deephaven session launched using the python launch method.

    This class extends LaunchedSession to manage Deephaven sessions running as local
    processes via the `deephaven server` command from a pip-installed deephaven-server
    package. It handles process lifecycle (launch and stop) and provides process-specific
    attributes.

    Attributes:
        launch_method (Literal["python"]): Always "python" for this class.
        host (str): The host the session is listening on (inherited from LaunchedSession).
        port (int): The port the session is listening on (inherited from LaunchedSession).
        auth_type (Literal["anonymous", "psk"]): Authentication type (inherited from LaunchedSession).
        auth_token (str | None): Authentication token for PSK auth (inherited from LaunchedSession).
        process (asyncio.subprocess.Process): The subprocess running the Deephaven server.
        _stopped (bool): Internal flag tracking whether stop() has been called (for idempotency).
    """

    def __init__(
        self,
        host: str,
        port: int,
        auth_type: Literal["anonymous", "psk"],
        auth_token: str | None,
        process: asyncio.subprocess.Process,
    ):
        """Initialize a PythonLaunchedSession.

        Args:
            host (str): The host the session is listening on.
            port (int): The port the session is listening on.
            auth_type (Literal["anonymous", "psk"]): Authentication type.
            auth_token (str | None): Authentication token for PSK auth, or None for anonymous.
            process (asyncio.subprocess.Process): The subprocess running the Deephaven server (must not be None).

        Raises:
            ValueError: If process is None.
            ValueError: If auth_type/auth_token are inconsistent (inherited from LaunchedSession).
        """
        super().__init__("python", host, port, auth_type, auth_token)

        # Validate process
        if process is None:
            raise ValueError("process must not be None")

        self.process = process
        self._stopped = False  # Track if stop() has been called

    @classmethod
    async def launch(
        cls,
        session_name: str,
        port: int,
        auth_token: str | None,
        heap_size_gb: int,
        extra_jvm_args: list[str],
        environment_vars: dict[str, str],
    ) -> "PythonLaunchedSession":
        """
        Launch a Deephaven session using the python launch method.

        This method starts a Deephaven server using the `deephaven server` command from
        a pip-installed deephaven-server package. The executable must be available in the
        current environment.

        Requirements:
            - The `deephaven-server` package must be installed (e.g., `pip install deephaven-server`)
            - The `deephaven` executable must be available (checked in venv first, then PATH)
            - The specified port must be available

        Args:
            session_name (str): Name for the session (used in logging).
            port (int): Port to bind the session to.
            auth_token (str | None): Authentication token (PSK) for the session, or None for anonymous.
            heap_size_gb (int): JVM heap size in gigabytes (integer only, e.g., 2 for -Xmx2g).
            extra_jvm_args (list[str]): Additional JVM arguments.
            environment_vars (dict[str, str]): Environment variables to set (empty dict for none).

        Returns:
            PythonLaunchedSession: The launched session.

        Raises:
            SessionLaunchError: If launch fails (e.g., deephaven command not found,
                port already in use, or server fails to start).
        """
        _LOGGER.info(
            f"[_launcher:PythonLaunchedSession] Launching python session '{session_name}' on port {port}"
        )

        # Build JVM arguments with authentication (using shared helper for consistency with Docker)
        jvm_args = _build_jvm_args(heap_size_gb, extra_jvm_args, auth_token)
        jvm_args_str = " ".join(jvm_args)
        
        # Log authentication configuration
        if auth_token:
            _LOGGER.debug(
                "[_launcher:PythonLaunchedSession] Configured PSK authentication"
            )
        else:
            _LOGGER.debug(
                "[_launcher:PythonLaunchedSession] Configured anonymous authentication"
            )

        # Find deephaven executable in the same venv as the current Python
        deephaven_cmd = _find_deephaven_executable()
        _LOGGER.debug(
            f"[_launcher:PythonLaunchedSession] Using deephaven executable: {deephaven_cmd}"
        )

        # Build command
        cmd = [
            deephaven_cmd,
            "server",
            "--port",
            str(port),
            "--no-browser",  # Never open browser for MCP sessions
            "--jvm-args",
            jvm_args_str,
        ]

        # Set up environment
        env = environment_vars.copy()
        if env:
            _LOGGER.debug(
                f"[_launcher:PythonLaunchedSession] Environment variables: {list(env.keys())}"
            )

        # Log command with PSK redacted for security
        _LOGGER.debug(
            f"[_launcher:PythonLaunchedSession] Command: {_redact_auth_token_from_command(cmd, auth_token)}"
        )

        # Launch process
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={**os.environ, **env},  # Merge with current environment
            )

            _LOGGER.info(
                f"[_launcher:PythonLaunchedSession] Successfully launched process PID {process.pid}"
            )

            return cls(
                host="localhost",
                port=port,
                auth_type="psk" if auth_token else "anonymous",
                auth_token=auth_token,
                process=process,
            )

        except Exception as e:
            raise SessionLaunchError(f"Failed to launch python session: {e}") from e

    async def stop(self) -> None:
        """Stop this python-launched session.

        This method is idempotent - calling it multiple times is safe.
        Subsequent calls after the first will be no-ops.

        Raises:
            SessionLaunchError: If stop fails.
        """
        # Idempotent: if already stopped, do nothing
        if self._stopped:
            _LOGGER.debug(
                f"[_launcher:PythonLaunchedSession] Process PID {self.process.pid} already stopped, skipping"
            )
            return

        _LOGGER.info(
            f"[_launcher:PythonLaunchedSession] Stopping process PID {self.process.pid}"
        )

        try:
            # Try graceful termination first
            self.process.terminate()

            try:
                # Wait up to 10 seconds for graceful shutdown
                await asyncio.wait_for(self.process.wait(), timeout=10.0)
                _LOGGER.info(
                    f"[_launcher:PythonLaunchedSession] Process PID {self.process.pid} terminated gracefully"
                )
            except TimeoutError:
                # Force kill if graceful shutdown times out
                _LOGGER.warning(
                    f"[_launcher:PythonLaunchedSession] Process PID {self.process.pid} did not terminate gracefully, forcing kill"
                )
                self.process.kill()
                await self.process.wait()
                _LOGGER.info(
                    f"[_launcher:PythonLaunchedSession] Process PID {self.process.pid} killed"
                )

            # Mark as stopped for idempotency
            self._stopped = True

        except Exception as e:
            raise SessionLaunchError(f"Failed to stop python session: {e}") from e


async def launch_session(
    launch_method: Literal["docker", "python"],
    session_name: str,
    port: int,
    auth_token: str | None,
    heap_size_gb: int,
    extra_jvm_args: list[str],
    environment_vars: dict[str, str],
    docker_image: str = "",
    docker_memory_limit_gb: float | None = None,
    docker_cpu_limit: float | None = None,
    docker_volumes: list[str] | None = None,
    instance_id: str | None = None,
) -> LaunchedSession:
    """
    Launch a Deephaven session using the specified method.

    This is a convenience function that delegates to the appropriate session class's
    launch() method based on the launch_method parameter.

    Args:
        launch_method (Literal["docker", "python"]): The launch method.
        session_name (str): Name for the session.
        port (int): Port to bind the session to.
        auth_token (str | None): Authentication token (PSK) for the session, or None for anonymous.
        heap_size_gb (int): JVM heap size in gigabytes (integer only, e.g., 2 for -Xmx2g).
        extra_jvm_args (list[str]): Additional JVM arguments.
        environment_vars (dict[str, str]): Environment variables to set.
        docker_image (str): Docker image to use (docker only).
        docker_memory_limit_gb (float | None): Container memory limit in GB (docker only).
        docker_cpu_limit (float | None): Container CPU limit in cores (docker only).
        docker_volumes (list[str] | None): Volume mounts (docker only), or None for no volumes.
        instance_id (str | None): MCP server instance ID for orphan tracking (docker only).

    Returns:
        LaunchedSession: The launched session (DockerLaunchedSession or PythonLaunchedSession).

    Raises:
        ValueError: If launch_method is not supported, or if Docker-specific parameters
            are provided when launch_method is "python".
        SessionLaunchError: If launch fails.
    """
    _LOGGER.debug(
        f"[_launcher:launch_session] Launching {launch_method} session '{session_name}' on port {port}"
    )

    # Handle mutable default arguments
    if docker_volumes is None:
        docker_volumes = []

    if launch_method == "docker":
        return await DockerLaunchedSession.launch(
            session_name=session_name,
            port=port,
            auth_token=auth_token,
            heap_size_gb=heap_size_gb,
            extra_jvm_args=extra_jvm_args,
            environment_vars=environment_vars,
            docker_image=docker_image,
            docker_memory_limit_gb=docker_memory_limit_gb,
            docker_cpu_limit=docker_cpu_limit,
            docker_volumes=docker_volumes,
            instance_id=instance_id,
        )
    elif launch_method == "python":
        # Validate that Docker-specific parameters aren't used with python
        if docker_image:
            raise ValueError(
                "docker_image parameter cannot be used with launch_method='python'"
            )
        if docker_memory_limit_gb is not None:
            raise ValueError(
                "docker_memory_limit_gb parameter cannot be used with launch_method='python'"
            )
        if docker_cpu_limit is not None:
            raise ValueError(
                "docker_cpu_limit parameter cannot be used with launch_method='python'"
            )
        if docker_volumes:
            raise ValueError(
                "docker_volumes parameter cannot be used with launch_method='python'"
            )

        return await PythonLaunchedSession.launch(
            session_name=session_name,
            port=port,
            auth_token=auth_token,
            heap_size_gb=heap_size_gb,
            extra_jvm_args=extra_jvm_args,
            environment_vars=environment_vars,
        )
    else:
        raise ValueError(f"Unsupported launch method: {launch_method}")
