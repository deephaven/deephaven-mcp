"""
Unit tests for community session launchers.

Tests the launcher infrastructure including session lifecycle management,
health checks, and session launching via Docker and pip.

Consolidated from multiple test files following project standard of
one source file = one test file.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from deephaven_mcp._exceptions import SessionLaunchError
from deephaven_mcp.resource_manager import (
    DockerLaunchedSession,
    LaunchedSession,
    PythonLaunchedSession,
    launch_session,
)

# ============================================================================
# LaunchedSession Base Class Validation Tests
# ============================================================================


class TestLaunchedSessionValidation:
    """Tests for LaunchedSession base class validation logic."""

    def test_init_validates_invalid_launch_method(self):
        """Test that invalid launch_method raises ValueError (covers line 109)."""
        # We need to bypass the type system to test runtime validation
        # Call LaunchedSession.__init__ directly with an invalid launch_method
        session = object.__new__(DockerLaunchedSession)
        with pytest.raises(
            ValueError, match="launch_method must be 'docker' or 'python'"
        ):
            LaunchedSession.__init__(
                session,
                launch_method="invalid",  # type: ignore
                host="localhost",
                port=10000,
                auth_type="anonymous",
                auth_token=None,
            )

    def test_init_validates_invalid_auth_type(self):
        """Test that invalid auth_type raises ValueError."""
        with pytest.raises(ValueError, match="auth_type must be 'anonymous' or 'psk'"):
            DockerLaunchedSession(
                host="localhost",
                port=10000,
                auth_type="invalid",  # type: ignore
                auth_token=None,
                container_id="test",
            )

    def test_init_validates_psk_requires_token(self):
        """Test that PSK auth requires auth_token."""
        with pytest.raises(
            ValueError, match="auth_token is required when auth_type is 'psk'"
        ):
            DockerLaunchedSession(
                host="localhost",
                port=10000,
                auth_type="psk",
                auth_token=None,
                container_id="test",
            )

    def test_init_validates_anonymous_rejects_token(self):
        """Test that anonymous auth rejects auth_token."""
        with pytest.raises(
            ValueError,
            match="auth_token should not be provided when auth_type is 'anonymous'",
        ):
            DockerLaunchedSession(
                host="localhost",
                port=10000,
                auth_type="anonymous",
                auth_token="should_not_have_this",
                container_id="test",
            )


# ============================================================================
# DockerLaunchedSession Tests
# ============================================================================


class TestDockerLaunchedSession:
    """Tests for DockerLaunchedSession class."""

    def test_init(self):
        """Test DockerLaunchedSession initialization."""
        session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container_id",
        )
        assert session.port == 10000
        assert session.host == "localhost"
        assert session.launch_method == "docker"
        assert session.container_id == "test_container_id"

    def test_connection_url_property(self):
        """Test that connection_url property returns correct URL."""
        session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )
        assert session.connection_url == "http://localhost:10000"

    def test_connection_url_with_custom_host(self):
        """Test connection_url with custom host."""
        session = DockerLaunchedSession(
            host="192.168.1.1",
            port=8080,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )
        assert session.connection_url == "http://192.168.1.1:8080"

    def test_connection_url_with_auth_psk(self):
        """Test connection_url_with_auth for PSK auth."""
        session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="psk",
            auth_token="secret_token",
            container_id="test_container",
        )
        assert (
            session.connection_url_with_auth
            == "http://localhost:10000/?authToken=secret_token"
        )

    def test_connection_url_with_auth_anonymous(self):
        """Test connection_url_with_auth for anonymous auth (covers line 158)."""
        session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )
        assert session.connection_url_with_auth == "http://localhost:10000"


class TestPythonLaunchedSession:
    """Tests for PythonLaunchedSession class."""

    def test_init(self):
        """Test PythonLaunchedSession initialization."""
        mock_process = "mock_process"
        session = PythonLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            process=mock_process,
        )
        assert session.port == 10000
        assert session.launch_method == "python"
        assert session.process == mock_process


# ============================================================================
# DockerLaunchedSession Launch Tests
# ============================================================================


class TestDockerLaunchedSessionLaunch:
    """Tests for DockerLauncher class."""

    @pytest.mark.asyncio
    async def test_launch_success(self):
        """Test successful Docker launch."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(
                return_value=(b"container_abc123\n", b"")
            )
            mock_subprocess.return_value = mock_process

            result = await DockerLaunchedSession.launch(
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
                docker_image="ghcr.io/deephaven/server:latest",
                docker_memory_limit_gb=None,
                docker_cpu_limit=None,
                docker_volumes=[],
            )

            assert result.container_id == "container_abc123"
            assert result.port == 10000
            assert result.launch_method == "docker"

            # Verify PSK auth was set via START_OPTS
            call_args = mock_subprocess.call_args[0]
            psk_found = False
            for arg in call_args:
                if "START_OPTS=" in str(arg) and "authentication.psk=token" in str(arg):
                    psk_found = True
            assert psk_found

    @pytest.mark.asyncio
    async def test_launch_with_memory_limit(self):
        """Test launch with memory limit."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b"container_id\n", b""))
            mock_subprocess.return_value = mock_process

            await DockerLaunchedSession.launch(
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
                docker_image="ghcr.io/deephaven/server:latest",
                docker_memory_limit_gb=8.0,
                docker_cpu_limit=None,
                docker_volumes=[],
            )

            # Verify subprocess was called
            mock_subprocess.assert_called_once()

    @pytest.mark.asyncio
    async def test_launch_failure(self):
        """Test Docker launch failure."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 1
            mock_process.communicate = AsyncMock(return_value=(b"", b"Docker error"))
            mock_subprocess.return_value = mock_process

            with pytest.raises(SessionLaunchError):
                await DockerLaunchedSession.launch(
                    session_name="test",
                    port=10000,
                    auth_token="token",
                    heap_size_gb=4,
                    extra_jvm_args=[],
                    environment_vars={},
                    docker_image="ghcr.io/deephaven/server:latest",
                    docker_memory_limit_gb=None,
                    docker_cpu_limit=None,
                    docker_volumes=[],
                )

    @pytest.mark.asyncio
    async def test_launch_empty_container_id(self):
        """Test Docker launch with empty container ID."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(
                return_value=(b"", b"")
            )  # Empty stdout
            mock_subprocess.return_value = mock_process

            with pytest.raises(SessionLaunchError, match="returned empty container ID"):
                await DockerLaunchedSession.launch(
                    session_name="test",
                    port=10000,
                    auth_token="token",
                    heap_size_gb=4,
                    extra_jvm_args=[],
                    environment_vars={},
                    docker_image="ghcr.io/deephaven/server:latest",
                    docker_memory_limit_gb=None,
                    docker_cpu_limit=None,
                    docker_volumes=[],
                )

    @pytest.mark.asyncio
    async def test_launch_docker_daemon_not_running(self):
        """Test Docker launch when daemon is not running."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 1
            mock_process.communicate = AsyncMock(
                return_value=(b"", b"Cannot connect to the Docker daemon at unix:///var/run/docker.sock")
            )
            mock_subprocess.return_value = mock_process

            with pytest.raises(SessionLaunchError, match="Docker is not available"):
                await DockerLaunchedSession.launch(
                    session_name="test",
                    port=10000,
                    auth_token="token",
                    heap_size_gb=4,
                    extra_jvm_args=[],
                    environment_vars={},
                    docker_image="ghcr.io/deephaven/server:latest",
                    docker_memory_limit_gb=None,
                    docker_cpu_limit=None,
                    docker_volumes=[],
                )

    @pytest.mark.asyncio
    async def test_launch_docker_command_not_found(self):
        """Test Docker launch when Docker is not installed."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 127
            mock_process.communicate = AsyncMock(
                return_value=(b"", b"docker: command not found")
            )
            mock_subprocess.return_value = mock_process

            with pytest.raises(SessionLaunchError, match="Docker command not found"):
                await DockerLaunchedSession.launch(
                    session_name="test",
                    port=10000,
                    auth_token="token",
                    heap_size_gb=4,
                    extra_jvm_args=[],
                    environment_vars={},
                    docker_image="ghcr.io/deephaven/server:latest",
                    docker_memory_limit_gb=None,
                    docker_cpu_limit=None,
                    docker_volumes=[],
                )

    @pytest.mark.asyncio
    async def test_launch_with_instance_id(self):
        """Test Docker launch with instance_id for orphan tracking (lines 443-444)."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(
                return_value=(b"container_xyz789\n", b"")
            )
            mock_subprocess.return_value = mock_process

            result = await DockerLaunchedSession.launch(
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
                docker_image="ghcr.io/deephaven/server:latest",
                docker_memory_limit_gb=None,
                docker_cpu_limit=None,
                docker_volumes=[],
                instance_id="test-instance-uuid-123",
            )

            assert result.container_id == "container_xyz789"

            # Verify the --label flag was added with instance_id
            call_args = mock_subprocess.call_args[0]
            label_found = False
            for i, arg in enumerate(call_args):
                if arg == "--label" and i + 1 < len(call_args):
                    if (
                        call_args[i + 1]
                        == "deephaven-mcp-server-instance=test-instance-uuid-123"
                    ):
                        label_found = True
                        break
            assert label_found, "Docker label with instance_id not found in command"

    @pytest.mark.asyncio
    async def test_stop_success(self):
        """Test successful Docker stop."""
        session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b"", b""))
            mock_subprocess.return_value = mock_process

            await session.stop()

            # Verify docker stop was called
            mock_subprocess.assert_called_once()
            call_args = mock_subprocess.call_args[0]
            assert "docker" in call_args
            assert "stop" in call_args
            assert "test_container" in call_args
            assert session._stopped is True

    @pytest.mark.asyncio
    async def test_stop_idempotent(self):
        """Test that stop() is idempotent - calling twice is safe."""
        session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b"", b""))
            mock_subprocess.return_value = mock_process

            # First stop
            await session.stop()
            assert mock_subprocess.call_count == 1
            assert session._stopped is True

            # Second stop should be no-op (hits lines 460-463)
            await session.stop()
            assert mock_subprocess.call_count == 1  # Still 1, not called again

    @pytest.mark.asyncio
    async def test_stop_with_force_kill(self):
        """Test Docker stop with force kill fallback."""
        session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )

        call_count = [0]

        async def mock_subprocess_exec(*args, **kwargs):
            call_count[0] += 1
            mock_process = AsyncMock()
            if call_count[0] == 1:
                # First call (stop) fails
                mock_process.returncode = 1
                mock_process.communicate = AsyncMock(return_value=(b"", b"Stop failed"))
            else:
                # Second call (kill) succeeds
                mock_process.returncode = 0
                mock_process.communicate = AsyncMock(return_value=(b"", b""))
            return mock_process

        with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess_exec):
            await session.stop()

            # Should have called both stop and kill
            assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_init_validates_empty_container_id(self):
        """Test that empty container_id raises ValueError on init."""
        with pytest.raises(ValueError, match="container_id must be a non-empty string"):
            DockerLaunchedSession(
                host="localhost",
                port=10000,
                auth_type="anonymous",
                auth_token=None,
                container_id="",
            )


# ============================================================================
# PythonLaunchedSession Launch Tests
# ============================================================================


class TestPythonLaunchedSessionLaunch:
    """Tests for PythonLaunchedSession class."""

    @pytest.mark.asyncio
    async def test_launch_success(self):
        """Test successful python launch."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.pid = 12345
            mock_subprocess.return_value = mock_process

            result = await PythonLaunchedSession.launch(
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
            )

            assert result.process == mock_process
            assert result.port == 10000
            assert result.launch_method == "python"

    @pytest.mark.asyncio
    async def test_launch_with_jvm_args(self):
        """Test launch with JVM args."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.pid = 12345
            mock_subprocess.return_value = mock_process

            await PythonLaunchedSession.launch(
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=["-XX:+UseG1GC"],
                environment_vars={},
            )

            # Verify subprocess was called
            mock_subprocess.assert_called_once()
            call_args = mock_subprocess.call_args[0]
            # First arg should be deephaven executable (path ending with 'deephaven')
            assert call_args[0].endswith("deephaven")
            assert "server" in call_args

    @pytest.mark.asyncio
    async def test_launch_with_environment_vars(self):
        """Test launch with environment variables."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.pid = 12345
            mock_subprocess.return_value = mock_process

            result = await PythonLaunchedSession.launch(
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={"TEST_VAR": "test_value", "ANOTHER_VAR": "123"},
            )

            # Verify subprocess was called
            mock_subprocess.assert_called_once()
            # Verify environment vars were passed
            call_kwargs = mock_subprocess.call_args[1]
            assert "env" in call_kwargs
            assert call_kwargs["env"]["TEST_VAR"] == "test_value"
            assert call_kwargs["env"]["ANOTHER_VAR"] == "123"
            assert result.process == mock_process

    @pytest.mark.asyncio
    async def test_stop_terminates_process(self):
        """Test process termination."""
        mock_process = AsyncMock()
        mock_process.returncode = None
        mock_process.pid = 12345

        session = PythonLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            process=mock_process,
        )

        async def mock_wait():
            mock_process.returncode = 0

        mock_process.wait = AsyncMock(side_effect=mock_wait)

        await session.stop()

        mock_process.terminate.assert_called_once()
        mock_process.wait.assert_called()
        assert session._stopped is True

    @pytest.mark.asyncio
    async def test_stop_idempotent(self):
        """Test that stop() is idempotent - calling twice is safe."""
        mock_process = AsyncMock()
        mock_process.returncode = None
        mock_process.pid = 12345

        session = PythonLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            process=mock_process,
        )

        async def mock_wait():
            mock_process.returncode = 0

        mock_process.wait = AsyncMock(side_effect=mock_wait)

        # First stop
        await session.stop()
        assert mock_process.terminate.call_count == 1
        assert session._stopped is True

        # Second stop should be no-op (hits lines 651-654)
        await session.stop()
        assert mock_process.terminate.call_count == 1  # Still 1, not called again

    @pytest.mark.asyncio
    async def test_stop_with_kill_on_timeout(self):
        """Test process kill when terminate times out."""
        mock_process = AsyncMock()
        mock_process.returncode = None
        mock_process.pid = 12345

        session = PythonLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            process=mock_process,
        )

        # Simulate wait timing out
        with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
            await session.stop()

            mock_process.terminate.assert_called_once()
            mock_process.kill.assert_called_once()

    @pytest.mark.asyncio
    async def test_init_validates_none_process(self):
        """Test that None process raises ValueError on init."""
        with pytest.raises(ValueError, match="process must not be None"):
            PythonLaunchedSession(
                host="localhost",
                port=10000,
                auth_type="anonymous",
                auth_token=None,
                process=None,
            )

    def test_find_deephaven_executable_not_found_raises_error(self):
        """Test exception raised when deephaven not found in venv (no PATH fallback)."""
        from deephaven_mcp.resource_manager._launcher import _find_deephaven_executable

        with patch("sys.executable", "/nonexistent/python"):
            with patch("pathlib.Path.exists", return_value=False):
                with pytest.raises(
                    SessionLaunchError,
                    match="'deephaven' command not found at",
                ):
                    _find_deephaven_executable(None)


# ============================================================================
# Edge Case Tests
# ============================================================================


class TestWaitUntilReadyEdgeCases:
    """Edge case tests for wait_for_session_ready."""

    @pytest.mark.asyncio
    async def test_unexpected_status_code(self):
        """Test handling of unexpected status codes (not 200/404/401/403)."""
        session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test",
        )

        mock_response = MagicMock()
        mock_response.status = 500  # Unexpected status
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_client = MagicMock()
        mock_client.get = MagicMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession", return_value=mock_client):
            result = await session.wait_until_ready(
                timeout_seconds=0.5, check_interval_seconds=0.1, max_retries=1
            )
            # Should timeout since 500 is not considered "ready"
            assert result is False


class TestDockerLauncherEdgeCases:
    """Edge case tests for DockerLauncher."""

    @pytest.mark.asyncio
    async def test_launch_with_cpu_limit(self):
        """Test Docker launch with CPU limit."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b"container_id\n", b""))
            mock_subprocess.return_value = mock_process

            await DockerLaunchedSession.launch(
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
                docker_image="ghcr.io/deephaven/server:latest",
                docker_memory_limit_gb=None,
                docker_cpu_limit=2.0,
                docker_volumes=[],
            )

            # Verify --cpus was in the command
            call_args = mock_subprocess.call_args[0]
            assert "--cpus" in call_args
            assert "2.0" in call_args

    @pytest.mark.asyncio
    async def test_launch_with_volumes(self):
        """Test Docker launch with volume mounts."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b"container_id\n", b""))
            mock_subprocess.return_value = mock_process

            await DockerLaunchedSession.launch(
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
                docker_image="ghcr.io/deephaven/server:latest",
                docker_memory_limit_gb=None,
                docker_cpu_limit=None,
                docker_volumes=["/host/path:/container/path"],
            )

            # Verify -v was in the command
            call_args = mock_subprocess.call_args[0]
            assert "-v" in call_args
            assert "/host/path:/container/path" in call_args

    @pytest.mark.asyncio
    async def test_launch_without_auth_token(self):
        """Test Docker launch without auth token (anonymous)."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b"container_id\n", b""))
            mock_subprocess.return_value = mock_process

            await DockerLaunchedSession.launch(
                session_name="test",
                port=10000,
                auth_token=None,  # No auth token
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
                docker_image="ghcr.io/deephaven/server:latest",
                docker_memory_limit_gb=None,
                docker_cpu_limit=None,
                docker_volumes=[],
            )

            # Verify anonymous auth was set via START_OPTS
            call_args = mock_subprocess.call_args[0]
            # Find the START_OPTS env var with AuthHandlers
            auth_handler_found = False
            for i, arg in enumerate(call_args):
                if "START_OPTS=" in str(arg) and "AuthHandlers" in str(arg):
                    assert "io.deephaven.auth.AnonymousAuthenticationHandler" in str(
                        arg
                    )
                    auth_handler_found = True
            assert auth_handler_found

    @pytest.mark.asyncio
    async def test_launch_with_extra_jvm_args(self):
        """Test Docker launch with extra JVM args."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b"container_id\n", b""))
            mock_subprocess.return_value = mock_process

            await DockerLaunchedSession.launch(
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=["-XX:+UseG1GC", "-XX:MaxGCPauseMillis=200"],
                environment_vars={},
                docker_image="ghcr.io/deephaven/server:latest",
                docker_memory_limit_gb=None,
                docker_cpu_limit=None,
                docker_volumes=[],
            )

            mock_subprocess.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_exception_handling(self):
        """Test Docker stop exception handling."""
        session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test_container",
        )

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_subprocess.side_effect = RuntimeError("Docker daemon not running")

            with pytest.raises(
                SessionLaunchError, match="Failed to stop Docker container"
            ):
                await session.stop()


class TestPythonLauncherEdgeCases:
    """Edge case tests for PythonLaunchedSession."""

    @pytest.mark.asyncio
    async def test_launch_without_auth_token(self):
        """Test python launch without auth token (anonymous)."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.pid = 12345
            mock_subprocess.return_value = mock_process

            result = await PythonLaunchedSession.launch(
                session_name="test",
                port=10000,
                auth_token=None,  # No auth token
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
            )

            assert result.process == mock_process
            # Verify auth was set in JVM args (not env vars)
            call_args = mock_subprocess.call_args[0]
            jvm_args_found = False
            for arg in call_args:
                if "--jvm-args" in str(arg) or "AuthHandlers" in str(arg):
                    jvm_args_found = True
            assert jvm_args_found

    @pytest.mark.asyncio
    async def test_launch_exception_handling(self):
        """Test python launch exception handling."""

        with patch("asyncio.create_subprocess_exec") as mock_subprocess:
            mock_subprocess.side_effect = FileNotFoundError(
                "deephaven command not found"
            )

            with pytest.raises(
                SessionLaunchError, match="Failed to launch python session"
            ):
                await PythonLaunchedSession.launch(
                    session_name="test",
                    port=10000,
                    auth_token="token",
                    heap_size_gb=4,
                    extra_jvm_args=[],
                    environment_vars={},
                )

    @pytest.mark.asyncio
    async def test_stop_exception_handling(self):
        """Test pip stop exception handling."""
        mock_process = AsyncMock()
        mock_process.returncode = None
        mock_process.pid = 12345
        # Make wait raise an exception after terminate is called
        mock_process.wait = AsyncMock(side_effect=RuntimeError("Process error"))

        session = PythonLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            process=mock_process,
        )

        with pytest.raises(SessionLaunchError, match="Failed to stop python session"):
            await session.stop()

    @pytest.mark.asyncio
    async def test_launch_with_custom_venv_path(self):
        """Test python launch with custom venv path."""
        with (
            patch("asyncio.create_subprocess_exec") as mock_subprocess,
            patch("pathlib.Path.exists") as mock_exists,
            patch("pathlib.Path.is_dir") as mock_is_dir,
        ):
            # Mock custom venv path validation
            mock_exists.return_value = True
            mock_is_dir.return_value = True

            mock_process = AsyncMock()
            mock_process.pid = 12345
            mock_subprocess.return_value = mock_process

            await PythonLaunchedSession.launch(
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
                python_venv_path="/custom/venv",
            )

            # Verify subprocess was called with deephaven from custom venv
            mock_subprocess.assert_called_once()
            call_args = mock_subprocess.call_args[0]
            # The command should use deephaven from custom venv
            assert "/custom/venv/bin/deephaven" in call_args[0]

    @pytest.mark.asyncio
    async def test_launch_with_custom_venv_path_not_exists(self):
        """Test python launch with custom venv path that doesn't exist."""
        with pytest.raises(
            SessionLaunchError, match="Custom python_venv_path does not exist"
        ):
            await PythonLaunchedSession.launch(
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
                python_venv_path="/nonexistent/venv",
            )

    @pytest.mark.asyncio
    async def test_launch_with_custom_venv_path_not_directory(self):
        """Test python launch with custom venv path that is not a directory."""
        with (
            patch("pathlib.Path.exists") as mock_exists,
            patch("pathlib.Path.is_dir") as mock_is_dir,
        ):
            mock_exists.return_value = True
            mock_is_dir.return_value = False

            with pytest.raises(
                SessionLaunchError, match="Custom python_venv_path is not a directory"
            ):
                await PythonLaunchedSession.launch(
                    session_name="test",
                    port=10000,
                    auth_token="token",
                    heap_size_gb=4,
                    extra_jvm_args=[],
                    environment_vars={},
                    python_venv_path="/some/file.txt",
                )

    def test_find_deephaven_executable_custom_venv_no_deephaven(self):
        """Test _find_deephaven_executable when deephaven not found in custom venv."""
        from deephaven_mcp.resource_manager._launcher import _find_deephaven_executable
        
        with (
            patch("deephaven_mcp.resource_manager._launcher.Path") as mock_path_class,
        ):
            # Create mock for venv path - exists and is a directory
            mock_venv_path = MagicMock()
            mock_venv_path.exists.return_value = True
            mock_venv_path.is_dir.return_value = True
            
            # Create mock for deephaven executable - doesn't exist
            mock_deephaven_path = MagicMock()
            mock_deephaven_path.exists.return_value = False
            
            # Setup truediv to return the path through "/bin" then to "deephaven"
            mock_bin_path = MagicMock()
            mock_venv_path.__truediv__.side_effect = lambda x: mock_bin_path if x == "bin" else MagicMock()
            mock_bin_path.__truediv__.return_value = mock_deephaven_path
            
            mock_path_class.return_value = mock_venv_path

            with pytest.raises(
                SessionLaunchError,
                match="'deephaven' command not found at",
            ):
                _find_deephaven_executable("/custom/venv")


class TestDynamicManagerEdgeCases:
    """Edge case tests for DynamicCommunitySessionManager."""

    def test_to_dict_with_container_id(self):
        """Test to_dict includes container_id for docker sessions."""
        from deephaven_mcp.resource_manager import DynamicCommunitySessionManager

        launched_session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="psk",
            auth_token="test_token",
            container_id="test_container",
        )
        config = {"host": "localhost", "port": 10000}

        manager = DynamicCommunitySessionManager(
            name="test-session",
            config=config,
            launched_session=launched_session,
        )

        result = manager.to_dict()
        assert "container_id" in result
        assert result["container_id"] == "test_container"
        assert "process_id" not in result


# ============================================================================
# wait_until_ready Tests
# ============================================================================


class TestWaitUntilReady:
    """Tests for wait_for_session_ready function."""

    @pytest.mark.asyncio
    async def test_immediate_success_200(self):
        """Test immediate success with 200 status."""
        session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test",
        )

        # Mock the entire aiohttp flow
        with patch("aiohttp.ClientSession") as MockClientSession:
            # Create mock response
            mock_response = MagicMock()
            mock_response.status = 200

            # Create mock for response context manager
            mock_response_cm = MagicMock()
            mock_response_cm.__aenter__ = AsyncMock(return_value=mock_response)
            mock_response_cm.__aexit__ = AsyncMock(return_value=None)

            # Create mock client
            mock_client = MagicMock()
            mock_client.get = MagicMock(return_value=mock_response_cm)

            # Create mock for client context manager
            mock_client_cm = MagicMock()
            mock_client_cm.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cm.__aexit__ = AsyncMock(return_value=None)

            MockClientSession.return_value = mock_client_cm

            result = await session.wait_until_ready(timeout_seconds=5)
            assert result is True

    @pytest.mark.asyncio
    async def test_accepts_404_status(self):
        """Test that 404 is considered ready."""
        session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test",
        )

        with patch("aiohttp.ClientSession") as MockClientSession:
            mock_response = MagicMock()
            mock_response.status = 404

            mock_response_cm = MagicMock()
            mock_response_cm.__aenter__ = AsyncMock(return_value=mock_response)
            mock_response_cm.__aexit__ = AsyncMock(return_value=None)

            mock_client = MagicMock()
            mock_client.get = MagicMock(return_value=mock_response_cm)

            mock_client_cm = MagicMock()
            mock_client_cm.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cm.__aexit__ = AsyncMock(return_value=None)

            MockClientSession.return_value = mock_client_cm

            result = await session.wait_until_ready(timeout_seconds=5)
            assert result is True

    @pytest.mark.asyncio
    async def test_timeout_on_connection_errors(self):
        """Test timeout when connection always fails."""
        session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test",
        )

        with patch("aiohttp.ClientSession") as MockClientSession:
            mock_client = MagicMock()
            mock_client.get = MagicMock(
                side_effect=aiohttp.ClientError("Connection refused")
            )

            mock_client_cm = MagicMock()
            mock_client_cm.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cm.__aexit__ = AsyncMock(return_value=None)

            MockClientSession.return_value = mock_client_cm

            result = await session.wait_until_ready(
                timeout_seconds=0.5, check_interval_seconds=0.1, max_retries=1
            )
            assert result is False

    @pytest.mark.asyncio
    async def test_unexpected_exception_raises(self):
        """Test that unexpected exceptions raise SessionLaunchError."""
        session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test",
        )

        with patch("aiohttp.ClientSession") as MockClientSession:
            mock_client = MagicMock()
            mock_client.get = MagicMock(side_effect=RuntimeError("Unexpected"))

            mock_client_cm = MagicMock()
            mock_client_cm.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cm.__aexit__ = AsyncMock(return_value=None)

            MockClientSession.return_value = mock_client_cm

            with pytest.raises(SessionLaunchError, match="Health check failed"):
                await session.wait_until_ready(timeout_seconds=5)

    @pytest.mark.asyncio
    async def test_wait_until_ready_detects_process_crash(self):
        """Test that wait_until_ready detects when python process crashes (covers lines 317-321)."""
        mock_process = AsyncMock()
        mock_process.pid = 12345
        mock_process.returncode = 1  # Process has crashed

        session = PythonLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            process=mock_process,
        )

        # Should detect crash immediately
        ready = await session.wait_until_ready(
            timeout_seconds=10, check_interval_seconds=1
        )
        assert ready is False


class TestWaitUntilReadyRetryBackoff:
    """Test line 197: backoff sleep between retries."""

    @pytest.mark.asyncio
    async def test_backoff_sleep_on_client_error(self):
        """Test that asyncio.sleep(0.5) is called when ClientError occurs and retries remain."""
        session = DockerLaunchedSession(
            host="localhost",
            port=10000,
            auth_type="anonymous",
            auth_token=None,
            container_id="test",
        )

        # Track sleep calls
        sleep_calls = []

        original_sleep = asyncio.sleep

        async def tracking_sleep(duration):
            sleep_calls.append(duration)
            await original_sleep(0.001)  # Don't actually sleep

        # Track attempts
        attempt_count = [0]

        # Create a class that acts as an async context manager for the response
        class MockResponse:
            status = 200

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

        # Mock client.get() to fail first, then succeed
        def mock_get(*args, **kwargs):
            attempt_count[0] += 1
            if attempt_count[0] == 1:
                # First attempt: raise ClientError immediately (not async)
                raise aiohttp.ClientError("Connection refused")
            # Second attempt: return async context manager
            return MockResponse()

        # Mock ClientSession
        mock_client = MagicMock()
        mock_client.get = mock_get
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with (
            patch("aiohttp.ClientSession", return_value=mock_client),
            patch("asyncio.sleep", side_effect=tracking_sleep),
        ):

            result = await session.wait_until_ready(
                timeout_seconds=10, check_interval_seconds=1, max_retries=3
            )

            # Should succeed after retry
            assert result is True
            # Should have called sleep(0.5) for backoff (line 197)
            assert (
                0.5 in sleep_calls
            ), f"Backoff sleep(0.5) was not called. Sleep calls: {sleep_calls}"


# ============================================================================
# launch_session Function Tests
# ============================================================================


class TestLaunchSession:
    """Tests for launch_session convenience function."""

    @pytest.mark.asyncio
    async def test_launch_session_docker(self):
        """Test launch_session delegates to DockerLaunchedSession."""
        with patch.object(
            DockerLaunchedSession, "launch", new_callable=AsyncMock
        ) as mock_launch:
            mock_launch.return_value = MagicMock()

            await launch_session(
                launch_method="docker",
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
                docker_image="test:latest",
            )

            mock_launch.assert_called_once()

    @pytest.mark.asyncio
    async def test_launch_session_pip(self):
        """Test launch_session delegates to PythonLaunchedSession."""
        with patch.object(
            PythonLaunchedSession, "launch", new_callable=AsyncMock
        ) as mock_launch:
            mock_launch.return_value = MagicMock()

            await launch_session(
                launch_method="python",
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
            )

            mock_launch.assert_called_once()

    @pytest.mark.asyncio
    async def test_launch_session_invalid_method(self):
        """Test launch_session raises on invalid method."""
        with pytest.raises(ValueError, match="Unsupported launch method: invalid"):
            await launch_session(
                launch_method="invalid",
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
            )

    @pytest.mark.asyncio
    async def test_launch_session_pip_rejects_docker_image(self):
        """Test launch_session pip rejects docker_image parameter."""
        with pytest.raises(
            ValueError,
            match="docker_image parameter cannot be used with launch_method='python'",
        ):
            await launch_session(
                launch_method="python",
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
                docker_image="test:latest",
            )

    @pytest.mark.asyncio
    async def test_launch_session_pip_rejects_docker_memory_limit(self):
        """Test launch_session pip rejects docker_memory_limit_gb parameter."""
        with pytest.raises(
            ValueError,
            match="docker_memory_limit_gb parameter cannot be used with launch_method='python'",
        ):
            await launch_session(
                launch_method="python",
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
                docker_memory_limit_gb=8.0,
            )

    @pytest.mark.asyncio
    async def test_launch_session_pip_rejects_docker_cpu_limit(self):
        """Test launch_session pip rejects docker_cpu_limit parameter."""
        with pytest.raises(
            ValueError,
            match="docker_cpu_limit parameter cannot be used with launch_method='python'",
        ):
            await launch_session(
                launch_method="python",
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
                docker_cpu_limit=2.0,
            )

    @pytest.mark.asyncio
    async def test_launch_session_pip_rejects_docker_volumes(self):
        """Test launch_session pip rejects docker_volumes parameter."""
        with pytest.raises(
            ValueError,
            match="docker_volumes parameter cannot be used with launch_method='python'",
        ):
            await launch_session(
                launch_method="python",
                session_name="test",
                port=10000,
                auth_token="token",
                heap_size_gb=4,
                extra_jvm_args=[],
                environment_vars={},
                docker_volumes=["/host:/container"],
            )
