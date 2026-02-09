"""
Tests for deephaven_mcp.mcp_systems_server._tools.script.
"""

import asyncio
import os
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

from conftest import MockContext, create_mock_instance_tracker

from deephaven_mcp.mcp_systems_server._tools.script import (
    session_pip_list,
    session_script_run,
)
from deephaven_mcp.mcp_systems_server._tools.session_community import (
    session_community_create,
    session_community_delete,
)
from deephaven_mcp import config
from deephaven_mcp.resource_manager import (
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
    PythonLaunchedSession,
    ResourceLivenessStatus,
    SystemType,
)


class TestRemainingEdgeCases:
    """Tests for remaining edge cases."""

    @pytest.mark.asyncio
    async def test_create_with_pip_and_process_id(self):
        """Test line 995: process_id in session details."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        community_config = {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {
                    "launch_method": "python",
                },
            }
        }

        full_config = {"community": community_config}
        mock_config_manager.get_config = AsyncMock(return_value=full_config)
        mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
        mock_session_registry.add_session = AsyncMock()
        mock_session_registry.get_all = AsyncMock(return_value={})

        mock_process = MagicMock()
        mock_process.pid = 99999
        mock_launched_session = MagicMock(spec=PythonLaunchedSession)
        mock_launched_session.port = 10000
        mock_launched_session.launch_method = "python"
        mock_launched_session.connection_url = "http://localhost:10000"
        mock_launched_session.connection_url_with_auth = "http://localhost:10000"
        mock_launched_session.process = mock_process
        mock_launched_session.auth_type = "anonymous"
        mock_launched_session.auth_token = None

        with (
            patch(
                "deephaven_mcp.mcp_systems_server._tools.session_community.launch_session"
            ) as mock_launch_session,
            patch(
                "deephaven_mcp.mcp_systems_server._tools.session_community.find_available_port",
                return_value=10000,
            ),
            patch(
                "deephaven_mcp.mcp_systems_server._tools.session_community.generate_auth_token",
                return_value="token",
            ),
            patch.object(
                mock_launched_session,
                "wait_until_ready",
                new=AsyncMock(return_value=True),
            ),
        ):

            mock_launch_session.return_value = mock_launched_session

            context = MockContext(
                {
                    "config_manager": mock_config_manager,
                    "session_registry": mock_session_registry,
                    "instance_tracker": create_mock_instance_tracker(),
                }
            )

            result = await session_community_create(
                context, session_name="test-session"
            )

            assert result["success"] is True
            assert result["process_id"] == 99999

    @pytest.mark.asyncio
    async def test_create_auth_token_env_var_not_found(self):
        """Test that error is raised when auth_token_env_var is configured but env var not found."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        community_config = {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {
                    "auth_token_env_var": "NONEXISTENT_VAR",
                },
            }
        }

        full_config = {"community": community_config}
        mock_config_manager.get_config = AsyncMock(return_value=full_config)
        mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
        mock_session_registry.add_session = AsyncMock()
        mock_session_registry.get_all = AsyncMock(return_value={})

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        with patch.dict(os.environ, {}, clear=True):  # Empty environment
            result = await session_community_create(
                context, session_name="test-session"
            )

            # Should fail because explicitly configured env var is not set
            assert result["success"] is False
            assert "NONEXISTENT_VAR" in result["error"]
            assert "not set" in result["error"]
            assert result["isError"] is True

    @pytest.mark.asyncio
    async def test_create_cleanup_fails_on_timeout(self):
        """Test lines 3824-3825: cleanup fails after health check timeout."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        community_config = {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {},
            }
        }

        full_config = {"community": community_config}
        mock_config_manager.get_config = AsyncMock(return_value=full_config)
        mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
        mock_session_registry.get_all = AsyncMock(return_value={})

        mock_launched_session = MagicMock(spec=DockerLaunchedSession)
        mock_launched_session.port = 10000
        mock_launched_session.launch_method = "docker"
        mock_launched_session.connection_url = "http://localhost:10000"
        mock_launched_session.connection_url_with_auth = (
            "http://localhost:10000/?psk=test_token"
        )
        mock_launched_session.container_id = "test"
        mock_launched_session.auth_type = "psk"
        mock_launched_session.auth_token = "test_token"

        with (
            patch(
                "deephaven_mcp.mcp_systems_server._tools.session_community.launch_session"
            ) as mock_launch_session,
            patch(
                "deephaven_mcp.mcp_systems_server._tools.session_community.find_available_port",
                return_value=10000,
            ),
            patch(
                "deephaven_mcp.mcp_systems_server._tools.session_community.generate_auth_token",
                return_value="token",
            ),
            patch.object(
                mock_launched_session,
                "wait_until_ready",
                new=AsyncMock(return_value=False),
            ),
        ):

            mock_launch_session.return_value = mock_launched_session
            mock_launched_session.stop = AsyncMock(
                side_effect=Exception("Cleanup failed")
            )

            context = MockContext(
                {
                    "config_manager": mock_config_manager,
                    "session_registry": mock_session_registry,
                    "instance_tracker": create_mock_instance_tracker(),
                }
            )

            result = await session_community_create(
                context, session_name="test-session"
            )

            assert result["success"] is False
            # Verify cleanup was attempted even though it failed
            mock_launched_session.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_removal_returns_none(self):
        """Test lines 4044-4047: removal returns None (not found)."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        mock_launched_session = MagicMock(spec=DockerLaunchedSession)
        mock_launched_session.launch_method = "docker"

        mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
        mock_manager.full_name = "community:dynamic:test-session"
        mock_manager._name = "test-session"
        mock_manager.source = "dynamic"
        mock_manager.system_type = SystemType.COMMUNITY
        mock_manager.launched_session = mock_launched_session
        mock_manager.close = AsyncMock()

        mock_session_registry.get = AsyncMock(return_value=mock_manager)
        mock_session_registry.remove_session = AsyncMock(return_value=None)  # Not found

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await session_community_delete(
            context, session_name="test-session"
        )

        # Should still succeed even though removal returned None
        assert result["success"] is True




def test_run_script_reads_script_from_file():
    mock_session = MagicMock()
    mock_session.run_script = AsyncMock()
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)
    mock_registry = AsyncMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    context = MockContext(
        {
            "session_registry": mock_registry,
            "config_manager": AsyncMock(),
        }
    )

    file_content = "print('hello')"

    class DummyFile:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def read(self):
            return file_content

    with patch("aiofiles.open", return_value=DummyFile()):
        result = asyncio.run(
            session_script_run(
                context, session_id="test_worker", script=None, script_path="dummy.py"
            )
        )
        assert result["success"] is True
        mock_session.run_script.assert_called_once_with(file_content)



@pytest.mark.asyncio
async def test_session_script_run_both_script_and_path():
    # Both script and script_path provided, should prefer script
    # Following the pattern in _mcp.py:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name)
    # 3. session = await session_manager.get()

    # Create a session mock with run_script method
    session = MagicMock()
    session.run_script = AsyncMock(return_value=None)

    # Set up session manager to return the session
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=session)

    # Set up session registry to return the manager
    session_registry = AsyncMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})
    result = await session_script_run(
        context, session_id="foo", script="print('hi')", script_path="/tmp/fake.py"
    )
    assert result["success"] is True
    assert session.run_script.call_count >= 1
    session.run_script.assert_any_call("print('hi')")



@pytest.mark.asyncio
async def test_session_script_run_missing_session():
    # Following the pattern in _mcp.py:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(session_id) - fails here
    # 3. session = await session_manager.get()

    # Set up session_registry to throw an exception when get() is called
    session_registry = AsyncMock()
    session_registry.get = AsyncMock(side_effect=Exception("no session"))

    context = MockContext({"session_registry": session_registry})
    result = await session_script_run(
        context, session_id=None, script="print('hi')"
    )
    assert result["success"] is False
    assert result["isError"] is True
    assert "no session" in result["error"]



@pytest.mark.asyncio
async def test_session_script_run_both_none():
    # Following the pattern in _mcp.py:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name)
    # 3. session = await session_manager.get()

    # This test shouldn't get as far as session creation since both script and script_path are None
    # But we still set up the mocks correctly
    session = AsyncMock()
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=session)

    session_registry = AsyncMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})
    result = await session_script_run(context, session_id="foo")
    assert result["success"] is False
    assert result["isError"] is True
    assert "Must provide either script or script_path" in result["error"]



@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_session_script_run_success():
    # Main success test for run_script
    class DummySession:
        called = None

        @staticmethod
        async def run_script(script):
            DummySession.called = script
            return None

    # Set up the session registry pattern correctly
    dummy_session = DummySession()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=dummy_session)

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})
    result = await session_script_run(
        context, session_id="worker", script="print(1)"
    )

    # Check correct session access pattern
    session_registry.get.assert_awaited_once_with("worker")
    mock_session_manager.get.assert_awaited_once()

    # Verify results
    assert result["success"] is True
    assert DummySession.called == "print(1)"



@pytest.mark.asyncio
async def test_session_script_run_no_script():
    mock_session_manager = MagicMock()
    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})
    res = await session_script_run(context, session_id="worker")

    # No calls to session_registry should be made since validation fails first
    session_registry.get.assert_not_awaited()

    # Verify error message
    assert res["success"] is False
    assert res["isError"] is True
    assert "Must provide either script or script_path." in res["error"]



@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_session_script_run_neither_script_nor_path():
    # Test validation that requires either script or script_path
    # This should fail before any session_registry calls
    mock_session_manager = MagicMock()
    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})

    # Call with neither script nor script_path
    res = await session_script_run(
        context, session_id="worker", script=None, script_path=None
    )

    # No calls to session_registry should be made since validation fails first
    session_registry.get.assert_not_awaited()

    # Verify error message
    assert res["success"] is False
    assert res["isError"] is True
    assert "Must provide either script or script_path." in res["error"]



@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_session_script_run_session_error():
    # Run with fake session registry that errors on get
    # so that we hit the exception branch in run_script
    session_registry = MagicMock()
    session_registry.get = AsyncMock(side_effect=Exception("fail"))

    context = MockContext({"session_registry": session_registry})
    res = await session_script_run(
        context, session_id="worker", script="print(1)"
    )

    # Verify the session registry was called with the correct session id
    session_registry.get.assert_awaited_once_with("worker")

    # Verify error response
    assert res["success"] is False
    assert res["isError"] is True
    assert "fail" in res["error"]



@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_session_script_run_script_path():
    # Test run_script with script_path and no script
    script_path = "/tmp/test.py"
    script_content = "print('loaded from file')"

    # Mock aiofiles.open properly as a context manager
    # This is the key part: We need a regular MagicMock that returns context manager methods
    mock_file_cm = MagicMock()
    mock_file_cm.__aenter__ = AsyncMock(
        return_value=MagicMock(read=AsyncMock(return_value=script_content))
    )
    mock_file_cm.__aexit__ = AsyncMock(return_value=None)

    mock_open = MagicMock(return_value=mock_file_cm)

    # Create a simple mock session class
    class DummySession:
        called = None

        @staticmethod
        async def run_script(script):
            DummySession.called = script
            return None

    # Set up session mocks
    dummy_session = DummySession()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=dummy_session)

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    # Apply the patches and run the test
    with patch("aiofiles.open", mock_open):
        context = MockContext({"session_registry": session_registry})
        res = await session_script_run(
            context, session_id="worker", script_path=script_path
        )

    # Verify session registry was called correctly
    session_registry.get.assert_awaited_once_with("worker")
    mock_session_manager.get.assert_awaited_once()

    # Verify file open and script execution
    mock_open.assert_called_once_with(script_path)
    assert DummySession.called == script_content
    assert res["success"] is True



@pytest.mark.asyncio
async def test_session_script_run_script_path_none_error():
    # Test case where neither script nor script_path is provided
    # This should fail with a validation error, not by calling session_registry.get
    session_registry = MagicMock()
    session_registry.get = AsyncMock()

    context = MockContext({"session_registry": session_registry})
    res = await session_script_run(
        context, session_id="worker", script=None, script_path=None
    )

    # Verify the validation error is returned
    assert res["success"] is False
    assert res["isError"] is True
    assert "Must provide either script or script_path" in res["error"]

    # Verify the session registry was NOT called (validation fails before that)
    session_registry.get.assert_not_awaited()



@pytest.mark.asyncio
async def test_session_pip_list_success():
    """Test successful retrieval of pip packages."""
    # Set up mock for the Arrow table and data frame
    mock_arrow_table = MagicMock()
    mock_df = MagicMock()
    mock_df.to_dict.side_effect = lambda *args, **kwargs: (
        [
            {"Package": "numpy", "Version": "1.25.0"},
            {"Package": "pandas", "Version": "2.0.1"},
        ]
        if kwargs.get("orient") == "records"
        else []
    )
    mock_arrow_table.to_pandas.return_value = mock_df

    # Mock the query that fetches pip packages
    mock_get_pip_packages_table = AsyncMock(return_value=mock_arrow_table)

    # Set up the session registry pattern:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name)
    # 3. session = await session_manager.get()
    mock_session = MagicMock()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_session_registry = MagicMock()
    mock_session_registry.get = AsyncMock(return_value=mock_session_manager)

    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        mock_get_pip_packages_table,
    ):
        context = MockContext(
            {
                "session_registry": mock_session_registry,
                "config_manager": AsyncMock(),
                "instance_tracker": create_mock_instance_tracker(),
            }
        )
        result = await session_pip_list(context, session_id="test_worker")

        # Check correct session access pattern
        mock_session_registry.get.assert_awaited_once_with("test_worker")
        mock_session_manager.get.assert_awaited_once()

        # Verify results
        assert result["success"] is True
        assert len(result["result"]) == 2
        assert result["result"][0]["package"] == "numpy"
        assert result["result"][0]["version"] == "1.25.0"



@pytest.mark.asyncio
async def test_session_pip_list_empty():
    """Test pip_packages with an empty table."""
    # Set up mock for the Arrow table and data frame with empty results
    mock_arrow_table = MagicMock()
    mock_df = MagicMock()
    mock_df.to_dict.side_effect = lambda *args, **kwargs: []
    mock_arrow_table.to_pandas.return_value = mock_df
    mock_get_pip_packages_table = AsyncMock(return_value=mock_arrow_table)

    # Set up the session registry pattern:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name)
    # 3. session = await session_manager.get()
    mock_session = MagicMock()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_session_registry = MagicMock()
    mock_session_registry.get = AsyncMock(return_value=mock_session_manager)

    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        mock_get_pip_packages_table,
    ):
        context = MockContext(
            {"session_registry": mock_session_registry, "config_manager": AsyncMock()}
        )
        result = await session_pip_list(context, session_id="test_worker")

    # Verify results
    assert result["success"] is True
    assert result["result"] == []

    # Check correct session access pattern
    mock_session_registry.get.assert_awaited_once_with("test_worker")
    mock_session_manager.get.assert_awaited_once()
    mock_get_pip_packages_table.assert_awaited_once()



@pytest.mark.asyncio
async def test_session_pip_list_malformed_data():
    """Test pip_packages with malformed data."""
    # Set up mock for the Arrow table and data frame with malformed results
    mock_arrow_table = MagicMock()
    mock_df = MagicMock()
    mock_df.to_dict.side_effect = lambda *args, **kwargs: (
        [{"badkey": 1}] if kwargs.get("orient") == "records" else []
    )  # missing 'Package' and 'Version'
    mock_arrow_table.to_pandas.return_value = mock_df
    mock_get_pip_packages_table = AsyncMock(return_value=mock_arrow_table)

    # Set up the session registry pattern:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name)
    # 3. session = await session_manager.get()
    mock_session = MagicMock()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_session_registry = MagicMock()
    mock_session_registry.get = AsyncMock(return_value=mock_session_manager)

    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        mock_get_pip_packages_table,
    ):
        context = MockContext(
            {
                "session_registry": mock_session_registry,
                "config_manager": AsyncMock(),
                "instance_tracker": create_mock_instance_tracker(),
            }
        )
        result = await session_pip_list(context, session_id="test_worker")

    # Verify results
    assert result["success"] is False
    assert result["isError"] is True
    assert "Malformed package data" in result["error"]

    # Check correct session access pattern
    mock_session_registry.get.assert_awaited_once_with("test_worker")
    mock_session_manager.get.assert_awaited_once()
    mock_get_pip_packages_table.assert_awaited_once()



@pytest.mark.asyncio
async def test_session_pip_list_error():
    """Test pip_packages with an error."""
    # Mock the query that fetches pip packages to throw an exception
    mock_get_pip_packages_table = AsyncMock(side_effect=Exception("Table error"))

    # Set up the session registry pattern:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name)
    # 3. session = await session_manager.get()
    mock_session = MagicMock()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_session_registry = MagicMock()
    mock_session_registry.get = AsyncMock(return_value=mock_session_manager)

    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        mock_get_pip_packages_table,
    ):
        context = MockContext(
            {
                "session_registry": mock_session_registry,
                "config_manager": AsyncMock(),
                "instance_tracker": create_mock_instance_tracker(),
            }
        )
        result = await session_pip_list(context, session_id="test_worker")

        # Verify results
        assert result["success"] is False
        assert result["isError"] is True
        assert "Table error" in result["error"]

        # Check correct session access pattern
        mock_session_registry.get.assert_awaited_once_with("test_worker")
        mock_session_manager.get.assert_awaited_once()



@pytest.mark.asyncio
async def test_session_pip_list_session_not_found():
    """Test pip_packages when the session is not found."""
    mock_get_pip_packages_table = AsyncMock(return_value=MagicMock())

    # Set up session_registry to fail when get() is called
    mock_session_registry = MagicMock()
    mock_session_registry.get = AsyncMock(side_effect=ValueError("Worker not found"))

    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        mock_get_pip_packages_table,
    ):
        context = MockContext(
            {
                "session_registry": mock_session_registry,
                "config_manager": AsyncMock(),
                "instance_tracker": create_mock_instance_tracker(),
            }
        )
        result = await session_pip_list(
            context, session_id="nonexistent_worker"
        )
        assert result["success"] is False
        assert "Worker not found" in result["error"]
        assert result["isError"] is True

        # Verify correct session access pattern
        mock_session_registry.get.assert_awaited_once_with("nonexistent_worker")


