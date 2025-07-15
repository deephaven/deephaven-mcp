"""
Tests for the deephaven_mcp.mcp_systems_server server and tools.
"""

import asyncio
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import deephaven_mcp.mcp_systems_server._mcp as mcp_mod
from deephaven_mcp import config


class MockRequestContext:
    def __init__(self, lifespan_context):
        self.lifespan_context = lifespan_context


class MockContext:
    def __init__(self, lifespan_context):
        self.request_context = MockRequestContext(lifespan_context)


# === refresh ===


def test_run_script_reads_script_from_file():
    mock_session = MagicMock()
    mock_session.run_script = MagicMock()
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
            mcp_mod.run_script(
                context, worker_name="test_worker", script=None, script_path="dummy.py"
            )
        )
        assert result["success"] is True
        mock_session.run_script.assert_called_once_with(file_content)


@pytest.mark.asyncio
async def test_refresh_missing_context_keys():
    # context missing session_registry
    config_manager = AsyncMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(return_value=None)
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock()
    context = MockContext(
        {"config_manager": config_manager, "refresh_lock": refresh_lock}
    )
    result = await mcp_mod.refresh(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "session_registry" in result["error"]


@pytest.mark.asyncio
async def test_refresh_lock_error():
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(side_effect=Exception("lock error"))
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock()
    session_registry.close = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry": session_registry,
            "refresh_lock": refresh_lock,
        }
    )
    result = await mcp_mod.refresh(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "lock error" in result["error"]


# Suppress ResourceWarning about unclosed sockets, which can be triggered by mocks or library internals in CI
# but are not caused by this test (no real sockets are created or left open). This is required for Python 3.12 and older.
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket")
@pytest.mark.asyncio
async def test_refresh_success():
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(return_value=None)
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock()
    session_registry.close = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry": session_registry,
            "refresh_lock": refresh_lock,
        }
    )
    result = await mcp_mod.refresh(context)
    assert result == {"success": True}
    config_manager.clear_config_cache.assert_awaited_once()
    session_registry.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_refresh_failure():
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(return_value=None)
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock(side_effect=RuntimeError("fail"))
    session_registry.close = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry": session_registry,
            "refresh_lock": refresh_lock,
        }
    )
    result = await mcp_mod.refresh(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "fail" in result["error"]


# === describe_workers ===
@pytest.mark.asyncio
async def test_describe_workers_all_available_with_versions():
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    config_manager.get_system_session_names = AsyncMock(return_value=["w1", "w2"])
    config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "sessions": {
                    "w1": {"session_type": "python"},
                    "w2": {"session_type": "python"},
                }
            }
        }
    )
    alive_session = MagicMock(is_alive=True)

    # Create a mock session manager that will be returned by the registry's get method
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=alive_session)
    session_registry.get = AsyncMock(return_value=mock_session_manager)
    config_manager.get_community_session_config = AsyncMock(
        return_value={"session_type": "python"}
    )
    with patch.object(
        mcp_mod.queries, "get_dh_versions", AsyncMock(return_value=("1.2.3", "4.5.6"))
    ) as mock_get_dh_versions:
        context = MockContext(
            {
                "config_manager": config_manager,
                "session_registry": session_registry,
            }
        )
        result = await mcp_mod.describe_workers(context)
        assert result == {
            "success": True,
            "result": [
                {
                    "worker": "w1",
                    "available": True,
                    "programming_language": "python",
                    "deephaven_core_version": "1.2.3",
                    "deephaven_enterprise_version": "4.5.6",
                },
                {
                    "worker": "w2",
                    "available": True,
                    "programming_language": "python",
                    "deephaven_core_version": "1.2.3",
                    "deephaven_enterprise_version": "4.5.6",
                },
            ],
        }


@pytest.mark.asyncio
async def test_describe_workers_all_available_no_versions():
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    config_manager.get_system_session_names = AsyncMock(return_value=["w1", "w2"])
    config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "sessions": {
                    "w1": {"session_type": "python"},
                    "w2": {"session_type": "python"},
                }
            }
        }
    )
    alive_session = MagicMock(is_alive=True)
    # Create a mock session manager that will be returned by the registry
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=alive_session)
    session_registry.get = AsyncMock(return_value=mock_session_manager)
    config_manager.get_community_session_config = AsyncMock(
        return_value={"session_type": "python"}
    )
    # Both versions are None
    with patch.object(
        mcp_mod.queries, "get_dh_versions", AsyncMock(return_value=(None, None))
    ) as mock_get_dh_versions:
        context = MockContext(
            {
                "config_manager": config_manager,
                "session_registry": session_registry,
            }
        )
        result = await mcp_mod.describe_workers(context)
        assert result == {
            "success": True,
            "result": [
                {"worker": "w1", "available": True, "programming_language": "python"},
                {"worker": "w2", "available": True, "programming_language": "python"},
            ],
        }


@pytest.mark.asyncio
async def test_describe_workers_some_unavailable():
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    config_manager.get_system_session_names = AsyncMock(return_value=["w1", "w2", "w3"])
    config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "sessions": {
                    "w1": {"session_type": "python"},
                    "w2": {"session_type": "python"},
                    "w3": {"session_type": "python"},
                }
            }
        }
    )
    alive_session = MagicMock(is_alive=True)
    dead_session = MagicMock(is_alive=False)

    # Mock different manager behaviors based on session name
    alive_manager = AsyncMock()
    alive_manager.get = AsyncMock(return_value=alive_session)

    error_manager = AsyncMock()
    error_manager.get = AsyncMock(side_effect=RuntimeError("fail"))

    dead_manager = AsyncMock()
    dead_manager.get = AsyncMock(return_value=dead_session)

    async def get_session_manager(name):
        if name == "w1":
            return alive_manager
        elif name == "w2":
            return error_manager
        else:
            return dead_manager

    session_registry.get = AsyncMock(side_effect=get_session_manager)
    config_manager.get_community_session_config = AsyncMock(
        return_value={"session_type": "python"}
    )
    # Only w1 is alive, w2 fails, w3 is dead
    with patch.object(
        mcp_mod.queries, "get_dh_versions", AsyncMock(return_value=("1.2.3", None))
    ) as mock_get_dh_versions:
        context = MockContext(
            {
                "config_manager": config_manager,
                "session_registry": session_registry,
            }
        )
        result = await mcp_mod.describe_workers(context)
        # Only w1 gets versions, w2 and w3 are unavailable
        assert result == {
            "success": True,
            "result": [
                {
                    "worker": "w1",
                    "available": True,
                    "programming_language": "python",
                    "deephaven_core_version": "1.2.3",
                },
                {"worker": "w2", "available": False, "programming_language": "python"},
                {"worker": "w3", "available": False, "programming_language": "python"},
            ],
        }


@pytest.mark.asyncio
async def test_describe_workers_non_python():
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    config_manager.get_system_session_names = AsyncMock(return_value=["w1"])
    config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "sessions": {
                    "w1": {"session_type": "groovy"},
                }
            }
        }
    )
    alive_session = MagicMock(is_alive=True)
    # Create a mock session manager that will be returned by the registry
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=alive_session)
    session_registry.get = AsyncMock(return_value=mock_session_manager)
    config_manager.get_community_session_config = AsyncMock(
        return_value={"session_type": "groovy"}
    )
    # Should never call get_dh_versions for non-python
    with patch.object(
        mcp_mod.queries,
        "get_dh_versions",
        AsyncMock(side_effect=Exception("should not be called")),
    ) as mock_get_dh_versions:
        context = MockContext(
            {
                "config_manager": config_manager,
                "session_registry": session_registry,
            }
        )
        result = await mcp_mod.describe_workers(context)
        assert result == {
            "success": True,
            "result": [
                {"worker": "w1", "available": True, "programming_language": "groovy"},
            ],
        }


@pytest.mark.asyncio
async def test_describe_workers_versions_error():
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    config_manager.get_system_session_names = AsyncMock(return_value=["w1"])
    config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "sessions": {
                    "w1": {"session_type": "python"},
                }
            }
        }
    )
    alive_session = MagicMock(is_alive=True)
    # Create a mock session manager that will be returned by the registry
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=alive_session)
    session_registry.get = AsyncMock(return_value=mock_session_manager)
    config_manager.get_community_session_config = AsyncMock(
        return_value={"session_type": "python"}
    )
    # get_dh_versions throws
    with patch.object(
        mcp_mod.queries,
        "get_dh_versions",
        AsyncMock(side_effect=Exception("fail-version")),
    ) as mock_get_dh_versions:
        context = MockContext(
            {
                "config_manager": config_manager,
                "session_registry": session_registry,
            }
        )
        result = await mcp_mod.describe_workers(context)
        # Should not include version keys if get_dh_versions fails
        assert result == {
            "success": True,
            "result": [
                {"worker": "w1", "available": True, "programming_language": "python"},
            ],
        }


@pytest.mark.asyncio
async def test_describe_workers_some_unavailable_with_versions():
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    config_manager.get_system_session_names = AsyncMock(return_value=["w1", "w2", "w3"])
    config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "sessions": {
                    "w1": {"session_type": "python"},
                    "w2": {"session_type": "python"},
                    "w3": {"session_type": "python"},
                }
            }
        }
    )
    alive_session = MagicMock(is_alive=True)
    dead_session = MagicMock(is_alive=False)

    # Mock different manager behaviors based on session name
    alive_manager = AsyncMock()
    alive_manager.get = AsyncMock(return_value=alive_session)

    error_manager = AsyncMock()
    error_manager.get = AsyncMock(side_effect=RuntimeError("fail"))

    dead_manager = AsyncMock()
    dead_manager.get = AsyncMock(return_value=dead_session)

    async def get_session_manager(name):
        if name == "w1":
            return alive_manager
        elif name == "w2":
            return error_manager
        else:
            return dead_manager

    session_registry.get = AsyncMock(side_effect=get_session_manager)
    config_manager.get_community_session_config = AsyncMock(
        return_value={"session_type": "python"}
    )
    # Only w1 is alive, w2 fails, w3 is dead
    with patch.object(
        mcp_mod.queries, "get_dh_versions", AsyncMock(return_value=("1.2.3", "4.5.6"))
    ) as mock_get_dh_versions:
        context = MockContext(
            {
                "config_manager": config_manager,
                "session_registry": session_registry,
            }
        )
        result = await mcp_mod.describe_workers(context)
        assert result == {
            "success": True,
            "result": [
                {
                    "worker": "w1",
                    "available": True,
                    "programming_language": "python",
                    "deephaven_core_version": "1.2.3",
                    "deephaven_enterprise_version": "4.5.6",
                },
                {"worker": "w2", "available": False, "programming_language": "python"},
                {"worker": "w3", "available": False, "programming_language": "python"},
            ],
        }


@pytest.mark.asyncio
async def test_describe_workers_worker_config_error():
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    config_manager.get_system_session_names = AsyncMock(return_value=["w1"])
    config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "sessions": {
                    "w1": {"session_type": "python"},
                }
            }
        }
    )
    # Simulate get_worker_config raising an exception
    config_manager.get_community_session_config = AsyncMock(
        side_effect=config.CommunitySessionConfigurationError("config-fail")
    )
    # Setup session registry pattern
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=MagicMock(is_alive=True))
    session_registry.get = AsyncMock(return_value=mock_session_manager)
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry": session_registry,
        }
    )
    result = await mcp_mod.describe_workers(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "config-fail" in result["error"]


@pytest.mark.asyncio
async def test_describe_workers_config_error():
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    config_manager.get_config = AsyncMock(side_effect=Exception("fail-cfg"))
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry": session_registry,
        }
    )
    result = await mcp_mod.describe_workers(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "fail-cfg" in result["error"]


# === table_schemas ===


@pytest.mark.asyncio
async def test_table_schemas_empty_table_names():
    session_registry = AsyncMock()

    class DummySession:
        tables = ["t1"]

        def open_table(self, name):
            class MetaTable:
                def to_arrow(self):
                    class Arrow:
                        def to_pylist(self):
                            return [{"Name": name, "DataType": "int"}]

                    return Arrow()

            class Table:
                meta_table = MetaTable()

            return Table()

    # Mock session manager behavior
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=DummySession())
    session_registry.get = AsyncMock(return_value=mock_session_manager)
    context = MockContext({"session_registry": session_registry})
    res = await mcp_mod.table_schemas(context, worker_name="worker", table_names=[])
    assert isinstance(res, list)
    assert res == []


@pytest.mark.asyncio
async def test_table_schemas_no_tables():
    session_registry = AsyncMock()

    class DummySession:
        tables = []

        def open_table(self, name):
            raise Exception("Should not be called")

    # Mock session manager behavior
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=DummySession())
    session_registry.get = AsyncMock(return_value=mock_session_manager)
    context = MockContext({"session_registry": session_registry})
    res = await mcp_mod.table_schemas(context, worker_name="worker", table_names=None)
    assert isinstance(res, list)
    assert res == []


@pytest.mark.asyncio
async def test_table_schemas_success():
    # Create a mock session
    dummy_session = MagicMock()
    dummy_session.tables = ["table1"]

    # Create a mock for queries.get_meta_table that returns proper schema data
    class MockArrowTable:
        def to_pylist(self):
            return [{"Name": "table1", "DataType": "int"}]

    mock_get_meta_table = AsyncMock(return_value=MockArrowTable())

    # Set up the session manager mock
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=dummy_session)

    # Set up the session registry mock
    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "session_registry": session_registry,
        }
    )

    # Patch queries.get_meta_table to return our mock data
    with patch("deephaven_mcp.queries.get_meta_table", mock_get_meta_table):
        # Call table_schemas with a specific table name
        result = await mcp_mod.table_schemas(
            context, worker_name="test-worker", table_names=["table1"]
        )

    # Verify correct session access pattern
    session_registry.get.assert_awaited_once_with("test-worker")
    mock_session_manager.get.assert_awaited_once()

    # Verify the result
    assert len(result) == 1
    assert result[0]["success"] is True
    assert result[0]["table"] == "table1"
    assert result[0]["schema"][0]["name"] == "table1"
    assert result[0]["schema"][0]["type"] == "int"


@pytest.mark.asyncio
async def test_table_schemas_all_tables():
    # Create a mock session with two tables
    dummy_session = MagicMock()
    dummy_session.tables = ["t1", "t2"]

    # Set up side_effect for queries.get_meta_table to handle multiple calls
    # Will return different data based on which table is requested
    def get_meta_table_side_effect(session, table_name):
        class MockArrowTable:
            def to_pylist(self):
                return [{"Name": table_name, "DataType": "int"}]

        return MockArrowTable()

    # Create the mock with side_effect to handle different tables
    mock_get_meta_table = AsyncMock(side_effect=get_meta_table_side_effect)

    # Set up session manager and registry mocks
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=dummy_session)

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "session_registry": session_registry,
        }
    )

    # Patch queries.get_meta_table to return our mock data
    with patch("deephaven_mcp.queries.get_meta_table", mock_get_meta_table):
        # Call table_schemas with no table_names to test getting all tables
        result = await mcp_mod.table_schemas(context, worker_name="worker")

    # Should return results for both tables in the dummy_session.tables list
    assert len(result) == 2
    assert result[0]["success"] is True
    assert result[1]["success"] is True
    assert result[0]["table"] in ["t1", "t2"]
    assert result[1]["table"] in ["t1", "t2"]
    assert result[0]["table"] != result[1]["table"]
    assert result[0]["schema"][0]["name"] in ["t1", "t2"]
    assert result[1]["schema"][0]["name"] in ["t1", "t2"]
    assert result[0]["schema"][0]["type"] == "int"
    assert result[1]["schema"][0]["type"] == "int"


@pytest.mark.asyncio
async def test_table_schemas_schema_key_error():
    # Create our mock session
    dummy_session = MagicMock()
    dummy_session.tables = ["table1"]

    # Create a mock for queries.get_meta_table that returns data with missing required keys
    class MockArrowTable:
        def to_pylist(self):
            # Missing 'Name' and 'DataType' keys, which should trigger the KeyError
            return [{"foo": "bar"}]

    # Set up the get_meta_table mock
    mock_get_meta_table = AsyncMock(return_value=MockArrowTable())

    # Set up session manager mock
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=dummy_session)

    # Set up session registry mock
    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "session_registry": session_registry,
        }
    )

    # Patch queries.get_meta_table to return our mock
    with patch("deephaven_mcp.queries.get_meta_table", mock_get_meta_table):
        # Call table_schemas with a specific table name
        result = await mcp_mod.table_schemas(
            context, worker_name="test-worker", table_names=["table1"]
        )

    # Verify correct session access pattern
    session_registry.get.assert_awaited_once_with("test-worker")
    mock_session_manager.get.assert_awaited_once()

    # Verify that the function returns the expected error about the missing keys
    assert result[0]["success"] is False
    assert "Name" in result[0]["error"] or "'Name'" in result[0]["error"]
    # Don't check for "required" word as the exact error message may vary
    assert "isError" in result[0] and result[0]["isError"] is True


@pytest.mark.asyncio
async def test_table_schemas_session_error():
    # Following the pattern in _mcp.py:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name) - set to fail here
    # 3. session = await session_manager.get()

    # Set up session_registry to throw an exception when get() is called
    session_registry = AsyncMock()
    session_registry.get = AsyncMock(side_effect=Exception("fail"))

    context = MockContext(
        {
            "session_registry": session_registry,
        }
    )
    res = await mcp_mod.table_schemas(context, worker_name="worker", table_names=["t1"])
    assert isinstance(res, list)
    assert res[0]["success"] is False
    assert res[0]["isError"] is True
    assert "fail" in res[0]["error"]


# === run_script ===


@pytest.mark.asyncio
async def test_run_script_both_script_and_path():
    # Both script and script_path provided, should prefer script
    # Following the pattern in _mcp.py:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name)
    # 3. session = await session_manager.get()

    # Create a session mock with run_script method
    session = MagicMock()
    session.run_script = MagicMock(return_value=None)

    # Set up session manager to return the session
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=session)

    # Set up session registry to return the manager
    session_registry = AsyncMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})
    result = await mcp_mod.run_script(
        context, worker_name="foo", script="print('hi')", script_path="/tmp/fake.py"
    )
    assert result["success"] is True
    assert session.run_script.call_count >= 1
    session.run_script.assert_any_call("print('hi')")


@pytest.mark.asyncio
async def test_run_script_missing_worker():
    # Following the pattern in _mcp.py:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name) - fails here
    # 3. session = await session_manager.get()

    # Set up session_registry to throw an exception when get() is called
    session_registry = AsyncMock()
    session_registry.get = AsyncMock(side_effect=Exception("no worker"))

    context = MockContext({"session_registry": session_registry})
    result = await mcp_mod.run_script(context, worker_name=None, script="print('hi')")
    assert result["success"] is False
    assert result["isError"] is True
    assert "no worker" in result["error"]


@pytest.mark.asyncio
async def test_run_script_both_none():
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
    result = await mcp_mod.run_script(context, worker_name="foo")
    assert result["success"] is False
    assert result["isError"] is True
    assert "Must provide either script or script_path" in result["error"]


@pytest.mark.asyncio
async def test_app_lifespan_yields_context_and_cleans_up():
    class DummyServer:
        name = "dummy-server"

    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()

    # Configure necessary mocks for the app_lifespan function to work
    config_manager.get_config = AsyncMock(return_value={})
    session_registry.initialize = AsyncMock()
    session_registry.close = AsyncMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.config.ConfigManager",
            return_value=config_manager,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.CombinedSessionRegistry",
            return_value=session_registry,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.asyncio.Lock",
            return_value=refresh_lock,
        ),
    ):
        from deephaven_mcp.mcp_systems_server._mcp import app_lifespan

        server = DummyServer()
        async with app_lifespan(server) as context:
            assert context["config_manager"] is config_manager
            assert context["session_registry"] is session_registry
            assert context["refresh_lock"] is refresh_lock
        session_registry.close.assert_awaited_once()


# Use filterwarnings to suppress ResourceWarning about unclosed sockets, which can be triggered by mocks or library internals in CI
# but are not caused by this test (no real sockets are created or left open). This is required for Python 3.12 and older.
@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_run_script_success():
    # Main success test for run_script
    class DummySession:
        called = None

        @staticmethod
        def run_script(script):
            DummySession.called = script
            return None

    # Set up the session registry pattern correctly
    dummy_session = DummySession()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=dummy_session)

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})
    result = await mcp_mod.run_script(context, worker_name="worker", script="print(1)")

    # Check correct session access pattern
    session_registry.get.assert_awaited_once_with("worker")
    mock_session_manager.get.assert_awaited_once()

    # Verify results
    assert result["success"] is True
    assert DummySession.called == "print(1)"


@pytest.mark.asyncio
async def test_run_script_no_script():
    mock_session_manager = MagicMock()
    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})
    res = await mcp_mod.run_script(context, worker_name="worker")

    # No calls to session_registry should be made since validation fails first
    session_registry.get.assert_not_awaited()

    # Verify error message
    assert res["success"] is False
    assert res["isError"] is True
    assert "Must provide either script or script_path." in res["error"]


# Use filterwarnings to suppress ResourceWarning about unclosed sockets and event loops, which can be triggered by mocks or library internals in CI
# but are not caused by this test (no real sockets or event loops are created or left open). This is required for Python 3.12+ and some CI environments.
@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_run_script_neither_script_nor_path():
    # Test validation that requires either script or script_path
    # This should fail before any session_registry calls
    mock_session_manager = MagicMock()
    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})

    # Call with neither script nor script_path
    res = await mcp_mod.run_script(
        context, worker_name="worker", script=None, script_path=None
    )

    # No calls to session_registry should be made since validation fails first
    session_registry.get.assert_not_awaited()

    # Verify error message
    assert res["success"] is False
    assert res["isError"] is True
    assert "Must provide either script or script_path." in res["error"]


# Use filterwarnings to suppress ResourceWarning about unclosed sockets, which can be triggered by mocks or library internals in CI
# but are not caused by this test (no real sockets are created or left open). This is required for Python 3.12 and older.
@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_run_script_session_error():
    # Run with fake session registry that errors on get
    # so that we hit the exception branch in run_script
    session_registry = MagicMock()
    session_registry.get = AsyncMock(side_effect=Exception("fail"))

    context = MockContext({"session_registry": session_registry})
    res = await mcp_mod.run_script(context, worker_name="worker", script="print(1)")

    # Verify the session registry was called with the correct worker name
    session_registry.get.assert_awaited_once_with("worker")

    # Verify error response
    assert res["success"] is False
    assert res["isError"] is True
    assert "fail" in res["error"]


# Use filterwarnings to suppress ResourceWarning about unclosed sockets, which can be triggered by mocks or library internals in CI
# but are not caused by this test (no real sockets are created or left open). This is required for Python 3.12 and older.
@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_run_script_script_path():
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
        def run_script(script):
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
        res = await mcp_mod.run_script(
            context, worker_name="worker", script_path=script_path
        )

    # Verify session registry was called correctly
    session_registry.get.assert_awaited_once_with("worker")
    mock_session_manager.get.assert_awaited_once()

    # Verify file open and script execution
    mock_open.assert_called_once_with(script_path)
    assert DummySession.called == script_content
    assert res["success"] is True


@pytest.mark.asyncio
async def test_run_script_script_path_none_error():
    # Test case where neither script nor script_path is provided
    # This should fail with a validation error, not by calling session_registry.get
    session_registry = MagicMock()
    session_registry.get = AsyncMock()

    context = MockContext({"session_registry": session_registry})
    res = await mcp_mod.run_script(
        context, worker_name="worker", script=None, script_path=None
    )

    # Verify the validation error is returned
    assert res["success"] is False
    assert res["isError"] is True
    assert "Must provide either script or script_path" in res["error"]

    # Verify the session registry was NOT called (validation fails before that)
    session_registry.get.assert_not_awaited()


# === pip_packages ===


@pytest.mark.asyncio
async def test_pip_packages_success():
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
            }
        )
        result = await mcp_mod.pip_packages(context, worker_name="test_worker")

        # Check correct session access pattern
        mock_session_registry.get.assert_awaited_once_with("test_worker")
        mock_session_manager.get.assert_awaited_once()

        # Verify results
        assert result["success"] is True
        assert len(result["result"]) == 2
        assert result["result"][0]["package"] == "numpy"
        assert result["result"][0]["version"] == "1.25.0"


@pytest.mark.asyncio
async def test_pip_packages_empty():
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
        result = await mcp_mod.pip_packages(context, worker_name="test_worker")

    # Verify results
    assert result["success"] is True
    assert result["result"] == []

    # Check correct session access pattern
    mock_session_registry.get.assert_awaited_once_with("test_worker")
    mock_session_manager.get.assert_awaited_once()
    mock_get_pip_packages_table.assert_awaited_once()


@pytest.mark.asyncio
async def test_pip_packages_malformed_data():
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
            }
        )
        result = await mcp_mod.pip_packages(context, worker_name="test_worker")

    # Verify results
    assert result["success"] is False
    assert result["isError"] is True
    assert "Malformed package data" in result["error"]

    # Check correct session access pattern
    mock_session_registry.get.assert_awaited_once_with("test_worker")
    mock_session_manager.get.assert_awaited_once()
    mock_get_pip_packages_table.assert_awaited_once()


@pytest.mark.asyncio
async def test_pip_packages_error():
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
            }
        )
        result = await mcp_mod.pip_packages(context, worker_name="test_worker")

        # Verify results
        assert result["success"] is False
        assert result["isError"] is True
        assert "Table error" in result["error"]

        # Check correct session access pattern
        mock_session_registry.get.assert_awaited_once_with("test_worker")
        mock_session_manager.get.assert_awaited_once()


@pytest.mark.asyncio
async def test_pip_packages_worker_not_found():
    """Test pip_packages when the worker is not found."""
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
            }
        )
        result = await mcp_mod.pip_packages(context, worker_name="nonexistent_worker")
        assert result["success"] is False
        assert "Worker not found" in result["error"]
        assert result["isError"] is True

        # Verify correct session access pattern
        mock_session_registry.get.assert_awaited_once_with("nonexistent_worker")
