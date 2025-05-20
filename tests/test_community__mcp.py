import asyncio
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import deephaven_mcp.community._mcp as mcp_mod


class MockRequestContext:
    def __init__(self, lifespan_context):
        self.lifespan_context = lifespan_context


class MockContext:
    def __init__(self, lifespan_context):
        self.request_context = MockRequestContext(lifespan_context)


# === refresh ===


def test_run_script_reads_script_from_file(monkeypatch):
    mock_session = MagicMock()
    mock_session.run_script = MagicMock()
    mock_manager = AsyncMock()
    mock_manager.get_or_create_session = AsyncMock(return_value=mock_session)
    context = MockContext(
        {
            "session_manager": mock_manager,
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
async def test_refresh_missing_context_keys(monkeypatch):
    # context missing session_manager
    config_manager = MagicMock()
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
    assert "session_manager" in result["error"]


@pytest.mark.asyncio
async def test_refresh_lock_error(monkeypatch):
    config_manager = MagicMock()
    session_manager = MagicMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(side_effect=Exception("lock error"))
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock()
    session_manager.clear_all_sessions = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_manager": session_manager,
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
async def test_refresh_success(monkeypatch):
    config_manager = MagicMock()
    session_manager = MagicMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(return_value=None)
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock()
    session_manager.clear_all_sessions = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_manager": session_manager,
            "refresh_lock": refresh_lock,
        }
    )
    result = await mcp_mod.refresh(context)
    assert result == {"success": True}
    config_manager.clear_config_cache.assert_awaited_once()
    session_manager.clear_all_sessions.assert_awaited_once()


@pytest.mark.asyncio
async def test_refresh_failure(monkeypatch):
    config_manager = MagicMock()
    session_manager = MagicMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(return_value=None)
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock(side_effect=RuntimeError("fail"))
    session_manager.clear_all_sessions = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_manager": session_manager,
            "refresh_lock": refresh_lock,
        }
    )
    result = await mcp_mod.refresh(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "fail" in result["error"]


# === worker_statuses ===
@pytest.mark.asyncio
async def test_worker_statuses_all_available(monkeypatch):
    config_manager = MagicMock()
    session_manager = MagicMock()
    config_manager.get_worker_names = AsyncMock(return_value=["w1", "w2"])
    alive_session = MagicMock(is_alive=True)
    session_manager.get_or_create_session = AsyncMock(return_value=alive_session)
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_manager": session_manager,
        }
    )
    result = await mcp_mod.worker_statuses(context)
    assert result == {
        "success": True,
        "result": [
            {"worker": "w1", "available": True},
            {"worker": "w2", "available": True},
        ],
    }


@pytest.mark.asyncio
async def test_worker_statuses_some_unavailable(monkeypatch):
    config_manager = MagicMock()
    session_manager = MagicMock()
    config_manager.get_worker_names = AsyncMock(return_value=["w1", "w2", "w3"])
    alive_session = MagicMock(is_alive=True)
    dead_session = MagicMock(is_alive=False)

    async def get_or_create_session(name):
        if name == "w1":
            return alive_session
        elif name == "w2":
            raise RuntimeError("fail")
        else:
            return dead_session

    session_manager.get_or_create_session = AsyncMock(side_effect=get_or_create_session)
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_manager": session_manager,
        }
    )
    result = await mcp_mod.worker_statuses(context)
    assert result == {
        "success": True,
        "result": [
            {"worker": "w1", "available": True},
            {"worker": "w2", "available": False},
            {"worker": "w3", "available": False},
        ],
    }


@pytest.mark.asyncio
async def test_worker_statuses_config_error(monkeypatch):
    config_manager = MagicMock()
    session_manager = MagicMock()
    config_manager.get_worker_names = AsyncMock(side_effect=Exception("fail-cfg"))
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_manager": session_manager,
        }
    )
    result = await mcp_mod.worker_statuses(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "fail-cfg" in result["error"]


# === table_schemas ===


@pytest.mark.asyncio
async def test_table_schemas_empty_table_names(monkeypatch):
    session_manager = MagicMock()

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

    session_manager.get_or_create_session = AsyncMock(return_value=DummySession())
    context = MockContext({"session_manager": session_manager})
    res = await mcp_mod.table_schemas(context, worker_name="worker", table_names=[])
    assert isinstance(res, list)
    assert res == []


@pytest.mark.asyncio
async def test_table_schemas_no_tables(monkeypatch):
    session_manager = MagicMock()

    class DummySession:
        tables = []

        def open_table(self, name):
            raise Exception("Should not be called")

    session_manager.get_or_create_session = AsyncMock(return_value=DummySession())
    context = MockContext({"session_manager": session_manager})
    res = await mcp_mod.table_schemas(context, worker_name="worker", table_names=None)
    assert isinstance(res, list)
    assert res == []


@pytest.mark.asyncio
async def test_table_schemas_success(monkeypatch):
    session_manager = MagicMock()

    class DummySession:
        def open_table(self, name):
            class Arrow:
                def to_pylist(self):
                    return [{"Name": name, "DataType": "int"}]

            class Table:
                def to_arrow(self):
                    return Arrow()

            return Table()

    session_manager.get_or_create_session = AsyncMock(return_value=DummySession())
    context = MockContext(
        {
            "session_manager": session_manager,
        }
    )
    res = await mcp_mod.table_schemas(context, worker_name="worker", table_names=["t1"])
    assert isinstance(res, list)
    assert res[0]["success"] is True
    assert res[0]["table"] == "t1"
    assert res[0]["schema"] == [{"name": "t1", "type": "int"}]


@pytest.mark.asyncio
async def test_table_schemas_all_tables(monkeypatch):
    session_manager = MagicMock()

    class DummySession:
        tables = ["t1", "t2"]

        def open_table(self, name):
            class Arrow:
                def to_pylist(self):
                    return [{"Name": name, "DataType": "int"}]

            class Table:
                def to_arrow(self):
                    return Arrow()

            return Table()

    session_manager.get_or_create_session = AsyncMock(return_value=DummySession())
    context = MockContext(
        {
            "session_manager": session_manager,
        }
    )
    res = await mcp_mod.table_schemas(context, worker_name="worker", table_names=None)
    assert isinstance(res, list)
    assert len(res) == 2
    for i, tname in enumerate(["t1", "t2"]):
        assert res[i]["success"] is True
        assert res[i]["table"] == tname
        assert res[i]["schema"] == [{"name": tname, "type": "int"}]


@pytest.mark.asyncio
async def test_table_schemas_schema_key_error(monkeypatch):
    session_manager = MagicMock()

    class DummySession:
        def open_table(self, name):
            class Arrow:
                def to_pylist(self):
                    # Missing 'Name' and/or 'DataType' keys
                    return [{"foo": "bar"}]

            class Table:
                def to_arrow(self):
                    return Arrow()

            return Table()

    session_manager.get_or_create_session = AsyncMock(return_value=DummySession())
    context = MockContext(
        {
            "session_manager": session_manager,
        }
    )
    res = await mcp_mod.table_schemas(context, worker_name="worker", table_names=["t1"])
    assert isinstance(res, list)
    assert res[0]["success"] is False
    assert res[0]["isError"] is True
    assert "Name" in res[0]["error"]


@pytest.mark.asyncio
async def test_table_schemas_session_error(monkeypatch):
    session_manager = MagicMock()
    session_manager.get_or_create_session = AsyncMock(side_effect=Exception("fail"))
    context = MockContext(
        {
            "session_manager": session_manager,
        }
    )
    res = await mcp_mod.table_schemas(context, worker_name="worker", table_names=["t1"])
    assert isinstance(res, list)
    assert res[0]["success"] is False
    assert res[0]["isError"] is True
    assert "fail" in res[0]["error"]


# === run_script ===


@pytest.mark.asyncio
async def test_run_script_both_script_and_path(monkeypatch):
    # Both script and script_path provided, should prefer script
    session = MagicMock()
    session.run_script = MagicMock(return_value=None)
    session_manager = AsyncMock()
    session_manager.get_or_create_session = AsyncMock(return_value=session)
    context = MockContext({"session_manager": session_manager})
    result = await mcp_mod.run_script(
        context, worker_name="foo", script="print('hi')", script_path="/tmp/fake.py"
    )
    assert result["success"] is True
    assert session.run_script.call_count >= 1
    session.run_script.assert_any_call("print('hi')")


@pytest.mark.asyncio
async def test_run_script_missing_worker(monkeypatch):
    session_manager = AsyncMock()
    session_manager.get_or_create_session = AsyncMock(
        side_effect=Exception("no worker")
    )
    context = MockContext({"session_manager": session_manager})
    result = await mcp_mod.run_script(context, worker_name=None, script="print('hi')")
    assert result["success"] is False
    assert result["isError"] is True
    assert "no worker" in result["error"]


@pytest.mark.asyncio
async def test_run_script_both_none(monkeypatch):
    session_manager = AsyncMock()
    session_manager.get_or_create_session = AsyncMock(return_value=AsyncMock())
    context = MockContext({"session_manager": session_manager})
    result = await mcp_mod.run_script(context, worker_name="foo")
    assert result["success"] is False
    assert result["isError"] is True
    assert "Must provide either script or script_path" in result["error"]


@pytest.mark.asyncio
async def test_app_lifespan_yields_context_and_cleans_up():
    from deephaven_mcp.community._mcp import app_lifespan

    class DummyServer:
        name = "dummy-server"

    config_manager = MagicMock()
    session_manager = MagicMock()
    refresh_lock = AsyncMock()
    config_manager.get_config = AsyncMock()
    session_manager.clear_all_sessions = AsyncMock()

    with (
        patch(
            "deephaven_mcp.community._mcp.config.ConfigManager",
            return_value=config_manager,
        ),
        patch(
            "deephaven_mcp.community._mcp.sessions.SessionManager",
            return_value=session_manager,
        ),
        patch("deephaven_mcp.community._mcp.asyncio.Lock", return_value=refresh_lock),
    ):
        server = DummyServer()
        async with app_lifespan(server) as context:
            assert context["config_manager"] is config_manager
            assert context["session_manager"] is session_manager
            assert context["refresh_lock"] is refresh_lock
        session_manager.clear_all_sessions.assert_awaited_once()


# Use filterwarnings to suppress ResourceWarning about unclosed sockets, which can be triggered by mocks or library internals in CI
# but are not caused by this test (no real sockets are created or left open). This is required for Python 3.12 and older.
@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_run_script_success(monkeypatch):
    session_manager = MagicMock()

    class DummySession:
        def run_script(self, script):
            DummySession.called = script

    session_manager.get_or_create_session = AsyncMock(return_value=DummySession())
    context = MockContext(
        {
            "session_manager": session_manager,
        }
    )
    res = await mcp_mod.run_script(context, worker_name="worker", script="print(1)")
    assert res["success"] is True
    assert DummySession.called == "print(1)"


@pytest.mark.asyncio
async def test_run_script_no_script(monkeypatch):
    context = MockContext({"session_manager": MagicMock()})
    res = await mcp_mod.run_script(context, worker_name="worker")
    assert res["success"] is False
    assert res["isError"] is True
    assert "Must provide either script or script_path." in res["error"]


# Use filterwarnings to suppress ResourceWarning about unclosed sockets and event loops, which can be triggered by mocks or library internals in CI
# but are not caused by this test (no real sockets or event loops are created or left open). This is required for Python 3.12+ and some CI environments.
@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_run_script_session_error(monkeypatch):
    session_manager = MagicMock()
    session_manager.get_or_create_session = AsyncMock(side_effect=Exception("fail"))
    context = MockContext({"session_manager": session_manager})
    res = await mcp_mod.run_script(context, worker_name="worker", script="print(1)")
    assert res["success"] is False
    assert res["isError"] is True
    assert "fail" in res["error"]


# Use filterwarnings to suppress ResourceWarning about unclosed sockets, which can be triggered by mocks or library internals in CI
# but are not caused by this test (no real sockets are created or left open). This is required for Python 3.12 and older.
@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_run_script_script_path(monkeypatch):
    session_manager = MagicMock()

    class DummySession:
        def run_script(self, script):
            DummySession.called = script

    session_manager.get_or_create_session = AsyncMock(return_value=DummySession())
    context = MockContext({"session_manager": session_manager})
    with patch("aiofiles.open", new_callable=MagicMock) as aio_open:
        mock_file = aio_open.return_value.__aenter__.return_value
        mock_file.read = AsyncMock(return_value="print(123)")
        res = await mcp_mod.run_script(
            context, worker_name="worker", script_path="foo.py"
        )
    assert res["success"] is True
    assert DummySession.called == "print(123)"


# Use filterwarnings to suppress ResourceWarning about unclosed sockets and event loops, which can be triggered by mocks or library internals in CI
# but are not caused by this test (no real sockets or event loops are created or left open). This is required for Python 3.12+ and some CI environments.
@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_run_script_script_path_error(monkeypatch):
    session_manager = MagicMock()

    class DummySession:
        def run_script(self, script):
            DummySession.called = script

    session_manager.get_or_create_session = AsyncMock(return_value=DummySession())
    context = MockContext({"session_manager": session_manager})
    with patch("aiofiles.open", new_callable=MagicMock) as aio_open:
        aio_open.side_effect = FileNotFoundError("fail-open")
        res = await mcp_mod.run_script(
            context, worker_name="worker", script_path="foo.py"
        )
        assert res["success"] is False
        assert res["isError"] is True
        assert "fail-open" in res["error"]


# === pip_packages ===


@pytest.mark.asyncio
async def test_pip_packages_empty(monkeypatch):
    mock_df = MagicMock()
    mock_df.to_dict.return_value = []
    mock_arrow_table = MagicMock()
    mock_arrow_table.to_pandas = MagicMock(return_value=mock_df)
    table = MagicMock()
    table.to_arrow = MagicMock(return_value=mock_arrow_table)
    mock_session = MagicMock()
    mock_session.run_script = MagicMock(return_value=None)
    mock_session.open_table = MagicMock(return_value=table)
    mock_manager = AsyncMock()
    mock_manager.get_or_create_session = AsyncMock(return_value=mock_session)
    context = MockContext(
        {"session_manager": mock_manager, "config_manager": AsyncMock()}
    )
    result = await mcp_mod.pip_packages(context, worker_name="test_worker")
    assert result["success"] is True
    assert result["result"] == []


@pytest.mark.asyncio
async def test_pip_packages_malformed_data(monkeypatch):
    mock_df = MagicMock()
    mock_df.to_dict.return_value = [{"badkey": 1}]
    mock_arrow_table = MagicMock()
    mock_arrow_table.to_pandas = MagicMock(return_value=mock_df)
    table = MagicMock()
    table.to_arrow = MagicMock(return_value=mock_arrow_table)
    mock_session = MagicMock()
    mock_session.run_script = MagicMock(return_value=None)
    mock_session.open_table = MagicMock(return_value=table)
    mock_manager = AsyncMock()
    mock_manager.get_or_create_session = AsyncMock(return_value=mock_session)
    context = MockContext(
        {"session_manager": mock_manager, "config_manager": AsyncMock()}
    )
    result = await mcp_mod.pip_packages(context, worker_name="test_worker")
    assert result["success"] is False
    assert result["isError"] is True
    assert "package" in result["error"] or "version" in result["error"]


@pytest.mark.asyncio
async def test_pip_packages_success(monkeypatch):
    # Test successful retrieval of pip packages.
    mock_df = MagicMock()
    mock_df.to_dict.return_value = [
        {"package": "numpy", "version": "1.25.0"},
        {"package": "pandas", "version": "2.0.1"},
    ]

    mock_arrow_table = MagicMock()
    mock_arrow_table.to_pandas.return_value = mock_df
    table = MagicMock()
    table.to_arrow.return_value = mock_arrow_table
    mock_session = MagicMock()
    mock_session.run_script = MagicMock(return_value=None)
    mock_session.open_table = MagicMock(return_value=table)
    mock_manager = AsyncMock()
    mock_manager.get_or_create_session = AsyncMock(return_value=mock_session)
    context = MockContext(
        {
            "session_manager": mock_manager,
            "config_manager": AsyncMock(),
        }
    )
    result = await mcp_mod.pip_packages(context, worker_name="test_worker")
    assert result["success"] is True
    assert len(result["result"]) == 2
    assert result["result"][0]["package"] == "numpy"
    assert result["result"][0]["version"] == "1.25.0"
    mock_manager.get_or_create_session.assert_awaited_once_with("test_worker")
    mock_session.run_script.assert_called_once()
    mock_session.open_table.assert_called_once_with("_pip_packages_table")
    mock_arrow_table.to_pandas.assert_called_once()
    mock_df.to_dict.assert_called_once_with(orient="records")


@pytest.mark.asyncio
async def test_pip_packages_worker_not_found(monkeypatch):
    """Test pip_packages when the worker is not found."""
    # Mock the session manager to raise an exception
    mock_manager = AsyncMock()
    mock_manager.get_or_create_session = AsyncMock(
        side_effect=ValueError("Worker not found")
    )

    # Create the context with our mocks
    context = MockContext(
        {
            "session_manager": mock_manager,
            "config_manager": AsyncMock(),
            "importlib_metadata": MagicMock(),
        }
    )

    # Call the function
    result = await mcp_mod.pip_packages(context, worker_name="nonexistent_worker")

    # Verify the result
    assert result["success"] is False
    assert "Worker not found" in result["error"]
    assert result["isError"] is True
    mock_manager.get_or_create_session.assert_awaited_once_with("nonexistent_worker")


@pytest.mark.asyncio
async def test_pip_packages_script_error(monkeypatch):
    """Test pip_packages when there's an error executing the script."""
    mock_session = MagicMock()
    mock_session.run_script = MagicMock(
        side_effect=Exception("Script execution failed")
    )
    mock_session.open_table = MagicMock()
    mock_manager = AsyncMock()
    mock_manager.get_or_create_session = AsyncMock(return_value=mock_session)
    context = MockContext(
        {
            "session_manager": mock_manager,
            "config_manager": AsyncMock(),
        }
    )
    result = await mcp_mod.pip_packages(context, worker_name="test_worker")
    assert result["success"] is False
    assert "Script execution failed" in result["error"]
    assert result["isError"] is True
    mock_manager.get_or_create_session.assert_awaited_once_with("test_worker")
    mock_session.run_script.assert_called_once()


@pytest.mark.asyncio
async def test_pip_packages_table_error(monkeypatch):
    """Test pip_packages when there's an error accessing the table."""
    mock_session = MagicMock()
    mock_session.run_script = MagicMock(return_value=None)
    mock_session.open_table = MagicMock(side_effect=Exception("Table not found"))
    mock_manager = AsyncMock()
    mock_manager.get_or_create_session = AsyncMock(return_value=mock_session)
    context = MockContext(
        {
            "session_manager": mock_manager,
            "config_manager": AsyncMock(),
        }
    )
    result = await mcp_mod.pip_packages(context, worker_name="test_worker")
    assert result["success"] is False
    assert "Table not found" in result["error"]
    assert result["isError"] is True
    mock_manager.get_or_create_session.assert_awaited_once_with("test_worker")
    mock_session.run_script.assert_called_once()
    mock_session.open_table.assert_called_once_with("_pip_packages_table")
