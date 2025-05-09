import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import deephaven_mcp.community._mcp as mcp_mod
from types import SimpleNamespace

class MockRequestContext:
    def __init__(self, lifespan_context):
        self.lifespan_context = lifespan_context

class MockContext:
    def __init__(self, lifespan_context):
        self.request_context = MockRequestContext(lifespan_context)


# === refresh ===
@pytest.mark.asyncio
async def test_refresh_success(monkeypatch):
    config_manager = MagicMock()
    session_manager = MagicMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(return_value=None)
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock()
    session_manager.clear_all_sessions = AsyncMock()
    context = MockContext({
        "config_manager": config_manager,
        "session_manager": session_manager,
        "refresh_lock": refresh_lock,
    })
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
    context = MockContext({
        "config_manager": config_manager,
        "session_manager": session_manager,
        "refresh_lock": refresh_lock,
    })
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
    context = MockContext({
        "config_manager": config_manager,
        "session_manager": session_manager,
    })
    result = await mcp_mod.worker_statuses(context)
    assert result == {"success": True, "result": [
        {"worker": "w1", "available": True},
        {"worker": "w2", "available": True}
    ]}

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
    context = MockContext({
        "config_manager": config_manager,
        "session_manager": session_manager,
    })
    result = await mcp_mod.worker_statuses(context)
    assert result == {"success": True, "result": [
        {"worker": "w1", "available": True},
        {"worker": "w2", "available": False},
        {"worker": "w3", "available": False}
    ]}

@pytest.mark.asyncio
async def test_worker_statuses_config_error(monkeypatch):
    config_manager = MagicMock()
    session_manager = MagicMock()
    config_manager.get_worker_names = AsyncMock(side_effect=Exception("fail-cfg"))
    context = MockContext({
        "config_manager": config_manager,
        "session_manager": session_manager,
    })
    result = await mcp_mod.worker_statuses(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "fail-cfg" in result["error"]

# === table_schemas ===
@pytest.mark.asyncio
async def test_table_schemas_success(monkeypatch):
    session_manager = MagicMock()
    class DummySession:
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
    context = MockContext({
        "session_manager": session_manager,
    })
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
    context = MockContext({
        "session_manager": session_manager,
    })
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
            class MetaTable:
                def to_arrow(self):
                    class Arrow:
                        def to_pylist(self):
                            # Missing 'Name' and/or 'DataType' keys
                            return [{"foo": "bar"}]
                    return Arrow()
            class Table:
                meta_table = MetaTable()
            return Table()
    session_manager.get_or_create_session = AsyncMock(return_value=DummySession())
    context = MockContext({
        "session_manager": session_manager,
    })
    res = await mcp_mod.table_schemas(context, worker_name="worker", table_names=["t1"])
    assert isinstance(res, list)
    assert res[0]["success"] is False
    assert res[0]["isError"] is True
    assert "Name" in res[0]["error"]

@pytest.mark.asyncio
async def test_table_schemas_session_error(monkeypatch):
    session_manager = MagicMock()
    session_manager.get_or_create_session = AsyncMock(side_effect=Exception("fail"))
    context = MockContext({
        "session_manager": session_manager,
    })
    res = await mcp_mod.table_schemas(context, worker_name="worker", table_names=["t1"])
    assert isinstance(res, list)
    assert res[0]["success"] is False
    assert res[0]["isError"] is True
    assert "fail" in res[0]["error"]

# === run_script ===

def test_app_lifespan_yields_context_and_cleans_up():
    from deephaven_mcp.community._mcp import app_lifespan
    class DummyServer:
        name = "dummy-server"
    config_manager = MagicMock()
    session_manager = MagicMock()
    refresh_lock = AsyncMock()
    config_manager.get_config = AsyncMock()
    session_manager.clear_all_sessions = AsyncMock()

    with patch("deephaven_mcp.community._mcp.config.ConfigManager", return_value=config_manager), \
         patch("deephaven_mcp.community._mcp.sessions.SessionManager", return_value=session_manager), \
         patch("deephaven_mcp.community._mcp.asyncio.Lock", return_value=refresh_lock):
        server = DummyServer()
        import asyncio
        async def run():
            async with app_lifespan(server) as context:
                assert context["config_manager"] is config_manager
                assert context["session_manager"] is session_manager
                assert context["refresh_lock"] is refresh_lock
            session_manager.clear_all_sessions.assert_awaited_once()
        asyncio.run(run())

@pytest.mark.asyncio
async def test_run_script_success(monkeypatch):
    session_manager = MagicMock()
    class DummySession:
        def run_script(self, script):
            DummySession.called = script
    session_manager.get_or_create_session = AsyncMock(return_value=DummySession())
    context = MockContext({
        "session_manager": session_manager,
    })
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

@pytest.mark.asyncio
async def test_run_script_session_error(monkeypatch):
    session_manager = MagicMock()
    session_manager.get_or_create_session = AsyncMock(side_effect=Exception("fail"))
    context = MockContext({"session_manager": session_manager})
    res = await mcp_mod.run_script(context, worker_name="worker", script="print(1)")
    assert res["success"] is False
    assert res["isError"] is True
    assert "fail" in res["error"]

@pytest.mark.asyncio
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
        res = await mcp_mod.run_script(context, worker_name="worker", script_path="foo.py")
        assert res["success"] is True
        assert DummySession.called == "print(123)"

@pytest.mark.asyncio
async def test_run_script_script_path_error(monkeypatch):
    session_manager = MagicMock()
    class DummySession:
        def run_script(self, script):
            DummySession.called = script
    session_manager.get_or_create_session = AsyncMock(return_value=DummySession())
    context = MockContext({"session_manager": session_manager})
    with patch("aiofiles.open", new_callable=MagicMock) as aio_open:
        aio_open.side_effect = RuntimeError("fail-open")
        res = await mcp_mod.run_script(context, worker_name="worker", script_path="foo.py")
        assert res["success"] is False
        assert res["isError"] is True
        assert "fail-open" in res["error"]
