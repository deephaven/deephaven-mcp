import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import deephaven_mcp.community._mcp as mcp_mod

# === refresh ===
@pytest.mark.asyncio
async def test_refresh_success(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_CONFIG_MANAGER", MagicMock())
    monkeypatch.setattr(mcp_mod, "_SESSION_MANAGER", MagicMock())
    mcp_mod._CONFIG_MANAGER.clear_config_cache = AsyncMock()
    mcp_mod._SESSION_MANAGER.clear_all_sessions = AsyncMock()
    result = await mcp_mod.refresh()
    assert result == {"success": True}
    mcp_mod._CONFIG_MANAGER.clear_config_cache.assert_awaited_once()
    mcp_mod._SESSION_MANAGER.clear_all_sessions.assert_awaited_once()

@pytest.mark.asyncio
async def test_refresh_failure(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_CONFIG_MANAGER", MagicMock())
    monkeypatch.setattr(mcp_mod, "_SESSION_MANAGER", MagicMock())
    mcp_mod._CONFIG_MANAGER.clear_config_cache = AsyncMock(side_effect=RuntimeError("fail"))
    result = await mcp_mod.refresh()
    assert result["success"] is False
    assert result["isError"] is True
    assert "fail" in result["error"]

# === default_worker ===
@pytest.mark.asyncio
async def test_default_worker_success(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_CONFIG_MANAGER", MagicMock())
    mcp_mod._CONFIG_MANAGER.get_worker_name_default = AsyncMock(return_value="worker1")
    result = await mcp_mod.default_worker()
    assert result == {"success": True, "result": "worker1"}

@pytest.mark.asyncio
async def test_default_worker_error(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_CONFIG_MANAGER", MagicMock())
    mcp_mod._CONFIG_MANAGER.get_worker_name_default = AsyncMock(side_effect=Exception("fail"))
    result = await mcp_mod.default_worker()
    assert result["success"] is False
    assert result["isError"] is True
    assert "fail" in result["error"]

# === worker_names ===
@pytest.mark.asyncio
async def test_worker_names_success(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_CONFIG_MANAGER", MagicMock())
    mcp_mod._CONFIG_MANAGER.get_worker_names = AsyncMock(return_value=["a", "b"])
    result = await mcp_mod.worker_names()
    assert result == {"success": True, "result": ["a", "b"]}

@pytest.mark.asyncio
async def test_worker_names_error(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_CONFIG_MANAGER", MagicMock())
    mcp_mod._CONFIG_MANAGER.get_worker_names = AsyncMock(side_effect=Exception("fail"))
    result = await mcp_mod.worker_names()
    assert result["success"] is False
    assert result["isError"] is True
    assert "fail" in result["error"]

# === table_schemas ===
@pytest.mark.asyncio
async def test_table_schemas_success(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_SESSION_MANAGER", MagicMock())
    monkeypatch.setattr(mcp_mod, "_CONFIG_MANAGER", MagicMock())
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
    mcp_mod._SESSION_MANAGER.get_or_create_session = AsyncMock(return_value=DummySession())
    mcp_mod._CONFIG_MANAGER.get_worker_config = AsyncMock(return_value=MagicMock())
    res = await mcp_mod.table_schemas("worker", ["t1"])
    assert isinstance(res, list)
    assert res[0]["success"] is True
    assert res[0]["table"] == "t1"
    assert res[0]["schema"] == [{"name": "t1", "type": "int"}]

@pytest.mark.asyncio
async def test_table_schemas_all_tables(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_SESSION_MANAGER", MagicMock())
    monkeypatch.setattr(mcp_mod, "_CONFIG_MANAGER", MagicMock())
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
    mcp_mod._SESSION_MANAGER.get_or_create_session = AsyncMock(return_value=DummySession())
    mcp_mod._CONFIG_MANAGER.get_worker_config = AsyncMock(return_value=MagicMock())
    res = await mcp_mod.table_schemas("worker", None)
    assert isinstance(res, list)
    assert len(res) == 2
    for i, tname in enumerate(["t1", "t2"]):
        assert res[i]["success"] is True
        assert res[i]["table"] == tname
        assert res[i]["schema"] == [{"name": tname, "type": "int"}]

@pytest.mark.asyncio
async def test_table_schemas_schema_key_error(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_SESSION_MANAGER", MagicMock())
    monkeypatch.setattr(mcp_mod, "_CONFIG_MANAGER", MagicMock())
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
    mcp_mod._SESSION_MANAGER.get_or_create_session = AsyncMock(return_value=DummySession())
    mcp_mod._CONFIG_MANAGER.get_worker_config = AsyncMock(return_value=MagicMock())
    res = await mcp_mod.table_schemas("worker", ["t1"])
    assert isinstance(res, list)
    assert res[0]["success"] is False
    assert res[0]["isError"] is True
    assert "Name" in res[0]["error"]

@pytest.mark.asyncio
async def test_table_schemas_session_error(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_SESSION_MANAGER", MagicMock())
    monkeypatch.setattr(mcp_mod, "_CONFIG_MANAGER", MagicMock())
    mcp_mod._SESSION_MANAGER.get_or_create_session = AsyncMock(side_effect=Exception("fail"))
    mcp_mod._CONFIG_MANAGER.get_worker_config = AsyncMock(return_value=MagicMock())
    res = await mcp_mod.table_schemas("worker", ["t1"])
    assert isinstance(res, list)
    assert res[0]["success"] is False
    assert res[0]["isError"] is True
    assert "fail" in res[0]["error"]

# === run_script ===
@pytest.mark.asyncio
async def test_run_script_success(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_SESSION_MANAGER", MagicMock())
    monkeypatch.setattr(mcp_mod, "_CONFIG_MANAGER", MagicMock())
    class DummySession:
        def run_script(self, script):
            DummySession.called = script
    mcp_mod._SESSION_MANAGER.get_or_create_session = AsyncMock(return_value=DummySession())
    mcp_mod._CONFIG_MANAGER.get_worker_config = AsyncMock(return_value=MagicMock())
    res = await mcp_mod.run_script("worker", script="print(1)")
    assert res["success"] is True
    assert DummySession.called == "print(1)"

@pytest.mark.asyncio
async def test_run_script_no_script(monkeypatch):
    res = await mcp_mod.run_script("worker")
    assert res["success"] is False
    assert res["isError"] is True
    assert "Must provide" in res["error"]

@pytest.mark.asyncio
async def test_run_script_session_error(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_SESSION_MANAGER", MagicMock())
    monkeypatch.setattr(mcp_mod, "_CONFIG_MANAGER", MagicMock())
    mcp_mod._SESSION_MANAGER.get_or_create_session = AsyncMock(side_effect=Exception("fail"))
    mcp_mod._CONFIG_MANAGER.get_worker_config = AsyncMock(return_value=MagicMock())
    res = await mcp_mod.run_script("worker", script="print(1)")
    assert res["success"] is False
    assert res["isError"] is True
    assert "fail" in res["error"]

@pytest.mark.asyncio
async def test_run_script_script_path(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_SESSION_MANAGER", MagicMock())
    monkeypatch.setattr(mcp_mod, "_CONFIG_MANAGER", MagicMock())
    class DummySession:
        def run_script(self, script):
            DummySession.called = script
    mcp_mod._SESSION_MANAGER.get_or_create_session = AsyncMock(return_value=DummySession())
    mcp_mod._CONFIG_MANAGER.get_worker_config = AsyncMock(return_value=MagicMock())
    with patch("aiofiles.open", new_callable=MagicMock) as aio_open:
        mock_file = aio_open.return_value.__aenter__.return_value
        mock_file.read = AsyncMock(return_value="print(123)")
        res = await mcp_mod.run_script("worker", script_path="myscript.py")
    assert res["success"] is True
    assert DummySession.called == "print(123)"

@pytest.mark.asyncio
async def test_run_script_script_path_error(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_SESSION_MANAGER", MagicMock())
    monkeypatch.setattr(mcp_mod, "_CONFIG_MANAGER", MagicMock())
    mcp_mod._SESSION_MANAGER.get_or_create_session = AsyncMock(return_value=MagicMock(run_script=AsyncMock()))
    mcp_mod._CONFIG_MANAGER.get_worker_config = AsyncMock(return_value=MagicMock())
    with patch("aiofiles.open", new_callable=MagicMock) as aio_open:
        mock_file = aio_open.return_value.__aenter__.return_value
        mock_file.read = AsyncMock(side_effect=FileNotFoundError("fail"))
        res = await mcp_mod.run_script("worker", script_path="notfound.py")
    assert res["success"] is False
    assert res["isError"] is True
    assert "fail" in res["error"]
