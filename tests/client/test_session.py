import asyncio
import sys
import types
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

# Patch sys.modules for enterprise imports BEFORE any tested imports
mock_enterprise = types.ModuleType("deephaven_enterprise")
mock_sm = types.ModuleType("deephaven_enterprise.client.session_manager")
mock_sm.SessionManager = MagicMock()
sys.modules["deephaven_enterprise"] = mock_enterprise
sys.modules["deephaven_enterprise.client"] = types.ModuleType(
    "deephaven_enterprise.client"
)
sys.modules["deephaven_enterprise.client.session_manager"] = mock_sm
# Patch controller client as well for _protobuf.py import
mock_controller = types.ModuleType("deephaven_enterprise.client.controller")
mock_controller.ControllerClient = MagicMock()
sys.modules["deephaven_enterprise.client.controller"] = mock_controller

# Patch pydeephaven Table, InputTable, and Query with dummy types for isinstance checks
import types as _types

mock_table_mod = _types.ModuleType("pydeephaven.table")
mock_query_mod = _types.ModuleType("pydeephaven.query")


class DummyTable:
    pass


class DummyInputTable:
    pass


class DummyQuery:
    pass


mock_table_mod.Table = DummyTable
mock_table_mod.InputTable = DummyInputTable
mock_query_mod.Query = DummyQuery
sys.modules["pydeephaven.table"] = mock_table_mod
sys.modules["pydeephaven.query"] = mock_query_mod
from pydeephaven.query import Query
from pydeephaven.table import InputTable, Table

from deephaven_mcp._exceptions import (
    DeephavenConnectionError,
    QueryError,
    ResourceError,
    SessionError,
)
from deephaven_mcp.client._session import CoreSession


class DummySession:
    def close(self):
        pass

    def is_alive(self):
        return True

    def tables(self):
        return ["foo", "bar"]

    def open_table(self, name):
        if name == "missing":
            raise KeyError("not found")
        if name == "conn":
            raise ConnectionError("fail")
        if name == "exc":
            raise Exception("fail")
        return Table()

    def empty_table(self, size):
        if size < 0:
            raise Exception("fail")
        if size == 42:
            raise ConnectionError("fail")
        return Table()

    def import_table(self, data):
        if data == "bad":
            raise Exception("fail")
        if data == "conn":
            raise ConnectionError("fail")
        return Table()

    def input_table(self, schema, init_table, key_cols, blink_table):
        if schema == "bad":
            raise ValueError("fail")
        if schema == "conn":
            raise ConnectionError("fail")
        if schema == "exc":
            raise Exception("fail")
        return InputTable()

    def bind_table(self, name, table):
        if name == "conn":
            raise ConnectionError("fail")
        if name == "exc":
            raise Exception("fail")

    def query(self, table):
        if table == "conn":
            raise ConnectionError("fail")
        if table == "exc":
            raise Exception("fail")
        return Query()


@pytest.fixture
def core_session():
    return CoreSession(DummySession())


@pytest.mark.asyncio
async def test_close_success(core_session):
    await core_session.close()


@pytest.mark.asyncio
async def test_close_connection_error(core_session):
    core_session.wrapped.close = MagicMock(side_effect=ConnectionError("fail"))
    with pytest.raises(DeephavenConnectionError):
        await core_session.close()


@pytest.mark.asyncio
async def test_close_other_error(core_session):
    core_session.wrapped.close = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(SessionError):
        await core_session.close()


@pytest.mark.asyncio
async def test_is_alive_success(core_session):
    assert await core_session.is_alive() is True


@pytest.mark.asyncio
async def test_is_alive_connection_error(core_session):
    core_session.wrapped.is_alive = MagicMock(side_effect=ConnectionError("fail"))
    with pytest.raises(DeephavenConnectionError):
        await core_session.is_alive()


@pytest.mark.asyncio
async def test_is_alive_other_error(core_session):
    core_session.wrapped.is_alive = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(SessionError):
        await core_session.is_alive()


@pytest.mark.asyncio
async def test_tables_success(core_session):
    assert await core_session.tables() == ["foo", "bar"]


@pytest.mark.asyncio
async def test_tables_connection_error(core_session):
    core_session.wrapped.tables = MagicMock(side_effect=ConnectionError("fail"))
    with pytest.raises(DeephavenConnectionError):
        await core_session.tables()


@pytest.mark.asyncio
async def test_tables_other_error(core_session):
    core_session.wrapped.tables = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(QueryError):
        await core_session.tables()


@pytest.mark.asyncio
async def test_open_table_success(core_session):
    assert isinstance(await core_session.open_table("foo"), Table)


@pytest.mark.asyncio
async def test_open_table_resource_error(core_session):
    with pytest.raises(ResourceError):
        await core_session.open_table("missing")


@pytest.mark.asyncio
async def test_open_table_connection_error(core_session):
    with pytest.raises(DeephavenConnectionError):
        await core_session.open_table("conn")


@pytest.mark.asyncio
async def test_open_table_other_error(core_session):
    with pytest.raises(QueryError):
        await core_session.open_table("exc")


@pytest.mark.asyncio
async def test_empty_table_success(core_session):
    assert isinstance(await core_session.empty_table(1), Table)


@pytest.mark.asyncio
async def test_empty_table_connection_error(core_session):
    with pytest.raises(DeephavenConnectionError):
        await core_session.empty_table(42)


@pytest.mark.asyncio
async def test_empty_table_other_error(core_session):
    with pytest.raises(QueryError):
        await core_session.empty_table(-1)


@pytest.mark.asyncio
async def test_import_table_success(core_session):
    assert isinstance(await core_session.import_table(pa.table({"a": [1]})), Table)


@pytest.mark.asyncio
async def test_import_table_connection_error(core_session):
    with pytest.raises(DeephavenConnectionError):
        await core_session.import_table("conn")


@pytest.mark.asyncio
async def test_import_table_other_error(core_session):
    with pytest.raises(QueryError):
        await core_session.import_table("bad")


@pytest.mark.asyncio
async def test_input_table_success(core_session):
    assert isinstance(await core_session.input_table(), InputTable)


@pytest.mark.asyncio
async def test_input_table_value_error(core_session):
    with pytest.raises(ValueError):
        await core_session.input_table(schema="bad")


@pytest.mark.asyncio
async def test_input_table_connection_error(core_session):
    with pytest.raises(DeephavenConnectionError):
        await core_session.input_table(schema="conn")


@pytest.mark.asyncio
async def test_input_table_other_error(core_session):
    with pytest.raises(QueryError):
        await core_session.input_table(schema="exc")


@pytest.mark.asyncio
async def test_bind_table_success(core_session):
    await core_session.bind_table("foo", Table())


@pytest.mark.asyncio
async def test_bind_table_connection_error(core_session):
    with pytest.raises(DeephavenConnectionError):
        await core_session.bind_table("conn", Table())


@pytest.mark.asyncio
async def test_bind_table_other_error(core_session):
    with pytest.raises(QueryError):
        await core_session.bind_table("exc", Table())


@pytest.mark.asyncio
async def test_query_success(core_session):
    assert isinstance(await core_session.query(Table()), Query)


@pytest.mark.asyncio
async def test_query_connection_error(core_session):
    with pytest.raises(DeephavenConnectionError):
        await core_session.query("conn")


@pytest.mark.asyncio
async def test_query_other_error(core_session):
    with pytest.raises(QueryError):
        await core_session.query("exc")


# __str__
def test_str(core_session):
    assert str(core_session) == str(core_session.wrapped)


def test_str_dunder_direct(core_session):
    # Directly call the dunder method to force coverage
    assert core_session.__str__() == str(core_session.wrapped)


# Minimal, non-mocked test to ensure coverage for __str__ and __repr__
class DummyStrRepr:
    def __str__(self):
        return "dummy-str"

    def __repr__(self):
        return "dummy-repr"


def test_str_minimal():
    cs = CoreSession(DummyStrRepr())
    assert str(cs) == "dummy-str"
    assert cs.__str__() == "dummy-str"


def test_repr_minimal():
    cs = CoreSession(DummyStrRepr())
    assert repr(cs) == "dummy-repr"
    assert cs.__repr__() == "dummy-repr"


# ========== NEW TESTS FOR UNCOVERED LINES BELOW ========== #


@pytest.mark.asyncio
async def test_time_table_success(core_session):
    core_session.wrapped.time_table = MagicMock(return_value=Table())
    assert isinstance(await core_session.time_table("PT1S"), Table)


@pytest.mark.asyncio
async def test_time_table_connection_error(core_session):
    core_session.wrapped.time_table = MagicMock(side_effect=ConnectionError("fail"))
    with pytest.raises(DeephavenConnectionError):
        await core_session.time_table("PT1S")


@pytest.mark.asyncio
async def test_time_table_other_error(core_session):
    core_session.wrapped.time_table = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(QueryError):
        await core_session.time_table("PT1S")


@pytest.mark.asyncio
async def test_merge_tables_success(core_session):
    core_session.wrapped.merge_tables = MagicMock(return_value=Table())
    assert isinstance(await core_session.merge_tables([Table(), Table()]), Table)


@pytest.mark.asyncio
async def test_merge_tables_connection_error(core_session):
    core_session.wrapped.merge_tables = MagicMock(side_effect=ConnectionError("fail"))
    with pytest.raises(DeephavenConnectionError):
        await core_session.merge_tables([Table()])


@pytest.mark.asyncio
async def test_merge_tables_other_error(core_session):
    core_session.wrapped.merge_tables = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(QueryError):
        await core_session.merge_tables([Table()])


@pytest.mark.asyncio
async def test_run_script_success(core_session):
    core_session.wrapped.run_script = MagicMock()
    await core_session.run_script("print('hi')")


@pytest.mark.asyncio
async def test_run_script_connection_error(core_session):
    core_session.wrapped.run_script = MagicMock(side_effect=ConnectionError("fail"))
    with pytest.raises(DeephavenConnectionError):
        await core_session.run_script("print('hi')")


@pytest.mark.asyncio
async def test_run_script_other_error(core_session):
    core_session.wrapped.run_script = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(QueryError):
        await core_session.run_script("print('hi')")


# --- Enterprise / CorePlusSession tests ---
from deephaven_mcp.client._session import CorePlusSession


class DummyDndSession:
    def pqinfo(self):
        return "pqinfo_obj"

    def historical_table(self, namespace, table_name):
        if namespace == "conn":
            raise ConnectionError("fail")
        if namespace == "missing":
            raise KeyError("not found")
        if namespace == "exc":
            raise Exception("fail")
        return Table()

    def live_table(self, namespace, table_name):
        if namespace == "conn":
            raise ConnectionError("fail")
        if namespace == "missing":
            raise KeyError("not found")
        if namespace == "exc":
            raise Exception("fail")
        return Table()

    def catalog_table(self):
        raise Exception("fail")


class DummyCorePlusQueryInfo:
    def __init__(self, obj):
        self.obj = obj


# Patch CorePlusQueryInfo in module namespace
import deephaven_mcp.client._session as session_mod

session_mod.CorePlusQueryInfo = DummyCorePlusQueryInfo


@pytest.fixture
def core_plus_session():
    cps = CorePlusSession(DummyDndSession())
    cps._session = DummyDndSession()  # patch only _session for enterprise methods
    return cps


def test_core_plus_session_init():
    cps = CorePlusSession(DummyDndSession())
    cps._session = DummyDndSession()
    assert isinstance(cps, CorePlusSession)


@pytest.mark.asyncio
async def test_pqinfo_success(core_plus_session):
    info = await core_plus_session.pqinfo()
    assert isinstance(info, DummyCorePlusQueryInfo)


@pytest.mark.asyncio
async def test_pqinfo_connection_error(core_plus_session):
    core_plus_session._session.pqinfo = MagicMock(side_effect=ConnectionError("fail"))
    with pytest.raises(DeephavenConnectionError):
        await core_plus_session.pqinfo()


@pytest.mark.asyncio
async def test_pqinfo_other_error(core_plus_session):
    core_plus_session._session.pqinfo = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(QueryError):
        await core_plus_session.pqinfo()


@pytest.mark.asyncio
async def test_historical_table_success(core_plus_session):
    table = await core_plus_session.historical_table("ns", "tbl")
    assert isinstance(table, Table)


@pytest.mark.asyncio
async def test_historical_table_connection_error(core_plus_session):
    with pytest.raises(DeephavenConnectionError):
        await core_plus_session.historical_table("conn", "tbl")


@pytest.mark.asyncio
async def test_historical_table_key_error(core_plus_session):
    with pytest.raises(ResourceError):
        await core_plus_session.historical_table("missing", "tbl")


@pytest.mark.asyncio
async def test_historical_table_other_error(core_plus_session):
    with pytest.raises(QueryError):
        await core_plus_session.historical_table("exc", "tbl")


@pytest.mark.asyncio
async def test_live_table_success(core_plus_session):
    table = await core_plus_session.live_table("ns", "tbl")
    assert isinstance(table, Table)


@pytest.mark.asyncio
async def test_live_table_connection_error(core_plus_session):
    with pytest.raises(DeephavenConnectionError):
        await core_plus_session.live_table("conn", "tbl")


@pytest.mark.asyncio
async def test_live_table_key_error(core_plus_session):
    with pytest.raises(ResourceError):
        await core_plus_session.live_table("missing", "tbl")


@pytest.mark.asyncio
async def test_live_table_other_error(core_plus_session):
    with pytest.raises(QueryError):
        await core_plus_session.live_table("exc", "tbl")


@pytest.mark.asyncio
async def test_catalog_table_connection_error(core_plus_session):
    # Patch to raise ConnectionError first
    core_plus_session._session.catalog_table = MagicMock(
        side_effect=ConnectionError("fail")
    )
    with pytest.raises(DeephavenConnectionError):
        await core_plus_session.catalog_table()
    # Patch to raise generic Exception
    core_plus_session._session.catalog_table = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(QueryError):
        await core_plus_session.catalog_table()
