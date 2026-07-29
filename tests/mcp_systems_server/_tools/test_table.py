"""
Tests for deephaven_mcp.mcp_systems_server._tools.table.
"""

import os
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

from deephaven_mcp import config
from deephaven_mcp.mcp_systems_server._tools.table import (
    session_table_data,
    session_table_schema,
    session_tables_list,
)
from deephaven_mcp.resource_manager import (
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
    PythonLaunchedSession,
    QualifiedSessionId,
    ResourceLivenessStatus,
    SystemType,
)

from ._helpers import (
    MockContext,
    create_mock_instance_tracker,
)


class _MockMetaTable:
    """Minimal stand-in for a PyArrow meta table."""

    def __init__(self, rows):
        self._rows = rows

    def to_pylist(self):
        return self._rows


def _schema_context(session):
    """Build a MockContext whose registry resolves to ``session``."""
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=session)
    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)
    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": session_registry,
        }
    )
    return context, session_registry, mock_session_manager


@pytest.mark.asyncio
async def test_session_table_schema_success():
    """Lean schema: name/type per column, no meta boilerplate."""
    context, session_registry, mock_session_manager = _schema_context(MagicMock())
    mock_get_meta_table = AsyncMock(
        return_value=_MockMetaTable(
            [
                {
                    "Name": "Symbol",
                    "DataType": "java.lang.String",
                    "ColumnType": "Normal",
                },
                {"Name": "Price", "DataType": "double", "ColumnType": "Normal"},
            ]
        )
    )

    with patch("deephaven_mcp.queries.get_session_meta_table", mock_get_meta_table):
        result = await session_table_schema(
            context, id="community:community:test-worker", table_name="trades"
        )

    session_registry.get.assert_awaited_once_with(
        QualifiedSessionId.from_str("community:community:test-worker")
    )
    mock_session_manager.get.assert_awaited_once()

    assert result == {
        "success": True,
        "id": "community:community:test-worker",
        "table_name": "trades",
        "schema": [
            {"name": "Symbol", "type": "java.lang.String"},
            {"name": "Price", "type": "double"},
        ],
        "column_count": 2,
    }


@pytest.mark.asyncio
async def test_session_table_schema_sparse_column_properties():
    """column_type is present only when meaningful."""
    context, _, _ = _schema_context(MagicMock())
    mock_get_meta_table = AsyncMock(
        return_value=_MockMetaTable(
            [
                {
                    "Name": "Date",
                    "DataType": "java.lang.String",
                    "ColumnType": "Partitioning",
                },
                {
                    "Name": "Prices",
                    "DataType": "double[]",
                    "ColumnType": "Normal",
                },
                {
                    "Name": "Price",
                    "DataType": "double",
                    "ColumnType": "Normal",
                },
            ]
        )
    )

    with patch("deephaven_mcp.queries.get_session_meta_table", mock_get_meta_table):
        result = await session_table_schema(
            context, id="community:community:worker", table_name="t"
        )

    assert result["success"] is True
    assert result["schema"] == [
        {"name": "Date", "type": "java.lang.String", "column_type": "Partitioning"},
        {"name": "Prices", "type": "double[]"},
        {"name": "Price", "type": "double"},
    ]


@pytest.mark.asyncio
async def test_session_table_schema_malformed_meta_table():
    """A meta table missing Name/DataType columns yields an error response."""
    context, _, _ = _schema_context(MagicMock())
    mock_get_meta_table = AsyncMock(return_value=_MockMetaTable([{"foo": "bar"}]))

    with patch("deephaven_mcp.queries.get_session_meta_table", mock_get_meta_table):
        result = await session_table_schema(
            context, id="community:community:worker", table_name="t"
        )

    assert result["success"] is False
    assert result["isError"] is True
    assert "t" in result["error"]


@pytest.mark.asyncio
async def test_session_table_schema_table_error():
    """A meta-table fetch failure yields an error response naming the table."""
    context, _, _ = _schema_context(MagicMock())
    mock_get_meta_table = AsyncMock(side_effect=Exception("no such table"))

    with patch("deephaven_mcp.queries.get_session_meta_table", mock_get_meta_table):
        result = await session_table_schema(
            context, id="community:community:worker", table_name="missing"
        )

    assert result["success"] is False
    assert result["isError"] is True
    assert "missing" in result["error"]
    assert "no such table" in result["error"]


@pytest.mark.asyncio
async def test_session_table_schema_session_error():
    """A registry lookup failure yields an error response."""
    session_registry = AsyncMock()
    session_registry.get = AsyncMock(side_effect=Exception("fail"))

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": session_registry,
        }
    )
    res = await session_table_schema(
        context, id="community:community:worker", table_name="t1"
    )
    assert isinstance(res, dict)
    assert res["success"] is False
    assert res["isError"] is True
    assert "fail" in res["error"]


@pytest.mark.asyncio
async def test_session_tables_list_success_multiple_tables():
    """Test session_tables_list with multiple tables."""

    # Create a mock session with multiple tables
    class DummySession:
        async def tables(self):
            return ["trades", "quotes", "orders"]

    # Set up session manager and registry mocks
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=DummySession())

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": session_registry,
        }
    )

    # Call list_tables
    result = await session_tables_list(context, id="community:community:test-session")

    # Verify correct session access pattern
    session_registry.get.assert_awaited_once_with(
        QualifiedSessionId.from_str("community:community:test-session")
    )
    mock_session_manager.get.assert_awaited_once()

    # Verify the result
    assert isinstance(result, dict)
    assert result["success"] is True
    assert result["id"] == "community:community:test-session"
    assert result["table_names"] == ["trades", "quotes", "orders"]
    assert result["count"] == 3


@pytest.mark.asyncio
async def test_session_tables_list_success_empty_session():
    """Test session_tables_list with no tables in session."""

    # Create a mock session with no tables
    class DummySession:
        async def tables(self):
            return []

    # Set up session manager and registry mocks
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=DummySession())

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": session_registry,
        }
    )

    # Call list_tables
    result = await session_tables_list(context, id="community:community:empty-session")

    # Verify the result
    assert isinstance(result, dict)
    assert result["success"] is True
    assert result["id"] == "community:community:empty-session"
    assert result["table_names"] == []
    assert result["count"] == 0


@pytest.mark.asyncio
async def test_session_tables_list_invalid_session_id():
    """Test session_tables_list with invalid id."""
    # Set up session registry to raise error on get
    session_registry = MagicMock()
    session_registry.get = AsyncMock(
        side_effect=Exception("Session not found: invalid-session")
    )

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": session_registry,
        }
    )

    # Call list_tables
    # Bare-name id fails at the QualifiedSessionId boundary check
    # before the registry mock is ever consulted; the structural error wins.
    result = await session_tables_list(context, id="invalid-session")

    # Verify error response
    assert isinstance(result, dict)
    assert result["success"] is False
    assert result["isError"] is True
    assert "Invalid session id" in result["error"]


@pytest.mark.asyncio
async def test_session_tables_list_session_connection_failure():
    """Test session_tables_list when session connection fails."""
    # Set up session manager to raise error on get
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(side_effect=Exception("Connection failed"))

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": session_registry,
        }
    )

    # Call list_tables
    result = await session_tables_list(context, id="community:community:test-session")

    # Verify error response
    assert isinstance(result, dict)
    assert result["success"] is False
    assert result["isError"] is True
    assert "Connection failed" in result["error"]


@pytest.mark.asyncio
async def test_session_tables_list_session_tables_method_failure():
    """Test session_tables_list when session.tables() method fails."""

    # Create a mock session where tables() raises an error
    class DummySession:
        async def tables(self):
            raise Exception("Failed to retrieve table list")

    # Set up session manager and registry mocks
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=DummySession())

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": session_registry,
        }
    )

    # Call list_tables
    result = await session_tables_list(context, id="community:community:test-session")

    # Verify error response
    assert isinstance(result, dict)
    assert result["success"] is False
    assert result["isError"] is True
    assert "Failed to retrieve table list" in result["error"]


@pytest.mark.asyncio
async def test_session_tables_list_community_session():
    """Test session_tables_list works with community sessions."""

    # Create a mock community session
    class CommunitySession:
        async def tables(self):
            return ["table1", "table2"]

    # Set up session manager and registry mocks
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=CommunitySession())

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": session_registry,
        }
    )

    # Call list_tables
    result = await session_tables_list(context, id="community:local:test")

    # Verify the result
    assert isinstance(result, dict)
    assert result["success"] is True
    assert result["id"] == "community:local:test"
    assert result["table_names"] == ["table1", "table2"]
    assert result["count"] == 2


@pytest.mark.asyncio
async def test_session_table_data_success_default_params():
    """Test get_table_data with default parameters."""
    # Mock context and session registry
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    # Mock arrow table (small size to trigger markdown-kv format)
    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=50)  # Small size -> markdown-kv
    mock_field1 = MagicMock()
    mock_field1.name = "col1"
    mock_field1.type = "int64"
    mock_field2 = MagicMock()
    mock_field2.name = "col2"
    mock_field2.type = "string"
    mock_arrow_table.schema = [mock_field1, mock_field2]
    mock_arrow_table.column_names = ["col1", "col2"]

    # Mock batch for formatters
    mock_batch = MagicMock()
    mock_batch.to_pylist.return_value = [
        {"col1": 1, "col2": "a"},
        {"col1": 2, "col2": "b"},
        {"col1": 3, "col2": "c"},
    ]
    mock_arrow_table.to_batches.return_value = [mock_batch]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.table.queries.get_table"
    ) as mock_get_table:
        mock_get_table.return_value = (mock_arrow_table, True)

        result = await session_table_data(context, "community:community:42", "table1")

        assert result["success"] is True
        assert result["table_name"] == "table1"
        assert (
            result["format"] == "markdown-table"
        )  # Default format changed to markdown-table
        assert result["row_count"] == 50
        assert result["is_complete"] is True
        assert "schema" in result
        assert "data" in result
        assert isinstance(result["data"], str)  # markdown-table returns string
        assert "|" in result["data"]  # markdown-table uses pipe delimiters

        # Verify queries.get_table was called with correct parameters
        mock_get_table.assert_called_once_with(
            mock_session, "table1", max_rows=1000, head=True
        )


@pytest.mark.asyncio
async def test_session_table_data_success_custom_params():
    """Test get_table_data with custom parameters."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    # Mock arrow table
    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=50)
    mock_field = MagicMock()
    mock_field.name = "col1"
    mock_field.type = "int64"
    mock_arrow_table.schema = [mock_field]
    mock_arrow_table.to_pylist.return_value = [{"col1": 1}, {"col1": 2}]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.table.queries.get_table"
    ) as mock_get_table:
        mock_get_table.return_value = (mock_arrow_table, False)

        result = await session_table_data(
            context,
            "community:community:42",
            "table1",
            max_rows=50,
            head=False,
            format="json-row",
        )

        assert result["success"] is True
        assert result["format"] == "json-row"
        assert result["is_complete"] is False

        mock_get_table.assert_called_once_with(
            mock_session, "table1", max_rows=50, head=False
        )


@pytest.mark.asyncio
async def test_session_table_data_success_full_table():
    """Test get_table_data with max_rows=None for full table."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    # Mock arrow table (large size to trigger CSV format)
    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=15000)  # >10000 rows -> csv
    mock_field = MagicMock()
    mock_field.name = "col1"
    mock_field.type = "int64"
    mock_arrow_table.schema = [mock_field]

    # Mock CSV output for large table
    with (
        patch("deephaven_mcp.formatters._csv.io.BytesIO") as mock_bytesio,
        patch("deephaven_mcp.formatters._csv.csv.write_csv") as mock_write_csv,
        patch(
            "deephaven_mcp.mcp_systems_server._tools.table.queries.get_table"
        ) as mock_get_table,
    ):

        mock_output = MagicMock()
        mock_output.getvalue.return_value = b"col1\n1\n2\n3"
        mock_bytesio.return_value = mock_output

        mock_get_table.return_value = (mock_arrow_table, True)

        result = await session_table_data(
            context, "community:community:42", "table1", max_rows=None
        )

        assert result["success"] is True
        assert (
            result["format"] == "markdown-table"
        )  # Default format is now markdown-table
        assert result["is_complete"] is True

        mock_get_table.assert_called_once_with(
            mock_session, "table1", max_rows=None, head=True
        )


@pytest.mark.asyncio
async def test_session_table_data_invalid_format():
    """Test get_table_data with invalid format parameter."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    # Mock arrow table
    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=10)
    mock_field = MagicMock()
    mock_field.name = "col1"
    mock_field.type = "int64"
    mock_arrow_table.schema = [mock_field]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.table.queries.get_table"
    ) as mock_get_table:
        mock_get_table.return_value = (mock_arrow_table, True)

        result = await session_table_data(
            context, "community:community:42", "table1", format="invalid"
        )

        assert result["success"] is False
        assert "Invalid format 'invalid'" in result["error"]
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_table_data_size_limit_exceeded():
    """Test get_table_data when response size exceeds limit."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    # Test values to trigger size limit (large table * many columns = large estimated size)
    large_row_count = 1_000_000  # Large number of rows to trigger size limit
    many_columns = 100  # Large number of columns to trigger size limit

    # Mock arrow table with large estimated size
    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=large_row_count)
    mock_arrow_table.schema = [MagicMock() for _ in range(many_columns)]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.table.queries.get_table"
    ) as mock_get_table:
        mock_get_table.return_value = (mock_arrow_table, True)

        result = await session_table_data(context, "community:community:42", "table1")

        assert result["success"] is False
        assert "max 52MB" in result["error"]
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_table_data_session_not_found():
    """Test get_table_data when session is not found."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=Exception("Session not found"))

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    result = await session_table_data(
        context, "community:community:invalid_session", "table1"
    )

    assert result["success"] is False
    assert "Session not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_table_data_table_not_found():
    """Test get_table_data when table is not found."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.table.queries.get_table"
    ) as mock_get_table:
        mock_get_table.side_effect = Exception("Table 'invalid_table' not found")

        result = await session_table_data(
            context, "community:community:session1", "invalid_table"
        )

        assert result["success"] is False
        assert "Table 'invalid_table' not found" in result["error"]
        assert result["isError"] is True


def test_register_tools_registers_table_tools():
    """register_tools() registers all table tools."""
    from mcp.server.fastmcp import FastMCP

    from deephaven_mcp.mcp_systems_server._tools.table import register_tools

    server = FastMCP("test-table-server")
    register_tools(server)
    tools = server._tool_manager._tools
    assert "session_table_schema" in tools
    assert "session_tables_list" in tools
    assert "session_table_data" in tools
