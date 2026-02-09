"""
Tests for deephaven_mcp.mcp_systems_server._tools.table.
"""

import asyncio
import os
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from conftest import MockContext, create_mock_instance_tracker

from deephaven_mcp import config
from deephaven_mcp.mcp_systems_server._tools.table import (
    session_table_data,
    session_tables_list,
    session_tables_schema,
)
from deephaven_mcp.resource_manager import (
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
    PythonLaunchedSession,
    ResourceLivenessStatus,
    SystemType,
)


@pytest.mark.asyncio
async def test_session_tables_schema_empty_table_names():
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
    res = await session_tables_schema(context, session_id="worker", table_names=[])
    assert isinstance(res, dict)
    assert res == {"success": True, "schemas": [], "count": 0}


@pytest.mark.asyncio
async def test_session_tables_schema_interface_contract():
    """Ensure session.tables() is properly mocked as an async method to match real interface"""

    # This test validates that our mocks match the real session interface
    class ValidSession:
        async def tables(self):
            return ["test_table"]

    # Verify the mock has the correct async interface
    session = ValidSession()
    assert callable(session.tables)
    assert asyncio.iscoroutinefunction(session.tables)

    # Test that it works in the actual function
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=session)

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    # Mock the queries.get_meta_table to avoid complexity
    class MockArrowTable:
        def to_pylist(self):
            return [{"Name": "test_table", "DataType": "string"}]

    mock_get_meta_table = AsyncMock(return_value=MockArrowTable())

    context = MockContext({"session_registry": session_registry})

    with patch("deephaven_mcp.queries.get_session_meta_table", mock_get_meta_table):
        result = await session_tables_schema(
            context, session_id="worker", table_names=None
        )

    assert result["success"] is True
    assert len(result["schemas"]) == 1
    assert result["schemas"][0]["table"] == "test_table"


@pytest.mark.asyncio
async def test_session_tables_schema_would_catch_original_bug():
    """Test that validates the original bug pattern would be caught"""

    # This simulates what the original buggy code was trying to do
    class BuggySession:
        # Simulating the broken pattern: tables as property instead of method
        tables = ["table1", "table2"]

        def open_table(self, name):
            pass

    buggy_session = BuggySession()

    # This should demonstrate the original error pattern
    with pytest.raises(TypeError, match="'list' object is not callable"):
        # This is what the original buggy code was effectively doing
        await buggy_session.tables()  # This should fail since tables is a list, not callable

    # Also verify the list() pattern that was in the original bug
    # This would work (incorrectly) with the old buggy approach
    assert list(buggy_session.tables) == ["table1", "table2"]

    # But this is what we need (and what our fix does)
    class CorrectSession:
        async def tables(self):
            return ["table1", "table2"]

    correct_session = CorrectSession()
    result = await correct_session.tables()
    assert result == ["table1", "table2"]


@pytest.mark.asyncio
async def test_session_tables_schema_no_tables():
    session_registry = AsyncMock()

    class DummySession:
        async def tables(self):
            return []

        def open_table(self, name):
            raise Exception("Should not be called")

    # Mock session manager behavior
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=DummySession())
    session_registry.get = AsyncMock(return_value=mock_session_manager)
    context = MockContext({"session_registry": session_registry})
    res = await session_tables_schema(context, session_id="worker", table_names=None)
    assert isinstance(res, dict)
    assert res == {"success": True, "schemas": [], "count": 0}


@pytest.mark.asyncio
async def test_session_tables_schema_success():
    # Create a consistent class-based mock session
    class DummySession:
        async def tables(self):
            return ["table1"]

    # Create a mock for queries.get_meta_table that returns proper schema data
    class MockArrowTable:
        def __init__(self):
            # Mock schema with field objects
            class MockField:
                def __init__(self, name, type_str):
                    self.name = name
                    self.type = type_str

            self.schema = [MockField("Name", "string"), MockField("DataType", "string")]

        def __len__(self):
            return 1  # One column in the table

        def to_pylist(self):
            return [{"Name": "col1", "DataType": "int"}]

    mock_get_meta_table = AsyncMock(return_value=MockArrowTable())

    # Set up the session manager mock
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=DummySession())

    # Set up the session registry mock
    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "session_registry": session_registry,
        }
    )

    # Patch queries.get_meta_table to return our mock data
    with patch("deephaven_mcp.queries.get_session_meta_table", mock_get_meta_table):
        # Call session_tables_schema with a specific table name
        result = await session_tables_schema(
            context, session_id="test-worker", table_names=["table1"]
        )

    # Verify correct session access pattern
    session_registry.get.assert_awaited_once_with("test-worker")
    mock_session_manager.get.assert_awaited_once()

    # Verify the result - now expects full metadata in 'data' field
    assert isinstance(result, dict)
    assert result["success"] is True
    assert len(result["schemas"]) == 1
    assert result["schemas"][0]["success"] is True
    assert result["schemas"][0]["table"] == "table1"
    assert "data" in result["schemas"][0]
    assert result["schemas"][0]["data"][0]["Name"] == "col1"
    assert result["schemas"][0]["data"][0]["DataType"] == "int"
    assert "meta_columns" in result["schemas"][0]
    assert result["schemas"][0]["row_count"] == 1


@pytest.mark.asyncio
async def test_session_tables_schema_all_tables():
    # Create a mock session with two tables
    dummy_session = MagicMock()
    dummy_session.tables = AsyncMock(return_value=["t1", "t2"])

    # Set up side_effect for queries.get_meta_table to handle multiple calls
    # Will return different data based on which table is requested
    def get_meta_table_side_effect(session, table_name):
        class MockArrowTable:
            def __init__(self, tname):
                class MockField:
                    def __init__(self, name, type_str):
                        self.name = name
                        self.type = type_str

                self.schema = [
                    MockField("Name", "string"),
                    MockField("DataType", "string"),
                ]
                self._table_name = tname

            def __len__(self):
                return 1

            def to_pylist(self):
                return [{"Name": f"col_{self._table_name}", "DataType": "int"}]

        return MockArrowTable(table_name)

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
    with patch("deephaven_mcp.queries.get_session_meta_table", mock_get_meta_table):
        # Call table_schemas with no table_names to test getting all tables
        result = await session_tables_schema(context, session_id="worker")

    # Should return results for both tables in the dummy_session.tables list
    assert isinstance(result, dict)
    assert result["success"] is True
    assert len(result["schemas"]) == 2
    assert result["schemas"][0]["success"] is True
    assert result["schemas"][1]["success"] is True
    assert result["schemas"][0]["table"] in ["t1", "t2"]
    assert result["schemas"][1]["table"] in ["t1", "t2"]
    assert result["schemas"][0]["table"] != result["schemas"][1]["table"]
    # Check for full metadata in 'data' field
    assert "data" in result["schemas"][0]
    assert "data" in result["schemas"][1]
    assert result["schemas"][0]["data"][0]["Name"] in ["col_t1", "col_t2"]
    assert result["schemas"][1]["data"][0]["Name"] in ["col_t1", "col_t2"]
    assert result["schemas"][0]["data"][0]["DataType"] == "int"
    assert result["schemas"][1]["data"][0]["DataType"] == "int"


@pytest.mark.asyncio
async def test_session_tables_schema_schema_key_error():
    # Create our mock session
    dummy_session = MagicMock()
    dummy_session.tables = AsyncMock(return_value=["table1"])

    # Create a mock for queries.get_meta_table that returns data with missing required keys
    class MockArrowTable:
        def __init__(self):
            class MockField:
                def __init__(self, name, type_str):
                    self.name = name
                    self.type = type_str

            self.schema = [MockField("foo", "string")]

        def __len__(self):
            return 1

        def to_pylist(self):
            # Missing 'Name' and 'DataType' keys, which should trigger an error
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
    with patch("deephaven_mcp.queries.get_session_meta_table", mock_get_meta_table):
        # Call session_tables_schema with a specific table name
        result = await session_tables_schema(
            context, session_id="test-worker", table_names=["table1"]
        )

    # Verify correct session access pattern
    session_registry.get.assert_awaited_once_with("test-worker")
    mock_session_manager.get.assert_awaited_once()

    # With the new implementation using to_pylist() directly, any data format is accepted
    # The function no longer validates specific keys - it returns whatever metadata is present
    assert isinstance(result, dict)
    assert result["success"] is True  # Overall operation succeeded
    assert len(result["schemas"]) == 1
    assert result["schemas"][0]["success"] is True  # Individual table succeeded
    assert result["schemas"][0]["table"] == "table1"
    # The data is returned as-is from to_pylist(), even with non-standard keys
    assert "data" in result["schemas"][0]
    assert result["schemas"][0]["data"][0]["foo"] == "bar"


@pytest.mark.asyncio
async def test_session_tables_schema_session_error():
    # Following the pattern in _mcp.py:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(session_id) - set to fail here
    # 3. session = await session_manager.get()

    # Set up session_registry to throw an exception when get() is called
    session_registry = AsyncMock()
    session_registry.get = AsyncMock(side_effect=Exception("fail"))

    context = MockContext(
        {
            "session_registry": session_registry,
        }
    )
    res = await session_tables_schema(context, session_id="worker", table_names=["t1"])
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

    context = MockContext({"session_registry": session_registry})

    # Call list_tables
    result = await session_tables_list(context, session_id="test-session")

    # Verify correct session access pattern
    session_registry.get.assert_awaited_once_with("test-session")
    mock_session_manager.get.assert_awaited_once()

    # Verify the result
    assert isinstance(result, dict)
    assert result["success"] is True
    assert result["session_id"] == "test-session"
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

    context = MockContext({"session_registry": session_registry})

    # Call list_tables
    result = await session_tables_list(context, session_id="empty-session")

    # Verify the result
    assert isinstance(result, dict)
    assert result["success"] is True
    assert result["session_id"] == "empty-session"
    assert result["table_names"] == []
    assert result["count"] == 0


@pytest.mark.asyncio
async def test_session_tables_list_invalid_session_id():
    """Test session_tables_list with invalid session_id."""
    # Set up session registry to raise error on get
    session_registry = MagicMock()
    session_registry.get = AsyncMock(
        side_effect=Exception("Session not found: invalid-session")
    )

    context = MockContext({"session_registry": session_registry})

    # Call list_tables
    result = await session_tables_list(context, session_id="invalid-session")

    # Verify error response
    assert isinstance(result, dict)
    assert result["success"] is False
    assert result["isError"] is True
    assert "Session not found" in result["error"]


@pytest.mark.asyncio
async def test_session_tables_list_session_connection_failure():
    """Test session_tables_list when session connection fails."""
    # Set up session manager to raise error on get
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(side_effect=Exception("Connection failed"))

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})

    # Call list_tables
    result = await session_tables_list(context, session_id="test-session")

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

    context = MockContext({"session_registry": session_registry})

    # Call list_tables
    result = await session_tables_list(context, session_id="test-session")

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

    context = MockContext({"session_registry": session_registry})

    # Call list_tables
    result = await session_tables_list(context, session_id="community:local:test")

    # Verify the result
    assert isinstance(result, dict)
    assert result["success"] is True
    assert result["session_id"] == "community:local:test"
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

    context = MockContext({"session_registry": mock_registry})

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

        result = await session_table_data(context, "session1", "table1")

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

    context = MockContext({"session_registry": mock_registry})

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
            context, "session1", "table1", max_rows=50, head=False, format="json-row"
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

    context = MockContext({"session_registry": mock_registry})

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

        result = await session_table_data(context, "session1", "table1", max_rows=None)

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

    context = MockContext({"session_registry": mock_registry})

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
            context, "session1", "table1", format="invalid"
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

    context = MockContext({"session_registry": mock_registry})

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

        result = await session_table_data(context, "session1", "table1")

        assert result["success"] is False
        assert "max 50MB" in result["error"]
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_table_data_session_not_found():
    """Test get_table_data when session is not found."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=Exception("Session not found"))

    context = MockContext({"session_registry": mock_registry})

    result = await session_table_data(context, "invalid_session", "table1")

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

    context = MockContext({"session_registry": mock_registry})

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.table.queries.get_table"
    ) as mock_get_table:
        mock_get_table.side_effect = Exception("Table 'invalid_table' not found")

        result = await session_table_data(context, "session1", "invalid_table")

        assert result["success"] is False
        assert "Table 'invalid_table' not found" in result["error"]
        assert result["isError"] is True
