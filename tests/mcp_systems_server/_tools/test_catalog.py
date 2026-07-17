"""
Tests for deephaven_mcp.mcp_systems_server._tools.catalog.
"""

import asyncio
import os
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from conftest import (
    MockContext,
    create_mock_instance_tracker,
)


# Test-specific helper functions (only used in this file)
def create_mock_arrow_meta_table(
    schema_data: list[dict], schema_fields: list | None = None
) -> MagicMock:
    """
    Create a mock PyArrow meta table for testing schema operations.

    This helper eliminates duplication across schema-related tests by providing
    a consistent way to create mock meta tables that match the behavior expected
    by format_schema_result().

    Args:
        schema_data: List of dicts representing metadata rows (e.g., column info)
        schema_fields: Optional list of mock field objects for the schema.
                      If None, creates default Name/DataType fields.

    Returns:
        MagicMock: A mock PyArrow Table with to_pylist(), __len__(), and schema
    """
    mock_arrow_meta = MagicMock()
    mock_arrow_meta.to_pylist = MagicMock(return_value=schema_data)
    mock_arrow_meta.__len__ = MagicMock(return_value=len(schema_data))

    if schema_fields:
        mock_arrow_meta.schema = schema_fields
    else:
        # Default schema fields that match typical Deephaven meta tables
        field1 = MagicMock()
        field1.name = "Name"
        field1.type = "string"

        field2 = MagicMock()
        field2.name = "DataType"
        field2.type = "string"

        mock_arrow_meta.schema = [field1, field2]

    return mock_arrow_meta


from deephaven_mcp import config
from deephaven_mcp.client import CorePlusSession
from deephaven_mcp.mcp_systems_server._tools.catalog import (
    catalog_namespaces_list,
    catalog_table_sample,
    catalog_table_schema,
    catalog_tables_list,
)
from deephaven_mcp.resource_manager import (
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
    PythonLaunchedSession,
    ResourceLivenessStatus,
    SystemType,
)


def _mock_catalog_arrow(entries, nbytes=1000):
    """Mock catalog arrow table whose Namespace/TableName selection yields ``entries``."""
    subset = MagicMock()
    subset.nbytes = nbytes
    subset.to_pylist.return_value = entries
    table = MagicMock()
    table.select.return_value = subset
    return table


def _catalog_context(session):
    """Build a MockContext whose registry resolves to ``session``."""
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=session)
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    return MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )


@pytest.mark.asyncio
async def test_catalog_tables_success_no_filters():
    """catalog_tables_list returns lean namespace/table_name entries."""
    mock_session = MagicMock(spec=CorePlusSession)
    context = _catalog_context(mock_session)
    mock_arrow = _mock_catalog_arrow(
        [
            {"Namespace": "ns1", "TableName": "t1"},
            {"Namespace": "ns2", "TableName": "t2"},
        ]
    )

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_arrow, True)

        result = await catalog_tables_list(context, "enterprise:prod:1")

        assert result == {
            "success": True,
            "id": "enterprise:prod:1",
            "tables": [
                {"namespace": "ns1", "table_name": "t1"},
                {"namespace": "ns2", "table_name": "t2"},
            ],
            "count": 2,
            "is_complete": True,
        }

        mock_get_catalog.assert_called_once_with(
            mock_session, max_rows=10000, filters=None, distinct_namespaces=False
        )
        mock_arrow.select.assert_called_once_with(["Namespace", "TableName"])


@pytest.mark.asyncio
async def test_catalog_tables_success_with_filters():
    """Filters are forwarded verbatim to the catalog query."""
    mock_session = MagicMock(spec=CorePlusSession)
    context = _catalog_context(mock_session)
    mock_arrow = _mock_catalog_arrow([{"Namespace": "market_data", "TableName": "p"}])

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_arrow, True)

        filters = ["Namespace = `market_data`", "TableName.contains(`price`)"]
        result = await catalog_tables_list(
            context, "enterprise:prod:1", filters=filters
        )

        assert result["success"] is True
        assert result["count"] == 1
        assert result["is_complete"] is True

        mock_get_catalog.assert_called_once_with(
            mock_session, max_rows=10000, filters=filters, distinct_namespaces=False
        )


@pytest.mark.asyncio
async def test_catalog_tables_incomplete_results():
    """Truncation by max_rows surfaces as is_complete=False."""
    context = _catalog_context(MagicMock(spec=CorePlusSession))
    entries = [{"Namespace": "ns", "TableName": f"t{i}"} for i in range(3)]
    mock_arrow = _mock_catalog_arrow(entries)

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_arrow, False)  # Incomplete

        result = await catalog_tables_list(context, "enterprise:prod:1", max_rows=3)

        assert result["success"] is True
        assert result["is_complete"] is False  # Truncated
        assert result["count"] == 3


@pytest.mark.asyncio
async def test_catalog_tables_not_enterprise_session():
    """A non-enterprise session is rejected before any catalog query runs."""
    context = _catalog_context(MagicMock())  # Not CorePlusSession

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        result = await catalog_tables_list(context, "community:local:2")

    assert result["success"] is False
    assert "only works with enterprise (Core+) sessions" in result["error"]
    assert result["isError"] is True
    mock_get_catalog.assert_not_called()


@pytest.mark.asyncio
async def test_catalog_tables_session_not_found():
    """Test catalog when session is not found."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=Exception("Session not found"))

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    result = await catalog_tables_list(context, "enterprise:prod:invalid_session")

    assert result["success"] is False
    assert "Session not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_tables_invalid_filter():
    """Test catalog with invalid filter syntax."""
    context = _catalog_context(MagicMock(spec=CorePlusSession))

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.side_effect = RuntimeError("Invalid filter syntax")

        result = await catalog_tables_list(
            context, "enterprise:prod:1", filters=["InvalidFilter!!!"]
        )

        assert result["success"] is False
        assert "Invalid filter syntax" in result["error"]
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_tables_size_limit_exceeded():
    """Test catalog when response size exceeds limit."""
    context = _catalog_context(MagicMock(spec=CorePlusSession))
    # Namespace/TableName subset already exceeds the 50MB limit
    mock_arrow = _mock_catalog_arrow([], nbytes=60_000_000)

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_arrow, False)

        result = await catalog_tables_list(context, "enterprise:prod:1")

        assert result["success"] is False
        assert "50MB" in result["error"] or "max" in result["error"].lower()
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_namespaces_success_no_filters():
    """Test catalog_namespaces with no filters and default max_rows."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock(spec=CorePlusSession)

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    # Mock namespaces arrow table
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.nbytes = 1000  # Small size, under limit
    namespaces_table_mock.column.return_value.to_pylist.return_value = [
        "market_data",
        "reference",
    ]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, True)

        result = await catalog_namespaces_list(context, "enterprise:prod:1")

        assert result["success"] is True
        assert result["id"] == "enterprise:prod:1"
        assert result["namespaces"] == ["market_data", "reference"]
        assert result["count"] == 2
        assert result["is_complete"] is True

        namespaces_table_mock.column.assert_called_once_with("Namespace")
        mock_get_namespaces.assert_called_once_with(
            mock_session, max_rows=1000, filters=None, distinct_namespaces=True
        )


@pytest.mark.asyncio
async def test_catalog_namespaces_success_with_filters():
    """Test catalog_namespaces with filters applied."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock(spec=CorePlusSession)

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    # Mock namespaces arrow table
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.nbytes = 500
    namespaces_table_mock.column.return_value.to_pylist.return_value = ["market_data"]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, True)

        filters = ["TableName.contains(`daily`)"]
        result = await catalog_namespaces_list(
            context, "enterprise:prod:1", filters=filters
        )

        assert result["success"] is True
        assert result["namespaces"] == ["market_data"]
        assert result["count"] == 1
        assert result["is_complete"] is True

        mock_get_namespaces.assert_called_once_with(
            mock_session, max_rows=1000, filters=filters, distinct_namespaces=True
        )


@pytest.mark.asyncio
async def test_catalog_namespaces_incomplete_results():
    """Test catalog_namespaces when results are incomplete (truncated by max_rows)."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock(spec=CorePlusSession)

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    # Mock namespaces arrow table
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.nbytes = 25000
    namespaces_table_mock.column.return_value.to_pylist.return_value = [
        f"ns_{i}" for i in range(500)
    ]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, False)  # Incomplete

        result = await catalog_namespaces_list(
            context, "enterprise:prod:1", max_rows=500
        )

        assert result["success"] is True
        assert result["is_complete"] is False  # Truncated
        assert result["count"] == 500


@pytest.mark.asyncio
async def test_catalog_namespaces_not_enterprise_session():
    """A non-enterprise session is rejected before any catalog query runs."""
    context = _catalog_context(MagicMock())  # Not CorePlusSession

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        result = await catalog_namespaces_list(context, "community:local:2")

    assert result["success"] is False
    assert "only works with enterprise (Core+) sessions" in result["error"]
    assert result["isError"] is True
    mock_get_namespaces.assert_not_called()


@pytest.mark.asyncio
async def test_catalog_namespaces_session_not_found():
    """Test catalog_namespaces when session is not found."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=Exception("Session not found"))

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    result = await catalog_namespaces_list(context, "enterprise:prod:invalid_session")

    assert result["success"] is False
    assert "Session not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_namespaces_size_limit_exceeded():
    """Test catalog_namespaces when response size exceeds limit."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock(spec=CorePlusSession)

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    # Mock namespaces arrow table with size exceeding limit
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.nbytes = 60_000_000  # 60MB, exceeds 50MB limit
    namespaces_table_mock.column.return_value.to_pylist.return_value = [
        f"ns_{i}" for i in range(100)
    ]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, False)

        result = await catalog_namespaces_list(context, "enterprise:prod:1")

        assert result["success"] is False
        assert "50MB" in result["error"] or "max" in result["error"].lower()
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_table_schema_success():
    """catalog_table_schema returns a lean schema with sparse column properties."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)
    context = _catalog_context(mock_session)

    meta_rows = [
        {"Name": "Date", "DataType": "java.lang.String", "ColumnType": "Partitioning"},
        {"Name": "Prices", "DataType": "double[]", "ColumnType": "Normal"},
        {"Name": "Price", "DataType": "double", "ColumnType": "Normal"},
    ]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_meta_table"
    ) as mock_get_schema:
        mock_get_schema.return_value = create_mock_arrow_meta_table(meta_rows)

        result = await catalog_table_schema(
            context, "enterprise:prod:1", "market_data", "daily_prices"
        )

    assert result == {
        "success": True,
        "id": "enterprise:prod:1",
        "namespace": "market_data",
        "table_name": "daily_prices",
        "schema": [
            {"name": "Date", "type": "java.lang.String", "column_type": "Partitioning"},
            {"name": "Prices", "type": "double[]"},
            {"name": "Price", "type": "double"},
        ],
        "column_count": 3,
    }
    mock_get_schema.assert_awaited_once_with(
        mock_session, "market_data", "daily_prices"
    )


@pytest.mark.asyncio
async def test_catalog_table_schema_not_enterprise_session():
    """catalog_table_schema fails with a non-enterprise session."""
    context = _catalog_context(MagicMock())  # Not CorePlusSession

    result = await catalog_table_schema(
        context, "community:local:2", "market_data", "daily_prices"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "enterprise" in result["error"].lower() or "Core+" in result["error"]


@pytest.mark.asyncio
async def test_catalog_table_schema_session_not_found():
    """catalog_table_schema surfaces session resolution failures."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=Exception("Session not found"))

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    result = await catalog_table_schema(
        context, "enterprise:prod:999", "market_data", "daily_prices"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Session not found" in result["error"]


@pytest.mark.asyncio
async def test_catalog_table_schema_retrieval_error():
    """catalog_table_schema surfaces meta-table retrieval failures."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)
    context = _catalog_context(mock_session)

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_meta_table"
    ) as mock_get_schema:
        mock_get_schema.side_effect = Exception("Table 'daily_prices' not found")

        result = await catalog_table_schema(
            context, "enterprise:prod:1", "market_data", "daily_prices"
        )

    assert result["success"] is False
    assert result["isError"] is True
    assert "daily_prices" in result["error"]


@pytest.mark.asyncio
async def test_catalog_table_sample_success():
    """Test catalog_table_sample with successful data retrieval."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    # Mock arrow table with data
    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=10)
    mock_arrow_table.schema = MagicMock()
    mock_arrow_table.schema.__len__ = MagicMock(return_value=3)
    mock_arrow_table.to_pydict = MagicMock(
        return_value={
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
            "col3": [4.5, 5.5, 6.5],
        }
    )

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table_data"
    ) as mock_get_data:
        mock_get_data.return_value = (mock_arrow_table, True)

        result = await catalog_table_sample(
            context, "enterprise:prod:1", "public", "users", max_rows=10
        )

    assert result["success"] is True
    assert result["row_count"] == 10
    assert result["is_complete"] is True
    assert (
        result["format"] == "markdown-table"
    )  # Default format changed to markdown-table
    assert result["namespace"] == "public"
    assert result["table_name"] == "users"
    assert "data" in result
    # Verify filters=None (auto-detect) is passed by default
    mock_get_data.assert_called_once()
    _, call_kwargs = mock_get_data.call_args
    assert call_kwargs["filters"] is None


@pytest.mark.asyncio
async def test_catalog_table_sample_with_format():
    """Test catalog_table_sample with different format."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=5)
    mock_arrow_table.schema = MagicMock()
    mock_arrow_table.schema.__len__ = MagicMock(return_value=2)
    mock_arrow_table.to_pydict = MagicMock(
        return_value={"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
    )

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table_data"
    ) as mock_get_data:
        mock_get_data.return_value = (mock_arrow_table, False)

        result = await catalog_table_sample(
            context,
            "enterprise:prod:1",
            "analytics",
            "events",
            max_rows=5,
            format="markdown-table",
        )

    assert result["success"] is True
    assert result["format"] == "markdown-table"
    assert result["is_complete"] is False


@pytest.mark.asyncio
async def test_catalog_table_sample_not_enterprise_session():
    """Test catalog_table_sample with non-enterprise session."""
    from deephaven_mcp.client import BaseSession

    mock_session = MagicMock(spec=BaseSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    result = await catalog_table_sample(context, "enterprise:prod:1", "public", "users")

    assert result["success"] is False
    assert "only works with enterprise (Core+) sessions" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_table_sample_exception():
    """Test catalog_table_sample with exception during data retrieval."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table_data"
    ) as mock_get_data:
        mock_get_data.side_effect = Exception("Database connection failed")

        result = await catalog_table_sample(
            context, "enterprise:prod:1", "public", "users"
        )

    assert result["success"] is False
    assert "Database connection failed" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_table_sample_response_too_large():
    """Test catalog_table_sample when response size exceeds limit."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    # Mock arrow table with huge data that exceeds size limit
    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=100000)  # 100k rows
    mock_arrow_table.schema = MagicMock()
    mock_arrow_table.schema.__len__ = MagicMock(return_value=100)  # 100 columns

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table_data"
    ) as mock_get_data:
        mock_get_data.return_value = (mock_arrow_table, True)

        result = await catalog_table_sample(
            context, "enterprise:prod:1", "public", "huge_table"
        )

    assert result["success"] is False
    assert "max 52MB" in result["error"]
    assert "reduce max_rows" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_table_sample_with_explicit_filters():
    """Test catalog_table_sample passes explicit filters to get_catalog_table_data."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=5)
    mock_arrow_table.schema = MagicMock()
    mock_arrow_table.schema.__len__ = MagicMock(return_value=2)
    mock_arrow_table.to_pydict = MagicMock(
        return_value={"Date": ["2024-01-15"], "Val": [1]}
    )

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table_data"
    ) as mock_get_data:
        mock_get_data.return_value = (mock_arrow_table, True)

        result = await catalog_table_sample(
            context,
            "enterprise:prod:1",
            "DbInternal",
            "ProcessEventLog",
            filters=["Date == `2024-01-15`"],
        )

    assert result["success"] is True
    _, call_kwargs = mock_get_data.call_args
    assert call_kwargs["filters"] == ["Date == `2024-01-15`"]


@pytest.mark.asyncio
async def test_catalog_table_sample_empty_filters_skips_autodetect():
    """Test catalog_table_sample with filters=[] passes empty list (skips auto-detect)."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    context = MockContext(
        {
            "config_manager": MagicMock(),
            "registry": mock_registry,
        }
    )

    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=0)
    mock_arrow_table.schema = MagicMock()
    mock_arrow_table.schema.__len__ = MagicMock(return_value=2)
    mock_arrow_table.to_pydict = MagicMock(return_value={})

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table_data"
    ) as mock_get_data:
        mock_get_data.return_value = (mock_arrow_table, True)

        await catalog_table_sample(
            context,
            "enterprise:prod:1",
            "DbInternal",
            "ProcessEventLog",
            filters=[],
        )

    _, call_kwargs = mock_get_data.call_args
    assert call_kwargs["filters"] == []


# ===== Tests for partition utility helpers in queries.py =====


@pytest.mark.asyncio
async def test_extract_partition_column_defs_returns_partition_cols():
    """_extract_partition_column_defs returns all IsPartitioning=True columns."""
    from deephaven_mcp.queries import _extract_partition_column_defs

    mock_table = MagicMock()
    mock_meta = MagicMock()
    mock_meta.to_pydict = MagicMock(
        return_value={
            "Name": ["Date", "Region", "Value"],
            "IsPartitioning": [True, True, False],
            "DataType": ["java.time.LocalDate", "java.lang.String", "double"],
        }
    )
    mock_table.meta_table.view.return_value.to_arrow.return_value = mock_meta

    result = await _extract_partition_column_defs(mock_table)

    assert result == [
        {"name": "Date", "type": "java.time.LocalDate"},
        {"name": "Region", "type": "java.lang.String"},
    ]
    mock_table.meta_table.view.assert_called_once_with(
        ["Name", "IsPartitioning", "DataType"]
    )


@pytest.mark.asyncio
async def test_extract_partition_column_defs_no_partition_cols():
    """_extract_partition_column_defs returns [] when no IsPartitioning=True columns."""
    from deephaven_mcp.queries import _extract_partition_column_defs

    mock_table = MagicMock()
    mock_meta = MagicMock()
    mock_meta.to_pydict = MagicMock(
        return_value={
            "Name": ["Col1", "Col2"],
            "IsPartitioning": [False, False],
            "DataType": ["int", "string"],
        }
    )
    mock_table.meta_table.view.return_value.to_arrow.return_value = mock_meta

    result = await _extract_partition_column_defs(mock_table)

    assert result == []


@pytest.mark.asyncio
async def test_extract_partition_column_defs_meta_error_propagates():
    """_extract_partition_column_defs propagates exceptions from meta_table access."""
    from deephaven_mcp.queries import _extract_partition_column_defs

    mock_table = MagicMock()
    mock_table.meta_table.view.side_effect = RuntimeError("meta table unavailable")

    with pytest.raises(RuntimeError, match="meta table unavailable"):
        await _extract_partition_column_defs(mock_table)


def test_format_partition_filter_string():
    """_format_partition_filter formats string values with backtick DQL syntax."""
    from deephaven_mcp.queries import _format_partition_filter

    result = _format_partition_filter("Date", "2024-01-15")
    assert result == "Date == `2024-01-15`"


def test_format_partition_filter_non_string_raises():
    """_format_partition_filter raises InternalError for non-string values."""
    from deephaven_mcp._exceptions import InternalError
    from deephaven_mcp.queries import _format_partition_filter

    with pytest.raises(InternalError):
        _format_partition_filter("Version", 42)


@pytest.mark.asyncio
async def test_get_distinct_column_values_descending_returns_sorted():
    """_get_distinct_column_values returns values from sorted Arrow table when descending=True."""
    from deephaven_mcp.queries import _get_distinct_column_values

    mock_table = MagicMock()
    mock_arrow = MagicMock()
    mock_arrow.__getitem__ = MagicMock(
        side_effect=lambda col: MagicMock(
            to_pylist=MagicMock(return_value=["2024-01-15", "2024-01-14", "2024-01-13"])
        )
    )
    mock_table.select_distinct.return_value.sort_descending.return_value.to_arrow.return_value = (
        mock_arrow
    )

    result = await _get_distinct_column_values(mock_table, "Date", descending=True)

    assert result == ["2024-01-15", "2024-01-14", "2024-01-13"]
    mock_table.select_distinct.assert_called_once_with("Date")
    mock_table.select_distinct.return_value.sort_descending.assert_called_once_with(
        "Date"
    )


@pytest.mark.asyncio
async def test_get_distinct_column_values_error_propagates():
    """_get_distinct_column_values propagates exceptions."""
    from deephaven_mcp.queries import _get_distinct_column_values

    mock_table = MagicMock()
    mock_table.select_distinct.side_effect = RuntimeError("column not found")

    with pytest.raises(RuntimeError, match="column not found"):
        await _get_distinct_column_values(mock_table, "Date", descending=True)


@pytest.mark.asyncio
async def test_find_recent_partition_filters_first_partition_has_data():
    """_find_recent_partition_filters returns filter for the first (most recent) partition with data."""
    from unittest.mock import patch as _patch

    from deephaven_mcp.queries import _find_recent_partition_filters

    mock_table = MagicMock()

    with (
        _patch("deephaven_mcp.queries._extract_partition_column_defs") as mock_extract,
        _patch("deephaven_mcp.queries._get_distinct_column_values") as mock_distinct,
    ):
        mock_extract.return_value = [{"name": "Date", "type": "java.time.LocalDate"}]
        mock_distinct.return_value = ["2024-01-15", "2024-01-14", "2024-01-13"]

        # First probe returns size=1 (has data)
        mock_table.where.return_value.size = 1

        result = await _find_recent_partition_filters(
            mock_table, "DbInternal", "ProcessEventLog"
        )

    assert result == ["Date == `2024-01-15`"]
    mock_table.where.assert_called_once_with(["Date == `2024-01-15`"])


@pytest.mark.asyncio
async def test_find_recent_partition_filters_second_partition_has_data():
    """_find_recent_partition_filters skips first partition (no data) and returns second."""
    from unittest.mock import patch as _patch

    from deephaven_mcp.queries import _find_recent_partition_filters

    mock_table = MagicMock()

    with (
        _patch("deephaven_mcp.queries._extract_partition_column_defs") as mock_extract,
        _patch("deephaven_mcp.queries._get_distinct_column_values") as mock_distinct,
    ):
        mock_extract.return_value = [{"name": "Date", "type": "java.time.LocalDate"}]
        mock_distinct.return_value = ["2024-01-15", "2024-01-14"]

        # First probe returns size=0, second returns size=5
        call_count = {"n": 0}

        def make_probe(size):
            m = MagicMock()
            m.size = size
            return m

        probes = [make_probe(0), make_probe(5)]

        def where_side_effect(f):
            idx = call_count["n"]
            call_count["n"] += 1
            return probes[idx]

        mock_table.where.side_effect = where_side_effect

        result = await _find_recent_partition_filters(
            mock_table, "DbInternal", "ProcessEventLog"
        )

    assert result == ["Date == `2024-01-14`"]


@pytest.mark.asyncio
async def test_find_recent_partition_filters_all_probes_empty():
    """_find_recent_partition_filters returns None when all probes find no data."""
    from unittest.mock import patch as _patch

    from deephaven_mcp.queries import _find_recent_partition_filters

    mock_table = MagicMock()
    mock_table.where.return_value.size = 0

    with (
        _patch("deephaven_mcp.queries._extract_partition_column_defs") as mock_extract,
        _patch("deephaven_mcp.queries._get_distinct_column_values") as mock_distinct,
    ):
        mock_extract.return_value = [{"name": "Date", "type": "java.time.LocalDate"}]
        mock_distinct.return_value = ["2024-01-15", "2024-01-14", "2024-01-13"]

        result = await _find_recent_partition_filters(
            mock_table, "DbInternal", "ProcessEventLog"
        )

    assert result is None


@pytest.mark.asyncio
async def test_find_recent_partition_filters_no_partition_cols():
    """_find_recent_partition_filters returns None when table has no partition columns."""
    from unittest.mock import patch as _patch

    from deephaven_mcp.queries import _find_recent_partition_filters

    mock_table = MagicMock()

    with _patch("deephaven_mcp.queries._extract_partition_column_defs") as mock_extract:
        mock_extract.return_value = []

        result = await _find_recent_partition_filters(mock_table, "public", "users")

    assert result is None
    mock_table.where.assert_not_called()


@pytest.mark.asyncio
async def test_find_recent_partition_filters_multiple_partition_cols_raises():
    """_find_recent_partition_filters raises InternalError when table has more than one partition column."""
    from unittest.mock import patch as _patch

    from deephaven_mcp._exceptions import InternalError
    from deephaven_mcp.queries import _find_recent_partition_filters

    mock_table = MagicMock()

    with _patch("deephaven_mcp.queries._extract_partition_column_defs") as mock_extract:
        mock_extract.return_value = [
            {"name": "Year", "type": "int"},
            {"name": "Month", "type": "int"},
        ]

        with pytest.raises(InternalError, match="2 partition columns"):
            await _find_recent_partition_filters(mock_table, "public", "sales")


def test_register_tools_registers_catalog_tools():
    """register_tools() registers all catalog tools."""
    from mcp.server.fastmcp import FastMCP

    from deephaven_mcp.mcp_systems_server._tools.catalog import register_tools

    server = FastMCP("test-catalog-server")
    register_tools(server)
    tools = server._tool_manager._tools
    assert "catalog_tables_list" in tools
    assert "catalog_namespaces_list" in tools
    assert "catalog_table_schema" in tools
    assert "catalog_table_sample" in tools
