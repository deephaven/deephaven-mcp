"""
Tests for deephaven_mcp.mcp_systems_server._tools.catalog.
"""

import asyncio
import os
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from conftest import MockContext, create_mock_instance_tracker


# Test-specific helper functions (only used in this file)
def create_mock_arrow_meta_table(
    schema_data: list[dict], schema_fields: list | None = None
) -> MagicMock:
    """
    Create a mock PyArrow meta table for testing schema operations.

    This helper eliminates duplication across schema-related tests by providing
    a consistent way to create mock meta tables that match the behavior expected
    by _format_meta_table_result().

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


def create_mock_catalog_schema_function(schema_data_map, error_tables=None):
    """
    Create a mock function for queries.get_catalog_meta_table that returns different
    schemas based on table name.

    This helper eliminates duplication in catalog schema tests by providing a
    flexible way to mock different table schemas and error conditions.

    Args:
        schema_data_map: Dict mapping table_name -> schema_data (list of dicts)
        error_tables: Optional set of table names that should raise exceptions

    Returns:
        Callable: A function that can be used as side_effect for get_catalog_meta_table
    """
    error_tables = error_tables or set()

    def mock_get_catalog_meta_table(session, namespace, table_name):
        if table_name in error_tables:
            raise Exception(f"Table '{table_name}' not found in catalog")

        schema_data = schema_data_map.get(
            table_name, [{"Name": "Col1", "DataType": "int"}]  # Default schema
        )
        return create_mock_arrow_meta_table(schema_data)

    return mock_get_catalog_meta_table


from deephaven_mcp import config
from deephaven_mcp.mcp_systems_server._tools.catalog import (
    catalog_namespaces_list,
    catalog_table_sample,
    catalog_tables_list,
    catalog_tables_schema,
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
async def test_catalog_tables_success_no_filters():
    """Test catalog_tables with no filters and default max_rows."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock catalog arrow table
    mock_catalog_table = MagicMock()
    mock_catalog_table.__len__ = MagicMock(return_value=100)
    mock_catalog_table.nbytes = 5000  # Small size, under limit
    mock_field1 = MagicMock()
    mock_field1.name = "Namespace"
    mock_field1.type = "string"
    mock_field2 = MagicMock()
    mock_field2.name = "TableName"
    mock_field2.type = "string"
    mock_catalog_table.schema = [mock_field1, mock_field2]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_catalog_table, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.format_table_data"
        ) as mock_format:
            mock_format.return_value = (
                "json-row",
                [{"Namespace": "ns1", "TableName": "t1"}],
            )

            result = await catalog_tables_list(context, "enterprise:prod:analytics")

            assert result["success"] is True
            assert result["session_id"] == "enterprise:prod:analytics"
            assert result["format"] == "json-row"
            assert result["row_count"] == 100
            assert result["is_complete"] is True
            assert len(result["columns"]) == 2
            assert result["data"] == [{"Namespace": "ns1", "TableName": "t1"}]

            mock_get_catalog.assert_called_once_with(
                mock_session, max_rows=10000, filters=None, distinct_namespaces=False
            )


@pytest.mark.asyncio
async def test_catalog_tables_success_with_filters():
    """Test catalog with filters applied."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock catalog arrow table
    mock_catalog_table = MagicMock()
    mock_catalog_table.__len__ = MagicMock(return_value=50)
    mock_catalog_table.nbytes = 2500
    mock_field1 = MagicMock()
    mock_field1.name = "Namespace"
    mock_field1.type = "string"
    mock_catalog_table.schema = [mock_field1]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_catalog_table, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.format_table_data"
        ) as mock_format:
            mock_format.return_value = ("json-row", [{"Namespace": "market_data"}])

            filters = ["Namespace = `market_data`", "TableName.contains(`price`)"]
            result = await catalog_tables_list(
                context, "enterprise:prod:analytics", filters=filters
            )

            assert result["success"] is True
            assert result["row_count"] == 50
            assert result["is_complete"] is True

            mock_get_catalog.assert_called_once_with(
                mock_session, max_rows=10000, filters=filters, distinct_namespaces=False
            )


@pytest.mark.asyncio
async def test_catalog_tables_success_csv_format():
    """Test catalog with CSV format."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock catalog arrow table
    mock_catalog_table = MagicMock()
    mock_catalog_table.__len__ = MagicMock(return_value=10)
    mock_catalog_table.nbytes = 500
    mock_field1 = MagicMock()
    mock_field1.name = "Namespace"
    mock_field1.type = "string"
    mock_catalog_table.schema = [mock_field1]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_catalog_table, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.format_table_data"
        ) as mock_format:
            mock_format.return_value = ("csv", "Namespace\nmarket_data\n")

            result = await catalog_tables_list(
                context, "enterprise:prod:analytics", format="csv"
            )

            assert result["success"] is True
            assert result["format"] == "csv"
            assert isinstance(result["data"], str)

            mock_format.assert_called_once_with(mock_catalog_table, "csv")


@pytest.mark.asyncio
async def test_catalog_tables_incomplete_results():
    """Test catalog when results are incomplete (truncated by max_rows)."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock catalog arrow table
    mock_catalog_table = MagicMock()
    mock_catalog_table.__len__ = MagicMock(return_value=1000)
    mock_catalog_table.nbytes = 50000
    mock_field1 = MagicMock()
    mock_field1.name = "Namespace"
    mock_field1.type = "string"
    mock_catalog_table.schema = [mock_field1]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_catalog_table, False)  # Incomplete

        with patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.format_table_data"
        ) as mock_format:
            mock_format.return_value = ("json-row", [])

            result = await catalog_tables_list(
                context, "enterprise:prod:analytics", max_rows=1000
            )

            assert result["success"] is True
            assert result["is_complete"] is False  # Truncated
            assert result["row_count"] == 1000


@pytest.mark.asyncio
async def test_catalog_tables_not_enterprise_session():
    """Test catalog with non-enterprise session (should fail)."""
    from deephaven_mcp._exceptions import UnsupportedOperationError

    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.side_effect = UnsupportedOperationError(
            "get_catalog_table only supports enterprise (Core+) sessions"
        )

        result = await catalog_tables_list(context, "community:local:test")

        assert result["success"] is False
        assert "enterprise" in result["error"].lower()
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_tables_session_not_found():
    """Test catalog when session is not found."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=Exception("Session not found"))

    context = MockContext({"session_registry": mock_registry})

    result = await catalog_tables_list(context, "invalid_session")

    assert result["success"] is False
    assert "Session not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_tables_invalid_filter():
    """Test catalog with invalid filter syntax."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.side_effect = RuntimeError("Invalid filter syntax")

        result = await catalog_tables_list(
            context, "enterprise:prod:analytics", filters=["InvalidFilter!!!"]
        )

        assert result["success"] is False
        assert "Invalid filter syntax" in result["error"]
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_tables_invalid_format():
    """Test catalog with invalid format parameter."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock catalog arrow table
    mock_catalog_table = MagicMock()
    mock_catalog_table.__len__ = MagicMock(return_value=10)
    mock_catalog_table.nbytes = 500

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_catalog_table, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.format_table_data"
        ) as mock_format:
            mock_format.side_effect = ValueError("Unsupported format: invalid")

            result = await catalog_tables_list(
                context, "enterprise:prod:analytics", format="invalid"
            )

            assert result["success"] is False
            assert "Unsupported format" in result["error"]
            assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_tables_size_limit_exceeded():
    """Test catalog when response size exceeds limit."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock catalog arrow table with size exceeding limit
    mock_catalog_table = MagicMock()
    mock_catalog_table.__len__ = MagicMock(return_value=1000000)
    mock_catalog_table.nbytes = 60_000_000  # 60MB, exceeds 50MB limit

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_catalog_table, False)

        result = await catalog_tables_list(context, "enterprise:prod:analytics")

        assert result["success"] is False
        assert "50MB" in result["error"] or "max" in result["error"].lower()
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_namespaces_success_no_filters():
    """Test catalog_namespaces with no filters and default max_rows."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock namespaces arrow table
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.__len__ = MagicMock(return_value=25)
    namespaces_table_mock.nbytes = 1000  # Small size, under limit
    mock_field = MagicMock()
    mock_field.name = "Namespace"
    mock_field.type = "string"
    namespaces_table_mock.schema = [mock_field]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.format_table_data"
        ) as mock_format:
            mock_format.return_value = ("json-row", [{"Namespace": "market_data"}])

            result = await catalog_namespaces_list(context, "enterprise:prod:analytics")

            assert result["success"] is True
            assert result["session_id"] == "enterprise:prod:analytics"
            assert result["format"] == "json-row"
            assert result["row_count"] == 25
            assert result["is_complete"] is True
            assert len(result["columns"]) == 1
            assert result["data"] == [{"Namespace": "market_data"}]

            mock_get_namespaces.assert_called_once_with(
                mock_session, max_rows=1000, filters=None, distinct_namespaces=True
            )


@pytest.mark.asyncio
async def test_catalog_namespaces_success_with_filters():
    """Test catalog_namespaces with filters applied."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock namespaces arrow table
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.__len__ = MagicMock(return_value=5)
    namespaces_table_mock.nbytes = 500
    mock_field = MagicMock()
    mock_field.name = "Namespace"
    mock_field.type = "string"
    namespaces_table_mock.schema = [mock_field]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.format_table_data"
        ) as mock_format:
            mock_format.return_value = ("json-row", [{"Namespace": "market_data"}])

            filters = ["TableName.contains(`daily`)"]
            result = await catalog_namespaces_list(
                context, "enterprise:prod:analytics", filters=filters
            )

            assert result["success"] is True
            assert result["row_count"] == 5
            assert result["is_complete"] is True

            mock_get_namespaces.assert_called_once_with(
                mock_session, max_rows=1000, filters=filters, distinct_namespaces=True
            )


@pytest.mark.asyncio
async def test_catalog_namespaces_success_csv_format():
    """Test catalog_namespaces with CSV format."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock namespaces arrow table
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.__len__ = MagicMock(return_value=10)
    namespaces_table_mock.nbytes = 500
    mock_field = MagicMock()
    mock_field.name = "Namespace"
    mock_field.type = "string"
    namespaces_table_mock.schema = [mock_field]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.format_table_data"
        ) as mock_format:
            mock_format.return_value = ("csv", "Namespace\nmarket_data\n")

            result = await catalog_namespaces_list(
                context, "enterprise:prod:analytics", format="csv"
            )

            assert result["success"] is True
            assert result["format"] == "csv"
            assert isinstance(result["data"], str)

            mock_format.assert_called_once_with(namespaces_table_mock, "csv")


@pytest.mark.asyncio
async def test_catalog_namespaces_incomplete_results():
    """Test catalog_namespaces when results are incomplete (truncated by max_rows)."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock namespaces arrow table
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.__len__ = MagicMock(return_value=500)
    namespaces_table_mock.nbytes = 25000
    mock_field = MagicMock()
    mock_field.name = "Namespace"
    mock_field.type = "string"
    namespaces_table_mock.schema = [mock_field]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, False)  # Incomplete

        with patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.format_table_data"
        ) as mock_format:
            mock_format.return_value = ("json-row", [])

            result = await catalog_namespaces_list(
                context, "enterprise:prod:analytics", max_rows=500
            )

            assert result["success"] is True
            assert result["is_complete"] is False  # Truncated
            assert result["row_count"] == 500


@pytest.mark.asyncio
async def test_catalog_namespaces_not_enterprise_session():
    """Test catalog_namespaces with non-enterprise session (should fail)."""
    from deephaven_mcp._exceptions import UnsupportedOperationError

    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.side_effect = UnsupportedOperationError(
            "get_catalog_namespaces only supports enterprise (Core+) sessions"
        )

        result = await catalog_namespaces_list(context, "community:local:test")

        assert result["success"] is False
        assert "enterprise" in result["error"].lower()
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_namespaces_session_not_found():
    """Test catalog_namespaces when session is not found."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=Exception("Session not found"))

    context = MockContext({"session_registry": mock_registry})

    result = await catalog_namespaces_list(context, "invalid_session")

    assert result["success"] is False
    assert "Session not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_namespaces_invalid_format():
    """Test catalog_namespaces with invalid format parameter."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock namespaces arrow table
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.__len__ = MagicMock(return_value=10)
    namespaces_table_mock.nbytes = 500

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.format_table_data"
        ) as mock_format:
            mock_format.side_effect = ValueError("Unsupported format: invalid")

            result = await catalog_namespaces_list(
                context, "enterprise:prod:analytics", format="invalid"
            )

            assert result["success"] is False
            assert "Unsupported format" in result["error"]
            assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_namespaces_size_limit_exceeded():
    """Test catalog_namespaces when response size exceeds limit."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock namespaces arrow table with size exceeding limit
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.__len__ = MagicMock(return_value=100000)
    namespaces_table_mock.nbytes = 60_000_000  # 60MB, exceeds 50MB limit

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, False)

        result = await catalog_namespaces_list(context, "enterprise:prod:analytics")

        assert result["success"] is False
        assert "50MB" in result["error"] or "max" in result["error"].lower()
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_tables_schema_success_with_namespace():
    """Test catalog_schemas with namespace filter."""
    from deephaven_mcp.client import CorePlusSession

    # Create mock CorePlusSession
    mock_session = MagicMock(spec=CorePlusSession)

    # Mock catalog table data
    catalog_data = [
        {"Namespace": "market_data", "TableName": "daily_prices"},
        {"Namespace": "market_data", "TableName": "quotes"},
    ]

    mock_catalog_table = MagicMock()
    mock_catalog_table.to_pylist = MagicMock(return_value=catalog_data)

    # Use helper to create mock schema function
    schema_map = {
        "daily_prices": [
            {"Name": "Date", "DataType": "LocalDate"},
            {"Name": "Price", "DataType": "double"},
        ],
        "quotes": [
            {"Name": "Date", "DataType": "LocalDate"},
            {"Name": "Price", "DataType": "double"},
        ],
    }
    mock_get_catalog_meta = create_mock_catalog_schema_function(schema_map)

    # Set up session manager and registry
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    # Mock both queries functions
    with (
        patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
        ) as mock_get_catalog,
        patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_meta_table"
        ) as mock_get_schema,
    ):
        mock_get_catalog.return_value = (mock_catalog_table, True)
        mock_get_schema.side_effect = mock_get_catalog_meta

        result = await catalog_tables_schema(
            context, "enterprise:prod:analytics", namespace="market_data"
        )

    # Verify result
    assert result["success"] is True
    assert result["count"] == 2
    assert result["is_complete"] is True
    assert len(result["schemas"]) == 2

    # Check first schema - now expects full metadata in 'data' field
    assert result["schemas"][0]["success"] is True
    assert result["schemas"][0]["namespace"] == "market_data"
    assert result["schemas"][0]["table"] == "daily_prices"
    assert "data" in result["schemas"][0]
    assert len(result["schemas"][0]["data"]) == 2  # 2 columns
    assert "meta_columns" in result["schemas"][0]
    assert "row_count" in result["schemas"][0]

    # Check second schema
    assert result["schemas"][1]["success"] is True
    assert result["schemas"][1]["namespace"] == "market_data"
    assert result["schemas"][1]["table"] == "quotes"
    assert "data" in result["schemas"][1]


@pytest.mark.asyncio
async def test_catalog_tables_schema_success_with_table_names():
    """Test catalog_schemas with specific table_names filter."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)

    # Catalog will be filtered by table_names at the catalog level
    # So it should only return the requested table
    catalog_data = [
        {"Namespace": "market_data", "TableName": "quotes"},
    ]

    mock_catalog_table = MagicMock()
    mock_catalog_table.to_pylist = MagicMock(return_value=catalog_data)

    # Use helper to create mock schema function (default schema for all tables)
    mock_get_catalog_meta = create_mock_catalog_schema_function({})

    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
        ) as mock_get_catalog,
        patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_meta_table"
        ) as mock_get_schema,
    ):
        mock_get_catalog.return_value = (mock_catalog_table, True)
        mock_get_schema.side_effect = mock_get_catalog_meta

        result = await catalog_tables_schema(
            context,
            "enterprise:prod:analytics",
            table_names=["quotes"],  # Only request quotes
        )

        # Verify the filter was passed correctly to get_catalog_table
        call_args = mock_get_catalog.call_args
        assert call_args[1]["filters"] == ["TableName in `quotes`"]

    # Should only return 1 schema (quotes)
    assert result["success"] is True
    assert result["count"] == 1
    assert len(result["schemas"]) == 1
    assert result["schemas"][0]["table"] == "quotes"


@pytest.mark.asyncio
async def test_catalog_tables_schema_max_tables_limit():
    """Test catalog_schemas respects max_tables limit."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)

    # Create 150 catalog entries, but get_catalog_table will only return 50 due to max_rows=50
    catalog_data = [
        {"Namespace": "market_data", "TableName": f"table_{i}"}
        for i in range(50)  # Only 50 entries returned due to max_rows limit
    ]

    mock_catalog_table = MagicMock()
    mock_catalog_table.to_pylist = MagicMock(return_value=catalog_data)

    # Use helper to create mock schema function (default schema for all tables)
    mock_get_catalog_meta = create_mock_catalog_schema_function({})

    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
        ) as mock_get_catalog,
        patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_meta_table"
        ) as mock_get_schema,
    ):
        mock_get_catalog.return_value = (
            mock_catalog_table,
            False,
        )  # is_complete=False because truncated
        mock_get_schema.side_effect = mock_get_catalog_meta

        result = await catalog_tables_schema(
            context,
            "enterprise:prod:analytics",
            max_tables=50,  # Limit to 50
        )

    # Should only return 50 schemas
    assert result["success"] is True
    assert result["count"] == 50
    assert result["is_complete"] is False  # Truncated
    assert len(result["schemas"]) == 50


@pytest.mark.asyncio
async def test_catalog_tables_schema_not_enterprise_session():
    """Test catalog_schemas fails with non-enterprise session."""
    # Create a non-CorePlusSession mock
    mock_session = MagicMock()  # Not CorePlusSession

    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    result = await catalog_tables_schema(
        context, "community:local:test", namespace="market_data"
    )

    # Should fail with error about enterprise-only
    assert result["success"] is False
    assert result["isError"] is True
    assert "enterprise" in result["error"].lower() or "Core+" in result["error"]


@pytest.mark.asyncio
async def test_catalog_tables_schema_mixed_success_failure():
    """Test catalog_schemas with some tables succeeding and some failing."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)

    catalog_data = [
        {"Namespace": "market_data", "TableName": "good_table"},
        {"Namespace": "market_data", "TableName": "bad_table"},
    ]

    mock_catalog_table = MagicMock()
    mock_catalog_table.to_pylist = MagicMock(return_value=catalog_data)

    # Use helper to create mock schema function - fail for bad_table
    mock_get_catalog_meta = create_mock_catalog_schema_function(
        schema_data_map={}, error_tables={"bad_table"}
    )

    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
        ) as mock_get_catalog,
        patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_meta_table"
        ) as mock_get_schema,
    ):
        mock_get_catalog.return_value = (mock_catalog_table, True)
        mock_get_schema.side_effect = mock_get_catalog_meta

        result = await catalog_tables_schema(context, "enterprise:prod:analytics")

    # Overall operation should succeed
    assert result["success"] is True
    assert result["count"] == 2

    # First table should succeed
    assert result["schemas"][0]["success"] is True
    assert result["schemas"][0]["table"] == "good_table"

    # Second table should fail
    assert result["schemas"][1]["success"] is False
    assert result["schemas"][1]["table"] == "bad_table"
    assert result["schemas"][1]["isError"] is True
    assert "not found" in result["schemas"][1]["error"].lower()


@pytest.mark.asyncio
async def test_catalog_tables_schema_session_not_found():
    """Test catalog_schemas when session is not found."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=Exception("Session not found"))

    context = MockContext({"session_registry": mock_registry})

    result = await catalog_tables_schema(context, "enterprise:prod:nonexistent")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Session not found" in result["error"]


@pytest.mark.asyncio
async def test_catalog_tables_schema_catalog_retrieval_error():
    """Test catalog_schemas when catalog table retrieval fails."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)

    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    # Mock queries.get_catalog_table to raise error
    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.side_effect = Exception("Catalog access denied")

        result = await catalog_tables_schema(context, "enterprise:prod:analytics")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Catalog access denied" in result["error"]


@pytest.mark.asyncio
async def test_catalog_tables_schema_with_filters():
    """Test catalog_schemas with custom filters."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)

    # Catalog will be pre-filtered by queries.get_catalog_table
    catalog_data = [
        {"Namespace": "market_data", "TableName": "daily_prices"},
    ]

    mock_catalog_table = MagicMock()
    mock_catalog_table.to_pylist = MagicMock(return_value=catalog_data)

    # Use helper to create mock schema function with specific schema for daily_prices
    schema_map = {"daily_prices": [{"Name": "Price", "DataType": "double"}]}
    mock_get_catalog_meta = create_mock_catalog_schema_function(schema_map)

    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
        ) as mock_get_catalog,
        patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_meta_table"
        ) as mock_get_schema,
    ):
        mock_get_catalog.return_value = (mock_catalog_table, True)
        mock_get_schema.side_effect = mock_get_catalog_meta

        result = await catalog_tables_schema(
            context,
            "enterprise:prod:analytics",
            filters=["TableName.contains(`price`)"],
        )

        # Verify filters were passed to get_catalog_table
        call_args = mock_get_catalog.call_args
        assert call_args[1]["filters"] == ["TableName.contains(`price`)"]

    assert result["success"] is True
    assert result["count"] == 1
    assert result["schemas"][0]["table"] == "daily_prices"


@pytest.mark.asyncio
async def test_catalog_tables_schema_empty_catalog():
    """Test catalog_schemas when catalog has no matching tables."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)

    # Empty catalog
    catalog_data = []

    mock_catalog_table = MagicMock()
    mock_catalog_table.to_pylist = MagicMock(return_value=catalog_data)

    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_catalog_table, True)

        result = await catalog_tables_schema(
            context, "enterprise:prod:analytics", namespace="nonexistent"
        )

    assert result["success"] is True
    assert result["count"] == 0
    assert result["is_complete"] is True
    assert len(result["schemas"]) == 0


@pytest.mark.asyncio
async def test_catalog_table_sample_success():
    """Test catalog_table_sample with successful data retrieval."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

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
            context, "enterprise:prod:analytics", "public", "users", max_rows=10
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


@pytest.mark.asyncio
async def test_catalog_table_sample_with_format():
    """Test catalog_table_sample with different format."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

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
            "enterprise:prod:analytics",
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

    context = MockContext({"session_registry": mock_registry})

    result = await catalog_table_sample(
        context, "enterprise:prod:analytics", "public", "users"
    )

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

    context = MockContext({"session_registry": mock_registry})

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table_data"
    ) as mock_get_data:
        mock_get_data.side_effect = Exception("Database connection failed")

        result = await catalog_table_sample(
            context, "enterprise:prod:analytics", "public", "users"
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

    context = MockContext({"session_registry": mock_registry})

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
            context, "enterprise:prod:analytics", "public", "huge_table"
        )

    assert result["success"] is False
    assert "max 50MB" in result["error"]
    assert "reduce max_rows" in result["error"]
    assert result["isError"] is True
