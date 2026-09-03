"""
Tests for deephaven_mcp.mcp_systems_server._tools.catalog.
"""

import asyncio
import os
import warnings
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

from deephaven_mcp import config
from deephaven_mcp.client import CorePlusSession
from deephaven_mcp.config.schema._enterprise import EnterpriseSettings
from deephaven_mcp.mcp_systems_server._tools.catalog import (
    catalog_namespaces_list,
    catalog_tables_list,
)
from deephaven_mcp.resource_manager import (
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
    EnterpriseSessionRegistry,
    PythonLaunchedSession,
    ResourceLivenessStatus,
    SystemType,
)

from ._helpers import (
    MockContext,
    create_mock_instance_tracker,
)


def _mock_catalog_arrow(entries, nbytes=1000, num_rows=None):
    """Mock catalog arrow table whose Namespace/TableName selection yields ``entries``."""
    subset = MagicMock()
    subset.nbytes = nbytes
    subset.num_rows = len(entries) if num_rows is None else num_rows
    subset.to_pylist.return_value = entries
    table = MagicMock()
    table.select.return_value = subset
    return table


def _wcd_yielding(session):
    """Build a fresh async CM per call that yields ``session``."""

    @asynccontextmanager
    async def _cm():
        yield session

    return MagicMock(side_effect=lambda: _cm())


def _wcd_failing(exc):
    """Build a fresh async CM per call that raises ``exc`` on entry."""

    @asynccontextmanager
    async def _cm():
        raise exc
        yield  # pragma: no cover - unreachable, keeps this a generator

    return MagicMock(side_effect=lambda: _cm())


def _catalog_context(session, system="prod", operate_as="jdoe"):
    """Build a MockContext whose enterprise registry serves ``session`` as WebClientData."""
    registry = MagicMock(spec=EnterpriseSessionRegistry)
    registry.system_name = system
    registry.web_client_data_session = _wcd_yielding(session)
    registry.effective_user = AsyncMock(return_value=operate_as)
    return MockContext({"config_manager": MagicMock(), "registry": registry})


def _unavailable_catalog_context(exc, system="prod"):
    """Build a MockContext whose WebClientData connect fails with ``exc``."""
    registry = MagicMock(spec=EnterpriseSessionRegistry)
    registry.system_name = system
    registry.web_client_data_session = _wcd_failing(exc)
    registry.effective_user = AsyncMock(return_value="jdoe")
    return MockContext({"config_manager": MagicMock(), "registry": registry})


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

        result = await catalog_tables_list(context, "prod")

        assert result == {
            "success": True,
            "system": "prod",
            "tables": [
                {"namespace": "ns1", "table_name": "t1"},
                {"namespace": "ns2", "table_name": "t2"},
            ],
            "count": 2,
            "is_complete": True,
        }

        mock_get_catalog.assert_called_once()
        call_kwargs = mock_get_catalog.call_args[1]
        assert mock_get_catalog.call_args[0][0] is mock_session
        assert call_kwargs["operate_as"] == "jdoe"
        assert call_kwargs["max_rows"] == 10000
        assert call_kwargs["filters"] is None
        assert call_kwargs["distinct_namespaces"] is False
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
        result = await catalog_tables_list(context, "prod", filters=filters)

        assert result["success"] is True
        assert result["count"] == 1
        assert result["is_complete"] is True

        mock_get_catalog.assert_called_once()
        call_kwargs = mock_get_catalog.call_args[1]
        assert call_kwargs["filters"] == filters
        assert call_kwargs["distinct_namespaces"] is False


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

        result = await catalog_tables_list(context, "prod", max_rows=3)

        assert result["success"] is True
        assert result["is_complete"] is False  # Truncated
        assert result["count"] == 3


@pytest.mark.asyncio
async def test_catalog_tables_unknown_system():
    """An unconfigured system is rejected before any catalog query runs."""
    context = _catalog_context(MagicMock(spec=CorePlusSession), system="prod")

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        result = await catalog_tables_list(context, "nope")

    assert result["success"] is False
    assert "nope" in result["error"]
    assert result["isError"] is True
    mock_get_catalog.assert_not_called()


@pytest.mark.asyncio
async def test_catalog_tables_web_client_data_unavailable():
    """A WebClientData connect failure surfaces as a structured error."""
    context = _unavailable_catalog_context(RuntimeError("WebClientData is not running"))

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        result = await catalog_tables_list(context, "prod")

    assert result["success"] is False
    assert "WebClientData is not running" in result["error"]
    assert result["isError"] is True
    mock_get_catalog.assert_not_called()


@pytest.mark.asyncio
async def test_catalog_tables_invalid_filter():
    """Test catalog with invalid filter syntax."""
    context = _catalog_context(MagicMock(spec=CorePlusSession))

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.side_effect = RuntimeError("Invalid filter syntax")

        result = await catalog_tables_list(
            context, "prod", filters=["InvalidFilter!!!"]
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

        result = await catalog_tables_list(context, "prod")

        assert result["success"] is False
        assert "50MB" in result["error"] or "max" in result["error"].lower()
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_tables_size_limit_counts_serialization_overhead():
    """Arrow bytes under the limit still fail when per-entry overhead pushes the estimate over."""
    context = _catalog_context(MagicMock(spec=CorePlusSession))
    # 49MB of Arrow buffers passes alone, but 100k entries add
    # 48 bytes of key/delimiter overhead each (+4.8MB), exceeding 50MB.
    mock_arrow = _mock_catalog_arrow([], nbytes=49_000_000, num_rows=100_000)

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_arrow, False)

        result = await catalog_tables_list(context, "prod")

        assert result["success"] is False
        assert "50MB" in result["error"] or "max" in result["error"].lower()
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_namespaces_success_no_filters():
    """Test catalog_namespaces with no filters and default max_rows."""
    mock_session = MagicMock(spec=CorePlusSession)
    context = _catalog_context(mock_session)

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

        result = await catalog_namespaces_list(context, "prod")

        assert result["success"] is True
        assert result["system"] == "prod"
        assert result["namespaces"] == ["market_data", "reference"]
        assert result["count"] == 2
        assert result["is_complete"] is True

        namespaces_table_mock.column.assert_called_once_with("Namespace")
        mock_get_namespaces.assert_called_once()
        call_kwargs = mock_get_namespaces.call_args[1]
        assert mock_get_namespaces.call_args[0][0] is mock_session
        assert call_kwargs["operate_as"] == "jdoe"
        assert call_kwargs["max_rows"] == 1000
        assert call_kwargs["filters"] is None
        assert call_kwargs["distinct_namespaces"] is True


@pytest.mark.asyncio
async def test_catalog_namespaces_success_with_filters():
    """Test catalog_namespaces with filters applied."""
    mock_session = MagicMock(spec=CorePlusSession)
    context = _catalog_context(mock_session)

    # Mock namespaces arrow table
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.nbytes = 500
    namespaces_table_mock.column.return_value.to_pylist.return_value = ["market_data"]

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, True)

        filters = ["TableName.contains(`daily`)"]
        result = await catalog_namespaces_list(context, "prod", filters=filters)

        assert result["success"] is True
        assert result["namespaces"] == ["market_data"]
        assert result["count"] == 1
        assert result["is_complete"] is True

        mock_get_namespaces.assert_called_once()
        call_kwargs = mock_get_namespaces.call_args[1]
        assert call_kwargs["filters"] == filters
        assert call_kwargs["distinct_namespaces"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tool,arrow",
    [
        (catalog_tables_list, _mock_catalog_arrow([])),
        (catalog_namespaces_list, MagicMock(nbytes=0)),
    ],
)
async def test_catalog_listing_forwards_the_configured_widget_timeout(tool, arrow):
    """The configured web_client_data timeout reaches the widget fetch verbatim."""
    settings = EnterpriseSettings.model_validate(
        {"timeouts": {"client": {"web_client_data_timeout_seconds": 7.5}}}
    )
    context = _catalog_context(MagicMock(spec=CorePlusSession))

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.get_enterprise_settings",
            return_value=settings,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
        ) as mock_get_catalog,
    ):
        mock_get_catalog.return_value = (arrow, True)

        result = await tool(context, "prod")

    assert result["success"] is True
    assert mock_get_catalog.call_args[1]["timeout_seconds"] == 7.5


@pytest.mark.asyncio
async def test_catalog_namespaces_incomplete_results():
    """Test catalog_namespaces when results are incomplete (truncated by max_rows)."""
    context = _catalog_context(MagicMock(spec=CorePlusSession))

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

        result = await catalog_namespaces_list(context, "prod", max_rows=500)

        assert result["success"] is True
        assert result["is_complete"] is False  # Truncated
        assert result["count"] == 500


@pytest.mark.asyncio
async def test_catalog_namespaces_unknown_system():
    """An unconfigured system is rejected before any catalog query runs."""
    context = _catalog_context(MagicMock(spec=CorePlusSession), system="prod")

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        result = await catalog_namespaces_list(context, "nope")

    assert result["success"] is False
    assert "nope" in result["error"]
    assert result["isError"] is True
    mock_get_namespaces.assert_not_called()


@pytest.mark.asyncio
async def test_catalog_namespaces_web_client_data_unavailable():
    """A WebClientData connect failure surfaces as a structured error."""
    context = _unavailable_catalog_context(RuntimeError("WebClientData is not running"))

    result = await catalog_namespaces_list(context, "prod")

    assert result["success"] is False
    assert "WebClientData is not running" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_namespaces_size_limit_exceeded():
    """Test catalog_namespaces when response size exceeds limit."""
    context = _catalog_context(MagicMock(spec=CorePlusSession))

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

        result = await catalog_namespaces_list(context, "prod")

        assert result["success"] is False
        assert "50MB" in result["error"] or "max" in result["error"].lower()
        assert result["isError"] is True


def test_register_tools_registers_catalog_tools():
    """register_tools() registers all catalog tools."""
    from mcp.server.fastmcp import FastMCP

    from deephaven_mcp.mcp_systems_server._tools.catalog import register_tools

    server = FastMCP("test-catalog-server")
    register_tools(server)
    tools = server._tool_manager._tools
    assert "catalog_tables_list" in tools
    assert "catalog_namespaces_list" in tools


# ===== error rendering =====


def _wrapped_filter_error():
    """Build the exception shape pydeephaven raises for a rejected filter.

    ``_table_service`` raises a DHError naming the server's ``error_info``,
    catches it, and re-raises a second DHError that says only "failed to
    finish <Op> operation". The useful text is therefore in ``__cause__``.
    """
    inner = RuntimeError(
        "Server error received for UnstructuredFilterOp: "
        "Cannot find column: namespace"
    )
    outer = RuntimeError("failed to finish UnstructuredFilterOp operation")
    outer.__cause__ = inner
    return outer


@pytest.mark.asyncio
async def test_catalog_tables_error_surfaces_the_wrapped_cause():
    """A bad filter reports the engine's complaint, not just the wrapper."""
    context = _catalog_context(MagicMock(spec=CorePlusSession))

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.side_effect = _wrapped_filter_error()

        result = await catalog_tables_list(context, "prod", filters=["namespace = `x`"])

    assert result["success"] is False
    assert "Cannot find column: namespace" in result["error"]


@pytest.mark.asyncio
async def test_catalog_namespaces_error_surfaces_the_wrapped_cause():
    context = _catalog_context(MagicMock(spec=CorePlusSession))

    with patch(
        "deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.side_effect = _wrapped_filter_error()

        result = await catalog_namespaces_list(context, "prod")

    assert result["success"] is False
    assert "Cannot find column: namespace" in result["error"]
