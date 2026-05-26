"""Tests for the ``list_systems`` MCP tool.

The tool is a thin read-through that returns
``multi_config.list_systems()`` as a structured list. Test coverage
focuses on:

* empty / community-only / enterprise-only / mixed configurations,
* ordering (community first, then enterprise in declaration order),
* the ``{"success": True, "systems": [...]}`` envelope shape,
* registration with FastMCP via :func:`register_tools`.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from conftest import MockContext

from deephaven_mcp._taxonomy import SystemRef, SystemType
from deephaven_mcp.mcp_systems_server._tools.session import (
    list_systems,
    register_tools,
)


def _ctx_with_systems(systems: list[SystemRef]) -> MockContext:
    """Build a MockContext whose ``multi_config.list_systems()`` returns ``systems``."""
    multi_config = MagicMock()
    multi_config.list_systems = MagicMock(return_value=list(systems))
    return MockContext({"multi_config": multi_config})


@pytest.mark.asyncio
async def test_list_systems_empty_config() -> None:
    ctx = _ctx_with_systems([])
    result = await list_systems(ctx)
    assert result == {"success": True, "systems": []}


@pytest.mark.asyncio
async def test_list_systems_community_only() -> None:
    ctx = _ctx_with_systems([SystemRef(name="community", type=SystemType.COMMUNITY)])
    result = await list_systems(ctx)
    assert result == {
        "success": True,
        "systems": [{"name": "community", "type": "community"}],
    }


@pytest.mark.asyncio
async def test_list_systems_enterprise_only() -> None:
    ctx = _ctx_with_systems(
        [
            SystemRef(name="prod", type=SystemType.ENTERPRISE),
            SystemRef(name="dev", type=SystemType.ENTERPRISE),
        ]
    )
    result = await list_systems(ctx)
    assert result == {
        "success": True,
        "systems": [
            {"name": "prod", "type": "enterprise"},
            {"name": "dev", "type": "enterprise"},
        ],
    }


@pytest.mark.asyncio
async def test_list_systems_mixed_preserves_order() -> None:
    """list_systems ordering is preserved verbatim into the response."""
    ctx = _ctx_with_systems(
        [
            SystemRef(name="community", type=SystemType.COMMUNITY),
            SystemRef(name="prod", type=SystemType.ENTERPRISE),
            SystemRef(name="dev", type=SystemType.ENTERPRISE),
        ]
    )
    result = await list_systems(ctx)
    assert result == {
        "success": True,
        "systems": [
            {"name": "community", "type": "community"},
            {"name": "prod", "type": "enterprise"},
            {"name": "dev", "type": "enterprise"},
        ],
    }


@pytest.mark.asyncio
async def test_list_systems_propagates_underlying_error() -> None:
    """An exception raised by ``list_systems`` is not caught by the tool."""
    multi_config = MagicMock()
    multi_config.list_systems = MagicMock(side_effect=RuntimeError("boom"))
    ctx = MockContext({"multi_config": multi_config})
    with pytest.raises(RuntimeError, match="boom"):
        await list_systems(ctx)


def test_register_tools_registers_list_systems() -> None:
    """``register_tools`` must wire ``list_systems`` onto the FastMCP server."""
    server = MagicMock()
    tool_decorator = MagicMock(side_effect=lambda fn: fn)
    server.tool = MagicMock(return_value=tool_decorator)

    register_tools(server)

    registered = [call.args[0] for call in tool_decorator.call_args_list]
    assert list_systems in registered
