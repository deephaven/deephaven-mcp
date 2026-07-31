"""Tests for ``deephaven_mcp.mcp_systems_server._fastmcp``.

Covers the FastMCP factory shared by both transports:

- ``_register_health_endpoint``: the ``/health`` route handler.
- ``_register_tools``: unconditional registration of every tool module.
- ``build_fastmcp``: wiring of the lifespan, tools, and health route.
"""

from __future__ import annotations

import json as _json
from unittest.mock import MagicMock, patch

import pytest

from deephaven_mcp.mcp_systems_server import _fastmcp as fastmcp_module


@pytest.mark.asyncio
async def test_register_health_endpoint_returns_ok():
    """The /health route handler returns ``{"status": "ok"}`` with HTTP 200."""
    captured: dict[str, object] = {}

    def _custom_route(path, methods):
        captured["path"] = path
        captured["methods"] = methods

        def decorator(fn):
            captured["fn"] = fn
            return fn

        return decorator

    fake_server = MagicMock()
    fake_server.custom_route = _custom_route
    fastmcp_module._register_health_endpoint(fake_server)

    # Route registered with the documented path + GET method.
    from deephaven_mcp._health import HEALTH_PATH

    assert captured["path"] == HEALTH_PATH
    assert captured["methods"] == ["GET"]

    # Invoke the handler with a dummy request; it must return a JSON 200.
    response = await captured["fn"](MagicMock())
    assert response.status_code == 200
    # The body is a JSONResponse; decoding the content payload is enough.
    assert _json.loads(response.body) == {"status": "ok"}


def test_register_tools_registers_every_module_unconditionally():
    """Every tool module registers regardless of configuration — no section gating.

    Tools self-report applicability at call time (empty result or a clean
    "not configured" error), so the tool surface is the same on every
    deployment shape.
    """
    fake_server = MagicMock()
    with (
        patch.object(fastmcp_module, "session") as m_session,
        patch.object(fastmcp_module, "table") as m_table,
        patch.object(fastmcp_module, "script") as m_script,
        patch.object(fastmcp_module, "session_community") as m_sc,
        patch.object(fastmcp_module, "session_enterprise") as m_se,
        patch.object(fastmcp_module, "catalog") as m_catalog,
        patch.object(fastmcp_module, "pq") as m_pq,
    ):
        fastmcp_module._register_tools(fake_server)
    for m in (m_session, m_table, m_script, m_sc, m_se, m_catalog, m_pq):
        m.register_tools.assert_called_once_with(fake_server)


def test_build_fastmcp_wires_lifespan_tools_and_health():
    """``build_fastmcp`` returns a FastMCP wired with the lifespan, tools, and health."""
    fake_server = MagicMock()
    fake_lifespan = object()
    with (
        patch.object(
            fastmcp_module, "FastMCP", return_value=fake_server
        ) as mock_fastmcp,
        patch.object(
            fastmcp_module, "make_lifespan", return_value=fake_lifespan
        ) as mock_lifespan,
        patch.object(fastmcp_module, "_register_tools") as mock_tools,
        patch.object(fastmcp_module, "_register_health_endpoint") as mock_health,
    ):
        holder = object()
        result = fastmcp_module.build_fastmcp("custom-name", holder=holder)
    assert result is fake_server
    mock_lifespan.assert_called_once_with(holder)
    mock_fastmcp.assert_called_once()
    # FastMCP receives the lifespan and the configured server name.
    assert mock_fastmcp.call_args.kwargs["lifespan"] is fake_lifespan
    assert mock_fastmcp.call_args.kwargs["name"] == "custom-name"
    mock_tools.assert_called_once_with(fake_server)
    mock_health.assert_called_once_with(fake_server)
