"""FastMCP instance construction for the systems server.

Builds the FastMCP instance used by both transports: the per-session lifespan
(from :func:`._lifespan.make_lifespan`), every MCP tool module, and the
``/health`` route.
"""

from __future__ import annotations

import logging

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from deephaven_mcp._health import HEALTH_PATH

from ._lifespan import LifespanContext, ProcessResources, make_lifespan
from ._tools import (
    catalog,
    pq,
    script,
    session,
    session_community,
    session_enterprise,
    table,
)

_LOGGER = logging.getLogger(__name__)

__all__ = ["build_fastmcp"]


def _register_tools(server: FastMCP[LifespanContext]) -> None:
    """Register every MCP tool on the multiplexed systems server.

    Every tool module registers unconditionally, regardless of which
    configuration sections were loaded. Tools that need an absent section
    self-report when invoked (``enterprise_systems_status`` returns an empty
    list; the session, ``catalog``, and ``pq`` tools return a clean "not
    configured" error).

    Args:
        server (FastMCP[LifespanContext]): The FastMCP instance to register
            tool modules on.
    """
    session.register_tools(server)
    table.register_tools(server)
    script.register_tools(server)
    session_community.register_tools(server)
    session_enterprise.register_tools(server)
    catalog.register_tools(server)
    pq.register_tools(server)
    _LOGGER.debug(
        "[mcp_systems_server._fastmcp:_register_tools] Registered all tool modules"
    )


def _register_health_endpoint(server: FastMCP[LifespanContext]) -> None:
    """Register the ``/health`` liveness/readiness route on ``server``.

    The route is registered for both transports but is only ever
    exercised under HTTP. It is also added to
    :class:`PSKMiddleware`'s ``bypass_paths`` so external probes do not
    need to share the PSK.

    Args:
        server (FastMCP[LifespanContext]): The FastMCP instance whose
            ASGI app will own the route.
    """

    @server.custom_route(HEALTH_PATH, methods=["GET"])  # type: ignore[untyped-decorator]
    async def health_check(_request: Request) -> JSONResponse:
        """Return a 200/JSON liveness probe for the systems server."""
        _LOGGER.debug(
            "[mcp_systems_server._fastmcp:health_check] Health check requested"
        )
        return JSONResponse({"status": "ok"})


def build_fastmcp(
    server_name: str,
    *,
    holder: ProcessResources,
) -> FastMCP[LifespanContext]:
    """Build the FastMCP instance with lifespan + tools + health route.

    Args:
        server_name (str): The FastMCP server name advertised in MCP
            handshakes; sourced from ``ServerConfig.server_name``.
        holder (ProcessResources): Holder read by the per-session lifespan
            from :func:`._lifespan.make_lifespan`. The caller must wrap the
            transport run in :func:`._lifespan.process_lifespan` (passing the
            same holder) so the context is populated before serving begins.

    Returns:
        FastMCP[LifespanContext]: The fully wired MCP server.
    """
    server: FastMCP[LifespanContext] = FastMCP(
        name=server_name,
        lifespan=make_lifespan(holder),
    )
    _register_tools(server)
    _register_health_endpoint(server)
    _LOGGER.debug(
        f"[mcp_systems_server._fastmcp:build_fastmcp] Built FastMCP instance "
        f"name={server_name!r}"
    )
    return server
