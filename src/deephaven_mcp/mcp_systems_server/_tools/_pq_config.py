"""Runtime tunables for the persistent-query (PQ) MCP tools.

Defines :class:`PqToolsConfig`, the Pydantic v2 model carrying the
defaults applied by the PQ tools in
:mod:`deephaven_mcp.mcp_systems_server._tools.pq`. The model is
loaded from ``enterprise/settings.json``'s ``pq_tools`` block at
startup (PQ tools are enterprise-only) and published into the
lifespan context as part of the validated
:class:`~deephaven_mcp.mcp_systems_server.config.MultiSystemConfig`.
PQ tool functions read it via
:func:`deephaven_mcp.mcp_systems_server._tools.shared.get_enterprise_settings`
(``.pq_tools``) at call time and pass it down to private helpers.

Every field carries its schema-level default so the JSON block is
fully optional. Authors who want to pull a value from an environment
variable write ``"<field>": "${env:NAME}"`` in the source JSON; the
templating engine resolves the placeholder before validation.
"""

from __future__ import annotations

__all__ = [
    "PqToolsConfig",
]

from typing import Annotated

from pydantic import Field

from deephaven_mcp._pydantic import StrictSchema


class PqToolsConfig(StrictSchema):
    """Defaults for persistent-query MCP tools."""

    default_max_concurrent: Annotated[int, Field(gt=0)] = 20
    """Default cap on the number of PQ operations that may run in
    parallel within a single batch tool call (``pq_start``,
    ``pq_stop``, ``pq_restart``, ``pq_delete``). Callers may override
    per-invocation; the default applies when they do not."""
