"""Schema for the persistent-query (PQ) tool defaults.

Defines :class:`PqToolsConfig`, the Pydantic model carrying the
defaults for the enterprise PQ batch tools (the parallel-operation cap).
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
