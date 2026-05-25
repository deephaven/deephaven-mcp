"""Runtime tunables for MCP-tool response-size guards.

Defines :class:`ResponseLimits`, the Pydantic v2 model carrying the
operator-tunable thresholds applied by the tool-side response-size
guard in
:func:`deephaven_mcp.mcp_systems_server._tools.shared.check_response_size`.
The model is loaded from each section's ``settings.json`` block at
startup (community and enterprise carry independent copies so the two
deployments can tune the guard separately) and published into the
lifespan context as part of the validated
:class:`~deephaven_mcp.mcp_systems_server.config.MultiSystemConfig`.
Tool functions read the appropriate copy via
:func:`deephaven_mcp.mcp_systems_server._tools.shared.get_response_limits`
at call time and pass it down to the size-check helper.

Every field carries its schema-level default so the JSON block is
fully optional. Authors who want to pull a value from an environment
variable write ``"<field>": "${env:NAME}"`` in the source JSON; the
templating engine resolves the placeholder before validation.
"""

from __future__ import annotations

__all__ = [
    "ResponseLimits",
]

from typing import Annotated, Self

from pydantic import Field, model_validator

from deephaven_mcp._pydantic import StrictSchema


class ResponseLimits(StrictSchema):
    """Operator-tunable thresholds for the tool-side response-size guard."""

    max_response_bytes: Annotated[int, Field(gt=0)] = 50 * 1024 * 1024
    """Maximum estimated response size in bytes. Tools refuse to
    serialize a response whose estimated payload exceeds this value
    and instead return a structured error asking the caller to reduce
    ``max_rows``."""

    warning_response_bytes: Annotated[int, Field(gt=0)] = 5 * 1024 * 1024
    """Estimated response size in bytes above which a warning is
    logged but the response is still served. Use this to surface
    over-large responses during operator-side monitoring without
    breaking caller workflows."""

    estimated_bytes_per_cell: Annotated[int, Field(gt=0)] = 50
    """Conservative blended estimate of bytes per table cell used to
    project a response's size before it is actually formatted. Covers
    a typical mix of strings, numerics, nulls, and JSON formatting
    overhead."""

    @model_validator(mode="after")
    def _validate_thresholds(self) -> Self:
        """Reject a configuration whose warning threshold exceeds the maximum."""
        if self.warning_response_bytes > self.max_response_bytes:
            raise ValueError(
                f"warning_response_bytes ({self.warning_response_bytes}) must "
                f"not exceed max_response_bytes ({self.max_response_bytes})."
            )
        return self
