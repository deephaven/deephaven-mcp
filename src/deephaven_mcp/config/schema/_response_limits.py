"""Schema for the tool-side response-size guard.

Defines :class:`ResponseLimits`, the Pydantic model of operator-tunable
byte-size thresholds that bound how large a tool response may be before
it is refused or flagged. The community and enterprise sections each
carry an independent copy.
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
