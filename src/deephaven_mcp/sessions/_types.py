"""Shared closed-vocabulary types for the session domain.

Home of the vocabulary aliases shared across layers that cannot all
import each other: the declaration models in this package, the
session launcher in :mod:`deephaven_mcp.resource_manager`, and the
config schemas in :mod:`deephaven_mcp.config.schema` (which
re-exports them for tool-layer consumers).
"""

from __future__ import annotations

__all__ = [
    "LaunchMethod",
    "ProgrammingLanguage",
]

from typing import Literal

LaunchMethod = Literal["docker", "python"]
"""Closed vocabulary for how a dynamic community session is launched."""

ProgrammingLanguage = Literal["Python", "Groovy"]
"""Closed vocabulary for a worker's scripting language. Title case is
canonical: the values are defined externally by Deephaven (the
enterprise controller's ``scriptLanguage`` field), not by this
project."""
