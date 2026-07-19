"""Shared closed-vocabulary types for the session domain.

Home of the vocabulary aliases — plus their ``get_args``-derived
runtime membership sets — shared across layers that cannot all
import each other: the declaration models in this package, the
session launcher in :mod:`deephaven_mcp.resource_manager`, and the
config schemas in :mod:`deephaven_mcp.config.schema` (which
re-exports the aliases for tool-layer consumers).
"""

from __future__ import annotations

__all__ = [
    "VALID_LAUNCH_METHODS",
    "VALID_PROGRAMMING_LANGUAGES",
    "LaunchMethod",
    "ProgrammingLanguage",
]

from typing import Literal, get_args

LaunchMethod = Literal["docker", "python"]
"""Closed vocabulary for how a dynamic community session is launched."""

ProgrammingLanguage = Literal["Python", "Groovy"]
"""Closed vocabulary for a worker's scripting language. Title case is
canonical: the values are defined externally by Deephaven (the
enterprise controller's ``scriptLanguage`` field), not by this
project."""

VALID_LAUNCH_METHODS: frozenset[str] = frozenset(get_args(LaunchMethod))
"""Runtime membership set for ``LaunchMethod``, derived via
``typing.get_args``."""

VALID_PROGRAMMING_LANGUAGES: frozenset[str] = frozenset(get_args(ProgrammingLanguage))
"""Runtime membership set for ``ProgrammingLanguage``, derived via
``typing.get_args``."""
