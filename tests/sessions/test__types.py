"""Tests for deephaven_mcp.sessions._types."""

from typing import get_args

from deephaven_mcp.sessions import (
    VALID_LAUNCH_METHODS,
    VALID_PROGRAMMING_LANGUAGES,
    LaunchMethod,
    ProgrammingLanguage,
)
from deephaven_mcp.sessions._types import LaunchMethod as LaunchMethodDirect
from deephaven_mcp.sessions._types import (
    ProgrammingLanguage as ProgrammingLanguageDirect,
)


def test_programming_language_vocabulary():
    """The vocabulary is exactly the Deephaven-canonical title-case values."""
    assert get_args(ProgrammingLanguage) == ("Python", "Groovy")


def test_launch_method_vocabulary():
    """The vocabulary is exactly the two supported launch mechanisms."""
    assert get_args(LaunchMethod) == ("docker", "python")


def test_types_exported_from_package():
    """The package re-exports are the same objects as the module definitions."""
    assert ProgrammingLanguage is ProgrammingLanguageDirect
    assert LaunchMethod is LaunchMethodDirect


def test_runtime_sets_match_literals():
    """The membership sets are derived from (never drift from) the Literals."""
    assert VALID_PROGRAMMING_LANGUAGES == frozenset(get_args(ProgrammingLanguage))
    assert VALID_LAUNCH_METHODS == frozenset(get_args(LaunchMethod))
