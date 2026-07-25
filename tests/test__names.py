"""Tests for :mod:`deephaven_mcp._names`.

Covers:

- :data:`_RESOURCE_NAME_PATTERN` membership.
- :func:`validate_resource_name` accept/reject behavior and error text.
"""

from __future__ import annotations

import pytest

from deephaven_mcp._exceptions import InvalidSessionNameError
from deephaven_mcp._names import (
    _RESOURCE_NAME_PATTERN,
    validate_resource_name,
)

# ---------------------------------------------------------------------------
# _RESOURCE_NAME_PATTERN
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "a",
        "A",
        "0",
        "abc",
        "ABC",
        "abc123",
        "a_b-c",
        "name_with-dashes",
        "9start",
    ],
)
def test_resource_name_pattern_matches_valid_values(value: str) -> None:
    assert _RESOURCE_NAME_PATTERN.match(value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "has space",
        "with/slash",
        "with:colon",
        "-leading-dash",
        ".leading-dot",
        "_leading-underscore",  # underscore is allowed in body, not at start
        "unicode\u00e9",
        "a.b.c",  # dots are reserved for config-path separators
        "prod.us-east",
        "trailing.",
    ],
)
def test_resource_name_pattern_rejects_invalid_values(value: str) -> None:
    # First character must be alphanumeric (no leading `_`, `-`, or `.`).
    assert _RESOURCE_NAME_PATTERN.match(value) is None


# ---------------------------------------------------------------------------
# validate_resource_name
# ---------------------------------------------------------------------------


def test_validate_resource_name_returns_value_unchanged() -> None:
    assert validate_resource_name("ok-name_1", field="system") == "ok-name_1"


def test_validate_resource_name_rejects_empty() -> None:
    with pytest.raises(InvalidSessionNameError) as exc:
        validate_resource_name("", field="session_name")
    assert "session_name" in str(exc.value)
    assert "empty" in str(exc.value)


def test_validate_resource_name_rejects_leading_dash() -> None:
    with pytest.raises(InvalidSessionNameError) as exc:
        validate_resource_name("-bad", field="system")
    # Error explains *why*: starts with `-`.
    assert "must not start with" in str(exc.value) or "illegal character" in str(
        exc.value
    )


def test_validate_resource_name_rejects_leading_dot() -> None:
    with pytest.raises(InvalidSessionNameError):
        validate_resource_name(".bad", field="system")


def test_validate_resource_name_reports_illegal_characters() -> None:
    with pytest.raises(InvalidSessionNameError) as exc:
        validate_resource_name("a b/c", field="session_name")
    msg = str(exc.value)
    assert "session_name" in msg
    assert "illegal character" in msg
    # The bad-char list should reference the actual offending characters.
    assert "' '" in msg
    assert "'/'" in msg


def test_validate_resource_name_rejects_dots_with_rename_hint() -> None:
    with pytest.raises(InvalidSessionNameError) as exc:
        validate_resource_name("prod.us-east", field="system_name")
    msg = str(exc.value)
    assert "system_name" in msg
    assert "'.'" in msg
    # The error must tell the user how to fix the name.
    assert "rename" in msg
