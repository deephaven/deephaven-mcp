"""Tests for deephaven_mcp._redaction."""

from deephaven_mcp import _redaction
from deephaven_mcp._redaction import REDACTED


def test_redacted_has_canonical_value():
    """Pin down the canonical redaction marker.

    Every call site in the codebase substitutes this exact string for
    sensitive values. Changing it is almost certainly a mistake (log
    parsers, monitoring rules, and documentation all assume this
    literal), so this test fails loudly if the value is ever flipped.
    """
    assert REDACTED == "[REDACTED]"


def test_redacted_is_in_all():
    """``REDACTED`` is the module's sole public symbol."""
    assert _redaction.__all__ == ["REDACTED"]
