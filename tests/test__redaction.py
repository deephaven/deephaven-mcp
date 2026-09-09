"""Tests for deephaven_mcp._redaction."""

import json

import pytest

from deephaven_mcp import _redaction
from deephaven_mcp._redaction import (
    REDACTED,
    UNPARSEABLE,
    AllowedField,
    parse_json_strict,
    project_json_fields,
    redact_json_sensitive_fields,
)


def test_redacted_has_canonical_value():
    """Pin down the canonical redaction marker.

    Every call site in the codebase substitutes this exact string for
    sensitive values. Changing it is almost certainly a mistake (log
    parsers, monitoring rules, and documentation all assume this
    literal), so this test fails loudly if the value is ever flipped.
    """
    assert REDACTED == "[REDACTED]"


def test_unparseable_has_canonical_value():
    """Pin down the canonical suppression marker, for the same reason."""
    assert UNPARSEABLE == "[UNPARSEABLE]"


def test_all_lists_the_public_surface():
    """The markers, the result type, and the two redactors are the public symbols."""
    assert _redaction.__all__ == [
        "REDACTED",
        "AllowedField",
        "Redaction",
        "UNPARSEABLE",
        "parse_json_strict",
        "project_json_fields",
        "redact_json_sensitive_fields",
    ]


_SAMPLE_FIELDS = (
    AllowedField("flag", bool),
    AllowedField("note", str),
    AllowedField("credential", str, secret=True),
)


@pytest.mark.parametrize(
    "stored,text,withheld",
    [
        ('{"flag": true}', '{"flag": true}', False),
        ('{"flag":true,"note":"hi"}', '{"flag": true, "note": "hi"}', False),
        ('{"credential": "tok"}', '{"credential": "[REDACTED]"}', True),
        # A secret is marked on presence alone, whatever type it turns out to hold.
        ('{"credential": {"a": 1}}', '{"credential": "[REDACTED]"}', True),
        ('{"unknown": "tok"}', "{}", True),
        ('{"flag": "not a bool"}', "{}", True),
        ('{"flag": true, "flag": true}', "[UNPARSEABLE]", True),
        ('{"flag": NaN}', "[UNPARSEABLE]", True),
        ("not json", "[UNPARSEABLE]", True),
        ("[1, 2]", "[UNPARSEABLE]", True),
        ("{}", "{}", False),
    ],
    ids=[
        "allowed-value-reported",
        "order-follows-the-declaration",
        "secret-replaced",
        "secret-not-inspected",
        "unknown-key-dropped",
        "wrong-type-dropped",
        "repeated-key-suppressed",
        "non-standard-constant-suppressed",
        "unparseable-suppressed",
        "non-object-suppressed",
        "empty-object",
    ],
)
def test_project_json_fields(stored, text, withheld):
    """Output is rebuilt from the declaration, so nothing undeclared is echoed."""
    assert project_json_fields(stored, _SAMPLE_FIELDS, label="sample") == (
        text,
        withheld,
    )


def test_parse_json_strict_accepts_distinct_keys():
    assert parse_json_strict('{"a": 1, "b": {"c": 2}}') == {
        "a": 1,
        "b": {"c": 2},
    }


@pytest.mark.parametrize(
    "text",
    [
        '{"a": 1, "a": 2}',
        '{"outer": {"a": 1, "a": 2}}',
        "not json",
        '{"a": NaN}',
        '{"a": Infinity}',
        '{"a": -Infinity}',
        "[NaN]",
    ],
    ids=[
        "repeated-key",
        "repeated-key-nested",
        "malformed",
        "nan",
        "infinity",
        "negative-infinity",
        "nan-in-an-array",
    ],
)
def test_parse_json_strict_rejects(text):
    """A repeated key or a non-standard constant is refused at any depth."""
    with pytest.raises(ValueError):
        parse_json_strict(text)


def test_redact_json_sensitive_fields_rejects_a_non_standard_constant():
    """json.dumps would write NaN back, so the value would not be valid JSON."""
    assert redact_json_sensitive_fields('{"port": NaN}') == ("[UNPARSEABLE]", True)


def test_redact_json_sensitive_fields_none_returns_none():
    assert redact_json_sensitive_fields(None) == (None, False)
    assert redact_json_sensitive_fields("") == (None, False)


def test_redact_json_sensitive_fields_unparseable():
    assert redact_json_sensitive_fields("not json") == ("[UNPARSEABLE]", True)


def test_redact_json_sensitive_fields_rejects_a_repeated_key():
    """A repeated key loses a value on parse, so the whole document is suppressed."""
    assert redact_json_sensitive_fields('{"a": 1, "a": 2}') == ("[UNPARSEABLE]", True)


def test_redact_json_sensitive_fields_withholds_nothing_without_a_sensitive_key():
    result = redact_json_sensitive_fields('{"port": 10000}')
    assert result.withheld is False
    assert result.text is not None


def test_redact_json_sensitive_fields_redacts_known_keys():
    raw = json.dumps(
        {
            "password": "secret-pw",
            "token": "abc",
            "nested": {"api_key": "k", "ok": "keep-me"},
            "items": [{"secret": "s", "x": 1}],
        }
    )
    out = redact_json_sensitive_fields(raw)
    assert out.withheld is True
    assert out.text is not None
    parsed = json.loads(out.text)
    assert parsed["password"] == "[REDACTED]"
    assert parsed["token"] == "[REDACTED]"
    assert parsed["nested"]["api_key"] == "[REDACTED]"
    assert parsed["nested"]["ok"] == "keep-me"
    assert parsed["items"][0]["secret"] == "[REDACTED]"
    assert parsed["items"][0]["x"] == 1
