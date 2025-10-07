"""Tests for formatters/_yaml.py - YAML formatter."""

import pyarrow as pa
import yaml

from deephaven_mcp.formatters._yaml import format_yaml


def test_format_yaml_basic():
    """Test basic YAML formatting with records array."""
    table = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [30, 25, 35],
        }
    )

    result = format_yaml(table)

    assert isinstance(result, str)
    assert "records:" in result
    assert "id: 1" in result
    assert "name: Alice" in result
    assert "age: 30" in result

    # Verify it's valid YAML
    parsed = yaml.safe_load(result)
    assert "records" in parsed
    assert len(parsed["records"]) == 3


def test_format_yaml_empty_table():
    """Test YAML formatting with empty table (empty records array)."""
    table = pa.table({"id": [], "name": []})

    result = format_yaml(table)

    assert "records:" in result

    # Verify it's valid YAML with empty records
    parsed = yaml.safe_load(result)
    assert "records" in parsed
    assert len(parsed["records"]) == 0


def test_format_yaml_special_characters():
    """Test YAML with special characters (handled by PyYAML)."""
    table = pa.table(
        {
            "id": [1, 2],
            "text": ["Normal text", "Text: with: colons"],
        }
    )

    result = format_yaml(table)

    # Verify it's valid YAML
    parsed = yaml.safe_load(result)
    assert parsed["records"][1]["text"] == "Text: with: colons"


def test_format_yaml_null_values():
    """Test YAML formatting with null values."""
    table = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", None, "Charlie"],
            "value": [10, 20, None],
        }
    )

    result = format_yaml(table)

    # Verify it's valid YAML
    parsed = yaml.safe_load(result)
    assert parsed["records"][0]["name"] == "Alice"
    assert parsed["records"][1]["name"] is None
    assert parsed["records"][2]["value"] is None


def test_format_yaml_unicode():
    """Test YAML with unicode characters."""
    table = pa.table(
        {
            "id": [1, 2],
            "name": ["Alice", "José"],
            "emoji": ["😀", "🎉"],
        }
    )

    result = format_yaml(table)

    # Verify it's valid YAML with unicode preserved
    parsed = yaml.safe_load(result)
    assert parsed["records"][1]["name"] == "José"
    assert parsed["records"][0]["emoji"] == "😀"


def test_format_yaml_single_row():
    """Test YAML with single row."""
    table = pa.table({"id": [1], "name": ["Alice"]})

    result = format_yaml(table)

    parsed = yaml.safe_load(result)
    assert len(parsed["records"]) == 1
    assert parsed["records"][0] == {"id": 1, "name": "Alice"}


def test_format_yaml_structure():
    """Test that YAML has correct structure."""
    table = pa.table(
        {
            "id": [1, 2],
            "name": ["Alice", "Bob"],
        }
    )

    result = format_yaml(table)

    parsed = yaml.safe_load(result)
    assert isinstance(parsed, dict)
    assert "records" in parsed
    assert isinstance(parsed["records"], list)
    assert all(isinstance(record, dict) for record in parsed["records"])
