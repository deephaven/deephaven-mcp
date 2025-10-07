"""Tests for formatters/_json.py - JSON formatters."""

import pyarrow as pa

from deephaven_mcp.formatters._json import format_json_column, format_json_row


def test_format_json_row_basic():
    """Test basic json-row formatting."""
    table = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [30, 25, 35],
        }
    )

    result = format_json_row(table)

    assert isinstance(result, list)
    assert len(result) == 3
    assert result[0] == {"id": 1, "name": "Alice", "age": 30}
    assert result[1] == {"id": 2, "name": "Bob", "age": 25}
    assert result[2] == {"id": 3, "name": "Charlie", "age": 35}


def test_format_json_row_empty_table():
    """Test json-row formatting with empty table."""
    table = pa.table({"id": [], "name": []})

    result = format_json_row(table)

    assert isinstance(result, list)
    assert len(result) == 0


def test_format_json_row_null_values():
    """Test json-row formatting with null values."""
    table = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", None, "Charlie"],
            "value": [10, 20, None],
        }
    )

    result = format_json_row(table)

    assert result[0] == {"id": 1, "name": "Alice", "value": 10}
    assert result[1] == {"id": 2, "name": None, "value": 20}
    assert result[2] == {"id": 3, "name": "Charlie", "value": None}


def test_format_json_column_basic():
    """Test basic json-column formatting."""
    table = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [30, 25, 35],
        }
    )

    result = format_json_column(table)

    assert isinstance(result, dict)
    assert result["id"] == [1, 2, 3]
    assert result["name"] == ["Alice", "Bob", "Charlie"]
    assert result["age"] == [30, 25, 35]


def test_format_json_column_empty_table():
    """Test json-column formatting with empty table."""
    table = pa.table({"id": [], "name": []})

    result = format_json_column(table)

    assert isinstance(result, dict)
    assert result["id"] == []
    assert result["name"] == []


def test_format_json_column_null_values():
    """Test json-column formatting with null values."""
    table = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", None, "Charlie"],
            "value": [10, 20, None],
        }
    )

    result = format_json_column(table)

    assert result["id"] == [1, 2, 3]
    assert result["name"] == ["Alice", None, "Charlie"]
    assert result["value"] == [10, 20, None]


def test_format_json_row_single_row():
    """Test json-row with single row."""
    table = pa.table({"id": [1], "name": ["Alice"]})

    result = format_json_row(table)

    assert len(result) == 1
    assert result[0] == {"id": 1, "name": "Alice"}


def test_format_json_column_single_row():
    """Test json-column with single row."""
    table = pa.table({"id": [1], "name": ["Alice"]})

    result = format_json_column(table)

    assert result == {"id": [1], "name": ["Alice"]}
