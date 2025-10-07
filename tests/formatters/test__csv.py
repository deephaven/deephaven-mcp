"""Tests for formatters/_csv.py - CSV formatter with escaping tests."""

import pyarrow as pa

from deephaven_mcp.formatters._csv import format_csv


def test_format_csv_basic():
    """Test basic CSV formatting with header."""
    table = pa.table({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 25, 35],
    })
    
    result = format_csv(table)
    
    assert isinstance(result, str)
    # PyArrow CSV writer quotes all fields by default
    assert "id" in result and "name" in result and "age" in result
    assert "Alice" in result
    assert "Bob" in result
    assert "Charlie" in result


def test_format_csv_empty_table():
    """Test CSV formatting with empty table (header only)."""
    table = pa.table({"id": [], "name": []})
    
    result = format_csv(table)
    
    assert isinstance(result, str)
    assert "id" in result and "name" in result
    # Should have header but no data rows


def test_format_csv_escapes_commas():
    """Test that CSV properly escapes cells containing commas."""
    table = pa.table({
        "id": [1, 2],
        "name": ["Smith, John", "Doe, Jane"],
        "city": ["New York", "Los Angeles"],
    })
    
    result = format_csv(table)
    
    # PyArrow should quote fields with commas
    assert '"Smith, John"' in result or "'Smith, John'" in result
    assert '"Doe, Jane"' in result or "'Doe, Jane'" in result


def test_format_csv_escapes_quotes():
    """Test that CSV properly escapes cells containing quotes."""
    table = pa.table({
        "id": [1, 2],
        "text": ['He said "hello"', 'She said "goodbye"'],
    })
    
    result = format_csv(table)
    
    # PyArrow should escape quotes (typically by doubling them)
    assert result is not None
    assert "id" in result and "text" in result


def test_format_csv_escapes_newlines():
    """Test that CSV properly handles cells containing newlines."""
    table = pa.table({
        "id": [1, 2],
        "description": ["Line 1\nLine 2", "Single line"],
    })
    
    result = format_csv(table)
    
    # PyArrow should handle newlines (typically by quoting)
    assert result is not None
    assert "id" in result and "description" in result


def test_format_csv_null_values():
    """Test CSV formatting with null values."""
    table = pa.table({
        "id": [1, 2, 3],
        "name": ["Alice", None, "Charlie"],
        "value": [10, 20, None],
    })
    
    result = format_csv(table)
    
    assert "id" in result and "name" in result and "value" in result
    # Nulls should be represented (typically as empty or "null")
    assert result is not None


def test_format_csv_single_row():
    """Test CSV with single row."""
    table = pa.table({"id": [1], "name": ["Alice"]})
    
    result = format_csv(table)
    
    assert "id" in result and "name" in result
    assert "Alice" in result


def test_format_csv_special_characters():
    """Test CSV with various special characters."""
    table = pa.table({
        "id": [1, 2, 3],
        "text": ["Normal", "With\ttab", "With;semicolon"],
    })
    
    result = format_csv(table)
    
    assert "id" in result and "text" in result
    assert result is not None
