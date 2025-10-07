"""Tests for formatters/_markdown.py - Markdown formatters with escaping tests."""

import pyarrow as pa

from deephaven_mcp.formatters._markdown import format_markdown_table, format_markdown_kv


# === Markdown Table Tests ===


def test_format_markdown_table_basic():
    """Test basic markdown table formatting with header and separator."""
    table = pa.table({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 25, 35],
    })
    
    result = format_markdown_table(table)
    
    assert isinstance(result, str)
    assert "| id | name | age |" in result
    assert "| --- | --- | --- |" in result
    assert "| 1 | Alice | 30 |" in result
    assert "| 2 | Bob | 25 |" in result
    assert "| 3 | Charlie | 35 |" in result


def test_format_markdown_table_empty():
    """Test markdown table with empty table (header and separator only)."""
    table = pa.table({"id": [], "name": []})
    
    result = format_markdown_table(table)
    
    assert "| id | name |" in result
    assert "| --- | --- |" in result


def test_format_markdown_table_escapes_pipes():
    """Test that markdown table escapes pipe characters in cells."""
    table = pa.table({
        "id": [1, 2],
        "text": ["Normal text", "Text | with | pipes"],
    })
    
    result = format_markdown_table(table)
    
    # Pipes should be escaped as \|
    assert "Text \\| with \\| pipes" in result


def test_format_markdown_table_null_values():
    """Test markdown table with null values."""
    table = pa.table({
        "id": [1, 2, 3],
        "name": ["Alice", None, "Charlie"],
        "value": [10, 20, None],
    })
    
    result = format_markdown_table(table)
    
    assert "| id | name | value |" in result
    assert "| 1 | Alice | 10 |" in result
    # Null should be displayed (as "None" or similar)
    assert "None" in result or "null" in result.lower()


def test_format_markdown_table_single_row():
    """Test markdown table with single row."""
    table = pa.table({"id": [1], "name": ["Alice"]})
    
    result = format_markdown_table(table)
    
    assert "| id | name |" in result
    assert "| --- | --- |" in result
    assert "| 1 | Alice |" in result


def test_format_markdown_table_special_characters():
    """Test markdown table with special characters."""
    table = pa.table({
        "id": [1, 2],
        "text": ["Normal", "With * asterisk"],
    })
    
    result = format_markdown_table(table)
    
    assert "| id | text |" in result
    assert "With * asterisk" in result


# === Markdown Key-Value Tests ===


def test_format_markdown_kv_basic():
    """Test basic markdown key-value format with record headers."""
    table = pa.table({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 25, 35],
    })
    
    result = format_markdown_kv(table)
    
    assert isinstance(result, str)
    assert "## Record 1" in result
    assert "id: 1" in result
    assert "name: Alice" in result
    assert "age: 30" in result
    assert "## Record 2" in result
    assert "name: Bob" in result
    assert "## Record 3" in result
    assert "name: Charlie" in result


def test_format_markdown_kv_empty():
    """Test markdown key-value with empty table (no records)."""
    table = pa.table({"id": [], "name": []})
    
    result = format_markdown_kv(table)
    
    # Should be empty or minimal
    assert result == "" or len(result) < 10


def test_format_markdown_kv_escapes_colons():
    """Test that markdown key-value escapes colons in values."""
    table = pa.table({
        "id": [1, 2],
        "text": ["Normal text", "Text: with: colons"],
    })
    
    result = format_markdown_kv(table)
    
    # Colons in values should be escaped as \:
    assert "Text\\: with\\: colons" in result


def test_format_markdown_kv_null_values():
    """Test markdown key-value with null values."""
    table = pa.table({
        "id": [1, 2, 3],
        "name": ["Alice", None, "Charlie"],
        "value": [10, 20, None],
    })
    
    result = format_markdown_kv(table)
    
    assert "## Record 1" in result
    assert "name: Alice" in result
    assert "## Record 2" in result
    # Null should be displayed
    assert "None" in result or "null" in result.lower()


def test_format_markdown_kv_record_numbering():
    """Test that markdown key-value records are numbered sequentially."""
    table = pa.table({
        "id": [10, 20, 30],
        "name": ["A", "B", "C"],
    })
    
    result = format_markdown_kv(table)
    
    # Records should be numbered 1, 2, 3 (not using id values)
    assert "## Record 1" in result
    assert "## Record 2" in result
    assert "## Record 3" in result
    assert "## Record 4" not in result


def test_format_markdown_kv_single_row():
    """Test markdown key-value with single row."""
    table = pa.table({"id": [1], "name": ["Alice"]})
    
    result = format_markdown_kv(table)
    
    assert "## Record 1" in result
    assert "id: 1" in result
    assert "name: Alice" in result
    assert "## Record 2" not in result


def test_format_markdown_kv_blank_line_separator():
    """Test that markdown key-value separates records with blank lines."""
    table = pa.table({
        "id": [1, 2],
        "name": ["Alice", "Bob"],
    })
    
    result = format_markdown_kv(table)
    
    # Should have blank line between records
    assert "\n\n" in result


def test_format_markdown_kv_column_order():
    """Test that markdown key-value preserves column order."""
    table = pa.table({
        "id": [1],
        "name": ["Alice"],
        "age": [30],
        "city": ["NYC"],
    })
    
    result = format_markdown_kv(table)
    
    # Check that fields appear in order
    id_pos = result.find("id:")
    name_pos = result.find("name:")
    age_pos = result.find("age:")
    city_pos = result.find("city:")
    
    assert id_pos < name_pos < age_pos < city_pos
