"""Tests for formatters/__init__.py - format_table_data() and auto-selection logic."""

import pytest
import pyarrow as pa

from deephaven_mcp.formatters import format_table_data, VALID_FORMATS
from deephaven_mcp.formatters import _resolve_format


# Helper to create test tables
def create_test_table(rows: int) -> pa.Table:
    """Create a simple test table with specified number of rows."""
    return pa.table({
        "id": list(range(1, rows + 1)),
        "name": [f"Name{i}" for i in range(1, rows + 1)],
        "value": [i * 10 for i in range(1, rows + 1)],
    })


# === VALID_FORMATS constant tests ===


def test_valid_formats_contains_all_expected():
    """Test that VALID_FORMATS contains all expected format names."""
    expected = {
        "auto",
        "optimize-accuracy",
        "optimize-cost",
        "optimize-speed",
        "json-row",
        "json-column",
        "csv",
        "markdown-table",
        "markdown-kv",
        "yaml",
        "xml",
    }
    assert VALID_FORMATS == expected


# === _resolve_format() unit tests ===


def test_resolve_format_auto_small_table():
    """Test _resolve_format with auto for small table (≤1000 rows)."""
    actual_format, reason = _resolve_format("auto", 500)
    assert actual_format == "markdown-kv"
    assert reason == "≤1000 rows, optimizing for accuracy"


def test_resolve_format_auto_at_1000_rows():
    """Test _resolve_format with auto at exactly 1000 rows."""
    actual_format, reason = _resolve_format("auto", 1000)
    assert actual_format == "markdown-kv"
    assert reason == "≤1000 rows, optimizing for accuracy"


def test_resolve_format_auto_medium_table():
    """Test _resolve_format with auto for medium table (1001-10000 rows)."""
    actual_format, reason = _resolve_format("auto", 5000)
    assert actual_format == "markdown-table"
    assert reason == "1001-10000 rows, balancing accuracy and scalability"


def test_resolve_format_auto_at_10000_rows():
    """Test _resolve_format with auto at exactly 10000 rows."""
    actual_format, reason = _resolve_format("auto", 10000)
    assert actual_format == "markdown-table"
    assert reason == "1001-10000 rows, balancing accuracy and scalability"


def test_resolve_format_auto_large_table():
    """Test _resolve_format with auto for large table (>10000 rows)."""
    actual_format, reason = _resolve_format("auto", 15000)
    assert actual_format == "csv"
    assert reason == ">10000 rows, optimizing for token efficiency"


def test_resolve_format_optimize_accuracy():
    """Test _resolve_format with optimize-accuracy strategy."""
    actual_format, reason = _resolve_format("optimize-accuracy", 50000)
    assert actual_format == "markdown-kv"
    assert reason == "optimize-accuracy strategy"


def test_resolve_format_optimize_cost():
    """Test _resolve_format with optimize-cost strategy."""
    actual_format, reason = _resolve_format("optimize-cost", 100)
    assert actual_format == "csv"
    assert reason == "optimize-cost strategy"


def test_resolve_format_optimize_speed():
    """Test _resolve_format with optimize-speed strategy."""
    actual_format, reason = _resolve_format("optimize-speed", 1000)
    assert actual_format == "json-column"
    assert reason == "optimize-speed strategy"


def test_resolve_format_explicit_json_row():
    """Test _resolve_format with explicit json-row format."""
    actual_format, reason = _resolve_format("json-row", 100)
    assert actual_format == "json-row"
    assert reason == "explicit format: json-row"


def test_resolve_format_explicit_csv():
    """Test _resolve_format with explicit csv format."""
    actual_format, reason = _resolve_format("csv", 5000)
    assert actual_format == "csv"
    assert reason == "explicit format: csv"


def test_resolve_format_explicit_markdown_kv():
    """Test _resolve_format with explicit markdown-kv format."""
    actual_format, reason = _resolve_format("markdown-kv", 20000)
    assert actual_format == "markdown-kv"
    assert reason == "explicit format: markdown-kv"


def test_resolve_format_auto_with_zero_rows():
    """Test _resolve_format with auto for empty table."""
    actual_format, reason = _resolve_format("auto", 0)
    assert actual_format == "markdown-kv"
    assert reason == "≤1000 rows, optimizing for accuracy"


# === Auto-selection tests ===


def test_auto_format_small_table():
    """Test auto format selection for small table (≤1000 rows) → markdown-kv."""
    table = create_test_table(500)
    actual_format, data = format_table_data(table, "auto")
    
    assert actual_format == "markdown-kv"
    assert isinstance(data, str)
    assert "## Record 1" in data


def test_auto_format_at_1000_rows():
    """Test auto format selection at exactly 1000 rows → markdown-kv."""
    table = create_test_table(1000)
    actual_format, data = format_table_data(table, "auto")
    
    assert actual_format == "markdown-kv"
    assert isinstance(data, str)


def test_auto_format_medium_table():
    """Test auto format selection for medium table (1001-10000 rows) → markdown-table."""
    table = create_test_table(5000)
    actual_format, data = format_table_data(table, "auto")
    
    assert actual_format == "markdown-table"
    assert isinstance(data, str)
    assert "| id | name | value |" in data


def test_auto_format_at_10000_rows():
    """Test auto format selection at exactly 10000 rows → markdown-table."""
    table = create_test_table(10000)
    actual_format, data = format_table_data(table, "auto")
    
    assert actual_format == "markdown-table"


def test_auto_format_large_table():
    """Test auto format selection for large table (>10000 rows) → csv."""
    table = create_test_table(15000)
    actual_format, data = format_table_data(table, "auto")
    
    assert actual_format == "csv"
    assert isinstance(data, str)
    assert "id" in data and "name" in data and "value" in data


# === Optimization strategy tests ===


def test_optimize_accuracy_small_table():
    """Test optimize-accuracy always returns markdown-kv for small tables."""
    table = create_test_table(100)
    actual_format, data = format_table_data(table, "optimize-accuracy")
    
    assert actual_format == "markdown-kv"


def test_optimize_accuracy_large_table():
    """Test optimize-accuracy always returns markdown-kv even for large tables."""
    table = create_test_table(50000)
    actual_format, data = format_table_data(table, "optimize-accuracy")
    
    assert actual_format == "markdown-kv"


def test_optimize_cost_small_table():
    """Test optimize-cost always returns csv for small tables."""
    table = create_test_table(100)
    actual_format, data = format_table_data(table, "optimize-cost")
    
    assert actual_format == "csv"


def test_optimize_cost_large_table():
    """Test optimize-cost always returns csv for large tables."""
    table = create_test_table(50000)
    actual_format, data = format_table_data(table, "optimize-cost")
    
    assert actual_format == "csv"


def test_optimize_speed_always_json_column():
    """Test optimize-speed always returns json-column (fastest)."""
    small_table = create_test_table(100)
    large_table = create_test_table(50000)
    
    # Small table
    actual_format, data = format_table_data(small_table, "optimize-speed")
    assert actual_format == "json-column"
    assert isinstance(data, dict)
    
    # Large table
    actual_format, data = format_table_data(large_table, "optimize-speed")
    assert actual_format == "json-column"


# === Explicit format tests ===


def test_explicit_format_json_row():
    """Test explicit json-row format."""
    table = create_test_table(3)
    actual_format, data = format_table_data(table, "json-row")
    
    assert actual_format == "json-row"
    assert isinstance(data, list)
    assert len(data) == 3
    assert data[0] == {"id": 1, "name": "Name1", "value": 10}


def test_explicit_format_json_column():
    """Test explicit json-column format."""
    table = create_test_table(3)
    actual_format, data = format_table_data(table, "json-column")
    
    assert actual_format == "json-column"
    assert isinstance(data, dict)
    assert data["id"] == [1, 2, 3]
    assert data["name"] == ["Name1", "Name2", "Name3"]


def test_explicit_format_csv():
    """Test explicit csv format."""
    table = create_test_table(3)
    actual_format, data = format_table_data(table, "csv")
    
    assert actual_format == "csv"
    assert isinstance(data, str)
    assert "id" in data and "name" in data and "value" in data
    assert "Name1" in data


def test_explicit_format_markdown_table():
    """Test explicit markdown-table format."""
    table = create_test_table(3)
    actual_format, data = format_table_data(table, "markdown-table")
    
    assert actual_format == "markdown-table"
    assert isinstance(data, str)
    assert "| id | name | value |" in data
    assert "| --- | --- | --- |" in data


def test_explicit_format_markdown_kv():
    """Test explicit markdown-kv format."""
    table = create_test_table(3)
    actual_format, data = format_table_data(table, "markdown-kv")
    
    assert actual_format == "markdown-kv"
    assert isinstance(data, str)
    assert "## Record 1" in data
    assert "id: 1" in data


def test_explicit_format_yaml():
    """Test explicit yaml format."""
    table = create_test_table(3)
    actual_format, data = format_table_data(table, "yaml")
    
    assert actual_format == "yaml"
    assert isinstance(data, str)
    assert "records:" in data
    assert "id: 1" in data


def test_explicit_format_xml():
    """Test explicit xml format."""
    table = create_test_table(3)
    actual_format, data = format_table_data(table, "xml")
    
    assert actual_format == "xml"
    assert isinstance(data, str)
    assert '<?xml version=' in data
    assert "<records" in data


# === Validation tests ===


def test_invalid_format_raises_value_error():
    """Test that invalid format raises ValueError with helpful message."""
    table = create_test_table(3)
    
    with pytest.raises(ValueError) as exc_info:
        format_table_data(table, "invalid-format")
    
    error_msg = str(exc_info.value)
    assert "Invalid format 'invalid-format'" in error_msg
    assert "Valid options:" in error_msg
    assert "json-row" in error_msg


def test_invalid_format_lists_all_valid_formats():
    """Test that error message lists all valid formats."""
    table = create_test_table(3)
    
    with pytest.raises(ValueError) as exc_info:
        format_table_data(table, "bad-format")
    
    error_msg = str(exc_info.value)
    # Check that all valid formats are mentioned
    for fmt in ["auto", "json-row", "csv", "markdown-kv", "optimize-accuracy"]:
        assert fmt in error_msg


# === Return value tests ===


def test_returns_tuple_of_format_and_data():
    """Test that format_table_data returns (str, object) tuple."""
    table = create_test_table(3)
    result = format_table_data(table, "json-row")
    
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], str)


def test_actual_format_matches_returned_format():
    """Test that first element of tuple matches the actual format used."""
    table = create_test_table(3)
    
    # Test with auto (should return actual format, not "auto")
    actual_format, data = format_table_data(table, "auto")
    assert actual_format in ["markdown-kv", "markdown-table", "csv"]
    assert actual_format != "auto"
    
    # Test with explicit format
    actual_format, data = format_table_data(table, "csv")
    assert actual_format == "csv"


# === Edge case tests ===


def test_empty_table():
    """Test formatting empty table."""
    empty_table = pa.table({"id": [], "name": []})
    
    actual_format, data = format_table_data(empty_table, "json-row")
    assert actual_format == "json-row"
    assert data == []


def test_single_row_table():
    """Test formatting single row table."""
    table = create_test_table(1)
    
    actual_format, data = format_table_data(table, "json-row")
    assert actual_format == "json-row"
    assert len(data) == 1


def test_auto_with_zero_rows():
    """Test auto selection with zero rows."""
    empty_table = pa.table({"id": [], "name": []})
    
    actual_format, data = format_table_data(empty_table, "auto")
    assert actual_format == "markdown-kv"  # Should use markdown-kv for 0 rows
