"""Tests for formatters/__init__.py - format_table_data() and optimization strategies."""

import pyarrow as pa
import pytest

from deephaven_mcp.formatters import VALID_FORMATS, _resolve_format, format_table_data


# Helper to create test tables
def create_test_table(rows: int) -> pa.Table:
    """Create a simple test table with specified number of rows."""
    return pa.table(
        {
            "id": list(range(1, rows + 1)),
            "name": [f"Name{i}" for i in range(1, rows + 1)],
            "value": [i * 10 for i in range(1, rows + 1)],
        }
    )


# === VALID_FORMATS constant tests ===


def test_valid_formats_contains_all_expected():
    """Test that VALID_FORMATS contains all expected format names."""
    expected = {
        "optimize-rendering",
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


def test_resolve_format_optimize_rendering():
    """Test _resolve_format with optimize-rendering strategy."""
    actual_format, reason = _resolve_format("optimize-rendering")
    assert actual_format == "markdown-table"
    assert reason == "optimize-rendering strategy"


def test_resolve_format_optimize_accuracy():
    """Test _resolve_format with optimize-accuracy strategy."""
    actual_format, reason = _resolve_format("optimize-accuracy")
    assert actual_format == "markdown-kv"
    assert reason == "optimize-accuracy strategy"


def test_resolve_format_optimize_cost():
    """Test _resolve_format with optimize-cost strategy."""
    actual_format, reason = _resolve_format("optimize-cost")
    assert actual_format == "csv"
    assert reason == "optimize-cost strategy"


def test_resolve_format_optimize_speed():
    """Test _resolve_format with optimize-speed strategy."""
    actual_format, reason = _resolve_format("optimize-speed")
    assert actual_format == "json-column"
    assert reason == "optimize-speed strategy"


def test_resolve_format_explicit_json_row():
    """Test _resolve_format with explicit json-row format."""
    actual_format, reason = _resolve_format("json-row")
    assert actual_format == "json-row"
    assert reason == "explicit format: json-row"


def test_resolve_format_explicit_csv():
    """Test _resolve_format with explicit csv format."""
    actual_format, reason = _resolve_format("csv")
    assert actual_format == "csv"
    assert reason == "explicit format: csv"


def test_resolve_format_explicit_markdown_kv():
    """Test _resolve_format with explicit markdown-kv format."""
    actual_format, reason = _resolve_format("markdown-kv")
    assert actual_format == "markdown-kv"
    assert reason == "explicit format: markdown-kv"


# === Optimization strategy tests ===


def test_optimize_rendering_always_markdown_table():
    """Test optimize-rendering always returns markdown-table."""
    small_table = create_test_table(100)
    large_table = create_test_table(50000)

    # Small table
    actual_format, data = format_table_data(small_table, "optimize-rendering")
    assert actual_format == "markdown-table"
    assert isinstance(data, str)
    assert "|" in data

    # Large table
    actual_format, data = format_table_data(large_table, "optimize-rendering")
    assert actual_format == "markdown-table"


def test_optimize_accuracy_always_markdown_kv():
    """Test optimize-accuracy always returns markdown-kv."""
    small_table = create_test_table(100)
    large_table = create_test_table(50000)

    # Small table
    actual_format, data = format_table_data(small_table, "optimize-accuracy")
    assert actual_format == "markdown-kv"

    # Large table
    actual_format, data = format_table_data(large_table, "optimize-accuracy")
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
    assert "<?xml version=" in data
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
    for fmt in [
        "optimize-rendering",
        "json-row",
        "csv",
        "markdown-kv",
        "optimize-accuracy",
    ]:
        assert fmt in error_msg


# === Return value tests ===


def test_returns_tuple_of_format_and_data():
    """Test that format_table_data returns (str, object) tuple."""
    table = create_test_table(3)
    result = format_table_data(table, "json-row")

    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], str)


def test_format_tuple_first_element_is_actual_format():
    """Test that first element of tuple matches the actual format used."""
    table = create_test_table(3)

    # Test with optimization strategy (should return actual format, not strategy name)
    actual_format, data = format_table_data(table, "optimize-rendering")
    assert actual_format == "markdown-table"
    assert actual_format != "optimize-rendering"

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


def test_optimize_rendering_with_zero_rows():
    """Test optimize-rendering with zero rows."""
    empty_table = pa.table({"id": [], "name": []})

    actual_format, data = format_table_data(empty_table, "optimize-rendering")
    assert actual_format == "markdown-table"  # Should use markdown-table
