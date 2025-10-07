"""Tests for formatters/_xml.py - XML formatter."""

import xml.etree.ElementTree as ET

import pyarrow as pa

from deephaven_mcp.formatters._xml import format_xml


def test_format_xml_basic():
    """Test basic XML formatting with records and record elements."""
    table = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [30, 25, 35],
        }
    )

    result = format_xml(table)

    assert isinstance(result, str)
    assert "<?xml version=" in result
    assert "<records>" in result or "<records " in result
    assert "</records>" in result or "/>" in result
    assert "<name>Alice</name>" in result
    assert "<age>30</age>" in result

    # Verify it's valid XML
    root = ET.fromstring(result)
    assert root.tag == "records"
    assert len(root) == 3


def test_format_xml_empty_table():
    """Test XML formatting with empty table (empty records element)."""
    table = pa.table({"id": [], "name": []})

    result = format_xml(table)

    # Empty records can be <records></records> or <records />
    assert "<records" in result
    assert "/>" in result or "</records>" in result

    # Verify it's valid XML with no record children
    root = ET.fromstring(result)
    assert root.tag == "records"
    assert len(root) == 0


def test_format_xml_special_characters():
    """Test XML with special characters (handled by ElementTree)."""
    table = pa.table(
        {
            "id": [1, 2],
            "text": ["Normal", "Text <with> & special"],
        }
    )

    result = format_xml(table)

    # Verify it's valid XML (ElementTree should escape special chars)
    root = ET.fromstring(result)
    records = list(root)
    assert records[1].find("text").text == "Text <with> & special"


def test_format_xml_null_values():
    """Test XML formatting with null values."""
    table = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", None, "Charlie"],
            "value": [10, 20, None],
        }
    )

    result = format_xml(table)

    # Verify it's valid XML
    root = ET.fromstring(result)
    assert len(root) == 3

    # Check null handling (typically empty element or "None")
    record2 = root[1]
    name_elem = record2.find("name")
    assert name_elem is not None


def test_format_xml_id_column_as_attribute():
    """Test that 'id' column becomes an attribute on record element."""
    table = pa.table(
        {
            "id": [1, 2],
            "name": ["Alice", "Bob"],
        }
    )

    result = format_xml(table)

    root = ET.fromstring(result)
    records = list(root)

    # id should be an attribute, not a child element
    assert records[0].get("id") == "1"
    assert records[1].get("id") == "2"
    assert records[0].find("id") is None  # Should not be a child element


def test_format_xml_single_row():
    """Test XML with single row."""
    table = pa.table({"id": [1], "name": ["Alice"]})

    result = format_xml(table)

    root = ET.fromstring(result)
    assert len(root) == 1
    assert root[0].get("id") == "1"
    assert root[0].find("name").text == "Alice"


def test_format_xml_structure():
    """Test that XML has correct structure."""
    table = pa.table(
        {
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "age": [30, 25],
        }
    )

    result = format_xml(table)

    root = ET.fromstring(result)
    assert root.tag == "records"

    for record in root:
        assert record.tag == "record"
        assert "id" in record.attrib
        assert record.find("name") is not None
        assert record.find("age") is not None


def test_format_xml_without_id_column():
    """Test XML formatting when there's no 'id' column."""
    table = pa.table(
        {
            "name": ["Alice", "Bob"],
            "age": [30, 25],
        }
    )

    result = format_xml(table)

    root = ET.fromstring(result)
    records = list(root)

    # Without id column, no attributes should be set
    assert len(records[0].attrib) == 0
    assert records[0].find("name").text == "Alice"
    assert records[0].find("age").text == "30"
