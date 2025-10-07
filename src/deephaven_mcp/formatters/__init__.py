"""
Table formatting for MCP responses.

This module provides table data formatting optimized for AI agent consumption, with format
selection based on empirical research showing significant accuracy differences between formats.

Features:
    - Multiple output formats: JSON (row/column), CSV, Markdown (table/kv), YAML, XML
    - Intelligent auto-selection based on table size and optimization goals
    - Research-backed format accuracy rankings (markdown-kv: 60.7%, csv: 44%)
    - Optimization strategies: accuracy, cost (token efficiency), speed
    - Comprehensive format validation with helpful error messages

Format Accuracy Rankings (from research):
    Based on: https://www.improvingagents.com/blog/best-input-data-format-for-llms
    - markdown-kv: 60.7% accuracy (highest, ~2.7x token cost vs CSV)
    - markdown-table: ~55% accuracy (good balance)
    - json-row, json-column: ~50% accuracy
    - yaml: ~50% accuracy
    - xml: ~45% accuracy
    - csv: ~44% accuracy (lowest, most token-efficient)

Auto-Selection Logic:
    - ≤1000 rows: markdown-kv (prioritize accuracy)
    - 1001-10000 rows: markdown-table (balance accuracy and scalability)
    - >10000 rows: csv (prioritize token efficiency)

Supported Formats:
    Explicit Formats (7 total):
        json-row: Array of row objects (returns list[dict])
            Example: [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        
        json-column: Column-oriented object (returns dict)
            Example: {"id": [1, 2], "name": ["Alice", "Bob"]}
        
        csv: Comma-separated values with header (returns str)
            Example: "id,name\\n1,Alice\\n2,Bob\\n"
        
        markdown-table: Pipe-delimited table with alignment (returns str)
            Example: "| id | name |\\n| --- | --- |\\n| 1 | Alice |"
        
        markdown-kv: Key-value pairs per record (returns str, 60.7% AI accuracy)
            Example: "## Record 1\\nid: 1\\nname: Alice\\n\\n## Record 2\\nid: 2\\nname: Bob"
        
        yaml: YAML records list (returns str)
            Example: "records:\\n  - id: 1\\n    name: Alice\\n  - id: 2\\n    name: Bob"
        
        xml: XML with records/record structure (returns str)
            Example: "<records><record id=\\"1\\"><name>Alice</name></record></records>"
    
    Optimization Strategies (4 total):
        auto: Smart selection based on table size (default, recommended)
        optimize-accuracy: Always use markdown-kv (highest accuracy)
        optimize-cost: Always use csv (most token-efficient)
        optimize-speed: Always use json-column (fastest conversion)

Usage:
    >>> from deephaven_mcp.formatters import format_table_data
    >>> import pyarrow as pa
    >>> 
    >>> table = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})
    >>> format_used, data = format_table_data(table, "auto")
    >>> # Returns: ("markdown-kv", "## Record 1\\nid: 1\\nname: Alice\\n...")

Dependencies:
    - pyarrow: Table data structure and CSV conversion
    - pyyaml: YAML formatting support
    - xml.etree.ElementTree: XML formatting (standard library)

Thread Safety:
    All formatter functions are pure functions with no shared state, making them thread-safe.
"""

import logging

import pyarrow as pa

from ._csv import format_csv
from ._json import format_json_column, format_json_row
from ._markdown import format_markdown_kv, format_markdown_table
from ._xml import format_xml
from ._yaml import format_yaml

_LOGGER = logging.getLogger(__name__)

# Format registry - maps format names to formatter functions
_FORMATTERS = {
    "json-row": format_json_row,
    "json-column": format_json_column,
    "csv": format_csv,
    "markdown-table": format_markdown_table,
    "markdown-kv": format_markdown_kv,
    "yaml": format_yaml,
    "xml": format_xml,
}

VALID_FORMATS: set[str] = set(_FORMATTERS.keys()) | {
    "auto",
    "optimize-accuracy",
    "optimize-cost",
    "optimize-speed",
}
"""Valid format names for table data formatting.

This set contains all supported format types: 7 explicit formats and 4 optimization strategies.
Total: 11 valid format names.

Use this constant to validate format names before calling format_table_data().

Type:
    set[str]

Contents:
    - Explicit formats: "json-row", "json-column", "csv", "markdown-table", 
      "markdown-kv", "yaml", "xml"
    - Optimization strategies: "auto", "optimize-accuracy", "optimize-cost", 
      "optimize-speed"

See the module docstring's "Supported Formats" section for detailed descriptions and examples
of each format.

Example:
    >>> if my_format in VALID_FORMATS:
    ...     format_used, data = format_table_data(table, my_format)
    >>> else:
    ...     print(f"Invalid format. Choose from: {sorted(VALID_FORMATS)}")
"""


def format_table_data(
    arrow_table: pa.Table,
    format_type: str,
) -> tuple[str, object]:
    """
    Convert Arrow table to specified format with automatic selection.

    Args:
        arrow_table (pa.Table): PyArrow Table to format
        format_type (str): Format name or optimization strategy. Valid options:
            - "auto": Smart selection balancing accuracy and cost (DEFAULT)
                      ≤1000 rows: markdown-kv (best accuracy)
                      1001-10000 rows: markdown-table (good accuracy, scalable)
                      >10000 rows: csv (cost-effective for large data)
            - "optimize-accuracy": Always use most accurate format (markdown-kv)
            - "optimize-cost": Always use most token-efficient format (csv)
            - "optimize-speed": Always use fastest conversion (json-column)
            - Explicit formats: "json-row", "json-column", "csv", "markdown-table",
                               "markdown-kv", "yaml", "xml"

    Returns:
        tuple[str, object]: A 2-tuple containing:
            - actual_format_used (str): The concrete format name used (e.g., "markdown-kv").
              For optimization strategies like "auto", this will be the resolved format,
              not the strategy name.
            - formatted_data (object): The formatted table data. Type varies by format:
                * json-row: list[dict] - Array of row objects
                * json-column: dict - Column-oriented dictionary
                * csv, markdown-table, markdown-kv, yaml, xml: str - Formatted string

    Raises:
        ValueError: If format_type is not in VALID_FORMATS. The error message includes
                   a comma-separated list of all valid format options for easy reference.

    Examples:
        >>> # Auto selection (smart)
        >>> format, data = format_table_data(table, "auto")
        >>> # format = "markdown-kv" (for tables ≤1000 rows)

        >>> # Always maximize accuracy
        >>> format, data = format_table_data(table, "optimize-accuracy")
        >>> # format = "markdown-kv" (regardless of size)

        >>> # Explicit format
        >>> format, data = format_table_data(table, "csv")
        >>> # format = "csv"
    """
    # Validate format
    if format_type not in VALID_FORMATS:
        valid_list = ", ".join(sorted(VALID_FORMATS))
        _LOGGER.error(
            f"[formatters:format_table_data] Invalid format '{format_type}'. "
            f"Valid options: {valid_list}"
        )
        raise ValueError(
            f"Invalid format '{format_type}'. Valid options: {valid_list}"
        )

    # Get row count from table
    row_count = len(arrow_table)
    col_count = len(arrow_table.schema)
    
    _LOGGER.debug(
        f"[formatters:format_table_data] Formatting table: {row_count} rows, "
        f"{col_count} columns, requested format='{format_type}'"
    )

    # Resolve optimization strategies to actual formats
    actual_format, reason = _resolve_format(format_type, row_count)
    _LOGGER.debug(f"[formatters:format_table_data] Using '{actual_format}' ({reason})")

    # Get formatter and apply
    formatter = _FORMATTERS[actual_format]
    data = formatter(arrow_table)
    
    _LOGGER.debug(
        f"[formatters:format_table_data] Successfully formatted {row_count} rows "
        f"as '{actual_format}'"
    )

    return actual_format, data


def _resolve_format(format_type: str, row_count: int) -> tuple[str, str]:
    """
    Resolve optimization strategy to concrete format name.

    Args:
        format_type (str): Format name or optimization strategy
        row_count (int): Number of rows in the table (used for auto-selection)

    Returns:
        tuple[str, str]: (actual_format, reason)
            - actual_format (str): Concrete format name to use (e.g., "markdown-kv", "csv")
            - reason (str): Human-readable explanation of why this format was chosen.
              Always non-empty. Examples: "≤1000 rows, optimizing for accuracy",
              "optimize-cost strategy", "explicit format: csv"

    Note:
        This function assumes format_type has already been validated against VALID_FORMATS.
    """
    if format_type == "auto":
        # Smart selection: balance accuracy and cost based on size
        if row_count <= 1000:
            return "markdown-kv", "≤1000 rows, optimizing for accuracy"
        elif row_count <= 10000:
            return "markdown-table", "1001-10000 rows, balancing accuracy and scalability"
        else:
            return "csv", ">10000 rows, optimizing for token efficiency"

    elif format_type == "optimize-accuracy":
        return "markdown-kv", "optimize-accuracy strategy"

    elif format_type == "optimize-cost":
        return "csv", "optimize-cost strategy"

    elif format_type == "optimize-speed":
        return "json-column", "optimize-speed strategy"

    else:
        # Explicit format specified
        return format_type, f"explicit format: {format_type}"


__all__ = ["format_table_data", "VALID_FORMATS"]
