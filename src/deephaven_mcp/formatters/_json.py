"""JSON formatters for PyArrow tables."""

import pyarrow as pa


def format_json_row(arrow_table: pa.Table) -> list[dict]:
    """
    Format Arrow table as array of row objects.

    Args:
        arrow_table (pa.Table): PyArrow Table to format

    Returns:
        list[dict]: Array of objects, each representing a row.
                   Example: [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    """
    return arrow_table.to_pylist()


def format_json_column(arrow_table: pa.Table) -> dict:
    """
    Format Arrow table as column-oriented object.

    Args:
        arrow_table (pa.Table): PyArrow Table to format

    Returns:
        dict: Object with column names as keys, arrays as values.
             Example: {"id": [1, 2], "name": ["Alice", "Bob"]}
    """
    return arrow_table.to_pydict()
