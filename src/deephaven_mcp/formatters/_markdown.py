"""Markdown formatters for PyArrow tables."""

import pyarrow as pa


def format_markdown_table(arrow_table: pa.Table) -> str:
    """
    Format Arrow table as markdown table.

    Args:
        arrow_table (pa.Table): PyArrow Table to format

    Returns:
        str: Markdown table string with header and separator rows.

    Example:
        | id | name | age |
        | --- | --- | --- |
        | 1 | Alice | 30 |
        | 2 | Bob | 25 |
    """
    # Get column names
    columns = arrow_table.column_names

    # Build header row
    header = "| " + " | ".join(columns) + " |"

    # Build separator row
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"

    # Build data rows (use iterator to avoid loading entire table into memory)
    rows = []
    for batch in arrow_table.to_batches():
        for row_dict in batch.to_pylist():
            # Escape pipe characters in cell values
            cells = [str(row_dict[col]).replace("|", "\\|") for col in columns]
            row_str = "| " + " | ".join(cells) + " |"
            rows.append(row_str)

    # Combine all parts
    parts = [header, separator] + rows
    return "\n".join(parts)


def format_markdown_kv(arrow_table: pa.Table) -> str:
    """
    Format Arrow table as markdown key-value pairs.

    Highest accuracy format for LLM consumption (60.7% per research).

    Args:
        arrow_table (pa.Table): PyArrow Table to format

    Returns:
        str: Markdown with record headers and key-value pairs.

    Example:
        ## Record 1
        id: 1
        name: Alice
        age: 30

        ## Record 2
        id: 2
        name: Bob
        age: 25
    """
    columns = arrow_table.column_names
    records = []
    idx = 0

    # Use iterator to avoid loading entire table into memory
    for batch in arrow_table.to_batches():
        for row_dict in batch.to_pylist():
            idx += 1
            # Record header
            record_lines = [f"## Record {idx}"]

            # Key-value pairs
            for col in columns:
                value = row_dict[col]
                # Escape colons in values if needed
                value_str = str(value).replace(":", "\\:")
                record_lines.append(f"{col}: {value_str}")

            records.append("\n".join(record_lines))

    # Join records with blank line separator
    return "\n\n".join(records)
