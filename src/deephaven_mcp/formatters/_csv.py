"""CSV formatter for PyArrow tables."""

import io
import pyarrow as pa
import pyarrow.csv as csv


def format_csv(arrow_table: pa.Table) -> str:
    """
    Format Arrow table as CSV string.

    Uses PyArrow's native CSV writer for memory efficiency and proper escaping.

    Args:
        arrow_table (pa.Table): PyArrow Table to format

    Returns:
        str: CSV-formatted string with header row.
             Example: "id,name\\n1,Alice\\n2,Bob\\n"

    Note:
        PyArrow's csv.write_csv() handles escaping automatically:
        - Commas in values are quoted
        - Quotes in values are escaped
        - Newlines in values are preserved within quotes
    """
    output = io.BytesIO()
    csv.write_csv(arrow_table, output)
    return output.getvalue().decode("utf-8")
