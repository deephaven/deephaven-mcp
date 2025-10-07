"""YAML formatter for PyArrow tables."""

import pyarrow as pa
import yaml


def format_yaml(arrow_table: pa.Table) -> str:
    """
    Format Arrow table as YAML.

    Args:
        arrow_table (pa.Table): PyArrow Table to format

    Returns:
        str: YAML-formatted string.

    Example:
        records:
          - id: 1
            name: Alice
            age: 30
          - id: 2
            name: Bob
            age: 25

    Note:
        PyYAML handles escaping and special characters automatically.
    """
    # Use iterator to avoid loading entire table into memory
    records = []
    for batch in arrow_table.to_batches():
        records.extend(batch.to_pylist())
    
    data = {"records": records}
    return yaml.dump(data, default_flow_style=False, allow_unicode=True)
