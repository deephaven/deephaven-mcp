"""XML formatter for PyArrow tables."""

import xml.etree.ElementTree as ET
import pyarrow as pa


def format_xml(arrow_table: pa.Table) -> str:
    """
    Format Arrow table as XML.

    Args:
        arrow_table (pa.Table): PyArrow Table to format

    Returns:
        str: XML-formatted string.

    Example:
        <?xml version="1.0" ?>
        <records>
          <record id="1">
            <name>Alice</name>
            <age>30</age>
          </record>
          <record id="2">
            <name>Bob</name>
            <age>25</age>
          </record>
        </records>

    Note:
        ElementTree handles XML escaping automatically for special characters.
    """
    root = ET.Element("records")

    # Use iterator to avoid loading entire table into memory
    for batch in arrow_table.to_batches():
        for row_dict in batch.to_pylist():
            record = ET.SubElement(root, "record")

            for key, value in row_dict.items():
                # Use first column as attribute if it's named 'id'
                if key.lower() == "id":
                    record.set("id", str(value))
                else:
                    child = ET.SubElement(record, key)
                    child.text = str(value)

    # Convert to string with XML declaration
    return ET.tostring(root, encoding="unicode", xml_declaration=True)
