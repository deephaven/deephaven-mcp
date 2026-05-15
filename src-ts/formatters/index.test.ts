/**
 * Tests for formatters/index module.
 */
import { describe, it, expect } from "vitest";
import { Table, makeTable, vectorFromArray } from "apache-arrow";
import {
  VALID_FORMATS,
  formatTableData,
  formatJsonRow,
  formatJsonColumn,
  formatCsv,
  formatMarkdownTable,
  formatMarkdownKv,
  formatYaml,
  formatXml,
  _resolveFormat,
} from "./index.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Create a simple test table with id and name columns. */
function makeTestTable(): Table {
  return makeTable({
    id: vectorFromArray([1, 2, 3]),
    name: vectorFromArray(["Alice", "Bob", "Charlie"]),
  });
}

/** Create a single-row test table. */
function makeSingleRowTable(): Table {
  return makeTable({
    id: vectorFromArray([42]),
    value: vectorFromArray(["hello"]),
  });
}

/** Create an empty table. */
function makeEmptyTable(): Table {
  return makeTable({
    id: vectorFromArray([] as number[]),
    name: vectorFromArray([] as string[]),
  });
}

// ---------------------------------------------------------------------------
// VALID_FORMATS
// ---------------------------------------------------------------------------

describe("VALID_FORMATS", () => {
  it("contains_all_explicit_formats", () => {
    const explicit = ["json-row", "json-column", "csv", "markdown-table", "markdown-kv", "yaml", "xml"];
    for (const f of explicit) {
      expect(VALID_FORMATS.has(f)).toBe(true);
    }
  });

  it("contains_all_optimization_strategies", () => {
    const strategies = ["optimize-rendering", "optimize-accuracy", "optimize-cost", "optimize-speed"];
    for (const s of strategies) {
      expect(VALID_FORMATS.has(s)).toBe(true);
    }
  });

  it("has_11_entries", () => {
    expect(VALID_FORMATS.size).toBe(11);
  });
});

// ---------------------------------------------------------------------------
// _resolveFormat
// ---------------------------------------------------------------------------

describe("_resolveFormat", () => {
  it("optimize_rendering_resolves_to_markdown_table", () => {
    const [fmt] = _resolveFormat("optimize-rendering");
    expect(fmt).toBe("markdown-table");
  });

  it("optimize_accuracy_resolves_to_markdown_kv", () => {
    const [fmt] = _resolveFormat("optimize-accuracy");
    expect(fmt).toBe("markdown-kv");
  });

  it("optimize_cost_resolves_to_csv", () => {
    const [fmt] = _resolveFormat("optimize-cost");
    expect(fmt).toBe("csv");
  });

  it("optimize_speed_resolves_to_json_column", () => {
    const [fmt] = _resolveFormat("optimize-speed");
    expect(fmt).toBe("json-column");
  });

  it("explicit_format_resolves_to_itself", () => {
    const [fmt, reason] = _resolveFormat("csv");
    expect(fmt).toBe("csv");
    expect(reason).toContain("explicit format: csv");
  });
});

// ---------------------------------------------------------------------------
// formatTableData
// ---------------------------------------------------------------------------

describe("formatTableData", () => {
  it("raises_error_for_invalid_format", () => {
    const table = makeTestTable();
    expect(() => formatTableData(table, "not-a-format")).toThrow(/Invalid format/);
    expect(() => formatTableData(table, "not-a-format")).toThrow(/not-a-format/);
  });

  it("returns_actual_format_and_data", () => {
    const table = makeTestTable();
    const [fmt, data] = formatTableData(table, "csv");
    expect(fmt).toBe("csv");
    expect(typeof data).toBe("string");
  });

  it("resolves_optimize_rendering_to_markdown_table", () => {
    const table = makeTestTable();
    const [fmt] = formatTableData(table, "optimize-rendering");
    expect(fmt).toBe("markdown-table");
  });

  it("resolves_optimize_accuracy_to_markdown_kv", () => {
    const table = makeTestTable();
    const [fmt] = formatTableData(table, "optimize-accuracy");
    expect(fmt).toBe("markdown-kv");
  });

  it("resolves_optimize_cost_to_csv", () => {
    const table = makeTestTable();
    const [fmt] = formatTableData(table, "optimize-cost");
    expect(fmt).toBe("csv");
  });

  it("resolves_optimize_speed_to_json_column", () => {
    const table = makeTestTable();
    const [fmt] = formatTableData(table, "optimize-speed");
    expect(fmt).toBe("json-column");
  });
});

// ---------------------------------------------------------------------------
// formatJsonRow
// ---------------------------------------------------------------------------

describe("formatJsonRow", () => {
  it("returns_array_of_row_objects", () => {
    const table = makeTestTable();
    const result = formatJsonRow(table);
    expect(Array.isArray(result)).toBe(true);
    expect(result).toHaveLength(3);
    expect(result[0]["id"]).toBe(1);
    expect(result[0]["name"]).toBe("Alice");
    expect(result[1]["id"]).toBe(2);
    expect(result[2]["name"]).toBe("Charlie");
  });

  it("returns_empty_array_for_empty_table", () => {
    const table = makeEmptyTable();
    const result = formatJsonRow(table);
    expect(result).toEqual([]);
  });

  it("single_row_table", () => {
    const table = makeSingleRowTable();
    const result = formatJsonRow(table);
    expect(result).toHaveLength(1);
    expect(result[0]["id"]).toBe(42);
    expect(result[0]["value"]).toBe("hello");
  });
});

// ---------------------------------------------------------------------------
// formatJsonColumn
// ---------------------------------------------------------------------------

describe("formatJsonColumn", () => {
  it("returns_column_oriented_object", () => {
    const table = makeTestTable();
    const result = formatJsonColumn(table);
    expect(result["id"]).toEqual([1, 2, 3]);
    expect(result["name"]).toEqual(["Alice", "Bob", "Charlie"]);
  });

  it("returns_empty_arrays_for_empty_table", () => {
    const table = makeEmptyTable();
    const result = formatJsonColumn(table);
    expect(result["id"]).toEqual([]);
    expect(result["name"]).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// formatCsv
// ---------------------------------------------------------------------------

describe("formatCsv", () => {
  it("includes_header_row", () => {
    const table = makeTestTable();
    const result = formatCsv(table);
    const lines = result.split("\n");
    expect(lines[0]).toBe("id,name");
  });

  it("includes_data_rows", () => {
    const table = makeTestTable();
    const result = formatCsv(table);
    expect(result).toContain("1,Alice");
    expect(result).toContain("2,Bob");
    expect(result).toContain("3,Charlie");
  });

  it("ends_with_newline", () => {
    const table = makeTestTable();
    const result = formatCsv(table);
    expect(result.endsWith("\n")).toBe(true);
  });

  it("quotes_values_with_commas", () => {
    const table = makeTable({
      name: vectorFromArray(["Alice, Jr."]),
    });
    const result = formatCsv(table);
    expect(result).toContain('"Alice, Jr."');
  });

  it("empty_table_returns_header_only", () => {
    const table = makeEmptyTable();
    const result = formatCsv(table);
    expect(result.trim()).toBe("id,name");
  });
});

// ---------------------------------------------------------------------------
// formatMarkdownTable
// ---------------------------------------------------------------------------

describe("formatMarkdownTable", () => {
  it("includes_header_with_pipes", () => {
    const table = makeTestTable();
    const result = formatMarkdownTable(table);
    expect(result).toContain("| id | name |");
  });

  it("includes_separator_row", () => {
    const table = makeTestTable();
    const result = formatMarkdownTable(table);
    expect(result).toContain("| --- | --- |");
  });

  it("includes_data_rows", () => {
    const table = makeTestTable();
    const result = formatMarkdownTable(table);
    expect(result).toContain("| 1 | Alice |");
    expect(result).toContain("| 2 | Bob |");
  });

  it("escapes_pipe_characters", () => {
    const table = makeTable({
      text: vectorFromArray(["a | b"]),
    });
    const result = formatMarkdownTable(table);
    expect(result).toContain("a \\| b");
  });

  it("empty_table_has_header_and_separator_only", () => {
    const table = makeEmptyTable();
    const result = formatMarkdownTable(table);
    const lines = result.split("\n");
    expect(lines.length).toBe(2);
    expect(lines[0]).toContain("id");
    expect(lines[1]).toContain("---");
  });
});

// ---------------------------------------------------------------------------
// formatMarkdownKv
// ---------------------------------------------------------------------------

describe("formatMarkdownKv", () => {
  it("includes_record_headers", () => {
    const table = makeTestTable();
    const result = formatMarkdownKv(table);
    expect(result).toContain("## Record 1");
    expect(result).toContain("## Record 2");
    expect(result).toContain("## Record 3");
  });

  it("includes_key_value_pairs", () => {
    const table = makeTestTable();
    const result = formatMarkdownKv(table);
    expect(result).toContain("id: 1");
    expect(result).toContain("name: Alice");
  });

  it("escapes_colons_in_values", () => {
    const table = makeTable({
      text: vectorFromArray(["key: value"]),
    });
    const result = formatMarkdownKv(table);
    expect(result).toContain("key\\: value");
  });

  it("separates_records_with_blank_line", () => {
    const table = makeTestTable();
    const result = formatMarkdownKv(table);
    expect(result).toContain("\n\n## Record 2");
  });

  it("empty_table_returns_empty_string", () => {
    const table = makeEmptyTable();
    const result = formatMarkdownKv(table);
    expect(result).toBe("");
  });
});

// ---------------------------------------------------------------------------
// formatYaml
// ---------------------------------------------------------------------------

describe("formatYaml", () => {
  it("starts_with_records_key", () => {
    const table = makeTestTable();
    const result = formatYaml(table);
    expect(result.startsWith("records:")).toBe(true);
  });

  it("includes_row_data", () => {
    const table = makeTestTable();
    const result = formatYaml(table);
    expect(result).toContain("id: 1");
    expect(result).toContain("name: Alice");
  });

  it("ends_with_newline", () => {
    const table = makeTestTable();
    const result = formatYaml(table);
    expect(result.endsWith("\n")).toBe(true);
  });

  it("empty_table_has_no_rows", () => {
    const table = makeEmptyTable();
    const result = formatYaml(table);
    expect(result).toBe("records:\n");
  });
});

// ---------------------------------------------------------------------------
// formatXml
// ---------------------------------------------------------------------------

describe("formatXml", () => {
  it("starts_with_xml_declaration", () => {
    const table = makeTestTable();
    const result = formatXml(table);
    expect(result.startsWith('<?xml version="1.0" ?>')).toBe(true);
  });

  it("wraps_in_records_element", () => {
    const table = makeTestTable();
    const result = formatXml(table);
    expect(result).toContain("<records>");
    expect(result).toContain("</records>");
  });

  it("uses_id_as_attribute", () => {
    const table = makeTestTable();
    const result = formatXml(table);
    expect(result).toContain('id="1"');
  });

  it("includes_name_as_element", () => {
    const table = makeTestTable();
    const result = formatXml(table);
    expect(result).toContain("<name>Alice</name>");
  });

  it("escapes_special_xml_characters", () => {
    const table = makeTable({
      text: vectorFromArray(["<a & b>"]),
    });
    const result = formatXml(table);
    expect(result).toContain("&lt;a &amp; b&gt;");
  });

  it("empty_table_has_empty_records", () => {
    const table = makeEmptyTable();
    const result = formatXml(table);
    expect(result).toContain("<records>");
    expect(result).toContain("</records>");
    // Should not contain any <record> child elements (note: <records> wrapping is fine)
    expect(result).not.toContain("<record ");
    expect(result).not.toContain("<record/>");
    expect(result).not.toContain("<record>");
  });
});
