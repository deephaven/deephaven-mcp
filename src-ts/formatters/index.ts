/**
 * Table formatting for MCP responses.
 *
 * This module provides table data formatting optimized for AI agent consumption, with format
 * selection based on empirical research showing significant accuracy differences between formats.
 *
 * Features:
 *   - Multiple output formats: JSON (row/column), CSV, Markdown (table/kv), YAML, XML
 *   - Optimization strategies for rendering, accuracy, cost, and speed
 *   - Research-backed format accuracy rankings (markdown-kv: 60.7%, csv: 44%)
 *   - Explicit format selection for advanced use cases
 *   - Comprehensive format validation with helpful error messages
 *
 * Format Accuracy Rankings (from research):
 *   Based on: https://www.improvingagents.com/blog/best-input-data-format-for-llms
 *   - markdown-kv: 60.7% accuracy (highest, ~2.7x token cost vs CSV)
 *   - markdown-table: ~55% accuracy (good balance)
 *   - json-row, json-column: ~50% accuracy
 *   - yaml: ~50% accuracy
 *   - xml: ~45% accuracy
 *   - csv: ~44% accuracy (lowest, most token-efficient)
 *
 * Optimization Strategies:
 *   - optimize-rendering: Always use markdown-table (best for AI agent table display)
 *   - optimize-accuracy: Always use markdown-kv (highest comprehension, more tokens)
 *   - optimize-cost: Always use csv (fewest tokens, most cost-effective)
 *   - optimize-speed: Always use json-column (fastest conversion)
 *
 * @example
 * ```typescript
 * import { formatTableData } from "./formatters/index.js";
 * import { makeTable, vectorFromArray } from "apache-arrow";
 *
 * const table = makeTable({ id: vectorFromArray([1, 2]), name: vectorFromArray(["Alice", "Bob"]) });
 * const [formatUsed, data] = formatTableData(table, "optimize-rendering");
 * // Returns: ["markdown-table", "| id | name |\\n| --- | --- |\\n..."]
 * ```
 */

import pino from "pino";
import { Table } from "apache-arrow";

const _logger = pino({ name: "deephaven-mcp:formatters" });

/**
 * Valid format names for table data formatting.
 *
 * Contains all supported format types: 7 explicit formats and 4 optimization strategies.
 * Total: 11 valid format names.
 */
export const VALID_FORMATS: ReadonlySet<string> = new Set([
  "json-row",
  "json-column",
  "csv",
  "markdown-table",
  "markdown-kv",
  "yaml",
  "xml",
  "optimize-rendering",
  "optimize-accuracy",
  "optimize-cost",
  "optimize-speed",
]);

// ---------------------------------------------------------------------------
// Individual format functions
// ---------------------------------------------------------------------------

/**
 * Format Arrow table as array of row objects.
 *
 * @param table - Apache Arrow Table to format.
 * @returns Array of objects, each representing a row.
 *   Example: [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- table rows have arbitrary column values
export function formatJsonRow(table: Table): Array<Record<string, any>> {
  const columns = table.schema.fields.map((f) => f.name);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- row values are arbitrary
  const result: Array<Record<string, any>> = [];
  for (let i = 0; i < table.numRows; i++) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- row has arbitrary column values
    const row: Record<string, any> = {};
    for (const col of columns) {
      const vector = table.getChild(col as never);
      row[col] = vector ? vector.get(i) : null;
    }
    result.push(row);
  }
  return result;
}

/**
 * Format Arrow table as column-oriented object.
 *
 * @param table - Apache Arrow Table to format.
 * @returns Object with column names as keys, arrays as values.
 *   Example: {"id": [1, 2], "name": ["Alice", "Bob"]}
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- column values are arbitrary
export function formatJsonColumn(table: Table): Record<string, any[]> {
  const columns = table.schema.fields.map((f) => f.name);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- column values are arbitrary
  const result: Record<string, any[]> = {};
  for (const col of columns) {
    const vector = table.getChild(col as never);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- values are arbitrary
    result[col] = vector ? Array.from({ length: table.numRows }, (_, i) => vector.get(i) as any) : [];
  }
  return result;
}

/**
 * Format Arrow table as CSV string.
 *
 * @param table - Apache Arrow Table to format.
 * @returns CSV-formatted string with header row.
 *   Example: "id,name\n1,Alice\n2,Bob\n"
 */
export function formatCsv(table: Table): string {
  const columns = table.schema.fields.map((f) => f.name);

  // Escape a CSV cell value: if it contains comma, quote, or newline, wrap in quotes
  function escapeCell(value: unknown): string {
    const str = String(value ?? "");
    if (str.includes(",") || str.includes('"') || str.includes("\n") || str.includes("\r")) {
      return '"' + str.replace(/"/g, '""') + '"';
    }
    return str;
  }

  const lines: string[] = [columns.map(escapeCell).join(",")];
  for (let i = 0; i < table.numRows; i++) {
    const cells = columns.map((col) => {
      const vector = table.getChild(col as never);
      return escapeCell(vector ? vector.get(i) : null);
    });
    lines.push(cells.join(","));
  }
  return lines.join("\n") + "\n";
}

/**
 * Format Arrow table as markdown table.
 *
 * @param table - Apache Arrow Table to format.
 * @returns Markdown table string with header and separator rows.
 */
export function formatMarkdownTable(table: Table): string {
  const columns = table.schema.fields.map((f) => f.name);

  const header = "| " + columns.join(" | ") + " |";
  const separator = "| " + columns.map(() => "---").join(" | ") + " |";

  const rows: string[] = [];
  for (let i = 0; i < table.numRows; i++) {
    const cells = columns.map((col) => {
      const vector = table.getChild(col as never);
      const value = vector ? vector.get(i) : null;
      return String(value ?? "").replace(/\|/g, "\\|");
    });
    rows.push("| " + cells.join(" | ") + " |");
  }

  return [header, separator, ...rows].join("\n");
}

/**
 * Format Arrow table as markdown key-value pairs.
 *
 * Highest accuracy format for LLM consumption (60.7% per research).
 *
 * @param table - Apache Arrow Table to format.
 * @returns Markdown with record headers and key-value pairs.
 */
export function formatMarkdownKv(table: Table): string {
  const columns = table.schema.fields.map((f) => f.name);
  const records: string[] = [];

  for (let i = 0; i < table.numRows; i++) {
    const lines = [`## Record ${i + 1}`];
    for (const col of columns) {
      const vector = table.getChild(col as never);
      const value = vector ? vector.get(i) : null;
      const valueStr = String(value ?? "").replace(/:/g, "\\:");
      lines.push(`${col}: ${valueStr}`);
    }
    records.push(lines.join("\n"));
  }

  return records.join("\n\n");
}

/**
 * Format Arrow table as YAML.
 *
 * @param table - Apache Arrow Table to format.
 * @returns YAML-formatted string.
 */
export function formatYaml(table: Table): string {
  const columns = table.schema.fields.map((f) => f.name);
  const lines: string[] = ["records:"];

  for (let i = 0; i < table.numRows; i++) {
    let firstCol = true;
    for (const col of columns) {
      const vector = table.getChild(col as never);
      const value = vector ? vector.get(i) : null;
      const valueStr = _yamlScalar(value);
      if (firstCol) {
        lines.push(`  - ${col}: ${valueStr}`);
        firstCol = false;
      } else {
        lines.push(`    ${col}: ${valueStr}`);
      }
    }
  }

  return lines.join("\n") + "\n";
}

/**
 * Render a value as a YAML scalar.
 *
 * @param value - The value to render.
 * @returns YAML scalar string.
 */
function _yamlScalar(value: unknown): string {
  if (value === null || value === undefined) return "null";
  if (typeof value === "boolean") return String(value);
  if (typeof value === "number") return String(value);
  const str = String(value);
  // Quote strings that could be misread as YAML special values or contain special chars
  if (
    str === "" ||
    str.includes(":") ||
    str.includes("#") ||
    str.includes("{") ||
    str.includes("}") ||
    str.includes("[") ||
    str.includes("]") ||
    str.includes(",") ||
    str.includes("&") ||
    str.includes("*") ||
    str.includes("?") ||
    str.includes("|") ||
    str.includes(">") ||
    str.includes("!") ||
    str.includes("'") ||
    str.includes('"') ||
    str.includes("%") ||
    str.includes("@") ||
    str.includes("`") ||
    str.includes("\n") ||
    str.includes("\r") ||
    str === "true" ||
    str === "false" ||
    str === "null" ||
    str === "~"
  ) {
    return '"' + str.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\n/g, "\\n") + '"';
  }
  return str;
}

/**
 * Format Arrow table as XML.
 *
 * @param table - Apache Arrow Table to format.
 * @returns XML-formatted string.
 */
export function formatXml(table: Table): string {
  const columns = table.schema.fields.map((f) => f.name);

  function escapeXml(str: string): string {
    return str
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&apos;");
  }

  const lines: string[] = ['<?xml version="1.0" ?>'];
  lines.push("<records>");

  for (let i = 0; i < table.numRows; i++) {
    // Build record element with possible id attribute
    const attrs: string[] = [];
    const children: string[] = [];

    for (const col of columns) {
      const vector = table.getChild(col as never);
      const value = vector ? vector.get(i) : null;
      const valueStr = escapeXml(String(value ?? ""));

      if (col.toLowerCase() === "id") {
        attrs.push(`id="${valueStr}"`);
      } else {
        children.push(`    <${col}>${valueStr}</${col}>`);
      }
    }

    const attrStr = attrs.length > 0 ? " " + attrs.join(" ") : "";
    if (children.length > 0) {
      lines.push(`  <record${attrStr}>`);
      lines.push(...children);
      lines.push("  </record>");
    } else {
      lines.push(`  <record${attrStr}/>`);
    }
  }

  lines.push("</records>");
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Format registry
// ---------------------------------------------------------------------------

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- formatter return types vary
type FormatterFn = (table: Table) => any;

const _FORMATTERS: Record<string, FormatterFn> = {
  "json-row": formatJsonRow,
  "json-column": formatJsonColumn,
  csv: formatCsv,
  "markdown-table": formatMarkdownTable,
  "markdown-kv": formatMarkdownKv,
  yaml: formatYaml,
  xml: formatXml,
};

// ---------------------------------------------------------------------------
// Format resolution
// ---------------------------------------------------------------------------

/**
 * Resolve optimization strategy to concrete format name.
 *
 * @param formatType - Format name or optimization strategy.
 * @returns Tuple of [actualFormat, reason].
 */
export function _resolveFormat(formatType: string): [string, string] {
  switch (formatType) {
    case "optimize-rendering":
      return ["markdown-table", "optimize-rendering strategy"];
    case "optimize-accuracy":
      return ["markdown-kv", "optimize-accuracy strategy"];
    case "optimize-cost":
      return ["csv", "optimize-cost strategy"];
    case "optimize-speed":
      return ["json-column", "optimize-speed strategy"];
    default:
      return [formatType, `explicit format: ${formatType}`];
  }
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

/**
 * Convert Arrow table to specified format.
 *
 * @param arrowTable - Apache Arrow Table to format.
 * @param formatType - Format name or optimization strategy.
 * @returns A 2-tuple of [actualFormatUsed, formattedData].
 *   For optimization strategies, actualFormatUsed will be the resolved format.
 * @throws {Error} If formatType is not in VALID_FORMATS.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- return data type varies by format
export function formatTableData(arrowTable: Table, formatType: string): [string, any] {
  if (!VALID_FORMATS.has(formatType)) {
    const validList = [...VALID_FORMATS].sort().join(", ");
    _logger.error(
      `[formatters:formatTableData] Invalid format '${formatType}'. Valid options: ${validList}`,
    );
    throw new Error(`Invalid format '${formatType}'. Valid options: ${validList}`);
  }

  const rowCount = arrowTable.numRows;
  const colCount = arrowTable.numCols;

  _logger.debug(
    `[formatters:formatTableData] Formatting table: ${rowCount} rows, ${colCount} columns, requested format='${formatType}'`,
  );

  const [actualFormat, reason] = _resolveFormat(formatType);
  _logger.debug(`[formatters:formatTableData] Using '${actualFormat}' (${reason})`);

  const formatter = _FORMATTERS[actualFormat];
  const data = formatter(arrowTable);

  _logger.debug(
    `[formatters:formatTableData] Successfully formatted ${rowCount} rows as '${actualFormat}'`,
  );

  return [actualFormat, data];
}
