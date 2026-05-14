---
name: tsdocs-improve
description: Comprehensively improve TypeScript TSDoc comments — correct inaccuracies, add missing sections, improve clarity; also enforces required MCP tool sections (Terminology Note, Format Accuracy for AI Agents)
---

Review the TSDoc comments in the specified file for correctness, completeness, and clarity. Also review the module-level JSDoc comment at the top of the file if one exists. Only change TSDoc comments — do not change source code.

Only make a change if there is a significant improvement. Unnecessary changes make code review harder.

**Correctness**: Apply the `tsdocs-accuracy` criteria — description, `@param`, `@returns`, `@throws` must all match the actual code; no stale documented behavior.

**Format**: Use TSDoc style throughout:
- Parameters: `@param name - description`
- Returns: `@returns description`
- Throws: `@throws {ErrorType} when condition`

Type information is already in the function signature — do not repeat it in the TSDoc body.

**Completeness**: Every exported function and class should have a TSDoc comment. `@param`, `@returns`, and `@throws` tags should be present when applicable.

**Contract, not context**: A TSDoc comment documents what the function accepts, returns, and throws — not its surrounding context. Specifically, do not include:
- A list of callers (grep-recoverable; creates maintenance friction when callers change)
- Per-caller behavioral exposition
- Future-evolution hedging like "current rules", "at present, only...", or "additional rules can be added"
- Implementation rationale beyond what a caller needs to use the function correctly

If a TSDoc comment describes the world *outside* the function (callers, design history, future plans), it is wrong.

**MCP tools** (functions registered via `server.tool()` inside `registerTools`) are consumed by AI agents. Their TSDoc comments must be very detailed and specific — the AI agent has no other way to know how to use the tool or interpret its results. The `server.tool()` description string is the primary tool description surfaced to agents; it must be comprehensive.

All MCP tools in TypeScript MCP server code must include a **"Terminology Note"** section with this exact wording:
- 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
- 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
- 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
- In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
- In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
- 'DHC' is shorthand for Deephaven Community (also called 'Core')
- 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

All MCP tools that return tabular data with a `format` parameter must include a **"Format Accuracy for AI Agents"** section immediately after the main tool description and before the "Terminology Note" section, with this exact wording:
- markdown-kv: 61% accuracy (highest comprehension, more tokens)
- markdown-table: 55% accuracy (good balance)
- json-row/json-column: 50% accuracy
- yaml: 50% accuracy
- xml: 45% accuracy
- csv: 44% accuracy (lowest comprehension, fewest tokens)
