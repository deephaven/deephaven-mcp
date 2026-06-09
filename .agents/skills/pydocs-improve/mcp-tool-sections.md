# Canonical MCP Tool Docstring Sections

Canonical, exact-match wording for the two required MCP-tool docstring sections. This file is the single source of truth; other skills reference it by name rather than duplicating the wording — duplicates drift. Load it when authoring or reviewing an MCP tool docstring.

## Terminology Note

All MCP tools in `src/deephaven_mcp/mcp_systems_server/` must include a "Terminology Note" section with this exact wording:

- 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
- 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
- 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
- In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
- In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
- 'DHC' is shorthand for Deephaven Community (also called 'Core')
- 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

## Format Accuracy for AI Agents

All MCP tools in `src/deephaven_mcp/mcp_systems_server/` that return tabular data with a `format` parameter must include a "Format Accuracy for AI Agents" (based on empirical research) section immediately after the main tool description and before the "Terminology Note" section, with this exact wording:

- markdown-kv: 61% accuracy (highest comprehension, more tokens)
- markdown-table: 55% accuracy (good balance)
- json-row/json-column: 50% accuracy
- yaml: 50% accuracy
- xml: 45% accuracy
- csv: 44% accuracy (lowest comprehension, fewest tokens)
