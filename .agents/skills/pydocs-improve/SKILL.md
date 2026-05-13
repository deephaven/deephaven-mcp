---
name: pydocs-improve
description: Comprehensively improve Python docstrings — correct inaccuracies, add missing sections, improve clarity; also enforces required MCP tool sections (Terminology Note, Format Accuracy for AI Agents)
---

Review the docstrings in the specified file for correctness, completeness, and clarity. Also review the module-level docstring at the top of the file. Only change docstrings — do not change source code.

Only make a change if there is a significant improvement. Unnecessary changes make code review harder.

**Correctness**: Apply the `pydocs-accuracy` criteria — description, Args, Returns, Raises must all match the actual code; no stale documented behavior.

**Type information**: Function signatures must have type annotations, and docstrings must also document types in Google style:
- Args: `param (type): description`
- Returns: `type: description`
- Raises: `ExceptionType: description`

**Completeness**: Every non-trivial function and class should have a docstring. Args, Returns, and Raises sections should be present when applicable.

**Contract, not context**: A docstring documents what the function accepts, returns, and raises — not its surrounding context. Specifically, do not include:
- A list of callers (it's grep-recoverable and creates maintenance friction when callers change)
- Per-caller behavioral exposition (each caller's reason for using the function belongs in that caller's docstring or in a design comment, not here)
- Future-evolution hedging like "current rules", "at present, only...", or "additional rules can be added" (the docstring describes what the function does today; future changes are documented when they happen)
- Implementation rationale beyond what a caller needs to use the function correctly (rationale belongs in commit messages, design docs, or inline comments at the implementation site)

If a docstring describes the world *outside* the function (callers, design history, future plans), it's wrong.

**MCP tools** (functions registered via `server.tool()(fn)` inside `register_tools`) are consumed by AI agents. Their docstrings must be very detailed and specific — the AI agent has no other way to know how to use the tool or interpret its results.

All MCP tools in `src/deephaven_mcp/mcp_systems_server/` must include a **"Terminology Note"** section with this exact wording:
- 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
- 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
- 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
- In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
- In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
- 'DHC' is shorthand for Deephaven Community (also called 'Core')
- 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

All MCP tools in `src/deephaven_mcp/mcp_systems_server/` that return tabular data with a `format` parameter must include a **Format Accuracy for AI Agents** (based on empirical research): section immediately after the main tool description and before the "Terminology Note" section, with this exact wording:
- markdown-kv: 61% accuracy (highest comprehension, more tokens)
- markdown-table: 55% accuracy (good balance)
- json-row/json-column: 50% accuracy
- yaml: 50% accuracy
- xml: 45% accuracy
- csv: 44% accuracy (lowest comprehension, fewest tokens)
