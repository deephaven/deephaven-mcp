---
name: pydocs-improve
description: Improve docstrings in a file for correctness, completeness, and clarity without changing source code
---

Review the docstrings in the specified file for correctness, completeness, and clarity. Also review the module-level docstring at the top of the file. Only change docstrings — do not change source code.

Only make a change if there is a significant improvement. Unnecessary changes make code review harder.

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
