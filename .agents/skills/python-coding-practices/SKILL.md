---
name: python-coding-practices
description: Python coding conventions and style guide for this project — invoke when writing or reviewing Python code
---

# Python Coding Practices

1. A Python file should not access private variables, functions, or methods in another file or package.  It is ok for the test file for a package to access and use the package being tested, even if it is private, and it is ok for the test file to access private variables, functions, and methods in the package.
2. All async functions registered via `server.tool()(fn)` inside a `register_tools(server: FastMCP)` function in a `_tools/` module are MCP tools.  Their associated pydocs will be consumed by AI agents.  As such, the pydocs need to be very clear and provide enough details so that the AI agent knows exactly how to use the tool and how exactly to interpret the results.
3. f-strings are preferred over `%` and `.format()` in format statements.
4. All MCP tools in src/deephaven_mcp/mcp_systems_server/ or a subdirectory registered as MCP tools (via `server.tool()(fn)` in `register_tools`) must include a "Terminology Note" section with this exact wording:
   - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
   - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
   - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
   - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
   - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
   - 'DHC' is shorthand for Deephaven Community (also called 'Core')
   - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')
5. All MCP tools in src/deephaven_mcp/mcp_systems_server/ or a subdirectory registered as MCP tools (via `server.tool()(fn)` in `register_tools`) that return tabular data with a `format` parameter must include a "**Format Accuracy for AI Agents** (based on empirical research)" section immediately after the main tool description and before the "Terminology Note" section, with this exact wording:
    - markdown-kv: 61% accuracy (highest comprehension, more tokens)
    - markdown-table: 55% accuracy (good balance)
    - json-row/json-column: 50% accuracy
    - yaml: 50% accuracy
    - xml: 45% accuracy
    - csv: 44% accuracy (lowest comprehension, fewest tokens)
6. When moving or removing files, use the git version of the command when appropriate to maintain history.
7. A python file named <file>.py should have a single test file named test_<file>.py.  An exception is made for integration tests which are named test_<file>_integration.py.
8. `Any` is generally a bad type hint.  If you need to use it, please justify why it is necessary.  Specific type hints should be used when possible.
9. `hasattr` and `getattr` are generally bad practice.  If you need to use them, please justify why.  They mask bugs.
10. Use American English spelling throughout all code, comments, docstrings, and documentation.  For example: "initialized" not "initialised", "recognized" not "recognised", "color" not "colour".
11. Unused function parameters should be indicated by prefixing the parameter name with a single underscore (e.g., `_request`, `_host`, `*_args`, `**_kwargs`).  This is the convention `ruff`/`pyright`/`pylint` recognize out of the box and derives from PEP 8's throwaway-variable convention.  Do not use `del param` at the top of a function body to silence unused-argument warnings.  The leading-underscore prefix is preferred over `del param` for all new code.
    - Exception: when callers of the function pass the argument by keyword (and changing the public name would be a breaking change), keep the original name.  In that case, either suppress the lint warning locally or use `*_args` / `**_kwargs` for generic stubs.
    - For handlers/callbacks driven by a framework or by a generic dispatcher (e.g., Starlette route handlers, protocol-dispatch callbacks in `_run_server`), always use the `_` prefix.
12. In tests, use `AsyncMock` for async functions/coroutines and `MagicMock` for synchronous ones. Using `MagicMock` where `AsyncMock` is needed is a common mistake — it causes tests to pass or fail misleadingly because the mock does not properly handle `await`.
