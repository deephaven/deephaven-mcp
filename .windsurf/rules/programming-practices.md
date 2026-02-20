---
trigger: always_on
---

# General programming practices that should be obeyed.
1. A Python file should not access private variables, functions, or methods in another file or package.  It is ok for the test file for a package to access and use the package being tested, even if it is private, and it is ok for the test file to access private variables, functions, and methods in the package.
2. There should be a one-to-one correspondance between source files and test files.  Unless there is a strongly compelling reason, all tests for a python source file should be in a single test file.
3. All functions decorated with `@mcp_server.tool()` are MCP tools.  Their associated pydocs will be consumed by AI agents.  As such, the pydocs need to be very clear and provide enough details so that the AI agent knows exactly how to use the tool and how exactly to interpret the results.
4. f-strings are preferred over % in format statements
5. All MCP tools in src/deephaven_mcp/mcp_systems_server/ or a subdirectory decorated with `@mcp_server.tool()` must include a "Terminology Note" section with this exact wording:
   - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
   - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
   - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
   - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
   - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
   - 'DHC' is shorthand for Deephaven Community (also called 'Core')
   - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')
6. All MCP tools in src/deephaven_mcp/mcp_systems_server/ or a subdirectory decorated with `@mcp_server.tool()` that return tabular data with a `format` parameter must include a "**Format Accuracy for AI Agents** (based on empirical research)" section immediately after the main tool description and before the "Terminology Note" section, with this exact wording:
    - markdown-kv: 61% accuracy (highest comprehension, more tokens)
    - markdown-table: 55% accuracy (good balance)
    - json-row/json-column: 50% accuracy
    - yaml: 50% accuracy
    - xml: 45% accuracy
    - csv: 44% accuracy (lowest comprehension, fewest tokens)
7. When moving or removing files, use the git version of the command when appropriate to maintain history.
8. A python file named <file>.py should have a single test file named test_<file>.py.  An exception is made for integration tests which are named test_<file>_integration.py.
9. "Any" is generally a bad type hint.  If you need to use it, please justify why it is necessary.  Specific type hints should be used when possible.
10. The use of "hasattr" and "getattr" is generally a bad practice.  If you need to use it, please justify why it is necessary.  They mask bugs.
11. Use American English spelling throughout all code, comments, docstrings, and documentation.  For example: "initialized" not "initialised", "recognized" not "recognised", "color" not "colour".