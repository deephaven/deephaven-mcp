---
name: _python-coding-practices
description: Python coding conventions and style guide for this project — invoke when writing or reviewing Python code
---

# Python Coding Practices

1. A Python file should not access private variables, functions, or methods in another file or package.  It is ok for the test file for a package to access and use the package being tested, even if it is private, and it is ok for the test file to access private variables, functions, and methods in the package.
2. All MCP tools (async functions registered via `server.tool()(fn)` inside `register_tools(server: FastMCP)` in a `_tools/` module) have specific docstring requirements — apply the `pydocs-improve` skill for the full rules, including required "Terminology Note" and "Format Accuracy for AI Agents" sections.
3. f-strings are preferred over `%` and `.format()` in format statements.
4. When moving or removing files, use the git version of the command when appropriate to maintain history.
5. A Python file named `<file>.py` should have a single test file named `test_<file>.py`.  An exception is made for integration tests which are named `test_<file>_integration.py`.
6. `Any` is generally a bad type hint.  If you need to use it, please justify why it is necessary.  Specific type hints should be used when possible.
7. `hasattr` and `getattr` are generally bad practice.  If you need to use them, please justify why.  They mask bugs.
8. Use American English spelling throughout all code, comments, docstrings, and documentation.  For example: "initialized" not "initialised", "recognized" not "recognised", "color" not "colour".
9. Unused function parameters should be indicated by prefixing the parameter name with a single underscore (e.g., `_request`, `_host`, `*_args`, `**_kwargs`).  This is the convention `ruff`/`pyright`/`pylint` recognize out of the box and derives from PEP 8's throwaway-variable convention.  Do not use `del param` at the top of a function body to silence unused-argument warnings.  The leading-underscore prefix is preferred over `del param` for all new code.
    - Exception: when callers of the function pass the argument by keyword (and changing the public name would be a breaking change), keep the original name.  In that case, either suppress the lint warning locally or use `*_args` / `**_kwargs` for generic stubs.
    - For handlers/callbacks driven by a framework or by a generic dispatcher (e.g., Starlette route handlers, protocol-dispatch callbacks in `_run_server`), always use the `_` prefix.
10. In tests, use `AsyncMock` for async functions/coroutines and `MagicMock` for synchronous ones. Using `MagicMock` where `AsyncMock` is needed is a common mistake — it causes tests to pass or fail misleadingly because the mock does not properly handle `await`.
