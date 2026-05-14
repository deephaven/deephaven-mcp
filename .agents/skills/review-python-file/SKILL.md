---
name: review-python-file
description: Perform a comprehensive review of a single Python file — design, correctness, DRY, security, type safety, pydocs, imports, logging, and test coverage
---

Perform a comprehensive review of the specified Python file as it currently exists.

1. **Design**: Is the code well-structured and consistent with the project? Apply the `_python-coding-practices` and `_python-mcp-module-organization` skills as relevant.
2. **Correctness**: Does the code do what it claims? Look for logic errors, incorrect assumptions, and edge cases.
3. **Simplification and DRY**: Can the code be simplified? Is logic duplicated that could be shared? Flag unnecessary abstraction or over-engineering.
4. **Code smells**: Long functions, deep nesting, magic numbers, overly complex conditions.
5. **Security**: Check for credential mishandling, session isolation issues, injection risks, and information disclosure. Ensure no default or fallback session IDs are used.
6. **Type safety**: Flag any `Any` type hints, `hasattr`, or `getattr` usage without justification (per `_python-coding-practices`).
7. **Docstrings**: Apply the `pydocs-improve` skill to all functions and classes in the file.
8. **Imports**: Remove unused imports. The `run-precommit` skill (ruff) will catch any that remain.
9. **Logging**: Apply the `_python-logging-standards` skill to review logging coverage and consistency.
10. **Test coverage**: Is the file adequately covered by its corresponding test file?
11. Do not remove TODOs without a very good reason.
