---
name: review-changes
description: Perform a deep review of a set of code changes — design, simplification, DRY, security, pydocs, imports, logging
---

Perform a deep review of the code changes in the current diff. If a branch is specified, review the local code relative to that branch. If no branch is specified, review the uncommitted changes relative to the current branch.

1. **Design**: Is the design sound and consistent with the project? Apply the `python-coding-practices` and `mcp-module-organization` skills as relevant.
2. **Correctness**: Does the code do what it claims? Look for logic errors, incorrect assumptions, and edge cases.
3. **Simplification and DRY**: Can the code be simplified? Is logic duplicated that could be shared? Flag unnecessary abstraction or over-engineering.
4. **Code smells**: Long functions, deep nesting, magic numbers, overly complex conditions.
5. **Security**: Check for credential mishandling, session isolation issues, injection risks, and information disclosure. Ensure no default or fallback session IDs are used.
6. **Type safety**: Flag any `Any` type hints, `hasattr`, or `getattr` usage without justification (per `python-coding-practices`).
7. **Docstrings**: Apply the `pydocs-improve` skill to all changed functions and classes.
8. **Imports**: Remove unused imports. The `run-precommit` skill (ruff) will catch any that remain.
9. **Logging**: Apply the `logging-standards` skill to review logging coverage and consistency.
10. **Test coverage**: Are the changes adequately covered by tests?
11. Do not remove TODOs without a very good reason.
