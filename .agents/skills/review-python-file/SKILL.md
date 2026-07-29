---
name: review-python-file
description: Perform a comprehensive review of a single Python file — invoke when reviewing one file in depth; use review-changes for a multi-file changeset. Covers design, correctness, DRY, security, type safety, pydocs, imports, logging, and test coverage
---

**Review like a senior engineer.** Every finding must answer three questions in concrete terms: *what is wrong*, *what is better*, *why the change is worth its cost*. All topics are fair game — correctness, security, design, duplication, clarity, naming, structure, tests.  Be conscious of suggestions that do not serve a purpose.

Perform a comprehensive review of the specified Python file as it currently exists.

1. **Design**: Is the code well-structured and consistent with the project? Apply the `ref-python-coding-practices` and `ref-mcp-module-organization` skills as relevant.
2. **Correctness**: Does the code do what it claims? Look for logic errors, incorrect assumptions, and edge cases.
3. **Simplification and DRY**: Can the code be simplified? Flag duplicated logic that should be shared, unnecessary abstraction, and over-engineering.
4. **Code smells**: Flag anything that makes a senior engineer pause and ask *"why is it like that?"* — long functions, deep nesting, magic numbers, complex conditions, oddly-shaped APIs (boolean-mode flags, stringly-typed parameters where a `Literal` or `Enum` belongs, long parameter lists), strange call syntax, dead or commented-out code, mixed abstraction levels within one function, mutable default arguments, broad `except` clauses, side effects in property getters or `__init__`, argument mutation, speculative generality, primitive obsession, feature envy, re-implementations of stdlib or library functionality, misleading names, inconsistent return types, sentinel returns where an exception belongs — *or anything else that just looks off.* This list is illustrative, not exhaustive; trust your judgment. Apply the general style rules in `ref-python-coding-practices`.
5. **Security**: Check for credential mishandling, session isolation issues, injection risks, and information disclosure — or anything else security-relevant. This list is illustrative, not exhaustive; trust your judgment. Flag any default or fallback ids — fully qualified ids arrive as explicit tool parameters and are validated by `QualifiedSessionId.from_str`, which raises rather than substituting a default.
6. **Type safety**: Flag any `Any` type hints, `hasattr`, or `getattr` usage without justification (per `ref-python-coding-practices`).
7. **Closed-set dispatch exhaustiveness**: For every dispatch on a `Literal`, `Enum`, or tuple of accepted values, verify a `match` + `typing.assert_never(value)` exhaustive form is used (or per-member metadata via `__new__` for enums). Flag any silent `if/elif/elif` fall-through default branch on a closed set. See `ref-python-coding-practices` rule #18.
8. **Suppression audit**: Search the file for `# pragma: no cover`, `# type: ignore`, `# noqa`, `# mypy: ignore-errors`. For each occurrence, determine whether a design move would eliminate the need (factor a helper, narrow with `cast`, rewrite the API). Flag any suppression without an inline justification comment. Bare `# type: ignore` (no bracketed error code) is always a bug. See `ref-python-coding-practices` rule #19.
9. **Docstrings**: Apply the `pydocs-improve` skill to all functions and classes in the file. For any Pydantic schemas (`StrictSchema` / `RedactableSchema` subclasses), verify every field carries a PEP 257 trailing docstring — the project enforces this with `tests/test_field_docs_contract.py`; flag any reliance on `Attributes:` blocks as a documentation bug.
   - **CLI help surface.** For files under `cli/_commands/` or `cli/_help.py`, the *surfaced* strings — command `help=` (via `build_help`), every `click.option(help=...)`, and group docstrings used as help — are governed by `ref-cli-help-standards`, not pydocs. Apply `cli-help-improve` to that surface: verify the section contract, single-sourced `OutputSpec`, and plain-text (no-RST) rule. Internal docstrings on the same file still go through `pydocs-improve`.
10. **Imports**: Flag any unused imports. (`run-precommit` removes them via ruff; the reviewer flags them for awareness, does not edit.)
11. **Logging**: Apply the `ref-logging-standards` skill to review logging coverage and consistency.
12. **Test coverage**: Verify the file is covered by its corresponding test file at the project's 100% per-source-file target (see `AGENTS.md` and `tests-improve`). Flag any uncovered branch.
    - **`__init__.py` files count.** Every `__init__.py` — including ones that only define `__all__` (even an empty `__all__`) or re-export from a sibling module — has its own dedicated `test_init.py`. The package surface is part of the project's API contract; an untested `__init__.py` is a silent-refactor hazard.
    - **What the `test_init.py` must pin**:
        - The exact set of names in `__all__`.
        - That every name in `__all__` resolves on the package (`hasattr(pkg, name)`).
        - That each re-export is the same object as the internal definition (`pkg.X is _module.X`).
        - That no `_`-prefixed names leak into the public surface.
    - **Canonical implementations**: `tests/config/schema/test_init.py`, `tests/config/test_init.py`, `tests/auth/middleware/test_init.py`.
13. **Output serialization**: For any user-facing payload built in this file (MCP tool return dict or CLI `OutputSpec` field), apply `ref-output-serialization-conventions` to every string field — value vocabulary, casing, and known carve-outs.
14. **Spelling**: apply `ref-python-coding-practices` rule 8 to the whole file (identifiers and string literals included, not just docstrings); run `uv run codespell <file>` and flag what it reports.

Do not remove TODOs without a very good reason.
