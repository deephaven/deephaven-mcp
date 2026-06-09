---
name: pydocs-improve
description: Comprehensively improve Python docstrings — correct inaccuracies, add missing sections, improve clarity; also enforces required MCP tool sections (Terminology Note, Format Accuracy for AI Agents)
---

> **Single source of truth.** The canonical, exact-match wording for the "Terminology Note" and "Format Accuracy for AI Agents" sections lives in [mcp-tool-sections.md](mcp-tool-sections.md). Other skills reference that file rather than duplicating the wording — duplicates drift. Load it when a tool docstring needs those sections.

Review the docstrings in the specified file for correctness, completeness, and clarity. Also review the module-level docstring at the top of the file. Only change docstrings — do not change source code.

Only make a change if there is a significant improvement. Unnecessary changes make code review harder.

**Correctness**: Apply the `pydocs-accuracy` criteria — description, Args, Returns, Raises must all match the actual code; no stale documented behavior.

**Type information**: Function signatures must have type annotations, and docstrings must also document types in Google style:

- Args: `param (type): description`
- Returns: `type: description`
- Raises: `ExceptionType: description`

**Completeness**: Every non-trivial function and class should have a docstring. Args, Returns, and Raises sections should be present when applicable.

**Pydantic fields**: Every field on a `StrictSchema` /
`RedactableSchema` subclass needs runtime-introspectable
documentation. Apply the `_configuration-conventions` skill for the
canonical rule and examples; in summary:

- Each field carries a PEP 257 trailing docstring (triple-quoted
  string immediately below the assignment line). The project enables
  `use_attribute_docstrings=True` on the base so Pydantic harvests
  it into `model_fields[name].description`.
- Sphinx `Attributes:` blocks on the class docstring are invisible
  to `model_json_schema()` and the MCP tool surface — remove any
  redundant `Attributes:` listing when adding trailing docstrings.
- Explicit `Field(description="...")` works at runtime but violates
  project style — convert to the trailing-docstring form
  opportunistically.
- Verify with `python -c "from <module> import <Model>; print(<Model>.model_fields['<field>'].description)"`;
  `None` or an empty string means the field is undocumented at
  runtime regardless of what the class docstring says.
- Enforced by `tests/test__pydantic_field_docs.py`.

**Contract, not context**: A docstring documents what the function accepts, returns, and raises — not its surrounding context. Specifically, do not include:

- A list of callers (it's grep-recoverable and creates maintenance friction when callers change)
- Per-caller behavioral exposition (each caller's reason for using the function belongs in that caller's docstring or in a design comment, not here)
- Future-evolution hedging like "current rules", "at present, only...", or "additional rules can be added" (the docstring describes what the function does today; future changes are documented when they happen)
- Implementation rationale beyond what a caller needs to use the function correctly (rationale belongs in commit messages, design docs, or inline comments at the implementation site)

If a docstring describes the world *outside* the function (callers, design history, future plans), it's wrong.

**Module and package docstrings**: A package `__init__.py` docstring is a one-line statement of what the package is, plus a list of its **surface** submodules — nothing more. The "Contract, not context" rule applies with the same force here; module/package docstrings are the most common place it is violated. Specifically:

- **List only the surface submodules** — the ones imported from outside the package. Omit underscored/internal submodules (`_os_support.py`): they are the package-private analogue of a private helper and do not belong in the documented surface. Canonical implementation: `_platform/__init__.py` (lists `fsutil`/`spawn`/`dir_permissions` and omits the internal `_os_support.py`); `auth/__init__.py` (lean `:mod:` bullet list, no rationale).
- **No design rationale.** Delete import-cycle explanations, "single home for X / one place to add Y" framing, and "intentionally import-free" justifications. If a constraint is load-bearing for future editors, put it in a `#` comment in the file body, not the docstring.
- **No cross-package tours.** Do not document what *other* packages do (a "the other axis lives in `config._data_root`" aside describes the world outside this package).
- Before/after (this repo's `_platform/__init__.py` cleanup):

    ```text
    # WRONG — rationale + private submodule + cross-package tour
    """OS abstraction layer: the project's single home for os.name dispatch.

    Every module that branches on the OS lives here, so "where do I add
    a new OS?" has one answer: this package.

    - _os_support — supported-OS set and error factory. A leaf module so
      submodules import it without an import cycle through __init__.
    - fsutil — ...
    A separate axis keys on sys.platform and lives in config._data_root ...
    """

    # RIGHT — one-line what + surface submodules only
    """OS abstraction layer: the home for code that branches on os.name.

    Submodules:

    - fsutil — advisory file locking, atomic private writes, ...
    - spawn — detached background-process spawn.
    - dir_permissions — per-OS private-dir hardening and auditing.
    """
    ```

**When to use this vs. `pydocs-accuracy`**: use `pydocs-improve` for a full review (accuracy + restructuring + missing-section enforcement). Use `pydocs-accuracy` for surgical correctness-only fixes during a code review where restructuring would expand scope.

**MCP tools** (functions registered via `server.tool()(fn)` inside `register_tools`) are consumed by AI agents. Their docstrings must be very detailed and specific — the AI agent has no other way to know how to use the tool or interpret its results. Every such tool requires a "Terminology Note" section, and every tool returning tabular data with a `format` parameter requires a "Format Accuracy for AI Agents" section. The canonical exact-match wording and placement rules for both are in [mcp-tool-sections.md](mcp-tool-sections.md) — use it verbatim.
