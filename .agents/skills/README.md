# Agent Skills

This directory contains the skills available to Cascade and other agents working in this repo. Skills are short, focused playbooks loaded as context (`_`-prefixed reference docs) or invoked directly (workflow skills, callable as slash commands). The system prompt's tool list is the source of truth at runtime; this index is for humans onboarding to the catalog.

Conventions:

- **`_`-prefixed skills** are AI-internal reference docs (standards, conventions, project layout). They are loaded as context by other skills, not invoked directly. The `_` prefix suppresses them from slash-command autocomplete.
- **Unprefixed skills** are user-invokable workflows (slash commands).
- New skills follow the same split — pick the prefix based on whether a human would ever type the slash command directly.

## Reference standards (`_`-prefixed)

| Skill | Purpose |
| --- | --- |
| `_configuration-conventions` | Canonical reference for the project's config model: JSON5 + Pydantic v2 + `${env:VAR}` / `${file:PATH}` templating; no ad-hoc env reads or `DEFAULT_FOO` constants. |
| `_logging-standards` | Logger instantiation, `[module:function] Action: details` message format, log levels, sensitive-data rules, redaction-aware Pydantic model logging. |
| `_markdown-documentation-standards` | Markdown formatting: JSON/JSON5 code block requirements, placeholder formatting, headings, links, tables, prose conventions. |
| `_mcp-module-organization` | Module placement and design patterns for MCP tool modules under `src/deephaven_mcp/mcp_systems_server/_tools/`. |
| `_project-reference` | High-level project map: architecture, server entry points and ports, config layout, code-quality check commands, test clients. |
| `_python-coding-practices` | Project-wide Python style and conventions (private-symbol access, MCP-tool docstring rules, f-strings, `Any`/`hasattr` policy, etc.). |

## Adding code & config

| Skill | Purpose |
| --- | --- |
| `mcp-tool-add` | Add a new MCP tool to the systems server end-to-end (placement, registration, docstring, logging, tests, docs). Wraps `_mcp-module-organization` + `pydocs-improve` + `_logging-standards`. |
| `config-add-tunable` | Add a new operator-tunable knob to the JSON config tree. Wraps the seven-step checklist from `_configuration-conventions`. |

## Reviewing code & docs

| Skill | Purpose |
| --- | --- |
| `review-changes` | Deep review of a changeset across all file types. |
| `review-python-file` | Comprehensive single-file Python review (design, correctness, security, types, pydocs, logging, tests). |
| `pydocs-accuracy` | Surgical Python docstring correctness fixes — no restructuring. |
| `pydocs-improve` | Full Python docstring overhaul: accuracy + restructure + missing-section enforcement. **Canonical source for the MCP tool "Terminology Note" and "Format Accuracy for AI Agents" wording.** |
| `docs-accuracy` | Surgical Markdown documentation accuracy fixes against the source code. |
| `docs-improve` | Full Markdown documentation overhaul: accuracy + organization + missing content + link fixes. |

When deciding between the `*-accuracy` and `*-improve` variants, see the "When to use this vs. the other" callout in each skill.

## Running tests & checks

| Skill | Purpose |
| --- | --- |
| `tests-run` | Full unit-test suite with coverage; reports failures and uncovered lines. |
| `tests-run-file` | Single test file's tests with coverage — required for per-file coverage assessment. |
| `tests-improve` | Comprehensively improve the test suite to 100% per-source-file coverage. |
| `integration-tests-run` | Run integration tests (Docker / pip / subprocess-based) — encodes the `-s` flag requirement and other silent-failure traps. |
| `check-deps-fresh` | Dependency freshness check (fresh resolve + mypy + pytest) plus failure diagnosis. |
| `run-precommit` | Run `precommit.sh` (isort, black, ruff, mypy, markdownlint) — modifies files in place; run before committing. |

## Operations & ad-hoc

| Skill | Purpose |
| --- | --- |
| `mcp-stress-test` | Stress test a deephaven-docs MCP server (dev or prod) by calling `docs_chat` N times sequentially. |
