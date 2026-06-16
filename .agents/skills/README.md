# Agent Skills

This directory contains the skills available to Cascade and other agents working in this repo. Skills are short, focused playbooks loaded as context (`_`-prefixed reference docs) or invoked directly (workflow skills, callable as slash commands). The system prompt's tool list is the source of truth at runtime; this index is for humans onboarding to the catalog.

**Cross-agent wiring:** these skills live here for cross-agent compatibility. Claude Code discovers skills only under `.claude/skills/`, so the repo ships `.claude/skills` as a symlink to `../.agents/skills` (one tree, no duplication), and a root `CLAUDE.md` that imports `@AGENTS.md`. The `_` prefix suppresses autocomplete only in Cascade; in Claude Code it has no effect, so `_`-prefixed skills also carry `user-invocable: false` in frontmatter.

Conventions:

- **`_`-prefixed skills** are AI-internal reference docs (standards, conventions, project layout). They are loaded as context by other skills, not invoked directly by humans. The `_` prefix suppresses them from slash-command autocomplete.
- **Unprefixed skills** are user-invokable workflows (slash commands). An unprefixed skill may *also* be referenced by other skills (`review-changes` calls `review-python-file`) — the prefix tracks *human invocability only*, not whether other skills reference it.
- New skills follow the same split — apply the `_skill-authoring-standards` §4 decision flowchart: will any human ever type `/<name>` to invoke this skill? Yes → unprefixed. No → prefix with `_`.

**Composition and effectiveness:** skills compose by reference (the directory layout is flat; the dependency graph is the architecture). Every skill is specialized, self-contained, and referenced by at least one parent. Content quality — whether a skill actually gets invoked and produces the right outcome — is governed by `_skill-effectiveness`; structural form is governed by `_skill-authoring-standards`. Apply both when adding (`skill-add`) or reviewing (`skill-review`) a skill.

## Reference standards (`_`-prefixed)

| Skill | Purpose |
| --- | --- |
| `_agents-md-curation` | Standards for editing the project root `AGENTS.md`: what belongs there vs in a skill, format conventions, composition with skills, sync rules. |
| `_cli-help-standards` | The content contract for `dh-mcp` CLI help: required help sections, `build_help` usage, plain-text/no-RST rule, the no-rewrap marker, single-source `OutputSpec` output schema, and the three-surface consistency rule (`docs/CLI.md`, `--help`, introspect). |
| `_cli-tool-wrapping` | Conventions for `dh-mcp` runtime commands that wrap MCP tools: the four wrapper categories, the shared `_wrapping` helpers, type scoping via `--system`/id/group-doc (never subgroups), and the `wraps_tool` schema-drift contract. |
| `_configuration-conventions` | Canonical reference for the project's config model: JSON5 + Pydantic v2 + `${env:VAR}` / `${file:PATH}` templating; no ad-hoc env reads or `DEFAULT_FOO` constants. |
| `_documentation-roles` | Defines the role (audience and scope) of every top-level markdown document; loaded by the docs workflows (`docs-improve`, `docs-accuracy`) and `cli-command-add` to keep edits in-scope and prevent content drift. |
| `_logging-standards` | Logger instantiation, `[module:function] Action: details` message format, log levels, sensitive-data rules, redaction-aware Pydantic model logging. |
| `_markdown-documentation-standards` | Markdown formatting: JSON/JSON5 code block requirements, placeholder formatting, headings, links, tables, prose conventions. |
| `_mcp-module-organization` | Module placement and design patterns for MCP tool modules under `src/deephaven_mcp/mcp_systems_server/_tools/`. |
| `_project-reference` | High-level project map: architecture, server entry points and ports, config layout, code-quality check commands, test clients. |
| `_python-coding-practices` | Project-wide Python style and conventions (private-symbol access, MCP-tool docstring rules, f-strings, `Any`/`hasattr` policy, etc.). |
| `_skill-authoring-standards` | Structural standards for skill files: composition hierarchy, frontmatter contract, `_`-prefix rule for human-invocability, body shape, length budgets, lifecycle, precedence. |
| `_skill-effectiveness` | Content-quality standards for any agent-facing prose: triggerability, actionability, outcome orientation, pattern density, clarity, hedge-word audit, anti-patterns, field failure signals. |

## Adding code & config

| Skill | Purpose |
| --- | --- |
| `mcp-tool-add` | Add a new MCP tool to the systems server end-to-end (placement, registration, docstring, logging, tests, docs). Wraps `_mcp-module-organization` + `pydocs-improve` + `_logging-standards`. |
| `cli-command-add` | Add a new command to the `dh-mcp` CLI end-to-end (click + `@run_async` + `CliError`, tests, `docs/CLI.md`, introspect). Wraps `_python-coding-practices` rule 15 + `_documentation-roles` + `_cli-help-standards`. |
| `config-field-add` | Add a new field, setting, or tunable to the JSON config tree (`server.json`, `cli.json`, `community/`, `enterprise/`). Wraps `_configuration-conventions`. |
| `skill-add` | Add a new agent skill end-to-end (composition gate, name + `_`-prefix decision, description, body, parent cross-links, catalog README row). Wraps `_skill-authoring-standards` + `_skill-effectiveness` + `_agents-md-curation`. |

## Reviewing code & docs

| Skill | Purpose |
| --- | --- |
| `review-changes` | Deep review of a changeset across all file types. |
| `review-python-file` | Comprehensive single-file Python review (design, correctness, security, types, pydocs, logging, tests). |
| `pydocs-accuracy` | Surgical Python docstring correctness fixes — no restructuring. |
| `pydocs-improve` | Full Python docstring overhaul: accuracy + restructure + missing-section enforcement. The canonical MCP tool "Terminology Note" and "Format Accuracy for AI Agents" wording lives in [`pydocs-improve/mcp-tool-sections.md`](pydocs-improve/mcp-tool-sections.md). |
| `docs-accuracy` | Surgical Markdown documentation accuracy fixes against the source code. |
| `docs-improve` | Full Markdown documentation overhaul: accuracy + organization + missing content + link fixes. |
| `cli-help-accuracy` | Surgical correctness fixes to CLI help — documented flags, arguments, error codes, exit codes, and output fields verified against the handler across `--help`, the introspect manifest, and `docs/CLI.md`. Wraps `_cli-help-standards`. |
| `cli-help-improve` | Full overhaul of a command's help surface: fill missing sections, single-source the `OutputSpec`, reconcile all three description surfaces. Wraps `_cli-help-standards`. |
| `skill-review` | Review an existing agent skill or `AGENTS.md` against the standards (composition audit, effectiveness audit, structural audit, cross-reference audit, README sync). Wraps `_skill-authoring-standards` + `_skill-effectiveness` + `_agents-md-curation`. |

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
