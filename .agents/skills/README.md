# Agent Skills

This directory contains the skills available to Cascade and other agents working in this repo. Skills are short, focused playbooks either loaded as context (`ref-`-prefixed reference docs) or invoked directly by a human (workflow skills). The system prompt's tool list is the source of truth at runtime; this index is for humans onboarding to the catalog.

**Cross-agent wiring:** these skills live here for cross-agent compatibility. Claude Code discovers skills only under `.claude/skills/`, so the repo ships `.claude/skills` as a symlink to `../.agents/skills` (one tree, no duplication), and a root `CLAUDE.md` that imports `@AGENTS.md`. Invocation syntax differs per agent; see `ref-skill-authoring-standards` *Cross-agent portability*.

Conventions:

- **`ref-`-prefixed skills** are AI-internal reference docs (standards, conventions, project layout), loaded as context by other skills rather than invoked by a human. The prefix is a naming convention that groups them and signals intent to a reader — it is not an enforcement mechanism. `user-invocable: false` in frontmatter is the only mechanism, and only Claude Code honors it.
- **Unprefixed skills** are human-invocable workflows. An unprefixed skill may *also* be referenced by other skills (`review-changes` calls `review-python-file`) — the prefix tracks *human invocability only*, not whether other skills reference it.
- New skills follow the same split — apply the `ref-skill-authoring-standards` *Naming and the prefix rule* decision flowchart: will any human ever invoke this skill directly? Yes → unprefixed. No → prefix with `ref-`.

**Composition and effectiveness:** skills compose by reference (the directory layout is flat; the dependency graph is the architecture). Every skill is specialized, self-contained, and referenced by at least one parent. Content quality — whether a skill actually gets invoked and produces the right outcome — is governed by `ref-skill-effectiveness`; structural form is governed by `ref-skill-authoring-standards`. Apply both when adding (`skill-add`) or reviewing (`skill-review`) a skill.

## Reference standards (`ref-`-prefixed)

| Skill | Purpose |
| --- | --- |
| `ref-agents-md-curation` | Standards for editing the project root `AGENTS.md`: what belongs there vs in a skill, format conventions, composition with skills, sync rules. |
| `ref-cli-design-prior-art` | Ground `dhcli` design decisions in the conventions of comparable CLIs (`kubectl`, `docker`, `gh`, `aws`, `gcloud`, `git`) — cite the tool and the specific behavior, and state the reason when diverging. |
| `ref-cli-help-standards` | The content contract for `dhcli` CLI help: required sections, `HelpSpec` authoring, plain-text rendering rules, single-source output schema, and `docs/CLI.md` consistency. |
| `ref-cli-tool-wrapping` | Conventions for `dhcli` runtime commands that wrap MCP tools: the five wrapper categories, the shared `_wrapping` helpers, type scoping via `--system`/id/group-doc (never subgroups), path-flag locality (CLI-read vs server-side), and the `wraps_tool` schema-drift contract. |
| `ref-configuration-conventions` | Canonical reference for the project's config model: JSON5 + Pydantic v2 + `${env:VAR}` / `${file:PATH}` templating; no ad-hoc env reads or `DEFAULT_FOO` constants. |
| `ref-documentation-roles` | Defines the role (audience and scope) of every top-level markdown document; loaded by the docs workflows (`docs-improve`, `docs-accuracy`) and `cli-command-add` to keep edits in-scope and prevent content drift. |
| `ref-logging-standards` | Logger instantiation, `[module:function] Action: details` message format, log levels, sensitive-data rules, redaction-aware Pydantic model logging. |
| `ref-markdown-documentation-standards` | Markdown formatting: JSON/JSON5 code block requirements, placeholder formatting, headings, links, tables, prose conventions. |
| `ref-mcp-module-organization` | Module placement and design patterns for MCP tool modules under `src/deephaven_mcp/mcp_systems_server/_tools/`. |
| `ref-output-serialization-conventions` | Project conventions for serializing values into user-facing output (MCP tool return dicts and CLI output fields). The hub where output-value rules accrete — enum-value casing and MCP-layer payload shape. |
| `ref-project-reference` | High-level project map: architecture, server entry points and ports, config layout, code-quality check commands, test clients. |
| `ref-python-coding-practices` | Project-wide Python style and conventions as numbered rules — apply before writing or reviewing any Python under `src/` or `tests/`, including docstring-only edits. Logging format, docstring content, and MCP tool module layout are owned by `ref-logging-standards`, `pydocs-improve`, and `ref-mcp-module-organization` respectively. |
| `ref-skill-authoring-standards` | Structural standards for skill files: composition hierarchy, frontmatter contract, `ref-` prefix rule for human-invocability, body shape, lifecycle, precedence. |
| `ref-skill-effectiveness` | Content-quality standards for any agent-facing prose: triggerability, actionability, outcome orientation, pattern density, clarity, hedge-word audit, anti-patterns, field failure signals. |

## Adding code & config

| Skill | Purpose |
| --- | --- |
| `mcp-tool-add` | Add a new MCP tool to the systems server end-to-end (placement, registration, docstring, logging, tests, docs). Wraps `ref-mcp-module-organization` + `pydocs-improve` + `ref-logging-standards`. |
| `cli-command-add` | Add a new command to the `dhcli` CLI end-to-end (click + `@run_async` + `CliError`, tests, `docs/CLI.md`, agents manifest). Wraps `ref-python-coding-practices` rule 15 + `ref-documentation-roles` + `ref-cli-help-standards`. |
| `config-field-add` | Add a new field, setting, or tunable to the JSON config tree (`server.json`, `cli.json`, `community/`, `enterprise/`). Wraps `ref-configuration-conventions`. |
| `skill-add` | Add a new agent skill end-to-end (composition gate, name + prefix decision, description, body, parent cross-links, catalog README row). Wraps `ref-skill-authoring-standards` + `ref-skill-effectiveness` + `ref-agents-md-curation`. |

## Reviewing code & docs

| Skill | Purpose |
| --- | --- |
| `review-changes` | Deep review of a changeset across all file types. |
| `review-python-file` | Comprehensive single-file Python review (design, correctness, security, types, pydocs, logging, tests). |
| `pydocs-accuracy` | Surgical Python docstring correctness fixes — no restructuring. |
| `pydocs-improve` | Full Python docstring overhaul: accuracy + restructure + missing-section enforcement. The canonical MCP tool "Terminology Note" and "Format Accuracy for AI Agents" wording lives in [`pydocs-improve/mcp-tool-sections.md`](pydocs-improve/mcp-tool-sections.md). |
| `docs-accuracy` | Surgical Markdown documentation accuracy fixes against the source code. |
| `docs-improve` | Full Markdown documentation overhaul: accuracy + organization + missing content + link fixes. |
| `cli-help-accuracy` | Surgical correctness fixes to CLI help — documented flags, arguments, error codes, exit codes, and output fields verified against the handler across `--help`, the agents manifest, and `docs/CLI.md`. Wraps `ref-cli-help-standards`. |
| `cli-help-improve` | Full overhaul of a command's help surface: fill missing sections, single-source the `OutputSpec`, reconcile all three description surfaces. Wraps `ref-cli-help-standards`. |
| `skill-review` | Review an existing agent skill or `AGENTS.md` against the standards (composition audit, effectiveness audit, structural audit, cross-reference audit, README sync). Wraps `ref-skill-authoring-standards` + `ref-skill-effectiveness` + `ref-agents-md-curation`. |

When deciding between the `*-accuracy` and `*-improve` variants, see the "When to use this vs. the other" callout in each skill.

## Running tests & checks

| Skill | Purpose |
| --- | --- |
| `tests-run` | Full unit-test suite with coverage; reports failures and uncovered lines. |
| `tests-run-file` | Single test file's tests with coverage — required for per-file coverage assessment. |
| `tests-improve` | Comprehensively improve the test suite to 100% per-source-file coverage. |
| `integration-tests-run` | Run integration tests (Docker / pip / subprocess-based) — encodes the `-s` flag requirement and other silent-failure traps. |
| `check-deps-fresh` | Dependency freshness check (fresh resolve + mypy + pytest) plus failure diagnosis. |
| `run-precommit` | Run `precommit.sh` before committing (isort, black, ruff, mypy, codespell, markdownlint, stopping at the first failure) — it modifies files in place, so re-check the diff afterward. |

## Operations & ad-hoc

| Skill | Purpose |
| --- | --- |
| `mcp-stress-test` | Stress test a deephaven-docs MCP server (dev or prod) by calling `docs_chat` N times sequentially. |
