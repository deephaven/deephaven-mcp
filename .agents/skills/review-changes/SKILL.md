---
name: review-changes
description: Perform a deep review of a changeset — invoke when reviewing a set of code changes spanning more than one file, before committing; use review-python-file for a single file in depth. Routes each file to its type-specific review skill, then adds the cross-cutting checks (unrequested surface, design consistency, DRY, coverage) no single-file review can catch
---

**Review like a senior engineer.** Every finding must answer three questions in concrete terms: *what is wrong*, *what is better*, *why the change is worth its cost*. All topics are fair game — correctness, security, design, duplication, clarity, naming, structure, tests.  Be conscious of suggestions that do not serve a purpose.

Review the changeset described in the prompt. Common forms:

| Prompt | Git command to get changed files |
|---|---|
| "uncommitted changes" (default) | `git diff HEAD --name-only` |
| "changes on this branch" / "vs main" | `git diff main...HEAD --name-only` |
| "staged changes" | `git diff --cached --name-only` |
| "the last commit" | `git diff HEAD~1 HEAD --name-only` |
| "changes vs &lt;branch&gt;" | `git diff <branch>...HEAD --name-only` |

If no changeset is specified, default to uncommitted changes.

## Steps

1. **Identify changed files**: Run the appropriate git command above to get the full list of changed files. If the changeset touches an area you have not worked in, load `ref-project-reference` first for the module map — what each package owns and which types cross package boundaries.

2. **Review each file** based on its type:

   | File type | Skill to apply |
   |---|---|
   | `.py` | `review-python-file` |
   | CLI help-bearing files (`cli/_commands/*.py`, `cli/_help.py`, `cli/_manifest.py`, `cli/_errors.py`) | `review-python-file` **plus** `cli-help-accuracy` (the surfaced help, error-code registry, and output schema must match the code and `docs/CLI.md`) |
   | MCP tool signatures (`mcp_systems_server/_tools/*.py`) **or** tool-wrapping CLI commands (`cli/_commands/*.py` setting `wraps_tool`/`wraps_tools`) | `review-python-file` **plus** run `uv run --extra test pytest tests/cli/test_tool_wrapper_drift.py -q` and apply `ref-cli-tool-wrapping` — a changed tool signature must be reflected in its wrapper's flags / `intentionally_unsupported`, and vice versa |
   | `.md`, `.rst` | `docs-accuracy` |
   | Pydantic config (`config/schema/*.py`, `config/tree.py`), `*.json5` examples under `config-samples/ai/config/`, `docs/CONFIGURATION.md` | `ref-configuration-conventions` |
   | `docs/ENV.md` | `ref-configuration-conventions` **plus** `docs-accuracy` — the canonical environment-variable inventory; verify every entry still matches its reader in the code |
   | `.agents/skills/**/SKILL.md`, `AGENTS.md` | `skill-review` **plus** run `uv run pytest tests/agents/test_skills_catalog.py` — it pins the frontmatter contract, the README row, and every section citation and canonical-implementation pointer across the catalog, none of which markdownlint checks |
   | `.json`, `.json5`, `.yaml`, `.toml` (config files outside the schemas above) | `ref-markdown-documentation-standards` for inline doc-block formatting; verify syntax + cross-reference with any matching Pydantic schema |
   | `.sh`, `bin/*`, `.github/workflows/*.yml` (shell scripts, CI) | Verify shellcheck-clean, `set -euo pipefail` for new scripts, idempotent, and that any failure mode produces a non-zero exit |
   | Other | Review for correctness and appropriateness — name the specific property being checked (e.g., "Dockerfile uses pinned base image", "lockfile changes match `pyproject.toml`") rather than gesturing |

3. **Cross-cutting review** — assess the changeset as a whole:
   - **Unrequested surface**: List every new environment variable, config field, CLI flag, error code, output field, MCP tool, noun or verb, exit code, public export, config file kind — or any other user-visible contract — in the changeset, and name the requirement each traces to. A surface entailed by an approved change (a requested command's own flags and error codes) traces to that request and is in scope. Flag any that trace to none (`AGENTS.md` *Scope discipline*). A well-executed unrequested feature — typed, tested, documented, fully covered — passes every other check in this list, so this is the only one that catches it.
   - **New environment variable**: Scan the whole diff — any file, including tests and docs — for an `os.environ` read, a click `envvar=`, or a new variable name. Each one must be explicitly requested and must carry its `docs/ENV.md` entry in the same changeset. A JSON field plus `${env:VAR}` templating is the default answer; a new variable is a product decision, not an implementation detail. Apply `ref-configuration-conventions`, plus `cli-command-add`'s anti-patterns for CLI code.
   - **Design consistency**: Names, abstractions, and patterns introduced in one file match what siblings already use. Flag any new pattern that supersedes an existing one without also retiring the old one.
   - **Code changes that stale a skill**: A changeset that adds, renames, or moves a module, or changes a symbol a skill names as a canonical implementation, staled a skill even when no skill file is in the diff. Check `ref-project-reference` (the module map) and grep the catalog for the old path or symbol — `grep -rn '<old-name>' .agents/ AGENTS.md`. This is the case the per-file routing table cannot catch, because the skill file itself is unchanged.
   - **DRY across files**: Identify any block of substantive logic that appears in two or more changed files; either extract to a shared helper in this changeset or flag for follow-up.
   - **Test coverage of new code paths**: Every new branch, error path, and public function in the changeset has at least one test that exercises it. List by file any new code paths with zero test coverage.
   - **Public-surface contract**: Every new or modified `__all__`, `__init__.py` export, MCP tool registration, CLI command, error code, or config field has its corresponding test pinning the surface (per `tests-improve` step 1 for `__init__.py`; per `mcp-tool-add` step 4 for tool registration; per `cli-command-add` for CLI; per `config-field-add` for config).
   - **Output-payload consistency across tools/CLI**: When the changeset touches more than one producer of a shared field (e.g., a field appearing in both an MCP tool return and the matching CLI wrapper), the emitted string values must agree exactly. Apply `ref-output-serialization-conventions`.
   - *...or anything else at the changeset level that looks off — backward compatibility, dependency/lockfile drift, performance regressions, security exposure, documentation that no longer matches the code. This list is illustrative, not exhaustive; trust your judgment.*

Do not remove TODOs without a very good reason.
