---
name: review-changes
description: "Perform a deep review of a set of code changes — all file types covered"
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

1. **Identify changed files**: Run the appropriate git command above to get the full list of changed files.

2. **Review each file** based on its type:

   | File type | Skill to apply |
   |---|---|
   | `.py` | `review-python-file` |
   | CLI help-bearing files (`cli/_commands/*.py`, `cli/_help.py`, `cli/_errors.py`) | `review-python-file` **plus** `cli-help-accuracy` (the surfaced help, error-code registry, and output schema must match the code and `docs/CLI.md`) |
   | `.md`, `.rst` | `docs-accuracy` |
   | Pydantic config (`mcp_systems_server/config/*.py`, `cli/config/*.py`, `_*_config.py`), `*.json5` examples under `config-samples/ai/config/`, `docs/CONFIGURATION.md` | `_configuration-conventions` |
   | `.agents/skills/**/SKILL.md`, `AGENTS.md` | `skill-review` |
   | `.json`, `.json5`, `.yaml`, `.toml` (config files outside the schemas above) | `_markdown-documentation-standards` for inline doc-block formatting; verify syntax + cross-reference with any matching Pydantic schema |
   | `.sh`, `bin/*`, `.github/workflows/*.yml` (shell scripts, CI) | Verify shellcheck-clean, `set -euo pipefail` for new scripts, idempotent, and that any failure mode produces a non-zero exit |
   | Other | Review for correctness and appropriateness — name the specific property being checked (e.g., "Dockerfile uses pinned base image", "lockfile changes match `pyproject.toml`") rather than gesturing |

3. **Cross-cutting review** — assess the changeset as a whole:
   - **Design consistency**: Names, abstractions, and patterns introduced in one file match what siblings already use. Flag any new pattern that supersedes an existing one without also retiring the old one.
   - **DRY across files**: Identify any block of substantive logic that appears in two or more changed files; either extract to a shared helper in this changeset or flag for follow-up.
   - **Test coverage of new code paths**: Every new branch, error path, and public function in the changeset has at least one test that exercises it. List by file any new code paths with zero test coverage.
   - **Public-surface contract**: Every new or modified `__all__`, `__init__.py` export, MCP tool registration, CLI command, error code, or config field has its corresponding test pinning the surface (per `tests-improve` step 1 for `__init__.py`; per `mcp-tool-add` step 4 for tool registration; per `cli-command-add` for CLI; per `config-field-add` for config).
   - *...or anything else at the changeset level that looks off — backward compatibility, dependency/lockfile drift, performance regressions, security exposure, documentation that no longer matches the code. This list is illustrative, not exhaustive; trust your judgement.*

Do not remove TODOs without a very good reason.
