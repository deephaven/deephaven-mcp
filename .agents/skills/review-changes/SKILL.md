---
name: review-changes
description: "Perform a deep review of a set of code changes — all file types covered: Python files reviewed in full, documentation files checked for accuracy"
---

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
   | `.md`, `.rst` | `docs-accuracy` |
   | Pydantic config (`mcp_systems_server/config/*.py`, `_*_config.py`), `*.json5` examples under `examples/ai/config/`, `docs/CONFIGURATION.md` | `_configuration-conventions` |
   | Other (scripts, CI, etc.) | Review for correctness and appropriateness |

3. **Cross-cutting review** — assess the changeset as a whole:
   - **Design consistency**: Is the design coherent across all changed files?
   - **DRY across files**: Is logic duplicated across the changeset that should be shared?
   - **Overall test coverage**: Does the changeset as a whole have adequate test coverage?

Do not remove TODOs without a very good reason.
