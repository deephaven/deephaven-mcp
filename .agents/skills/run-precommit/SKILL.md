---
name: run-precommit
description: Run precommit.sh before committing — invoke after any code or documentation edit, and before staging. It modifies files in place, so re-check the diff afterward. Applies isort, black, ruff, mypy, codespell, and markdownlint in sequence, stopping at the first failure
---

```bash
./bin/precommit.sh
```

Runs in order: isort → black → ruff → mypy → codespell → markdownlint. **It modifies files** — isort, black, ruff, and markdownlint all apply fixes in place (codespell only reports; fix manually or run `uv run codespell -w` and review the diff — its auto-fix can rewrite intentional forms). After a successful run, check for modified files and include them in the commit.

It does not run tests. Use the `tests-run` skill for that.

The script stops at the first failure (`set -euo pipefail`), so a failure in ruff means mypy, codespell, and markdownlint have not yet run. isort, black, and markdownlint are unlikely to fail since they auto-fix. Ruff may report unfixable lint errors; mypy reports type errors; codespell reports misspellings and British spellings — all require manual fixes before re-running. The spelling rule and its inline-ignore convention are owned by `ref-python-coding-practices` rule 8 (markdown delta: `ref-markdown-documentation-standards` Prose conventions).

Ruff/mypy failures need code edits — apply the `review-python-file` skill (which loads `ref-python-coding-practices`, `ref-logging-standards`, and `ref-mcp-module-organization`) before fixing, so the fix matches project standards rather than only silencing the linter.
