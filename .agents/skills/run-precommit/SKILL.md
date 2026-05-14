---
name: run-precommit
description: Run precommit.sh — applies all code quality checks for Python and TypeScript in sequence; modifies files in place; run before committing
---

```bash
./bin/precommit.sh
```

Runs in order: isort → black → ruff → mypy → markdownlint → prettier → eslint → tsc

**It modifies files** — isort, black, ruff (with `--fix`), markdownlint, prettier, and eslint (with `--fix`) all apply fixes in place. After a successful run, check for modified files and include them in the commit.

It does not run tests. Use the `tests-run` skill (Python) or `tests-run-ts` skill (TypeScript) for that.

The script stops at the first failure (`set -euo pipefail`). isort, black, prettier, and markdownlint are unlikely to fail since they auto-fix. Ruff and eslint may report unfixable lint errors; mypy and tsc report type errors — all require manual fixes before re-running.
