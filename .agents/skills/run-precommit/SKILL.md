---
name: run-precommit
description: Run the precommit script to auto-format, lint, and type-check before committing
---

```bash
./bin/precommit.sh
```

Runs in order: isort → black → ruff → mypy → markdownlint. **It modifies files** — isort, black, ruff, and markdownlint all apply fixes in place. After a successful run, check for modified files and include them in the commit.

It does not run tests. Use the `tests-run` skill for that.

The script stops at the first failure (`set -euo pipefail`), so a failure in ruff means mypy and markdownlint have not yet run. isort, black, and markdownlint are unlikely to fail since they auto-fix. Ruff may report unfixable lint errors; mypy reports type errors — both require manual fixes before re-running.
