---
name: review-basic
description: Perform a basic review and cleanup of a file — pydocs, unused imports, logging consistency
---

1. Apply the `pydocs-improve` skill to review and improve all docstrings in the file.
2. Remove unused imports. The `run-precommit` skill (ruff) will catch any that remain.
3. Apply the `logging-standards` skill to review logging coverage and consistency.
4. Do not remove TODOs without a very good reason.
