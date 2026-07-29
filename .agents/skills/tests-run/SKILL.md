---
name: tests-run
description: Run the full unit test suite with coverage — invoke to verify a change across the whole project; use tests-run-file when assessing one source file's per-file coverage. Reports test failures and uncovered lines
---

Run all unit tests with coverage:

```bash
uv run pytest
```

pytest is pre-configured in `pyproject.toml` to collect coverage automatically — no extra flags needed. The run produces a coverage report showing which lines are not covered. Report any test failures and uncovered lines to the user.
