---
name: tests-run-file
description: Run a single test file's tests with coverage — required for assessing per-file coverage of a single source file
---

Run a single test file:

```bash
uv run pytest tests/path/to/test_<file>.py
```

pytest is pre-configured for coverage in `pyproject.toml`. **Do not add `--cov=` or `--cov-report=` flags** — they are already set; overriding them silently disables the project's coverage configuration.

Per-file runs are required when assessing whether a single source file is fully covered by its own test file. The full-suite run (`tests-run`) cannot answer that question because coverage from unrelated test files masks gaps.

For integration tests (`test_<file>_integration.py`, marked `@pytest.mark.integration`), see `integration-tests-run` — they have separate invocation rules.
