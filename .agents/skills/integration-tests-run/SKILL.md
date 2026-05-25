---
name: integration-tests-run
description: Run integration tests (Docker / pip / subprocess-based) — encodes the -s flag requirement and other gotchas that cause silent failures otherwise
---

Integration tests are marked `@pytest.mark.integration` and are **excluded by default** in `pyproject.toml`'s `addopts` (`-m 'not integration'`).

## Invocation

```bash
uv run pytest -m integration -s
```

## Required flags

- **`-m integration`** — selects integration tests (deselected by default).
- **`-s`** — disables pytest's stdout/stderr capture. **Required.** Pytest's capture mechanism interferes with `asyncio.subprocess.PIPE`; the Deephaven JVM detects the broken pipe and aborts with `Aborted!`. The failure mode is non-obvious: the pip integration test will pass in isolation and fail when run after Docker tests under capture.

## Other requirements

- **Java 11+ in PATH** — Deephaven server subprocesses require it.
- **Docker daemon running** — for the Docker-launched session tests.
- **Single test file**: `uv run pytest tests/path/to/test_<file>_integration.py -s` (the `-m integration` selector is implied by file content but the `-s` flag is still required).

## Cross-references

- Naming convention: `test_<file>_integration.py` (see `_python-coding-practices` rule 5).
- For unit-test-only runs, see `tests-run` and `tests-run-file`.
