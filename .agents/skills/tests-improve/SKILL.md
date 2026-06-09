---
name: tests-improve
description: Comprehensively improve unit tests across the project — ensure test files exist, add missing tests, remove unneeded tests, restructure, and achieve 100% coverage per source file
---

For every file in `src/deephaven_mcp` except `_version.py`:

1. **Make sure that there is a test file.**
    - **`__init__.py` files count.** Every `__init__.py` — even one that only declares `__all__` or re-exports from siblings — gets its own `test_init.py`. The package surface is part of the API contract; an untested `__init__.py` is a silent-refactor hazard.
    - **What the `test_init.py` must pin**: the exact set of names in `__all__`, that each re-export is the same object as the internal definition, and that no `_`-prefixed names leak into the public surface.
    - **Canonical implementations**: `tests/mcp_systems_server/config/test_init.py`, `tests/auth/middleware/test_init.py`, `tests/cli/config/test_init.py`.
2. **Make sure the test file is in the correct directory with a name that meets project standards.**
    - Unit tests: `test_<file>.py`. Integration tests: `test_<file>_integration.py`. Both live in `tests/<package>/` mirroring the source (see `_python-coding-practices` rule 5).
    - For `__init__.py` the test file is `test_init.py` (single underscore between `test` and `init`, matching `tests/auth/middleware/test_init.py`).
3. Analyze the test file and flag tests that are dead, redundant, test implementation detail rather than observable behavior, or are overly fragile (assert on incidental output, internal call order, or private state). List the flagged tests before changing anything.
4. Add tests for every uncovered branch and error path (coverage rises toward 100%); testing private functions is appropriate for simpler, more targeted tests.
5. Remove the tests flagged dead or redundant in step 3; confirm coverage holds after removal.
6. Restructure the tests flagged fragile in step 3 (those asserting on incidental output, call order, or private state) to assert on observable behavior instead.
7. Run the individual test file. Test files must be run one-by-one to accurately assess per-file coverage.
8. Target 100% coverage. If coverage is below 100%, add tests until it reaches 100%.

pytest is pre-configured for coverage — no extra flags needed. To run a single file: `uv run pytest <file>`. Do not add `--cov=` or `--cov-report=`; these are already configured.

Per-file runs are required because running the full suite together does not show whether a source file's own test file covers it completely. See the `tests-run-file` skill for the per-file invocation rules.

After per-file runs pass, run the `tests-run` skill to verify the full suite. For integration tests (`test_<file>_integration.py`), use the `integration-tests-run` skill — they have separate invocation rules (`-m integration -s`).
