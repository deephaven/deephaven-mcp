---
name: tests-improve
description: Comprehensively improve unit tests across the project — ensure test files exist, add missing tests, remove unneeded tests, restructure, and achieve 100% coverage per source file
---

For every file in src/deephaven_mcp except _version.py:
1. Make sure that there is a test file.
2. Make sure the test file is in the correct directory with a name that meets project standards (`test_<file>.py`).
3. Analyze the test file to make sure that all of the tests are appropriate.
4. Add missing tests. Testing private functions is appropriate to create simpler, more targeted tests.
5. Remove unneeded tests.
6. Restructure tests where appropriate.
7. Run the individual test file. Test files must be run one-by-one to accurately assess per-file coverage.
8. Target 100% coverage. If coverage is below 100%, add tests until it reaches 100%.

pytest is pre-configured for coverage — no extra flags needed. To run a single file: `uv run pytest <file>`. Do not add `--cov=` or `--cov-report=`; these are already configured.

Per-file runs are required because running the full suite together does not show whether a source file's own test file covers it completely.
