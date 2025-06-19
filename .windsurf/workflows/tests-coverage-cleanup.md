---
description: Bring all unit tests to 100% coverage
---

Do a very detailed cleanup of all unit tests.

For every file in src/deephaven_mcp except _version.py:
1. Make sure that there is a test file.
2. Make sure the test file is located in the right file with a name that meets project standards.
3. Analyze the test file to make sure that all of the tests are appropriate.
4. Add missing tests.  Testing private functions is appropriate to create more simple tests.
5. Remove unneeded tests.
6. Restructure tests where appropriate.
7. Run the individual test file.  Test files must be run one-by-one to get the best assessment of how well test files cover the code.
8. Target 100% coverage.  If there is less than 100% coverage, attempt to get the coverage to 100%.

Running "uv run pytest" generates a coverage report containing uncovered lines.  To run a file, just use "uv run pytest <file>".  Do not use "--cov=" or "--cov-report=".  These are already taken into account.

I want to know that the test file for a source file covers 100% of the source file.  Running all tests together does not provide this information.