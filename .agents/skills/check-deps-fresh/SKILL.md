---
name: check-deps-fresh
description: Run the dependency freshness check — invoke before a release, or after changing dependencies in pyproject.toml. Performs a fresh resolve then runs mypy and pytest against it, and diagnoses any failures
---

## Steps

1. Run `bash bin/check-deps-fresh.sh` and report the output.
2. Triage the result:
   - **Exit 0 (success)**: summarize which versions of `pydeephaven`, `deephaven-server`, and `deephaven-coreplus-client` were resolved (extract from the `uv pip list` section).
   - **`sync` failed**: report the resolver error verbatim. Do not edit code.
   - **`mypy` failed**: list each error with file/line and a one-sentence root-cause hypothesis. Do not edit code yet — wait for the user to confirm before making fixes.
   - **`pytest` failed**: group failures by test file, show the first failure traceback, and propose a minimal hypothesis for each group. Do not edit code yet. Note: the script runs pytest with `--no-cov`; no coverage report is produced.
3. End with a one-line verdict: `PASS` (with resolved versions) or `FAIL: <stage>` where `<stage>` is `sync`, `mypy`, or `pytest`.
