---
description: Run the dependency freshness check (fresh resolve + mypy + pytest) and diagnose any failures
---

1. Run `bash bin/check-deps-fresh.sh` and report the output.
// turbo
2. If the script exits successfully, summarize which versions of `pydeephaven`, `deephaven-server`, and `deephaven-coreplus-client` were resolved (extract from the `uv pip list` section). Stop.
3. If mypy fails, list each error with file/line and a one-sentence root-cause hypothesis. Do NOT edit code yet — wait for the user to confirm before making fixes.
4. If pytest fails, group failures by test file, show the first failure traceback, and propose a minimal hypothesis for each group. Do NOT edit code yet — wait for the user to confirm.
5. End with a one-line verdict: `PASS` (with resolved versions) or `FAIL` (with the failing stage: `sync` / `mypy` / `pytest`).
