---
name: cli-help-accuracy
description: Verify dh-mcp CLI help is factually accurate against the code — documented flags, arguments, error codes, exit codes, and output fields all match the handler — and fix inaccuracies in place across --help, the introspect manifest, and docs/CLI.md
---

**When to use this vs. `cli-help-improve`**: use `cli-help-accuracy` for surgical correctness-only fixes when the help structure is already complete. Use `cli-help-improve` for a full review that may add missing sections, single-source the output schema, or reconcile all three surfaces.

## Before doing anything

Load `_cli-help-standards`. The three surfaces — `--help` (`build_help` in the command), the `introspect` manifest (`build_manifest`), and `docs/CLI.md` — must agree. Fixing an inaccuracy in one surface without the other two reintroduces drift, so check and fix all three together.

## Verify every claim against the handler

For each command in scope, read the handler and confirm:

- **Options and arguments** — every documented flag/argument exists with the stated name, type, default, and effect; every actual option has a `help=`; `show_default=True` where there is a default. No documented flag is missing from the code, and no code flag is undocumented.
- **Error codes** — the `error_codes=` block lists exactly the `error_code`s the command can raise. Trace each `CliError(code=...)` in the handler and in any shared helper it calls (e.g. `acquire_daemon` raises `daemon_startup_timeout` / `daemon_not_running` / `daemon_registry_corrupt`). No phantom codes; no missing ones.
- **Exit codes** — the documented numeric codes match reality. Every `CliError` exits `2` except `ErrorCode.TOOL_RETURNED_ERROR`, the sole code whose `exit_code` is `3`; the exit code is carried by the `ErrorCode`, not a `CliError` parameter (there is no `exit_code=` argument).
- **Output fields** — every `OutputField` names a real key in the `format_output(...)` payload, with the correct type; the `mode` (`object`/`list`/`text`) matches what is printed. The same `OutputSpec` constant feeds both `output_spec=` and `output=` (single source — never two literals that can drift). Each field's `help` string must accurately describe the value vocabulary the code emits — apply `_output-serialization-conventions`.
- **Examples** — each example command is runnable and uses real flags/args; `jq` filters reference real output keys.
- **Plain text** — no `` `` `` or `:func:`/`:class:` markup in any surfaced string.

## Fix

For each inaccuracy: report what the help says, what the code says, and fix it in place — in the command's `build_help`/`OutputSpec`, and in the matching `docs/CLI.md` entry. Then run:

```bash
uv run pytest tests/cli/test_help_contract.py tests/cli/test__help.py tests/cli/_commands/test_introspect.py
```
