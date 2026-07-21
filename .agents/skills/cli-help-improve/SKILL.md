---
name: cli-help-improve
description: Comprehensively improve the help surface of one or more dhcli CLI commands — fill missing sections, single-source the output schema, fix inaccuracies, and reconcile the description surfaces (the HelpSpec behind --help and the agents manifest, and docs/CLI.md) — invoke when a command's help is thin, stale, or newly added
---

**When to use this vs. `cli-help-accuracy`**: use `cli-help-improve` for a full review (accuracy + missing sections + output single-sourcing + reconciling all three surfaces). Use `cli-help-accuracy` for surgical correctness-only fixes when the help structure is already complete.

## Before doing anything

Load `_cli-help-standards` — it is the contract this skill enforces. Identify the target: a single command module under `cli/_commands/`, or the whole CLI. For each command, the surfaced help (the command's `HelpSpec`, every `click.option(help=...)`, group docstrings) is in scope; internal docstrings are not (those are `pydocs-improve`'s job).

## Steps

For each command in scope:

1. **Read the handler.** Determine what the command actually does, every positional argument and option, every `CliError(code=...)` it raises (directly or via a shared helper like `acquire_daemon`), the exit codes it can return, and the exact shape of what it prints (`format_output(...)` payload).
2. **Check section coverage** against `_cli-help-standards` §2. Add any missing section via the command's `HelpSpec`: Summary, Description (with side effects), Arguments, Output, Examples (one human + one agent), See also, Exit codes, Error codes. Drop a section only when genuinely not applicable (no positional args → no Arguments; pure discovery command → no Error codes).
3. **Single-source the output** (§4). Define one `OutputSpec` constant; pass it as the spec's `output=` field (`output_spec` derives from it). Each `OutputField(name, type, help)` must name a real key in the printed payload. Free-form output uses `mode="text"` with empty fields.
4. **Apply `cli-help-accuracy`** to the command — every documented flag, argument, error code, exit code, and output field must match the code.
5. **Strip RST** (§5) from every surfaced string: no double-backtick literals, no `:func:`/`:class:` roles. Use single quotes for inline literals.
6. **Reconcile the third surface.** Update the command's entry in `docs/CLI.md` so flags, arguments, exit codes, error codes, and output fields agree. `docs/CLI.md` has no automated check — verify by hand (apply `docs-accuracy` for that file).
7. **Verify** (§7): render `--help`, inspect the agents node (params + error codes + output, no double-backtick literals or `\b`), and run the contract tests.

## Verify

```bash
dhcli <noun> <verb> --help
dhcli <noun> <verb> --agents    # or: dhcli agents command <noun> <verb> — both default to compact json
uv run pytest tests/cli/test_help_contract.py tests/cli/test__help.py tests/cli/_commands/test_agents.py
```

If you added or changed a command's params, output, or errors, also apply `tests-improve` to the command's test file. Do not weaken the contract tests to make help pass — fix the help.
