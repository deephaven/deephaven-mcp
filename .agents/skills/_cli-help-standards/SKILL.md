---
name: _cli-help-standards
description: The content contract for dh-mcp CLI help — required help sections, build_help usage, plain-text/no-RST boundary, the no-rewrap marker, single-source output schema, and the three-surface consistency rule (docs/CLI.md, --help, introspect) — invoke when authoring or reviewing any command help, option help, or the introspect manifest
user-invocable: false
---

# CLI Help Standards

The dh-mcp CLI is described by three surfaces that must agree. Help text is plain text rendered verbatim by click in the terminal and surfaced verbatim in the introspect manifest, so it carries no reStructuredText markup. Every leaf command's help follows a fixed section contract so a human or an AI agent can operate the command without reading the source or `docs/CLI.md`.

## 1. Three surfaces, one contract

| Surface | Role | Canonical location | Edited via |
| --- | --- | --- | --- |
| `docs/CLI.md` | Exhaustive reference | `docs/CLI.md` | `docs-improve` / `docs-accuracy` (`_documentation-roles`) |
| `--help` text | Self-sufficient per-command help | `cli/_help.py` (`build_help`) | `cli-help-improve` / `cli-help-accuracy` |
| introspect manifest | Machine surface for agents | `cli/_commands/introspect.py` (`build_manifest`) | `cli-help-improve` / `cli-help-accuracy` |

**Consistency rule.** A command's flags, arguments, exit codes, `error_code`s, and output fields must match across all three surfaces. Change one, update the other two in the same edit. The contract tests in `tests/cli/test_help_contract.py` (per-command section coverage, parametrized over the live click tree), `tests/cli/test__help.py` (`build_help` rendering), and `tests/cli/_commands/test_introspect.py` (manifest cleanliness + output schema) pin the `--help`/manifest side; `docs/CLI.md` has no automated check — verify it by hand.

## 2. Help-content contract — every leaf command

Compose help with `build_help(...)`. Required sections (drop one only when genuinely not applicable — a command with no positional args has no Arguments section):

- **Summary** — one imperative line, ≤ ~70 chars. Click also uses it as the parent group's short help.
- **Description** — what it does *and* when to use it. State side effects (spawns a daemon, quarantines a file).
- **Arguments** — every positional argument, with its meaning and a discovery hint. Click renders no help for positional arguments, so the `arguments=` block is the only place a reader learns what `NAME` is and how to find a valid value.
- **Options** — set `help=` on every `click.option`: state the effect **and the value space** (what to type), never value-blind help that only restates the effect. For a **closed** value set use a `click.Choice` — it validates and self-documents (e.g. `--launch-method`, `--language`). For a **free or dynamic** value, name the form and point to a discovery command (e.g. `--system`: a system name — `community` or a configured Enterprise system; run `dh-mcp system list`). Include decode/repeat rules; set `show_default=True` for options with a default. Click renders the Options table itself. Root-group options are position-immaterial (any position accepted); subcommand-local options must follow their subcommand; never redeclare a root option on a subcommand. Mechanism: `cli/_main.py` (`_lift_root_options`).
- **Output** — what the command prints, and the key fields an agent reads under `-o json`. Single-source it (§4).
- **Examples** — at least one human example and at least one agent example (`-o json … | jq …`).
- **Exit codes** — the numeric process codes (default set covers `0`/`2`/`3`).
- **Error codes** — the stable `error_code` strings this command can emit (e.g. `tool_not_found`, `arg_parse_error`). Distinct from numeric exit codes; agents branch on them. A pure discovery command with no `CliError` path (`introspect`) omits this section.
- **See also** — related commands and the discovery flow (`tool call` points at `tool list` and `tool show`).

**Group commands**: orient the reader and say when to use which verb. **Root group**: a getting-started workflow plus an explicit pointer to `dh-mcp introspect` for agents.

Canonical implementation: `cli/_commands/tool.py` (`tool_call`, `_OUTPUT_CALL`) is the model command — it exercises every section (Arguments, Output, Examples, See also, Exit codes, Error codes) and the single-source output spec.

## 3. build_help sections

Every help string is built by `build_help`; never hand-concatenate. Parameters map to sections rendered in this fixed order: summary → description → `arguments` → `output` → `examples` → `see_also` → `environment` (default set) → `exit_codes` → `error_codes`.

No `build_help` section takes a positional `(str, str)` tuple. `arguments` and `environment` take `HelpEntry(name, help)` objects; `error_codes` and `exit_codes` take enum **members** (`ErrorCode` / `ExitCode`), rendered as `(value, help_text)` so the descriptions are single-sourced from the enums (`cli/_errors.py`) and can never disagree with the introspect manifest. `exit_codes` members are `SUCCESS`/`USER_ERROR`/`TOOL_ERROR`. Put any per-command failure nuance in the `description`, not a re-typed code description.

```python
help=build_help(
    summary="Invoke a single MCP tool and print its result.",
    description="...",
    arguments=(HelpEntry("NAME", "Tool name. Run 'dh-mcp tool list' to discover names."),),
    output=_OUTPUT_CALL,              # see §4
    examples=("$ dh-mcp tool call session_list", "$ dh-mcp -o json tool call ... | jq ."),
    see_also=("dh-mcp tool list", "dh-mcp tool show NAME"),
    exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
    error_codes=(ErrorCode.ARG_PARSE_ERROR, ErrorCode.TOOL_RETURNED_ERROR),
)
```

Canonical implementation: `cli/_help.py` (`build_help`, `HelpEntry`, `COMMON_ENV_VARS`), `cli/_errors.py` (`ErrorCode`, `ExitCode`).

## 4. Output is single-source

Define each leaf command's output shape once as an `OutputSpec` constant, then reference that one constant twice in the decorator: `build_help(output=SPEC)` renders the human-readable **Output** section, and `output_spec=SPEC` attaches the structured shape that introspect emits. One definition, two consumers — no drift.

- `OutputSpec(mode, fields, note)` and `OutputField(name, type, help)` live in `cli/_help.py`.
- Leaf commands are `HelpfulCommand` instances (carried by `HelpfulGroup.command_class`), which store `output_spec`; introspect reads it and emits `commands[...].output = {mode, fields: [{name, type, help}], note}`.
- A command that prints free-form or human-only output uses `mode="text"` with an empty `fields` tuple.

Canonical implementation: `cli/_help.py` (`OutputSpec`, `HelpfulGroup`), `cli/_commands/introspect.py` (`_describe_command`).

## 5. Plain text, not reStructuredText

Surfaced text is plain text: command/group help, every `click.option(help=...)`, and group docstrings used as help. No double-backtick literals, no `:func:` / `:class:` roles — they render as literal noise in the terminal and leak into the manifest. Use single quotes for inline literals where emphasis helps (`'config'`, `'tail -f'`).

Internal docstrings and comments that are *not* surfaced (helper functions, module docstrings) keep the repo's RST convention — markup is a property of the rendering target, not the file. Apply `pydocs-improve` to those; this skill governs only the surfaced strings.

## 6. No-rewrap marker

Click rewraps each help paragraph unless it begins with a backspace marker on its own line. `build_help` prefixes every pre-formatted section (Arguments, Output, Examples, Environment, Exit codes, Error codes) with `_NO_WRAP` so the column alignment and one-item-per-line layout survive. Never put `\b` in a summary or description (prose should rewrap). introspect strips the marker via `_clean_help` so the manifest is control-char-free.

## 7. Verify

- `dh-mcp <noun> <verb> --help` renders every required section as a distinct block.
- `dh-mcp introspect | jq '.commands.<noun>.subcommands.<verb>'` shows `params`, the relevant error codes, and `output`; no `` `` `` and no `\b` anywhere in the manifest.
- `uv run pytest tests/cli/test_help_contract.py tests/cli/test__help.py tests/cli/_commands/test_introspect.py` is green.
