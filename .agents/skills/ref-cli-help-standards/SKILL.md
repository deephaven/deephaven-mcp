---
name: ref-cli-help-standards
description: The content contract for dhcli CLI help — required sections, HelpSpec authoring, plain-text rendering rules, single-source output schema, and docs/CLI.md consistency — invoke when authoring or reviewing any command help, option help, or the agents manifest
user-invocable: false
---

# CLI Help Standards

The dhcli CLI is described by three surfaces from **two authored sources**: in-code help renders both the `--help` text and the agents-manifest node — a `HelpSpec` on the root and every leaf command, a function docstring on each noun group (split into manifest summary/description by `_split_help_text`; see *Help-content contract*, *Group commands*) — and `docs/CLI.md` is written by hand. Help text is plain text rendered verbatim by click in the terminal, so it carries no reStructuredText markup. Every leaf command's help follows a fixed section contract so a human or an AI agent can operate the command without reading the source or `docs/CLI.md`.

## 1. Three surfaces, two sources

| Surface | Role | Canonical source | Edited via |
| --- | --- | --- | --- |
| `docs/CLI.md` | Exhaustive reference | `docs/CLI.md` | `docs-improve` / `docs-accuracy` (`ref-documentation-roles`) |
| `--help` text | Self-sufficient per-command help | the command's `HelpSpec` (`cli/_help.py`, rendered by `build_help`) | `cli-help-improve` / `cli-help-accuracy` |
| agents manifest | Machine surface for agents (`--help` for AI agents) | the same `HelpSpec` (`cli/_manifest.py`: `build_summary_tree`, `build_manifest`, `describe_command`); wired into commands by `cli/_commands/agents.py` | `cli-help-improve` / `cli-help-accuracy` |

**Consistency rule.** `--help` and the agents manifest render from the one `HelpSpec`, so they cannot drift from each other — the surface to reconcile by hand is `docs/CLI.md`: a command's flags, arguments, exit codes, `error_code`s, and output fields must match between the spec and the doc. Change one, update the other in the same edit. The contract tests in `tests/cli/test_help_contract.py` (per-command section coverage, parametrized over the live click tree), `tests/cli/test__help.py` (`build_help` rendering), and `tests/cli/test__manifest.py` (the manifest builders: cleanliness, node schema, content preservation) pin the spec side; `tests/cli/_commands/test_agents.py` pins the `agents` verbs' CLI behavior (output modes, config bypass, flag/verb equivalence). `docs/CLI.md` has no automated check — verify it by hand.

## 2. Help-content contract — every leaf command

Declare help as `help_spec=HelpSpec(...)` on the command decorator. Required sections (drop one only when genuinely not applicable — a command with no positional args has no Arguments section):

- **Summary** — one imperative line, short enough to read at a glance in a command list. Click also uses it as the parent group's short help, so it competes for width with sibling summaries.
- **Description** — what it does *and* when to use it. State side effects (spawns a daemon, quarantines a file).
- **Arguments** — every positional argument, with its meaning and a discovery hint. Click renders no help for positional arguments, so the `arguments=` block is the only place a reader learns what `NAME` is and how to find a valid value.
  - **A defaulted argument names the command that reveals its default.** An argument filled from persistent state when omitted (the sticky context) is the one input the command's own argv does not show, so stating "defaults to the sticky context" without naming `dhcli context show` leaves a term the reader cannot act on — an agent especially, since it cannot glance at the terminal. Keep the pointer terse (`See 'dhcli context show'.`): `build_help` renders the Arguments block pre-formatted, so a long entry does not rewrap. Canonical implementation: `CONTEXT_HINT` in `cli/_context.py` — one definition, referenced by every defaultable argument rather than retyped; pinned by `test_context_defaultable_command_names_its_discovery_command`.
  - **A verb whose target is a name it did not choose states the target-selection rule.** Any verb that acts on a session/system/PQ id — destructive, state-leaving, *or* credential-disclosing — carries `TARGET_SELECTION_HINT` (`cli/_context.py`) on its id argument: act on an id you were given or created, never one merely returned by a listing. The listings span every user's resources including production, so "it came back from `session list`" is not authorization. Placement is the rule: the one-line hint goes on the *verb*; the full `TARGET_SELECTION_GUIDANCE` paragraph goes only on the two listings that produce candidate ids (`session list`, `pq list`) and in the manifest's `conventions` — repeating the paragraph per verb is what pushed the tree-wide surfaces past their budget. `test_target_sensitive_command_states_the_target_selection_rule` enforces the hint structurally for verbs with `--yes`; **a credential-disclosing verb has no structural marker and must be added to `_CREDENTIAL_DISCLOSING_PATHS`** in that test.
  - **A verb that destroys, executes, or leaves state states the hazard when its target is defaultable.** Say what a wrong target costs ("executes or destroys in the wrong worker or system") in the `description`, where click rewraps — not in the Arguments block. Single-source the wording rather than hand-writing it per verb; vague "be careful" phrasing is what agents skip. Canonical implementation: `CONTEXT_RISK_DESTRUCTIVE` / `CONTEXT_RISK_STATEFUL` in `cli/_context.py`, one template with a per-tier clause.
- **Options** — set `help=` on every `click.option`: state the effect **and the value space** (what to type), never value-blind help that only restates the effect. Click renders the Options table itself. A flag's *name* follows whatever a comparable CLI already established for the same idea — apply `ref-cli-design-prior-art`.
  - **Closed value set**: use a `click.Choice` — it validates and self-documents (e.g. `--launch-method`, `--language`).
  - **Free or dynamic value**: name the form and point to a discovery command (e.g. `--system`: a system name — `community` or a configured Enterprise system; run `dhcli system list`).
  - **Decode/repeat and defaults**: include decode/repeat rules; set `show_default=True` for options with a default.
  - **Path-valued option**: state **where the path resolves** — 'read by the CLI' (local, `-` = stdin) or the server-side namespace it names (e.g. "the Enterprise controller's Git-backed script repository"); the locality rule itself is `ref-cli-tool-wrapping` *Path locality*.
  - **Position**: root-group options are position-immaterial (any position accepted); subcommand-local options must follow their subcommand; never redeclare a root option on a subcommand. Mechanism: `cli/_main.py` (`_lift_root_options`).
- **Output** — what the command prints, and the key fields an agent reads under `-o json`. Single-source it (*Output is single-source*).
- **Examples** — at least one human example and at least one agent example (`-o json … | jq …`). A command with structured output must show how to consume it programmatically; `test_structured_command_has_an_agent_example` enforces this.
- **Exit codes** — the numeric process codes (default set covers `0`/`2`/`3`).
- **Error codes** — the stable `error_code` strings this command can emit (e.g. `tool_not_found`, `arg_parse_error`). Distinct from numeric exit codes; agents branch on them. A pure discovery command with no `CliError` path (`agents tree`, `agents errors`) omits this section. State the *set* accurately: the agents node names the codes without their meanings, so a missing code is a failure an agent cannot anticipate, and a phantom one sends it chasing a case that cannot happen.
- **See also** — related commands and the discovery flow (`tool call` points at `tool list` and `tool show`).

**Group commands**: orient the reader and say when to use which verb (group docstrings are the help source; the manifest derives their `summary`/`description` from the docstring's first paragraph and remainder). **Root group**: a getting-started workflow plus an explicit pointer to `dhcli agents tree` (and the universal `--agents` flag) for agents. The root description **enumerates every noun group** — it is the first surface an agent reads (`dhcli --agents`, the `agents tree` root summary), so a missing noun makes that whole feature invisible at the top level. Adding a noun without adding it here is the most likely way a new group goes undiscovered.

Canonical implementation: `cli/_commands/tool.py` (`tool_call`, `_OUTPUT_CALL`) is the model command — it exercises every section (Arguments, Output, Examples, See also, Exit codes, Error codes) and the single-source output spec.

## 3. HelpSpec fields

Every leaf command's help is a `HelpSpec`; never hand-concatenate help text or pass a raw `help=` string. `build_help` renders the spec's fields as sections in this fixed order: summary → description → `arguments` → `output` → `examples` → `see_also` → `environment` (default set) → `exit_codes` → `error_codes`. The manifest emits the same fields structurally — authoring the spec authors both surfaces.

No `HelpSpec` field takes a positional `(str, str)` tuple. `arguments` and `environment` take `HelpEntry(name, help)` objects; `error_codes` and `exit_codes` take enum **members** (`ErrorCode` / `ExitCode`), so a code's description is single-sourced from the enum (`cli/_errors.py`) and never re-typed per command. `--help` renders each code with its text; the agents manifest emits **bare error-code strings** and leaves the meanings to the `dhcli agents errors` registry (*What the agents manifest carries* has the reasoning; never add a second copy of a code's text to fill the apparent gap). `exit_codes` members are `SUCCESS`/`USER_ERROR`/`TOOL_ERROR`. Put any per-command failure nuance in the `description`, not a re-typed code description. An `arguments` entry name is the metavar (`"NAME"`, `"PATH..."`); the manifest maps it onto the click parameter by lowercasing and stripping trailing `...`, so the metavar must match the parameter name.

```python
help_spec=HelpSpec(
    summary="Invoke a single MCP tool and print its result.",
    description="...",
    arguments=(HelpEntry("NAME", "Tool name. Run 'dhcli tool list' to discover names."),),
    output=_OUTPUT_CALL,              # see Output is single-source
    examples=("$ dhcli tool call sessions_list", "$ dhcli -o json tool call ... | jq ."),
    see_also=("dhcli tool list", "dhcli tool show NAME"),
    exit_codes=(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR),
    error_codes=(ErrorCode.ARG_PARSE_ERROR, ErrorCode.TOOL_RETURNED_ERROR),
)
```

Canonical implementation: `cli/_help.py` (`HelpSpec`, `build_help`, `HelpEntry`, `COMMON_ENV_VARS`), `cli/_errors.py` (`ErrorCode`, `ExitCode`).

## 4. Output is single-source

Define each leaf command's output shape once as an `OutputSpec` constant and reference it once, as the spec's `output=` field: `build_help` renders it into the human-readable **Output** section, and the manifest emits it as the node's `output` key (sparse: `mode` always, `fields` / `note` when present). One definition, two surfaces — no drift.

- `OutputSpec(mode, fields, note)` and `OutputField(name, type, help)` live in `cli/_help.py`.
- Leaf commands are `HelpfulCommand` instances (carried by `HelpfulGroup.command_class`); `output_spec` derives from `help_spec.output` — never pass it separately.
- A command that prints free-form or human-only output uses `mode="text"` with an empty `fields` tuple.
- **Structured output names its fields.** `mode="object"` must declare `fields`, and `mode="list"` must declare `fields` or a `note` describing the element shape — otherwise an agent has to run the command to learn what it returns, which is the whole failure the manifest exists to prevent. Enforced by `test_structured_output_names_its_fields`; reach for `mode="text"` only when the output genuinely has no shape, never to dodge the requirement.
- **A field whose value can mislead says so in its own `help`.** A truncating row cap, a partial-result flag, a best-effort batch — state it on the field or in the spec's `note`, not only in the description. Reuse `TABULAR_OUTPUT_FIELDS` / `TABULAR_OUTPUT_NOTE` (`cli/_commands/_wrapping.py`) for a row-data verb rather than re-authoring the `is_complete` / truncation wording; one definition keeps the four tabular verbs from drifting apart.

Canonical implementation: `cli/_help.py` (`OutputSpec`), `cli/_command.py` (`HelpfulGroup`), `cli/_manifest.py` (`build_summary_tree`, `build_manifest`, `describe_command`).

## 5. What the agents manifest carries

The manifest is an agent's context budget, not a dump of everything known. Each standing decision below is pinned by a test:

- **Error-code meanings are hoisted, permanently.** A node names the codes it can emit; `dhcli agents errors` holds the meanings. Inlining them copied the whole registry onto every node that can fail, to restate what one fetch serves for the entire tree and what a failure already conveys — the error payload carries a raise-site message beside its `error_code`. Pinned by `test_error_codes_are_bare_in_every_node_style`; the codes a node names must all resolve in the registry (`test_every_code_a_node_names_is_decodable_from_the_registry`).
- **The `conventions` block is for tree-wide rules only, and every claim in it must be true.** `AGENT_CONVENTIONS` (`cli/_manifest.py`) rides on the summary tree — the first surface an agent reads — so a rule earns a place there only if it holds for *every* command **and** cannot be read off a single node. A per-command hazard (a truncating cap, one prompt's non-interactive behavior) belongs on the command that has it, where it can be stated without hedging; hedged into tree-wide form it becomes the kind of text agents skip. The failure this block has actually suffered is a *false* tree-wide claim, so two tests re-derive its claims from the code: every command path it names must resolve (`test_agent_conventions_name_only_commands_that_exist`) and every closed set it enumerates — exit codes, output modes, error-payload keys — must match the enum (`test_agent_conventions_state_the_real_failure_contract`). Adding an `ExitCode` or an output mode therefore fails until the block states it.
- **A key a node's position already implies is omitted from the embedded style.** `path` and `usage` appear on standalone nodes only; inside `tree --full` the nesting gives the path and `params` gives the argument order.

## 6. Plain text, not reStructuredText

Surfaced text is plain text: every `HelpSpec` string, every `click.option(help=...)`, and group docstrings used as help. No double-backtick literals, no `:func:` / `:class:` roles — they render as literal noise in the terminal and leak into the manifest. Use single quotes for inline literals where emphasis helps (`'config'`, `'tail -f'`).

Internal docstrings and comments that are *not* surfaced (helper functions, module docstrings) keep the repo's RST convention — markup is a property of the rendering target, not the file. Apply `pydocs-improve` to those; this skill governs only the surfaced strings.

## 7. No-rewrap marker

Click rewraps each help paragraph unless it begins with a backspace marker on its own line. `build_help` prefixes every pre-formatted section (Arguments, Output, Examples, Environment, Exit codes, Error codes) with `_NO_WRAP` so the column alignment and one-item-per-line layout survive. Never put `\b` in a summary or description (prose should rewrap). The manifest never sees the marker — it renders from the structured `HelpSpec`, not the rendered text (docstring-help fallbacks are scrubbed by `_split_help_text`).

## 8. Verify

- `dhcli <noun> <verb> --help` renders every required section as a distinct block.
- `dhcli <noun> <verb> --agents` (or `dhcli agents command <noun> <verb>`) shows `summary`, `params` (positional args carry the Arguments help), the error codes as bare strings, and `output`; no double-backtick literals and no `\b` anywhere. Both default to compact `json` like every command — use `-o json-pretty` (or pipe through `jq .`) to pretty-print.
- `uv run pytest tests/cli/test_help_contract.py tests/cli/test__help.py tests/cli/test__manifest.py tests/cli/_commands/test_agents.py` is green — the content-preservation test in `tests/cli/test__manifest.py` asserts every `HelpSpec` fact surfaces in the node, with one deliberate carve-out for error-code meanings (*HelpSpec fields*).
