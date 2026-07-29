---
name: cli-command-add
description: Add a new command to the dhcli CLI — invoke when adding or editing a verb under cli/_commands/. Wraps the click + Pattern B + structured-error + agents-manifest conventions; prevents the most common bugs (calling asyncio.run inline, printing errors to stderr, forgetting to update docs/CLI.md)
---

Apply the `ref-python-coding-practices` skill (rule 15 covers click + `@run_async` + `CliError` discipline), the `ref-documentation-roles` skill (it defines `docs/CLI.md`'s role as the exhaustive CLI reference), the `ref-cli-help-standards` skill (the help-content contract every command's `HelpSpec` — rendering both `--help` and the agents manifest — must satisfy), and the `ref-output-serialization-conventions` skill for the command's output payload (every `OutputField`'s value vocabulary, casing, and known carve-outs). **If the command wraps an MCP tool** (any verb under `session`/`system`/`table`/`catalog`/`pq`/`docs`), also apply the `ref-cli-tool-wrapping` skill — it owns the shared `_wrapping` helpers, the five wrapper categories, the type-scoping rule, the path-flag locality rule, and the `wraps_tool` drift contract (step 2's wrapper note has the binding details).

## Steps

1. **Pick or create the noun group** under `src/deephaven_mcp/cli/_commands/`.
   - **The nouns**, with the routing gloss you need to place a verb (run `dhcli agents tree` for the live tree — it emits summaries, not "which noun should host this"): `daemon` (lifecycle of the local daemon), `tool` (inspect / invoke MCP tools — the raw escape hatch), `session` / `system` / `table` / `catalog` / `pq` (runtime MCP-tool wrappers — apply `ref-cli-tool-wrapping`), `docs` (documentation Q&A — connects directly to the docs MCP server, no daemon), `config` (inspect, validate, **and author** the configuration tree — `show`/`validate`/`get`/`keys`/`files`/`set`/`unset`/`edit`/`init` plus `session`/`system` add/remove/list; the authoring and inspection verbs declare `needs_runtime=False` and operate on files directly, making `config` the principal recovery surface when the tree is broken or empty), `context` (the sticky default session/system/PQ id, persisted in `<runtime_dir>/context.json`), `self` (tool self-management, e.g. shell completion — verbs here declare `needs_runtime=False` when they must work without a config tree).
   - **Not an add target:** `agents` emits machine-readable metadata about the live click tree, not user actions; new commands appear there automatically.
   - **Default to an existing noun:** add a verb under an existing noun; create a new noun only for a genuinely new domain.
   - **Ground the naming in prior art:** apply `ref-cli-design-prior-art` when choosing a noun, verb, or flag name — follow the convention a comparable CLI already established, cite the tool and the specific behavior, and state the reason if you depart from it.
   - **File layout:** each top-level noun is exactly one module `_commands/<noun>.py`, holding its group, all its verbs, any sub-groups, and its private helpers — do not create per-verb files or noun packages.
   - **Module naming:** the file is named for its noun alone (`session.py`, `context.py`). `self_cmd.py` is the sole suffixed filename, because `self` shadows the conventional instance parameter — do not generalize that `_cmd` suffix to any other noun.
   - **If you add a noun**, update the `cli/` entry in `ref-project-reference` in the same changeset — it maps the CLI package and goes stale silently otherwise.

2. **Add the click command.** Copy the decorator stack from the model verb `tool_call` in `cli/_commands/tool.py`: `@<group>.command(name, help_spec=HelpSpec(...))` → click options/arguments → `@click.pass_obj` (or `@click.pass_context`) → `@run_async` → `async def`. Exception: a verb that performs no I/O (pure metadata / self-management — canonical implementations: the `agents` verbs in `agents.py`, `self completion` in `self_cmd.py`) stays a plain synchronous `def` with no `@run_async`; never wrap a synchronous body in `async def`. Compose the help as a `HelpSpec` from `cli/_help.py` per the `ref-cli-help-standards` *Help-content contract*, and define the output shape once as an `OutputSpec` constant passed as the spec's `output=` field (*Output is single-source*) — `output_spec` derives from it automatically.

   **If the command wraps an MCP tool**, apply `ref-cli-tool-wrapping` and:
   - Build the body from the `_wrapping` helpers (`acquire` → `call_tool` → `tool_payload` → `require_success`) instead of hand-rolling the daemon/MCP dance.
   - Declare the binding on the command (`wraps_tool=` / `wraps_tools=`, plus `intentionally_unsupported=` / `router_params=` as needed) so `tests/cli/test_tool_wrapper_drift.py` can verify it.
   - Name each flag/argument exactly as its tool parameter, then run `uv run --extra test pytest tests/cli/test_tool_wrapper_drift.py -q`.

3. **Body: do the work.** Use the `Runtime` on `ctx.obj` for config / runtime-dir / output mode / timeouts. For commands that talk to the daemon, use `acquire_daemon(runtime, ...)` from `cli/_commands/_acquire.py` (it remaps the daemon-lifecycle exceptions to `CliError`) and `McpClient.for_daemon(...)` from `cli/_mcp_client.py`.

   The runtime is fully validated by the time your body runs: read `runtime.config.cli` for output mode and timeouts, `runtime.config.server` / `.community` / `.enterprise` for the systems sections, and `runtime.daemon_dir` for the registry handle. There is no upgrade gate — `HelpfulCommand.invoke` materializes the `Runtime` (loading and validating the whole config tree) right before your body runs, so a malformed config has already produced a `config_invalid` exit before dispatch reaches your verb. Commands that must work without a valid config tree declare `needs_runtime=False` on the command: the `agents` verbs (pure metadata), and — critically — the `config` authoring/inspection verbs (`get`/`set`/`unset`/`keys`/`files`/`edit`/`init`, `session`/`system` add/remove/list). Those operate on files directly through `ConfigStore` and must never force a full-tree load, since forcing one would make it impossible to *fix* a broken tree with the CLI (the recovery surface would be gated by the very failure it exists to repair). See `docs/CLI.md` *Configuration loading* and `AGENTS.md` *CLI* for the rationale.

4. **Errors.** Raise `CliError(message, code=ErrorCode.X)` from `cli/_errors.py` (`CliError.__init__` takes only `message` and the keyword-only `code` — the exit code is carried by the `ErrorCode` member via `code.exit_code`, not passed in). If no existing `ErrorCode` fits, add a new enum value with a single-line docstring describing what triggers it.

5. **Output.** `click.echo(format_output(payload, output=runtime.config.cli.output.format))`. The `format_output` function (`cli/_format.py`) handles `human` / `json` / `json-pretty` / `yaml` consistently. Do not branch on the output mode in the command body. Note: `output.format` is a nested field — `CliConfig` groups domain-specific knobs under `output.*` / `daemon.*` / `request.*`. Canonical implementation: `config/schema/_cli.py` (`CliConfig`, `OutputConfig`).

6. **Tests.** Add cases to `tests/cli/_commands/test_<noun>.py` (one test file per source file under `_commands/`). Use `click.testing.CliRunner` with `load_runtime` patched to return a test `Runtime` — patch it in `deephaven_mcp.cli._runtime` (where `RuntimeSpec.resolve` looks it up), using `fake_load_runtime` from `tests/cli/_helpers.py`. Cover:
   - Happy path. For a tool-wrapping command, cover **one** structured mode and leave the `human`/`json`/`json-pretty`/`yaml` matrix to `format_output`'s own tests (`ref-cli-tool-wrapping` *Testing*). For a command with bespoke rendering logic, cover each mode that exercises a distinct code path.
   - Every error path — each one should produce a `CliError` with the expected `error_code` (parse the JSON output and assert).
   - Any new option / argument validation.

   Apply the `tests-improve` skill for the 100%-per-file coverage target.

7. **Update `docs/CLI.md`.** Add the new verb under the noun's section: synopsis, description, every flag, exit codes, error codes, output fields, at least one runnable example. The three surfaces must agree (`ref-cli-help-standards` *Three surfaces, two sources* consistency rule). If a new `ErrorCode` was introduced, add it to the `error_code` registry table in the same document. Docs have no automated check — this is the most commonly forgotten step.

8. **Sanity-check the agents manifest.** `dhcli agents tree` walks the live click tree, so a newly-registered command appears there automatically (and `agents tree --full` carries its full node). `tests/cli/test__manifest.py` confirms the noun is wired and its content-preservation test asserts every `HelpSpec` fact surfaces in the node. (The current test does not snapshot the full sub-tree; tightening it is a known TODO, tracked outside this skill.)

9. **Run checks.**

   ```bash
   uv run pytest tests/cli/ -q
   ./bin/precommit.sh
   uv run dhcli <noun> <verb> --help    # eyeball the rendered help
   uv run dhcli <noun> <verb> --agents    # machine-readable node (twin of --help)
   ```

## Anti-patterns

- **Introducing a new environment variable.** Out of bounds without an explicit request (`AGENTS.md` *Scope discipline*).
  - **A click `envvar=` binding counts.** It is the same user-visible decision as an `os.environ` read, not a convenience.
  - **`docs/ENV.md` is the canonical inventory.** The CLI reads nothing it does not list; a new variable means a new entry there in the same edit.
  - **Where the value should go instead**: a `cli.json` field (`config-field-add`) or a flag. Ephemeral per-invocation state belongs in neither — it belongs in the runtime dir. Canonical implementation: `ContextStore` in `cli/_context.py`, persisting to `<runtime_dir>/context.json`.
  - **One deliberate oddity**: `_env_output_mode` in `cli/_main.py` re-reads the output-mode variable click has already bound. That second read is intentional — the fallback error renderer runs after the click context is gone.
- **Hand-rolled output formatting.** Always go through `format_output(...)`; otherwise `-o yaml` and the structured-error renderer drift.
- **Mixing in MCP-server-tool conventions.** This is a CLI command, not an MCP tool. No `register_tools()`, no `Terminology Note`, no `Format Accuracy for AI Agents`. Apply `ref-mcp-module-organization` only to MCP server tools.
- **Redeclaring a root option on a subcommand.** Every option declared on the root callback is auto-lifted to the front of argv by `_lift_root_options` (in `cli/_main.py`), which reads them from `cli.params` — so `dhcli daemon status -o json` is rewritten to `dhcli -o json daemon status` before click parses it. For the live set, read the root callback's decorators in `cli/_main.py` or run `dhcli --help`. The flag already applies to every subcommand — duplicating it on a subcommand creates a parser collision and breaks the auto-lift. If you add a new root option, `_lift_root_options` picks it up automatically from `cli.params`; if it must *not* be lifted (eager / context-sensitive like `--help` or `--version`), add the explicit exclusion in `_liftable_options` and a regression test in `tests/cli/test__main.py`.
