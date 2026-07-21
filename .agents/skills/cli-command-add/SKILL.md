---
name: cli-command-add
description: Add a new command to the dh-mcp CLI — wraps the click + Pattern B + structured-error + agents-manifest conventions; prevents the most common bugs (calling asyncio.run inline, printing errors to stderr, forgetting to update docs/CLI.md)
---

Apply the `_python-coding-practices` skill (rule 15 covers click + `@run_async` + `CliError` discipline), the `_documentation-roles` skill (it defines `docs/CLI.md`'s role as the exhaustive CLI reference), the `_cli-help-standards` skill (the help-content contract every command's `HelpSpec` — rendering both `--help` and the agents manifest — must satisfy), and the `_output-serialization-conventions` skill for the command's output payload (every `OutputField`'s value vocabulary, casing, and known carve-outs). **If the command wraps an MCP tool** (any verb under `session`/`system`/`table`/`catalog`/`pq`/`docs`), also apply the `_cli-tool-wrapping` skill — it owns the shared `_wrapping` helpers, the five wrapper categories, the type-scoping rule, the path-flag locality rule, and the `wraps_tool` drift contract (step 2's wrapper note has the binding details).

## Steps

1. **Pick the noun group.** Look in `src/deephaven_mcp/cli/_commands/` for an existing noun that fits: `daemon` (lifecycle of the local daemon), `tool` (inspect / invoke MCP tools — the raw escape hatch), `session` / `system` / `table` / `catalog` / `pq` (runtime MCP-tool wrappers — apply `_cli-tool-wrapping`), `docs` (documentation Q&A — connects directly to the docs MCP server, no daemon), `config` (inspect / validate the configuration tree). `agents` is not an add target — it emits machine-readable metadata about the live click tree, not user actions, and new commands appear there automatically. Only create a new noun if the command represents a genuinely new domain — adding a verb under an existing noun is the default.

2. **Add the click command.** Copy the decorator stack from the model verb `tool_call` in `cli/_commands/tool.py`: `@<group>.command(name, help_spec=HelpSpec(...))` → click options/arguments → `@click.pass_obj` (or `@click.pass_context`) → `@run_async` → `async def`. Compose the help as a `HelpSpec` from `cli/_help.py` per the `_cli-help-standards` §2 section contract, and define the output shape once as an `OutputSpec` constant passed as the spec's `output=` field (§4) — `output_spec` derives from it automatically.

   **If the command wraps an MCP tool**, apply `_cli-tool-wrapping` and:
   - Build the body from the `_wrapping` helpers (`acquire` → `call_tool` → `tool_payload` → `require_success`) instead of hand-rolling the daemon/MCP dance.
   - Declare the binding on the command (`wraps_tool=` / `wraps_tools=`, plus `intentionally_unsupported=` / `router_params=` as needed) so `tests/cli/test_tool_wrapper_drift.py` can verify it.
   - Name each flag/argument exactly as its tool parameter, then run `uv run --extra test pytest tests/cli/test_tool_wrapper_drift.py -q`.

3. **Body: do the work.** Use the `Runtime` on `ctx.obj` for config / runtime-dir / output mode / timeouts. For commands that talk to the daemon, use `acquire_daemon(runtime, ...)` from `cli/_commands/_acquire.py` (it remaps the daemon-lifecycle exceptions to `CliError`) and `McpClient.for_daemon(...)` from `cli/_mcp_client.py`.

   The runtime is fully validated by the time your body runs: read `runtime.config.cli` for output mode and timeouts, `runtime.config.server` / `.community` / `.enterprise` for the systems sections, and `runtime.daemon_dir` for the registry handle. There is no upgrade gate — `HelpfulCommand.invoke` materializes the `Runtime` (loading and validating the whole config tree) right before your body runs, so a malformed config has already produced a `config_invalid` exit before dispatch reaches your verb. Commands that must work without a valid config tree (the `agents` verbs) declare `needs_runtime=False` on the command. See `docs/CLI.md` *Configuration loading* and `AGENTS.md` *CLI* for the rationale.

4. **Errors.** Raise `CliError(message, code=ErrorCode.X)` from `cli/_errors.py` (`CliError.__init__` takes only `message` and the keyword-only `code` — the exit code is carried by the `ErrorCode` member via `code.exit_code`, not passed in). If no existing `ErrorCode` fits, add a new enum value with a single-line docstring describing what triggers it.

5. **Output.** `click.echo(format_output(payload, output=runtime.config.cli.output.format))`. The `format_output` function (`cli/_format.py`) handles `human` / `json` / `json-pretty` / `yaml` consistently. Do not branch on the output mode in the command body. Note: `output.format` is a nested field — `CliConfig` groups domain-specific knobs under `output.*` / `daemon.*` / `request.*`. Canonical implementation: `config/schema/_cli.py` (`CliConfig`, `OutputConfig`).

6. **Tests.** Add cases to `tests/cli/_commands/test_<noun>.py` (one test file per source file under `_commands/`). Use `click.testing.CliRunner` with `load_runtime` patched to return a test `Runtime` — patch it in `deephaven_mcp.cli._runtime` (where `RuntimeSpec.resolve` looks it up), using `fake_load_runtime` from `tests/cli/_helpers.py`. Cover:
   - Happy path. For a tool-wrapping command, cover **one** structured mode and leave the `human`/`json`/`json-pretty`/`yaml` matrix to `format_output`'s own tests (`_cli-tool-wrapping` *Testing*). For a command with bespoke rendering logic, cover each mode that exercises a distinct code path.
   - Every error path — each one should produce a `CliError` with the expected `error_code` (parse the JSON output and assert).
   - Any new option / argument validation.

   Apply the `tests-improve` skill for the 100%-per-file coverage target.

7. **Update `docs/CLI.md`.** Add the new verb under the noun's section: synopsis, description, every flag, exit codes, error codes, output fields, at least one runnable example. The three surfaces must agree (`_cli-help-standards` §1 consistency rule). If a new `ErrorCode` was introduced, add it to the `error_code` registry table in the same document. Docs have no automated check — this is the most commonly forgotten step.

8. **Sanity-check the agents manifest.** `dh-mcp agents tree` walks the live click tree, so a newly-registered command appears there automatically (and `agents tree --full` carries its full node). `tests/cli/test__help.py` confirms the noun is wired and its content-preservation test asserts every `HelpSpec` fact surfaces in the node. (The current test does not snapshot the full sub-tree; tightening it is a known TODO, tracked outside this skill.)

9. **Run checks.**

   ```bash
   uv run pytest tests/cli/ -q
   ./bin/precommit.sh
   uv run dh-mcp <noun> <verb> --help    # eyeball the rendered help
   uv run dh-mcp <noun> <verb> --agents    # machine-readable node (twin of --help)
   ```

## Anti-patterns

- **Reading `os.environ` directly.** Add `envvar=` to the click option. The only env var the CLI reads outside of click is `DH_MCP_DATA_DIR`, and `_runtime.py` already handles it.
- **Hand-rolled output formatting.** Always go through `format_output(...)`; otherwise `-o yaml` and the structured-error renderer drift.
- **Mixing in MCP-server-tool conventions.** This is a CLI command, not an MCP tool. No `register_tools()`, no `Terminology Note`, no `Format Accuracy for AI Agents`. Apply `_mcp-module-organization` only to MCP server tools.
- **Redeclaring a root option on a subcommand.** The root callback's options (`-o/--output`, `--timeout`, `-v/--verbose`, `-q/--quiet`, `--no-auto-start`, `--config-dir`, `--runtime-dir`) are auto-lifted to the front of argv by `_lift_root_options` (in `cli/_main.py`), so `dh-mcp daemon status -o json` is rewritten to `dh-mcp -o json daemon status` before click parses it. The flag already applies to every subcommand — duplicating it on a subcommand creates a parser collision and breaks the auto-lift. If you add a new root option, `_lift_root_options` picks it up automatically from `cli.params`; if it must *not* be lifted (eager / context-sensitive like `--help` or `--version`), add the explicit exclusion in `_liftable_options` and a regression test in `tests/cli/test__main.py`.
