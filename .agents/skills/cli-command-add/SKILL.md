---
name: cli-command-add
description: Add a new command to the dh-mcp CLI — wraps the click + Pattern B + structured-error + introspect conventions; prevents the most common bugs (calling asyncio.run inline, printing errors to stderr, forgetting to update docs/CLI.md)
---

Apply the `_python-coding-practices` skill (rule 15 covers click + `@run_async` + `CliError` discipline), the `_documentation-roles` skill (`docs/CLI.md` is the single source of truth for the CLI surface), and the `_cli-help-standards` skill (the help-content contract every command's `--help` and the introspect manifest must satisfy).

## Steps

1. **Pick the noun group.** Look in `src/deephaven_mcp/cli/_commands/` for an existing noun that fits: `daemon` (lifecycle of the local daemon), `tool` (inspect / invoke MCP tools), `config` (inspect / validate the configuration tree). Only create a new noun if the command represents a genuinely new domain — adding a verb under an existing noun is the default.

2. **Add the click command.** Copy the decorator stack from the model verb `tool_call` in `cli/_commands/tool.py`: `@<group>.command(name, output_spec=SPEC, help=build_help(...))` → click options/arguments → `@click.pass_obj` (or `@click.pass_context`) → `@run_async` → `async def`. Compose the help with `build_help(...)` from `cli/_help.py` per the `_cli-help-standards` §2 section contract, and define the output shape once as an `OutputSpec` constant passed to both `output_spec=` and `build_help(output=...)` (§4).

3. **Body: do the work.** Use the `Runtime` on `ctx.obj` for config / runtime-dir / output mode / timeouts. For commands that talk to the daemon, use `get_or_start_daemon(runtime)` and `McpClient` from `cli/_daemon.py` and `cli/_mcp_client.py`.

   The runtime is fully validated by the time your body runs: read `runtime.config.cli` for output mode and timeouts, `runtime.config.server` / `.community` / `.enterprise` for the systems sections, and `runtime.daemon_dir` for the registry handle. There is no upgrade gate — eager validation in `_main`'s root callback means a malformed config has already produced a `config_invalid` exit before dispatch reaches your verb. See `docs/CLI.md` *Configuration loading* and `AGENTS.md` *CLI* for the rationale.

4. **Errors.** Raise `CliError(message, code=ErrorCode.X)` from `cli/_errors.py` (`CliError.__init__` takes only `message` and the keyword-only `code` — the exit code is carried by the `ErrorCode` member via `code.exit_code`, not passed in). If no existing `ErrorCode` fits, add a new enum value with a single-line docstring describing what triggers it.

5. **Output.** `click.echo(format_output(payload, output=runtime.config.cli.output.format))`. The `format_output` function (`cli/_format.py`) handles `human` / `json` / `yaml` consistently. Do not branch on the output mode in the command body. Note: `output.format` is a nested field — `CliConfig` groups domain-specific knobs under `output.*` / `daemon.*` / `request.*`. Canonical implementation: `config/schema/_cli.py` (`CliConfig`, `OutputConfig`).

6. **Tests.** Add cases to `tests/cli/_commands/test_<noun>.py` (one test file per source file under `_commands/`). Use `click.testing.CliRunner` with `load_runtime` patched to return a test `Runtime`. Cover:
   - Happy path in each output mode (`human`, `json`, `yaml`).
   - Every error path — each one should produce a `CliError` with the expected `error_code` (parse the JSON output and assert).
   - Any new option / argument validation.

   Apply the `tests-improve` skill for the 100%-per-file coverage target.

7. **Update `docs/CLI.md`.** Add the new verb under the noun's section: synopsis, description, every flag, exit codes, error codes, output fields, at least one runnable example. The three surfaces must agree (`_cli-help-standards` §1 consistency rule). If a new `ErrorCode` was introduced, add it to the `error_code` registry table in the same document. Docs have no automated check — this is the most commonly forgotten step.

8. **Sanity-check the introspect manifest.** `dh-mcp introspect` walks the live click tree, so a newly-registered command appears there automatically. `tests/cli/_commands/test_introspect.py` confirms the noun is wired and reports its top-level metadata. (The current test does not snapshot the full sub-tree; tightening it is a known TODO, tracked outside this skill.)

9. **Run checks.**

   ```bash
   uv run pytest tests/cli/ -q
   ./bin/precommit.sh
   uv run dh-mcp <noun> <verb> --help    # eyeball the rendered help
   uv run dh-mcp introspect | jq '.commands.<noun>.subcommands.<verb>'
   ```

## Anti-patterns

- **Reading `os.environ` directly.** Add `envvar=` to the click option. The only env var the CLI reads outside of click is `DH_MCP_DATA_DIR`, and `_runtime.py` already handles it.
- **Hand-rolled output formatting.** Always go through `format_output(...)`; otherwise `-o yaml` and the structured-error renderer drift.
- **Mixing in MCP-server-tool conventions.** This is a CLI command, not an MCP tool. No `register_tools()`, no `Terminology Note`, no `Format Accuracy for AI Agents`. Apply `_mcp-module-organization` only to MCP server tools.
