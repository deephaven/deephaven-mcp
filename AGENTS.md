# Deephaven MCP — Agent Guidelines

## Testing

- Always use `uv run pytest` instead of `pytest` directly. This ensures the correct virtual environment and dependencies are used.
- Target 100% per-source-file coverage. Apply `tests-improve` to bring a file up to target.

## Timeouts

These operations are slow by nature, not hung — set generous timeouts:

- `uv pip install` / `uv sync` / any `uv` package operation: allow at least 120 seconds.
- `./bin/precommit.sh`: allow at least 60 seconds (~22 seconds typical).
- `uv run pytest` (full suite): allow at least 120 seconds.
- `uv run mypy src/`: allow at least 30 seconds (~15 seconds typical).

## Version Control

- When moving or renaming files, use `git mv` rather than delete + create. When removing files, use `git rm`. Preserving history matters.
- Never stage or commit. Do not run `git add`, `git commit`, `git reset`, or anything else that mutates the index or history unless the USER explicitly requests that exact operation. The USER reviews and stages every change personally; staging on their behalf destroys partial-review state (e.g., hunks staged with `git add -p`) irrecoverably. The `git mv` / `git rm` rule above is the sole carve-out, and only for the files the task itself moves or removes. When work is done, leave all changes unstaged and report.

## Scope discipline

- Adding a user-visible surface — environment variable, config field, CLI flag, error code, output field, MCP tool, or any other user-visible contract — requires an explicit request or an approved plan. Propose it and wait; never introduce one as a side effect of another task. Each is a contract that gets documented, tested, and is costly to withdraw once shipped.
- **Carve-out for entailed surfaces**: a surface the approved change *requires* is in scope. A requested command brings its flags, its error codes, and its output fields; a requested config field brings its key. What needs approval is a surface that stands on its own, not the parts the request already implies.
- Environment variables are the most-violated case: `docs/ENV.md` is the canonical inventory and every entry in it is deliberate. Apply `ref-configuration-conventions` before changing env-var behavior, plus `cli-command-add` for CLI code.

## Agent Skills

Skills live in `.agents/skills/`, surfaced to each agent through thin wiring shims (a `.claude/skills` symlink and a `CLAUDE.md` that imports `@AGENTS.md` for Claude Code). `ref-skill-authoring-standards` *Cross-agent portability* owns that cross-agent wiring contract.

The `ref-` prefix tracks human invocability: a `ref-`-prefixed skill is never invoked directly by a human (loaded only as context by other skills); an unprefixed skill is human-invocable, even if other skills also reference it. The prefix is a naming convention, not a mechanism — `user-invocable: false` is the mechanism, and only Claude Code honors it. The decision flowchart and worked examples live in `ref-skill-authoring-standards` *Naming and the prefix rule*.

For the standards on writing, reviewing, and curating skills:

- Apply `skill-add` (workflow) to add a new skill.
- Apply `skill-review` (workflow) to audit an existing skill or this `AGENTS.md`; also dispatched by `review-changes` on `.agents/skills/**` and `AGENTS.md` edits.
- Apply `ref-skill-authoring-standards` for structural rules (composition, frontmatter, the prefix decision flowchart, lifecycle, precedence).
- Apply `ref-skill-effectiveness` for content quality (triggerability, actionability, clarity, anti-patterns).
- Apply `ref-agents-md-curation` when editing this file.

## MCP Server Configuration

- **Servers**: Two server binaries — the multiplexed `dh-mcp-systems-server` (handles community + enterprise) and `dh-mcp-docs-server` (documentation Q&A). `ref-project-reference` owns the launch commands, transports, ports, and flags.
- **Dependencies**: Both Community Core (`deephaven-server`) and Enterprise/Core+ (`deephaven-coreplus-client`) are always-installed base dependencies — there are no `community`/`enterprise` pip extras and no install-time optionality. Enterprise modules are imported directly at module top level; never reintroduce `try/except ImportError` availability guards, an `is_enterprise_available()` probe, or sentinel-`None` enterprise symbols. (Whether a community/enterprise *section* is *configured* is a separate axis, handled by `CommunityNotConfiguredError`/`EnterpriseNotConfiguredError` and tools self-reporting applicability — that stays.)
- **HTTP transport is loopback-only, and that is not negotiable**: the server refuses to bind a non-loopback host, there is no TLS and no opt-out, and a PSK is mandatory (`X-Deephaven-PSK` header, matching `server.json`'s `psk`, which may use `${env:NAME}` templating). Never add an option that widens the bind or makes the PSK optional; remote access is a reverse proxy's job.
- **Configuration directory**: `dh-mcp-systems-server` reads a directory tree, not a single file. It lives under the user-data root as the `config/` subdirectory; `DH_AI_DATA_DIR` moves the root, `--config-dir` overrides only that subdir, and there is no per-subdir env var. Config changes require a server restart — there is no reload tool. See `config-samples/ai/config/` for the layout and `ref-project-reference` for the module map.
- **Removed pieces — do not reintroduce**: `dh-mcp-community-server`, `dh-mcp-enterprise-server`, `DH_MCP_CONFIG_FILE`, the `mcp_reload` tool, the auth backends, the `community`/`enterprise` pip extras, `is_enterprise_available()`, and `MissingEnterprisePackageError`. The `auth/` package holds only `PSKMiddleware` (`auth/middleware/_psk.py`) and the outbound credential / TLS dataclasses (`auth/credentials/`, `auth/tls/`).

## CLI

- **Binary**: `dhcli` — the Deephaven command-line tool for humans and especially AI agents; its runtime commands are currently backed by the MCP systems server (the mechanism, not the scope). Distinct from the server binaries `dh-mcp-systems-server` and `dh-mcp-docs-server`.
- **Framework**: `click` (>=8.4); command callbacks that perform I/O are async (Pattern B async-to-sync adapter); pure-metadata verbs (the `agents` verbs, `self completion`) are plain synchronous functions. Apply `ref-python-coding-practices` rule 15 (`@run_async` from `cli/_async.py`, no `argparse`, `CliError` discipline) and the `cli-command-add` skill before adding or editing a command.
- **Structure**: noun-verb groups under `src/deephaven_mcp/cli/_commands/`, one module per noun, plus a universal `--agents` flag on every command — the machine-readable twin of `--help`. Run `dhcli agents tree` for the live noun-verb tree with summaries rather than trusting a list here; `cli-command-add` owns the per-noun routing guidance and the file-layout rules.
- **Wrapping MCP tools**: runtime nouns wrap MCP tools; each binds its tool(s) via `wraps_tool`/`wraps_tools` on `HelpfulCommand` so `tests/cli/test_tool_wrapper_drift.py` catches schema drift, and type (community/enterprise) is never a command subgroup. Most wrappers acquire the local daemon; `docs ask` is a direct-URL wrapper (category 5 in `ref-cli-tool-wrapping`, which owns the details). Apply `ref-cli-tool-wrapping` before adding or editing a wrapper; rationale in `docs/design/CLI_TOOL_WRAPPING.md`.
- **Output modes**: `-o human|json|json-pretty|yaml` (default `json` — compact single-line JSON on every command; the CLI is machine-first; envvar `DHCLI_OUTPUT`; humans opt into `-o human` or `-o json-pretty`, or set `cli.json` `output.format`). Errors are structured (`{error, error_code, exit_code, command}`) in the structured modes; stable `error_code` strings live in `cli/_errors.py`.
- **Exit codes**: `0` success, `2` user-facing failure, `3` tool returned `isError=True`.
- **Leaf-boundary configuration loading**: every runtime-dependent leaf invocation parses and validates the entire configuration directory once, just before the command's body runs, so any malformed file fails fast with `config_invalid` (exit 2) before any subcommand body executes. `--help` and `--agents` (at any depth) exit during argument parsing, before the load, and the `dhcli agents` verbs never touch configuration — there is no argv pre-scanning. Subcommand bodies receive a fully-validated `Runtime` and read `runtime.config.{cli,server,community,enterprise}` directly — no upgrade gate, no overlay. `ref-project-reference` owns the load mechanism; `docs/CLI.md` *Configuration loading* has the recovery story.
- **Sticky context**: a verb whose session/system/PQ id is omitted falls back to the persisted default in `<runtime_dir>/context.json` (`cli/_context.py`), so an omitted id does *not* mean "no target" — it means "whatever the context holds", and an unresolvable one exits 2 with `context_not_set`. `--no-context` disables the *read*; `--no-set-context` on `session create` / `pq create` disables the *write*; `cli.json`'s `context.enabled` / `context.confirm_destructive` are the persistent knobs. Run `dhcli context show` before any consequential verb you intend to invoke without an explicit id. `docs/CLI.md` *`dhcli context`* is the contract; `ref-cli-tool-wrapping` owns the wiring rules for a defaultable target. An *explicit* id is a separate question — `docs/CLI.md` *Choosing a target* owns which ids are legitimate to act on, and a listing does not confer permission.
- **Blank parameter values are rejected CLI-wide**, by two guards that must both stay: `HelpfulCommand.invoke` rejects any empty or whitespace-only parameter — including each element of a repeatable or variadic one — with `missing_argument` (exit 2) before the body runs, because no parameter in the tree has a legitimate blank value and `--system ''` would otherwise silently fall back to the sticky context. Path-valued options additionally need `NonBlankPath` (`cli/_params.py`) instead of `click.Path`, which rejects the blank *during* parsing: `click.Path` converts `''` to `Path('.')`, so a post-parse check cannot tell `--config-dir ''` from an explicit `--config-dir .`, and a blank would silently retarget the whole config tree or the daemon registry. `click.Path` is the only click type with that flaw (every other rejects a blank outright; a bare `STRING` stays blank for the leaf guard), and `tests/cli/test_help_contract.py::test_path_options_reject_a_blank_value` enforces it. A `KEY=VALUE` option is unaffected (`--env 'DEBUG='` sets an empty env var; the parameter itself is non-blank). Never add a per-command opt-out.
- **Adding a command**: apply the `cli-command-add` skill. Never `print(..., file=sys.stderr); return 2` — raise `CliError(msg, code=ErrorCode.X)`.
- **Self-discovery for agents**: `dhcli agents tree` emits a compact summary of the command tree (`--full` for the complete manifest with params, error codes, and output schemas); append `--agents` to any command for its full node (the machine-readable twin of `--help`). A node names the error codes it can emit but not their meanings — `dhcli agents errors` is the decoder, fetched once. All surfaces default to `json` like the rest of the CLI (compact; `-o json-pretty` for indented). The agents surfaces honor `-o`/`DHCLI_OUTPUT` only — they bypass the runtime config load and do not read `cli.json`'s `output.format`. Prefer these over scraping `--help`.
- **Help content**: the root and every leaf command are described once as a `HelpSpec` (`cli/_help.py`), which renders both the `--help` text and the agents-manifest node; noun groups are described by their function docstrings, from which the manifest derives summary/description (`_split_help_text`). `docs/CLI.md` is authored separately and must be reconciled by hand. Surfaced help is plain text (no RST markup); the output schema is single-sourced as an `OutputSpec` on the spec. The contract is `ref-cli-help-standards`; apply `cli-help-improve` / `cli-help-accuracy` to author or verify help.

## Python version

Apply `ref-python-coding-practices` rule 16 before using version-gated syntax (PEP 695 generics, `tomllib`, `typing.override`, `StrEnum`, etc.). The floor is authoritative in `pyproject.toml` (`requires-python`) — never duplicate the version number elsewhere.

## Python Code

Before writing or editing Python source under `src/` or `tests/` — including docstring-only edits — load `ref-python-coding-practices` and apply its rules. The most-violated are rule 12 (docstrings state *what*, not *why* — no design rationale, API-symmetry justifications, or cross-call-site narratives), rule 14 (per-field trailing docstrings on value classes, never a class-level `Attributes:` / `Members:` block), and rule 18 (closed-set dispatch is `match` + `assert_never` — never an `if`/`elif` with a silent default branch).

## Output conventions

Apply `ref-output-serialization-conventions` when authoring or changing any user-facing string field or payload shape in an MCP tool return or CLI output.

## Build & Distribution

- **Wheels**: built and published to PyPI by CI on `v*` tag pushes (`.github/workflows/publish-wheels.yml`); the dependency list is authoritative in `pyproject.toml`.
- **Standalone binaries (PyApp)**: self-contained, offline, no-Python executables (`dh-mcp-systems-server`, `dhcli`) built per-platform via `uv run scripts/build_pyapp.py` and released by `.github/workflows/build-pyapp.yml`. See `docs/STANDALONE_BINARIES.md` for the full procedure.
