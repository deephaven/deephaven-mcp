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

## Agent Skills

Skills live in `.agents/skills/`, surfaced to each agent through thin wiring shims (a `.claude/skills` symlink and a `CLAUDE.md` that imports `@AGENTS.md` for Claude Code). `_skill-authoring-standards` §14 owns that cross-agent wiring contract.

The `_` prefix tracks human invocability: a `_`-prefixed skill is never invoked directly by a human (loaded only as context by other skills); an unprefixed skill is human-invocable via slash command, even if other skills also reference it. The decision flowchart, the cross-agent `user-invocable` frontmatter rule, and worked examples live in `_skill-authoring-standards` §4.

For the standards on writing, reviewing, and curating skills:

- Apply `skill-add` (workflow) to add a new skill.
- Apply `skill-review` (workflow) to audit an existing skill or this `AGENTS.md`; also dispatched by `review-changes` on `.agents/skills/**` and `AGENTS.md` edits.
- Apply `_skill-authoring-standards` for structural rules (composition, frontmatter, `_`-prefix decision flowchart, lifecycle, precedence).
- Apply `_skill-effectiveness` for content quality (triggerability, actionability, clarity, anti-patterns).
- Apply `_agents-md-curation` when editing this file.

## MCP Server Configuration

- **Servers**: Two server binaries — the multiplexed `dh-mcp-systems-server` (handles community + enterprise) and `dh-mcp-docs-server` (documentation Q&A).
- **Dependencies**: Both Community Core (`deephaven-server`) and Enterprise/Core+ (`deephaven-coreplus-client`) are always-installed base dependencies — there are no `community`/`enterprise` pip extras and no install-time optionality. Enterprise modules are imported directly at module top level; never reintroduce `try/except ImportError` availability guards, an `is_enterprise_available()` probe, or sentinel-`None` enterprise symbols. (Whether a community/enterprise *section* is *configured* is a separate axis, handled by `CommunityNotConfiguredError`/`EnterpriseNotConfiguredError` and tools self-reporting applicability — that stays.)
- **Transports**: `dh-mcp-systems-server` supports `stdio` (default) and `http` (streamable-HTTP), selectable via `--transport`. `dh-mcp-docs-server` supports only streamable-HTTP.
- **HTTP transport**: Loopback only (`127.0.0.1` / `::1` / `localhost`); no TLS; PSK is required and shared via the `X-Deephaven-PSK` header (matches `server.json`'s `psk` field, which may use `${env:NAME}` templating).
- **Configuration directory**: `dh-mcp-systems-server` reads a directory tree (not a single file). Lives under the user-data root (`~/.deephaven/ai/` on POSIX; `%APPDATA%/Deephaven/ai/` on Windows) as the `config/` subdirectory. Override the root with `DH_AI_DATA_DIR`, or override only the config subdir with the `--config-dir` CLI arg. There is no per-subdir env var. See `config-samples/ai/config/` for the layout.
- **Removed pieces**: `dh-mcp-community-server`, `dh-mcp-enterprise-server`, `DH_MCP_CONFIG_FILE`, the `mcp_reload` tool, the auth backends, the `community`/`enterprise` pip extras, `is_enterprise_available()`, and `MissingEnterprisePackageError`. The `auth/` package now contains only `PSKMiddleware` (`auth/middleware/_psk.py`, mounted by the HTTP transport) and the outbound credential / TLS dataclasses (`auth/credentials/`, `auth/tls/`). Restart the server to pick up config changes.

## CLI

- **Binary**: `dhcli` — the Deephaven command-line tool for humans and especially AI agents; its runtime commands are currently backed by the MCP systems server (the mechanism, not the scope). Distinct from the server binaries `dh-mcp-systems-server` and `dh-mcp-docs-server`.
- **Framework**: `click` (>=8.4); command callbacks are async (Pattern B async-to-sync adapter). Apply `_python-coding-practices` rule 15 (`@run_async` from `cli/_async.py`, no `argparse`, `CliError` discipline) and the `cli-command-add` skill before adding or editing a command.
- **Structure**: noun-verb groups under `src/deephaven_mcp/cli/_commands/`. Top-level nouns: `daemon` (process lifecycle), `tool` (raw MCP escape hatch), `session`/`system`/`table`/`catalog`/`pq` (runtime MCP-tool wrappers), `docs` (documentation Q&A — connects directly to the docs MCP server at `cli.json`'s `docs.url`; the daemon is not involved), `config` (static config), `agents` (machine-readable metadata: `tree`/`command`/`errors` verbs), and `self` (tool self-management — module `self_cmd.py`; its `completion` verb prints the click-generated shell tab-completion script for bash/zsh/fish as raw text, `needs_runtime=False`, exercised per-shell by `scripts/test_shell_completion.sh` via `.github/workflows/cli-completion.yml`). Plus a universal `--agents` flag on every command — the machine-readable twin of `--help`.
- **Wrapping MCP tools**: runtime nouns wrap MCP tools; each binds its tool(s) via `wraps_tool`/`wraps_tools` on `HelpfulCommand` so `tests/cli/test_tool_wrapper_drift.py` catches schema drift, and type (community/enterprise) is never a command subgroup. Most wrappers acquire the local daemon; `docs ask` is a direct-URL wrapper (category 5 in `_cli-tool-wrapping`, which owns the details). Apply `_cli-tool-wrapping` before adding or editing a wrapper; rationale in `docs/design/CLI_TOOL_WRAPPING.md`.
- **Output modes**: `-o human|json|json-pretty|yaml` (default `json` — compact single-line JSON on every command; the CLI is machine-first; envvar `DHCLI_OUTPUT`; humans opt into `-o human` or `-o json-pretty`, or set `cli.json` `output.format`). Errors are structured (`{error, error_code, exit_code, command}`) in the structured modes; stable `error_code` strings live in `cli/_errors.py`.
- **Exit codes**: `0` success, `2` user-facing failure, `3` tool returned `isError=True`.
- **Leaf-boundary configuration loading**: every runtime-dependent leaf invocation parses and validates the entire configuration directory once, just before the command's body runs. The root callback stores a cheap `RuntimeSpec` (the load recipe) on `ctx.obj`; `HelpfulCommand.invoke` swaps it for a fully-loaded `Runtime` (`RuntimeSpec.resolve` → `load_runtime`). Any malformed file fails fast with `config_invalid` (exit 2) before any subcommand body runs. `--help` and `--agents` (at any depth) exit during argument parsing — before the load — and the `dhcli agents` verbs are declared `needs_runtime=False`; none of them touch configuration, with no argv pre-scanning. Subcommand bodies receive a fully-validated `Runtime` via `click.pass_obj` and read `runtime.config.{cli,server,community,enterprise}` directly — no upgrade gate, no overlay. CLI flag overrides (`-o`, `--timeout`, `--no-auto-start`) are applied to `runtime.config.cli` inside `load_runtime`. See `docs/CLI.md` *Configuration loading* for the recovery story.
- **Adding a command**: apply the `cli-command-add` skill. Never `print(..., file=sys.stderr); return 2` — raise `CliError(msg, code=ErrorCode.X)`.
- **Self-discovery for agents**: `dhcli agents tree` emits a compact summary of the command tree (`--full` for the complete manifest with params, error codes, and output schemas); append `--agents` to any command for its self-contained node (the machine-readable twin of `--help`). All surfaces default to `json` like the rest of the CLI (compact; `-o json-pretty` for indented). The agents surfaces honor `-o`/`DHCLI_OUTPUT` only — they bypass the runtime config load and do not read `cli.json`'s `output.format`. Prefer these over scraping `--help`.
- **Help content**: the root and every leaf command are described once as a `HelpSpec` (`cli/_help.py`), which renders both the `--help` text and the agents-manifest node; noun groups are described by their function docstrings, from which the manifest derives summary/description (`_split_help_text`). `docs/CLI.md` is authored separately and must be reconciled by hand. Surfaced help is plain text (no RST markup); the output schema is single-sourced as an `OutputSpec` on the spec. The contract is `_cli-help-standards`; apply `cli-help-improve` / `cli-help-accuracy` to author or verify help.

## Python version

Apply `_python-coding-practices` rule 16 before using version-gated syntax (PEP 695 generics, `tomllib`, `typing.override`, `StrEnum`, etc.). The floor is authoritative in `pyproject.toml` (`requires-python`) — never duplicate the version number elsewhere.

## Python Code

Before writing or editing Python source under `src/` or `tests/` — including docstring-only edits — load `_python-coding-practices` and apply its rules. The most-violated are rule 12 (docstrings state *what*, not *why*), rule 14 (per-field trailing docstrings on value classes, never a class-level `Attributes:` / `Members:` block), and rule 18 (closed-set dispatch is `match` + `assert_never` — never an `if`/`elif` with a silent default branch).

## Output conventions

Apply `_output-serialization-conventions` when authoring or changing any user-facing string field or payload shape in an MCP tool return or CLI output. The skill owns the value-vocabulary rules, casing rules, known carve-outs, field-specific exceptions, and the MCP-layer payload-shape rules (domain-named arrays, sparse optional keys, echo-back key vocabulary, bounded output, truncation semantics, among others).

## Build & Distribution

- **Wheels**: built and published to PyPI by CI on `v*` tag pushes (`.github/workflows/publish-wheels.yml`); the dependency list is authoritative in `pyproject.toml`.
- **Standalone binaries (PyApp)**: self-contained, offline, no-Python executables (`dh-mcp-systems-server`, `dhcli`) built per-platform via `uv run scripts/build_pyapp.py` and released by `.github/workflows/build-pyapp.yml`. See `docs/STANDALONE_BINARIES.md` for the full procedure.
