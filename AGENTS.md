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
- **Transports**: `dh-mcp-systems-server` supports `stdio` (default) and `http` (streamable-HTTP), selectable via `--transport`. `dh-mcp-docs-server` supports only streamable-HTTP.
- **HTTP transport**: Loopback only (`127.0.0.1` / `::1` / `localhost`); no TLS; PSK is required and shared via the `X-Deephaven-PSK` header (matches `server.json`'s `psk` field, which may use `${env:NAME}` templating).
- **Configuration directory**: `dh-mcp-systems-server` reads a directory tree (not a single file). Lives under the user-data root (`~/.deephaven/ai/` on POSIX; `%APPDATA%/Deephaven/ai/` on Windows) as the `config/` subdirectory. Override the root with `DH_MCP_DATA_DIR`, or override just the config subdir with the `--config-dir` CLI arg. There is no per-subdir env var. See `config-samples/ai/config/` for the layout.
- **Removed pieces**: `dh-mcp-community-server`, `dh-mcp-enterprise-server`, `DH_MCP_CONFIG_FILE`, the `mcp_reload` tool, and the auth backends. The `auth/` package now contains only `PSKMiddleware` (`auth/middleware/_psk.py`, mounted by the HTTP transport) and the outbound credential / TLS dataclasses (`auth/credentials/`, `auth/tls/`). Restart the server to pick up config changes.

## CLI

- **Binary**: `dh-mcp` — a thin local client. Distinct from the server binaries `dh-mcp-systems-server` and `dh-mcp-docs-server`.
- **Framework**: `click` (>=8.4). All command callbacks are async, wrapped with `@run_async` from `deephaven_mcp.cli._async` (Pattern B async-to-sync adapter). `argparse` is not used in this CLI; do not introduce it.
- **Structure**: noun-verb groups under `src/deephaven_mcp/cli/_commands/`. Top-level nouns: `daemon`, `tool`, `config`. Meta command: `introspect`.
- **Output modes**: `-o human|json|yaml` (default `human`; envvar `DH_MCP_OUTPUT`). Errors are structured (`{error, error_code, exit_code, command}`) in `json` / `yaml` modes; stable `error_code` strings live in `cli/_errors.py`.
- **Exit codes**: `0` success, `2` user-facing failure, `3` tool returned `isError=True`.
- **Eager configuration loading**: every invocation parses and validates the entire configuration directory up front in `_main`'s root callback (`load_runtime` → `Runtime`). Any malformed file fails fast with `config_invalid` (exit 2) before any subcommand body runs. The only paths that bypass the load are `--help` (at any depth) and `dh-mcp introspect`. Subcommand bodies receive a fully-validated `Runtime` via `click.pass_obj` and read `runtime.config.{cli,server,community,enterprise}` directly — no upgrade gate, no overlay. CLI flag overrides (`-o`, `--timeout`, `--no-auto-start`) are applied to `runtime.config.cli` inside `load_runtime`. See `docs/CLI.md` *Configuration loading* for the recovery story.
- **Adding a command**: apply the `cli-command-add` skill. Never `print(..., file=sys.stderr); return 2` — raise `CliError(msg, code=ErrorCode.X)`.
- **Self-discovery for agents**: `dh-mcp introspect` emits the full command tree as JSON — including each command's params, error codes, and output schema; prefer it over scraping `--help`.
- **Help content**: a command is described by three surfaces that must agree — `docs/CLI.md`, the `--help` text (built via `build_help` in `cli/_help.py`), and the introspect manifest. Surfaced help is plain text (no RST markup); the output schema is single-sourced as an `OutputSpec`. The contract is `_cli-help-standards`; apply `cli-help-improve` / `cli-help-accuracy` to author or verify help.

## Python version

The supported Python floor is authoritative in `pyproject.toml` under `requires-python`. Check it (`grep requires-python pyproject.toml`) before using version-gated syntax (PEP 695 generics, `tomllib`, `typing.override`, `StrEnum`, etc.). Do not duplicate the version number elsewhere.

## Python Code

Before writing or editing Python source under `src/` or `tests/` — including docstring-only edits — load `_python-coding-practices` and apply its rules. The most-violated are rule 12 (docstrings describe *what*, not *why*; no rationale, API-symmetry, or cross-call-site narrative) and rule 14 (every field on a field-bearing value class — a Pydantic schema, `@dataclass`, or `NamedTuple` — carries a per-field PEP 257 trailing docstring, never a class-level `Attributes:` block; see `Runtime` in `cli/_runtime.py`). Enums are different: they bind per-member metadata via `__new__`, not field docstrings — see `ErrorCode` in `cli/_errors.py`.
