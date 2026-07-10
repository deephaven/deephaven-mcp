---
name: _project-reference
description: Reference guide for this project — architecture, server commands and ports, config layout, code quality check commands, test clients — invoke when working with server configuration, running checks, or navigating the codebase
user-invocable: false
---

# Deephaven MCP Repository Reference

## Architecture and Key Files

- `src/deephaven_mcp/mcp_systems_server/server.py` — Multiplexed systems server entry point (`main()`); CLI parsing, transport selection, PSK resolution.
- `src/deephaven_mcp/mcp_systems_server/_lifespan.py` — FastMCP lifespan factory: builds `MultiSystemRegistry` from `ConfigTree`, starts one `Evictor` per child registry.
- `src/deephaven_mcp/auth/middleware/_psk.py` — `PSKMiddleware`: Starlette middleware gating inbound HTTP requests on a single shared PSK (`X-Deephaven-PSK` header). Mounted by the systems server's HTTP transport; reusable by other MCP servers.
- `src/deephaven_mcp/auth/credentials/` — Outbound credential dataclasses passed to `CorePlusSessionFactory.from_credentials`.
- `src/deephaven_mcp/mcp_systems_server/_tools/` — MCP tool modules. All registered on the single multiplexed server: `session`, `table`, `script`, `session_community`, `session_enterprise`, `catalog`, `pq`. Shared helpers in `shared.py`. Tools that name a system take a `system` argument; tools that take an `id` (`<type>:<system>:<name>`) parse the system out of it. There is no `mcp_reload` tool — config changes require a restart.
- `src/deephaven_mcp/mcp_docs_server/` — Docs MCP server for documentation Q&A.
- `src/deephaven_mcp/config/` — General-purpose config primitives reusable by any MCP server: `_file_loader.py` (async JSON5 reader + `ConfigurationError` wrapping), `_templating.py` (`${env:VAR}` / `${env:VAR:-default}` / `${file:PATH}` placeholder engine), `_config_dir.py` (default directory + `DH_MCP_DATA_DIR` resolver), `_dir_permissions.py` (`verify_config_directory_permissions` — the startup *policy*: existence/is-dir checks, refuse-to-start, and aggregation of audit violations into one `ConfigurationError`; the per-OS audit mechanics are delegated to `_platform.dir_permissions.audit_tree`).
- `src/deephaven_mcp/_platform/` — OS abstraction layer (HAL): the single home for code that branches on `os.name`. `_os_support.py` (leaf module — `SUPPORTED_OS_NAMES = frozenset({"posix", "nt"})` plus `unsupported_os_error(component)`, the one dispatch-error factory every site uses), `fsutil.py` (advisory file locking, atomic private writes, Windows-retry filesystem helpers), `spawn.py` (`spawn_detached` detached-process launcher — `start_new_session` on POSIX, `creationflags` on Windows, fail-fast `InternalError` otherwise), `dir_permissions.py` (`harden_private_dir` + `audit_tree`, with per-OS `_harden_*`/`_audit_*` impls). The package `__init__.py` is docstring-only and imports no submodules (cycle-safety); submodules import the contract from the leaf `._os_support`. Note: `sys.platform`-keyed path/venv resolution (`config/_data_root.py`, `resource_manager/_launcher.py`) is a different axis and intentionally stays in its domain modules.
- `src/deephaven_mcp/config/schema/` — Pydantic section schemas for the whole product (consumed by both the systems server and the `dh-mcp` CLI): the per-section schema/loader modules `_server.py` (`ServerConfig` + `DaemonProcessConfig`), `_cli.py` (`CliConfig` + `DaemonControlConfig`), `_community.py`, `_enterprise.py` (each owns its umbrella schema and `load_<section>` function), plus the tool-tunable schemas `_response_limits.py` (`ResponseLimits`) and `_pq_config.py` (`PqToolsConfig`) embedded by the community/enterprise schemas.
- `src/deephaven_mcp/config/tree.py` — `ConfigTree` (mirrors the on-disk layout `server.json`, `cli.json`, `community/`, `enterprise/` one-for-one; the canonical aggregator type) and `ConfigTreeLoader` (walks the configuration directory and produces a validated `ConfigTree`). Lives at the top of the `config` package so both `cli` and `mcp_systems_server` depend on it without depending on each other; `config/__init__.py` stays primitives-only so `import deephaven_mcp.config` does not pull in the schema graph.
- `src/deephaven_mcp/resource_manager/_registry_multi.py` — `MultiSystemRegistry`: composite registry over one community child + one enterprise child per configured system; routes session-id reads to the correct child.
- `src/deephaven_mcp/cli/` — Local `dh-mcp` CLI (click + Pattern B). `_main.py` (root group), `_commands/{daemon,tool,session,system,table,catalog,pq,config,introspect}.py` (noun groups; `daemon` for daemon lifecycle, `tool` is the raw MCP escape hatch, `config` reads the local config tree, the runtime nouns `session`/`system`/`table`/`catalog`/`pq` wrap specific MCP tools, `introspect` emits machine-readable CLI metadata), `_async.py` (`run_async` async-to-sync adapter), `_errors.py` (`CliError` + `ErrorCode` registry), `_help.py` (Examples / Environment / Exit-codes templating), `_format.py` (human/json/yaml renderers), `_runtime.py` (resolved `Runtime` context on `ctx.obj`), `_daemon.py` (pure daemon-lifecycle orchestration: registry poll + `get_or_start_daemon(ctx, *, auto_start, startup_deadline_seconds) -> DaemonRegistryEntry` / `stop_daemon(directory, *, kill_after_seconds)`; commands build the `DaemonContext` from a `Runtime` via `build_daemon_context(runtime)` and read tunables from `runtime.config.cli.daemon`; the OS-specific spawn mechanic is delegated to `_platform.spawn.spawn_detached`), `_mcp_client.py` (loopback HTTP client). Async handlers must be wrapped with `@run_async` — see `_python-coding-practices` rule 15 and the `cli-command-add` skill.
- `src/deephaven_mcp/daemon_registry.py` — Shared wire contract between the CLI and a local daemon process. `DaemonRegistryEntry` (Pydantic model for `daemon.json` with field-level invariants — `Literal["127.0.0.1"]` host, port range, `AwareDatetime` started_at, etc.; the recorded `(pid, create_time_ns, process_name)` triple is exposed as a `ProcessIdentity` via the `.identity` property, and `DaemonRegistryEntry.is_live()` is the single PID-reuse-safe liveness predicate shared by the CLI lifecycle and the server's registry-publish refusal), `DaemonDirectory` (typed handle to `<runtime_dir>/daemon/` exposing `registry_path`/`lock_path`/`log_path` and atomic registry CRUD via `tempfile.mkstemp`), `RegistryCorruptError`, and filename constants. Imported by both `mcp_systems_server` (daemon entry point writes the registry) and `cli` (spawn/poll/stop reads it; reachable via `runtime.daemon_dir`).
- `src/deephaven_mcp/_processes.py` — Portable process primitives (no `os.name` branch; stays top-level rather than under `_platform`). `ProcessIdentity` value object: a frozen `(pid, create_time_ns)` pair that anchors all PID-reuse-safe operations on a process. Provides `is_alive()`, `send_signal_safely(sig) -> SignalOutcome` (`DELIVERED`/`GONE`/`DENIED`/`RECYCLED`), and `capture(pid, process_name)` for first-publish capture. The recorded `create_time_ns` is compared by integer equality (no float drift). Replaces the `_capture_create_time` / `_send_sigterm` / `_force_kill` / `_process_still_running` helpers that previously had to trade a raw `(pid, create_time)` tuple by hand at every call site. The OS-dispatched detached-process launcher that used to live here moved to `_platform.spawn.spawn_detached`.
- `src/deephaven_mcp/mcp_systems_server/_idle.py` — Generic idle-shutdown machinery: `IdleTimer` (monotonic-clock data), `ActivityMiddleware` (Starlette middleware bumping the timer), `idle_watcher` (lifespan coroutine that calls a supplied `exit_fn` on expiry). Opted into via `make_lifespan(..., idle=IdleWatcher(...))`; daemon mode always sets it.
- `scripts/` — Test clients and utilities.
- `tests/` — Comprehensive test suite with high line coverage on `src/deephaven_mcp/` (run `tests-run` for the current count and report).
- `pyproject.toml` — Project configuration and dependencies. Authoritative source for the supported Python floor (`requires-python`); check it before using version-gated syntax.

Entry points: `dh-mcp-systems-server` (multiplexed community + enterprise; default HTTP port 8000), `dh-mcp-docs-server` (port 8001), `dh-mcp` (local thin client; auto-spawns a per-user daemon and dispatches MCP tool calls over loopback).

## MCP Server Commands

**Systems Server** — multiplexed; hosts every configured Community session and Enterprise system in one process. Two transports:

```bash
# stdio (no auth; OS pipe is the trust boundary). Default transport.
DH_MCP_DATA_DIR=/path/to/data-root dh-mcp-systems-server --transport stdio

# HTTP with PSK (loopback only).
DH_MCP_DATA_DIR=/path/to/data-root DH_MCP_PSK=$(openssl rand -hex 32) \
  dh-mcp-systems-server --transport http --host 127.0.0.1 --port 8000

# CLI flags also available: --config-dir, --psk, --host, --port.
# Every operator-tunable knob (transport, host, port, psk, server_name)
# lives in server.json; client-layer timeouts live in
# community/settings.json and enterprise/settings.json. The persistent-query
# tool defaults (pq_tools) live in enterprise/settings.json. CLI flags
# override the JSON value per-field when supplied. Use ${env:NAME} inside
# any JSON value to source it from the environment.
```

> **HTTP transport is loopback-only by design.** The server refuses to bind to any non-loopback host; there is no TLS support and no opt-out. To expose the server beyond the local machine, run it behind a TLS-terminating reverse proxy that forwards to `127.0.0.1`.

**Docs Server** — documentation Q&A:

```bash
INKEEP_API_KEY=your-key dh-mcp-docs-server
INKEEP_API_KEY=your-key MCP_DOCS_HOST=0.0.0.0 MCP_DOCS_PORT=8001 dh-mcp-docs-server
# Production endpoint: https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/mcp
```

## Local CLI (`dh-mcp`)

Thin local client for the systems server: auto-spawns a per-user background daemon on first use and dispatches MCP tool calls over loopback HTTP with a PSK. Built on `click` (>=8.4) with Pattern B async wrapping (`@run_async` from `cli/_async.py`). Noun-verb command tree under `cli/_commands/{daemon,tool,session,system,table,catalog,pq,config,introspect}.py`.

- **Reference**: `docs/CLI.md` is the full command/flag/exit-code/error-code surface.
- **Agent self-discovery**: run `dh-mcp introspect tree` for the JSON manifest of the live tree (per-command output schema included), or append `--introspect` to any command for just its node.
- **Machine-first defaults**: every command (including introspect surfaces) defaults to `json`; opt into terminal-friendly output with `-o human` or `DH_MCP_OUTPUT=human`. The introspect surfaces honor `-o`/`DH_MCP_OUTPUT` only — they run before the runtime config load and do not read `cli.json`'s `output.format`.
- **Three-surface contract**: `docs/CLI.md`, `--help`, and the introspect manifest must agree; the help-content contract is `_cli-help-standards`.
- **Adding/editing commands**: apply `cli-command-add` to add; `cli-help-improve` / `cli-help-accuracy` to improve or verify a command's help.

## Configuration

The systems server reads a per-user **directory tree** (not a single file). Default location: `~/.deephaven/ai/config/` on POSIX, `%APPDATA%\Deephaven\ai\config\` on Windows. Override via `--config-dir` or `$DH_MCP_DATA_DIR/config`.

Layout:

```text
server.json                    # PSK for HTTP transport (optional under stdio)
community/
  settings.json                # community-wide globals (optional)
  sessions/
    <name>.json                # one file per static community session
enterprise/
  settings.json                # enterprise-wide globals (optional)
  systems/
    <name>.json                # one file per enterprise system
```

`server.json` example (PSK via env-var indirection):

```json5
{
  "psk": "${env:DH_MCP_PSK}"
}
```

Community session example (`community/sessions/local.json`):

```json5
{
  "host": "localhost",
  "port": 10000,
  "programming_language": "Python",
  "auth": {"credentials": {"type": "anonymous"}}
}
```

Enterprise system example (`enterprise/systems/prod.json`); supports `password` or `private_key` auth:

```json5
{
  "connection_json_url": "https://dhe.example.com/iris/connection.json",
  "auth": {
    "credentials": {
      "type": "password",
      "username": "service-account",
      "password": "${env:DHE_PROD_PASSWORD}"
    }
  }
}
```

Filename stems are validated against the `session_name` / `system_name` field inside each file (when present). The directory permission audit (POSIX strict, Windows best-effort) runs before any file is parsed. There is **no** legacy single-file mode and no `DH_MCP_CONFIG_FILE` env var.

**Docs Server**: requires `INKEEP_API_KEY` env var. `MCP_DOCS_HOST` (default `127.0.0.1`), `MCP_DOCS_PORT` / `PORT` (default 8001).

## Code Quality Checks

```bash
./bin/precommit.sh                                                            # all checks + markdownlint (~22 seconds)
uv run black --check --diff . --exclude '_version\.py|\.venv'                 # formatting (~1.3 seconds)
uv run ruff check src --exclude _version.py                                   # linting (~0.015 seconds)
uv run isort . --check-only --diff --skip _version.py --skip .venv            # import sort (~0.34 seconds)
uv run mypy src/                                                              # type checking (~15 seconds)
npx --yes markdownlint-cli2                                                   # markdown linting (requires node)
```

## Run Tests

```bash
uv run pytest tests/config/test_init.py -v     # config smoke test (~0.5 seconds)
uv run pytest tests/config/ tests/client/ -v   # core tests (~2 seconds)
uv run pytest                                   # full test suite (~19 seconds)
```

## MCP Test Clients

For testing MCP wire protocol directly:

```bash
python scripts/mcp_systems_test_client.py \
  --transport streamable-http --url http://127.0.0.1:8000/mcp

INKEEP_API_KEY=your-key python scripts/mcp_docs_test_client.py \
  --url http://127.0.0.1:8001/mcp --prompt "What is Deephaven?"
```

## Common Issues

- **Port conflicts**: Systems server defaults to 8000 (HTTP transport). Override with `--port` or `server.json`'s `port` field. Docs server defaults to 8001; override with `MCP_DOCS_PORT`.
- **Non-loopback bind refused**: HTTP transport requires a loopback host. Front the server with a reverse proxy if remote access is needed.
- **PSK missing**: HTTP transport requires a PSK from `--psk` or `server.json`'s `psk` field. Stdio transport never reads the PSK.
- **Java required**: Deephaven test server requires Java 11+ in PATH.
- **Reinstalling dependencies**: `uv pip install ".[dev]"` (~3 seconds) — installs the base dependencies plus the tooling extras (`test`, `lint`; `dev` = `test,lint`). There are no per-component extras. The dependency model is owned by `AGENTS.md` (Dependencies); the authoritative package list is `pyproject.toml`'s `[project]` `dependencies` and `[project.optional-dependencies]`.
