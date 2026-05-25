---
name: _project-reference
description: Reference guide for this project — architecture, server commands and ports, config layout, code quality check commands, test clients — invoke when working with server configuration, running checks, or navigating the codebase
---

# Deephaven MCP Repository Reference

## Architecture and Key Files

- `src/deephaven_mcp/mcp_systems_server/server.py` — Multiplexed systems server entry point (`main()`); CLI parsing, transport selection, PSK resolution.
- `src/deephaven_mcp/mcp_systems_server/_lifespan.py` — FastMCP lifespan factory: builds `MultiSystemRegistry` from `MultiSystemConfig`, starts one `Evictor` per child registry.
- `src/deephaven_mcp/auth/middleware/_psk.py` — `PSKMiddleware`: Starlette middleware gating inbound HTTP requests on a single shared PSK (`X-Deephaven-PSK` header). Mounted by the systems server's HTTP transport; reusable by other MCP servers.
- `src/deephaven_mcp/auth/credentials/` — Outbound credential dataclasses passed to `CorePlusSessionFactory.from_credentials`.
- `src/deephaven_mcp/mcp_systems_server/_tools/` — MCP tool modules. All registered on the single multiplexed server: `session`, `table`, `script`, `session_community`, `session_enterprise`, `catalog`, `pq`. Shared helpers in `shared.py`. Tools that name a system take a `system` argument; tools that take a `session_id` (`<type>:<source>:<name>`) parse the system out of it. There is no `mcp_reload` tool — config changes require a restart.
- `src/deephaven_mcp/mcp_docs_server/` — Docs MCP server for documentation Q&A.
- `src/deephaven_mcp/config/` — General-purpose config primitives reusable by any MCP server: `_file_loader.py` (async JSON5 reader + `ConfigurationError` wrapping), `_templating.py` (`${env:VAR}` / `${env:VAR:-default}` / `${file:PATH}` placeholder engine), `_config_dir.py` (default directory + `DH_MCP_CONFIG_DIR` resolver), `_dir_permissions.py` (POSIX strict / Windows best-effort permission audit).
- `src/deephaven_mcp/mcp_systems_server/config/` — Systems-server-specific Pydantic schemas and orchestration: `_multi.py` (walks the configuration directory; `MultiSystemConfigManager`) plus the per-section schema/loader modules `_server.py`, `_community.py`, `_enterprise.py` (each owns its umbrella schema and `load_<section>` function).
- `src/deephaven_mcp/resource_manager/_registry_multi.py` — `MultiSystemRegistry`: composite registry over one community child + one enterprise child per configured system; routes session-id reads to the correct child.
- `scripts/` — Test clients and utilities.
- `tests/` — Comprehensive test suite with high line coverage on `src/deephaven_mcp/` (run `tests-run` for the current count and report).
- `pyproject.toml` — Project configuration and dependencies.

Entry points: `dh-mcp-systems-server` (multiplexed community + enterprise; default HTTP port 8000), `dh-mcp-docs-server` (port 8001).

## MCP Server Commands

**Systems Server** — multiplexed; hosts every configured Community session and Enterprise system in one process. Two transports:

```bash
# stdio (no auth; OS pipe is the trust boundary). Default transport.
DH_MCP_CONFIG_DIR=/path/to/config-dir dh-mcp-systems-server --transport stdio

# HTTP with PSK (loopback only).
DH_MCP_CONFIG_DIR=/path/to/config-dir DH_MCP_PSK=$(openssl rand -hex 32) \
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

## Configuration

The systems server reads a per-user **directory tree** (not a single file). Default location: `~/.deephaven/ai/config/` on POSIX, `%APPDATA%\Deephaven\ai\config\` on Windows. Override via `--config-dir` or `$DH_MCP_CONFIG_DIR`.

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
python scripts/mcp_community_test_client.py \
  --transport streamable-http --url http://127.0.0.1:8000/mcp

INKEEP_API_KEY=your-key python scripts/mcp_docs_test_client.py \
  --url http://127.0.0.1:8001/mcp --prompt "What is Deephaven?"
```

## Common Issues

- **Port conflicts**: Systems server defaults to 8000 (HTTP transport). Override with `--port` or `server.json`'s `port` field. Docs server defaults to 8001; override with `MCP_DOCS_PORT`.
- **Non-loopback bind refused**: HTTP transport requires a loopback host. Front the server with a reverse proxy if remote access is needed.
- **PSK missing**: HTTP transport requires a PSK from `--psk` or `server.json`'s `psk` field. Stdio transport never reads the PSK.
- **Java required**: Deephaven test server requires Java 11+ in PATH.
- **Reinstalling dependencies**: `uv pip install ".[dev]"` (~3 seconds). For enterprise (Core+) support, install the `enterprise` extra — `uv pip install ".[enterprise]"` or `pip install "deephaven-mcp[enterprise]"` — which pulls `deephaven-coreplus-client` from PyPI (see `pyproject.toml`'s `[project.optional-dependencies]` `enterprise` and `dev` entries).
