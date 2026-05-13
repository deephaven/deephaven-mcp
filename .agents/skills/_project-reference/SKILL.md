---
name: _project-reference
description: Reference guide for this project — architecture, server commands and ports, config file formats, code quality check commands, test clients — invoke when working with server configuration, running checks, or navigating the codebase
---

# Deephaven MCP Repository Reference

## Architecture and Key Files

- `src/deephaven_mcp/mcp_systems_server/server.py` — Community and Enterprise MCP server entry points (`community()` and `enterprise()`)
- `src/deephaven_mcp/mcp_systems_server/_tools/` — Shared MCP tools (session, table, script, catalog, pq, etc.)
- `src/deephaven_mcp/mcp_docs_server/` — Docs MCP server for documentation Q&A
- `src/deephaven_mcp/config/` — Config loading/validation (`community.py`, `enterprise.py`, `__init__.py`)
- `scripts/` — Test clients and utilities
- `tests/` — Comprehensive test suite
- `pyproject.toml` — Project configuration and dependencies

Entry points: `dh-mcp-community-server` (port 8003), `dh-mcp-enterprise-server` (port 8002), `dh-mcp-docs-server` (port 8001).

## MCP Server Commands

**Community Server** — manages one or more DHC / Deephaven Community Core workers:
```bash
DH_MCP_CONFIG_FILE=/path/to/config.json dh-mcp-community-server
dh-mcp-community-server --config /path/to/config.json --host 0.0.0.0 --port 8003
# Host/port also via MCP_HOST / MCP_PORT env vars
```

**Enterprise Server** — manages a single DHE / Deephaven Enterprise system:
```bash
DH_MCP_CONFIG_FILE=/path/to/enterprise.json dh-mcp-enterprise-server
dh-mcp-enterprise-server --config /path/to/enterprise.json --host 0.0.0.0 --port 8002
```

**Docs Server** — documentation Q&A:
```bash
INKEEP_API_KEY=your-key dh-mcp-docs-server
INKEEP_API_KEY=your-key MCP_DOCS_HOST=0.0.0.0 MCP_DOCS_PORT=8001 dh-mcp-docs-server
# Production endpoint: https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/mcp
```

## Configuration

**Community Server** config schema:
```json
{
  "sessions": {
    "session-name": {
      "host": "localhost",
      "port": 10000,
      "session_type": "python",
      "auth_type": "Anonymous"
    }
  }
}
```

**Enterprise Server** config schema (all fields at top level, no nesting):
```json
{
  "system_name": "prod",
  "connection_json_url": "https://dhe.example.com/iris/connection.json",
  "auth_type": "password",
  "username": "user",
  "password_env_var": "DHE_PASSWORD"
}
```

**Docs Server**: requires `INKEEP_API_KEY` env var. `MCP_DOCS_HOST` (default `127.0.0.1`), `MCP_DOCS_PORT` / `PORT` (default 8001).

## Code Quality Checks

```bash
./bin/precommit.sh                                                            # all checks + markdownlint (~22 seconds)
uv run black --check --diff . --exclude '_version\.py|\.venv'                # formatting (~1.3 seconds)
uv run ruff check src --exclude _version.py                                  # linting (~0.015 seconds)
uv run isort . --check-only --diff --skip _version.py --skip .venv           # import sort (~0.34 seconds)
uv run mypy src/                                                              # type checking (~15 seconds)
npx --yes markdownlint-cli2                                                   # markdown linting (requires node)
```

## Run Tests

```bash
uv run pytest tests/config/test_init.py -v     # config smoke test (~0.5 seconds)
uv run pytest tests/config/ tests/client/ -v   # core tests (~2 seconds)
uv run pytest                                   # full test suite
```

## MCP Test Clients

For testing MCP wire protocol directly:
```bash
python scripts/mcp_community_test_client.py \
  --transport streamable-http --url http://127.0.0.1:8003/mcp

INKEEP_API_KEY=your-key python scripts/mcp_docs_test_client.py \
  --url http://127.0.0.1:8001/mcp --prompt "What is Deephaven?"
```

## Common Issues

- **Port conflicts**: Community defaults to 8003, enterprise to 8002, docs to 8001. Override with `--port` or `MCP_PORT` (`MCP_DOCS_PORT` for docs).
- **Java required**: Deephaven test server requires Java 11+ in PATH.
- **Reinstalling dependencies**: `uv pip install ".[dev]"` (~3 seconds); enterprise wheel: `./bin/dev_manage_coreplus_client.sh --venv .venv install-wheel --wheel-file ops/artifacts/deephaven_coreplus_client-*-py3-none-any.whl`
