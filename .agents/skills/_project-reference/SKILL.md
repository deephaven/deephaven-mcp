---
name: _project-reference
description: Reference guide for this project — architecture, server commands and ports, config file formats, code quality check commands, test clients — invoke when working with server configuration, running checks, or navigating the codebase
---

# Deephaven MCP Repository Reference

## Architecture and Key Files

- `src/deephaven_mcp/mcp_systems_server/server.py` — Community and Enterprise MCP server entry points (`community()` and `enterprise()`)
- `src/deephaven_mcp/mcp_systems_server/_tools/` — MCP tool modules. Shared on both servers: `session`, `table`, `script`. Community-exclusive: `session_community`. Enterprise-exclusive: `session_enterprise`, `catalog`, `pq`. Per-server `reload` variant. Registration is wired in `server.py` (`_SHARED_TOOLS`, `_register_community_tools`, `_register_enterprise_tools`).
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
dh-mcp-community-server --config /path/to/config.json --host 127.0.0.1 --port 8003
# Host/port also via MCP_HOST / MCP_PORT env vars
```

**Enterprise Server** — manages a single DHE / Deephaven Enterprise system:
```bash
DH_MCP_CONFIG_FILE=/path/to/enterprise.json dh-mcp-enterprise-server
dh-mcp-enterprise-server --config /path/to/enterprise.json --host 127.0.0.1 --port 8002
```

> **Non-loopback binds require a transport-security opt-in.** The systems
> server (`dh-mcp-community-server` / `dh-mcp-enterprise-server`) refuses to
> start with a non-loopback `--host` (e.g. `0.0.0.0`) unless one of the
> following is configured: native TLS via `--ssl-keyfile` + `--ssl-certfile`
> (or `MCP_SSL_KEYFILE` / `MCP_SSL_CERTFILE`); a fronting TLS-terminating
> proxy via `--trust-forwarded-proto` (paired with `--forwarded-allow-ips`,
> default `127.0.0.1`); or the explicit cleartext opt-out
> `--allow-cleartext` (`MCP_ALLOW_CLEARTEXT=1`). Auth headers
> (`X-Deephaven-Password`, `X-Deephaven-Private-Key`, `X-Deephaven-PSK`)
> travel in cleartext on the wire, so a non-loopback bind without any of
> these is treated as a hard startup error. Examples:
>
> ```bash
> # Native TLS
> dh-mcp-community-server --host 0.0.0.0 --port 8003 \
>     --ssl-keyfile /path/key.pem --ssl-certfile /path/cert.pem
>
> # Behind a TLS-terminating reverse proxy on the same host
> dh-mcp-enterprise-server --host 0.0.0.0 --port 8002 \
>     --trust-forwarded-proto --forwarded-allow-ips 127.0.0.1
>
> # Emergency cleartext opt-out (logs a loud warning each startup)
> dh-mcp-community-server --host 0.0.0.0 --port 8003 --allow-cleartext
> ```

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

**Enterprise Server** config schema (all fields at top level, no nesting). The
config file declares only *which* auth backends the server mounts; user
credentials are supplied per request via `X-Deephaven-*` HTTP headers and are
never stored in this file:
```json
{
  "system_name": "prod",
  "connection_json_url": "https://dhe.example.com/iris/connection.json",
  "auth": {
    "backends": ["password", "private_key"],
    "allow_effective_user": false
  }
}
```

The legacy top-level credential fields (`auth_type`, `username`, `password`,
`password_env_var`, `private_key_path`) have been removed from the schema and
will fail validation at startup. See
`docs/DEVELOPER_GUIDE.md#enterprise-auth-model` for the full per-request auth
model and supported `X-Deephaven-*` headers.

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
- **Reinstalling dependencies**: `uv pip install ".[dev]"` (~3 seconds). For enterprise (Core+) support, install the `enterprise` extra — `uv pip install ".[enterprise]"` or `pip install "deephaven-mcp[enterprise]"` — which pulls `deephaven-coreplus-client` from PyPI (see `pyproject.toml`'s `[project.optional-dependencies]` `enterprise` and `dev` entries).
