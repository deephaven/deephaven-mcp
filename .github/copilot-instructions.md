# Deephaven MCP Repository Instructions

Always follow these instructions precisely when working in this repository. Only search for additional context if the information here is incomplete or incorrect.

## Working Effectively

Bootstrap and build the repository:

- `git clone https://github.com/deephaven/deephaven-mcp.git` -- timing varies by network speed and repository size
- `cd deephaven-mcp`
- `pip install uv` -- if uv not installed
- `uv venv .venv -p 3.12` -- takes 0.01 seconds  
- `uv pip install ".[dev]"` -- installs all dependencies including dev tools, takes 3 seconds. NEVER CANCEL. Set timeout to 60+ seconds.
- `./bin/dev_manage_coreplus_client.sh --venv .venv install-wheel --wheel-file ops/artifacts/deephaven_coreplus_client-*-py3-none-any.whl` -- install enterprise Core Plus wheel for enterprise features

Run tests:

- `uv run pytest tests/config/test_init.py -v` -- config smoke test, takes 0.5 seconds
- `uv run pytest tests/config/ tests/client/ -v` -- core tests, takes 2 seconds. NEVER CANCEL.
- `uv run pytest` -- full test suite (may require additional dependencies)

## MCP Servers

This repository provides three Model Context Protocol (MCP) servers:

**Community Server** (manages one or more DHC / Deephaven Community Core workers):

- HTTP-only transport (streamable-http). Default port **8003**.
- `dh-mcp-community-server --help` -- shows available options
- `DH_MCP_CONFIG_FILE=/path/to/config.json dh-mcp-community-server` -- starts on 127.0.0.1:8003
- `dh-mcp-community-server --config /path/to/config.json --host 0.0.0.0 --port 8003` -- explicit args
- Host/port can also be set via `MCP_HOST` / `MCP_PORT` environment variables

**Enterprise Server** (manages a single DHE / Deephaven Enterprise system):

- HTTP-only transport (streamable-http). Default port **8002**.
- One server instance per DHE system (run multiple instances for multiple systems).
- `dh-mcp-enterprise-server --help` -- shows available options
- `DH_MCP_CONFIG_FILE=/path/to/enterprise.json dh-mcp-enterprise-server` -- starts on 127.0.0.1:8002
- `dh-mcp-enterprise-server --config /path/to/enterprise.json --host 0.0.0.0 --port 8002` -- explicit args
- Host/port can also be set via `MCP_HOST` / `MCP_PORT` environment variables

**Docs Server** (documentation Q&A):

- HTTP-only transport (streamable-http). Default port **8001**.
- `INKEEP_API_KEY=your-key dh-mcp-docs-server` -- starts on 127.0.0.1:8001
- `INKEEP_API_KEY=your-key MCP_DOCS_HOST=0.0.0.0 MCP_DOCS_PORT=8001 dh-mcp-docs-server` -- explicit args
- Host/port can also be set via `MCP_DOCS_HOST` / `MCP_DOCS_PORT` environment variables (or `PORT` for Cloud Run)

## Configuration

**Community Server Configuration:**

- Requires `DH_MCP_CONFIG_FILE` environment variable or `--config` CLI argument pointing to a JSON/JSON5 config file
- Create a basic config file for testing:

```bash
cat > /tmp/deephaven_community.json << 'EOF'
{
  "community": {
    "sessions": {
      "local_test": {
        "host": "localhost",
        "port": 10000,
        "session_type": "python",
        "auth_type": "Anonymous"
      }
    }
  }
}
EOF
```

**Enterprise Server Configuration:**

- Requires `DH_MCP_CONFIG_FILE` environment variable or `--config` CLI argument pointing to a flat JSON/JSON5 config file
- All fields are at the top level (no nesting under a system name):

```bash
cat > /tmp/deephaven_enterprise.json << 'EOF'
{
  "system_name": "prod",
  "connection_json_url": "https://dhe.example.com/iris/connection.json",
  "auth_type": "password",
  "username": "user",
  "password_env_var": "DHE_PASSWORD"
}
EOF
```

**Docs Server Configuration:**

- `INKEEP_API_KEY` -- required for docs server functionality
- `MCP_DOCS_HOST` -- host to bind to (default: `127.0.0.1`); set to `0.0.0.0` for external access
- `MCP_DOCS_PORT` / `PORT` -- port override (default: 8001; `PORT` is checked as a fallback for Cloud Run compatibility)
- Production streamable-http MCP server: `https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/mcp`

## Validation and Code Quality

Always run code quality checks before committing:

- `./bin/precommit.sh` -- runs all checks, takes 22 seconds. NEVER CANCEL. Set timeout to 60+ seconds.

Individual checks:

- `uv run black --check --diff . --exclude '_version\.py|\.venv'` -- formatting check, takes 1.3 seconds
- `uv run ruff check src --exclude _version.py` -- linting, takes 0.015 seconds  
- `uv run isort . --check-only --diff --skip _version.py --skip .venv` -- import sorting, takes 0.34 seconds
- `uv run mypy src/` -- type checking, takes ~15 seconds (part of precommit.sh)
- `uv run pydocstyle src` -- docstring style check

## Testing MCP Functionality

Test the servers without a full Deephaven instance:

**Community Server Test:**

```bash
DH_MCP_CONFIG_FILE=/tmp/deephaven_community.json python scripts/mcp_community_test_client.py \
  --transport streamable-http \
  --url http://127.0.0.1:8003/mcp
```

**Docs Server Test:**

```bash
INKEEP_API_KEY=your-inkeep-api-key python scripts/mcp_docs_test_client.py \
  --url http://127.0.0.1:8001/mcp \
  --prompt "What is Deephaven?"
```

## Validation Scenarios

After making changes, always validate with these complete scenarios:

1. **Fresh Environment Test:**
   - Clone repository fresh
   - Set up virtual environment
   - Install dependencies
   - Run basic tests
   - Start all three servers briefly to verify functionality

2. **Code Quality Validation:**
   - Run `./bin/precommit.sh` and ensure it passes
   - Verify no new linting or formatting issues

3. **MCP Server Validation:**
   - Test community server starts with config file
   - Test enterprise server starts with config file
   - Test docs server starts with API key
   - Verify all servers respond to basic commands

## Architecture and Key Files

**Main Components:**

- `src/deephaven_mcp/mcp_systems_server/server.py` -- Community and Enterprise MCP server entry points (`community()` and `enterprise()`)
- `src/deephaven_mcp/mcp_systems_server/_tools/` -- Shared MCP tools (session, table, script, catalog, pq, etc.)
- `src/deephaven_mcp/mcp_docs_server/` -- Docs MCP server for documentation Q&A  
- `src/deephaven_mcp/config/` -- Config loading/validation (`_community.py`, `_enterprise.py`, `__init__.py`)
- `scripts/` -- Test clients and utilities
- `tests/` -- Comprehensive test suite
- `pyproject.toml` -- Project configuration and dependencies

**Entry Points:**

- `dh-mcp-community-server` -- Community server command (HTTP-only, port 8003)
- `dh-mcp-enterprise-server` -- Enterprise server command (HTTP-only, port 8002; one instance per DHE system)
- `dh-mcp-docs-server` -- Docs server command (HTTP-only, port 8001)

**Transport Modes:**

- `streamable-http` -- HTTP streaming (only supported transport for all three servers)

## Critical Timing Information

**NEVER CANCEL these operations:**

- Package installation: Allow 60+ seconds
- Pre-commit checks: Allow 60+ seconds  
- Any `uv pip install` commands: Allow 60+ seconds

**Expected timing:**

- Git clone: (network-dependent, may vary)
- Virtual environment creation: ~0.01 seconds
- Base package install: ~3 seconds
- Test dependency install: ~1.5 seconds
- Code formatting check: ~1.3 seconds
- Basic tests: ~0.5 seconds
- Pre-commit full suite: ~22 seconds

## Common Issues

- **Missing `DH_MCP_CONFIG_FILE`**: Community and enterprise servers require this (or `--config` arg) pointing to a valid JSON/JSON5 config file
- **Missing `INKEEP_API_KEY`**: Docs server requires this for documentation functionality  
- **Wrong command names**: Use `dh-mcp-community-server`, `dh-mcp-enterprise-server`, and `dh-mcp-docs-server`
- **Port conflicts**: Community server defaults to 8003, enterprise to 8002, docs to 8001. Override with `--port` or `MCP_PORT` (`MCP_DOCS_PORT` for docs server)
- **No stdio/sse**: All three servers (`dh-mcp-community-server`, `dh-mcp-enterprise-server`, `dh-mcp-docs-server`) only support `streamable-http`
- **Java required**: Deephaven test server requires Java 11+ to be installed and in PATH

The project uses modern Python development practices with uv for fast package management, comprehensive testing, and excellent tooling integration for code quality.
