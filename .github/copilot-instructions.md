# Deephaven MCP Repository Instructions

Always follow these instructions precisely when working in this repository. Only search for additional context if the information here is incomplete or incorrect.

## Working Effectively

Bootstrap and build the repository:
- `git clone https://github.com/deephaven/deephaven-mcp.git` -- timing varies by network speed and repository size
- `cd deephaven-mcp`
- `pip install uv` -- if uv not installed
- `uv venv .venv -p 3.12` -- takes 0.01 seconds  
- `source .venv/bin/activate` (Linux/macOS) or `.venv\Scripts\activate` (Windows)
- `uv pip install ".[dev]"` -- installs all dependencies including dev tools, takes 3 seconds. NEVER CANCEL. Set timeout to 60+ seconds.

For enterprise features, install the Core Plus wheel separately:
- `./bin/dev_manage_coreplus_client.sh install-wheel --file /path/to/wheel.whl` -- install from local wheel file

Run tests:
- `uv run pytest tests/test_init.py -v` -- basic smoke test, takes 0.5 seconds
- `uv run pytest tests/test__*.py -v` -- core tests, takes 2 seconds. NEVER CANCEL.
- `uv run pytest` -- full test suite (may require additional dependencies)

## MCP Servers

This repository provides two Model Context Protocol (MCP) servers:

**Systems Server** (orchestrates Deephaven Community Core workers):
- `dh-mcp-systems-server --help` -- shows available options
- `DH_MCP_CONFIG_FILE=/path/to/config.json dh-mcp-systems-server --transport sse` -- starts on port 8000
- `DH_MCP_CONFIG_FILE=/path/to/config.json dh-mcp-systems-server` -- stdio mode (default)

**Docs Server** (documentation Q&A):
- `INKEEP_API_KEY=your-key dh-mcp-docs-server --transport sse` -- starts on port 8001 
- `INKEEP_API_KEY=your-key dh-mcp-docs-server` -- streamable-http mode (default)
- Use `PORT=8001` environment variable to change port

## Configuration

**Systems Server Configuration:**
- Requires `DH_MCP_CONFIG_FILE` environment variable pointing to JSON config file
- Create a basic config file for testing:
```bash
cat > /tmp/deephaven_mcp.json << 'EOF'
{
  "community": {
    "sessions": {
      "local_test": {
        "host": "localhost",
        "port": 10000,
        "session_type": "python",
        "auth_type": "anonymous"
      }
    }
  }
}
EOF
```

**Docs Server Configuration:**
- `INKEEP_API_KEY` -- required for docs server functionality
- `OPENAI_API_KEY` -- optional fallback for docs server

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

**Systems Server Test:**
```bash
python scripts/mcp_community_test_client.py \
  --transport stdio \
  --stdio-cmd "uv run dh-mcp-systems-server --transport stdio" \
  --env DH_MCP_CONFIG_FILE=/tmp/deephaven_mcp.json
```

**Docs Server Test:**
```bash
python scripts/mcp_docs_test_client.py \
  --transport stdio \
  --prompt "What is Deephaven?" \
  --env INKEEP_API_KEY=your-inkeep-api-key \
  --env OPENAI_API_KEY=your-openai-api-key
```

## Validation Scenarios

After making changes, always validate with these complete scenarios:

1. **Fresh Environment Test:**
   - Clone repository fresh
   - Set up virtual environment 
   - Install dependencies
   - Run basic tests
   - Start both servers briefly to verify functionality

2. **Code Quality Validation:**
   - Run `./bin/precommit.sh` and ensure it passes
   - Verify no new linting or formatting issues

3. **MCP Server Validation:**
   - Test systems server starts with config file
   - Test docs server starts with API key
   - Verify both servers respond to basic commands

## Architecture and Key Files

**Main Components:**
- `src/deephaven_mcp/mcp_systems_server/` -- Systems MCP server for Deephaven orchestration
- `src/deephaven_mcp/mcp_docs_server/` -- Docs MCP server for documentation Q&A  
- `scripts/` -- Test clients and utilities
- `tests/` -- Comprehensive test suite
- `pyproject.toml` -- Project configuration and dependencies

**Entry Points:**
- `dh-mcp-systems-server` -- Systems server command
- `dh-mcp-docs-server` -- Docs server command

**Transport Modes:**
- `stdio` -- Standard input/output (default for systems server, good for Claude Desktop)
- `sse` -- Server-Sent Events over HTTP (good for testing)  
- `streamable-http` -- HTTP streaming (default for docs server, optimal performance)

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

- **Missing `DH_MCP_CONFIG_FILE`**: Systems server requires this environment variable pointing to a valid JSON config
- **Missing `INKEEP_API_KEY`**: Docs server requires this for documentation functionality  
- **Wrong command names**: Use `dh-mcp-systems-server` and `dh-mcp-docs-server`, not abbreviated versions
- **Port conflicts**: Systems server uses port 8000, docs server uses 8001 by default (configurable with `PORT` env var)
- **Java required**: Deephaven test server requires Java 11+ to be installed and in PATH

The project uses modern Python development practices with uv for fast package management, comprehensive testing, and excellent tooling integration for code quality.