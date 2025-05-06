
# Deephaven MCP Community

A Python implementation of a Model Context Protocol (MCP) server for Deephaven Community Core, built with [FastMCP](https://github.com/jlowin/fastmcp). This project enables the orchestration, inspection, and management of Deephaven Community Core worker nodes via the MCP protocol, supporting both SSE (Server-Sent Events) and stdio transports.

---

## Table of Contents
- [Project Overview](#project-overview)
- [Features](#features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Running the MCP Server](#running-the-mcp-server)
  - [Test Client](#test-client)
  - [Inspector Integration](#inspector-integration)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Resources](#resources)
- [License](#license)

---

## Project Overview

This repository provides an implementation of a Deephaven Community Core MCP server, exposing a set of tools for remote orchestration and inspection of Deephaven Community Core worker nodes. It is designed for use with the Model Context Protocol ecosystem and integrates with the [MCP Inspector](https://github.com/modelcontextprotocol/inspector) and other MCP-compatible tools.

## Features
- **MCP Server:** Implements the MCP protocol for Deephaven Community Core workers.
- **Multiple Transports:** Supports both SSE (for web) and stdio (for local/subprocess) communication.
- **Tooling:** Exposes tools for refreshing configuration, listing workers, inspecting table schemas, and running scripts.
- **Configurable:** Loads worker configuration from a JSON file or environment variable.
- **Async Lifecycle:** Uses FastMCP's async lifespan for robust startup and shutdown handling.
- **Test Client:** Includes a Python script for invoking tools and testing server endpoints.

## Architecture
- **Server:** Built on [FastMCP](https://github.com/jlowin/fastmcp) and [autogen-ext](https://github.com/jlowin/autogen-ext).
- **Workers:** Each worker is a Deephaven Community Core server defined in a config file.
- **Tools:** Exposed as MCP tools (refresh, default_worker, worker_names, table_schemas, run_script).
- **Transport:** Selectable via CLI (`--transport sse` or `--transport stdio`).

## Quick Start

### 1. Clone and Install
```sh
git clone https://github.com/deephaven/deephaven-mcp.git
cd deephaven-mcp/mcp-community
uv pip install .[dev]  # or pip install -e .[dev] if you use pip
```

### 2. Prepare Worker Configuration
Create a JSON file describing your Deephaven workers. Example:
```json
[
  {"name": "worker1", "host": "localhost", "port": 10000},
  {"name": "worker2", "host": "localhost", "port": 10001}
]
```

The `DH_MCP_CONFIG_FILE` environment variable should be set to the path of this file.

### 3. Run the MCP Server
#### SSE Transport (for web/Inspector):
```sh
DH_MCP_CONFIG_FILE=/path/to/deephaven_workers.json uv run dh-mcp-community --transport sse
```

#### stdio Transport (for local/test):
```sh
DH_MCP_CONFIG_FILE=/path/to/deephaven_workers.json uv run dh-mcp-community --transport stdio
```

---

## Configuration

- **Config file:** Set `DH_MCP_CONFIG_FILE` to point to your worker JSON.
- **Logging:** Controlled by `PYTHONLOGLEVEL` env variable (e.g., `export PYTHONLOGLEVEL=DEBUG`).
- **Other env:** Pass additional environment variables using the test client’s `--env` flag.

---

## Usage

### Running the MCP Server
- **SSE:** For integration with web-based tools (e.g., Inspector).
- **stdio:** For local development, CLI tools, or subprocess-based clients.

### Test Client
A Python script for exercising the MCP tools and validating server functionality.

#### Example usage:
```sh
uv run scripts/mcp_test_client.py --transport stdio --env DH_MCP_CONFIG_FILE=/path/to/deephaven_workers.json
```
- `--transport`: Choose `sse` or `stdio`.
- `--env`: Pass environment variables as `KEY=VALUE` (can be repeated).
- `--url`: URL for SSE server (if using SSE transport).
- `--stdio-cmd`: Command to launch stdio server (if using stdio transport).

### Inspector Integration
Use the [MCP Inspector](https://github.com/modelcontextprotocol/inspector) to interactively explore and invoke MCP tools:

```sh
npx @modelcontextprotocol/inspector@latest --config ../mcp-config.json --server mcp-community
```

---

## Development

### Install Dev Dependencies
```sh
uv pip install .[dev]
```

### Useful Commands
- **Run tests:** `uv pytest`
- **Type checking:** `uv mypy .`
- **Linting:** `uv ruff .`
- **Format code:** `uv black .`
- **Sort imports:** `uv isort .`

### Debugging
- Run the server directly with a custom entrypoint:
  ```sh
  PYTHONPATH=src uv run mcp dev src/deephaven_mcp/community/_mcp.py:mcp_server
  ```
- Use the Inspector or the test client for interactive tool calls.

---

## Troubleshooting

- **Config File Not Found:**
  - Ensure `DH_MCP_CONFIG_FILE` points to a valid JSON file.
  - Example error: `FileNotFoundError: No such file or directory: ...`
- **Timeouts:**
  - Check that Deephaven workers are running and reachable.
  - Increase timeouts if needed in client/server code.
- **Transport Issues:**
  - Verify you are using the correct transport and URL/command.
  - For SSE, ensure ports are open and not firewalled.
- **Session Errors:**
  - Review logs for session cache or connection errors.
- **KeyboardInterrupt / CancelledError:**
  - Normal if you stop the server with Ctrl+C.

---

## Resources
- [FastMCP Tutorial](https://www.firecrawl.dev/blog/fastmcp-tutorial-building-mcp-servers-python)
- [FastMCP GitHub](https://github.com/jlowin/fastmcp)
- [autogen-ext GitHub](https://github.com/jlowin/autogen-ext)
- [MCP Inspector](https://github.com/modelcontextprotocol/inspector)

---

## License

[Apache License 2.0](../LICENSE)

---

For questions, issues, or contributions, please open an issue or pull request on GitHub.

