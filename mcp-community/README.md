
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
uv pip install ".[dev]"
```

### 2. Prepare Worker Configuration

The MCP server requires a JSON configuration file describing the available Deephaven Community Core workers. This file must be an object with a `workers` mapping and a `default_worker` key.

See the section on [Worker Configuration File Specification](#worker-configuration-file-specification) below for a complete list of fields and options.

**Example:**
```json
{
  "workers": {
    "worker1": {
      "host": "localhost",
      "port": 10000
    },
    "worker2": {
      "host": "localhost",
      "port": 10001
    }
  },
  "default_worker": "worker1"
}
```

Set the `DH_MCP_CONFIG_FILE` environment variable to the path of this file before starting the server.

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

## Worker Configuration File Specification

The worker configuration file is a JSON object that defines all available Deephaven Community Core workers and their connection details. It is referenced via the `DH_MCP_CONFIG_FILE` environment variable.

### Full Example


```json
{
  "workers": {
    "worker1": {
      "host":             "localhost",   // Optional: hostname or IP address of the Deephaven server (default: localhost)
      "port":             10000,          // Optional: port number for the Deephaven server (default: 10000)
      "auth_type":        "Anonymous",   // Optional: authentication type (default: Anonymous)
      "auth_token":       "",            // Optional: authentication token
      "use_tls":          false,          // Optional: whether to use TLS (default: false)
      "session_type":     "python",      // Optional: session type (default: python)
      "never_timeout":    false,          // Optional: prevent session timeout (default: false)
      "tls_root_certs":   null,           // Optional: path to root CA certs for TLS
      "client_cert_chain":null,           // Optional: path to client certificate chain for TLS
      "client_private_key":null           // Optional: path to client private key for TLS
    },
    "worker2": {
      "host": "localhost",
      "port": 10001
    }
  },
  "default_worker": "worker1"
}
```

> **Note:** The comments in the above example (lines starting with `//`) are for documentation only and must be removed in your actual configuration file. Standard JSON does not support comments.

### Field Reference
| Field                | Type      | Required | Default      | Description                                                      |
|----------------------|-----------|----------|--------------|------------------------------------------------------------------|
| `workers`            | object    | Yes      | —            | Map of worker names to worker configuration objects               |
| `default_worker`     | string    | Yes      | —            | Name of the default worker (must match a key in `workers`)       |
| `host`               | string    | No       | "localhost"  | Hostname or IP address of the Deephaven server (optional; only required for direct TCP connections) |
| `port`               | integer   | No       | 10000        | Port number for the Deephaven server (optional; only required for direct TCP connections)           |
| `auth_type`          | string    | No       | "Anonymous"  | Authentication type (`Anonymous`, `Basic`, `Bearer`, etc.)       |
| `auth_token`         | string    | No       | ""           | Authentication token, if required                                |
| `use_tls`            | boolean   | No       | false         | Whether to use TLS/SSL for the connection                        |
| `session_type`       | string    | No       | "python"     | Session type for Deephaven connection                            |
| `never_timeout`      | boolean   | No       | false         | Prevent session timeout                                          |
| `tls_root_certs`     | string    | No       | null          | Path to root CA certificates for TLS                             |
| `client_cert_chain`  | string    | No       | null          | Path to client certificate chain for mutual TLS                  |
| `client_private_key` | string    | No       | null          | Path to client private key for mutual TLS                        |

### Notes
- The `default_worker` must be the name of one of the keys in `workers`.
- The config file must be valid JSON (no comments allowed in actual file).
- Unknown fields are not allowed.
- This schema may be extended in future releases; consult the documentation for updates.

---

## Usage

### Running the MCP Server
- **SSE:** For integration with web-based tools (e.g., Inspector).
- **stdio:** For local development, CLI tools, or subprocess-based clients.

### Test Client
A Python script for exercising the MCP tools and validating server functionality.

**Arguments:**
- `--transport`: Choose `sse` or `stdio`.
- `--env`: Pass environment variables as `KEY=VALUE` (can be repeated).
- `--url`: URL for SSE server (if using SSE transport).
- `--stdio-cmd`: Command to launch stdio server (if using stdio transport).

> **Note:** You must start a test Deephaven server before using the test client. Use the provided script:
>
> ```sh
> uv run scripts/run_deephaven_test_server.py
> ```
>
> The server you start must be represented as `worker1` in your configuration file (see `DH_MCP_CONFIG_FILE`). Ensure the server is running and accessible according to your worker configuration before running the test client.

#### Example usage (stdio):
```sh
uv run scripts/mcp_test_client.py --transport stdio --env DH_MCP_CONFIG_FILE=/path/to/deephaven_workers.json
```

#### Example usage (SSE):
First, start the MCP server in SSE mode (in a separate terminal):
```sh
DH_MCP_CONFIG_FILE=/path/to/deephaven_workers.json uv run dh-mcp-community --transport sse
```
Then, in another terminal, run the test client:
```sh
uv run scripts/mcp_test_client.py --transport sse
```

### MCP Inspector Integration
The [MCP Inspector](https://github.com/modelcontextprotocol/inspector) is a very useful tool for testing MCP servers, especially when developing new features. It provides an interactive UI for exploring and invoking MCP tools.

#### Recommended workflow:
1. **Start a Deephaven Community Core worker** (in one terminal):
   - The provided test server script is a convenient option:
     ```sh
     uv run scripts/run_deephaven_test_server.py
     ```
   - Alternatively, you may start any compatible Deephaven Community Core server instance.
2. **Start the MCP server in SSE mode** (in another terminal):
   ```sh
   DH_MCP_CONFIG_FILE=/path/to/deephaven_workers.json uv run dh-mcp-community --transport sse
   ```
3. **Start the MCP Inspector** (in a third terminal, no arguments needed):
   ```sh
   npx @modelcontextprotocol/inspector@latest
   ```
4. **Connect to the MCP server via SSE**
   - Open the Inspector in your browser (URL will be shown in the terminal, typically `http://localhost:6274`).
   - In the Inspector UI, select "Connect" and enter the SSE URL (e.g., `http://localhost:8000/sse`).

This workflow allows you to interactively test and debug your MCP server implementation with minimal setup.

---

## Development

### Install Dev Dependencies
```sh
uv pip install .[dev]
```

### Useful Commands
- **Run tests:** `uv pytest`
- **Run all tests with coverage:**
  ```sh
  uv run pytest --cov=src --cov-report=term-missing
  ```
  This will print a coverage summary to the terminal. To generate an HTML report:
  ```sh
  uv run pytest --cov=src --cov-report=html
  open htmlcov/index.html
  ```
- **Type checking:** `uv run mypy .`
- **Linting:** `uv run ruff .`
- **Format code:** `uv run black .`
- **Sort imports:** `uv run isort .`

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

