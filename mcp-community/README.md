
# Deephaven MCP Community

> **Note:** This document contains low-level technical details for developers. **Users seeking high-level usage and onboarding information should refer to the main documentation in the [root README](../README.md).**

> _A Python server for orchestrating Deephaven Community Core nodes via the Model Context Protocol (MCP)_

A Python implementation of a Model Context Protocol (MCP) server for Deephaven Community Core, built with [FastMCP](https://github.com/jlowin/fastmcp). This project enables the orchestration, inspection, and management of Deephaven Community Core worker nodes via the MCP protocol, supporting both SSE (Server-Sent Events) and stdio transports.

---

## Table of Contents
- [Project Overview](#project-overview)
- [Features](#features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Running the Test Deephaven Server](#running-the-test-deephaven-server)
  - [Running the MCP Server](#running-the-mcp-server)
  - [Test Client](#test-client)
  - [MCP Inspector](#mcp-inspector)
  - [Claude Desktop Integration](#claude-desktop-integration)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Resources](#resources)
- [License](#license)

---

## Project Overview

This repository provides an implementation of a Deephaven Community Core MCP server, exposing a set of tools for remote orchestration and inspection of Deephaven Community Core worker nodes. The server uses the [Deephaven Community Core Python client](https://docs.deephaven.io/core/client-api/python/) to connect to and manage worker nodes. It is designed for use with the Model Context Protocol ecosystem and integrates with the [MCP Inspector](https://github.com/modelcontextprotocol/inspector) and other MCP-compatible tools.

## Features
- **MCP Server:** Implements the MCP protocol for Deephaven Community Core workers using the [Deephaven Community Core Python client](https://docs.deephaven.io/core/client-api/python/) for communication.
- **Multiple Transports:** Supports both SSE (for web) and stdio (for local/subprocess) communication.
- **Tooling:** Exposes tools for refreshing configuration, listing workers, inspecting table schemas, and running scripts.
- **Configurable:** Loads worker configuration from a JSON file or environment variable.
- **Async Lifecycle:** Uses FastMCP's async lifespan for robust startup and shutdown handling.
- **Test Client:** Includes a Python script for invoking tools and testing server endpoints.

## Architecture
- **Server:** Built on [FastMCP](https://github.com/jlowin/fastmcp) and [autogen-ext](https://github.com/jlowin/autogen-ext).
- **Workers:** Each worker is a Deephaven Community Core server defined in a config file.
- **Tools:** Exposed as MCP tools:
  - `refresh`: Reload configuration and clear all active worker sessions atomically.
  - `worker_statuses`: List all configured workers and their availability status.
  - `table_schemas`: Retrieve schemas for one or more tables from a worker (requires `worker_name`).
  - `run_script`: Execute a script on a specified Deephaven worker (requires `worker_name` and a script or script path).
- **Transport:** Selectable via CLI (`--transport sse` or `--transport stdio`).

```
          +----------------------+
          |   MCP Inspector      |
          | Claude Desktop, etc.|
          +----------+-----------+
                     |
              SSE/stdio (MCP)
                     |
           +---------v---------+
           |   MCP Server      |
           +---------+---------+
                     |
         +-----------+-----------+
         |                       |
+--------v--------+     +--------v--------+
| Deephaven Core  | ... | Deephaven Core  |
| Worker (worker1)|     | Worker (workerN)|
+-----------------+     +-----------------+
```

**Diagram:** Clients (Inspector, Claude Desktop) connect to the MCP Server via SSE or stdio. The MCP Server manages multiple Deephaven Community Core workers.

## Quick Start

### Environment Variables

| Variable                | Required | Description                                                  | Where Used                |
|-------------------------|----------|--------------------------------------------------------------|---------------------------|
| `DH_MCP_CONFIG_FILE`    | Yes      | Path to worker config JSON file for MCP server and clients.   | MCP Server, Test Client   |
| `PYTHONLOGLEVEL`        | No       | Python logging level (e.g., DEBUG, INFO).                    | Server, Client (optional) |

> Set `DH_MCP_CONFIG_FILE` before running the MCP server or test client. See the [Configuration](#configuration) section for details.

### 1. Clone and Install
```sh
git clone https://github.com/deephaven/deephaven-mcp.git
cd deephaven-mcp/mcp-community
uv pip install ".[dev]"
```

### 2. Prepare Worker Configuration

The MCP server requires a JSON configuration file describing the available Deephaven Community Core workers. This file must be an object with a `workers` mapping. 

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

## Usage

### Running the Test Deephaven Server

To use the MCP test client or Inspector, you need a running Deephaven Community Core server. For development and testing, use the provided script:

```sh
uv run scripts/run_deephaven_test_server.py --table-group {simple|financial|all}
```

- **Purpose:** Launches a local Deephaven server with demo tables for MCP development and testing. Not for production use.
- **Arguments:**
  - `--table-group {simple|financial|all}` (**required**): Which demo tables to create
  - `--host HOST` (default: `localhost`)
  - `--port PORT` (default: `10000`)
- **Requirements:** `deephaven-server` Python package, Java in PATH, 8GB+ RAM

Make sure the server is running and matches the worker config in your `DH_MCP_CONFIG_FILE`.

### Running the MCP Server

To start the MCP server, run:
```sh
uv run dh-mcp-community --transport {sse|stdio}
```

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

### MCP Inspector
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

### Claude Desktop Integration
Claude Desktop is very useful for debugging and interactively exploring MCP servers. The configuration file format described in this documentation is also used by most AI Agents that support MCP, making it easy to reuse your setup across different tools.

To configure Claude Desktop to use your MCP server:

1. **Open Claude Desktop.**
2. **Navigate to `Settings > Developer > Edit Config`.**
3. **Edit the `claude_desktop_config.json` file.**
4. **Add your MCP server under the `mcpServers` section.**
   - We recommend using the `stdio` transport for best results.
   - Example configuration:
     ```json
     {
       "mcpServers": {
         "mcp-community": {
           "command": "uv",
           "args": [
             "--directory",
             "/path/to/deephaven-mcp/mcp-community",
             "run",
             "dh-mcp-community"
           ],
           "env": {
             "DH_MCP_CONFIG_FILE": "/path/to/deephaven_workers.json"
           }
         }
       }
     }
     ```
5. **Save the configuration and restart Claude Desktop if needed.**

Claude Desktop will now be able to discover and use the tools exposed by your Deephaven MCP server.

> **Log Location:** For troubleshooting, Claude Desktop MCP logs are located at:
> - **macOS:** `~/Library/Logs/Claude`
> - **Windows:** `%APPDATA%\Claude\logs`
> 
> - `mcp.log` contains general logging about MCP connections and connection failures.
> - Files named `mcp-server-SERVERNAME.log` contain error (stderr) logs from each configured server.
> 
> These logs can help diagnose connection and communication issues.


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
  }
}
```

> **Note:** The comments in the above example (lines starting with `//`) are for documentation only and must be removed in your actual configuration file. Standard JSON does not support comments.

### Field Reference
| Field                | Type      | Required | Default      | Description                                                      |
|----------------------|-----------|----------|--------------|------------------------------------------------------------------|
| `workers`            | object    | Yes      | —            | Map of worker names to worker configuration objects               |
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

### Running the Test Deephaven Server

To use the MCP test client or Inspector, you need a running Deephaven Community Core server. For development and testing, use the provided script:

```sh
uv run scripts/run_deephaven_test_server.py --table-group {simple|financial|all}
```

- **Purpose:** Launches a local Deephaven server with demo tables for MCP development and testing. Not for production use.
- **Arguments:**
  - `--table-group {simple|financial|all}` (**required**): Which demo tables to create
  - `--host HOST` (default: `localhost`)
  - `--port PORT` (default: `10000`)
- **Requirements:** `deephaven-server` Python package, Java in PATH, 8GB+ RAM

Make sure the server is running and matches the worker config in your `DH_MCP_CONFIG_FILE`.

---

## Development

### Setting Up Your Development Environment

1. **Install Dev Dependencies**
   ```sh
   uv pip install .[dev]
   ```
   This will install all necessary packages for development, testing, linting, and formatting.

2. **Pre-commit Hooks (Recommended)**
   To automatically run linting and formatting before each commit, you can set up pre-commit hooks:
   ```sh
   uv pip install pre-commit
   pre-commit install
   ```

---

### Useful Commands

#### Testing & Coverage
- **Run all tests with coverage summary:**
  ```sh
  uv run pytest --cov=src --cov-report=term-missing
  ```
  Prints a concise coverage report to the terminal.

- **Generate HTML coverage report:**
  ```sh
  uv run pytest --cov=src --cov-report=html
  open htmlcov/index.html
  ```
  Opens a detailed, navigable HTML report in your browser.

- **Run tests only (no coverage):**
  ```sh
  uv run pytest
  ```

#### Static Analysis & Code Quality
- **Type checking:**
  ```sh
  uv run mypy src
  ```
  Checks for type errors using [mypy](http://mypy-lang.org/).

- **Linting:**
  ```sh
  uv run ruff check src
  uv run ruff check src --fix  # Auto-fix common issues
  ```
  Uses [Ruff](https://docs.astral.sh/ruff/) for fast linting and code quality enforcement.

- **Format code:**
  ```sh
  uv run black .
  ```
  Formats your code using [Black](https://black.readthedocs.io/).

- **Sort imports:**
  ```sh
  uv run isort .
  ```
  Sorts imports for consistency using [isort](https://pycqa.github.io/isort/).

---

### Running and Debugging the Server

- **Run the server directly (development mode):**
  ```sh
  PYTHONPATH=src uv run mcp dev src/deephaven_mcp/community/_mcp.py:mcp_server
  ```
  This command starts the MCP server for local development. You can specify different entrypoints as needed.

- **Interactive Tools:**
  Use the Inspector or the test client for interactive tool calls and debugging.

---

## Troubleshooting

### Common Errors & Solutions

- **Config File Not Found:**
  - Ensure `DH_MCP_CONFIG_FILE` points to a valid JSON file.
  - Example error: `FileNotFoundError: No such file or directory: ...`
- **Invalid JSON/Schema in Config:**
  - Double-check your worker config file for syntax errors or unsupported fields.
  - Use a JSON linter or validator if unsure.
- **Port Already in Use:**
  - Change the port in your config or ensure no other process is using it.
  - Example error: `OSError: [Errno 98] Address already in use`
- **Timeouts:**
  - Check that Deephaven workers are running and reachable.
  - Increase timeouts if needed in client/server code.
- **Transport Issues:**
  - Verify you are using the correct transport and URL/command.
  - For SSE, ensure ports are open and not firewalled.
  - For stdio, check the command path and environment variables.
- **Missing Dependencies:**
  - Ensure all Python dependencies are installed (`uv pip install .[dev]`).
- **Session Errors:**
  - Review logs for session cache or connection errors.
- **KeyboardInterrupt / CancelledError:**
  - Normal if you stop the server with Ctrl+C.

### Log File Locations

- **MCP Server:**
  - Standard output/error in your terminal.
  - Set `PYTHONLOGLEVEL=DEBUG` for more verbose logs.
- **MCP Inspector:**
  - Shown in browser and terminal running `npx @modelcontextprotocol/inspector@latest`.
- **Claude Desktop:**
  - macOS: `~/Library/Logs/Claude/`
  - Windows: `%APPDATA%\Claude\logs`
  - `mcp.log` for general MCP logs; `mcp-server-SERVERNAME.log` for server-specific errors.

### Debugging Tips

- **Enable Debug Logging:**
  - Set `PYTHONLOGLEVEL=DEBUG` before running the server or client for detailed logs.
- **Check System Resources:**
  - Deephaven requires significant RAM (8GB+ recommended) and Java.
- **Use MCP Inspector:**
  - Great for interactively testing and debugging tool calls.
- **Use Claude Desktop:**
  - Excellent for interactively exploring and debugging MCP servers with a desktop UI; provides detailed logs and supports custom server configurations.
- **Validate Worker Connectivity:**
  - Use `ping`, `telnet`, or similar tools to confirm worker host/port accessibility.
- **Check Configuration:**
  - Ensure all paths and environment variables are correct and accessible from your shell.
- **Update Dependencies:**
  - If you experience strange errors, try upgrading to the latest versions of dependencies.

> For persistent issues, open an issue on GitHub with logs and configuration details.

---

## Resources
- [FastMCP Tutorial](https://www.firecrawl.dev/blog/fastmcp-tutorial-building-mcp-servers-python)
- [FastMCP GitHub](https://github.com/jlowin/fastmcp)
- [autogen-ext GitHub](https://github.com/jlowin/autogen-ext)
- [MCP Inspector](https://github.com/modelcontextprotocol/inspector)

---

## License

This project is licensed under the [Apache License 2.0](../LICENSE).

---

For questions, issues, or contributions, please open an issue or pull request on GitHub.

