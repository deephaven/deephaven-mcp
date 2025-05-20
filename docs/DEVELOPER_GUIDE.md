# Deephaven MCP

> **You are reading the [Developer & Contributor Guide](DEVELOPER_GUIDE.md) for Deephaven MCP.**

> **Project repository:** [https://github.com/deephaven/deephaven-mcp](https://github.com/deephaven/deephaven-mcp)

> **Note:** This document contains low-level technical details for contributors working on the [deephaven-mcp/mcp-community](https://github.com/deephaven/deephaven-mcp/mcp-community) project. **End users seeking high-level usage and onboarding information should refer to the main documentation in the [`../README.md`](../README.md).**

This repository houses the Python-based Model Context Protocol (MCP) servers for Deephaven:
1. **Deephaven MCP Community Server**: Orchestrates Deephaven Community Core nodes.
2. **Deephaven MCP Docs Server**: Provides conversational Q&A about Deephaven documentation.

> **Requirements**: [Python](https://www.python.org/) 3.10 or later is required to run these servers.

---

## Table of Contents

- [Introduction](#introduction)
  - [About This Project](#about-this-project)
  - [Key Features](#key-features)
  - [System Architecture](#system-architecture)
- [Quick Start Guide](#quick-start-guide)
  - [Community Server Quick Start](#community-server-quick-start)
  - [Docs Server Quick Start](#docs-server-quick-start)
- [MCP Server Implementations](#mcp-server-implementations)
  - [Community Server](#community-server)
    - [Overview](#community-server-overview)
    - [Configuration](#community-server-configuration)
    - [Running the Community Server](#running-the-community-server)
    - [Using the Community Server](#using-the-community-server)
    - [Community Server Tools](#community-server-tools)
    - [Community Server Test Components](#community-server-test-components)
  - [Docs Server](#docs-server)
    - [Docs Server Overview](#docs-server-overview)
    - [Docs Server Configuration](#docs-server-configuration)
    - [Running the Docs Server](#running-the-docs-server)
    - [Docs Server Tools](#docs-server-tools)
    - [Docs Server HTTP Endpoints](#docs-server-http-endpoints)
    - [Docs Server Test Components](#docs-server-test-components)
- [Integration Methods](#integration-methods)
  - [MCP Inspector](#mcp-inspector)
    - [With Community Server](#with-community-server)
    - [With Docs Server](#with-docs-server)
  - [Claude Desktop](#claude-desktop)
    - [Configuration](#configuration)
  - [mcp-proxy](#mcp-proxy)
    - [With Community Server](#with-community-server-1)
    - [With Docs Server](#with-docs-server-1)
  - [Programmatic API](#programmatic-api)
    - [Community Server Example](#community-server-example)
    - [Docs Server Example](#docs-server-example)
- [Development](#development)
  - [Development Workflow](#development-workflow)
  - [Docker Compose](#docker-compose)
  - [Advanced Development Techniques](#advanced-development-techniques)
  - [Development Commands](#development-commands)
  - [Performance Testing](#performance-testing)
    - [Usage Example](#usage-example)
    - [Arguments](#arguments)
- [Troubleshooting](#troubleshooting)
  - [Session Management](#session-management)
  - [Common Errors & Solutions](#common-errors--solutions)
  - [Debugging Tips](#debugging-tips)
  - [Log File Locations](#log-file-locations)
- [Resources](#resources)
  - [Deephaven API Reference](#deephaven-api-reference)
  - [Tools & Related Projects](#tools--related-projects)
  - [Community & Support](#community--support)
- [License](#license)

---

## Introduction

### About This Project

The [deephaven-mcp/mcp-community](https://github.com/deephaven/deephaven-mcp/mcp-community) project provides Python implementations of two Model Context Protocol (MCP) servers:

1. **Deephaven MCP Community Server**:
   * Enables orchestration, inspection, and management of Deephaven Community Core worker nodes via the MCP protocol
   * Built with [FastMCP](https://github.com/jlowin/fastmcp)
   * Exposes tools for refreshing configuration, listing workers, inspecting table schemas, and running scripts
   * Maintains [PyDeephaven](https://github.com/deephaven/deephaven-core/tree/main/py) client sessions to each configured worker, with sophisticated session management.
   * The Community Server orchestrates multiple Deephaven Core worker nodes, providing a unified interface for managing workers, their sessions, and data through the Model Context Protocol (MCP). It includes sophisticated session management with automatic caching, concurrent access safety, and lifecycle management.

2. **Deephaven MCP Docs Server**:
   * Offers an agentic, LLM-powered API for Deephaven documentation Q&A and chat
   * Uses Inkeep/OpenAI APIs for its LLM capabilities
   * Designed for integration with orchestration frameworks

Both servers are designed for integration with MCP-compatible tools like the [MCP Inspector](https://github.com/modelcontextprotocol/inspector) and [Claude Desktop](https://claude.ai).

### Key Features

**Community Server Features:**
* **MCP Server:** Implements the MCP protocol for Deephaven Community Core workers
* **Multiple Transports:** Supports both SSE (for web) and stdio (for local/subprocess) communication
* **Configurable:** Loads worker configuration from a JSON file or environment variable
* **Async Lifecycle:** Uses FastMCP's async lifespan for robust startup and shutdown handling
* **Lazy Loading:** Sessions are created on-demand to improve startup performance and resilience

**Docs Server Features:**
* **MCP-compatible server** for documentation Q&A and chat
* **LLM-powered:** Uses Inkeep/OpenAI APIs
* **FastAPI backend:** Deployable locally or via Docker
* **Single tool:** `docs_chat` for conversational documentation assistance
* **Extensible:** Python-based for adding new tools or extending context

### System Architecture

**Community Server Architecture:**

```
          +----------------------+
          |   MCP Inspector      |
          | Claude Desktop, etc. |
          +----------+-----------+
                     |
              SSE/stdio (MCP)
                     |
           +---------v---------+
           |   MCP Server      |
           |  (Community)      |
           +---------+---------+
                     |
         +-----------+-----------+
         |                       |
+--------v--------+     +--------v--------+
| Deephaven Core  | ... | Deephaven Core  |
| Worker (worker1)|     | Worker (workerN)|
+-----------------+     +-----------------+
```

Clients (Inspector, Claude Desktop) connect to the MCP Server via SSE or stdio. The MCP Server manages multiple Deephaven Community Core workers. The architecture allows for scalable worker management and flexible client integrations.

**Docs Server Architecture:**

```
+--------------------+
|  User/Client/API   |
+---------+----------+
          |
      HTTP/MCP
          |
+---------v----------+
|   MCP Docs Server  |
|   (FastAPI, LLM)   |
+---------+----------+
          |
  [Deephaven Docs]
```

Users or API clients send natural language questions or documentation queries over HTTP using the Model Context Protocol (MCP). These requests are received by the server, which is built on FastAPI and powered by a large language model (LLM) via the Inkeep API.

## Quick Start Guide

### Community Server Quick Start

1. **Set up worker configuration:**
   Create a JSON configuration file for your Deephaven workers:
   ```json
   {
     "workers": {
       "local_worker": {
         "host": "localhost",
         "port": 10000
       }
     }
   }
   ```
   Save this as `deephaven_workers.json` in your project directory.

2. **Start a test Deephaven server in one terminal:**
   ```bash
   uv run scripts/run_deephaven_test_server.py --table-group simple
   ```
   
   > This script is located at [`../scripts/run_deephaven_test_server.py`](../scripts/run_deephaven_test_server.py) and creates a local Deephaven server with test data.

3. **Run the Community Server:**
   ```sh
   DH_MCP_CONFIG_FILE=deephaven_workers.json uv run dh-mcp-community --transport sse
   ```

4. **Test with the MCP Inspector:**
   ```sh
   npx @modelcontextprotocol/inspector@latest
   ```
   Connect to `http://localhost:8000/sse` in the Inspector UI.

### Docs Server Quick Start

1. **Set up Inkeep API key:**
   ```sh
   export INKEEP_API_KEY=your-inkeep-api-key  # Get from https://inkeep.com
   ```
   
2. **Run the Docs Server:**
   ```sh
   uv run dh-mcp-docs --transport sse
   ```

3. **Test with the MCP Inspector:**
   ```sh
   npx @modelcontextprotocol/inspector@latest
   ```
   Connect to `http://localhost:8000/sse` in the Inspector UI and test the `docs_chat` tool.

## Command Line Entry Points

This package registers the following console entry points for easy command-line access:

| Command | Description | Source |
|---------|-------------|--------|
| `dh-mcp-community` | Start the Community Server | `deephaven_mcp.community:main` |
| `dh-mcp-docs` | Start the Docs Server | `deephaven_mcp.docs:main` |

These commands are automatically available in your PATH after installing the package.

## MCP Server Implementations

### Community Server

#### Community Server Overview

The Deephaven MCP Community Server is an [MCP](https://github.com/modelcontextprotocol/spec)-compatible server (built with [FastMCP](https://github.com/jlowin/fastmcp)) that provides tools for interacting with Deephaven Community Core instances.

Key architectural features include:

- **Efficient Session Management**: Implements a sophisticated session caching system using [PyDeephaven](https://github.com/deephaven/deephaven-core/tree/main/py) that automatically reuses existing connections when possible and manages session lifecycles.
- **Concurrent Access Safety**: Uses [asyncio](https://docs.python.org/3/library/asyncio.html) Lock mechanisms to ensure thread-safe operations during configuration refreshes and session management.
- **Automatic Resource Cleanup**: Gracefully handles session termination and cleanup during server shutdown or refresh operations.
- **On-Demand Session Creation**: Sessions to worker nodes are created only when needed and cached for future use.
- **Async-First Design**: Built around [asyncio](https://docs.python.org/3/library/asyncio.html) for high-concurrency performance and non-blocking operations.
- **Configurable Session Behavior**: Supports worker configuration options such as `never_timeout` to control session persistence and lifecycle management.

#### Community Server Configuration

##### Environment Variables
| Variable                | Required | Description                                                  | Where Used                |
|-------------------------|----------|--------------------------------------------------------------|---------------------------|
| `DH_MCP_CONFIG_FILE`    | Yes      | Path to worker config JSON file for MCP server and clients.   | MCP Server, Test Client   |
| `PYTHONLOGLEVEL`        | No       | Python logging level (e.g., DEBUG, INFO).                    | Server, Client (optional) |

> Set `DH_MCP_CONFIG_FILE` before running the MCP server or test client. Environment variables can also be loaded from `.env` files using [python-dotenv](https://github.com/theskumar/python-dotenv).

##### Worker Configuration File Specification (`DH_MCP_CONFIG_FILE`)
The MCP Community server requires a JSON configuration file specified by the `DH_MCP_CONFIG_FILE` environment variable. This file describes the available Deephaven Community Core workers and must be a JSON object with a top-level `workers` key. The path should be absolute for reliability across different working directories.

**Configuration Schema:**

```json
{
  "workers": {                  // Required top-level object mapping worker names to configs
    "worker_name": {          // Each worker has its own named configuration block
      "host": "localhost",    // String: Hostname or IP address of the worker
      "port": 10000,          // Integer: Port number for the worker connection
      "auth_type": "token",   // String: "token", "basic", or "anonymous"
      "auth_token": "...",    // String: Authentication token or password (if needed)
      "never_timeout": true,  // Boolean: If true, sessions to this worker never time out
      "session_type": "multi", // String: "single" (one session) or "multi" (multiple sessions)
      "use_tls": true,        // Boolean: Whether to use TLS/SSL for the connection
      "tls_root_certs": "...", // String: Path to PEM file with root certificates
      "client_cert_chain": "...", // String: Path to PEM file with client cert chain
      "client_private_key": "..." // String: Path to PEM file with client private key
    }
  }
}
```

**Notes:**
- All fields are optional
- Sensitive fields like `auth_token` are automatically redacted in logs

**Example Configuration:**

```json
{
  "workers": {
    "local": {
      "host": "localhost",
      "port": 10000,
      "auth_type": "anonymous"
    },
    "prod": {
      "host": "deephaven-server.example.com",
      "port": 10000,
      "auth_type": "token",
      "auth_token": "your-token-here",
      "use_tls": true,
      "never_timeout": true,
      "session_type": "single"
    },
    "secure_worker": {
      "host": "secure.deephaven.io",
      "port": 10002,
      "auth_type": "token",
      "auth_token": "your_bearer_token_here",
      "never_timeout": true,
      "session_type": "multi",
      "use_tls": true,
      "tls_root_certs": "/path/to/trusted_cas.pem",
      "client_cert_chain": "/path/to/client_cert_and_chain.pem",
      "client_private_key": "/path/to/client_private_key.pem"
    }
  }
}
```

**Fields:**
The configuration file must be a JSON object with one top-level key:
*   `workers` (object, required): A map where keys are unique worker names (strings) and values are worker configuration objects.
    *   Each worker configuration object can contain the following fields. All fields within an individual worker's configuration are optional from the perspective of the configuration parser. However, fields like `host` and `port` are practically necessary for a worker to function.
        *   `host` (string): Hostname or IP address of the Deephaven worker.
        *   `port` (integer): Port number for the Deephaven worker connection.
        *   `auth_type` (string): Authentication type. Supported values:
            *   `"anonymous"`: No authentication is used.
            *   `"basic"`: HTTP Basic authentication. Credentials are typically provided via the `auth_token` field (e.g., base64 encoded "user:password").
            *   `"token"`: Bearer token authentication. The token is provided via the `auth_token` field.
        *   `auth_token` (string): The authentication token or password string. This field is used when `auth_type` is `"basic"` or `"token"`.
        *   `never_timeout` (boolean): If `true`, sessions established with this worker should ideally not time out.

#### Running the Community Server

Follow these steps to start the Community Server:

1. **Start a Deephaven Core worker**:
      ```sh
      uv run scripts/run_deephaven_test_server.py --table-group simple
      ```
      This script is located at [`../scripts/run_deephaven_test_server.py`](../scripts/run_deephaven_test_server.py).

2. **Start the MCP Community Server**:
   ```sh
   uv run dh-mcp-community --transport sse --port 8000
   ```

   Remember to set `DH_MCP_CONFIG_FILE` first.

##### CLI Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `-t, --transport` | Transport type (`sse` or `stdio`) | `stdio` |
| `-h, --help` | Show help message | - |

> **Note:** When using SSE transport, the server binds to port 8000 by default. This can be modified by setting the `PORT` environment variable.

*   **SSE Transport (for web/Inspector):**
    ```sh
    # Default port (8000)
    DH_MCP_CONFIG_FILE=/path/to/workers.json uv run dh-mcp-community --transport sse
    
    # Custom port (8001)
    PORT=8001 DH_MCP_CONFIG_FILE=/path/to/workers.json uv run dh-mcp-community --transport sse
    ```
*   **stdio Transport (for direct/subprocess use):**
    ```sh
    DH_MCP_CONFIG_FILE=/path/to/workers.json uv run dh-mcp-community --transport stdio
    ```

#### Using the Community Server

Once running, you can interact with the Community Server in several ways:

- Connect using [MCP Inspector](#with-community-server)
- Use with [Claude Desktop](#claude-desktop)
- Run the [Test Client](#test-client) script
- Build your own MCP client application

#### Community Server Tools

The Community Server exposes the following MCP tools, each designed for a specific aspect of Deephaven worker management:

All Community Server tools return responses with a consistent format:
- Success: `{ "success": true, ... }` with additional fields depending on the tool
- Error: `{ "success": false, "error": { "type": "error_type", "message": "Error description" } }`

#### Error Handling

All Community Server tools use a consistent error response format when encountering problems:

```json
{
  "success": false,
  "error": {
    "type": "error_type",
    "message": "Human-readable error description"
  }
}
```

**Common Error Types:**
- `worker_not_found`: The specified worker does not exist or is not configured
- `worker_unavailable`: The worker exists but is not currently running or accessible
- `internal_error`: Unexpected internal server error
- `invalid_argument`: Incorrect or missing parameters in the tool request

This consistent format makes error handling and response parsing more predictable across all tools.

#### MCP Tools

The Community Server provides the following MCP tools:

##### `refresh`

**Purpose**: Atomically reload configuration and clear all active worker sessions.

**Parameters**: None

**Returns**: 
```json
{
  "success": true
}
```

On error:
```json
{
  "success": false,
  "error": "Error message",
  "isError": true
}
```

**Description**: This tool reloads the worker configuration from the file specified in `DH_MCP_CONFIG_FILE` and clears all active sessions. It's useful after changing the worker configuration to ensure changes are immediately applied. The tool uses an asyncio.Lock to ensure thread safety and atomicity of the operation.

##### `worker_statuses`

**Purpose**: List all configured Deephaven workers and check their availability status.

**Parameters**: None

**Returns**:
```json
{
  "success": true,
  "result": [
    {"worker": "worker_name_1", "available": true},
    {"worker": "worker_name_2", "available": false}
  ]
}
```

On error:
```json
{
  "success": false,
  "error": "Error message",
  "isError": true
}
```

**Description**: This tool checks the status of all configured workers, attempting to establish a connection to verify availability.

##### `table_schemas`

**Purpose**: Retrieve schemas for one or more tables from a specific worker.

**Parameters**:
- `worker_name` (required): Name of the Deephaven worker.
- `table_names` (optional): List of table names to fetch schemas for. If omitted, schemas for all tables are returned.

**Returns**:
The tool returns a list of objects, one for each table:
```json
[
  {
    "success": true,
    "table": "table_name",
    "schema": [
      {"name": "column1", "type": "int"},
      {"name": "column2", "type": "string"}
    ]
  },
  {
    "success": false,
    "table": "missing_table",
    "error": "Table not found",
    "isError": true
  }
]
```

On complete failure (e.g., worker not available):
```json
[
  {
    "success": false,
    "table": null,
    "error": "Failed to connect to worker: ...",
    "isError": true
  }
]
```

**Description**: This tool retrieves column schemas (name and type) for tables in the specified worker.

##### `run_script`

**Purpose**: Execute a script on a specified Deephaven worker.

**Parameters**:
- `worker_name` (required): Name of the Deephaven worker.
- `script` (optional): Script content as a string. The script language (Python, Groovy, etc.) is determined by the worker's configuration.
- `script_path` (optional): Path to a script file.

**Note**: Either `script` or `script_path` must be provided, but not both.

**Returns**:
```json
{
  "success": true
}
```

**Important Limitations**: The tool only returns a success status and does not include stdout or a list of created/modified tables in the response. Any output or tables created by the script will need to be accessed via other tools (like `table_schemas`), not from this response.

On error:
```json
{
  "success": false,
  "error": "Error message",
  "isError": true
}
```

**Description**: This tool executes code on a specified worker. The script language is determined by the worker's configuration and can be Python, Groovy, or other supported languages. According to the source code, it only returns success status and does not include stdout or a list of created/modified tables in the response.

##### `pip_packages`

**Purpose**: Retrieve all installed pip packages (name and version) from a specified worker.

**Parameters**:
- `worker_name` (str): The name of the Deephaven worker to query.

**Returns**:
```json
{
  "success": true,
  "result": [
    {"package": "numpy", "version": "1.25.0"},
    {"package": "pandas", "version": "2.0.1"}
  ]
}
```

On error:
```json
{
  "success": false,
  "error": "Table not found",
  "isError": true
}
```

**Description**:  
This tool connects to the specified Deephaven worker, gathers installed pip packages using Python's [`importlib.metadata`](https://docs.python.org/3/library/importlib.metadata.html), and returns them as a list of dictionaries.

#### Community Server Test Components

##### Test Server

For development and testing the MCP Community server, you often need a running Deephaven Community Core server. A script is provided for this:

```sh
uv run scripts/run_deephaven_test_server.py --table-group {simple|financial|all}
```

**Arguments:**
* `--table-group {simple|financial|all}` (**required**): Which demo tables to create
* `--host HOST` (default: `localhost`): Host to bind to
* `--port PORT` (default: `10000`): Port to listen on

##### Test Client

A Python script ([`../scripts/mcp_community_test_client.py`](../scripts/mcp_community_test_client.py)) is available for exercising the Community MCP tools and validating server functionality without setting up a full MCP Inspector deployment. The script connects to a running server, lists all available tools, and demonstrates calling each tool with appropriate arguments.

```sh
uv run scripts/mcp_community_test_client.py --transport {sse|stdio|streamable-http} [OPTIONS]
```

**Key Arguments:**
* `--transport`: Choose `sse` (default) or `stdio`
* `--env`: Pass environment variables as `KEY=VALUE` (e.g., `DH_MCP_CONFIG_FILE=/path/to/config.json`). Can be repeated for multiple variables
* `--url`: URL for SSE server (default: `http://localhost:8000/sse`)
* `--stdio-cmd`: Command to launch stdio server (default: `uv run dh-mcp-community --transport stdio`)

**Example Usage:**
```sh
# Connect to running SSE server
uv run scripts/mcp_community_test_client.py --transport sse --url http://localhost:8000/sse

# Launch stdio server with environment variables (useful for CI/CD or agent integration)
uv run scripts/mcp_community_test_client.py --transport stdio --env DH_MCP_CONFIG_FILE=/absolute/path/to/config.json
```

> ⚠️ **Prerequisites:** 
> - You must have a test Deephaven server running (see [Running the Community Server](#running-the-community-server))
> - The MCP Community server must be running (or use `--stdio-cmd` for the client to launch it)
> - For troubleshooting connection issues, see [Common Errors & Solutions](#common-errors--solutions)

> 💡 **Tips:** 
> - Use stdio mode in CI/CD pipelines and SSE mode for interactive development
> - Environment variables can be set in your shell or passed via `--env` parameter
> - For multiple environment variables, use `--env` multiple times: `--env VAR1=value1 --env VAR2=value2`

---

### Docs Server

#### Docs Server Overview

The Deephaven MCP Docs Server is a specialized MCP server that provides a single tool for conversational chat about Deephaven documentation.

- **Primary LLM**: Uses the [Inkeep](https://inkeep.com/) `inkeep-context-expert` model with domain-specific knowledge of Deephaven documentation
- **Fallback Mechanism**: Automatically falls back to [OpenAI](https://openai.com/) if the Inkeep API is unavailable or returns an error
- **System Prompting**: Uses a specialized system prompt that instructs the model to answer with reference to Deephaven documentation
- **Error Resilience**: Implements robust error handling with custom `OpenAIClientError` for detailed diagnostics
- **Conversational Context**: Maintains conversation history for multi-turn Q&A sessions
- **Health Monitoring**: Provides a dedicated `/health` endpoint for operational monitoring

The server helps users learn and troubleshoot Deephaven through natural language conversation about features, APIs, and concepts.

The MCP Docs Server acts as a bridge between users (or client applications) and the Deephaven documentation.

```
+--------------------+
|  User/Client/API   |
+---------+----------+
          |
      HTTP/MCP
          |
+---------v----------+
|   MCP Docs Server  |
|   (FastAPI, LLM)   |
+---------+----------+
          |
  [Deephaven Docs]
```

Users or API clients send natural language questions or documentation queries over HTTP using the Model Context Protocol (MCP). These requests are received by the server, which is built on FastAPI and powered by a large language model (LLM) via the Inkeep API.

#### Docs Server Configuration

The MCP Docs Server requires an Inkeep API key for accessing documentation and generating responses. An OpenAI API key can also be used as an optional backup.

##### Environment Variables

- **`INKEEP_API_KEY`**: (Required) Your Inkeep API key for accessing the documentation assistant. This is the primary API used by the `docs_chat` tool and must be set.
- **`OPENAI_API_KEY`**: (Optional) Your OpenAI API key as a fallback option. If provided, the system will attempt to use OpenAI's services if the Inkeep API call fails, providing redundancy.
- **`PYTHONLOGLEVEL`**: (Optional) Set to 'DEBUG', 'INFO', 'WARNING', etc. to control logging verbosity. Useful for troubleshooting issues.
- **`PORT`**: (Optional) Port for the [FastAPI](https://fastapi.tiangolo.com/) HTTP server (powered by [Uvicorn](https://www.uvicorn.org/)) when using SSE transport mode (default: `8000`). This setting only affects the server when running in SSE mode and has no effect on stdio mode. It controls which port the `/health` endpoint and SSE connections will be available on.

##### Example Configuration

```sh
# Required for accessing Deephaven documentation knowledge base
export INKEEP_API_KEY=your-inkeep-api-key

# Optional for using OpenAI as a fallback
export OPENAI_API_KEY=your-openai-api-key

# Optional for detailed logging
export PYTHONLOGLEVEL=DEBUG
```

> **Security Note:** Always store API keys in environment variables or secure configuration files, never hardcode them in application code.

#### Running the Docs Server

Ensure `INKEEP_API_KEY` is set before running the Docs Server.

##### CLI Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `-t, --transport` | Transport type (`sse` or `stdio`) | `stdio` |
| `-p, --port` | Port for SSE server (overrides PORT env var) | `8000` |
| `-h, --help` | Show help message | - |

##### SSE Transport Mode

```sh
# Default port (8000)
INKEEP_API_KEY=your-api-key uv run dh-mcp-docs --transport sse

# Custom port (8001)
INKEEP_API_KEY=your-api-key uv run dh-mcp-docs --transport sse --port 8001
# or
PORT=8001 INKEEP_API_KEY=your-api-key uv run dh-mcp-docs --transport sse
```

##### stdio Transport Mode

```sh
INKEEP_API_KEY=your-api-key uv run dh-mcp-docs --transport stdio
```

> **Note:** The SSE transport is useful for interactive testing with tools like MCP Inspector, while stdio transport is better for integration with LLM platforms like Claude.

#### Docs Server Tools

The Deephaven MCP Docs Server exposes a single MCP-compatible tool:

##### `docs_chat`

- **Purpose**: Interact with the Deephaven documentation assistant using conversational natural language queries
- **Parameters**:
  - `prompt` (required): Query or question about Deephaven or its documentation as a natural language string
  - `history` (optional): Previous conversation history for context (list of messages with 'role' and 'content' keys)
    ```python
    [
        {"role": "user", "content": "How do I install Deephaven?"},
        {"role": "assistant", "content": "To install Deephaven, ..."}  
    ]
    ```
- **Returns**: String containing the assistant's response message
- **Error Handling**: If the underlying LLM API call fails, an `OpenAIClientError` is raised with a descriptive error message. Common errors include:
    - Invalid or missing API keys
    - Network connectivity issues
    - Rate limiting from the LLM provider
    - Invalid message format in history
  All errors are logged and propagated with meaningful context
- **Usage Notes**:
  - This tool is asynchronous and should be awaited when used programmatically
  - For multi-turn conversations, providing conversation history improves contextual understanding
  - Powered by Inkeep's LLM API service for retrieving documentation-specific responses

**Example (programmatic use):**
```python
from deephaven_mcp.docs._mcp import docs_chat

async def get_docs_answer():
    response = await docs_chat(
        prompt="How do I filter tables in Deephaven?",
        history=[
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi! How can I help you with Deephaven today?"}
        ]
    )
    return response
```

#### Docs Server HTTP Endpoints

**Example Usage:**
```bash
curl http://localhost:8000/health
# Response: {"status": "ok"}
```

**`/health` (GET)**
- **Purpose**: Health check endpoint for liveness and readiness probes in deployment environments
- **Parameters**: None
- **Returns**: JSON response `{"status": "ok"}` with HTTP 200 status code
- **Usage**: Used by load balancers, orchestrators, or monitoring tools to verify the server is running
- **Implementation**: Defined using `@mcp_server.custom_route("/health", methods=["GET"])` decorator in the source code
- **Availability**: Available in both SSE and stdio transport modes, but only accessible via HTTP when using SSE transport
- **Authentication**: No authentication or parameters required
- **Deployment**: Intended for use as a liveness or readiness probe in Kubernetes, Cloud Run, or similar environments
- **Note**: This endpoint is only available in the Docs Server, not in the Community Server

#### Docs Server Test Components

##### Test Client

A Python script is provided for testing the MCP Docs tool and validating server functionality without setting up a full MCP Inspector deployment. The script connects to the docs server, demonstrates calling the `docs_chat` tool with your query, and displays the response.

**Script Location**: [`../scripts/mcp_docs_test_client.py`](../scripts/mcp_docs_test_client.py)

**Arguments:**
- `--transport`: Choose `sse` or `stdio` (default: `sse`)
- `--env`: Pass environment variables as `KEY=VALUE` (can be repeated; for stdio mode)
- `--url`: URL for SSE server (default: `http://localhost:8000/sse`)
- `--stdio-cmd`: Command to launch stdio server (default: `uv run dh-mcp-docs --transport stdio`)
- `--prompt`: Prompt/question to send to the docs_chat tool (required)
- `--history`: Optional chat history (JSON string) for multi-turn conversations

**Example Usage:**
```sh
# Connect to a running SSE server
uv run scripts/mcp_docs_test_client.py --prompt "What is Deephaven?"

# Launch a new stdio server with environment variables set
uv run scripts/mcp_docs_test_client.py --transport stdio \
  --prompt "How do I create a table?" \
  --env INKEEP_API_KEY=your-inkeep-api-key \
  --env OPENAI_API_KEY=your-openai-api-key

# Multi-turn conversation with history (using JSON string for previous messages)
uv run scripts/mcp_docs_test_client.py --prompt "How do I filter this table?" \
  --history '[{"role":"user","content":"How do I create a table?"},{"role":"assistant","content":"To create a table in Deephaven..."}]'
```

> ⚠️ **Prerequisites:** 
> - For the Docs Server test client, you need a valid [Inkeep API key](https://inkeep.com/) (required)
> - An [OpenAI API key](https://openai.com/) is optional but recommended as a fallback
> - For troubleshooting API issues, see [Common Errors & Solutions](#common-errors--solutions)

> 💡 **Tips:** 
> - Replace placeholder API keys with your actual keys
> - For multi-turn conversations, the history parameter accepts properly formatted JSON
> - Use `jq` to format complex history objects: `echo '$HISTORY' | jq -c .`

---

## Integration Methods

### MCP Inspector

The [MCP Inspector](https://github.com/modelcontextprotocol/inspector) is a web-based tool for interactively exploring and testing MCP servers. It provides an intuitive UI for discovering available tools, invoking them, and inspecting responses.

#### With Community Server

1. **Start a Deephaven Community Core worker** (in one terminal):
   ```sh
   uv run scripts/run_deephaven_test_server.py --table-group simple
   ```

2. **Start the MCP Community server in SSE mode** (in another terminal):
   ```sh
   DH_MCP_CONFIG_FILE=/path/to/deephaven_workers.json uv run dh-mcp-community --transport sse
   ```

3. **Start the MCP Inspector** (in a third terminal):
   ```sh
   npx @modelcontextprotocol/inspector@latest
   ```

4. **Connect to the MCP server via SSE**:
   - Open the Inspector in your browser (URL shown in terminal, typically `http://localhost:6274`)
   - In the Inspector UI, select "Connect" and enter the SSE URL (e.g., `http://localhost:8000/sse`)
   - Explore and invoke tools like `refresh`, `worker_statuses`, `table_schemas` and `run_script`

#### With Docs Server

1. **Start the MCP Docs server in SSE mode** (in a terminal):
   ```sh
   INKEEP_API_KEY=your-api-key uv run dh-mcp-docs --transport sse
   ```

2. **Start the MCP Inspector** (in another terminal):
   ```sh
   npx @modelcontextprotocol/inspector@latest
   ```

3. **Connect to the MCP server via SSE**:
   - Open the Inspector in your browser (URL shown in terminal, typically `http://localhost:6274`)
   - In the Inspector UI, select "Connect" and enter the SSE URL (e.g., `http://localhost:8000/sse`)
   - Explore and invoke the `docs_chat` tool to ask questions about Deephaven documentation

### Claude Desktop

Claude Desktop is very useful for debugging and interactively exploring MCP servers. The configuration file format described in this documentation is also used by most AI Agents that support MCP, making it easy to reuse your setup across different tools.

#### Configuration

1. **Open Claude Desktop.**
2. **Navigate to `Settings > Developer > Edit Config`.**
3. **Edit the `claude_desktop_config.json` file.**
4. **Add your MCP server under the `mcpServers` section.**
   - We recommend using the `stdio` transport for best results.
   - Example configuration with full path (recommended):
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
         },
         "mcp-docs": {
           "command": "uv",
           "args": [
             "--directory",
             "/path/to/deephaven-mcp/mcp-community",
             "run",
             "dh-mcp-docs"
           ],
           "env": {
             "INKEEP_API_KEY": "your-inkeep-api-key"
           }
         }
       }
     }
     ```
5. **Save the configuration and restart Claude Desktop if needed.**

#### Claude Desktop Log Locations

For troubleshooting Claude Desktop MCP integration, log files are located at:
- **macOS:** `~/Library/Logs/Claude`
- **Windows:** `%APPDATA%\Claude\logs`

- `mcp.log` contains general logging about MCP connections and connection failures
- Files named `mcp-server-SERVERNAME.log` contain error (stderr) logs from each configured server

### mcp-proxy

[mcp-proxy](https://github.com/modelcontextprotocol/mcp-proxy) can bridge an MCP server's SSE endpoint to stdio for tools like Claude Desktop. This is useful when connecting to tools that don't natively support SSE. The `mcp-proxy` utility is included as a dependency in this project.

#### With Community Server

1. Ensure the MCP Community Server is running in SSE mode:
   ```sh
   DH_MCP_CONFIG_FILE=/path/to/workers.json uv run dh-mcp-community --transport sse
   ```

2. Run `mcp-proxy` to connect to your running MCP server:
   ```sh
   mcp-proxy --server-url http://localhost:8000/sse --stdio
   ```
   (Replace URL if your server runs elsewhere)

3. Configure your client (e.g., Claude Desktop) to use the stdio interface provided by `mcp-proxy`

#### With Docs Server

1. Ensure the MCP Docs Server is running in SSE mode:
   ```sh
   INKEEP_API_KEY=your-api-key uv run dh-mcp-docs --transport sse
   ```

2. Run `mcp-proxy`:
   ```sh
   mcp-proxy --server-url http://localhost:8000/sse --stdio
   ```

3. Configure your client (e.g., Claude Desktop) to use the stdio interface provided by `mcp-proxy`

### Programmatic API

Both servers can be used programmatically within Python applications:

#### Community Server Example

```python
# Import the server components
from deephaven_mcp.community import mcp_server, run_server

# Use the MCP tools directly (synchronous)
from deephaven_mcp.community._mcp import refresh, worker_statuses, table_schemas, run_script

# Example: Get status of all workers
result = worker_statuses(context)  # Requires MCP context

# Or start the server with a specific transport
run_server(transport="sse")  # Starts SSE server
```

#### Docs Server Example

```python
# Import the server components
from deephaven_mcp.docs import mcp_server, run_server

# Use the docs_chat tool directly (asynchronous)
from deephaven_mcp.docs._mcp import docs_chat

# Example: Get documentation answer
async def get_answer():
    response = await docs_chat(
        prompt="How do I filter tables in Deephaven?",
        history=[{"role": "user", "content": "Hello"}]
    )
    return response

# Or start the server with a specific transport
run_server(transport="stdio")  # Starts stdio server
```

Both servers expose their tools through FastMCP, following the Model Context Protocol.

---

## Development

<div align="center">

🛠️ *Guidelines and tools for developers working on the MCP servers* 💻

</div>

### Development Workflow

1. **Set up your environment**:
   ```sh
   # Clone the repository
   git clone https://github.com/deephaven/deephaven-mcp.git
   cd deephaven-mcp/mcp-community
   
   # Install dependencies with uv
   uv pip install -e ".[dev]"
   ```
   
   > [UV](https://github.com/astral-sh/uv) is a fast Python package installer and resolver, but you can also use regular `pip install -e .` if preferred.

2. **Run the test server** (in one terminal):
   ```sh
   uv run scripts/run_deephaven_test_server.py --table-group simple
   ```

3. **Run the MCP Community server** (in another terminal):
   ```sh
   DH_MCP_CONFIG_FILE=/path/to/workers.json uv run dh-mcp-community --transport sse
   ```

4. **Use the MCP Inspector or test client** to validate your changes.

### Advanced Development Techniques

- **Run the server directly (development mode):**
  ```sh
  PYTHONPATH=src uv run mcp dev src/deephaven_mcp/community/_mcp.py:mcp_server
  ```
  This command starts the MCP server for local development with advanced debugging capabilities. You can specify different entrypoints as needed.

- **Interactive Tools:**
  Use the Inspector or the test client for interactive tool calls and debugging during development.

- **Code Style & Linting:**
  ```sh
  # Sort imports with isort
  uv run isort . --skip _version.py --skip .venv
  
  # Format code with black
  uv run black . --exclude '(_version.py|.venv)'
  
  # Lint code with ruff
  uv run ruff check src --fix --exclude _version.py --exclude .venv
  ```
  
  > The project follows Python best practices using [isort](https://pycqa.github.io/isort/), [black](https://black.readthedocs.io/), and [ruff](https://beta.ruff.rs/docs/) for code quality.

- **Type Checking:**
  ```sh
  # Run static type checking with mypy
  uv run mypy src/
  ```
  
  > [mypy](https://mypy.readthedocs.io/) provides static type checking for Python code.

- **Pre-commit Hooks:**
  For automatic linting and formatting before each commit using [pre-commit](https://pre-commit.com/):
  ```sh
  uv pip install pre-commit
  pre-commit install
  ```
  
  > This sets up Git hooks that automatically run code formatters and linters before each commit.

### Development Commands

```sh
# Run tests with pytest
uv run pytest  # Runs all unit and integration tests

# Run code style and lint checks

# Sort imports (fixes in place)
uv run isort . --skip _version.py --skip .venv
# Check import sorting only (no changes)
uv run isort . --check-only --diff --skip _version.py --skip .venv

# Format code (fixes in place)
uv run black .
# Check formatting only (no changes)
uv run black . --check

# Run pylint
uv run pylint src tests
```

### Project Structure

The codebase is organized as follows:

```
deephaven-mcp/
├── src/
│   └── deephaven_mcp/
│       ├── config.py             # Configuration management and validation
│       ├── openai.py             # OpenAI API client for LLM integration
│       ├── community/            # Community Server module
│       │   ├── __init__.py       # Server entrypoint and CLI interface
│       │   ├── _mcp.py           # MCP tool implementations
│       │   └── _sessions.py      # Session management for Deephaven workers
│       └── docs/                 # Docs Server module
│           ├── __init__.py       # Server entrypoint and CLI interface
│           ├── server.py         # FastAPI and FastMCP server setup
│           └── _mcp.py           # MCP tools (docs_chat) implementation
├── tests/                        # Unit and integration tests
│   └── ...
├── scripts/                      # Utility scripts for dev and testing
│   ├── run_deephaven_test_server.py
│   ├── mcp_community_test_client.py
│   ├── mcp_docs_test_client.py
│   └── mcp_docs_stress_sse.py
├── docker/
│   └── mcp-docs/
│       ├── Dockerfile            # Dockerfile for the MCP Docs server
│       ├── docker-compose.yml    # Docker Compose config for the MCP Docs server
│       └── README.md             # Docker usage notes for the MCP Docs server
├── docs/                         # Documentation
│   └── DEVELOPER_GUIDE.md        # This developer & contributor guide
├── pyproject.toml                # Package metadata and dependencies
└── README.md                     # Main user-facing README
```

#### Script References

The project includes several utility scripts to help with development and testing:

| Script | Purpose | Usage |
|--------|---------|-------|
| [`../scripts/run_deephaven_test_server.py`](../scripts/run_deephaven_test_server.py) | Starts a local Deephaven server for testing | `uv run scripts/run_deephaven_test_server.py --table-group simple` |
| [`../scripts/mcp_community_test_client.py`](../scripts/mcp_community_test_client.py) | Tests the Community Server tools | `uv run scripts/mcp_community_test_client.py --transport sse` |
| [`../scripts/mcp_docs_test_client.py`](../scripts/mcp_docs_test_client.py) | Tests the Docs Server chat functionality | `uv run scripts/mcp_docs_test_client.py --prompt "What is Deephaven?"` |
| [`../scripts/mcp_docs_stress_sse.py`](../scripts/mcp_docs_stress_sse.py) | Stress tests the SSE endpoint | `uv run scripts/mcp_docs_stress_sse.py --sse-url "http://localhost:8000/sse"` |

### Dependencies

All dependencies are managed in the [pyproject.toml](../pyproject.toml) file, which includes:

- Core runtime dependencies for async I/O, MCP protocol, Deephaven integration, and LLM APIs
- Development dependencies for testing, code quality, and CI

These dependencies are automatically installed when using `pip install -e .` or [uv](https://github.com/astral-sh/uv) `pip install -e .`. For the complete list, refer to the `dependencies` and `optional-dependencies` sections in [pyproject.toml](../pyproject.toml).

### Versioning

This package uses [setuptools-scm](https://github.com/pypa/setuptools_scm) for dynamic versioning based on git tags. Version information is automatically generated during the build process and stored in `src/deephaven_mcp/_version.py`. This file should not be manually edited or tracked in version control.

### Docker Compose

A [Docker Compose](https://docs.docker.com/compose/) configuration for the MCP Docs server is provided at [`docker/mcp-docs/docker-compose.yml`](../docker/mcp-docs/docker-compose.yml):

```sh
# Start the MCP Docs server
docker compose -f docker/mcp-docs/docker-compose.yml up --build

# View logs
docker compose -f docker/mcp-docs/docker-compose.yml logs -f

# Stop services
docker compose -f docker/mcp-docs/docker-compose.yml down
```

> **Note:** The build context is the repo root, so all code/assets are accessible to the Dockerfile. Other services may have their own Compose files under the `docker/` directory.

### Performance Testing

A script is provided for stress testing the SSE transport for production deployments. This is useful for validating the stability and performance of production or staging deployments under load. The script uses [aiohttp](https://docs.aiohttp.org/) for asynchronous HTTP requests and [aiolimiter](https://github.com/mjpieters/aiolimiter) for rate limiting.

#### Usage Example

The [`../scripts/mcp_docs_stress_sse.py`](../scripts/mcp_docs_stress_sse.py) script can be used to stress test the SSE endpoint:

```sh
uv run scripts/mcp_docs_stress_sse.py \
    --concurrency 10 \
    --requests-per-conn 100 \
    --sse-url "http://localhost:8000/sse" \
    --max-errors 5 \
    --rps 10 \
    --max-response-time 2
```

#### Arguments

- `--concurrency`: Number of concurrent connections (default: 100)
- `--requests-per-conn`: Number of requests per connection (default: 100)
- `--sse-url`: Target SSE endpoint URL
- `--max-errors`: Maximum number of errors before stopping the test (default: 5)
- `--rps`: Requests per second limit per connection (default: 0, no limit)
- `--max-response-time`: Maximum allowed response time in seconds (default: 1)

The script will create multiple concurrent connections and send requests to the specified SSE endpoint, reporting errors and response times. It will print "PASSED" if the test completes without exceeding the error threshold, or "FAILED" with the reason if the error threshold is reached.

---

## Troubleshooting

<div align="center">

🔍 *Common issues and their solutions to help you quickly resolve problems* 🔧

</div>

### Common Issues

1. **Worker Configuration Errors**:
   - The worker configuration must be valid JSON with no comments
   - All required fields must be present for each worker
   - Unknown fields in the configuration will cause validation errors
   - Check error messages for specific validation issues

2. **API Key Issues**:
   - Ensure your Inkeep API key is valid and active
   - Verify the API key is properly set in environment variables
   - Check for typos in key names or values

3. **SSE Connection Failures**:
   - Verify the SSE server is running on the expected port
   - Check for firewall or network issues
   - Ensure the client is using the correct URL

4. **Deephaven Worker Connectivity**:
   - Confirm the Deephaven server is running and accessible
   - Verify that the worker configuration has the correct host/port
   - Check for authentication issues if using secured connections

5. **Environment Variable Problems**:
   - Make sure `DH_MCP_CONFIG_FILE` points to a valid, readable file
   - The value should be an absolute path for reliability across different working directories
   - When using `.env` files, verify they're properly loaded (the application uses `python-dotenv`)

6. **Debug with Logging**:
   - Set `PYTHONLOGLEVEL=DEBUG` for more detailed logs
   - For SSE mode, logs appear in the terminal
   - For stdio mode, logs are sent to stderr, which may require redirection
   - The server automatically redacts sensitive fields (auth_token, binary credentials) in logs

### Common Errors & Solutions

1. **Config File Not Found:**
   - Ensure `DH_MCP_CONFIG_FILE` points to a valid JSON file with absolute path
   - Example error: `FileNotFoundError: No such file or directory: ...`
   - Fix: Verify the file path and permissions

2. **Invalid JSON/Schema in Config:**
   - Double-check your worker config file for syntax errors or unsupported fields
   - Use a JSON validator if unsure about the format
   - Common errors: missing commas, unquoted keys, trailing commas

3. **Port Already in Use:**
   - Change the port in your config or ensure no other process is using it
   - Example error: `OSError: [Errno 98] Address already in use`
   - Fix: Use a different port or stop the process using the current port

4. **Connection Timeouts:**
   - Check that Deephaven workers are running and reachable
   - Verify network connectivity between MCP server and workers
   - If using TLS, ensure certificates are valid and trusted

5. **Transport Issues:**
   - Verify you are using the correct transport and URL/command
   - For SSE, ensure ports are open and not firewalled
   - For stdio, check the command path and environment variables

6. **Missing Dependencies:**
   - Ensure all Python dependencies are installed (`uv pip install .[dev]`)
   - Java must be installed and in PATH for running Deephaven test servers

7. **Session Errors:**
   - Review logs for session cache or connection errors
   - Try refreshing the session with the `refresh` tool

---

## Resources

<div align="center">

📖 *Additional documentation, references, and tools to support your work* 📚

</div>

### Documentation

- [Model Context Protocol (MCP) Specification](https://github.com/modelcontextprotocol/spec)
- [Deephaven Documentation](https://deephaven.io/docs/)
- [Inkeep API Documentation](https://inkeep.com/docs)

### Deephaven API Reference

- [Deephaven Python Client API](https://docs.deephaven.io/core/client-api/python/): Main Python client documentation
- [Table API Reference](https://docs.deephaven.io/core/client-api/python/table/): For working with Deephaven tables
- [Query API Reference](https://docs.deephaven.io/core/client-api/python/query/): For formulating Deephaven queries
- [Formula API Reference](https://docs.deephaven.io/core/client-api/python/formula/): For creating Deephaven formulas
- [Session API Reference](https://docs.deephaven.io/core/client-api/python/session/): For managing Deephaven sessions

### Tools & Related Projects

- [MCP Inspector](https://github.com/modelcontextprotocol/inspector) - Interactive UI for exploring MCP servers
- [MCP Proxy](https://github.com/modelcontextprotocol/mcp-proxy) - Bridge from SSE to stdio transport
- [FastMCP](https://github.com/jlowin/fastmcp) - Python library for building MCP servers
- [FastMCP Tutorial](https://www.firecrawl.dev/blog/fastmcp-tutorial-building-mcp-servers-python) - Guide to building MCP servers with Python
- [autogen-ext](https://github.com/jlowin/autogen-ext) - Extensions for AutoGen including MCP support
- [Model Context Protocol (MCP)](https://github.com/modelcontextprotocol) - Main MCP organization with specs and tools

### Contributing

- [Contributing Guidelines](../CONTRIBUTING.md) - Guide for making contributions to the project
- [GitHub Issues](https://github.com/deephaven/deephaven-mcp/issues) - Report bugs or request features
- [Pull Requests](https://github.com/deephaven/deephaven-mcp/pulls) - View open changes and contribute your own

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](../LICENSE) file for details.
