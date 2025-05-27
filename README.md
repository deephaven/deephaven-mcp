# Deephaven MCP

[![PyPI](https://img.shields.io/pypi/v/deephaven-mcp)](https://pypi.org/project/deephaven-mcp/)
[![License](https://img.shields.io/github/license/deephaven/deephaven-mcp)](https://github.com/deephaven/deephaven-mcp/blob/main/LICENSE)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/deephaven/deephaven-mcp/ci.yml?branch=main)

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation & Initial Setup](#installation--initial-setup)
- [Configure MCP Server's Access to Deephaven Community Core](#configure-mcp-servers-access-to-deephaven-community-core)
- [Configure Your LLM Tool to Use MCP Servers](#configure-your-llm-tool-to-use-mcp-servers)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [Advanced Usage & Further Information](#advanced-usage--further-information)
- [Community & Support](#community--support)
- [License](#license)

---

## Overview

Deephaven MCP, which implements the [Model Context Protocol (MCP) standard](https://spec.modelcontextprotocol.io/), provides tools to orchestrate, inspect, and interact with [Deephaven Community Core](https://deephaven.io/) servers, and to access conversational documentation via LLM-powered Docs Servers. It's designed for data scientists, engineers, and anyone looking to leverage Deephaven's capabilities through programmatic interfaces or integrated LLM tools.

### What is Deephaven MCP?

Deephaven MCP consists of two main server components:

*   **Community Server**: Manages and connects to multiple [Deephaven Community Core](https://deephaven.io/) worker nodes. This allows for unified control and interaction with your Deephaven instances from various client applications.
*   **Docs Server**: Provides access to an LLM-powered conversational Q&A interface for Deephaven documentation. Get answers to your Deephaven questions in natural language.

### Key Use Cases

*   Integrate Deephaven with LLM-powered development tools (e.g., [Claude Desktop](https://www.anthropic.com/claude), [GitHub Copilot](https://github.com/features/copilot)) for AI-assisted data exploration, code generation, and analysis.
*   Programmatically manage and query multiple Deephaven worker nodes.
*   Quickly find information and examples from Deephaven documentation using natural language queries.

### Architecture Diagrams

#### Community Server Architecture

```mermaid
graph TD
    A[Clients: MCP Inspector / Claude Desktop / etc.] -- SSE/stdio (MCP) --> B(MCP Community Server);
    B -- Manages --> C(Deephaven Core Worker 1);
    B -- Manages --> D(Deephaven Core Worker N);
```
*Clients connect to the [MCP Community Server](#community-server), which in turn manages and communicates with one or more [Deephaven Community Core](https://deephaven.io/) workers.*

#### Docs Server Architecture

```mermaid
graph TD
    A[User/Client/API e.g., Claude Desktop] -- stdio (MCP) --> PROXY(mcp-proxy);
    PROXY -- HTTP (SSE) --> B(MCP Docs Server - FastAPI, LLM);
    B -- Accesses --> C[Deephaven Documentation Corpus];
```
*LLM tools and other stdio-based clients connect to the [Docs Server](#docs-server) via the [`mcp-proxy`](https://github.com/modelcontextprotocol/mcp-proxy), which forwards requests to the main HTTP/SSE-based Docs Server.*

---

## Prerequisites

*   **Git**: For cloning the repository. ([Installation Guide](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git))
*   **Python**: Version 3.9 or later. ([Download Python](https://www.python.org/downloads/))
*   **Access to [Deephaven Community Core](https://deephaven.io/) instance(s):** To use the [MCP Community Server](#community-server) for interacting with Deephaven, you will need one or more [Deephaven Community Core](https://deephaven.io/) instances running and network-accessible.
*   **Choose your Python environment setup method:**
    *   **Option A: `uv` (Recommended)**: A very fast Python package installer and resolver. If you don't have it, you can install it via `pip install uv` or see the [uv installation guide](https://github.com/astral-sh/uv#installation).
    *   **Option B: Standard Python `venv` and `pip`**: Uses Python's built-in [virtual environment (`venv`)](https://docs.python.org/3/library/venv.html) tools and [`pip`](https://pip.pypa.io/en/stable/getting-started/).

---

## Installation & Initial Setup

### Clone the Repository

First, clone the `deephaven-mcp` repository to your local machine:

```sh
git clone https://github.com/deephaven/deephaven-mcp.git
cd deephaven-mcp
```
*Note: The main server packages and their [`pyproject.toml`](pyproject.toml) are located in this `deephaven-mcp` root directory. Commands below are run from this root directory.*

### Set Up Environment and Install Dependencies

Choose one of the following options based on your preference from the [Prerequisites](#prerequisites) section.

#### Option A: Using `uv` (Recommended)

If you have [`uv`](docs/UV.md) installed, run the following command from the `deephaven-mcp` (root) directory:

```sh
uv pip install ".[dev]"
```
This command creates a virtual environment (if one isn't active and `UV_AUTO_CREATE_VENV` is not `false`) and installs all necessary dependencies, including `dh-mcp-community`, `dh-mcp-docs`, and [`mcp-proxy`](https://github.com/modelcontextprotocol/mcp-proxy) if they are specified as project dependencies or extras.
If [`uv`](docs/UV.md) creates or uses a virtual environment (typically `.venv` in the project root), remember to activate it (e.g., `source .venv/bin/activate` or `.venv\Scripts\activate`) in any new terminal session for manual command-line operations or if your LLM tool doesn't manage environment activation.

#### Option B: Using `pip` and `venv`

1.  **Create a virtual environment** (e.g., named `.venv`) in the `deephaven-mcp` (root) directory:
    ```sh
    python -m venv .venv
    ```
2.  **Activate the virtual environment:**
    *   On macOS/Linux:
        ```sh
        source .venv/bin/activate
        ```
    *   On Windows (Command Prompt/PowerShell):
        ```sh
        .venv\Scripts\activate
        ```
3.  **Install dependencies** into the activated virtual environment (from the `deephaven-mcp` root directory):
    ```sh
    pip install ".[dev]"
    ```
    Remember to activate this virtual environment (`source .venv/bin/activate` or `.venv\Scripts\activate`) in any new terminal session where you intend to run these tools manually or if your LLM tool doesn't manage the environment activation itself when spawning processes.

---

## Configure MCP Server's Access to Deephaven Community Core

This section explains how to configure the [Deephaven MCP Community Server](#community-server) to connect to and manage your [Deephaven Community Core](https://deephaven.io/) instances. This involves creating a [worker definition file](#the-deephaven_workersjson-file-defining-your-core-workers) and understanding how the server locates this file.

### The `deephaven_workers.json` File (Defining Your Core Workers)

#### Purpose and Structure

The [Deephaven MCP Community Server](#community-server) requires a JSON configuration file that describes the [Deephaven Community Core](https://deephaven.io/) worker instances it can connect to. 

*   The file must be a JSON object with a top-level key named `"workers"`.
*   The value of `"workers"` is an object where each key is a unique worker name (e.g., `"local_worker"`, `"prod_cluster_1"`) and the value is a configuration object for that worker.

#### Worker Configuration Fields

*All fields are optional. Default values are applied by the server if a field is omitted.*

*   `host` (string): Hostname or IP address of the [Deephaven Community Core](https://deephaven.io/) worker (e.g., `"localhost"`).
*   `port` (integer): Port number for the worker connection (e.g., `10000`).
*   `auth_type` (string): Authentication type. Supported values include:
    *   `"token"`: For token-based authentication.
    *   `"basic"`: For username/password authentication (use `auth_token` for `username:password` or see server docs for separate fields if supported).
    *   `"anonymous"`: For no authentication.
*   `auth_token` (string): The authentication token if `auth_type` is `"token"`. For `"basic"` auth, this is typically the password, or `username:password` if the server expects it combined. Consult your [Deephaven server's authentication documentation](https://deephaven.io/core/docs/how-to-guides/authentication/auth-uname-pw/) for specifics.
*   `never_timeout` (boolean): If `true`, the MCP server will attempt to configure the session to this worker to never time out. Server-side configurations may still override this.
*   `session_type` (string): Specifies the type of session to create. Common values are `"groovy"` or `"python"`.
*   `use_tls` (boolean): Set to `true` if the connection to the worker requires TLS/SSL.
*   `tls_root_certs` (string): Absolute path to a PEM file containing trusted root CA certificates for TLS verification. If omitted, system CAs might be used, or verification might be less strict depending on the client library.
*   `client_cert_chain` (string): Absolute path to a PEM file containing the client's TLS certificate chain. Used for client-side certificate authentication (mTLS).
*   `client_private_key` (string): Absolute path to a PEM file containing the client's private key. Used for client-side certificate authentication (mTLS).

#### Example `deephaven_workers.json`

```json
{
  "workers": {
    "my_local_deephaven": {
      "host": "localhost",
      "port": 10000
    },
    "secure_remote_worker": {
      "host": "secure.deephaven.example.com",
      "port": 10001,
      "auth_type": "token",
      "auth_token": "your-secret-api-token-here",
      "use_tls": true,
      "tls_root_certs": "/path/to/root.crt",
      "client_cert_chain": "/path/to/client.crt",
      "client_private_key": "/path/to/client.key"
    }
  }
}
```

#### Security Note for `deephaven_workers.json`

The `deephaven_workers.json` file can contain sensitive information such as authentication tokens, usernames, and passwords. Ensure that this file is protected with appropriate filesystem permissions to prevent unauthorized access. For example, on Unix-like systems (Linux, macOS), you can restrict permissions to the owner only using the command: 

```bash
chmod 600 /path/to/your/deephaven_workers.json
```

#### Additional Notes for `deephaven_workers.json`

*   Ensure all file paths within the config (e.g., for TLS certificates if used) are absolute and accessible by the server process.
*   The worker names are arbitrary and used to identify workers in client tools.

### Setting `DH_MCP_CONFIG_FILE` (Informing the MCP Server)

The `DH_MCP_CONFIG_FILE` environment variable tells the [Deephaven MCP Community Server](#community-server) where to find your `deephaven_workers.json` file (detailed in [The `deephaven_workers.json` File (Defining Your Core Workers)](#the-deephaven_workersjson-file-defining-your-core-workers)). You will set this environment variable as part of the server launch configuration within your LLM tool, as detailed in the [Configure Your LLM Tool to Use MCP Servers](#configure-your-llm-tool-to-use-mcp-servers) section. 

When launched by an LLM tool, the [MCP Community Server](#community-server) process reads this variable to load your worker definitions. For general troubleshooting or if you need to set other environment variables like `PYTHONLOGLEVEL` (e.g., to `DEBUG` for verbose logs), these are also typically set within the LLM tool's MCP server configuration (see [Defining MCP Servers for Your LLM Tool (The `mcpServers` JSON Object)](#defining-mcp-servers-for-your-llm-tool-the-mcpservers-json-object)).

---

## Configure Your LLM Tool to Use MCP Servers

This section details how to configure your LLM tool (e.g., [Claude Desktop](https://www.anthropic.com/claude), [GitHub Copilot](https://github.com/features/copilot)) to launch and communicate with the [Deephaven MCP Community Server](#community-server) and the [Deephaven MCP Docs Server](#docs-server). This involves providing a JSON configuration, known as the [`"mcpServers"` object](#defining-mcp-servers-for-your-llm-tool-the-mcpservers-json-object), to your LLM tool.

### How LLM Tools Launch MCP Servers (Overview)

LLM tools that support the Model Context Protocol (MCP) can be configured to use the Deephaven MCP Community and Docs Servers. The LLM tool's configuration will typically define how to *start* the necessary MCP server processes.

### Understanding Deephaven Core Worker Status (via MCP)

The [MCP Community Server](#community-server), launched by your LLM tool, will attempt to connect to the [Deephaven Community Core](https://deephaven.io/) instances defined in your `deephaven_workers.json` file (pointed to by `DH_MCP_CONFIG_FILE` as described in [Setting `DH_MCP_CONFIG_FILE` (Informing the MCP Server)](#setting-dh_mcp_config_file-informing-the-mcp-server)).

It's important to understand the following:
*   **MCP Server Independence**: The [MCP Community Server](#community-server) itself will start and be available to your LLM tool even if some or all configured [Deephaven Community Core](https://deephaven.io/) workers are not currently running or accessible. The LLM tool will be able to list the configured workers and see their status (e.g., unavailable, connected).
*   **Worker Interaction**: To successfully perform operations on a specific [Deephaven Community Core](https://deephaven.io/) worker (e.g., list tables, execute scripts), that particular worker must be running and network-accessible from the environment where the [MCP Community Server](#community-server) process is executing.
*   **Configuration is Key**: Ensure your `deephaven_workers.json` file accurately lists the workers you intend to use. The MCP server uses this configuration to know which workers to attempt to manage.

### Defining MCP Servers for Your LLM Tool (The `mcpServers` JSON Object)

Your LLM tool requires a specific JSON configuration to define how MCP servers are launched. This configuration is structured as a JSON object with a top-level key named `"mcpServers"`. This `"mcpServers"` object tells the tool how to start the [Deephaven MCP Community Server](#community-server) (for interacting with [Deephaven Community Core](https://deephaven.io/)) and the `mcp-proxy` (for interacting with the [Docs Server](#docs-server)).

Depending on your LLM tool, this `"mcpServers"` object might be:
*   The entire content of a dedicated file (e.g., named `mcp.json` in VS Code).
*   A part of a larger JSON configuration file used by the tool (e.g., for [Claude Desktop](https://www.anthropic.com/claude)).

Consult your LLM tool's documentation for the precise file name and location. Below are two examples of the `"mcpServers"` JSON structure. Choose the one that matches your Python environment setup (either [`uv`](docs/UV.md) or `pip + venv`).

**Important: All paths in the JSON examples (e.g., `/full/path/to/...`) must be replaced with actual, absolute paths on your system.**

#### Example `"mcpServers"` object for `uv` users:

```json
{
  "mcpServers": {
    "deephaven-community": {
      "command": "uv",
      "args": [
        "--directory",
        "/full/path/to/deephaven-mcp",
        "run",
        "dh-mcp-community"
      ],
      "env": {
        "DH_MCP_CONFIG_FILE": "/full/path/to/your/deephaven_workers.json",
        "PYTHONLOGLEVEL": "INFO" 
      }
    },
    "deephaven-docs": {
      "command": "uv",
      "args": [
        "--directory",
        "/full/path/to/deephaven-mcp",
        "run",
        "mcp-proxy",
        "https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/sse"
      ]
    }
  }
}
```
*Note: You can change `"PYTHONLOGLEVEL": "INFO"` to `"PYTHONLOGLEVEL": "DEBUG"` for more detailed server logs, as further detailed in the [Troubleshooting section](#troubleshooting).*

#### Example `"mcpServers"` object for `pip + venv` users:

```json
{
  "mcpServers": {
    "deephaven-community": {
      "command": "/full/path/to/your/deephaven-mcp/.venv/bin/dh-mcp-community",
      "args": [], 
      "env": {
        "DH_MCP_CONFIG_FILE": "/full/path/to/your/deephaven_workers.json",
        "PYTHONLOGLEVEL": "INFO"
      }
    },
    "deephaven-docs": {
      "command": "/full/path/to/your/deephaven-mcp/.venv/bin/mcp-proxy",
      "args": [
        "https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/sse"
      ]
    }
  }
}
```
*Note: You can change `"PYTHONLOGLEVEL": "INFO"` to `"PYTHONLOGLEVEL": "DEBUG"` for more detailed server logs, as further detailed in the [Troubleshooting section](#troubleshooting).*

### Tool-Specific File Locations for the `mcpServers` Configuration

The `"mcpServers"` JSON object, whose structure is detailed in [Defining MCP Servers for Your LLM Tool (The `mcpServers` JSON Object)](#defining-mcp-servers-for-your-llm-tool-the-mcpservers-json-object), needs to be placed in a specific configuration file or setting area for your LLM tool. Here’s how to integrate it with common tools:

*   **[Claude Desktop](https://www.anthropic.com/claude):**
    *   The `mcpServers` object should be added to the main JSON object within this file:
    *   macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
    *   Windows: `%APPDATA%\Claude\claude_desktop_config.json` (e.g., `C:\Users\<YourUsername>\AppData\Roaming\Claude\claude_desktop_config.json`)
    *   Linux: `~/.config/Claude/claude_desktop_config.json`
*   **[GitHub Copilot](https://github.com/features/copilot) ([Visual Studio Code](https://code.visualstudio.com/)):**
    *   In your project's root directory, create or edit the file `.vscode/mcp.json`.
    *   This file's content should be the `"mcpServers"` JSON object, as shown in the examples in [Defining MCP Servers for Your LLM Tool (The `mcpServers` JSON Object)](#defining-mcp-servers-for-your-llm-tool-the-mcpservers-json-object).
*   **[GitHub Copilot](https://github.com/features/copilot) ([JetBrains IDEs](https://www.jetbrains.com/products/#type=ide) - [IntelliJ IDEA](https://www.jetbrains.com/idea/), [PyCharm](https://www.jetbrains.com/pycharm/), etc.):**
    *   The method for configuring custom MCP servers may vary. Please consult the official [GitHub Copilot](https://github.com/features/copilot) extension documentation for your specific JetBrains IDE for the most current instructions. It might involve a specific settings panel or a designated configuration file.

### Restarting Your LLM Tool (Applying the Configuration)

Once you have saved the `"mcpServers"` JSON object in the correct location for your LLM tool, **restart the tool** ([Claude Desktop](https://www.anthropic.com/claude), [VS Code](https://code.visualstudio.com/), [JetBrains IDEs](https://www.jetbrains.com/products/#type=ide), etc.). The configured servers (e.g., `deephaven-community`, `deephaven-docs`) should then be available in its MCP interface.

### Verifying Your Setup

After restarting your LLM tool, the first step is to verify that the MCP servers are recognized:

*   Open your LLM tool's interface where it lists available MCP servers or data sources.
*   You should see `deephaven-community` and `deephaven-docs` (or the names you configured in the `mcpServers` object) listed.
*   Attempt to connect to or interact with one of them (e.g., by listing available [Deephaven Community Core](https://deephaven.io/) workers via the `deephaven-community` server).

If the servers are not listed or you encounter errors at this stage, please proceed to the [Troubleshooting](#6-troubleshooting) section for guidance.

---

## Troubleshooting

*   **LLM Tool Can't Connect / Server Not Found:**
    *   Verify all paths in your LLM tool's JSON configuration are **absolute and correct**.
    *   Ensure `DH_MCP_CONFIG_FILE` environment variable is correctly set in the JSON config and points to a valid worker file.
    *   Ensure any [Deephaven Community Core](https://deephaven.io/) workers you intend to use (as defined in `deephaven_workers.json`) are running and accessible from the [MCP Community Server](#community-server)'s environment.
    *   Check for typos in server names, commands, or arguments in the JSON config.
    *   Validate the syntax of your JSON configurations (`mcpServers` object in the LLM tool, and `deephaven_workers.json`). A misplaced comma or incorrect quote can prevent the configuration from being parsed correctly. Use a [JSON validator tool](https://jsonlint.com/) or your IDE's linting features.
        *   Set `PYTHONLOGLEVEL=DEBUG` in the `env` block of your JSON config to get more detailed logs from the MCP servers. For example, [Claude Desktop](https://www.anthropic.com/claude) often saves these to files like `~/Library/Logs/Claude/mcp-server-SERVERNAME.log`. Consult your LLM tool's documentation for specific log file locations.
*   **Firewall or Network Issues:**
        *   Ensure that there are no firewall rules (local or network) preventing:
            *   The [MCP Community Server](#community-server) from connecting to your [Deephaven Community Core](https://deephaven.io/) instances on their specified hosts and ports.
            *   Your LLM tool or client from connecting to the `mcp-proxy`'s target URL (`[https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io](https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io)`) if using the [Docs Server](#docs-server).
        *   Test basic network connectivity (e.g., using [`ping`](https://en.wikipedia.org/wiki/Ping_(networking_utility)) or [`curl`](https://curl.se/docs/manpage.html) from the relevant machine) if connections are failing.
*   **`command not found` for [`uv`](docs/UV.md) (in LLM tool logs):**
    *   Ensure [`uv`](docs/UV.md) is installed and its installation directory is in your system's `PATH` environment variable, accessible by the LLM tool.
*   **`command not found` for `dh-mcp-community` or [`mcp-proxy`](https://github.com/modelcontextprotocol/mcp-proxy) (venv option in LLM tool logs):**
    *   Double-check that the `command` field in your JSON config uses the **correct absolute path** to the executable within your `.venv/bin/` (or `.venv\Scripts\`) directory.
*   **Port Conflicts:** If a server fails to start (check logs), another process might be using the required port (e.g., port 8000 for default SSE).
*   **Python Errors in Server Logs:** Check the server logs for Python tracebacks. Ensure all dependencies were installed correctly (see [Set Up Environment and Install Dependencies](#set-up-environment-and-install-dependencies)).
*   **Worker Configuration Issues:**
        *   If the [Community Server](#community-server) starts but can't connect to [Deephaven Community Core](https://deephaven.io/) workers, verify your `deephaven_workers.json` file (see [The `deephaven_workers.json` File (Defining Your Core Workers)](#the-deephaven_workersjson-file-defining-your-core-workers) for details on its structure and content).
        *   Ensure the target [Deephaven Community Core](https://deephaven.io/) instances are running and network-accessible.
        *   Confirm that the process running the [MCP Community Server](#community-server) has read permissions for the `deephaven_workers.json` file itself.

---

## Contributing

We warmly welcome contributions to Deephaven MCP! Whether it's bug reports, feature suggestions, documentation improvements, or code contributions, your help is valued.

*   **Reporting Issues:** Please use the [GitHub Issues](https://github.com/deephaven/deephaven-mcp/issues) tracker.
*   **Development Guidelines:** For details on setting up your development environment, coding standards, running tests, and the pull request process, please see our [Developer & Contributor Guide](docs/DEVELOPER_GUIDE.md).

---
## Advanced Usage & Further Information


*   **Detailed Server APIs and Tools:** For in-depth information about the tools exposed by the [Community Server](#community-server) (e.g., [`refresh`](docs/DEVELOPER_GUIDE.md#refresh), [`describe_workers`](docs/DEVELOPER_GUIDE.md#describe_workers)) and the [Docs Server](#docs-server) ([`docs_chat`](docs/DEVELOPER_GUIDE.md#docs_chat)), refer to the [Developer & Contributor Guide](docs/DEVELOPER_GUIDE.md).
*   **`uv` Workflow:** For more details on using `uv` for project management, see [docs/UV.md](docs/UV.md).

---

## Community & Support

*   **GitHub Issues:** For bug reports and feature requests: [https://github.com/deephaven/deephaven-mcp/issues](https://github.com/deephaven/deephaven-mcp/issues)
*   **Deephaven Community Slack:** Join the conversation and ask questions: [https://deephaven.io/slack](https://deephaven.io/slack)

---

## License

This project is licensed under the [Apache 2.0 License](./LICENSE). See the [LICENSE](LICENSE) file for details.
