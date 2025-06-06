# deephaven-mcp

[![PyPI](https://img.shields.io/pypi/v/deephaven-mcp)](https://pypi.org/project/deephaven-mcp/)
[![License](https://img.shields.io/github/license/deephaven/deephaven-mcp)](https://github.com/deephaven/deephaven-mcp/blob/main/LICENSE)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/deephaven/deephaven-mcp/ci.yml?branch=main)

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation & Initial Setup](#installation--initial-setup)
- [Configuring `deephaven_mcp.json`](#configuring-deephaven_mcpjson)
- [Configure Your LLM Tool to Use MCP Servers](#configure-your-llm-tool-to-use-mcp-servers)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [Advanced Usage & Further Information](#advanced-usage--further-information)
- [Community & Support](#community--support)
- [License](#license)

---

## Overview

Deephaven MCP, which implements the [Model Context Protocol (MCP) standard](https://spec.modelcontextprotocol.io/), provides tools to orchestrate, inspect, and interact with [Deephaven Community Core](https://deephaven.io/) servers, and to access conversational documentation via LLM-powered Docs Servers. It's designed for data scientists, engineers, and anyone looking to leverage Deephaven's capabilities through programmatic interfaces or integrated LLM tools.

### Deephaven MCP Components

#### Community Server
Manages and connects to multiple [Deephaven Community Core](https://deephaven.io/) worker nodes. This allows for unified control and interaction with your Deephaven instances from various client applications.

#### Docs Server
Provides access to an LLM-powered conversational Q&A interface for Deephaven documentation. Get answers to your Deephaven questions in natural language.

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

*   **Python**: Version 3.9 or later. ([Download Python](https://www.python.org/downloads/))
*   **Access to [Deephaven Community Core](https://deephaven.io/) instance(s):** To use the [MCP Community Server](#community-server) for interacting with Deephaven, you will need one or more [Deephaven Community Core](https://deephaven.io/) instances running and network-accessible.
*   **Choose your Python environment setup method:**
    *   **Option A: `uv` (Recommended)**: A very fast Python package installer and resolver. If you don't have it, you can install it via `pip install uv` or see the [uv installation guide](https://github.com/astral-sh/uv#installation).
    *   **Option B: Standard Python `venv` and `pip`**: Uses Python's built-in [virtual environment (`venv`)](https://docs.python.org/3/library/venv.html) tools and [`pip`](https://pip.pypa.io/en/stable/getting-started/).

---

## Installation & Initial Setup

The recommended way to install `deephaven-mcp` is from PyPI. This provides the latest stable release and is suitable for most users.

### Installing from PyPI (Recommended for Users)

Choose one of the following Python environment and package management tools:

#### Option A: Using `uv` (Fast, Recommended)

If you have [`uv`](docs/UV.md) installed (or install it via `pip install uv`):

1.  **Create and activate a virtual environment with your desired Python version:**
    [uv](docs/UV.md) works best when operating within a virtual environment. To create one (e.g., named `.venv`) using a specific Python interpreter (e.g., Python 3.9), run:
    ```sh
    uv venv .venv -p 3.9 
    ```
    Replace `3.9` with your target Python version (e.g., `3.10`, `3.11`) or the full path to a Python executable.
    Then, activate it:
    *   On macOS/Linux: `source .venv/bin/activate`
    *   On Windows (PowerShell): `.venv\Scripts\Activate.ps1`
    *   On Windows (CMD): `.venv\Scripts\activate.bat`

2.  **Install `deephaven-mcp`:**
    ```sh
    uv pip install deephaven-mcp
    ```
This command installs `deephaven-mcp` and its dependencies into the active virtual environment. If you skipped the explicit virtual environment creation step above, [`uv`](docs/UV.md) might still create or use one automatically (typically `.venv` in your current directory if `UV_AUTO_CREATE_VENV` is not `false`, or a globally managed one). In any case where a virtual environment is used (either explicitly created or automatically by `uv`), ensure it remains active for manual command-line use of `dh-mcp-community` or `dh-mcp-docs`, or if your LLM tool requires an active environment.

#### Option B: Using Standard `pip` and `venv`

1.  **Create a virtual environment** (e.g., named `.venv`):
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
3.  **Install `deephaven-mcp`** into the activated virtual environment:
    ```sh
    pip install deephaven-mcp
    ```
    Ensure this virtual environment is active in any terminal session where you intend to run `dh-mcp-community` or `dh-mcp-docs` manually, or if your LLM tool requires an active environment when spawning these processes.

---

## Configuring `deephaven_mcp.json`

This section explains how to configure the [Deephaven MCP Community Server](#community-server) to connect to and manage your [Deephaven Community Core](https://deephaven.io/) instances. This involves creating a [community session definition file](#the-deephaven_mcp.json-file-defining-your-community-sessions) and understanding how the server locates this file.

### The `deephaven_mcp.json` File

#### Purpose and Structure

The [Deephaven MCP Community Server](#community-server) requires a JSON configuration file that describes the [Deephaven Community Core](https://deephaven.io/) worker instances it can connect to. 

*   The file must be a JSON object. It can be an empty object `{}` if no community sessions are to be configured.
*   Optionally, it can contain a top-level key named `"community_sessions"`.
    *   If this key is present, its value must be an object (which can be empty, e.g., `{}`) where each key is a unique session name (e.g., `"local_session"`, `"prod_cluster_1_session"`) and the value is a configuration object for that session. An empty object signifies no sessions are configured under this key.
    *   If this key is absent from the JSON file, it is treated as a valid configuration with no community sessions defined.

In addition to `"community_sessions"`, the `deephaven_mcp.json` file can optionally include an `"enterprise_sessions"` key for configuring connections to Deephaven Enterprise instances. The configuration details for both `community_sessions` and `enterprise_sessions` are provided below.

#### Community Session Configuration Fields

*The fields listed below pertain to **community sessions**. All community session fields are optional. Default values are applied by the server if a field is omitted. Configuration fields for **enterprise sessions** are detailed in a subsequent section.*

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

#### Enterprise Session Configuration Fields

The `enterprise_sessions` key in `deephaven_mcp.json` is a dictionary mapping custom session names (e.g., `"prod_cluster"`, `"data_science_env"`) to their specific configuration objects. Each configuration object supports the following fields:

**Required Fields:**

*   `connection_json_url` (string): URL to the Deephaven Enterprise server's `connection.json` file (e.g., `"https://enterprise.example.com/iris/connection.json"`). This file provides the necessary details for the client to connect to the server.
*   `auth_type` (string): Specifies the authentication method. Must be one of:
    *   `"api_key"`: For API key-based authentication.
    *   `"password"`: For username/password authentication.
    *   `"saml_private_key"`: For SAML authentication using a private key.
    *   `"none"`: For no authentication (use with caution, typically for development or trusted environments).

**Conditional Fields (based on `auth_type`):**

*   **If `auth_type` is `"api_key"`:**
    *   One of the following must be provided:
        *   `api_key` (string): The API key itself.
        *   `api_key_env_var` (string): The name of an environment variable that holds the API key (e.g., `"MY_ENTERPRISE_API_KEY"`).
*   **If `auth_type` is `"password"`:**
    *   `username` (string): The username for authentication (required).
    *   One of the following must be provided for the password:
        *   `password` (string): The password itself.
        *   `password_env_var` (string): The name of an environment variable that holds the password (e.g., `"MY_ENTERPRISE_PASSWORD"`).
*   **If `auth_type` is `"saml_private_key"`:**
    *   `private_key_path` (string): The absolute path to the SAML private key file (e.g., `"/path/to/your/private_key.pem"`) (required).

*Note: All paths, like `private_key_path`, should be absolute and accessible by the MCP server process.*

#### Example `deephaven_mcp.json`

```json
{
  "community_sessions": {
    "my_local_deephaven": {
      "host": "localhost",
      "port": 10000,
      "session_type": "python"
    },
    "secure_community_worker": {
      "host": "secure.deephaven.example.com",
      "port": 10001,
      "auth_type": "token",
      "auth_token": "your-community-secret-api-token-here",
      "use_tls": true,
      "tls_root_certs": "/path/to/community_root.crt"
    }
  },
  "enterprise_sessions": {
    "prod_enterprise_cluster": {
      "connection_json_url": "https://enterprise.example.com/iris/connection.json",
      "auth_type": "api_key",
      "api_key_env_var": "PROD_ENTERPRISE_API_KEY"
    },
    "staging_enterprise_env": {
      "connection_json_url": "https://staging.enterprise.example.com/iris/connection.json",
      "auth_type": "password",
      "username": "testuser",
      "password_env_var": "STAGING_USER_PASSWORD"
    }
  }
}
```


#### Security Note for `deephaven_mcp.json`

The `deephaven_mcp.json` file can contain sensitive information such as authentication tokens, usernames, and passwords. Ensure that this file is protected with appropriate filesystem permissions to prevent unauthorized access. For example, on Unix-like systems (Linux, macOS), you can restrict permissions to the owner only using the command: 

```bash
chmod 600 /path/to/your/deephaven_mcp.json
```

#### Additional Notes for `deephaven_mcp.json`

*   Ensure all file paths within the config (e.g., for TLS certificates if used) are absolute and accessible by the server process.
*   The session names are arbitrary and used to identify sessions in client tools.

### Setting `DH_MCP_CONFIG_FILE` (Informing the MCP Server)

The `DH_MCP_CONFIG_FILE` environment variable tells the [Deephaven MCP Community Server](#community-server) where to find your `deephaven_mcp.json` file (detailed in [The `deephaven_mcp.json` File (Defining Your Community Sessions)](#the-deephaven_mcp.json-file-defining-your-community-sessions)). You will set this environment variable as part of the server launch configuration within your LLM tool, as detailed in the [Configure Your LLM Tool to Use MCP Servers](#configure-your-llm-tool-to-use-mcp-servers) section. 

When launched by an LLM tool, the [MCP Community Server](#community-server) process reads this variable to load your session definitions. For general troubleshooting or if you need to set other environment variables like `PYTHONLOGLEVEL` (e.g., to `DEBUG` for verbose logs), these are also typically set within the LLM tool's MCP server configuration (see [Defining MCP Servers for Your LLM Tool (The `mcpServers` JSON Object)](#defining-mcp-servers-for-your-llm-tool-the-mcpservers-json-object)).

---

## Configure Your LLM Tool to Use MCP Servers

This section details how to configure your LLM tool (e.g., [Claude Desktop](https://www.anthropic.com/claude), [GitHub Copilot](https://github.com/features/copilot)) to launch and communicate with the [Deephaven MCP Community Server](#community-server) and the [Deephaven MCP Docs Server](#docs-server). This involves providing a JSON configuration, known as the [`"mcpServers"` object](#defining-mcp-servers-for-your-llm-tool-the-mcpservers-json-object), to your LLM tool.

### How LLM Tools Launch MCP Servers (Overview)

LLM tools that support the Model Context Protocol (MCP) can be configured to use the Deephaven MCP Community and Docs Servers. The LLM tool's configuration will typically define how to *start* the necessary MCP server processes.

### Understanding Deephaven Core Worker Status (via MCP)

The [MCP Community Server](#community-server), launched by your LLM tool, will attempt to connect to the [Deephaven Community Core](https://deephaven.io/) instances defined in your `deephaven_mcp.json` file (pointed to by `DH_MCP_CONFIG_FILE` as described in [Setting `DH_MCP_CONFIG_FILE` (Informing the MCP Server)](#setting-dh_mcp_config_file-informing-the-mcp-server)).

It's important to understand the following:
*   **MCP Server Independence**: The [MCP Community Server](#community-server) itself will start and be available to your LLM tool even if some or all configured [Deephaven Community Core](https://deephaven.io/) workers are not currently running or accessible. The LLM tool will be able to list the configured workers and see their status (e.g., unavailable, connected).
*   **Worker Interaction**: To successfully perform operations on a specific [Deephaven Community Core](https://deephaven.io/) worker (e.g., list tables, execute scripts), that particular worker must be running and network-accessible from the environment where the [MCP Community Server](#community-server) process is executing.
*   **Configuration is Key**: Ensure your `deephaven_mcp.json` file accurately lists the community session configurations you intend to use. The MCP server uses this configuration to know which sessions to attempt to manage.

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
        "DH_MCP_CONFIG_FILE": "/full/path/to/your/deephaven_mcp.json",
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
        "DH_MCP_CONFIG_FILE": "/full/path/to/your/deephaven_mcp.json",
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

If the servers are not listed or you encounter errors at this stage, please proceed to the [Troubleshooting](#troubleshooting) section for guidance.

---

## Troubleshooting

*   **LLM Tool Can't Connect / Server Not Found:**
    *   Verify all paths in your LLM tool's JSON configuration are **absolute and correct**.
    *   Ensure `DH_MCP_CONFIG_FILE` environment variable is correctly set in the JSON config and points to a valid worker file.
    *   Ensure any [Deephaven Community Core](https://deephaven.io/) workers you intend to use (as defined in `deephaven_mcp.json`) are running and accessible from the [MCP Community Server](#community-server)'s environment.
    *   Check for typos in server names, commands, or arguments in the JSON config.
    *   Validate the syntax of your JSON configurations (`mcpServers` object in the LLM tool, and `deephaven_mcp.json`). A misplaced comma or incorrect quote can prevent the configuration from being parsed correctly. Use a [JSON validator tool](https://jsonlint.com/) or your IDE's linting features.
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
*   **Python Errors in Server Logs:** Check the server logs for Python tracebacks. Ensure all dependencies were installed correctly (see [Installation & Initial Setup](#installation--initial-setup)).
*   **Worker Configuration Issues:**
        *   If the [Community Server](#community-server) starts but can't connect to [Deephaven Community Core](https://deephaven.io/) workers, verify your `deephaven_mcp.json` file (see [The `deephaven_mcp.json` File (Defining Your Community Sessions)](#the-deephaven_mcp.json-file-defining-your-community-sessions) for details on its structure and content).
        *   Ensure the target [Deephaven Community Core](https://deephaven.io/) instances are running and network-accessible.
        *   Confirm that the process running the [MCP Community Server](#community-server) has read permissions for the `deephaven_mcp.json` file itself.

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
