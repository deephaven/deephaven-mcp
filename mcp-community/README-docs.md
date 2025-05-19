
# Deephaven MCP Docs Server

> **Note:** This document contains low-level technical details for developers. **Users seeking high-level usage and onboarding information should refer to the main documentation in the [root README](../README.md).**

_A Python server for conversational Q&A and documentation assistance about Deephaven, via the Model Context Protocol (MCP)._

A Model Context Protocol (MCP) server for learning about Deephaven Data Labs documentation. This server exposes an agentic, LLM-powered API for documentation Q&A, chat, and integration with orchestration frameworks.

---

## Table of Contents
- [Features](#features)
- [Quickstart](#quickstart)
- [Usage](#usage)
  - [Test Client](#test-client)
  - [MCP Inspector](#mcp-inspector)
  - [Using mcp-proxy](#using-mcp-proxy)
- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
- [Architecture](#architecture)
- [Main Files & Structure](#main-files--structure)
- [Development](#development)
  - [Workflow](#workflow)
  - [Commands](#commands)
  - [Running with Docker Compose](#running-with-docker-compose)
  - [Stress Testing the /sse Endpoint](#stress-testing-the-sse-endpoint)
- [Troubleshooting](#troubleshooting)
- [Resources](#resources)
- [License](#license)

---

## Features
- **MCP-compatible server** for documentation Q&A and chat
- **LLM-powered** (uses Inkeep/OpenAI APIs)
- **FastAPI** backend, deployable locally or via Docker/Cloud Run
- **Single tool:** `docs_chat` for conversational documentation assistance
- **Extensible:** add new tools or extend the context via Python
- **Robust error handling** and environment validation

---

## Quickstart

Follow these steps to get started quickly:

1. **Clone the repository and enter the docs directory:**
   ```bash
   git clone https://github.com/deephaven/deephaven-mcp.git
   cd deephaven-mcp/mcp-docs
   ```
2. **Create a uv-managed virtual environment:**
   ```bash
   uv venv
   ```
3. **Install dependencies (including dev extras):**
   ```bash
   uv pip install '.[dev]'
   ```
4. **Set environment variables:**
   - Create a `.env` file or export variables directly:
     ```sh
     export INKEEP_API_KEY=your-inkeep-api-key   # Get this from https://inkeep.com/
     export PYTHONLOGLEVEL=INFO                  # Optional
     export PORT=8000                            # Optional, default is 8000
     ```
5. **Run the server:**
   ```bash
   uv run dh-mcp-docs
   ```

> **Troubleshooting:**
> - If the server fails to start, ensure you have set `INKEEP_API_KEY` and installed dependencies.
> - For more help, see the Troubleshooting section below.

### Docker

> **Docker images are automatically built and published by [GitHub Actions CI/CD](../.github/workflows/docker-mcp-docs.yml).**
>
> You can pull the latest images from Artifact Registry or use CI-published tags. Manual building is only necessary for local development/testing before PRs.

#### Manual build/run (for local testing)
```bash
cd mcp-docs
# (Make sure Docker is running)
docker build -t deephaven-mcp-docs .

docker run --rm -e INKEEP_API_KEY=your-inkeep-api-key -p 8000:8000 deephaven-mcp-docs
```

---

## Usage

### Test Client
A Python script for exercising the MCP Docs tool and validating server functionality.

**Arguments:**
- `--transport`: Choose `sse` or `stdio` (default: `sse`).
- `--env`: Pass environment variables as `KEY=VALUE` (can be repeated; for stdio).
- `--url`: URL for SSE server (if using SSE transport; default: `http://localhost:8000/sse`).
- `--stdio-cmd`: Command to launch stdio server (if using stdio transport; default: `uv run dh-mcp-docs --transport stdio`).
- `--prompt`: Prompt/question to send to the docs_chat tool.
- `--history`: Optional chat history (JSON string).

> **Note:** You must start the MCP Docs server before using the test client. For stdio, the default command is:
> ```sh
> uv run dh-mcp-docs --transport stdio
> ```
> For SSE, start the server in SSE mode in a separate terminal:
> ```sh
> uv run dh-mcp-docs --transport sse
> ```

#### Example usage (stdio):
```sh
uv run scripts/mcp_docs_test_client.py --transport stdio --env INKEEP_API_KEY=your-inkeep-api-key
```

#### Example usage (SSE):
First, start the MCP Docs server in SSE mode (in a separate terminal):
```sh
INKEEP_API_KEY=your-inkeep-api-key uv run dh-mcp-docs --transport sse
```
Then, in another terminal, run the test client:
```sh
uv run scripts/mcp_docs_test_client.py --transport sse
```

### MCP Inspector
The [MCP Inspector](https://github.com/modelcontextprotocol/inspector) is a web-based tool for interactively exploring and testing MCP servers, including mcp-docs. It provides an intuitive UI for discovering available tools, invoking them, and inspecting responses.

**How to use with mcp-docs:**
1. **Start the MCP Docs server in SSE mode (in one terminal):**
    ```sh
    INKEEP_API_KEY=your-inkeep-api-key uv run dh-mcp-docs --transport sse
    ```
2. **Start the MCP Inspector locally (in a second terminal):**
    ```sh
    npx @modelcontextprotocol/inspector@latest
    ```
3. **Open the Inspector in your browser:**
    - The Inspector UI will open automatically, or visit [http://localhost:6274](http://localhost:6274)
4. **Connect to the MCP Docs server via SSE:**
    - In the Inspector UI, click "Connect" and enter the SSE URL: `http://localhost:8000/sse`
    - (If running on a remote server, use the appropriate URL.)
5. **Explore and invoke tools:**
    - Select `docs_chat` from the list of available tools.
    - Enter a prompt/question and submit. The response will be shown in the Inspector UI.

> **Tip:** By running the Inspector locally, you ensure compatibility with local MCP servers and can experiment with all features interactively. The default Inspector URL is [http://localhost:6274](http://localhost:6274).

### Using mcp-proxy

[`mcp-proxy`](https://github.com/modelcontextprotocol/mcp-proxy) is a utility for bridging Model Context Protocol (MCP) servers that use SSE/Streamable-HTTP to clients that expect standard input/output (stdio) or HTTP endpoints, such as Claude Desktop. This is especially useful if you want to connect Claude Desktop or other tools that do not natively support SSE to your MCP server.

#### When to use
- You want to use Claude Desktop (or another tool) with this MCP server, but it does not support SSE/Streamable-HTTP directly.
- You need to proxy between the MCP server's SSE endpoint and a local client.

#### How to use
1. **Install dependencies** (already handled if you installed from `pyproject.toml`):
    ```sh
    pip install mcp-proxy
    # or, if using uv/other modern tools:
    uv pip install mcp-proxy
    ```
2. **Run mcp-proxy** to connect to your running MCP server:
    ```sh
    mcp-proxy --server-url http://localhost:8000/sse --stdio
    ```
    - Replace `http://localhost:8000/sse` with your MCP server's SSE endpoint URL as needed.
    - The `--stdio` flag tells the proxy to communicate using standard input/output, which is compatible with Claude Desktop.
    - For more options, run `mcp-proxy --help`.
3. **Configure Claude Desktop** to connect to the local proxy (typically via stdio or a local HTTP endpoint, depending on your setup).

#### Additional notes
- `mcp-proxy` is included as a dependency in this project, so you do not need to install it separately.
- For advanced configuration or troubleshooting, see the [mcp-proxy documentation](https://pypi.org/project/mcp-proxy/) or run `mcp-proxy --help`.

---

## Configuration
- **Python:** 3.10+
- **API Key:** You MUST set `INKEEP_API_KEY` (from Inkeep) in the environment for the server to run. Get your API key from [Inkeep](https://inkeep.com/).
- **Port:** Defaults to 8000 (configurable via args)
- **Config files:** See `pyproject.toml` for dependencies and scripts

### Environment Variables
| Variable           | Required | Description                                   | Where Used |
|--------------------|----------|-----------------------------------------------|------------|
| `INKEEP_API_KEY`   | Yes      | API key for Inkeep LLM access                 | Server     |
| `PYTHONLOGLEVEL`   | No       | Python logging level (e.g., DEBUG, INFO)      | Server     |
| `PORT`             | No       | Port for FastAPI server (default: 8000)       | Server     |

#### Example `.env` file
```env
INKEEP_API_KEY=your-inkeep-api-key  # Get this from https://inkeep.com/
PYTHONLOGLEVEL=INFO                 # Optional
PORT=8000                          # Optional
```

---

## Architecture

The MCP Docs Server is designed as a bridge between users (or client applications) and the Deephaven documentation. Users or API clients send natural language questions or documentation queries over HTTP using the Model Context Protocol (MCP). These requests are received by the server, which is built on FastAPI and powered by a large language model (LLM) via the Inkeep API. The server interprets the user's intent, fetches relevant information from the Deephaven documentation, and returns a conversational answer or structured response. This architecture allows for seamless integration with orchestration frameworks and other MCP-compatible tools.

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

---

## Main Files & Structure
- [`src/deephaven_mcp/docs/__init__.py`](src/deephaven_mcp/docs/__init__.py) — Server entrypoint, CLI, and FastAPI app
- [`src/deephaven_mcp/docs/_mcp.py`](src/deephaven_mcp/docs/_mcp.py) — Tool definitions, server instance, API key validation
- [`src/deephaven_mcp/openai.py`](src/deephaven_mcp/openai.py) — OpenAI/LLM API client
- [`Dockerfile`](Dockerfile) — Production container build
- [`tests/`](tests/) — Unit and integration tests

---

## Development

> **All development and testing should be performed using [uv](https://github.com/astral-sh/uv), a fast Python package/dependency manager and runner.**

### Workflow
- Make code changes in a new branch.
- Use the commands below to run tests, check formatting, and lint your code before submitting a pull request.
- Ensure all checks pass locally to match CI.

### Commands
```sh
# Run tests
uv run pytest  # Runs all unit and integration tests

# Run code style and lint checks

# Sort imports (fixes in place)
uv run isort . --skip _version.py --skip .venv
# Check import sorting only (no changes)
uv run isort . --check-only --diff --skip _version.py --skip .venv

# Lint and format code (fixes in place)
uv run ruff check src --fix --exclude _version.py --exclude .venv
uv run black . --exclude '(_version.py|.venv)'

# Check code formatting only (no changes)
uv run black . --check --diff --exclude '(_version.py|.venv)'

# Type checking
uv run mypy src/
```

### Running with Docker Compose

To build and run the `mcp-docs` service using Docker Compose:

1. Ensure you have a `.env` file in the parent directory (one level above the `mcp-docs` folder) containing required environment variables (e.g., `INKEEP_API_KEY`).
2. From the `mcp-docs` directory, run:

```sh
docker compose up --build
```

This will build the Docker image and start the service on port 8000, loading environment variables from the root directory's `.env` file.

### Stress Testing the /sse Endpoint

The script is provided to stress test the `/sse` endpoint of a Deephaven MCP deployment. This is intended for validating the stability and performance of production or staging deployments under load.

This script is intended for use by engineers or SREs validating MCP deployments.

#### Stress Testing the /sse Endpoint (Production Validation)

A script is provided to stress test the `/sse` endpoint of a Deephaven MCP deployment. This is intended for validating the stability and performance of production or staging deployments under load.

#### Usage Example

Run the stress test script from the `mcp-docs` directory:

```sh
uv run ./scripts/mcp_docs_stress_sse.py \
    --concurrency 10 \
    --requests-per-conn 100 \
    --sse-url "https://deephaven-mcp-docs-dev.dhc-demo.deephaven.io/sse" \
    --max-errors 5 \
    --rps 10 \
    --max-response-time 2
```

```sh
uv run ./scripts/mcp_docs_stress_sse.py \
    --concurrency 10 \
    --requests-per-conn 100 \
    --sse-url "https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/sse" \
    --max-errors 5 \
    --rps 10 \
    --max-response-time 2
```

#### Arguments

The script accepts the following arguments:

- `--concurrency`: Number of concurrent connections (default: 100)
- `--requests-per-conn`: Number of requests per connection (default: 100)
- `--sse-url`: Target SSE endpoint URL (default: http://localhost:8000/sse)
- `--max-errors`: Maximum number of errors before stopping the test (default: 5)
- `--rps`: Requests per second limit per connection (default: 0, no limit)
- `--max-response-time`: Maximum allowed response time in seconds (default: 1)

#### Output

The script will print the following:

- Logs warnings and errors for slow responses, bad status codes, or exceptions.
- Prints only the reason string for any error encountered.
- Prints "PASSED" if the test completes without exceeding the error threshold, or "FAILED" with the reason if the error threshold is reached or another fatal error occurs.

---

## Troubleshooting

- **Missing API Key:** Ensure `INKEEP_API_KEY` is set in your environment or `.env` file.
- **Port Conflicts:** The default port is 8000; use the `PORT` variable to change if needed.
- **Dependency Issues:** Ensure Python 3.10+ and all dependencies from `pyproject.toml` are installed.
- **Docker Issues:** Make sure Docker is running and you have permission to build/run images.
- **Cloud Run/CI/CD:** See `.github/workflows/docker-mcp-docs.yml` and Terraform configs in `/terraform` for deployment troubleshooting.
- **CORS Errors:** If accessing from a browser, ensure your client and server are on allowed origins.
- **Invalid API Key:** Double-check your `INKEEP_API_KEY` and that it is valid for your account.
- **Logs and Debugging:** Set `PYTHONLOGLEVEL=DEBUG` for more verbose logs.
- **Server Not Responding:** Check if another process is using the port, or if there are errors in the logs.

> For additional help, open an issue or discussion on GitHub.

---

## Resources
- [Deephaven Documentation](https://deephaven.io/docs/)
- [Model Context Protocol (MCP)](https://github.com/modelcontextprotocol)
- [Inkeep](https://inkeep.com/)
- [FastAPI](https://fastapi.tiangolo.com/)
- [uv](https://github.com/astral-sh/uv)

---

## License
This project is licensed under the [Apache License 2.0](../LICENSE).

---

For questions, issues, or contributions, please open an issue or pull request on GitHub.
