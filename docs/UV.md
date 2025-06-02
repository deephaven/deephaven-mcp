# Using `uv` in deephaven-mcp

[`uv`](https://github.com/astral-sh/uv) is a fast Python package manager and workflow tool. This project uses `uv` for installing dependencies, running scripts, and managing virtual environments in a reproducible way.

> **Note:** Using `uv` is recommended for consistency, but it is not strictly required. You could use `pip` and standard Python tools instead; however, only `uv`-based workflows are documented here.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Why use uv?](#why-use-uv)
- [Installing uv](#installing-uv)
- [Typical Workflows](#typical-workflows-with-uv)
- [How uv Lock Files Work](#how-uv-lock-files-work)
- [Upgrading Dependencies](#upgrading-dependencies)
- [Environment Variables](#environment-variables-with-uv)
- [.env Example](#env-example)
- [Code Quality & Testing](#code-quality-testing-and-linting)
- [CI/CD Usage](#cicd-usage)
- [Common Pitfalls & FAQ](#common-pitfalls--faq)
- [Troubleshooting](#troubleshooting)
- [Further Reading](#further-reading)

---

## Quick Start

1. Install `uv`:
    ```sh
    pip install uv
    ```
2. Install all project dependencies:
    ```sh
    uv pip install ".[dev]"
    ```
3. Run the Community Server:
    ```sh
    DH_MCP_CONFIG_FILE=deephaven_workers.json uv run dh-mcp-community --transport sse
    ```
4. Run tests:
    ```sh
    uv run pytest
    ```

---

## Why use `uv`?

- **Speed:** Much faster than pip for installing and resolving dependencies.
- **Reproducibility:** Ensures consistent environments across machines.
- **Convenience:** Can run Python scripts and manage virtual environments easily.

---

## Installing `uv`

To install `uv`, run:
```sh
pip install uv
```
Or see the [uv installation guide](https://github.com/astral-sh/uv#installation) for other options and the latest instructions.

### Creating a Virtual Environment with `uv`

Once [`uv`](https://github.com/astral-sh/uv) is installed, it's highly recommended to create and use a virtual environment for your project. This isolates dependencies and ensures consistency.

1.  **Create a virtual environment (e.g., named `.venv`) with a specific Python version:**
    Use the `-p` option to specify your desired Python interpreter (e.g., Python 3.9, 3.10, or a full path to an executable):
    ```sh
    uv venv .venv -p 3.9
    ```
    Replace `3.9` with your target Python version or path.

2.  **(Optional) Activate the virtual environment:**
    *   On macOS/Linux: `source .venv/bin/activate`
    *   On Windows (PowerShell): `.venv\Scripts\Activate.ps1`
    *   On Windows (CMD): `.venv\Scripts\activate.bat`

After activating, any `uv` commands (like `uv pip install ...` or `uv run ...`) will operate within this environment.

---

## Typical Workflows with `uv`

### 1. Installing Dependencies

```sh
uv pip install ".[dev]"
```

### 2. Synchronizing Dependencies

```sh
uv sync
```
This will install all dependencies to exactly match your lock file(s) for reproducible environments.

### 3. Running Servers and Scripts

**Community Server (SSE):**

```sh
DH_MCP_CONFIG_FILE=deephaven_workers.json uv run dh-mcp-community --transport sse
```

**Docs Server (SSE):**

```sh
INKEEP_API_KEY=your-inkeep-api-key uv run dh-mcp-docs --transport sse
```

**Run a test server:**

```sh
uv run scripts/run_deephaven_test_server.py --table-group simple
```

**Run a test client:**

```sh
uv run scripts/mcp_community_test_client.py --transport sse --url http://localhost:8000/sse
uv run scripts/mcp_docs_test_client.py --prompt "What is Deephaven?"
```

**Run a stress test:**

```sh
uv run scripts/mcp_docs_stress_sse.py --sse-url "http://localhost:8000/sse"
```

---

## How uv Lock Files Work

- `uv` uses `pyproject.toml` for dependency specification and generates a `uv.lock` file for reproducible installs.
- Use `uv sync` to ensure your environment matches the lock file exactly.
- If you update dependencies, always regenerate the lock file (see below).

---

## Upgrading Dependencies

1. Update your `pyproject.toml` as needed.
2. Run:
    ```sh
    uv pip install ".[dev]" --upgrade
    uv pip freeze > requirements.txt  # Optional: update requirements.txt for reference
    uv lock  # Regenerate lock file if needed (see uv docs)
    ```
3. Commit both `pyproject.toml` and `uv.lock` to version control.

---

## Environment Variables with uv

- `DH_MCP_CONFIG_FILE`: Path to worker config JSON file (required for Community Server)
- `INKEEP_API_KEY`: Required for Docs Server
- `OPENAI_API_KEY`: Optional fallback for Docs Server
- `PYTHONLOGLEVEL`: Set log verbosity (e.g., DEBUG, INFO)

> You can also use a `.env` file with [python-dotenv](https://github.com/theskumar/python-dotenv) to manage environment variables.

---

## .env Example

Create a `.env` file in your project root for local development:

```env
# .env example
DH_MCP_CONFIG_FILE=/absolute/path/to/deephaven_workers.json
INKEEP_API_KEY=your-inkeep-api-key
OPENAI_API_KEY=your-optional-openai-key
PYTHONLOGLEVEL=DEBUG
```

---

## Code Quality & Testing

**Run all tests:**

```sh
uv run pytest
```

**Sort imports:**

```sh
uv run isort . --skip _version.py --skip .venv
```

**Check import sorting only:**

```sh
uv run isort . --check-only --diff --skip _version.py --skip .venv
```

**Format code:**

```sh
uv run black . --exclude '(_version.py|.venv)'
```

**Lint code:**

```sh
uv run ruff check src --fix --exclude _version.py --exclude .venv
```

**Type checking:**

```sh
uv run mypy src/
```

**Pre-commit hooks:**

```sh
uv pip install pre-commit
pre-commit install
bin/precommit.sh
```

---

## CI/CD Usage

Example GitHub Actions step for using `uv`:

```yaml
- name: Set up Python
  uses: actions/setup-python@v4
  with:
    python-version: '3.10'
- name: Install uv
  run: pip install uv
- name: Install dependencies
  run: uv pip install ".[dev]"
- name: Run tests
  run: uv run pytest
```

---

## Common Pitfalls & FAQ

**Q: Do I have to use `uv`?**
A: No, but only `uv` workflows are documented and tested. Using pip directly may break reproducibility.

**Q: Why is my environment not matching the lock file?**
A: Always use `uv sync` after changing dependencies or cloning the repo.

**Q: How do I set environment variables for `uv run`?**
A: Prefix your command or use a `.env` file.

**Q: Can I use `uv` in CI/CD?**
A: Yes! See the example above.

---

## Troubleshooting

| Problem                              | Solution                                                                |
|--------------------------------------|-------------------------------------------------------------------------|
| Command not found: uv                | Run `pip install uv`                                                    |
| Missing env var error                | Check your shell or `.env` file                                         |
| Port already in use                  | Change with `--port` or set `PORT` env var                              |
| API key errors                       | Verify `INKEEP_API_KEY` or `OPENAI_API_KEY` is set and valid            |
| Dependency mismatch                  | Run `uv sync`                                                           |
| Lock file out of date                | Run `uv lock` or `uv pip install ".[dev]" --upgrade`                      |
| Permission denied for .env file      | Ensure correct file permissions                                         |

---

## Tips & Troubleshooting

- `uv` can be used as a drop-in replacement for most pip and python commands.
- If you encounter issues with environment variables, check your `.env` file or shell configuration.
- For port conflicts, change the server port using the `--port` argument or `PORT` environment variable.
- For missing API keys, ensure they are set in your environment or `.env` file.
- For more detailed logs, set `PYTHONLOGLEVEL=DEBUG`.

---

## Further Reading

- See the [Developer & Contributor Guide](DEVELOPER_GUIDE.md) for advanced workflows, integration, and troubleshooting.
- For Docker usage, see the `docker/` directory and related documentation.
- For more on MCP Inspector and integration methods, see the Developer Guide.
- [uv documentation](https://github.com/astral-sh/uv)
- [python-dotenv](https://github.com/theskumar/python-dotenv)
