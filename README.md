# deephaven-mcp

![GitHub Workflow Status](https://img.shields.io/github/workflow/status/deephaven/deephaven-mcp/CI)
![PyPI](https://img.shields.io/pypi/v/deephaven-mcp)
![License](https://img.shields.io/github/license/deephaven/deephaven-mcp)

#TODO: document

## About `uv`

This project uses [`uv`](https://github.com/astral-sh/uv), a fast Python package manager and workflow tool. `uv` is used for installing dependencies, running scripts, and managing virtual environments in a reproducible way.

> **Note:** Using `uv` is recommended for consistency, but it is not strictly required. You could use `pip` and standard Python tools instead; however, only `uv`-based workflows are documented here.

### Why use `uv`?
- **Speed:** Much faster than pip for installing and resolving dependencies.
- **Reproducibility:** Ensures consistent environments across machines.
- **Convenience:** Can run Python scripts and manage virtual environments easily.

### Installing `uv`
To install `uv`, run:
```sh
pip install uv
```

Or see the [uv installation guide](https://github.com/astral-sh/uv#installation) for other options and the latest instructions.

### Example Usage
Install dependencies:
```sh
uv pip install .[dev]
```

Synchronize dependencies exactly as specified in your lock files (for reproducible environments):
```sh
uv sync
```
This will install all dependencies (including optional groups like `[dev]` if specified in your lock file) to exactly match the versions in your `uv.lock` or `requirements.lock` file. Use `uv sync` after updating dependencies or when setting up a new environment to ensure consistency.

Run a script:
```sh
uv run scripts/mcp_community_test_client.py
```

You can use `uv` as a drop-in replacement for many common pip and python commands in this project.

