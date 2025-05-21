#!/bin/bash
set -euo pipefail

# Deephaven MCP pre-commit script
# - Sort imports, format code, lint, and type-check
# - Run from the project root

# Sort imports with isort
uv run isort . --skip _version.py --skip .venv

# Format code with black
uv run black . --exclude '(_version.py|.venv)'

# Lint code with ruff
uv run ruff check src --fix --exclude _version.py --exclude .venv

# Run static type checking with mypy
uv run mypy src/

echo "Pre-commit checks passed!"
