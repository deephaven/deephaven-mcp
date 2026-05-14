#!/bin/bash
set -euo pipefail

# Deephaven MCP pre-commit script
# - Sort imports, format code, lint, type-check, and validate markdown (Python)
# - Format, lint, and type-check TypeScript
# - Run from the project root

# Sort imports with isort
uv run isort . --skip _version.py --skip .venv

# Format code with black
uv run black . --exclude '(_version.py|.venv)'

# Lint code with ruff (includes pydocstyle D-rules)
uv run ruff check src --fix --exclude _version.py --exclude .venv

# Run static type checking with mypy
uv run mypy src/

# Lint markdown files with markdownlint
npx --yes markdownlint-cli2 --fix

# Format, lint, and type-check TypeScript
pnpm exec prettier --write src-ts/
pnpm exec eslint src-ts/ --fix
pnpm exec tsc --noEmit

echo "Pre-commit checks passed!"
