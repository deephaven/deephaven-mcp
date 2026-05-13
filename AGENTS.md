# Deephaven MCP — Agent Guidelines

## Testing

- Always use `uv run pytest` instead of `pytest` directly. This ensures the correct virtual environment and dependencies are used.
- Unit tests should target 100% coverage.

## Timeouts

These operations are slow by nature, not hung — set generous timeouts:

- `uv pip install` / `uv sync` / any `uv` package operation: allow at least 120 seconds.
- `./bin/precommit.sh`: allow at least 60 seconds (~22 seconds typical).
- `uv run pytest` (full suite): allow at least 120 seconds.
- `uv run mypy src/`: allow at least 30 seconds (~15 seconds typical).

## Version Control

- When moving or renaming files, use `git mv` rather than delete + create. When removing files, use `git rm`. Preserving history matters.

## MCP Server Configuration

- **Transport**: All three servers support only `streamable-http`. They do not support `stdio` or `sse`.
- **Command names**: `dh-mcp-community-server`, `dh-mcp-enterprise-server`, `dh-mcp-docs-server`. Wrong names are a common error.
- **Config file**: Community and enterprise servers require `DH_MCP_CONFIG_FILE` env var or `--config` CLI arg. Missing this is the most common startup failure.
