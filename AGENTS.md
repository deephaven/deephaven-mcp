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

## Agent Skills

Skills in `.agents/skills/` fall into two categories:

- **User-invokable** (no prefix): intended to be triggered directly via slash commands (e.g., `review-changes`, `tests-run`).
- **AI-internal** (`_` prefix): reference/standards documents loaded as context by agents or other skills, not invoked directly by users (e.g., `_python-coding-practices`, `_logging-standards`). The `_` prefix suppresses them from slash command autocomplete.

When adding a new skill, prefix it with `_` if it is a reference document or standard that other skills invoke rather than something a user would trigger directly.

## MCP Server Configuration

- **Servers**: Two server binaries — the multiplexed `dh-mcp-systems-server` (handles community + enterprise) and `dh-mcp-docs-server` (documentation Q&A).
- **Transports**: `dh-mcp-systems-server` supports `stdio` (default) and `http` (streamable-HTTP), selectable via `--transport`. `dh-mcp-docs-server` supports only streamable-HTTP.
- **HTTP transport**: Loopback only (`127.0.0.1` / `::1` / `localhost`); no TLS; PSK is required and shared via the `X-Deephaven-PSK` header (matches `server.json`'s `psk` field, which may use `${env:NAME}` templating).
- **Configuration directory**: `dh-mcp-systems-server` reads a directory tree (not a single file). Override via `DH_MCP_CONFIG_DIR` env var or `--config-dir` CLI arg. Default: `~/.deephaven/ai/config/` (POSIX) or `%APPDATA%/Deephaven/ai/config/` (Windows). See `examples/ai/config/` for the layout.
- **Removed pieces**: `dh-mcp-community-server`, `dh-mcp-enterprise-server`, `DH_MCP_CONFIG_FILE`, the `mcp_reload` tool, and the auth backends. The `auth/` package now contains only `PSKMiddleware` (`auth/middleware/_psk.py`, mounted by the HTTP transport) and the outbound credential / TLS dataclasses (`auth/credentials/`, `auth/tls/`). Restart the server to pick up config changes.
