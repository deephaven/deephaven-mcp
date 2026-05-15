# Deephaven MCP — Agent Guidelines

## Testing

- **Python**: Always use `uv run pytest` instead of `pytest` directly. This ensures the correct virtual environment and dependencies are used.
- **TypeScript**: Always use `pnpm vitest run` instead of bare `vitest` or `npx vitest`. This uses the project-local Vitest installation.
- Unit tests should target 100% coverage.

## Timeouts

These operations are slow by nature, not hung — set generous timeouts:

- `uv pip install` / `uv sync` / any `uv` package operation: allow at least 120 seconds.
- `./bin/precommit.sh`: allow at least 60 seconds (~22 seconds typical).
- `uv run pytest` (full suite): allow at least 120 seconds.
- `uv run mypy src/`: allow at least 30 seconds (~15 seconds typical).
- `pnpm install`: allow at least 60 seconds.
- `pnpm vitest run` (full suite): allow at least 120 seconds.
- `pnpm exec tsc --noEmit`: allow at least 30 seconds.

## Version Control

- When moving or renaming files, use `git mv` rather than delete + create. When removing files, use `git rm`. Preserving history matters.

## Agent Skills

Skills in `.agents/skills/` fall into two categories:

- **User-invokable** (no prefix): intended to be triggered directly via slash commands (e.g., `review-changes`, `tests-run`).
- **AI-internal** (`_` prefix): reference/standards documents loaded as context by agents or other skills, not invoked directly by users (e.g., `_python-coding-practices`, `_python-logging-standards`). The `_` prefix suppresses them from slash command autocomplete.

When adding a new skill, prefix it with `_` if it is a reference document or standard that other skills invoke rather than something a user would trigger directly.

## MCP Server Configuration

- **Transport**: All three servers support only `streamable-http`. They do not support `stdio` or `sse`.
- **Command names**: `dh-mcp-community-server`, `dh-mcp-enterprise-server`, `dh-mcp-docs-server`. Wrong names are a common error.
- **Config file**: Community and enterprise servers require `DH_MCP_CONFIG_FILE` env var or `--config` CLI arg. Missing this is the most common startup failure.

## TypeScript Port (`src-ts/`)

- **Mirror structure**: `src-ts/` mirrors `src/deephaven_mcp/` module-by-module (file naming: `_foo_bar.py` → `foo-bar.ts`, packages → same kebab-case directory).
- **npm packages**: `zod`, `pino`, `async-mutex`, `apache-arrow`, `json5`, `js-yaml`, `@deephaven/jsapi-nodejs`, `openai`. See `docs/PNPM.md` for workflows; see `_py-to-ts-translation-guide` for Python→TypeScript substitution mappings.
- **DHC/DHE client**: Both use `@deephaven/jsapi-nodejs`. DHE servers serve their own JSAPI bundle — DHE classes (`CorePlusSession`, `EnterpriseSessionManager`, etc.) must be implemented using the real DHE JS API, never stubbed. Docs: [Deephaven Enterprise JS client](https://deephaven.io/enterprise/gplus/docs/clients/javascript/)
- **Build before running**: `pnpm build` compiles `src-ts/` to `dist/`. Always build before running servers or after code changes.
- **Running servers**: Systems: `DH_MCP_CONFIG_FILE=config.json node dist/mcp-systems-server/server.js`; Docs: `INKEEP_API_KEY=<key> node dist/mcp-docs-server/server.js`
