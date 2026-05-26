---
name: mcp-tool-add
description: Add a new MCP tool to the systems server — wraps _mcp-module-organization and pydocs-improve, and prevents the most common bug (forgetting to register the tool)
---

Apply the `_mcp-module-organization` skill for module placement and the `pydocs-improve` skill for docstring requirements (Terminology Note + Format Accuracy for AI Agents).

## Steps

1. **Pick the module.** Look in `src/deephaven_mcp/mcp_systems_server/_tools/` for an existing domain that fits (`session.py`, `table.py`, `script.py`, `session_community.py`, `session_enterprise.py`, `catalog.py`, `pq.py`). Only create a new module if the tool represents a genuinely new domain. Typical healthy module size: 300–700 lines.
2. **Write the tool function.** Async, takes `context: Context` first, returns a `dict`. **No `@server.tool()` decorator** on the function itself — registration is explicit (see step 4).
3. **Write the docstring.** AI agents see only the docstring; be specific about parameters, return shape, and errors. Apply the `pydocs-improve` skill — including the canonical "Terminology Note" and (if the tool returns tabular data with a `format` parameter) "Format Accuracy for AI Agents" sections.
4. **Register the tool.** Add `server.tool()(my_tool)` inside the module's `register_tools(server: FastMCP) -> None`. If you created a new module, also add `module.register_tools(server)` to `_register_tools()` in `src/deephaven_mcp/mcp_systems_server/server.py`. **This is the most commonly forgotten step** — without it the tool is invisible to clients.
5. **Add helpers** with a leading underscore. Place them in the same module unless used by 3+ modules, in which case promote to `_tools/shared.py`.
6. **Add logging.** Apply the `_logging-standards` skill: emit a `_LOGGER.info(f"[mcp_systems_server:{tool_name}] Invoked: ...")` at entry, a `Success:` line on completion, and a `_LOGGER.error(f"[mcp_systems_server:{tool_name}] Failed: {e!r}", exc_info=True)` on the failure path. Never log secrets — see the "Sensitive data" section of `_logging-standards`.
7. **Add tests.** A new tool gets its own test cases covering the happy path, error paths, and any session-id parsing. Apply the `tests-improve` skill for coverage targets.
8. **Update docs.** `docs/CONFIGURATION.md` and `docs/DEVELOPER_GUIDE.md` if the tool changes the user-visible surface.
9. **Run** `run-precommit` and `tests-run-file` on the changed test files, then `tests-run` for a full-suite check.

## Anti-patterns

- Decorating the tool function with `@server.tool()` directly — the project uses explicit registration via `register_tools()`.
- Skipping the Terminology Note or Format Accuracy sections — these are required for every MCP tool docstring; see `pydocs-improve` for the canonical wording.
- Operator-tunable values (timeouts, defaults, thresholds) hardcoded in the module — see `config-add-tunable` and `_configuration-conventions`.
