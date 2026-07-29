---
name: mcp-tool-add
description: Add a new MCP tool to the systems server — invoke when adding a tool under mcp_systems_server/_tools/. Wraps ref-mcp-module-organization, pydocs-improve, and ref-logging-standards; prevents the most common bug (forgetting to register the tool)
---

Apply the `ref-mcp-module-organization` skill for module placement, the `pydocs-improve` skill for docstring requirements (Terminology Note + Format Accuracy for AI Agents), and the `ref-output-serialization-conventions` skill for the tool's return payload (every string field's value vocabulary, casing, and known carve-outs).

## Steps

1. **Pick the module.** Look in `src/deephaven_mcp/mcp_systems_server/_tools/` for an existing domain that fits (`session.py`, `table.py`, `script.py`, `session_community.py`, `session_enterprise.py`, `catalog.py`, `pq.py`). Only create a new module if the tool represents a genuinely new domain. Module size follows from that rule — one domain per module — not from a line target. For how the tool packages relate to the rest of the tree, see `ref-project-reference`; if you add a module, update its map in the same changeset.
2. **Write the tool function.** Async, takes `context: Context` first, returns a `dict`. **No `@server.tool()` decorator** on the function itself — registration is explicit (see step 4).
3. **Write the docstring.** AI agents see only the docstring; be specific about parameters, return shape, and errors. Apply the `pydocs-improve` skill — including the canonical "Terminology Note" and (if the tool returns tabular data with a `format` parameter) "Format Accuracy for AI Agents" sections.
4. **Register the tool.** Add `server.tool()(my_tool)` inside the module's `register_tools(server: FastMCP) -> None`. If you created a new module, also add `module.register_tools(server)` to `_register_tools()` in `src/deephaven_mcp/mcp_systems_server/_fastmcp.py`. **This is the most commonly forgotten step** — without it the tool is invisible to clients.
5. **Add helpers** with a leading underscore. Place them in the same module unless used by 3+ modules, in which case promote to `_tools/shared.py`.
6. **Add logging.** Apply the `ref-logging-standards` skill: log entry, success, and the failure path for the tool in its canonical message format; never log secrets.
7. **Add tests.** A new tool gets its own test cases covering the happy path, error paths, and any session-id parsing. Apply the `tests-improve` skill for coverage targets.
8. **Update the capability list.** Add the tool to the *Available MCP Tools* list in `README.md` (one line: name + one-line purpose). The docstring from step 3 is the tool's reference — no other doc needs a per-tool entry. Update `docs/CONFIGURATION.md` only if the tool adds a configuration field.
9. **Run** `run-precommit` and `tests-run-file` on the changed test files, then `tests-run` for a full-suite check.

## Anti-patterns

- Decorating the tool function with `@server.tool()` directly — the project uses explicit registration via `register_tools()`.
- Skipping the Terminology Note or Format Accuracy sections — these are required for every MCP tool docstring; see `pydocs-improve` for the canonical wording.
- Operator-tunable values (timeouts, defaults, thresholds) hardcoded in the module — see `config-field-add` and `ref-configuration-conventions`.
