---
name: _mcp-module-organization
description: Module organization and design patterns for MCP tool development in this project — invoke when creating or modifying MCP tool modules
user-invocable: false
---

# MCP Tools Module Organization Guidelines

> **Scope.** This skill applies only to MCP server tool modules under `src/deephaven_mcp/mcp_systems_server/_tools/`. For commands in the local `dh-mcp` CLI under `src/deephaven_mcp/cli/_commands/`, apply the `cli-command-add` skill instead — the CLI uses a different framework (`click`), a different async pattern (`@run_async` from `cli/_async.py`), and a different error contract (`CliError` from `cli/_errors.py`). Do not transplant patterns between the two.

## File Organization Principles

1. **Module Cohesion**: Each file in `_tools/` should contain MCP tools and helpers for a single, well-defined domain (e.g., table operations, session lifecycle, script execution).

2. **Helper Function Placement**:
   - If used by 1 module → keep it in that module (private with `_` prefix).
   - If used by 2 modules → duplicate is acceptable. Promote to `_tools/shared.py` when a third consumer appears (rule below).
   - If used by 3+ modules → move to `_tools/shared.py`.

3. **Constant Placement**:
   - Module-specific constants → keep in that module after imports
   - Shared constants → place in `_tools/shared.py` or the most relevant module
   - Always include a docstring explaining the constant's purpose
   - Operator-tunable values (timeouts, defaults, thresholds) are **not** module constants — see the `_configuration-conventions` skill for where they live and how to read them.

4. **New MCP Tool Placement**:
   - Before creating a new file, check if the tool fits an existing domain
   - Only create a new module if the tool represents a distinct new domain
   - Typical module size: 300-700 lines is healthy

5. **Registering New Tool Modules**:
   - **CRITICAL**: Every tool module must define a `register_tools(server: FastMCP) -> None` function
   - This function calls `server.tool()(tool_fn)` for each tool in the module
   - After creating a new module, add a `module.register_tools(server)` call to `_register_tools()` in `src/deephaven_mcp/mcp_systems_server/server.py`. The systems server is multiplexed in one binary, but tool *registration* is section-gated: cross-cutting modules (`session`, `table`, `script` — anything that operates on either side) register unconditionally; section-specific modules register only when the corresponding configuration section is present (`session_community` when `multi_config.community is not None`; `session_enterprise`, `catalog`, `pq` when `multi_config.enterprise is not None`). See `_register_tools` in `mcp_systems_server/server.py`.

## Required Pattern for MCP Tool Modules

All tool modules must follow this pattern — **no `@decorator` on tool functions**:

```python
from mcp.server.fastmcp import Context, FastMCP

async def my_tool(context: Context, ...) -> dict:
    """Tool docstring (consumed by AI agents)."""
    ...

def register_tools(server: FastMCP) -> None:
    """Register all tools in this module with the given FastMCP server."""
    server.tool()(my_tool)
```

Common shared utilities (import only what you need):

```python
from deephaven_mcp.mcp_systems_server._tools.shared import (
    error_response,                   # Build a standard error response dict
    format_partial_result,            # Flag an incomplete (partial-discovery) result
    get_lifespan_context,             # Get the LifespanContext from MCP context
    get_registry,                     # Get the MultiSystemRegistry from context
    get_multi_config,                 # Get the merged ConfigTree from context
    parse_session_id,                 # Parse + validate a session-id string (raises if malformed)
    get_community_registry,           # Get CommunitySessionRegistry from context
    get_enterprise_registry,          # Get EnterpriseSessionRegistry from context
    get_session_from_context,         # Get session from MCP context
    get_enterprise_session,           # Get + validate Enterprise session
    check_response_size,              # Validate response size limits
    format_meta_table_result,         # Format metadata tables
    build_table_data_response,        # Build a table data response dict
    redact_json_sensitive_fields,     # Redact sensitive fields from JSON strings
)
```

## Naming Conventions

### MCP Tool Functions (Public API)

- **Pattern**: `{domain}_{action}` (e.g., `session_table_data`, `pq_create`, `catalog_tables_list`)
- **No underscore prefix**: These are the public MCP tools exposed to AI agents
- **Descriptive and specific**: Name states what the tool does in `{domain}_{action}` form
- **Registered explicitly**: via `server.tool()(fn)` inside `register_tools()`

### Helper Functions (Internal Use Only)

- **Always private**: Use underscore prefix (e.g., `_validate_launch_method`, `_build_response`)
- **Purpose**: Support MCP tools within the same module or shared utilities
- **Not exported**: Never include in `__all__` (if present)
- **Local scope**: Keep in the module where used, unless used by 3+ modules

### Module-Level Objects

- **Constants**: ALLCAPS with docstring (e.g., `MAX_RESPONSE_SIZE`, `DEFAULT_TIMEOUT`)
- **Logger**: `_LOGGER = logging.getLogger(__name__)` (private, standard pattern)
- **Type variables**: Follow typing conventions (e.g., `T = TypeVar("T")`)

## Module Independence

- Avoid circular dependencies between `_tools/` modules
- `_tools/shared.py` should not import from other `_tools/` modules
- Cross-module communication should go through the shared utilities or MCP context
