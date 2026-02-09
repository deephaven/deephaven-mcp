---
trigger: always_on
---

# MCP Tools Module Organization Guidelines

## File Organization Principles

1. **Module Cohesion**: Each file in `_tools/` should contain MCP tools and helpers for a single, well-defined domain (e.g., table operations, session lifecycle, script execution).

2. **Helper Function Placement**:
   - If used by 1 module → keep it in that module (private with `_` prefix)
   - If used by 2 modules → duplicate is acceptable, consider shared
   - If used by 3+ modules → move to `_tools/shared.py`

3. **Constant Placement**:
   - Module-specific constants → keep in that module after imports
   - Shared constants → place in `_tools/shared.py` or the most relevant module
   - Always include a docstring explaining the constant's purpose

4. **New MCP Tool Placement**:
   - Before creating a new file, check if the tool fits an existing domain
   - Only create a new module if the tool represents a distinct new domain
   - Typical module size: 300-700 lines is healthy

## Required Imports for MCP Tools

All files with `@mcp_server.tool()` decorated functions must import:

```python
from deephaven_mcp.mcp_systems_server._tools.mcp_server import mcp_server
```

Common shared utilities (import only what you need):

```python
from deephaven_mcp.mcp_systems_server._tools.shared import (
    _get_session_from_context,      # Get session from MCP context
    _get_enterprise_session,         # Get + validate Enterprise session
    _check_response_size,            # Validate response size limits
    _format_meta_table_result,       # Format metadata tables
    _get_system_config,              # Get Enterprise system config
)
```

## Naming Conventions

### MCP Tool Functions (Public API)
- **Pattern**: `{domain}_{action}` (e.g., `session_table_data`, `pq_create`, `catalog_tables_list`)
- **No underscore prefix**: These are the public MCP tools exposed to AI agents
- **Descriptive and specific**: Name should clearly indicate what the tool does
- **Registered automatically**: `@mcp_server.tool()` decorator registers them with the server

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
