---
name: _typescript-mcp-module-organization
description: Module organization and design patterns for MCP tool development in TypeScript — invoke when creating or modifying TypeScript MCP tool modules
---

# TypeScript MCP Tools Module Organization Guidelines

## File Organization Principles

1. **Module Cohesion**: Each file in `_tools/` contains MCP tools and helpers for a single, well-defined domain (e.g., table operations, session lifecycle, script execution). Mirrors the Python `_tools/` structure.

2. **Helper Function Placement**:
   - Used by 1 module → keep it in that module (unexported, `_`-prefixed)
   - Used by 2 modules → duplication is acceptable; consider shared
   - Used by 3+ modules → move to `_tools/shared.ts`

3. **Constant Placement**:
   - Module-specific constants → keep in that module after imports, with a TSDoc comment
   - Shared constants → place in `_tools/shared.ts` or the most relevant module

4. **New MCP Tool Placement**:
   - Before creating a new file, check if the tool fits an existing domain
   - Only create a new module if the tool represents a distinct new domain

5. **Registering New Tool Modules**:
   - Every tool module must export a `registerTools(server: McpServer): void` function
   - After creating a new module, add it to the server entry point:
     - Shared (community + enterprise) → add to the shared registration array
     - Enterprise-only → call `registerTools` in the enterprise registration function
     - Community-only → call `registerTools` in the community registration function

## Required Pattern for MCP Tool Modules

All tool modules must follow this pattern — **no decorators on tool functions**:

```typescript
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";

async function myTool(params: { param: string }): Promise<{ result: string }> {
  // Tool implementation
}

export function registerTools(server: McpServer): void {
  server.tool(
    "domain_action",
    "Tool description (consumed by AI agents — be specific).",
    { param: z.string().describe("Parameter description") },
    myTool,
  );
}
```

Input schemas use Zod (replaces FastMCP's automatic inference from Python type annotations). Always add `.describe()` to each schema field — the description is surfaced to AI agents as parameter documentation.

## Naming Conventions

### MCP Tool Functions (Public API)
- **Pattern**: `{domain}_{action}` (e.g., `session_table_data`, `pq_create`, `catalog_tables_list`)
- **No underscore prefix**: These are the public MCP tools exposed to AI agents
- **Not exported directly**: Passed to `server.tool()` inside `registerTools`; the function itself need not be exported

### Helper Functions (Internal Use Only)
- **Always unexported**: Use `_` prefix (e.g., `_validateLaunchMethod`, `_buildResponse`)
- **Not in `registerTools`**: Never register a helper as a tool
- **Local scope**: Keep in the module where used, unless used by 3+ modules

### Module-Level Objects
- **Constants**: `UPPER_SNAKE_CASE` with TSDoc (e.g., `MAX_RESPONSE_SIZE`, `DEFAULT_TIMEOUT`)
- **Logger**: `const _logger = pino({ name: "module-name" })` (unexported, per `_typescript-logging-standards`)

## Module Independence

- Avoid circular dependencies between `_tools/` modules
- `_tools/shared.ts` must not import from other `_tools/` modules
- Cross-module communication goes through shared utilities or the MCP server context
