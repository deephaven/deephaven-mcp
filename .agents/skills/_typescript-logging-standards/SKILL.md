---
name: _typescript-logging-standards
description: TypeScript logging conventions — module-level _logger instantiation with pino, message format ([module:function] Action: details), log levels, and coverage rules
---

## Logger Instantiation

Each module declares one private module-level logger:

```typescript
import pino from "pino";

const _logger = pino({ name: "module-name" });
```

Use the file's module name (e.g., `"session"`, `"mcp-systems-server"`) as the `name` value. Keep the variable name `_logger` — the underscore marks it as module-private per project conventions.

## Message Format

```text
[server_or_module:function_name] Action: details
```

Examples:

```typescript
_logger.info(`[mcp-systems-server:catalogTablesList] Invoked: sessionId=${sessionId}`);
_logger.info(`[mcp-systems-server:mcpReload] Success: session configuration reloaded.`);
_logger.error({ err }, `[mcp-systems-server:mcpReload] Failed to reload`);
```

## Log Levels

- `trace` — highly detailed steps for diagnosing low-level behavior (equivalent to Python DEBUG verbose)
- `debug` — detailed operational steps useful for diagnosing behavior (loop iterations, intermediate values)
- `info` — significant events: tool invocation, successful completion, notable state changes
- `warn` — degraded but non-fatal conditions (equivalent to Python WARNING)
- `error` — failures and exceptions; always pass the error object as a structured field: `{ err }`

## Error Logging

Always pass the error as a structured pino field, not in the message string:

```typescript
// Correct
_logger.error({ err }, `[module:fn] Failed to do X`);

// Incorrect — error details lost as string, not parseable
_logger.error(`[module:fn] Failed to do X: ${err}`);
```

Pino serializes `err` with full stack trace automatically when passed as a field.

## When to Log

Add log statements for:
- Entry to any significant operation (`Invoked:` with key parameters)
- Successful completion (`Success:` or a summary of what was done)
- Failures and exceptions (`Failed to ...:` with `{ err }` as a field)

Do not log inside tight loops or use `info` or above for routine intermediate steps — use `debug` or `trace`.

## Accuracy

Log messages must accurately describe what the code does at that point. A message that says "Invoked" must appear at entry, not mid-function. An `info` message must not appear on an error path.
