---
name: _logging-standards
description: Python logging conventions — module-level _LOGGER instantiation, message format ([module:function] Action: details), log levels, and coverage rules
---

## Logger Instantiation

Each module declares one private module-level logger:

```python
_LOGGER = logging.getLogger(__name__)
```

## Message Format

```
[server_or_module:function_name] Action: details
```

Examples:

```python
_LOGGER.info(f"[mcp_systems_server:catalog_tables_list] Invoked: session_id={session_id!r}")
_LOGGER.info(f"[mcp_systems_server:mcp_reload] Success: session configuration reloaded.")
_LOGGER.error(f"[mcp_systems_server:mcp_reload] Failed to reload: {e!r}", exc_info=True)
```

## Log Levels

- `DEBUG` — detailed operational steps useful for diagnosing behavior (loop iterations, intermediate values)
- `INFO` — significant events: tool invocation, successful completion, notable state changes
- `WARNING` — degraded but non-fatal conditions
- `ERROR` — failures and exceptions; when logging a caught exception, include `{e!r}` in the message and `exc_info=True`

## When to Log

Add log statements for:
- Entry to any significant operation (`Invoked:` with key parameters)
- Successful completion (`Success:` or a summary of what was done)
- Failures and exceptions (`Failed to ...: {e!r}`)

Do not log inside tight loops or use INFO or above for routine intermediate steps — use DEBUG.

## Accuracy

Log messages must accurately describe what the code does at that point. A message that says "Invoked" should appear at entry, not mid-function. An INFO message should not appear on an error path.
