---
name: _logging-standards
description: "Python logging conventions — module-level _LOGGER instantiation, message format ([module:function] Action: details), log levels, sensitive-data redaction, and coverage rules — invoke when writing or reviewing logging statements in Python code"
user-invocable: false
---

## Logger Instantiation

Each module declares one private module-level logger:

```python
_LOGGER = logging.getLogger(__name__)
```

## Message Format

```text
[server_or_module:function_name] Action: details
```

Examples:

```python
_LOGGER.info(f"[mcp_systems_server:catalog_tables_list] Invoked: id={id!r}")
_LOGGER.info(f"[mcp_systems_server:session_enterprise_create] Success: id={id!r}")
_LOGGER.error(f"[mcp_systems_server:session_enterprise_create] Failed to create session: {e!r}", exc_info=True)
```

## Log Levels

- `DEBUG` — detailed operational steps useful for diagnosing behavior (loop iterations, intermediate values)
- `INFO` — significant events: tool invocation, successful completion, notable state changes
- `WARNING` — degraded but non-fatal conditions
- `ERROR` — failures and exceptions; when logging a caught exception, include `{e!r}` in the message and `exc_info=True`. `{e!r}` is the *log* form only — user-facing strings render per `_python-coding-practices` rule 20; do not swap one form for the other.

## When to Log

Add log statements for:

- Entry to any significant operation (`Invoked:` with key parameters)
- Successful completion (`Success:` or a summary of what was done)
- Failures and exceptions (`Failed to ...: {e!r}`)

Do not log inside tight loops or use INFO or above for routine intermediate steps — use DEBUG.

## Accuracy

Log messages must accurately describe what the code does at that point. A message that says "Invoked" should appear at entry, not mid-function. An INFO message should not appear on an error path.

When refactoring (renaming a function, splitting it, moving code between modules), update the `[module:function]` prefix and any inline action description in every affected log line. A stale prefix is a documentation bug that survives lint and tests.

## Sensitive data

Never log:

- PSKs, auth tokens (resolved `auth_token` values), passwords, API keys.
- File contents pulled in by `${file:PATH}` templating (private keys, credentials).
- Environment-variable values resolved by `${env:VAR}` if the variable is known to hold a secret (`*_PASSWORD`, `*_TOKEN`, `*_KEY`, `PSK`).

When logging Pydantic config models, use the project's redaction-aware helpers — never `repr(model)` or a plain `model.model_dump()`. The default dump masks `SecretStr` with `"**********"`, and `repr` may leak nested non-secret fields verbatim:

- `log_redacted(model, label=..., logger=...)` (from `deephaven_mcp._pydantic`) — logs the model at INFO with secrets replaced by the project's `REDACTED` sentinel. This is the standard call site for "log the loaded config" lines.
- `model.model_dump(context={"redact": True})` — for inline use when you need the redacted dict (e.g. embedding it in a larger structured log payload). Only `RedactableSchema` subclasses honor the `redact` context flag; plain `StrictSchema` subclasses fall back to default serialization.

## Structured payloads

When logging a non-trivial object, log the specific fields you care about, not the whole object:

```python
# Good
_LOGGER.info(f"[mcp_systems_server:session_create] Created: id={id!r} system={system!r}")

# Bad — leaks every field, including future-added secrets
_LOGGER.info(f"[mcp_systems_server:session_create] Created: {session_config!r}")
```

For dicts that may contain secrets, redact at the call site or use a redacted projection — never rely on a downstream filter to scrub the message.
