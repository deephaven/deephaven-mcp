---
name: _output-serialization-conventions
description: "Project conventions for serializing values into user-facing output — MCP tool return dicts and CLI output fields — invoke when authoring or reviewing any string field that ships to a user or agent (a return payload or a CLI OutputField)"
user-invocable: false
---

This skill is the hub for project conventions on how values are serialized into user-facing output. New rules accrete here rather than spawning sibling skills.

## Enum value casing

Pick the right enum accessor at definition time; never normalize casing at the call site.

### The two categories

**Categorical labels** — kind, type, classification, identifier-like tokens. Emit `StrEnum.value`. Lowercase by convention.

- `type`: `SystemType.COMMUNITY.value` → `"community"`
- `origin`: `SessionOrigin.STATIC.value` → `"static"`
- `system`: lowercase system name
- `phase`: `InitializationPhase.COMPLETED.value` → `"completed"`
- `launch_method`: `"docker"` / `"python"`

**Runtime state** — current liveness, status that changes over the session's lifetime. Emit `.name`. UPPERCASE.

- `liveness_status`: `ResourceLivenessStatus.ONLINE.name` → `"ONLINE"`

Canonical implementations:

- Categorical: `mcp_systems_server/_tools/session.py` (`_build_sessions_list_row`) reads `mgr.system_type.value` and `mgr.origin.value`.
- Runtime state: `mcp_systems_server/_tools/session.py` (`_get_session_liveness_info`) reads `status.name`.

### The call-site rule

Never call `.upper()` or `.lower()` on an enum value at the call site. If a value's casing feels wrong, fix the enum definition or you have found a vocabulary mismatch that needs a deeper fix, not a transform.

```python
# Good — the enum's chosen accessor is what ships
{"type": mgr.system_type.value, "liveness_status": status.name}

# Bad — call-site normalization
{"type": mgr.system_type.name.lower(), "origin": mgr.origin.name.lower()}
```

### CLI human-mode column headers

Column headers match the JSON key exactly (lowercase). They are not stylistic; they map one-to-one to filter flags (`--type`, `--origin`) and JSONPath keys (`.sessions[].type`). Uppercasing headers introduces a one-way translation step purely for cosmetics. Canonical implementation: `cli/_commands/session.py` (`session_list` `_OUTPUT_LIST`).

### Known carve-outs

**`auth_type` is not governed by this rule.** Its canonical vocabulary is defined externally by Deephaven, not by the project: `pydeephaven.Session(auth_type=...)` accepts `"Anonymous"`, `"Basic"`, or a Java handler FQCN like `"io.deephaven.authentication.psk.PskAuthenticationHandler"` — title case for built-ins, FQCN for custom. The codebase currently emits `"PSK"` / `"ANONYMOUS"` in several places, which is a known cross-tool inconsistency awaiting a coordinated fix. Do not "fix" `auth_type` casing under this rule; the fix is to emit Deephaven's canonical strings, not to lowercase or uppercase the existing ones.

### Anti-patterns

- **Call-site `.upper()` / `.lower()` to mint a "presentation" form.** Example failure mode: a launcher flag (lowercase Literal) is uppercased inside one tool's `to_dict()` while another tool's caller emits the same field lowercase — the same logical session then appears as `type: "community"` and `session_type: "COMMUNITY"` in one merged payload. Fix: emit the canonical vocabulary at definition time, not at the call site.
- **Duplicate keys with different casing in one payload.** Two producers contribute to a merged response with different accessors (`.name` in one, `.value` in another) and the merge ships both. Fix: name one key per concept; pick the accessor once per field.
- **Mixing `.name` and `.value` for the same field across tools.** If `sessions_list` emits `type: "community"`, then `session_details` must also emit `type: "community"`, not `type: "COMMUNITY"`. The vocabulary is per field, not per tool.
