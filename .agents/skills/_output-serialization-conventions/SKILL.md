---
name: _output-serialization-conventions
description: "Project conventions for serializing values into user-facing output — MCP tool return dicts and CLI output fields — invoke when authoring or reviewing any string field or payload shape that ships to a user or agent (a return payload or a CLI OutputField)"
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

**Discriminator when a field mutates**: a lifecycle value that belongs to a documented protocol vocabulary — echoed in docs, filters, and agent retry logic — is categorical even though it changes over time (`phase` progresses `not_started` → `completed` and emits `.value`). Runtime state is a point-in-time health probe with no protocol vocabulary (`liveness_status`).

Canonical implementations:

- Categorical: `resource_manager/_manager.py` (`SessionManager.to_dict`) reads `self.system_type.value` and `self.origin.value`.
- Runtime state: `mcp_systems_server/_tools/session.py` (`_get_session_liveness_info`) reads `status.name`.

### The call-site rule

Never call `.upper()` or `.lower()` on an enum value at the call site. If a value's casing feels wrong, fix the enum definition — or recognize a vocabulary mismatch that needs a deeper fix, not a transform.

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

## Payload shape is designed at the MCP layer

MCP tool return dicts are the single design surface for output shape. A CLI wrapper projects the payload it receives — it never renames keys, restructures nesting, or re-cases values. When CLI output looks wrong, fix the tool's payload upstream, not the wrapper. Wrapper mechanics (helper choice, stdout/stderr split, diagnostic re-surfacing) are owned by `_cli-tool-wrapping` (*The shared flow*).

**Design lens: the consumer is an AI agent with a bounded context window.** Every token a payload spends is taken from the rest of the agent's task, so payload design is token economy. This lens settles the marginal-field questions the rules below do not enumerate: a field ships by default only when an agent acting on the response needs it at decision time; bulk detail lives behind a narrower follow-up call rather than inside a discovery response. Small addressable calls beat one exhaustive response — single-item tools parallelize and let the agent fetch only what it needs (canonical: `session_table_schema` / `catalog_table_schema` are deliberately single-table; discovery is a separate list call).

The `{success, error, isError}` envelope is fixed by the MCP contract; these rules govern what goes *inside* it:

- **Domain-named array.** A list result lives under a key that names the domain: `sessions`, `systems`, `table_names`, `tables`, `pqs`, `packages`, `namespaces`. Never an anonymous key (`result`, `data`, `items`).
- **Shape matches the data.** A list of names is a plain array of strings, not a single-column table. Tabular envelopes (`columns` + `data` + `format`) are for genuinely multi-column data (`session_table_data`, `catalog_table_sample`). Listing and schema tools return lean identity dicts instead (`catalog_tables_list`'s `{namespace, table_name}` entries; `session_table_schema` / `catalog_table_schema`'s `{name, type}` column entries) so agent context is not flooded.
- **Sparse optional keys.** A key that is only meaningful for a minority of items is omitted when it does not apply — never emitted with a placeholder value (`null`, `false`, a default like `"Normal"`). Absence is the documented default: the tool docstring and CLI `OutputSpec` state the omission rule. Rationale: the consumers are agents reading JSON — a rare key is salient, a per-item placeholder is context noise. Canonical implementations: the envelope's `isError` (present and True only on error responses); `format_schema_result` in `mcp_systems_server/_tools/shared.py` (`column_type` omitted for Normal columns). Known inconsistency: `_format_pq_config` in `mcp_systems_server/_tools/pq.py` renders a full-detail PQ config as a fixed shape with `None` placeholders for unset optional fields (only `worker_kind` is sparse so far) — that shape predates this rule and awaits a coordinated migration; do not copy the placeholder pattern into new payloads.
- **Echo back addressing arguments, never tunables.** Every success payload echoes the arguments that identify what the result is about (`id`, `namespace`, `table_name`, `system`) at the top level, ordered first after `success` — results get detached from their requests (fan-out, caching, summarized context), and the echo is what keeps attribution. Tunables (`max_rows`, `head`, `filters`) are never echoed; `row_count` / `is_complete` / resolved `format` report their effect. Carve-out: a tool whose payload is a single identity-bearing object does not duplicate identity at top level (`session_details` returns `session` with `id` inside). Canonical implementations: `format_schema_result` / `build_table_data_response` in `mcp_systems_server/_tools/shared.py`.
- **Echo-back keys reuse the established field vocabulary** — in every tool, not only list tools. A tool that echoes its addressing argument uses the same key the rest of the payload surface uses for that concept (`id`, `system`) — never a private variant. Known inconsistency: `session_enterprise_create` and `session_enterprise_delete` currently emit `system_name`; that key awaits a coordinated rename. Do not copy it into new tools, and do not rename it piecemeal.
- **Bounded output.** A tool that can return unbounded data takes a `max_rows` cap with a conservative default and guards the assembled payload against the configured `ResponseLimits` (`get_response_limits` / `check_response_size` in `mcp_systems_server/_tools/shared.py`); the over-limit error names the remedy (reduce `max_rows`). The path to more data is a narrower query, not a lifted cap.
- **Truncation is a successful partial result, not an error.** A caller-chosen row cap that trims the list sets `is_complete: false` alongside the data; multi-system discovery attaches `partial_result` instead. Never fail the call for a shorter-than-complete list.
- **Exceptions embedded in `error` fields render via `deephaven_mcp._exception_utils`** — apply `_python-coding-practices` rule 20, which owns the canonical form, the renderer choice, and the parsed-contract rationale.
