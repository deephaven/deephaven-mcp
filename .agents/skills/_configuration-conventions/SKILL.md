---
name: _configuration-conventions
description: How Deephaven MCP servers consume runtime configuration — JSON5 + Pydantic v2 + templating, schema defaults, no ad-hoc env reads — invoke before adding a new tunable, refactoring a config model, or wiring an environment variable into the code
---

# Configuration Conventions

Reference for how the Deephaven MCP servers consume runtime
configuration. Read this before adding a new tunable, refactoring
a config model, or wiring an environment variable into the code.

Authoritative end-user reference: `docs/CONFIGURATION.md`. This
skill is the *internal-to-the-codebase* counterpart and explains
**how** to extend the configuration safely. The only env var the
server itself reads is `DH_MCP_CONFIG_DIR` (consumed by
`deephaven_mcp.config.resolve_config_dir`); every
other tunable lives in the JSON tree.

## Architecture in one sentence

Configuration lives in **JSON5 files** under `DH_MCP_CONFIG_DIR`,
is shaped by **Pydantic v2 models** (which double as the wire-
format schema and the runtime types), and pulls in env-vars or
file contents through a **templating engine** that runs *before*
Pydantic validation.

## The six rules

### 1. JSON is the single source of truth

Every tunable lives in one of the JSON files described in
`docs/CONFIGURATION.md`. The only exception is the
`DH_MCP_CONFIG_DIR` env var itself, which tells the server *where*
to find the JSON tree.

Do **not** add ad-hoc `os.environ[...]` reads inside tool code,
session helpers, registries, or factories. If a new knob needs to
be exposed to operators, add it to the appropriate Pydantic schema
and (when relevant) to the `*_settings` / `*_systems` documentation
in `docs/CONFIGURATION.md`.

### 2. Pydantic models carry the schema defaults AND the field documentation

#### 2a. Defaults at the field declaration

When a JSON field is optional, encode the default at the field
declaration site:

```python
class CommunitySessionCreationDefaults(RedactableSchema):
    heap_size_gb: Annotated[float, Field(gt=0)] = 4.0
    """JVM heap size in gigabytes for the worker process."""

    startup_retries: Annotated[int, Field(ge=0)] = 3
    """Number of times the launcher retries worker creation."""
```

Do **not** maintain a parallel `DEFAULT_FOO` constant alongside
the schema field.

#### 2b. Every field carries a PEP 257 trailing docstring

Every field on every `StrictSchema` / `RedactableSchema` subclass
**must** carry a triple-quoted string literal immediately under the
field declaration. The project enables `use_attribute_docstrings=True`
on `StrictSchema.model_config`, so Pydantic v2 harvests that string
into `model_fields[name].description` at class-build time.

This matters because the description is what flows into:

- `Model.model_json_schema()` — the JSON Schema for the model.
- The MCP tool/resource schemas that AI agents consume at runtime
  (agents have no other way to learn what a parameter means).
- `pydantic.fields.FieldInfo.description` — used by hover-info,
  OpenAPI generators, and `inspect`-based tooling.

**Forbidden — invisible to runtime introspection:**

- **Sphinx-style `Attributes:` block on the class docstring.** This
  is plain text in `__doc__` and never reaches
  `model_fields[name].description` or `model_json_schema()`. An AI
  agent calling the model's MCP-exposed schema sees the field with
  no description. `tests/test__pydantic_field_docs.py` fails the
  build when any production field has no runtime description.

**Discouraged — works at runtime but violates project style:**

- **Explicit `Field(description="...")`.** This *does* populate
  `description` correctly, so it is not a functional bug — but the
  project standardizes on the trailing-docstring style for
  consistency and readability (especially for multi-line prose,
  where triple-quoted strings beat parenthesized string
  concatenations). New code uses trailing docstrings; existing
  `Field(description=...)` usages should be converted opportunistically.

The class docstring keeps only **class-level** prose: what the model
represents, when it is used, validator behavior, wire-format
examples. It does *not* list fields — the field declarations are the
single source of truth for field documentation.

#### 2c. Consumers read fields by attribute access

Consumers read fields by **attribute access** on the typed model:

```python
defaults = settings.session_creation.defaults
heap = tool_param or defaults.heap_size_gb        # OK
heap = defaults.get("heap_size_gb", 4.0)          # NOT OK
```

### 3. Indirection happens through templating, not shadow fields

Environment-variable and file indirection are written into the
JSON value using one of three placeholders resolved at file-load
time by `deephaven_mcp.config._templating`:

| Placeholder              | Resolves to                                        |
| ------------------------ | -------------------------------------------------- |
| `${env:VAR}`             | Value of `VAR`; error if unset or empty.           |
| `${env:VAR:-default}`    | Value of `VAR` when set, else the literal default. |
| `${file:/abs/path}`      | UTF-8 contents of the file.                        |

Do **not** add a `<field>_env_var` or `<field>_path` shadow field
alongside a secret field, and do not write a helper that reads
env-vars or files inside a `model_validator`.

When defining a new secret field:

- Declare a single `SecretStr` (or `str`) field on the model.
- Don't add a sibling `_env_var` field.
- Don't write a `model_validator(mode="before")` that reads env
  vars or files — that work has already been done by
  `expand_tree(...)` inside
  `deephaven_mcp.config._file_loader.load_config_from_file`.

### 4. Restart, don't reload

Configuration is read **once** at server startup. The historical
`mcp_reload` tool was removed. If you change a JSON file you
restart the server. Add tests that exercise the loader path
(`tests/mcp_systems_server/config/test__multi.py`) rather than
mutating live `MultiSystemConfig` instances; the model is frozen.

### 5. Colocate config models with their consumers

Pydantic config models live in the same package as the code that
consumes them, even when that creates an "upward"-looking import.
Working examples in this codebase:

- **`PqToolsConfig`** lives in
  `deephaven_mcp.mcp_systems_server._tools._pq_config` (next to the
  PQ tools that read it), even though it is referenced by
  `mcp_systems_server.config._server.ServerConfig.pq_tools`.
- **`MultiSystemConfig`** lives in
  `deephaven_mcp.mcp_systems_server.config._multi` and is consumed
  directly by `deephaven_mcp.resource_manager._registry_multi`.
  The composite registry stays with the other registries; the
  config stays with its loader.
- **Per-system schemas** (`CommunitySessionConfig`,
  `EnterpriseSystemConfig`) live in `deephaven_mcp.sessions/`
  because they are shared declaration types loaded by the
  systems-server config layer *and* used at runtime when MCP tools
  build new sessions.

If you are tempted to move a config type "down" into a more
generic-looking package to flatten the import graph, **don't**.
Keep the model next to its consumer and document the placement in
the module docstring. Layering aesthetics is not a justification
for moving config; the user has rejected such refactors more than
once.

### 6. Configuration flows through Pydantic instances — no parallel scalar overrides

Once a knob is exposed as a field on a Pydantic config model,
every code path that wants to vary that knob **must** go through
the model. Do not add a parallel scalar parameter
(`idle_timeout: float | None`, `connect_timeout_seconds: int`,
`override_foo: str`, etc.) to a function that already accepts the
model — even "for tests" or "for short-lived tooling." The
Pydantic instance is the wire format **and** the runtime override
mechanism; a parallel scalar path duplicates state, skips
validation, and grows helpers that re-implement what
`model_copy(update=...)` and `model_validate(...)` already do.

When you genuinely need a partial override, use Pydantic's own
machinery:

```python
# Fast path: no re-validation. Safe when the override values are
# already trusted (e.g. produced by another Pydantic model) or
# when the field has no validators worth re-running.
overridden = configured.model_copy(update={"field": value})

# Validating path: re-runs the schema (catches an out-of-range
# override, a wrong type, etc.). Use this when the override
# values come from a less-trusted source (CLI args, test inputs,
# etc.).
overridden = MyModel.model_validate(
    configured.model_dump() | {"field": value},
)
```

If a test wants predictable timers, it constructs the
`MultiSystemConfig` (or its relevant sub-model) with the values it
actually wants — not a parallel scalar that side-steps the
schema. If you find yourself writing a helper that maps
`Optional[float]` kwargs onto a Pydantic field with conditional
expressions, you are working around this rule; delete the helper
and the kwargs together.

(Anti-pattern that triggered this rule: ``make_lifespan(...,
idle_timeout: float | None, sweep_interval: float | None)`` —
two scalar kwargs that overrode an
``EvictionTimeouts`` block already present on the
``MultiSystemConfig`` argument, with a 12-line
``_resolve_eviction_timeouts`` helper that reconstructed the
model field-by-field. Both were deleted; tests now build the
``EvictionTimeouts`` they want into the config they pass in.)

## Layout reference

| File                                | Schema model                                                       |
| ----------------------------------- | ------------------------------------------------------------------ |
| `server.json`                       | `deephaven_mcp.mcp_systems_server.config.ServerConfig`             |
| `community/settings.json`           | `deephaven_mcp.mcp_systems_server.config.CommunitySettings`        |
| `community/sessions/<name>.json`    | `deephaven_mcp.sessions.CommunitySessionConfig`                    |
| `enterprise/systems/<name>.json`    | `deephaven_mcp.sessions.EnterpriseSystemConfig`                    |

The systems-server schemas and per-section loaders live in
`deephaven_mcp.mcp_systems_server.config` — each per-section module
(`_server`, `_community`, `_enterprise`) owns its umbrella schema
and a matching `load_<section>` function; `_multi` aggregates them
through `MultiSystemConfigManager`. The general-purpose
file-loading + templating primitives that those loaders sit on top
of live in `deephaven_mcp.config` (`_file_loader`, `_templating`,
`_config_dir`, `_dir_permissions`).

## Adding a new tunable — checklist

1. **Decide where it lives.** Per-session? Use the relevant
   session-config model. Project-wide for community? Add to
   `CommunitySettings` (or its `session_creation.defaults`). Server
   transport? Use `ServerConfig`.
2. **Add the field.** Annotate it (`Annotated[..., Field(gt=0)]`
   etc.) and set the default *at the field*. Document it with a
   PEP 257 trailing docstring on the field declaration (see rule 2b
   above) — not in an `Attributes:` block on the class docstring.
   The `tests/test__pydantic_field_docs.py` regression test will
   fail the build if you forget.
3. **Wire the consumer** to read the field by attribute access
   from the validated model. Do *not* re-introduce dict-based
   `.get()` lookups against `model_dump(...)`.
4. **Update the four artifacts that move together.** Every config
   schema change must touch all of these in lockstep, or operators
   will see drift between the model, the docs, the examples, and
   the test coverage:
   - The Pydantic model (field + trailing docstring).
   - `docs/CONFIGURATION.md` — table row, default value, any
     cross-references in surrounding prose.
   - `config-samples/ai/config/` — the corresponding example file when
     the field is operator-facing.
   - Tests under `tests/mcp_systems_server/config/` (loader path)
     and `tests/sessions/` (per-session schema path) covering
     both the typed access path (model-validate a dict, read the
     attribute) and the loader path when templating is involved
     (write a temp file, call the loader, assert the resolved
     value). Tests for the shared file-loading and templating
     primitives live under `tests/config/`.

## Five anti-patterns to avoid

### A. The field name is forever

Once a field ships in a release, renaming it is a breaking change
for every operator's `community/settings.json`, `enterprise/`
file, or `server.json`. Decide on the right name *before* merge
— shipping under a placeholder name and renaming later costs more
than a careful review pass.

### B. Prefer structural fixes to cross-field validators

When two fields are coupled (e.g. "these four fields apply only
when `launch_method=='docker'`"), the *structural* fix — group
the coupled fields under a nested model or a discriminated union
— is better than reaching for `model_validator(mode='after')`.
Grouping makes the coupling visible in the wire format; a
validator hides it behind a runtime check. Use cross-field
validators only when the natural grouping is not nesting (e.g.
`port > 0 if transport == 'http'`).

### C. Do not use `0` as a sentinel for "unbounded" or "disabled"

Operators reading a field set to `0` will reasonably guess that
it means "do nothing" / "reject everything" — not "no limit". Use
`None` (with `Field(gt=0)`) for the unbounded/disabled sentinel,
so the schema enforces the distinction:

```python
# Wrong — 0 silently means "unbounded"
max_concurrent_sessions: Annotated[int, Field(ge=0)] = 5

# Right — None means "unbounded", 0 is rejected
max_concurrent_sessions: Annotated[int | None, Field(default=5, gt=0)] = 5
```

### D. Single source of truth: derive, don't re-declare

If field A is fully derivable from field B, derive it at the
consumer boundary; do not expose both as independent JSON knobs.
Independent knobs that *must* agree will eventually disagree
(operator typo, copy-paste from a different system, etc.) and
the error surfaces as a runtime auth failure or worse, not as a
config validation error. The escape hatch for genuinely-distinct
values is a discriminated credentials/credentials-like union,
not two parallel fields.

### E. Don't expose internal sentinels as JSON knobs

If the field's docstring says some variant of *"operators should
leave this at the default"*, that is a sign the field belongs
in code, not in the JSON wire format. Either remove the field
(hardcode at the call site) or rewrite the docstring to describe
a real use case for changing it.

## When you think you need an env var directly

Almost never. `DH_MCP_CONFIG_DIR` is the only legitimate case
today (read by
`deephaven_mcp.config.resolve_config_dir`). Before
adding another, ask: can the operator put this value in a JSON
file and use `${env:NAME}` to pull the env-var contents into it?
Usually yes — and that path keeps the schema, validation,
defaults, and redaction all in one place.
