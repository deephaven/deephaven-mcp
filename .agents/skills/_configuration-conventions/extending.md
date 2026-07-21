# Extending Configuration

Procedure and design guidance for adding or changing a configuration knob. Load this when actually extending config; the conceptual rules it builds on are in `SKILL.md` (the six rules, architecture, layout).

## Adding a new tunable — checklist

1. **Decide where it lives.** Server-wide knobs go in
   `config/schema/_server.py` (`ServerConfig`);
   community-client knobs in `_community.py`; enterprise-system knobs
   in `_enterprise.py`. Per-session knobs use the relevant
   session-config model (`CommunitySettings`, or its
   `session_creation.defaults`). Per-tool knobs (e.g. `pq_tools`) live
   in their tool's `_*_config.py`, colocated with the tool —
   colocation is intentional; do not move config types "down" into
   shared packages.
2. **Add the field.** Annotate it (`Annotated[..., Field(gt=0)]`
   etc.) and set the default *at the field*. Document it with a
   PEP 257 trailing docstring on the field declaration (see rule 2b
   in `SKILL.md`) — not in an `Attributes:` block on the class docstring.
   The `tests/test__pydantic_field_docs.py` regression test will
   fail the build if you forget.
3. **Wire the consumer** to read the field by attribute access on
   the validated model (e.g. `settings.session_creation.defaults.heap_size_gb`).
   Do *not* call `.get()` on `model_dump(...)` results, round-trip
   through dicts, or read `os.environ[...]` — env-overridable values
   use `${env:VAR}` templating in JSON5. Attribute access preserves
   types, defaults, and mypy support; dict access loses all three.
4. **Update the four artifacts that move together.** Every config
   schema change must touch all of these in lockstep, or operators
   will see drift between the model, the docs, the examples, and
   the test coverage:
   - The Pydantic model (field + trailing docstring).
   - `docs/CONFIGURATION.md` — table row, default value, any
     cross-references in surrounding prose.
   - `config-samples/ai/config/` — the corresponding example file when
     the field is operator-facing.
   - Tests under `tests/config/schema/` (loader path)
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

### D. Single source of truth: derive, don't redeclare

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

Almost never. `DH_AI_DATA_DIR` (read by
`deephaven_mcp.config.resolve_data_root`) and the
`PYTHONLOGLEVEL` logging control (read by `setup_logging()`) are
the only legitimate cases today — `docs/ENV.md` is the canonical
inventory. Before
adding another, ask: can the operator put this value in a JSON
file and use `${env:NAME}` to pull the env-var contents into it?
Usually yes — and that path keeps the schema, validation,
defaults, and redaction all in one place.
