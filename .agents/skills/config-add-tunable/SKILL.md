---
name: config-add-tunable
description: Add a new operator-tunable knob to the JSON configuration tree — wraps the seven-step checklist from _configuration-conventions and prevents ad-hoc env reads or DEFAULT_FOO constants
---

Apply the `_configuration-conventions` skill — it is the canonical reference for this project's configuration model (JSON5 + Pydantic v2 + `${env:VAR}` / `${file:PATH}` templating, no ad-hoc `os.environ` reads, no `DEFAULT_FOO` constants).

## Steps (do not skip)

1. **Locate the right schema.** Server-wide knobs go in `mcp_systems_server/config/_server.py` (`ServerConfig`). Community-client knobs go in `_community.py`. Enterprise-system knobs go in `_enterprise.py`. Per-tool knobs (e.g., `pq_tools`) live in their tool's `_*_config.py` colocated with the tool — colocation is intentional, do not move config types "down" into shared packages.
2. **Add the field** to the appropriate `StrictSchema` / `RedactableSchema` subclass with a precise type, validator constraints (`gt=0`, `ge=0`, etc.), and a sensible default.
3. **Add a PEP 257 trailing docstring** immediately below the field assignment. Sphinx `Attributes:` blocks do **not** reach `model_fields[name].description`. Verify:

   ```bash
   python -c "from <module> import <Model>; print(<Model>.model_fields['<field>'].description)"
   ```

   `tests/test__pydantic_field_docs.py` will fail otherwise.
4. **Read the field through the config object** at the call site. Use **attribute access** on the validated model (e.g. `settings.session_creation.defaults.heap_size_gb`) — do not call `.get()` on `model_dump(...)` results, do not round-trip through dicts, and never `os.environ[...]` (if the value should be env-overridable, that is what `${env:VAR}` templating in JSON5 is for). Attribute access preserves types, defaults, and IDE/mypy support; dict access loses all three.
5. **Update the example config** under `examples/ai/config/` and the docs under `docs/CONFIGURATION.md`.
6. **Add tests**: a unit test for the schema (default, override, validation failure) and a test that the consumer reads the field correctly.
7. **Run** `run-precommit` and `tests-run-file` on the changed test files.

## Anti-patterns (rejected)

- `os.environ.get("DH_MCP_FOO", "default")` anywhere in `src/` other than `_env.py`'s `DH_MCP_CONFIG_DIR` read.
- Module-level `DEFAULT_FOO = ...` constants for operator-tunable values.
- Moving config types out of the package that consumes them for "layering" reasons.
