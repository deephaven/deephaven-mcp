---
name: config-field-add
description: Add a new configuration field, setting, or tunable — invoke when adding a knob to server.json, cli.json, community/, or enterprise/. Covers the Pydantic schema, the PEP 257 trailing docstring, config-samples, and tests; prevents ad-hoc os.environ reads and DEFAULT_FOO constants
---

Adding a configuration field is governed by the `ref-configuration-conventions` skill. Apply it and follow its **add-a-tunable checklist** — schema location, the field plus a PEP 257 trailing docstring, an attribute-access consumer, the four artifacts that move in lockstep, and tests — together with the design anti-patterns it catalogs (no ad-hoc `os.environ` / `DEFAULT_FOO`, the field-design pitfalls). This workflow adds the glue around that checklist:

1. **Verify the field's docstring reached runtime.** Pydantic harvests the trailing docstring into `model_fields[name].description`; confirm it before relying on it (`tests/test_field_docs_contract.py` fails otherwise):

   ```bash
   uv run python -c "from <module> import <Model>; print(<Model>.model_fields['<field>'].description)"
   ```

2. **Run** `run-precommit` and `tests-run-file` on the changed test files.
