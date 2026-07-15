---
name: config-field-add
description: Add a new configuration field, setting, or tunable to server.json / cli.json / community/ / enterprise/ (Pydantic schema + PEP 257 docstring + config-samples + tests) — prevents ad-hoc os.environ reads and DEFAULT_FOO constants
---

Adding a configuration field is governed by the `_configuration-conventions` skill. Apply it and follow its **add-a-tunable checklist** — schema location, the field plus a PEP 257 trailing docstring, an attribute-access consumer, the four artifacts that move in lockstep, and tests — together with the design anti-patterns it catalogs (no ad-hoc `os.environ` / `DEFAULT_FOO`, the field-design pitfalls). This workflow adds the glue around that checklist:

1. **Verify the field's docstring reached runtime.** Pydantic harvests the trailing docstring into `model_fields[name].description`; confirm it before relying on it (`tests/test__pydantic_field_docs.py` fails otherwise):

   ```bash
   python -c "from <module> import <Model>; print(<Model>.model_fields['<field>'].description)"
   ```

2. **Run** `run-precommit` and `tests-run-file` on the changed test files.
