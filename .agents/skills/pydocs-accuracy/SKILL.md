---
name: pydocs-accuracy
description: Verify Python docstrings are factually accurate — descriptions, Args, Returns, and Raises all match the actual code; surgical fixes only, no restructuring or additions
---

**When to use this vs. `pydocs-improve`**: use `pydocs-accuracy` for surgical correctness-only fixes (e.g., during a code review where restructuring would expand scope). Use `pydocs-improve` for a full review that may also restructure docstrings or add missing required sections.

For the specified file or function, verify that all docstrings are factually accurate. Fix any inaccuracies directly.

**Docstrings** — check:
- Description matches what the function actually does
- `Args` section matches actual parameter names, types, and behavior
- `Returns` section matches what the function actually returns
- `Raises` section lists only exceptions the function actually raises
- No documented behavior that the code no longer implements

**Pydantic fields** — when the change touches a `StrictSchema` /
`RedactableSchema` subclass, check the field's trailing PEP 257
docstring (the triple-quoted string immediately below the
assignment) for **accuracy only**:
- The description matches the field's current type, constraints
  (`gt=0`, `ge=0`, etc.), and behavior
- Mentioned default values still match the actual default
- No documented behavior that the code no longer implements

**Restructuring is out of scope for this skill.** If a field has no
trailing docstring (or is documented only via a Sphinx `Attributes:`
block on the class), do **not** add or convert here — flag it for
`pydocs-improve`, which owns the convention. The
`tests/test__pydantic_field_docs.py` regression test will also
surface the missing description on the next test run.

**Do not remove TODOs.** Fix inaccuracies; do not rewrite or restructure docstrings beyond what accuracy requires.
