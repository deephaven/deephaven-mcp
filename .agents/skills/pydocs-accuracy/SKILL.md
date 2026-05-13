---
name: pydocs-accuracy
description: Verify Python docstrings are factually accurate — descriptions, Args, Returns, and Raises all match the actual code; surgical fixes only, no restructuring or additions
---

For the specified file or function, verify that all docstrings are factually accurate. Fix any inaccuracies directly.

**Docstrings** — check:
- Description matches what the function actually does
- `Args` section matches actual parameter names, types, and behavior
- `Returns` section matches what the function actually returns
- `Raises` section lists only exceptions the function actually raises
- No documented behavior that the code no longer implements

**Do not remove TODOs.** Fix inaccuracies; do not rewrite or restructure docstrings beyond what accuracy requires.
