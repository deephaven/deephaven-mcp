---
name: pydocs-accuracy
description: Verify that pydocs and loggers are factually accurate and reflect the actual code
---

For the specified file or function, verify that all docstrings and log messages are factually accurate. Fix any inaccuracies directly.

**Docstrings** — check:
- Description matches what the function actually does
- `Args` section matches actual parameter names, types, and behavior
- `Returns` section matches what the function actually returns
- `Raises` section lists only exceptions the function actually raises
- No documented behavior that the code no longer implements

**Log messages** — apply the `logging-standards` skill to verify accuracy and level appropriateness.

**Do not remove TODOs.** Fix inaccuracies; do not rewrite or restructure docstrings beyond what accuracy requires.
