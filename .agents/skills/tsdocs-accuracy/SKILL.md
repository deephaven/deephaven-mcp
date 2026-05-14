---
name: tsdocs-accuracy
description: Verify TypeScript TSDoc comments are factually accurate — descriptions, @param, @returns, and @throws all match the actual code; surgical fixes only, no restructuring or additions
---

For the specified file or function, verify that all TSDoc comments are factually accurate. Fix any inaccuracies directly.

**TSDoc comments** — check:
- Description matches what the function actually does
- `@param name` matches actual parameter names and behavior
- `@returns` matches what the function actually returns
- `@throws {Type}` lists only exceptions the function actually throws
- No documented behavior that the code no longer implements

**Do not remove TODOs.** Fix inaccuracies; do not rewrite or restructure TSDoc comments beyond what accuracy requires.
