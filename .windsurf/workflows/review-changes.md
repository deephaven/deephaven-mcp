---
description: Perform a deep review of a set of code changes
---

Perform a deep review of the code changes in the current diff.  If a branch is specified, review the local code relative to that branch.  If no branch is specified, review the uncommitted changes relative to the current branch.

1. Perform a very thorough review of the changes.
2. Review the design of the code.  Is the design sound and consistent with the rest of the project and following industry best practices?
3. Can the code be simplified?
4. Are there code smells?
5. Assess DRY.
6. Assess the security model and identify any potential security vulnerabilities.
7. Review all of the pydocs to see if any can be improved.
8. Remove unused imports.
9. Do not remove TODOs without a very good reason.
10. Review logging to see where logging needs to be added and where log messages need to be made consistent with the rest of the project.