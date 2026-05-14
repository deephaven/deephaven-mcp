---
name: tests-improve-ts
description: Comprehensively improve TypeScript unit tests — ensure test files exist, add missing tests, remove unneeded tests, restructure, and achieve 100% coverage per source file
---

For every file in `src-ts/` (except generated files):
1. Make sure that there is a test file.
2. Make sure the test file is in the same directory as the source file, named `<file>.test.ts`.
3. Analyze the test file to make sure that all of the tests are appropriate.
4. Add missing tests. Testing unexported (private) functions directly is appropriate to create simpler, more targeted tests.
5. Remove unneeded tests.
6. Restructure tests where appropriate.
7. Run the individual test file. Test files must be run one-by-one to accurately assess per-file coverage:
   ```bash
   pnpm vitest run <file>
   ```
8. Target 100% coverage. If coverage is below 100%, add tests until it reaches 100%.
9. Run the `tests-run-ts` skill to verify the full suite passes with no regressions.

Vitest is pre-configured for coverage in `vitest.config.ts` — no extra flags needed. Do not add `--coverage` flags manually; they are already configured.

Per-file runs are required because running the full suite together does not show whether a source file's own test file covers it completely.

In tests, use `vi.fn().mockResolvedValue(...)` for async mocks and `vi.fn().mockReturnValue(...)` for synchronous mocks. Using a plain `vi.fn()` where an async mock is needed is a common mistake — it causes tests to behave misleadingly (per `_typescript-coding-practices`).
