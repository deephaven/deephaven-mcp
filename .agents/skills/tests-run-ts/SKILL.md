---
name: tests-run-ts
description: Run the full TypeScript unit test suite with coverage (pnpm vitest run) and report test failures and uncovered lines
---

Run all TypeScript unit tests with coverage:

```bash
pnpm vitest run
```

Vitest is pre-configured in `vitest.config.ts` to collect coverage automatically — no extra flags needed. The run produces a coverage report showing which lines are not covered. Report any test failures and uncovered lines to the user.
