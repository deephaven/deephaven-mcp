---
name: py-to-ts-project
description: Translate the full Python project to TypeScript — dependency-ordered file translation, full test suite, precommit, and correction of prior documentation errors; invoke when starting a clean translation or after significant Python refactors
---

Orchestrates the complete Python → TypeScript translation of the project. Applies `py-to-ts-file` to every source file in dependency order, then verifies the full suite and corrects documentation.

## Step 0 — Initialize Translation Report

Create (or overwrite) `TRANSLATION_REPORT.md` with this skeleton — individual file sections will be appended as each file is translated:

```markdown
# TypeScript Translation Report

Generated: <YYYY-MM-DD>

## Overall Summary
- Files attempted: (filled in after full suite)
- Files completed: (filled in after full suite)
- Files with problems: (filled in after full suite)
- Total Python tests: (filled in after full suite)  →  TypeScript tests: (filled in after full suite)
- Full suite: (PASS or FAIL — filled in after full suite)

## File-by-File Results

## Translation Gaps
(items that could not be fully translated — missing Node.js/JS API equivalent, incomplete DHE API coverage, etc.)

## Items Requiring Human Review
(completed but needs verification — DHE API signatures, behavioral equivalence assumptions, multiple inheritance deviations)

## DHE Implementation Notes
```

## Step 1 — Inventory

Inventory all Python source files in `src/deephaven_mcp/`. Exclude `__pycache__` and `.pyc` files.

## Step 2 — Build the Dependency Graph

Determine translation order from the actual Python import statements — do not assume a fixed order.

1. For each Python source file found in Step 1, collect its project-internal imports:
   ```bash
   grep "^from deephaven_mcp\|^import deephaven_mcp" <file>
   ```
   All project-internal imports match this pattern (e.g., `from deephaven_mcp._exceptions import McpError`).

2. Map each import statement to its source file. Examples:
   - `from deephaven_mcp._exceptions import ...` → `src/deephaven_mcp/_exceptions.py`
   - `from deephaven_mcp.client import ...` → `src/deephaven_mcp/client/__init__.py`
   - `from deephaven_mcp.auth.backends._password import ...` → `src/deephaven_mcp/auth/backends/_password.py`

3. Build the dependency graph: file A depends on file B if A has an import that maps to B.

4. Topologically sort the graph — files with no project-internal imports are Level 0 and go first. A file belongs to level N+1 if its highest-depth dependency is at level N. The resulting levels are the translation waves.

5. Verify no cycles exist (Python disallows circular imports, so any cycle indicates a bug — stop and report it rather than proceeding).

Translate files level by level. Within a level, files are independent and can be translated in any order.

## Step 3 — Translate Each File

For each Python source file in dependency order, apply the full `py-to-ts-file` procedure. Each file must pass its own tests before moving on to the next.

## Step 4 — Full Test Suite

After all files are translated, run the complete test suite with coverage:
```bash
pnpm vitest run --coverage
```
All tests must pass. If any fail, fix the implementations — never the tests. Do not proceed to step 5 until the full suite is green.

Update the **Overall Summary** section of `TRANSLATION_REPORT.md` with final counts: files attempted, files completed, files with problems, total Python `def test_` count across all translated files, total TypeScript `it()` count, and full suite result (PASS or FAIL).

## Step 5 — Precommit

Run precommit to apply all linting, formatting, and type checking:
```bash
./bin/precommit.sh
```
Fix any issues before continuing.

## Step 6 — Remove docs/TYPESCRIPT.md

`docs/TYPESCRIPT.md` is a stale artifact from the prior translation attempt. Delete it if it still exists:
```bash
git rm docs/TYPESCRIPT.md 2>/dev/null || rm -f docs/TYPESCRIPT.md
```

Then update the files that referenced it:
- `README.md` — remove or update any link to `docs/TYPESCRIPT.md`
- `AGENTS.md` — remove any sentence referring to `docs/TYPESCRIPT.md`
- `docs/ENV.md` — remove any reference to `docs/TYPESCRIPT.md`

## Step 7 — Update AGENTS.md

- Verify the "TypeScript Port" section accurately describes the current DHE approach (DHE JS API accessed via `@deephaven/jsapi-nodejs`)
- Add or confirm the three translation skills are listed: `_py-to-ts-translation-guide`, `py-to-ts-file`, `translate-py-project`
- Update any stale notes about server commands or configuration

## Step 8 — Update README.md

Update any TypeScript usage documentation that changed as a result of the translation: new server commands, DHE support, configuration options, etc.

## Step 9 — Review Additional Docs (conditional)

Review these files and update only if their TypeScript-relevant content changed:
- `docs/PNPM.md` — update if new npm packages were added or build commands changed
- `docs/DEVELOPER_GUIDE.md` — update if server architecture or startup configuration changed
- `docs/ENV.md` — update if new TypeScript-specific environment variables were introduced
