---
name: py-to-ts-file
description: Translate a single Python source file and its tests to TypeScript — tests-first, completeness inventory, behavioral assertions, and quality gates; invoke as py-to-ts-file <python-source-path>
---

Translates one Python source file and its test file to their TypeScript equivalents. Follow the four phases below in order. Never skip a phase.

Consult `_py-to-ts-translation-guide` for all language mappings, naming rules, library substitutions, and anti-patterns.

## Phase 1 — Inventory

1. Read the specified Python source file and its test file in full.

2. Determine the target TypeScript paths using the file mapping rules in `_py-to-ts-translation-guide`.

3. Build a **completeness manifest**: list every public function, private function, class, class method, property, module-level constant, and exported name. Each item starts with status `needed`.

4. If a TypeScript file already exists at the target path, compare it against the manifest:
   - Mark each item `complete`, `stub` (throws without real logic), or `missing`
   - Items marked `stub` or `missing` require full implementation — do not patch stubs; understand and implement the actual logic
   - Existing DHE stubs throwing `MissingEnterprisePackageError` are always `stub` — the DHE JavaScript API is real and must be implemented (see `_py-to-ts-translation-guide`)

Open `TRANSLATION_REPORT.md` (create it with the header below if it does not exist) and append a new per-file section:

```markdown
### `<python-source-path>` → `<ts-path>`
- **Status**: In progress
- **Python tests**: (counted below)
- **Coverage**: (measured below)
- **Notable deviations**: (recorded below)
- **Translation gaps**: (recorded below — items that could not be translated)
- **Problems**: (recorded below)
```

If creating the file fresh, prepend this header first:

```markdown
# TypeScript Translation Report

Generated: <YYYY-MM-DD>

## Overall Summary
(filled in by py-to-ts-project after full suite runs)

## File-by-File Results

## Translation Gaps
(items that could not be fully translated — missing Node.js/JS API equivalent, incomplete DHE API coverage, etc.)

## Items Requiring Human Review
(completed but needs verification — DHE API signatures, behavioral equivalence assumptions, multiple inheritance deviations)

## DHE Implementation Notes
```

## Phase 2 — Translate Tests First

5. Identify and resolve project-internal dependencies before writing any TypeScript:
   - Grep the Python file for project-internal imports:
     ```bash
     grep "^from deephaven_mcp\|^import deephaven_mcp" <python-file>
     ```
   - Map each import to its Python source file path and corresponding TypeScript path (using the file mapping rules in `_py-to-ts-translation-guide`)
   - For each TypeScript file that does not yet exist, apply this skill recursively to translate that file first, then return to the current file

6. Write `<path>.test.ts` as a faithful port of the Python test file:
   - **Test case count**: count `def test_` occurrences in the Python test file — the TypeScript file must have at least that many `it()` calls
   - **One-to-one mapping**: for each Python test, write a corresponding TypeScript test with the same scenario name, setup, and assertion intent
   - **Grouping**: use `describe` blocks matching Python's test class names (`class TestFoo`) or comment-group headings
   - **Parametrized tests**: translate `@pytest.mark.parametrize` to `it.each([values])(...)` — never manually unroll into separate `it()` calls
   - **Mocks**: `MagicMock()` → `vi.fn().mockReturnValue(...)`; `AsyncMock()` → `vi.fn().mockResolvedValue(...)`; `side_effect=Err()` → `vi.fn().mockRejectedValue(new Err())`
   - **Environment**: `monkeypatch.setenv("K","V")` → `vi.stubEnv("K","V")` or direct `process.env` assignment with `afterEach` cleanup
   - **Behavioral assertions required**: every `it()` must include at least one of: a value assertion (`toBe`, `toEqual`, `toStrictEqual`), a mock call assertion (`toHaveBeenCalledWith`), or a typed error assertion (`toThrow(ErrorType)`) — `toThrow()` without a type does not verify behavior
   - **Mock call assertions**: port `mock.assert_called_once_with(a, b)` → `expect(mockFn).toHaveBeenCalledOnce()` and `expect(mockFn).toHaveBeenCalledWith(a, b)`
   - Do not invent tests designed to pass an unwritten implementation — every test must assert behavior stated in the Python original

7. Run the test file and **confirm tests FAIL**:
   ```bash
   pnpm vitest run <test-file>
   ```
   Tests must fail at this stage. If they pass before the implementation is written, the tests are not asserting real behavior — fix them until they fail, then continue.

## Phase 3 — Translate Implementation

8. Implement `<path>.ts` following all rules in `_py-to-ts-translation-guide`. Work through the completeness manifest item by item:
   - Full logic required — no stubs, no empty bodies, no `throw new Error("not implemented")`
   - DHE code: consult the DHE JS API docs in `_py-to-ts-translation-guide`; implement real logic
   - Factory methods (`from_config`, `create_and_register`, etc.): translate all — never omit
   - Multiple inheritance: use the TypeScript strategy in `_py-to-ts-translation-guide`
   - Process/OS APIs: use the Node.js equivalents from `_py-to-ts-translation-guide`
   - Private helpers: export with `export` so tests can import them (Python does this for test access)
   - Deep copies: use `structuredClone()`, not `JSON.parse(JSON.stringify())`
   - Error messages: match Python originals exactly

9. Run the test file and **confirm all tests PASS**:
   ```bash
   pnpm vitest run <test-file>
   ```
   If tests fail, fix the **implementation**. Never change tests to make them pass.

Update the per-file section in `TRANSLATION_REPORT.md`: fill in the Python `def test_` count and the TypeScript `it()` count. Record any notable deviations encountered during translation (multiple inheritance changes, DHE implementation approach, API differences). Add DHE implementation details to the **DHE Implementation Notes** section if any DHE classes were implemented.

## Phase 4 — Quality Gates

10. Re-check the completeness manifest: every item must now be `complete`. No stubs, no empty bodies.

11. Apply `tsdocs-improve` on the TypeScript file — documentation depth must approximate the Python original. Python docstrings with 60-line architecture sections require equivalent TSDoc.

12. Apply `review-typescript-file` on the TypeScript file.

13. Run `tests-improve-ts` on the file to verify 100% per-file coverage:
    ```bash
    pnpm vitest run <test-file>
    ```

Finalize the per-file section in `TRANSLATION_REPORT.md`: set **Status** to `Complete` (or `Incomplete` if gaps remain), fill in the coverage percentage. Then update the two global sections:
- **Translation Gaps** — any symbol, function, or behavior that could not be translated due to a missing Node.js equivalent, an unexposed DHE JS API feature, or a platform-specific Python dependency; include the symbol name and a one-line reason
- **Items Requiring Human Review** — completed translations that need verification: DHE API call signatures that differ from Python, behavioral equivalence assumptions, multiple inheritance deviations; include the file path and a one-line description
