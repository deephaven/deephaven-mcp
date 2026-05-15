---
name: py-to-ts-review
description: Review and repair a Python-to-TypeScript translation — verifies existence, symbol completeness, exports, documentation (and writes missing TSDoc), test coverage (count + one-to-one mapping), stubs, and error messages; emits a structured verdict for TRANSLATION_REPORT.md; invoke as py-to-ts-review <python-source-path>
---

Given a Python source file path, audits the corresponding TypeScript translation for completeness and correctness. Actively **fixes** documentation gaps by translating Python docstrings to TSDoc. **Reports** structural problems that require agent judgment. Called from `py-to-ts-file` Phase 4 and from `py-to-ts-project` after the full translation pass.

Consult `_py-to-ts-translation-guide` for file path mapping rules, naming conventions, and test file discovery rules.

## Step 1 — Existence check

Compute the expected TypeScript file path using the file mapping rules in `_py-to-ts-translation-guide`. Check whether the file exists on disk.

If the TypeScript file does **not** exist:
- Emit verdict `MISSING` (see Output section)
- Stop — do not run Steps 2–7

## Step 2 — Symbol completeness

Read both the Python source file and the TypeScript file in full.

Build a list of every **Python public symbol**: functions, classes, class methods, class variables, module-level constants, enum types and their named member values, TypedDict types and their fields, Protocol types and their methods, dataclass types and their fields, property accessors, and every name in `__all__` if present. For each Python symbol, apply the naming rules from `_py-to-ts-translation-guide` to compute the expected TypeScript name (`snake_case` → `camelCase`, `UpperCamelCase` unchanged, `UPPER_SNAKE_CASE` unchanged, `_prefix` preserved).

Verify the expected TypeScript name exists in the TypeScript file as an export (or as a public class member). For enum types, each member name (e.g., `COMMUNITY`, `ENTERPRISE`) must exist as a member of the corresponding TypeScript enum — missing enum members are a FAIL. Collect any missing symbols.

**Modifier completeness check**: for each Python symbol, verify the TypeScript counterpart uses the correct modifier:
- Class with `@abstractmethod` methods or inheriting from `ABC`: TypeScript class must be `abstract class`. If not, report FAIL.
- Method with `@abstractmethod`: TypeScript method must have `abstract` keyword. If not, report FAIL.
- Method with `@override`: TypeScript method must have `override` keyword. If not, report FAIL.
- `ClassVar[T]` annotation: TypeScript property must have `static` modifier. If not, report FAIL.
- `Final[T]` annotation on a field: TypeScript field must have `readonly` modifier. If not, report FAIL.
- `_single_underscore` class method (not a module-level helper): TypeScript method should have `protected` modifier. Report NEEDS_WORK if absent.

## Step 3 — Export completeness

If the Python file has `__all__`, verify every name in it is exported from the TypeScript file. A name present in `__all__` with no corresponding `export` in the TypeScript file is a FAIL.

## Step 4 — Documentation: check and fix

For every Python docstring in the source file (module-level, class-level, method/function-level), check whether the corresponding TypeScript item has a `/** ... */` TSDoc block that covers the same content.

**Fix policy**: If a TSDoc block is **missing entirely**, **write it now**. If a TSDoc block is **present but abbreviated**, **expand it now**. A TSDoc block is considered abbreviated if ANY of the following is true:
- It contains fewer than half the sentences of the Python original.
- The Python docstring has named sections (any colon-terminated heading on its own line such as `Values:`, `Status Categories:`, `Usage Context:`, `Deployment Characteristics:`, `Attributes:`, `Args:`, `Returns:`, `Raises:`, `Example:`, `Notes:`, `See Also:`) and the TypeScript TSDoc is missing one or more of those sections.
- The Python docstring is longer than 5 lines and the TypeScript TSDoc has only 1–2 sentences.

Translation rules:
- Python module docstring → `/** ... */` block at the very top of the `.ts` file, before all imports
- Python class docstring → `/** ... */` immediately above `export class` (or `abstract class`)
- Python method/function docstring → `/** ... */` immediately above the method/function signature; use `@param name - description`, `@returns description`, `@throws {ErrorType} when condition`, `@example` as applicable
- **Preserve all content verbatim** — translate language only (Python → English where needed); never abbreviate, omit examples, or paraphrase
- For class variables documented with a trailing `"""..."""` in Python: add a `/** ... */` comment above the property declaration in TypeScript

**Multiple inheritance `@remarks`**: For every class that has two or more parents in Python (e.g., `class Foo(SessionError, ValueError)`), check that the TypeScript class has a `@remarks` note of the form:

> In Python this class also extends `SecondaryParent` (secondary parent not expressible in TypeScript single-inheritance).

If the `@remarks` is missing, add it.

**Named section preservation**: Python docstrings frequently use colon-terminated section headers on their own line (e.g., `Values:`, `Status Categories:`, `Usage Context:`, `Deployment Characteristics:`, `Args:`, `Returns:`, `Raises:`, `Example:`, `Notes:`, `See Also:`). The complete Python docstring — introductory paragraph AND all named sections — must be preserved in the TypeScript TSDoc.

Exception for enum "Values:" / "Members:" sections: translate each member entry to a per-member `/** */` comment rather than repeating it in the class-level TSDoc. Other named sections (intro paragraph, "Status Categories:", "Usage Context:", "Example:", etc.) go in the class-level TSDoc above the `export enum`.

If the class-level TSDoc is present but missing named sections that exist in the Python, add those sections now.

**Property docstrings**: For every `@property` method in the Python source that has a docstring, verify the TypeScript `get` accessor has a `/** ... */` TSDoc block covering the same content. If missing, add it. TSDoc goes on the getter only (standard convention when both getter and setter exist).

**Enum member docstrings**: Scan the Python enum class for per-member docstrings (trailing `"""..."""` on the line after the member assignment, or entries in the class-level "Values:" or "Members:" section). For each documented member, verify the TypeScript enum has a `/** ... */` comment immediately before that member declaration. If missing, add it.

Multi-line entries in the "Values:" section include a primary line and indented continuation lines. Include the full text (primary + continuation) in the per-member TSDoc comment. For a continuation-indented entry like:
```
ONLINE: Resource is healthy, responsive, and ready for operational use.
    Indicates successful connectivity and passing health checks.
```
The per-member TSDoc becomes:
```typescript
/** Resource is healthy, responsive, and ready for operational use.
 *  Indicates successful connectivity and passing health checks. */
ONLINE = 1,
```
Do not truncate continuation lines.

**TypedDict and dataclass field documentation**: If the Python class docstring includes an `Attributes:` section documenting fields, verify each TypeScript interface/class field has a `/** ... */` comment. If absent, add the field comments drawn from the Attributes section.

**reST cross-reference conversion**: Scan the TypeScript file for raw `:class:`, `:meth:`, `:func:`, `:py:`, `:attr:` patterns (reStructuredText role syntax copied verbatim from Python). For each occurrence, convert it: `:class:Foo` or `:py:class:Foo` → `{@link Foo}` if Foo is a type in scope, otherwise plain `Foo`; `:meth:bar` → `{@link bar}` or plain `bar()`; `:func:baz` → `{@link baz}` or plain `baz()`; `:attr:x` → plain `x`. Never leave raw reST syntax in TSDoc.

**Constant and class variable docstrings**: scan the Python file for trailing `"""..."""` patterns — a bare string literal on the line immediately following a module-level assignment or class variable declaration is a docstring for that item. For each one found, verify the TypeScript file has a `/** ... */` comment immediately *before* the corresponding declaration. If missing, add it.

**Inline rationale comments**: scan the Python file for multi-line `#` comment blocks (2+ consecutive `#` lines). For each block, verify a corresponding `//` comment block exists at the same logical location in the TypeScript file. If missing, add it. Also scan for single-line `#` comments containing rationale keywords (`NOTE`, `WARNING`, `IMPORTANT`, `because`, `workaround`, `subtle`, `semantic`, `invariant`) and verify TypeScript `//` equivalents are present.

**Blank lines between TSDoc and declarations**: scan the TypeScript file for the pattern `*/` followed by one or more blank lines before a non-comment declaration (`export`, `class`, `function`, `const`, `enum`, `interface`, `import`, or a bare method/property signature). For each occurrence found, remove the blank lines so the closing `*/` is on the line immediately before the declaration. Blank lines between two consecutive `/** ... */` blocks or between `*/` and a `//` comment are acceptable and must not be removed.

After writing all documentation, record: N/M **function/method** docstrings present, N/M **property** docstrings present, N/M **enum member** docstrings present, N constant docstrings present out of M found in Python, N inline comment blocks present out of M found in Python.

**Sanity check**: Run `grep -c '/\*\*' <ts-file>` and `grep -c '"""' <py-file>`. If the TypeScript `/**` count is less than two-thirds the Python `"""` count, documentation is likely still incomplete — review and complete before continuing. (Two-thirds because constant docstrings appear in both counts.)

## Step 5 — Test coverage: count and one-to-one mapping

Locate the Python test file using the **Test File Discovery** rules from `_py-to-ts-translation-guide`:
- Apply the deterministic naming rule; do not search.

Also compute the expected TypeScript test file path from the source mapping rules (e.g., the test file for `src-ts/auth/backends/base.ts` is `src-ts/auth/backends/base.test.ts`) and verify it exists on disk. If the TypeScript test file is absent, record FAIL — test coverage cannot be verified and the file must be created.
- If source stem starts with `_` (e.g., `_env.py`): test file is `test__<stem>.py` (double underscore)
- If source is `__init__.py`: test file is `test_init.py`
- Otherwise: test file is `test_<stem>.py`
- Test file lives at `tests/<mirror-of-src-path>/`

**Count check**: Count `def test_` occurrences in the Python test file. Count `it(` occurrences in the TypeScript test file. If TypeScript count < Python count, report the delta as a FAIL.

**One-to-one mapping check**: For every `def test_<name>` in the Python test file, verify a corresponding TypeScript `it(...)` exists for the same scenario:
- Strip the `test_` prefix and convert the remaining `snake_case` to word stems; verify a TypeScript `it(` call exists whose description string contains those stem words (e.g., `test_missing_header` → check for `"missing header"` or `"missingHeader"` or `"missing_header"`)
- Parametrized tests (`@pytest.mark.parametrize`): the function name maps to one `it.each(...)` call — verify the `it.each` exists (the count may be lower due to collapsing, which is acceptable)
- Class-grouped tests (`class TestFoo: def test_bar`): look for the scenario name inside the corresponding `describe("Foo", ...)` block

If any Python test function has no recognizable TypeScript counterpart, list it as a FAIL with the Python function name.

If no Python test file exists at the expected path, record "Python tests: 0 (no test file found at `<expected-path>`)" and skip the mapping check.

## Step 6 — Stub detection

Grep the TypeScript file for leftover stubs:
```bash
grep -n "not implemented\|TODO\|FIXME\|throw new MissingEnterprisePackageError" <ts-file>
```

Report every matching line number and content.

## Step 7 — Error message spot-check

Find Python error-message string literals: quoted strings that appear as the argument to `raise`, `McpError(`, `ValueError(`, `TypeError(`, `ConfigurationError(`, or similar exception constructors.

For each non-trivial string (> 20 characters), verify the same string appears verbatim in the TypeScript file. Flag any mismatch.

## Output

Append the following subsection to the per-file section of `TRANSLATION_REPORT.md` and print it to stdout. Use the exact field names shown — downstream tools parse this format.

```markdown
#### Review
- **Existence**: PRESENT | MISSING
- **Symbol completeness**: COMPLETE | MISSING: [list each missing symbol]
- **Export completeness**: COMPLETE | PARTIAL: [list each missing export]
- **Documentation**: FULL | PARTIAL | MISSING
  - Function/method docs: N/M present
  - Property docs: N/M present
  - Enum member docs: N/M present
  - Multiple inheritance `@remarks`: N/M required notes present
  - Constant/variable docs: N/M present
  - Inline comment blocks: N/M present
- **Test file**: PRESENT (`<ts-test-path>`) | MISSING (`<ts-test-path>`)
- **Test count**: Python N → TypeScript M (PASS | FAIL delta=-K)
- **Test mapping**: COMPLETE | GAPS: [list each unmapped Python test]
- **Stubs**: CLEAN | STUBS FOUND: [list line numbers and text]
- **Error messages**: PASS | MISMATCH: [list each mismatched string]
- **Overall**: PASS | NEEDS_WORK | FAIL
```

**Verdict definitions**:
- `FAIL` — any of: source file missing, TypeScript test file missing, symbols missing, exports missing, stubs found, TS test count < Python count, unmapped Python tests
- `NEEDS_WORK` — documentation partially missing or error message mismatches, but no structural gaps (all symbols present, all exports present, no stubs, test counts correct)
- `PASS` — all checks pass, documentation is FULL or PARTIAL with no outstanding gaps
