---
name: _py-to-ts-translation-guide
description: Reference guide for translating Python source files to TypeScript — file mapping, naming, library substitutions, type translations, language patterns, and anti-patterns from the prior failed attempt; invoke when translating any Python file
---

This guide defines the rules for translating Python source files to idiomatic TypeScript in this project. All rules here are mandatory. The prior translation attempt violated many of them — the anti-pattern list at the end names the exact failures to avoid repeating.

## File/Module Path Mapping

| Python | TypeScript |
|--------|-----------|
| `src/deephaven_mcp/foo_bar.py` | `src-ts/foo-bar.ts` |
| `src/deephaven_mcp/foo_bar/__init__.py` | `src-ts/foo-bar/index.ts` |
| `src/deephaven_mcp/foo_bar/baz.py` | `src-ts/foo-bar/baz.ts` |
| `tests/test_foo_bar.py` | `src-ts/foo-bar.test.ts` |
| `tests/foo_bar/test_baz.py` | `src-ts/foo-bar/baz.test.ts` |

Module file names use kebab-case (not snake_case, not camelCase).

## Naming Conventions

- `snake_case` functions and variables → `camelCase`
- `UpperCamelCase` classes → `UpperCamelCase` (unchanged)
- `UPPER_SNAKE_CASE` constants → `UPPER_SNAKE_CASE` (unchanged)
- `_private_fn` underscore prefix → `_privateFn` (underscore preserved, body camelCase)

## Library Substitution Table

These choices are pre-established by the project — do not substitute alternatives.

| Python | TypeScript |
|--------|-----------|
| `pydeephaven` (DHC) | `@deephaven/jsapi-nodejs` + `@deephaven/jsapi-types` |
| `deephaven_enterprise` (DHE) | `@deephaven/jsapi-nodejs` connecting to a DHE server; see DHE section below |
| `pydantic.BaseModel` | Zod schema + `z.infer<typeof Schema>` type |
| `asyncio.Lock` | `Mutex` from `async-mutex` |
| `pyarrow` | `apache-arrow` |
| `logging` stdlib | `pino` (see `_typescript-logging-standards`) |
| `os.environ` | `process.env` |
| `pytest` | `vitest` |

## Deephaven DHC and DHE JavaScript Clients

`@deephaven/jsapi-nodejs` loads the JSAPI bundle from whatever Deephaven server it connects to.

**DHC**: Public JSAPI — use `@deephaven/jsapi-nodejs` and `@deephaven/jsapi-types`. Consult `node_modules/@deephaven/jsapi-types/` for exact type shapes.

**DHE**: The Deephaven Enterprise JavaScript API exists and is served by DHE servers. It provides enterprise capabilities: `client.getKnownConfigs()`, persistent queries via `QueryInfo`, enterprise authentication, `getTable()` from PQ, and more. Reference docs:
- [Deephaven Enterprise JS client overview](https://deephaven.io/enterprise/gplus/docs/clients/javascript/)
- [Deephaven Enterprise JS API reference](https://deephaven.io/enterprise/gplus/docs/clients/js-api-docs/)

Consult these docs when implementing DHE-specific classes (`CorePlusSession`, `CorePlusSessionFactory`, `EnterpriseSessionManager`, etc.).

## Type Translations

| Python | TypeScript | Notes |
|--------|-----------|-------|
| `str` | `string` | |
| `int` | `number` | TS has no distinct integer type; `number` is 64-bit float, exact for integers up to 2^53; use `bigint` only for values requiring arbitrary-precision integers |
| `float` | `number` | TS has no distinct float type |
| `bool` | `boolean` | |
| `bytes` / `bytearray` | `Uint8Array` | Use `Buffer.isBuffer()` for Node.js Buffer detection |
| `None` (absent value) | `undefined` | |
| `None` (explicit null) | `null` | |
| `Optional[T]` | `T \| undefined` | |
| `Union[A, B]` | `A \| B` | |
| `Any` | `unknown` | Never use `any` without an inline justification comment |
| `dict[K, V]` | `Record<K, V>` or `Map<K, V>` | |
| `list[T]` | `T[]` or `readonly T[]` | |
| `tuple[A, B]` | `readonly [A, B]` | |
| `set[T]` | `Set<T>` | |
| Typed numeric buffers | `Int32Array`, `Float64Array`, `BigInt64Array`, etc. | For typed binary data |

## Language Patterns

**Pydantic `BaseModel` / `@dataclass`**: Translate to a Zod schema plus an inferred type:
```typescript
export const FooSchema = z.object({ name: z.string(), count: z.number() });
export type Foo = z.infer<typeof FooSchema>;
```

**`@classmethod` factory methods** (`from_config`, `create`, etc.): Translate to `static` methods. Never omit factory methods.
```typescript
static fromConfig(config: FooConfig): Foo { ... }
```

**`@property` / `@foo.setter`**: Translate to `get`/`set` accessors.

**Python `Enum` / `StrEnum` / `IntEnum`** → TypeScript `enum`:
```python
class SystemType(StrEnum):
    COMMUNITY = "community"
    ENTERPRISE = "enterprise"
```
```typescript
export enum SystemType {
    COMMUNITY = "community",
    ENTERPRISE = "enterprise",
}
```
- Member names stay `UPPER_SNAKE_CASE` (unchanged from Python)
- `StrEnum` → string-valued TypeScript enum; `IntEnum` → number-valued TypeScript enum; plain `Enum` → use value type matching the Python value literals
- Custom `__str__` or `__repr__` on the enum class: translate to a standalone helper function if needed

**`TypedDict`** → TypeScript `interface` (preferred over Zod for structural type-only shapes):
```python
class FooContext(TypedDict):
    bar: str
    baz: int
```
```typescript
export interface FooContext {
    bar: string;
    baz: number;
}
```
- Generic TypedDict (parameterized with `Generic[T]`) → TypeScript generic interface `interface FooContext<T>`
- Optional fields (`total=False` or `Required`/`NotRequired`) → `field?: Type` in the interface

**`Protocol`** → TypeScript `interface`:
```python
class AsyncClosable(Protocol):
    async def close(self) -> None: ...
```
```typescript
export interface AsyncClosable {
    close(): Promise<void>;
}
```
- `@runtime_checkable` is not expressible in TypeScript — add a `@remarks` TSDoc note if present in Python

**`async with`**: Translate to explicit `.open()`/`.close()` calls, or implement `Symbol.asyncDispose`.

**`copy.deepcopy()`**: Use `structuredClone()`. Do NOT use `JSON.parse(JSON.stringify())` — it fails silently on `Uint8Array`, `Date`, `Map`, `Set`, circular references, and `undefined` values.

**Python generators (`yield`)**: Translate to TypeScript generators (`function*`) or async generators (`async function*`).

**`*args: T`**: `...args: T[]`. **`**kwargs: V`**: `{ ...opts }: Opts`.

## Multiple Inheritance (TypeScript limitation)

Python sometimes inherits from two classes for dual catchability, e.g.:
```python
class InvalidSessionNameError(SessionError, ValueError): ...
```

TypeScript does not support multiple class inheritance. Strategy:
- `extends` the primary parent class
- `implements` an interface derived from the secondary parent if it has required methods
- Add a `@remarks` TSDoc note naming the secondary Python parent that is not represented
- Do not silently drop the secondary parent

## Critical Invariants

- **Input normalization stays in the function**: If Python normalizes inputs inside a function (e.g., lowercasing HTTP header keys), the TypeScript version must do the same — do not delegate this to callers.
- **Return type specificity preserved**: If Python returns `PasswordCredentials`, TypeScript must return `PasswordCredentials`, not `Credentials`.
- **Export `_private` helpers**: Python exports private functions for direct unit test access. TypeScript must do the same using `export` even on `_`-prefixed functions.
- **Do not change module purpose**: A generic reusable class (e.g., `OpenAIClient` that works with any OpenAI-compatible API) must stay generic — never specialize it for a single vendor during translation.
- **Error messages must match exactly**: Do not simplify, rephrase, or shorten Python error messages.

## Process/OS API Mappings

| Python | TypeScript (Node.js) |
|--------|---------------------|
| `sys.excepthook = handler` | `process.on('uncaughtException', handler)` |
| asyncio loop exception handler | `process.on('unhandledRejection', handler)` |
| `signal.signal(SIGTERM, handler)` | `process.on('SIGTERM', handler)` |
| `os.kill(os.getpid(), sig)` | `process.kill(process.pid, sig)` |
| `psutil.pid_exists(pid)` | `process.kill(pid, 0)` in a try/catch (throws if PID does not exist) |
| `resource.getrlimit` | Not available in Node.js — document the gap in TSDoc |

## Test File Discovery

The naming rule for Python test files is **deterministic** — apply it directly; do not search.

| Python source file | Test file |
|---|---|
| `_foo.py` (leading underscore) | `test__foo.py` (double underscore) |
| `__init__.py` | `test_init.py` |
| `foo.py` (no leading underscore) | `test_foo.py` |

The test file mirrors the source path under `tests/`:
- `src/deephaven_mcp/_env.py` → `tests/test__env.py`
- `src/deephaven_mcp/_exceptions.py` → `tests/test__exceptions.py`
- `src/deephaven_mcp/auth/backends/_base.py` → `tests/auth/backends/test__base.py`
- `src/deephaven_mcp/config/__init__.py` → `tests/config/test_init.py`
- `src/deephaven_mcp/config/community.py` → `tests/config/test_community.py`

Integration test files (`test__*_integration.py`, `test_server_integration.py`, `test_launcher_integration.py`) are separate supplemental tests — do not count them as the primary Python test file and do not include them in the Python `def test_` count.

If no test file exists at the computed path, record "Python tests: 0 (no test file found at `<expected-path>`)" in TRANSLATION_REPORT.md. Never write `0` without noting the path checked.

## Testing Pattern Translations

| Python (pytest) | TypeScript (vitest) |
|----------------|-------------------|
| `def test_foo():` | `it("foo", () => {})` |
| `class TestFoo:` grouping | `describe("Foo", () => {})` |
| `pytest.raises(Err)` sync | `expect(() => fn()).toThrow(Err)` |
| `pytest.raises(Err)` async | `await expect(fn()).rejects.toThrow(Err)` |
| `@pytest.mark.asyncio` | Nothing — vitest handles `async () => {}` natively |
| `monkeypatch.setenv("K","V")` | `vi.stubEnv("K","V")` or `process.env.K = "V"` + `afterEach` cleanup |
| `monkeypatch.delenv("K")` | `delete process.env.K` in `beforeEach`, restore in `afterEach` |
| `MagicMock()` | `vi.fn().mockReturnValue(...)` |
| `AsyncMock()` | `vi.fn().mockResolvedValue(...)` |
| `mock.side_effect = Err()` | `vi.fn().mockRejectedValue(new Err())` |
| `@pytest.mark.parametrize("x", [...])` | `it.each([...])("description %s", (x) => {})` |
| `pytest.fixture` | outer-scope variable with `beforeEach`/`afterEach` |
| `mock.assert_called_once_with(a, b)` | `expect(mockFn).toHaveBeenCalledOnce(); expect(mockFn).toHaveBeenCalledWith(a, b)` |

## Documentation Translation

- Python module docstring → file-level `/** ... */` block, preserving all architectural context and examples
- Python class docstring → `/** ... */` immediately above `export class` (or `abstract class`)
- Python function/method docstring → TSDoc `/** ... */` with `@param`, `@returns`, `@throws`, `@example` as applicable
- **Python constant/variable docstrings** — a bare `"""..."""` string on the line immediately after a module-level assignment or class variable declaration is a docstring for that item. Translate to a `/** ... */` comment placed *before* the declaration in TypeScript:
  ```python
  # Python
  _KNOWN_AUTH_TYPES: set[str] = {"PSK", "Anonymous"}
  """Commonly known auth_type values. Custom authenticators trigger a warning."""
  ```
  becomes:
  ```typescript
  /** Commonly known auth_type values. Custom authenticators trigger a warning. */
  const _KNOWN_AUTH_TYPES: ReadonlySet<string> = new Set(["PSK", "Anonymous"]);
  ```
  This applies to module-level constants, class variables, and instance variables.
- **Inline `#` comments — preserve rationale, discard noise.** Translate `#` → `//`. Do not convert inline rationale comments into TSDoc blocks; they belong next to the code they explain.
  - **Preserve**: multi-line `#` comment blocks (2+ consecutive lines); any single-line comment containing `NOTE`, `WARNING`, `IMPORTANT`, `because`, `workaround`, `subtle`, `semantic`, `invariant`, or any other phrase explaining a non-obvious design decision, external reference, or production-correctness constraint.
  - **Discard**: comments that merely restate what the adjacent code already says in plain English (e.g., `# Call the function`, `# Return the result`); tool annotations (`# type: ignore`, `# noqa`, `# pylint:`, `# fmt:`).
- **Property docstrings**: a `@property` method with a docstring → `/** ... */` immediately above the `get` accessor in TypeScript. If both getter and setter exist, the TSDoc goes on the getter only.
- **Enum member docstrings**: a Python enum member followed by a trailing `"""..."""` on the next line → `/** ... */` immediately before that member in the TypeScript enum:
  ```python
  class InitializationPhase(enum.Enum):
      NOT_STARTED = "not_started"
      """The resource has not begun initializing."""
  ```
  ```typescript
  export enum InitializationPhase {
      /** The resource has not begun initializing. */
      NOT_STARTED = "not_started",
  }
  ```
  If the enum documents all members in the class-level docstring ("Values:" or "Members:" section) rather than per-member trailing strings, translate each entry to a per-member `/** */` comment in TypeScript.
- **TypedDict and dataclass field documentation**: if the Python class docstring includes an `Attributes:` section, translate each field description to a `/** ... */` comment immediately before the corresponding field in the TypeScript `interface` or class.
- **reST cross-references**: Python docstrings use `:class:Foo`, `:meth:bar`, `:func:baz`, `:py:func:baz`, `:attr:x` (reStructuredText role syntax). When translating to TSDoc, convert:
  - `:class:Foo` or `:py:class:Foo` → `{@link Foo}` if Foo is in scope; otherwise plain `Foo`
  - `:meth:bar` or `:py:meth:bar` → `{@link bar}` or plain `bar()`
  - `:func:baz` → `{@link baz}` or plain `baz()`
  - `:attr:x` → plain `x`
  - Never copy `:class:`, `:meth:`, `:func:`, `:py:` raw into TSDoc — they are reST-specific and unreadable in TypeScript
- **Preserve ALL documentation verbatim** — translation of language is the only allowed change; never abbreviate, omit, or paraphrase
- MCP tools: apply `tsdocs-improve` standards (Terminology Note and Format Accuracy sections with exact wording)

## Anti-Patterns — Mistakes from the Prior Failed Translation

These specific errors occurred in the prior attempt and must not be repeated:

1. **Stubbing DHE with `MissingEnterprisePackageError`** instead of implementing real DHE logic using the DHE JavaScript API
2. **Omitting base classes** (e.g., `BaseSession` was entirely missing from `session.ts`)
3. **Omitting factory methods** (`from_config()`, `create_and_register()`, etc.)
4. **Using `JSON.parse(JSON.stringify())` for deep clone** — use `structuredClone()` instead
5. **Reducing a generic client to a single-vendor factory** (e.g., `OpenAIClient` → Inkeep-only factory)
6. **Hiding private helpers from exports** — they must remain importable by tests
7. **Moving input normalization to callers** instead of keeping it inside the function
8. **Widening return types** (e.g., returning `Credentials` instead of `PasswordCredentials`)
9. **Writing tests that only check `toThrow()` without a type** — this does not verify behavior
10. **Changing error message text** instead of preserving the Python originals exactly
11. **Omitting signal handlers** (`SIGABRT`, `SIGQUIT`, `SIGBREAK`) that Python registers
12. **Skipping `setup_global_exception_logging`** — Node.js equivalents (`uncaughtException`, `unhandledRejection`) must be implemented
13. **Dropping constant/variable docstrings** — a trailing `"""..."""` after an assignment is a docstring; it must appear as a `/** */` comment before the TypeScript declaration
14. **Silently discarding inline rationale comments** — multi-line `#` blocks and any comment explaining WHY must be preserved as `//` comments; only noise comments (code restatements, tool annotations) may be omitted
15. **Dropping enum member docstrings** — each Python enum member with a docstring (trailing `"""..."""` or documented in the class-level "Values" section) must have a `/** */` comment before it in the TypeScript enum; silently omitting them is a FAIL
16. **Leaving reST cross-references raw** — `:class:Foo`, `:meth:bar`, `:func:baz`, `:py:` prefixes must be converted to `{@link}` or plain text; copying them verbatim into TSDoc produces unreadable documentation
