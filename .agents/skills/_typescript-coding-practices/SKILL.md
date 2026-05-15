---
name: _typescript-coding-practices
description: TypeScript coding conventions and style guide for this project — invoke when writing or reviewing TypeScript code
---

# TypeScript Coding Practices

1. A TypeScript file must not access unexported (private) symbols from another module. It is acceptable for a test file to access unexported symbols of the module under test.

2. All MCP tools (async functions registered via `server.tool()` inside `registerTools(server: McpServer)` in a `_tools/` module) have specific TSDoc requirements — apply the `tsdocs-improve` skill for the full rules, including required "Terminology Note" and "Format Accuracy for AI Agents" sections.

3. Template literals are preferred over string concatenation: `` `Hello ${name}` `` not `"Hello " + name`. Exception: splitting a long static string literal across multiple source lines for readability is acceptable with `+` concatenation — a multi-line template literal introduces actual newline characters in the string value and is not equivalent. Template literals are mandatory whenever the string includes any dynamic expression (`${...}`).

4. When moving or removing files, use the git version of the command to maintain history (`git mv`, `git rm`).

5. A TypeScript file `<file>.ts` must have a single test file `<file>.test.ts` in the same directory. Two exceptions: (a) files that contain only `type` and/or `interface` declarations with no runtime code have nothing to test and do not need a test file; (b) process entry-point files (containing `process.exit`, `process.on`, or `StdioServerTransport`) need at minimum a smoke test that imports the module and asserts its exported function(s) exist — a full integration suite is not required.

6. `any` is generally a bad type. Use `unknown` and narrow with type guards, or use specific types. If you must use `any`, justify why it is necessary in a comment. Prefer `unknown` at system boundaries (external APIs, JSON parsing).

7. `as` type assertions require justification. Prefer type narrowing (`typeof`, `instanceof`, discriminated unions) over casting. An unjustified `as` masks the same class of bugs as Python's `hasattr`/`getattr`. Non-null assertions (`x!`) fall under the same rule: every `!` must have an inline comment explaining why TypeScript's null/undefined analysis is wrong at that point (e.g., `// Non-null safe: authenticate() verified the header is present`). Both `as` and `!` without justification are review blockers.

8. Use American English spelling throughout all code, comments, TSDoc, and documentation. For example: "initialized" not "initialised", "color" not "colour".

9. Unused function parameters must be prefixed with `_` (e.g., `_req`, `_ctx`, `_event`). This is the convention TypeScript, ESLint (`@typescript-eslint/no-unused-vars`), and the language server recognize. Do not delete the parameter or use a workaround.

10. In Vitest tests, use `vi.fn().mockResolvedValue(...)` for async functions and `vi.fn().mockReturnValue(...)` for synchronous ones. Using a plain `vi.fn()` where an async mock is needed is a common mistake — it causes tests to pass or fail misleadingly because the mock does not return a Promise. This mirrors the Python distinction between `AsyncMock` and `MagicMock`.

11. Use `const` over `let`; never use `var`. Prefer immutability.

12. Use named exports over default exports. Named exports are more refactorable and make imports grep-friendly.

13. All exported functions must have explicit return type annotations. Non-exported (private) helpers should also have return types when the inferred type is non-obvious.

14. Use `type` for type aliases and union/intersection types. Use `interface` for object shapes that may be extended or implemented. Be consistent within a module.

15. Mark data that should not be mutated as `readonly`. Prefer `readonly` arrays (`readonly T[]` or `ReadonlyArray<T>`) over mutable arrays for function parameters.

16. `tsconfig.json` must include `"strict": true`. This enables `strictNullChecks`, `noImplicitAny`, `strictFunctionTypes`, and related checks. Do not disable individual strict-mode flags without justification.

17. Use Zod for runtime validation at system boundaries (user input, config files, external API responses). TypeScript's type system is compile-time only; Zod enforces types at runtime.

18. Prefer `undefined` over `null` for optional values in new code. Use `null` only when interfacing with external APIs or libraries that return `null`.

19. Prefer native ECMAScript private fields (`#field`, `#method()`) over TypeScript's `private` keyword. TypeScript's `private` is compile-time only — the compiled JavaScript exposes the field as a regular property, accessible to any non-TypeScript caller. `#field` is enforced at runtime by the JavaScript engine and appears as a true private in the emitted code. Use `#` for all class fields and methods that should not be accessible outside the class.
