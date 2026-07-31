---
name: docs-accuracy
description: Verify a markdown documentation file is factually accurate — invoke for surgical correctness-only fixes when the document's structure is already sound; use docs-improve for a full review. Checks commands, file paths, config keys, API names, code examples, and URLs against source code, and fixes inaccuracies in place
---

**When to use this vs. `docs-improve`**: use `docs-accuracy` for surgical correctness-only fixes against a single document. Use `docs-improve` for a full review that may also reorganize sections, fix link rot, or add missing content.

Before editing, load `ref-documentation-roles` and confirm that proposed fixes stay inside the document's role. When a fact is wrong in this doc but right in another, fix it here — do **not** delete it and link out unless the role of this doc says it should not have been here in the first place. Self-contained docs (currently `SECURITY.md`) must remain self-contained.

For the specified document, verify every factual claim against the source code. Check:

- **Commands and flags**: run the command or read `--help` output; confirm flags exist and do what the doc says
- **File paths**: confirm each path exists in the repo
- **Config keys and values**: check against the actual config schema or parser in the source
- **API names, function signatures, and module paths**: grep or read the source
- **Code examples**: verify syntax is valid and matches the actual API; apply the `ref-markdown-documentation-standards` skill for JSON/JSON5 code block and placeholder formatting
- **Port numbers, URLs, environment variable names**: confirm against source or config
- **Output value vocabularies**: when the doc documents the values of a tool / CLI output field (e.g., enumerating `--origin static|dynamic|discovered` or sample JSON values), apply `ref-output-serialization-conventions` and confirm the documented values match what the code emits
- **Spelling**: apply `ref-markdown-documentation-standards` Prose conventions; verify with `uv run codespell <file>`

For each inaccuracy found, report what the document says, what it should say, and fix it directly in the document.
