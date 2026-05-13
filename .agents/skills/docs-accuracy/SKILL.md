---
name: docs-accuracy
description: Verify that a document is accurate by checking all details against the source code
---

For the specified document, verify every factual claim against the source code. Check:

- **Commands and flags**: run the command or read `--help` output; confirm flags exist and do what the doc says
- **File paths**: confirm each path exists in the repo
- **Config keys and values**: check against the actual config schema or parser in the source
- **API names, function signatures, and module paths**: grep or read the source
- **Code examples**: verify syntax is valid and matches the actual API; apply the `markdown-documentation-standards` skill for JSON/JSON5 code block and placeholder formatting
- **Port numbers, URLs, environment variable names**: confirm against source or config

For each inaccuracy found, report what the document says, what it should say, and fix it directly in the document.
