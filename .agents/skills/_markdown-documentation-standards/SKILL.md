---
name: _markdown-documentation-standards
description: Documentation format standards for markdown files — JSON/JSON5 code block requirements, placeholder formatting, copy-paste readiness — invoke when editing or creating markdown documentation
user-invocable: false
---

# Documentation Standards

## JSON/JSON5 Code Blocks in Markdown

1. **Use only ASCII characters in JSON/JSON5 code blocks**: JSON examples in markdown files must contain only ASCII characters (character codes 0-127). Do not use emoji or other non-ASCII Unicode characters (such as ✅, ❌, ⚠️) within JSON/JSON5 code blocks, even in comments.

   **Rationale**: Users copy-paste these examples into configuration files. Many JSON parsers and text editors have issues with non-ASCII characters, leading to parsing errors.

   **Correct**:

   ```json5
   // community/sessions/local_dev.json
   {
     "host": "localhost",
     "port": 10000,
     "auth": {
       "credentials": {
         "type": "psk",
         "token": "${env:DH_AUTH_TOKEN}"  // RECOMMENDED: source the token from an env var
       }
     }
   }
   ```

   **Incorrect**:

   ```json5
   // community/sessions/local_dev.json
   {
     "host": "localhost",
     "port": 10000,
     "auth": {
       "credentials": {
         "type": "psk",
         "token": "${env:DH_AUTH_TOKEN}"  // ✅ RECOMMENDED: source the token from an env var
       }
     }
   }
   ```

2. **Use ```json5 for code blocks with comments**: If a JSON code block contains comments (// or /* */), mark it as```json5, not ```json. Standard JSON does not support comments.

   **Correct**:

   ```json5
   // This is a valid JSON5 comment
   {
     "key": "value"  // Inline comment
   }
   ```

   **Incorrect**:

   ```json
   // This will cause a parser error
   {
     "key": "value"
   }
   ```

3. **Validate JSON blocks**: All code blocks marked as ```json must be valid, parseable JSON. Use a JSON validator to verify examples before committing.

## General Documentation Standards

1. **Keep examples copy-paste ready**: All code examples should work when copied directly from documentation without modification (except for placeholder values like passwords, URLs, etc.).

2. **Use clear placeholder values**: Make it obvious what values need to be replaced:
   - Good: `"password": "your-password-here"`, `"host": "your-server.example.com"`
   - Bad: `"password": "xxxx"`, `"host": "server"`

3. **Document placeholder requirements**: When using placeholders, add comments explaining what format is expected (e.g., `"auth_token": "username:password"  // Must be in "user:pass" format`).

## Heading Structure

1. **Single H1 per file**: Each markdown file has exactly one `#` heading at the top — the document title. All other section headings are H2 (`##`) or deeper. (Enforced by markdownlint MD025 / MD041.)
2. **No skipped levels**: Do not jump from `##` to `####`. (markdownlint MD001.)
3. **Title case for H1, sentence case for H2+**: Match the existing style of neighbouring docs (`README.md`, `docs/CONFIGURATION.md`, `docs/DEVELOPER_GUIDE.md`); do not introduce a new convention.

## Table of Contents

Human-facing documentation (anything user- or operator-facing) requires a Table of Contents immediately after the H1 when the document either:

- exceeds approximately 100 lines, **or**
- contains more than 5 H2 sections.

Requirements:

- Place the TOC under a `## Table of Contents` heading directly after the H1 and any short intro paragraph.
- Each entry must link to a live anchor in the same file (lowercase, hyphen-separated, generated from the heading text).
- The TOC must reflect the live H2 / H3 structure — verify after every edit that adds, removes, or renames a heading.
- Keep entries to H2 (and selected H3) only; deeper levels create noise.
- Never include the document's H1 title as a root entry — it links to the page the reader is already on and indents every real entry one level.
- Never include a self-referential "Table of Contents" entry.

### Auto-generated TOC fingerprint

A TOC whose first entry links to the document's own H1, or that lists "Table of Contents" inside itself, was written by an editor extension (e.g. Markdown All in One on default settings), not by hand. `docs/CLI.md` and `docs/DEVELOPER_GUIDE.md` have both been hit.

To repair one while the extension may still be active:

- Delete the H1 root entry and the self-referential entry, then dedent every remaining entry one level. **Do not thin the TOC's depth in the same edit** — TOC-updater extensions rewrite on save any TOC that omits headings they track, so a depth reduction is silently reverted while entry-removal plus dedent is stable.
- Thin over-deep entries (H4+) only after confirming the rewrite loop is dead: edit, wait a few seconds, re-read the file from disk.
- The workspace kill switch for Markdown All in One is `"markdown.extension.toc.updateOnSave": false` in `.vscode/settings.json`.

AI-configuration files (e.g. `AGENTS.md`) and skill definitions under `.agents/skills/` do **not** require a TOC, regardless of length — they are not human documentation.

## Code Block Language Tags

Every fenced code block must declare its language. Use:

- ` ```bash ` for shell commands.
- ` ```python ` for Python.
- ` ```json ` for strict JSON, ` ```json5 ` if the block contains comments or trailing commas.
- ` ```text ` for plain output, file trees, or formatted text without a real language.
- ` ```yaml `, ` ```toml `, ` ```dockerfile ` as appropriate.

Untagged ` ``` ` blocks defeat syntax highlighting and bypass language-specific lint checks.

## Links and Anchors

- **Repo-internal links**: relative paths (e.g., `[CONFIGURATION](docs/CONFIGURATION.md)`). Do not use absolute filesystem paths or `https://github.com/...` URLs for files in the same repo.
- **Anchors**: lowercase, hyphen-separated, generated from the heading text. Verify anchors after renaming a heading.
- **External services**: link the first mention of every product/service (Deephaven, Inkeep, FastMCP, etc.).

## Tables

- Use Markdown pipe tables. Align the leader pipes; padding inside cells is optional but consistent within a file.
- A header row is required; markdownlint flags borderless tables.
- For long cells, prefer a bulleted list outside the table over a wrapped cell.

## Prose conventions

- Plain prose may use Unicode em dashes and curly quotes. The ASCII-only rule applies **only inside JSON / JSON5 code blocks** for copy-paste safety.
- Emoji are discouraged in production documentation unless explicitly requested by the user. The ASCII-only rule for JSON/JSON5 blocks is strict; the no-emoji preference is the default for prose.
- American English spelling throughout (matches `_python-coding-practices` rule 8).
