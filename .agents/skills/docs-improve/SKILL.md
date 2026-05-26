---
name: docs-improve
description: Comprehensively improve a markdown documentation file — fill missing content, fix inaccuracies, improve organization, fix broken links, and apply formatting standards
---

**When to use this vs. `docs-accuracy`**: use `docs-improve` for a full review (accuracy + reorganization + missing content + formatting). Use `docs-accuracy` for surgical correctness-only fixes when the doc's structure is already sound.

## Before doing anything

Load `_documentation-roles` and identify the role of the document being edited. **Every proposed change must be evaluated against that role:**

- **In-scope content stays**, even if it could "logically" live elsewhere.
- **Out-of-scope content moves to the document that owns it** (don't just rewrite it in place).
- **Length is not a goal in itself.** Concision matters only when it serves the reader of *this* document. Do not strip content that helps the reader because the file is "long."
- **Self-contained docs** (currently `SECURITY.md`) must remain self-contained for *critical* content. Links out are fine for supplemental material (further reading, deeper schema, related policy); they are not fine for content the reader needs to act on within the document's scope.
- **Generic docs** (currently `UV.md`) must stay generic. Do not introduce project-specific commands, env vars, or examples.
- **README must not send an end user to `DEVELOPER_GUIDE.md`** outside the Contributing / Community sections.

If a proposed change violates any of the above, stop and reconsider before editing.

## Steps

Create a plan for improving this markdown file, then execute it.

1. Confirm the document's role per `_documentation-roles`. Note any sections currently out of scope; plan to relocate them, not rewrite them in place.
2. Look for content that is missing from this file *for its role* and needs to be added. Use the source code as a reference. If there is a conflict, the source code should be believed over the markdown.
3. Apply the `docs-accuracy` skill.
4. Improve the outline and organization for this document, within its role.
5. Reorganize the sections to match the new outline.
6. Make sure that all referenced files and paths have hyperlinks.
7. Check all of the links in the document to make sure they are correct.
8. Are there any products, websites, or services that are being used that should be linked?
9. Apply the `_markdown-documentation-standards` skill for formatting compliance (including the Table of Contents requirement).
10. How else can this document be improved?

When making documentation changes, only do major rewrites if they are necessary. Do not make massive changes without a compelling reason.
