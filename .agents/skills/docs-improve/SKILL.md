---
name: docs-improve
description: Comprehensively improve a markdown documentation file — fill missing content, fix inaccuracies, improve organization, fix broken links, and apply formatting standards
---

**When to use this vs. `docs-accuracy`**: use `docs-improve` for a full review (accuracy + reorganization + missing content + formatting). Use `docs-accuracy` for surgical correctness-only fixes when the doc's structure is already sound.

## Before doing anything

Load `_documentation-roles` — the canonical source for each document's role and the editing rules that follow from it (in-scope content stays; out-of-scope content relocates rather than being rewritten in place; self-contained vs. generic docs; the cross-reference policy). Evaluate every proposed change against the target document's role; if a change would violate it, stop and reconsider before editing.

## Steps

Create a plan for improving this markdown file, then execute it.

1. Confirm the document's role per `_documentation-roles`. Note any sections currently out of scope; plan to relocate them, not rewrite them in place.
2. Look for content that is missing from this file *for its role* and needs to be added. Use the source code as a reference. If there is a conflict, the source code should be believed over the markdown.
3. Apply the `docs-accuracy` skill.
4. Draft the target section outline for this document's role: one section per in-scope responsibility, ordered along the reader's path through the task. Produce the outline as an explicit list before editing.
5. Reorganize the sections to match the outline from step 4; every section's content stays within the document's role.
6. Make sure that all referenced files and paths have hyperlinks.
7. Check all of the links in the document to make sure they are correct.
8. Link every product, website, or service the document references to its canonical URL.
9. Apply the `_markdown-documentation-standards` skill for formatting compliance (including the Table of Contents requirement).
10. **Final residual-check pass.** Walk the document end-to-end one more time against the role from step 1; list any remaining bullets where you can name a concrete defect (out-of-scope content, missing example, broken link, stale fact, ambiguous instruction). If you cannot name a concrete defect, the doc is done — stop. Vague "could be improved" feelings are not findings.

When making documentation changes, only do major rewrites if they are necessary. Do not make massive changes without a compelling reason.
