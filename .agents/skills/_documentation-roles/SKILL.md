---
name: _documentation-roles
description: Defines the role (audience and scope) of every top-level markdown document in this project — invoked by `docs-improve` and `docs-accuracy` to keep edits in-scope and prevent content drift between documents
---

# Documentation Roles

Every top-level markdown file in this repository has a single, intentional role. Edits must respect that role. This skill is the canonical source of truth for those roles; `docs-improve` and `docs-accuracy` load it before making changes.

## Roles

| File | Reader | In scope | Out of scope |
| --- | --- | --- | --- |
| `README.md` | end user (not developer) | Get a user installed, configured, and connected to their AI tool with minimum friction. Quick start, install (including extras), upgrade path, AI-tool setup, capability list, troubleshooting, architecture overview. | Detailed schema reference, internal mechanics, contributor workflows. |
| `docs/CONFIGURATION.md` | operator | Configuration *files and directories* — schema, fields, defaults, templating syntax. | Install options (uv extras, pip), security narrative, developer workflows. |
| `docs/ENV.md` | operator | Environment variables consumed by the server processes. | Templating syntax (lives in `CONFIGURATION.md`), uv. |
| `docs/SECURITY.md` | operator deploying the server | **Short, self-contained** guide. A reader must be able to stand up a secure deployment without leaving the page. Trust model, hardening checklist, authentication, transport security, secret handling, rotation. | Anything that requires bouncing to `DEVELOPER_GUIDE.md` to act on. |
| `docs/UV.md` | developer new to `uv` | Generic `uv` crash course. | Project-specific commands, project env vars, project tests, project install lines. |
| `docs/DEVELOPER_GUIDE.md` | contributor | Everything a developer working *on* the project needs. Catch-all. | (No restrictions.) |
| `AGENTS.md` | AI agent | Agent process rules. Not human documentation; **no TOC**. | Anything intended for humans. |

## Editing rules

When editing any of the documents above:

- **Identify the role first.** Every proposed change must be evaluated against the document's role.
- **In-scope content stays.** Even if a section "could logically" live elsewhere, do not move it if it is in-scope here.
- **Out-of-scope content moves.** Do not rewrite it in place; relocate it to the document that owns it, and replace it with a link only if the receiving document is in-scope for the reader of the source.
- **Length is not a goal.** Concision matters only when it serves the reader of *this* document. Do not strip helpful content because the file is "long."
- **Self-contained docs stay self-contained for *critical* content.** `SECURITY.md` is the canonical example: a reader must be able to deploy securely without navigating away for any step they need to perform. Links out are fine for *supplemental* material (full schema reference, deeper architecture, vulnerability-disclosure policy) — not for content the reader needs to act on.
- **Generic docs stay generic.** `UV.md` describes `uv`, not this project. Do not introduce project-specific commands, env vars, or examples.
- **README serves the user.** Optimize for "easy to get running." Do not remove content that helps the user, even if it appears similar to content elsewhere — only true second-instance duplication within `README.md` itself is a candidate for de-duplication.
- **`AGENTS.md` is not human documentation.** It does not need a TOC, narrative, or audience-friendly framing.

## Cross-reference policy

- A document may link to another document for *further reading* without violating its role.
- A document **may not** rely on another document for content that is in-scope for itself. (E.g. `SECURITY.md` may link to `CONFIGURATION.md` "for the full schema reference," but it must inline the credential kinds an operator needs to act on the security checklist.)
- README must never send an end user to `DEVELOPER_GUIDE.md`. Linking from README to `DEVELOPER_GUIDE.md` is acceptable only in the Contributing / Community sections, where the reader has self-identified as a contributor.

## When to invoke this skill

- Before any `docs-improve` or `docs-accuracy` edit to a top-level markdown file.
- When deciding where new documentation content belongs.
- When a section feels out of place — consult the role table above; the answer is usually "move it, don't rewrite it."
