---
name: _documentation-roles
description: Defines the role (audience and scope) of every top-level markdown document in this project — loaded by the docs and CLI-authoring workflows to keep edits in-scope and prevent content drift between documents
user-invocable: false
---

# Documentation Roles

Every top-level markdown file in this repository has a single, intentional role. Edits must respect that role. This skill is the canonical source of truth for those roles; the docs workflows (`docs-improve`, `docs-accuracy`) and `cli-command-add` load it before making changes.

## Roles

| File | Reader | In scope | Out of scope |
| --- | --- | --- | --- |
| `README.md` | end user (not developer) | Get a user installed, configured, and connected to their AI tool with minimum friction. Quick start, install (including extras), upgrade path, AI-tool setup, capability list (one line per tool), troubleshooting, architecture overview. | Detailed schema reference, per-tool reference detail, internal mechanics, contributor workflows. |
| `docs/CONFIGURATION.md` | operator | Configuration *files and directories* — schema, fields, defaults, templating syntax. | Install options (uv extras, pip), security narrative, developer workflows, the v1→v2 migration procedure (lives in `MIGRATION.md`). |
| `docs/MIGRATION.md` | end user upgrading from v1 (not developer) | One-time v1→v2 configuration conversion, end-user friendly: running the converter, its CLI options, where it writes, and how to act on warnings. | Steady-state config schema/fields/defaults (lives in `CONFIGURATION.md`); package/tool upgrade commands (live in README's "Quick Upgrade"); templating syntax. |
| `docs/ENV.md` | operator | Environment variables consumed by the server processes. | Templating syntax (lives in `CONFIGURATION.md`), uv. |
| `docs/SECURITY.md` | operator deploying the server | **Short, self-contained** guide. A reader must be able to stand up a secure deployment without leaving the page. Trust model, hardening checklist, authentication, transport security, secret handling, rotation. | Anything that requires bouncing to `DEVELOPER_GUIDE.md` to act on. |
| `docs/UV.md` | developer new to `uv` | Generic `uv` crash course. | Project-specific commands, project env vars, project tests, project install lines. |
| `docs/CLI.md` | operator + AI agent using the local `dhcli` CLI | Full `dhcli` reference: command surface (noun-verb tree), global flags, env-var bindings, exit codes, `error_code` registry, output modes (`human` / `json` / `json-pretty` / `yaml`), examples, shell completion, `dhcli agents` self-discovery (the `--agents` flag and summary tree). | Server-side configuration (lives in `CONFIGURATION.md`); developer/contributor mechanics (lives in `DEVELOPER_GUIDE.md`); environment variables consumed by the server processes (those live in `ENV.md`; CLI-specific env vars stay here). |
| `docs/DEVELOPER_GUIDE.md` | contributor | Everything a developer working *on* the project needs. Catch-all. | Per-tool reference (parameters, returns, examples) — see [Tool reference is owned by code](#tool-reference-is-owned-by-code). |
| `docs/STANDALONE_BINARIES.md` | anyone building, installing, or deploying the standalone binaries (not just contributors) | **Self-contained, end-to-end**: installing a prebuilt binary (download, extract, point an AI tool at it) and building/releasing them (prerequisites including the Rust/PyApp setup needed to build, build commands, output artifacts, supported platforms, the CI release workflow). README's install section is a one-line pointer here, not a parallel copy. | Project-maintainer minutiae (e.g. bumping pinned-version source constants); server configuration schema (lives in `CONFIGURATION.md`, linked); per-tool reference (lives in code docstrings). |
| `docs/design/*.md` | contributor / architect | Design rationale for a subsystem — the *why* behind a structural decision (trade-offs weighed, chosen approach, invariants). One file per subsystem; e.g. `docs/design/CLI_TOOL_WRAPPING.md`. | Per-command / per-API reference (lives in `docs/CLI.md` or the code); step-by-step contributor workflow (lives in `DEVELOPER_GUIDE.md`). |
| `AGENTS.md` | AI agent | Agent process rules. Not human documentation; **no TOC**. | Anything intended for humans. |

## Tool reference is owned by code

MCP tool reference — each tool's parameters, return shape, and examples — has one source of truth: the tool's **docstring**. It is sent to AI agents over MCP and surfaced live by `dhcli tool show <name>`, so it cannot drift from the installed code. **No markdown document hand-maintains this content** — a hand-kept copy drifts the moment a signature changes (the prior guide had accumulated fabricated and missing parameters).

- **README** (*Available MCP Tools*) — the one-line-per-tool capability list (name + purpose), nothing deeper.
- **Every other doc** — links to `dhcli tool show` / the `_tools/` source; it never reproduces a tool's parameters or returns.
- **Adding or removing a tool** — the docstring is the reference; README's one-liner is the only doc that changes. See `mcp-tool-add`.

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
- `docs/CLI.md` may link to `CONFIGURATION.md` for the server config schema, but it must inline its own command/flag/exit-code/error-code reference — a CLI user must not have to bounce to another document to learn what `dhcli` accepts and how it fails.
- `docs/CLI.md` must agree with the CLI's self-describing surfaces — the `--help` text and the agents manifest, which both render from each command's `HelpSpec` in `cli/`. When a command's flags, arguments, exit codes, `error_code`s, or output fields change, update the spec and the doc in the same edit. The contract lives in `_cli-help-standards`; `docs/CLI.md` is the only surface with no automated check, so verify it by hand.
- README must never send an end user to `DEVELOPER_GUIDE.md`. Linking from README to `DEVELOPER_GUIDE.md` is acceptable only in the Contributing / Community sections, where the reader has self-identified as a contributor.

## When to invoke this skill

- Before any `docs-improve` or `docs-accuracy` edit to a top-level markdown file.
- When deciding where new documentation content belongs.
- When a section feels out of place — consult the role table above; the answer is "move it, don't rewrite it." Relocation preserves intent and the original author's framing; in-place rewrite loses both.
