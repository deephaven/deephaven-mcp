---
name: _skill-authoring-standards
description: Structural standards for agent skill files — composition, frontmatter, body shape, multi-file layout, lifecycle — invoke when adding, editing, or reviewing a skill's structure; pair with `_skill-effectiveness` for content quality
user-invocable: false
---

# Skill Authoring Standards (Structural)

This document defines the structural standards for files under `.agents/skills/<name>/SKILL.md`. It is loaded by `skill-add` and `skill-review`. **Content-quality rules (triggerability, actionability, clarity, anti-patterns) live in `_skill-effectiveness`; this document covers form and architecture.**

The frontmatter `description` field is the only text the agent sees at decision time — every other concern here is downstream of getting the description right. Apply `_skill-effectiveness` §1 for description quality before applying any of the structural rules below.

## 1. What is a skill

A skill is defined by a `SKILL.md` file at `.agents/skills/<name>/SKILL.md` with YAML frontmatter (`name`, `description`) and a Markdown body; it may include Level-3 linked files for heavy detail (§8). Skills are loaded as agent context when triggered. The catalog index at `.agents/skills/README.md` is the human's view of the dependency graph.

## 2. Composition is the architecture

Hierarchy is expressed by reference, not by directory nesting. The directory layout stays flat; the dependency graph is what carries structure.

- **Three-property test.** Every skill is **specialized** (one concern), **self-contained** (invokable without inlining other skills), and **referenced** (invoked by at least one parent — the user, another skill, or both).
- **Extraction trigger.** Two or more skills share more than ~5 lines of substantive guidance → factor a shared sub-skill that both invoke. Mirrors code DRY.
- **Merge trigger.** A sub-skill is invoked by exactly one parent and has no independent trigger → inline it back into the parent. A single-use abstraction is a bad abstraction.
- **Pointer rule.** When a skill needs another skill's content, point with `Apply <skill>` or `See <skill> §N`. Never paraphrase another skill's body.
- **Canonical-implementation rule.** When a rule is grounded in real code, name the file and symbol: ``Canonical implementation: `cli/_format.py` (`OutputMode`, `format_output`)``. Pointers stay accurate when prose drifts.
- **Compatibility.** A reference sub-skill is invokable by any parent without parent-specific assumptions. If a sub-skill's content makes sense only inside one parent, it is a section, not a skill.

## 3. Frontmatter contract

Required keys: `name`, `description`.

- `name` matches the directory name exactly and is kebab-case.
- `description` quality is governed by `_skill-effectiveness` §1 (triggerability). The structural rule is one line, no embedded line breaks; soft cap ~250 chars to keep catalog rendering tight.
- `user-invocable: false` on every `_`-prefixed reference skill; unprefixed workflows omit it (they default to invocable). This key, not the prefix, is what keeps a context-only reference out of Claude Code's `/` menu — §14 explains why both are needed.

## 4. Naming and the `_`-prefix rule

Kebab-case throughout. Workflows are verb-noun (`cli-command-add`, `skill-review`). References are nouns or noun phrases (`_skill-effectiveness`, `_logging-standards`).

**`_`-prefix rule.** A skill is `_`-prefixed if and only if it is *not* intended for direct human invocation. The prefix answers one question: *"Will a human ever want this name in their slash-command autocomplete?"* If yes — even occasionally — no prefix. If no, prefix.

| Intended for human invocation? | Also referenced by other skills? | Prefix |
| --- | --- | --- |
| Yes | No | none |
| Yes | Yes | none |
| No | Yes (always, by definition) | `_` |
| No | No | not a skill — delete it |

**Decision flowchart:**

1. Will any human type `/<skill-name>` to invoke this skill? **Yes → unprefixed. No → continue.**
2. Is this skill referenced by at least one other skill? **No → not a skill, delete. Yes → `_`-prefixed.**

**Worked examples (all consistent with the rule):**

- ✅ `review-python-file` — human invokes via slash; *also* dispatched by `review-changes`. Unprefixed. The fact that another skill references it does **not** drive the prefix decision.
- ✅ `review-changes` — human invokes via slash; invokes `review-python-file`. Unprefixed.
- ✅ `cli-command-add` — human-invokable workflow. Unprefixed.
- ✅ `_python-coding-practices` — loaded by `review-python-file`, `cli-command-add`, and others; never typed by a human. Prefixed.
- ✅ `_skill-effectiveness` — loaded by every meta-skill; never typed by a human. Prefixed.
- ❌ `_run-precommit` would be wrong: humans invoke precommit directly. The catalog correctly has `run-precommit` unprefixed.
- ❌ `skill-effectiveness` (unprefixed) would be wrong: this skill is a context-only reference, not a human workflow.

**Rationale.** `_`-prefixed names are suppressed from slash-command autocomplete. Prefixing a human-invokable workflow makes it undiscoverable; omitting the prefix on a context-only reference clutters every human's autocomplete with names they will never type. Both errors are common and both are caught by `skill-review`'s frontmatter audit.

**Cross-agent note.** The prefix governs autocomplete in Cascade only; Claude Code ignores it and relies on the paired `user-invocable: false` key instead (§3). Keep prefix and key in sync; §14 owns the full cross-agent contract.

## 5. Single concern per skill

A skill has one outcome. Trigger test: *"If the agent invokes this skill, it always wants this one outcome."* If two outcomes can fit, factor a sub-skill or split the parent.

## 6. Structure of the `SKILL.md` body

- **Workflow skills**: imperative numbered steps. Each step is one action with one verifiable outcome (`_skill-effectiveness` §3). Cross-reference siblings with `Apply <skill>`; never paraphrase.
- **Reference skills**: lead sentence + named-bold-sub-bullet shape for multi-part rules. Fenced code blocks for canonical forms. `Canonical implementation: <path> (<symbol>)` pointers wherever a real exemplar exists.
- **Sub-bullet convention exemplars**: `_python-coding-practices` rules #9, #17, #18, #19.
- **Writing quality** is governed by `_skill-effectiveness` §5 (voice, hedge words, decorative language, sentence length, throat-clearing).

## 7. Length budget

- **Description**: one line. Soft cap ~250 chars.
- **Body**: 30–250 lines (stricter than Claude Code's 500-line guideline; keep `SKILL.md` lean).
- **> 250 lines**: first move heavy, not-always-needed reference detail to a linked file (§8); if two skills still share material, apply the §2 extraction trigger.
- **< 20 lines and with only one parent**: merge candidate (apply §2 merge trigger).

## 8. Progressive disclosure and multi-file skills

A skill loads in three levels; only the lightest level is always resident, so what you put where is a real token-cost decision:

- **Level 1 — `name` + `description`**: always in context for every installed skill (the catalog the agent routes from). Costs tokens every session whether or not the skill fires.
- **Level 2 — the `SKILL.md` body**: loaded only when the skill is invoked, and then it stays for the rest of the session. Every line is a recurring cost once loaded.
- **Level 3 — linked files** (`reference.md`, `examples.md`, fixture JSON, `scripts/`): loaded only when the body links to them and the agent reads them. Heavy detail here costs nothing until needed.

**Rule.** Keep the body to the always-needed core — the steps or rules that apply on every invocation. Heavy reference detail that a given invocation rarely needs (long tables, canonical wording other skills cite, exhaustive examples, API dumps) goes in a Level-3 linked file, not the body. Bloating the body or spawning a new `_`-prefixed skill to hold it are both the wrong move; the linked file is the release valve for the §7 budget.

**Granularity — scope each linked file to one independently-loadable chunk.** Match file boundaries to *what is needed together* — the same single-concern test §5 applies to skills.

- **Test**: would a given task need all of a file, or just part? If just part, split along that seam so the agent loads only the slice it needs.
- **Too coarse**: a catch-all `reference.md` bundling unrelated topics forces the agent to load everything to get anything.
- **Too fine**: detail always consumed together (a checklist and the rules it depends on) split into fragments the body must pull in lockstep.

**Mechanics.** The frontmatter stays in `SKILL.md`; linked files are plain Markdown with no frontmatter, referenced by a relative-path markdown link (§9), and the body must point at each one (an orphan file the body never references is dead weight). Name each file for its slice, not generically. A linked file is **private to its owning skill**: another skill that needs the content references the owning skill by name (§9), never reaches into its linked file by path. Canonical layout:

```text
my-skill/
├── SKILL.md           # entry point: always-needed core + links out
├── api-reference.md   # one heavy topic, loaded only when that topic is needed
├── error-codes.md     # a different topic, loaded independently
├── examples.md        # worked examples, loaded only when an example is wanted
└── scripts/           # executed, not loaded into context
```

Scripts are executed, not read into context — reference them by relative path (or `${CLAUDE_SKILL_DIR}/...` when the working directory varies).

## 9. Referencing files and skills

Use the form that matches the target:

| Target | Form |
| --- | --- |
| A skill's **own** bundled file | relative-path markdown link: `[reference.md](reference.md)` |
| **Another skill** | its name in backticks: `cli-command-add` — never a path |
| **Source / test code** | a backtick pointer with symbol (§2): `cli/_help.py` (`build_help`) — not a link |
| A **repo doc** | a markdown link: `[CLI](docs/CLI.md)` |

- **Reference skills by `name`, never by file path.** Skills relocate; names are stable. A skill's bundled files are private to it (§8) — reach them through the owning skill, never by path.
- **Wrapping skills enumerate the wrapped skills explicitly** in their description or lead paragraph (e.g., "wraps `_mcp-module-organization` + `pydocs-improve` + `_logging-standards`").
- **Dependency direction**: workflows → references → more-specialized references. Reference skills do not invoke workflow skills.

## 10. README catalog sync

Every skill appears in `.agents/skills/README.md` under the appropriate section table with a one-sentence purpose. The one-sentence purpose is itself subject to `_skill-effectiveness` §1 (it functions as a second-tier description). The README is the human's view of the dependency graph; an unlisted skill is invisible to contributors.

## 11. Lifecycle

- **Renaming.** Update every cross-reference simultaneously: sibling skills, the catalog README, `AGENTS.md` pointers. Use `git mv <old>/SKILL.md <new>/SKILL.md` to preserve history. Verify with `grep -r '<old-name>' .agents/ AGENTS.md` — zero hits before merge.
- **Prefix change.** A skill's human-invocability can change (a reference becomes a workflow, or vice versa). Rename to add or remove the `_` prefix; follow the renaming procedure above. The prefix change is not a separate operation from the rename.
- **Deprecation.** When replacing a skill rather than renaming it, leave the file in place for one development cycle with a top-of-body callout (`> **Deprecated**: use <new-skill> instead.`) so cross-references can migrate. Remove the file in the next cycle.
- **Removal.** Verify with `grep -r '<name>' .agents/ AGENTS.md docs/` — zero hits before deleting the file. Remove the catalog README row in the same commit.

## 12. Conflict and precedence

When two skills overlap on a topic:

- **More-specific wins.** `_logging-standards` overrides a generic "follow project style" rule.
- **`AGENTS.md` is always-loaded but terse.** It does not override a skill's detailed guidance; it points at the skill.
- **Canonical-implementation pointer beats prose.** When the skill's prose and the real code disagree, the code is the source of truth. Refresh the prose; do not refresh the code to match outdated prose.

## 13. Common failure modes

Used by `skill-review`'s structural audit. Drift signals that say a skill needs a structural fix. The list is illustrative, not exhaustive — flag any other signal that the skill's structure is degrading.

- **Composition rot**: skill > 250 lines; parallel sections in two skills; sub-skill with one parent; paraphrase where pointer would suffice.
- **Wrong prefix**: human-invokable workflow with `_`; context-only reference without `_`.
- **Multi-concern skill**: two outcomes in one file.
- **Wall-of-text rule**: multi-part rule that should be sub-bullets.
- **Stale cross-references**: named sibling does not exist; file pointer does not resolve; "rule #N" pointer does not match current numbering.
- **Gratuitous cross-reference**: an `Apply X` that the host step or bullet does not need to do its job — added by topical association ("this step mentions output, and X is about output") rather than necessity. The cross-reference analog of decorative prose (`_skill-effectiveness` §6). Count is irrelevant: a parent may cite X at every location that genuinely needs it. **Load-bearing test**: delete the reference and ask whether the host step can still be completed correctly. If yes, it was decoration — remove it. Two common sources: (a) the step reaches X anyway through a sibling it already invokes (e.g. `pydocs-improve` cites X, but its correctness step already chains through `pydocs-accuracy`, which owns X); (b) the step's real task is mechanical and independent of X (e.g. "single-source the OutputSpec constant" does not depend on output-serialization conventions — verifying the field text does, one step later).
- **Missing README row**: skill on disk, no entry in catalog.
- **Frontmatter drift**: `name` ≠ directory; description fails `_skill-effectiveness` §1.

Content failures (triggerless description, mushy outcome, hedge cluster, throat-clearing, decorative prose) are governed by `_skill-effectiveness` §6, not by this skill.

## 14. Cross-agent portability

Skills live in `.agents/skills/` (the cross-agent location, read natively by Cascade). Claude Code does not read this tree or the root `AGENTS.md`; it reads `.claude/skills/` and `CLAUDE.md`. Two wiring artifacts bridge the gap and must stay intact:

- **`.claude/skills` → `../.agents/skills`** — a git-tracked symlink (mode `120000`) so Claude Code discovers the one skill tree with no duplication.
- **Root `CLAUDE.md`** — imports the shared rules with a single `@AGENTS.md` line so Claude Code loads the same always-on guidance Cascade reads from `AGENTS.md`.

The `_`-prefix / `user-invocable` pairing (§3, §4) is the third portability rule: the prefix serves Cascade, the key serves Claude Code, and a reference skill needs both. When restructuring the tree, renaming the root instructions file, or moving skills, re-verify all three so neither agent silently loses the catalog.
