---
name: ref-skill-authoring-standards
description: Structural standards for agent skill files — invoke when adding, editing, or reviewing a skill's structure. Covers the published-guidance baseline, composition, frontmatter, the ref- naming rule, degrees of freedom, body shape, multi-file layout, and lifecycle; pair with `ref-skill-effectiveness` for content quality
user-invocable: false
---

# Skill Authoring Standards (Structural)

This document defines the structural standards for files under `.agents/skills/<name>/SKILL.md`. It is loaded by `skill-add` and `skill-review`. **Content-quality rules (triggerability, actionability, clarity, anti-patterns) live in `ref-skill-effectiveness`; this document covers form and architecture.**

The frontmatter `description` field is the only text the agent sees at decision time — every other concern here is downstream of getting the description right. Apply `ref-skill-effectiveness` *Triggerability* for description quality before applying any of the structural rules below.

## Sources

These standards are a **delta on published guidance**, not a local invention. Where a rule below traces to a source, it is cited; where it has no upstream equivalent, it is a project-specific addition and says so. A rule with neither a source nor a named failure it prevents is folklore — delete it (`ref-skill-effectiveness` *Anti-patterns checklist*).

Precedence order, highest first:

1. [Agent Skills specification](https://agentskills.io/specification) — the portable format. Binding for anything cross-agent.
2. [Skill authoring best practices](https://platform.claude.com/docs/en/agents-and-tools/agent-skills/best-practices) — authoring guidance and the *Checklist for effective Skills*. **The standing standard where sources conflict.**
3. [Claude Code skills](https://code.claude.com/docs/en/skills) — Claude-specific frontmatter and invocation control.
4. [Cascade skills](https://docs.devin.ai/desktop/cascade/skills) — Windsurf / Devin Desktop discovery and invocation.
5. [Context engineering for Claude 5 generation models](https://claude.com/blog/the-new-rules-of-context-engineering-for-claude-5-generation-models) — model-specific, and a blog rather than a spec. Cite only where a rule is genuinely blog-specific.

Inline the actionable rule; link the rationale. A skill must work offline, and an external URL is not a loadable disclosure level — it needs network the sandbox may not have and can redirect or 404. Copying upstream's worked examples in is the opposite error.

> Sources last reviewed 2026-07. Treat only this line as time-sensitive; every rule below is written to be read without it.

## What is a skill

A skill is defined by a `SKILL.md` file at `.agents/skills/<name>/SKILL.md` with YAML frontmatter (`name`, `description`) and a Markdown body; it may include linked files for heavy detail (*Progressive disclosure and multi-file skills*). Skills are loaded as agent context when triggered. The catalog index at `.agents/skills/README.md` is the human's view of the dependency graph.

## Composition is the architecture

No upstream equivalent — this is the project's own model. Hierarchy is expressed by reference, not by directory nesting. The directory layout stays flat; the dependency graph is what carries structure.

- **Three-property test.** Every skill is **specialized** (one concern), **self-contained** (invocable without inlining other skills), and **referenced** (invoked by at least one parent — the user, another skill, or both).
- **Extraction trigger.** Two or more skills need the same substantive guidance → factor a shared sub-skill that both invoke. Mirrors code DRY. The test is whether the guidance is *the same rule*, not how many lines it occupies.
- **Merge trigger.** A sub-skill is invoked by exactly one parent and has no independent trigger → inline it back into the parent. A single-use abstraction is a bad abstraction.
- **Pointer rule.** When a skill needs another skill's content, point with `Apply <skill>` or `See <skill>` *Section name*. Never paraphrase another skill's body. A reader who must **act** on a rule at that point gets the contract restated in one line; a reader who needs depth gets the pointer. That is the whole test — no line threshold.
- **Canonical-implementation rule.** When a rule is grounded in real code, name the file and symbol: ``Canonical implementation: `cli/_format.py` (`OutputMode`, `format_output`)``. Pointers stay accurate when prose drifts, and upstream endorses the practice directly: *"prefer files that are in code as it provides clear, high-fidelity instructions to Claude in a language it knows very well."*
- **Compatibility.** A reference sub-skill is invocable by any parent without parent-specific assumptions. If a sub-skill's content makes sense only inside one parent, it is a section, not a skill.

## Frontmatter contract

Required keys: `name`, `description`. The spec makes both binding:

- `name` — lowercase letters, numbers, and hyphens only; no leading or trailing hyphen; at most 64 characters; **matches the directory name exactly**.
- `description` — at most 1024 characters, one line, no embedded line breaks. Write it in **third person** and state what the skill does **and** when to use it. Hosts shorten long descriptions when the catalog listing is large, so **front-load the trigger words**: the clause that decides whether the skill fires must survive truncation. There is no project-specific character target — position matters, length does not. Content quality is governed by `ref-skill-effectiveness` *Triggerability*.
- `user-invocable: false` — on every `ref-`-prefixed reference skill. Only Claude Code honors it; other agents ignore unknown keys. See *Cross-agent portability*.

Every rule above is enforced by `tests/agents/test_skills_catalog.py`, which also pins the README row, the section citations, and the canonical-implementation pointers across the catalog. Frontmatter is YAML, so a `description` containing a colon-space or other YAML-significant punctuation must be double-quoted — markdownlint does not parse frontmatter and will not catch it.

## Naming and the prefix rule

Kebab-case throughout. Workflows are verb-noun (`cli-command-add`, `skill-review`). References are nouns or noun phrases (`ref-skill-effectiveness`, `ref-logging-standards`).

**The `ref-` prefix rule.** A skill carries the `ref-` prefix if and only if it is *not* intended for direct human invocation. The prefix is a **naming convention**, not an enforcement mechanism: it groups the reference set and tells a reader the skill is context-only. `user-invocable: false` is the mechanism, and only Claude Code honors it. No portable frontmatter field expresses "model-only, never human-invoked," which is why the name carries the signal.

| Intended for human invocation? | Also referenced by other skills? | Prefix |
| --- | --- | --- |
| Yes | No | none |
| Yes | Yes | none |
| No | Yes (always, by definition) | `ref-` |
| No | No | not a skill — delete it |

**Decision flowchart:**

1. Will any human ever invoke this skill directly? **Yes → unprefixed. No → continue.**
2. Is this skill referenced by at least one other skill? **No → not a skill, delete. Yes → `ref-`-prefixed.**

**Worked examples (all consistent with the rule):**

- ✅ `review-python-file` — a human invokes it directly; *also* dispatched by `review-changes`. Unprefixed. The fact that another skill references it does **not** drive the prefix decision.
- ✅ `cli-command-add` — human-invocable workflow. Unprefixed.
- ✅ `ref-python-coding-practices` — loaded by `review-python-file`, `cli-command-add`, and others; never invoked by a human. Prefixed.
- ❌ `ref-run-precommit` would be wrong: humans run precommit directly. The catalog correctly has `run-precommit` unprefixed.
- ❌ `skill-effectiveness` (unprefixed) would be wrong: it is a context-only reference, not a human workflow.

**Why it matters.** Prefixing a human-invocable workflow buries it in the reference set where no one looks for it; omitting the prefix on a context-only reference offers humans a name that does nothing useful when invoked alone. Both errors are caught by `skill-review`'s frontmatter audit.

## Degrees of freedom

From upstream, and it replaces any instinct to write every rule at the same specificity: **match specificity to the task's fragility.**

- **High freedom** — multiple valid approaches, and the agent's judgment is an asset. State the objective and the constraints; let the agent choose the path. Upstream's image is an open field.
- **Low freedom** — the operation is fragile, order-dependent, or has one correct form. Give exact commands, exact wording, and explicit prohibitions ("do not modify X"). Upstream's image is a narrow bridge.

Apply the test per rule, not per skill: one skill legitimately contains open-field guidance and narrow-bridge commands side by side. Over-constraining an open field wastes tokens and invites the agent to discount the rule; under-constraining a narrow bridge produces confident, wrong work.

The corollary is upstream's *"concise is key"*: **assume the model is competent.** Default assumption is that the agent is already very smart — a rule exists to convey a project-specific opinion or a non-obvious constraint, not to supply general good practice. Ask of every paragraph whether it justifies its token cost.

## Single concern per skill

A skill has one outcome. Trigger test: *"If the agent invokes this skill, it always wants this one outcome."* If two outcomes can fit, factor a sub-skill or split the parent.

## Structure of the `SKILL.md` body

- **Workflow skills**: imperative numbered steps. Each step is one action with one verifiable outcome (`ref-skill-effectiveness` *Outcome orientation*). Cross-reference siblings with `Apply <skill>`; never paraphrase.
- **Reference skills**: lead sentence + named-bold-sub-bullet shape for multi-part rules. Fenced code blocks for canonical forms. `Canonical implementation: <path> (<symbol>)` pointers wherever a real exemplar exists.
- **Sub-bullet convention exemplars**: `ref-python-coding-practices` rules 9, 17, 18, 19.
- **Writing quality** is governed by `ref-skill-effectiveness` *Clarity and writing standards* (voice, hedge words, decorative language, sentence length, throat-clearing).

## Body length

**Keep the body under 500 lines** — the published limit. There is no project-specific floor or tighter ceiling: a three-line skill that encodes one real gotcha is a good skill, and inventing a minimum only pads it.

Length is a symptom, not the disease. When a body grows past the limit — or grows heavy well before it — the fix is ordered:

1. Move detail a given invocation rarely needs into a linked file (*Progressive disclosure and multi-file skills*). Upstream is explicit: *"For long skills, try and use progressive disclosure as much as possible — divide it into many files and split them out."*
2. If two skills still carry the same material, apply the *Composition is the architecture* extraction trigger.
3. If the body covers two outcomes, split it (*Single concern per skill*).

## Progressive disclosure and multi-file skills

A skill loads in three levels; only the lightest level is always resident, so what you put where is a real token-cost decision:

- **Level 1 — `name` + `description`**: always in context for every installed skill (the catalog the agent routes from). Costs tokens every session whether or not the skill fires.
- **Level 2 — the `SKILL.md` body**: loaded only when the skill is invoked, and then it stays for the rest of the session. Every line is a recurring cost once loaded.
- **Level 3 — linked files** (`reference.md`, `examples.md`, fixture JSON, `scripts/`): loaded only when the body links to them and the agent reads them. Heavy detail here costs nothing until needed.

**Rule.** Keep the body to the always-needed core — the steps or rules that apply on every invocation. Heavy reference detail that a given invocation rarely needs (long tables, canonical wording other skills cite, exhaustive examples, API dumps) goes in a Level-3 linked file, not the body. Bloating the body or spawning a new `ref-`-prefixed skill to hold it are both the wrong move; the linked file is the release valve for *Body length*.

**Keep references one level deep.** A `SKILL.md` links to its own files; those files do not link onward to a third level. Upstream states the mechanism: the agent may read a nested file partially — `head -100` on a long file — and act on an incomplete picture. Depth is what makes that failure likely, and it is silent.

**Granularity — scope each linked file to one independently-loadable chunk.** Match file boundaries to *what is needed together* — the same single-concern test *Single concern per skill* applies to skills.

- **Test**: would a given task need all of a file, or just part? If just part, split along that seam so the agent loads only the slice it needs.
- **Too coarse**: a catch-all `reference.md` bundling unrelated topics forces the agent to load everything to get anything.
- **Too fine**: detail always consumed together (a checklist and the rules it depends on) split into fragments the body must pull in lockstep.

**Mechanics.** The frontmatter stays in `SKILL.md`; linked files are plain Markdown with no frontmatter, referenced by a relative-path markdown link (*Referencing files and skills*), and the body must point at each one (an orphan file the body never references is dead weight). Name each file for its slice, not generically. A linked file is **private to its owning skill**: another skill that needs the content references the owning skill by name, never reaches into its linked file by path. Canonical layout:

```text
my-skill/
├── SKILL.md           # entry point: always-needed core + links out
├── api-reference.md   # one heavy topic, loaded only when that topic is needed
├── error-codes.md     # a different topic, loaded independently
├── examples.md        # worked examples, loaded only when an example is wanted
└── scripts/           # executed, not loaded into context
```

Scripts are executed, not read into context — reference them by relative path (or `${CLAUDE_SKILL_DIR}/...` when the working directory varies).

## Referencing files and skills

Use the form that matches the target:

| Target | Form |
| --- | --- |
| A skill's **own** bundled file | relative-path markdown link: `[reference.md](reference.md)` |
| **Another skill** | its name in backticks: `cli-command-add` — never a path |
| **A section of another skill** | the skill name plus the section *name*, never a number: ``ref-cli-help-standards`` *Help-content contract* |
| **Source / test code** | a backtick pointer with symbol: `cli/_help.py` (`build_help`) — not a link |
| A **repo doc** | a markdown link that resolves from the skill file: `[CLI](../../../docs/CLI.md)` — a `SKILL.md` sits three levels below the repo root, so a root-relative `docs/CLI.md` would resolve inside the skill directory and break |

- **Cite sections by name, not by number.** A name survives insertion and renumbering, is findable by string match even in a partial read, and tells the reader at the call site *what* is being referenced. Section numbers are an addressing scheme that silently rots. The exception is a skill whose content *is* a numbered list, where the number is the item's real name — the numbered rules in `ref-python-coding-practices` and the numbered steps in `review-python-file`. Even there, a citation carries a short name alongside the number ("rule 15 (CLI is click + `@run_async`)", "step 4 (Code smells)"), so it survives renumbering and is findable by string match.
- **Reference skills by `name`, never by file path.** Skills relocate; names are stable. A skill's bundled files are private to it — reach them through the owning skill, never by path.
- **Wrapping skills enumerate the wrapped skills explicitly** in their description or lead paragraph (e.g., "wraps `ref-mcp-module-organization` + `pydocs-improve` + `ref-logging-standards`").
- **Dependency direction**: workflows → references → more-specialized references. Reference skills do not invoke workflow skills.

## README catalog sync

Every skill appears in `.agents/skills/README.md` under the appropriate section table with a one-sentence purpose. The one-sentence purpose is itself subject to `ref-skill-effectiveness` *Triggerability* (it functions as a second-tier description). The README is the human's view of the dependency graph; an unlisted skill is invisible to contributors.

## Lifecycle

- **Renaming.** Update every cross-reference simultaneously: sibling skills, the catalog README, `AGENTS.md` pointers. Use `git mv <old>/SKILL.md <new>/SKILL.md` to preserve history. Verify with `grep -r '<old-name>' .agents/ AGENTS.md` — zero hits before merge.
- **Prefix change.** A skill's human-invocability can change (a reference becomes a workflow, or vice versa). Rename to add or remove the `ref-` prefix, and add or remove `user-invocable: false` in the same edit; follow the renaming procedure above. The prefix change is not a separate operation from the rename.
- **Deprecation.** When replacing a skill rather than renaming it, leave the file in place for one development cycle with a top-of-body callout (`> **Deprecated**: use <new-skill> instead.`) so cross-references can migrate. Remove the file in the next cycle.
- **Removal.** Verify with `grep -r '<name>' .agents/ AGENTS.md docs/` — zero hits before deleting the file. Remove the catalog README row in the same commit.

## Conflict and precedence

When two skills overlap on a topic:

- **More-specific wins.** `ref-logging-standards` overrides a generic "follow project style" rule.
- **`AGENTS.md` is always-loaded but terse.** It does not override a skill's detailed guidance; it points at the skill.
- **Canonical-implementation pointer beats prose.** When the skill's prose and the real code disagree, the code is the source of truth. Refresh the prose; do not refresh the code to match outdated prose.

## Common failure modes

Used by `skill-review`'s structural audit. Drift signals that say a skill needs a structural fix. The list is illustrative, not exhaustive — flag any other signal that the skill's structure is degrading.

- **Composition rot**: body over 500 lines, or heavy well before it; parallel sections in two skills; sub-skill with one parent; paraphrase where a pointer would suffice.
- **Wrong prefix**: human-invocable workflow named `ref-`; context-only reference without it, or with the prefix but no `user-invocable: false`.
- **Multi-concern skill**: two outcomes in one file.
- **Wall-of-text rule**: multi-part rule that should be sub-bullets.
- **Stale cross-references**: named sibling does not exist; file pointer does not resolve; a cited section name or numbered rule no longer matches the target.
- **Gratuitous cross-reference**: an `Apply X` that the host step or bullet does not need to do its job — added by topical association ("this step mentions output, and X is about output") rather than necessity. The cross-reference analog of decorative prose (`ref-skill-effectiveness` *Anti-patterns checklist*). Count is irrelevant: a parent may cite X at every location that genuinely needs it. **Load-bearing test**: delete the reference and ask whether the host step can still be completed correctly. If yes, it was decoration — remove it. Two common sources: (a) the step reaches X anyway through a sibling it already invokes (e.g. `pydocs-improve` cites X, but its correctness step already chains through `pydocs-accuracy`, which owns X); (b) the step's real task is mechanical and independent of X (e.g. "single-source the OutputSpec constant" does not depend on output-serialization conventions — verifying the field text does, one step later).
- **Missing README row**: skill on disk, no entry in catalog.
- **Frontmatter drift**: `name` violates the spec charset or ≠ directory; description fails `ref-skill-effectiveness` *Triggerability*.
- **Staled by a code change**: a skill that enumerates modules, nouns, or symbols goes stale when the tree changes, even though no skill file was touched. `ref-project-reference` (the module map) and `cli-command-add` (the noun list) are the two that enumerate by design, so they carry this risk permanently — the owning workflow updates them in the same changeset, and `review-changes` checks for it.
- **Voodoo constant**: a numeric threshold with no source and no property behind it. Upstream states the rule directly — *"No 'voodoo constants' (all values justified)."* Either name the constraint the number enforces, or state the property and drop the number.

Content failures (triggerless description, mushy outcome, hedge cluster, throat-clearing, decorative prose) are governed by `ref-skill-effectiveness` *Anti-patterns checklist*, not by this skill.

## Cross-agent portability

`.agents/skills/` is the portable location — read natively by Cascade, Cursor, and Codex. Claude Code reads neither it nor the root `AGENTS.md`. Three wiring rules bridge the gap and must stay intact:

- **`.claude/skills` → `../.agents/skills`** — a git-tracked symlink (mode `120000`) so Claude Code discovers the one skill tree with no duplication.
- **Root `CLAUDE.md`** — imports the shared rules with a single `@AGENTS.md` line so Claude Code loads the same always-on guidance Cascade reads from `AGENTS.md`.
- **The `ref-` prefix plus `user-invocable: false`** — the name is the portable signal, the key is the mechanism honored only by Claude Code. A reference skill carries both.

**The portable interface is `name` + `description` only.** Every other frontmatter key is a best-effort extension that some agents ignore. Never make a skill's correctness depend on one.

Per-agent discovery paths, invocation syntax, and invocation-control keys are in [cross-agent.md](cross-agent.md) — load it when portability is the task. When restructuring the tree, renaming the root instructions file, or moving skills, re-verify all three wiring rules so neither agent silently loses the catalog.
