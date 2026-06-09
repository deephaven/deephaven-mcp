---
name: skill-review
description: Review an existing agent skill (`.agents/skills/**/SKILL.md`) or the project `AGENTS.md` — composition audit, effectiveness audit, structural audit, cross-reference audit, README sync, AGENTS.md overlap — invoke when editing or auditing a skill or `AGENTS.md`; also dispatched by `review-changes` on changes to those paths
---

# Review an Agent Skill or AGENTS.md

**Review like a senior engineer.** Every finding must answer three questions in concrete terms: *what is wrong*, *what is better*, *why the change is worth its cost*. Be conscious of suggestions that do not serve a purpose.

Wraps `_skill-authoring-standards` (structural standards), `_skill-effectiveness` (content standards), and `_agents-md-curation` (when the target is `AGENTS.md`). Effectiveness and composition are co-headline audits; structure is the supporting audit.

The findings report has three subsections — **Architecture**, **Effectiveness**, **Structure** — so the user can act on each axis independently. Architecture findings extract or merge skills; effectiveness findings rewrite descriptions and bodies; structural findings are surgical fixes.

## Steps

1. **Identify the target.** Is this a skill file (`.agents/skills/<name>/SKILL.md`) or `AGENTS.md`? Branch on the answer:
    - **Skill**: apply `_skill-authoring-standards` + `_skill-effectiveness`.
    - **`AGENTS.md`**: apply `_agents-md-curation` + `_skill-effectiveness`.

2. **Composition audit (Architecture).** Two non-negotiable refactoring questions; each gets an explicit answer in the report.
    - **Should material in this skill move *into* a sub-skill?** Triggers: section > ~50 lines on a discrete sub-concern; section paraphrased in another skill; section conceptually independent with its own trigger.
    - **Should material in a sub-skill move *out of* it?** Triggers: sub-skill < ~20 lines; sub-skill referenced by exactly one parent; sub-skill has no independent trigger.
    - **Are the children correct?** Skills this one invokes must each contribute their concern. Drift signal: an `Apply <skill>` line that does not carry weight in the body.
    - **Are the parents correct?** Skills that invoke this one must each have a real reason to. Drift signal: a parent that wraps in name only.
    - **Synthesis**: would an agent invoking this skill waste tokens reading content it does not need (extract), or take an extra hop for content it always needs (merge)?

3. **Effectiveness audit (Effectiveness).** Apply `_skill-effectiveness`.
    - **Triggerability test** (§1): read the description alongside ten sibling descriptions; can a reader route a representative task correctly without reading bodies?
    - **Actionability test** (§2): does each workflow step / reference rule have one verb and one verifiable outcome?
    - **Outcome test** (§3): is success verifiable? Workflows state an end state; reference rules state a verifiable property.
    - **Pattern density** (§4): are non-trivial rules grounded in canonical-implementation pointers and concrete examples?
    - **Scope-completeness audit** (§2): test every enumerated list against the §2 closed-vs-open-ended rule; flag a closed checklist on an open-ended topic (code smells, security risks, design problems) as a false-closure bug — the agent reads the list as exhaustive. §2 owns the remediation.
    - **Hedge-word grep**: search the file for `might`, `could`, `generally`, `typically`, `usually`, `sometimes`, `perhaps`, `consider`, `try to`, `often`, `mostly`. Flag every hit.
    - **Decorative-prose grep**: search for `obviously`, `simply`, `just`, `clearly`, `elegant`, `clean`. Flag every hit.
    - **Anti-patterns walk** (§6): step through every item in the checklist.

4. **Frontmatter audit (skills only).**
    - `name` matches the directory name exactly.
    - `description` satisfies `_skill-effectiveness` §1.
    - **`_`-prefix matches the human-invocability rule** (`_skill-authoring-standards` §4): `_`-prefixed iff no human invokes it. Being referenced by another skill does not drive the decision — that is the most common mistake.
    - **`user-invocable` matches the prefix** (`_skill-authoring-standards` §3): a `_`-prefixed reference skill carries `user-invocable: false`; an unprefixed workflow omits the key. A prefix without the key leaks the reference into Claude Code's menu.

5. **Structural audit (Structure).** Apply `_skill-authoring-standards` §6 and §13.
    - Workflow → imperative numbered steps; reference → lead sentence + sub-bullets; fenced code blocks for canonical forms; canonical-implementation pointers.
    - Drift signals from §13: composition rot, wrong prefix, multi-concern skill, wall-of-text rule, stale cross-references, missing README row, frontmatter drift.

6. **Length and progressive-disclosure audit.** Apply `_skill-authoring-standards` §7 and §8.
    - Body within 30–250 lines. Outside the band needs justification (extract or merge per §2, or move reference detail to a linked file per §8).
    - Flag any body carrying heavy reference detail a given invocation rarely needs (long tables, exhaustive examples, canonical wording other skills cite) — it belongs in a Level-3 linked file, not the always-loaded body. A body near the budget that has no linked files is the signal.
    - **Granularity** (§8): flag a catch-all linked file that bundles topics reached for separately, and the opposite — fragments always loaded together that should merge.
    - Every Level-3 linked file is referenced from the body by relative path; flag orphan files the body never points at.

7. **Cross-reference audit.**
    - Every named sibling skill exists on disk.
    - Every `path/to/file.py` canonical-implementation pointer resolves.
    - Every "rule #N" or "§N" pointer matches the current numbering of the target skill.
    - Every Level-3 linked file referenced by the body resolves on disk.
    - No skill references another skill's linked file by path; cross-skill references name the owning skill (§8, §9). Flag any `<other-skill>/<file>.md` path pointer.
    - Reference forms follow §9: a skill's own bundled file is a markdown link; another skill is named, not path-linked; source/test code is a backtick pointer.
    - Dual-agent wiring resolves (`_skill-authoring-standards` §14): `.claude/skills` symlinks to `../.agents/skills`, and root `CLAUDE.md` imports `@AGENTS.md`.

8. **README sync (skills only).** Skill present in `.agents/skills/README.md` under the correct section. One-sentence purpose accurate. The purpose itself satisfies `_skill-effectiveness` §1.

9. **`AGENTS.md` overlap.** No fact duplicated between the target skill and `AGENTS.md`. Apply `_agents-md-curation` §5. If duplication exists, identify the single source of truth and reduce the other side to a pointer.

10. **Lifecycle check.** If the target is being renamed, deprecated, or removed, apply `_skill-authoring-standards` §11.

11. **Report findings** in three subsections, in this order:
    - **Architecture**: composition findings (extract / merge / re-parent / re-child).
    - **Effectiveness**: triggerability, actionability, clarity, anti-pattern findings.
    - **Structure**: form, cross-reference, length, README sync, frontmatter findings.

    Each finding answers: *what is wrong*, *what is better*, *why the change is worth its cost*.

## Verification

- All three audit subsections (Architecture, Effectiveness, Structure) present in the report, even if empty (an empty subsection is itself a finding: "no Architecture concerns").
- Hedge-word grep and decorative-prose grep run and reported.
- Triggerability test run against ten sibling descriptions.
- Every cross-reference checked on disk, including Level-3 linked files and the dual-agent wiring.
- `_`-prefix / `user-invocable` pairing checked.
