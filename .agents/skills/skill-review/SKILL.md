---
name: skill-review
description: Review an existing agent skill (`.agents/skills/**/SKILL.md`) or the project `AGENTS.md` — invoke when editing or auditing a skill or `AGENTS.md`; also dispatched by `review-changes` on changes to those paths. Runs the composition, effectiveness, structural, and cross-reference audits plus README sync and `AGENTS.md` overlap, against the published Agent Skills checklist
---

# Review an Agent Skill or AGENTS.md

**Review like a senior engineer.** Every finding must answer three questions in concrete terms: *what is wrong*, *what is better*, *why the change is worth its cost*. Be conscious of suggestions that do not serve a purpose.

Wraps `ref-skill-authoring-standards` (structural standards), `ref-skill-effectiveness` (content standards), and `ref-agents-md-curation` (when the target is `AGENTS.md`). Effectiveness and composition are co-headline audits; structure is the supporting audit.

The findings report has three subsections — **Architecture**, **Effectiveness**, **Structure** — so the user can act on each axis independently. Architecture findings extract or merge skills; effectiveness findings rewrite descriptions and bodies; structural findings are surgical fixes.

## Steps

1. **Identify the target.** Is this a skill file (`.agents/skills/<name>/SKILL.md`) or `AGENTS.md`? Branch on the answer:
    - **Skill**: apply `ref-skill-authoring-standards` + `ref-skill-effectiveness`.
    - **`AGENTS.md`**: apply `ref-agents-md-curation` + `ref-skill-effectiveness`.

2. **Composition audit (Architecture).** Two non-negotiable refactoring questions; each gets an explicit answer in the report.
    - **Should material in this skill move *into* a sub-skill?** Triggers: a section covering a discrete sub-concern that a given invocation rarely needs; a section paraphrased in another skill; a section conceptually independent with its own trigger.
    - **Should material in a sub-skill move *out of* it?** Triggers: the sub-skill is referenced by exactly one parent, or has no independent trigger. Size is not a trigger on its own — a short skill that encodes one real gotcha is a good skill.
    - **Are the children correct?** Skills this one invokes must each contribute their concern. Drift signal: an `Apply <skill>` line that does not carry weight in the body.
    - **Are the parents correct?** Skills that invoke this one must each have a real reason to. Drift signal: a parent that wraps in name only.
    - **Synthesis**: would an agent invoking this skill waste tokens reading content it does not need (extract), or take an extra hop for content it always needs (merge)?

3. **Effectiveness audit (Effectiveness).** Apply `ref-skill-effectiveness`.
    - **Triggerability test** (*Triggerability*): read the description alongside ten sibling descriptions; can a reader route a representative task correctly without reading bodies? Confirm it is third person, states what **and** when, and front-loads the trigger so truncation cannot remove it.
    - **Actionability test** (*Actionability*): does each workflow step / reference rule have one verb and one verifiable outcome?
    - **Outcome test** (*Outcome orientation*): is success verifiable? Workflows state an end state; reference rules state a verifiable property.
    - **Grounding test** (*Examples*): is each non-trivial rule grounded in a canonical-implementation pointer, or in a named failure it prevents? An example is required only where a precise statement cannot carry the point — flag padding as readily as absence.
    - **Degrees-of-freedom test** (`ref-skill-authoring-standards` *Degrees of freedom*): is specificity matched to fragility? Flag exact-command prescription where several approaches are valid, and vague guidance on a fragile, order-dependent operation.
    - **Over-constraint test**: flag any rule whose only job is to stop a competent agent from doing something obviously wrong. Keep it only if it encodes a project-specific opinion or a test enforces it.
    - **Scope-completeness audit** (*Actionability*): test every enumerated list against the closed-vs-open-ended rule; flag a closed checklist on an open-ended topic (code smells, security risks, design problems) as a false-closure bug — the agent reads the list as exhaustive.
    - **Hedge-word grep**: search the file for `might`, `could`, `generally`, `typically`, `usually`, `sometimes`, `perhaps`, `consider`, `try to`, `often`, `mostly`. Flag every hit.
    - **Decorative-prose grep**: search for `obviously`, `simply`, `just`, `clearly`, `elegant`, `clean`. Flag every hit.
    - **Anti-patterns walk** (*Anti-patterns checklist*): step through every item.
    - **Published checklist walk**: step through the *Checklist for effective Skills* in the [best-practices doc](https://platform.claude.com/docs/en/agents-and-tools/agent-skills/best-practices) — Core quality and Code and scripts. It is the outer standard this audit serves; the local checks above are the project-specific delta on it.

4. **Frontmatter audit (skills only).** Run `uv run pytest tests/agents/test_skills_catalog.py` first — it decides every item below mechanically, plus the README row, the section citations, and the canonical-implementation pointers. Read the rest of this step to interpret a failure, not to hand-check what the test already covers.
    - `name` matches the directory name exactly, and is spec-legal: lowercase letters, numbers, and hyphens only, no leading or trailing hyphen, at most 64 characters.
    - `description` satisfies `ref-skill-effectiveness` *Triggerability* and is at most 1024 characters.
    - **The `ref-` prefix matches the human-invocability rule** (`ref-skill-authoring-standards` *Naming and the prefix rule*): prefixed iff no human invokes it. Being referenced by another skill does not drive the decision — that is the most common mistake.
    - **`user-invocable` matches the prefix** (*Frontmatter contract*): a `ref-`-prefixed reference skill carries `user-invocable: false`; an unprefixed workflow omits the key.

5. **Structural audit (Structure).** Apply `ref-skill-authoring-standards` *Structure of the `SKILL.md` body* and *Common failure modes*.
    - Workflow → imperative numbered steps; reference → lead sentence + sub-bullets; fenced code blocks for canonical forms; canonical-implementation pointers.
    - Walk the *Common failure modes* drift signals: composition rot, wrong prefix, multi-concern skill, wall-of-text rule, stale cross-references, gratuitous cross-reference, missing README row, frontmatter drift, voodoo constant.
    - **Voodoo-constant grep**: `grep -nE '(^|[^A-Za-z0-9])~?[0-9]+ (lines|chars|characters|bullets|words|sites|places|commands|tools|files|skills|nouns|verbs|steps|rules)'`. Every surviving number names the constraint it enforces or the source it comes from; otherwise state the property and delete the number. Countable nouns matter as much as size budgets — a hardcoded "across all 21 sites" drifts the moment a caller is added, and no test pins it.

6. **Length and progressive-disclosure audit.** Apply `ref-skill-authoring-standards` *Body length* and *Progressive disclosure and multi-file skills*.
    - Body under 500 lines. There is no lower bound and no tighter project ceiling.
    - Flag any body carrying heavy reference detail a given invocation rarely needs (long tables, exhaustive examples, canonical wording other skills cite) — it belongs in a linked file, not the always-loaded body. A long body with no linked files is the signal, well before the limit.
    - **Granularity**: flag a catch-all linked file that bundles topics reached for separately, and the opposite — fragments always loaded together that should merge.
    - **Depth**: references stay one level deep. A linked file that links onward invites a partial read (`head -100`) and silent incompleteness.
    - Every linked file is referenced from the body by relative path; flag orphan files the body never points at.

7. **Cross-reference audit.**
    - Every named sibling skill exists on disk.
    - Every `path/to/file.py` canonical-implementation pointer resolves.
    - **Sections are cited by name, not number**: `grep '§'` on the target returns nothing, except inside a grep command that documents this very check (`skill-add` and this skill). Every cited section name resolves to a real heading in the target skill; every numbered-rule citation carries its short name and still matches.
    - Every linked file referenced by the body resolves on disk.
    - No skill references another skill's linked file by path; cross-skill references name the owning skill. Flag any `<other-skill>/<file>.md` path pointer.
    - Reference forms follow *Referencing files and skills*: a skill's own bundled file is a markdown link; another skill is named, not path-linked; source/test code is a backtick pointer.
    - Dual-agent wiring resolves (`ref-skill-authoring-standards` *Cross-agent portability*): `.claude/skills` symlinks to `../.agents/skills`, and root `CLAUDE.md` imports `@AGENTS.md`.

8. **README sync (skills only).** Skill present in `.agents/skills/README.md` under the correct section. One-sentence purpose accurate. The purpose itself satisfies `ref-skill-effectiveness` *Triggerability*.

9. **`AGENTS.md` overlap.** No fact duplicated between the target skill and `AGENTS.md`. Apply `ref-agents-md-curation` *Single source of truth*. If duplication exists, identify the single source of truth and reduce the other side to a pointer.

10. **Lifecycle check.** If the target is being renamed, deprecated, or removed, apply `ref-skill-authoring-standards` *Lifecycle*.

11. **Report findings** in three subsections, in this order:
    - **Architecture**: composition findings (extract / merge / re-parent / re-child).
    - **Effectiveness**: triggerability, actionability, clarity, anti-pattern findings.
    - **Structure**: form, cross-reference, length, README sync, frontmatter findings.

    Each finding answers: *what is wrong*, *what is better*, *why the change is worth its cost*.

## Verification

- All three audit subsections (Architecture, Effectiveness, Structure) present in the report, even if empty (an empty subsection is itself a finding: "no Architecture concerns").
- Hedge-word grep and decorative-prose grep run and reported.
- Triggerability test run against ten sibling descriptions.
- Every cross-reference checked on disk, including linked files and the dual-agent wiring.
- `ref-` prefix / `user-invocable` pairing checked.
- The section-pointer grep and the voodoo-constant grep both run and reported. The voodoo-constant grep covers countable nouns (`sites`, `commands`, `files`) as well as `lines`/`chars`/`bullets`/`words` — a hardcoded count of call sites drifts exactly like a line budget.
- `uv run pytest tests/agents/test_skills_catalog.py` is green.
- The published *Checklist for effective Skills* walked.
