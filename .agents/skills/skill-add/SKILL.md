---
name: skill-add
description: Add a new agent skill end-to-end — run the composition gate, choose name and `_`-prefix correctly, draft a triggering description, draft a body that satisfies effectiveness and structural standards, cross-link parents, register in the catalog README, run `skill-review` — invoke when creating a new file under `.agents/skills/`
---

# Add an Agent Skill

End-to-end workflow for creating a new skill at `.agents/skills/<name>/SKILL.md`. Wraps `_skill-authoring-standards` (structure) + `_skill-effectiveness` (content) + `_agents-md-curation` (decide whether the content belongs in `AGENTS.md` instead).

A new skill that no parent invokes is invisible. The Composition gate at step 2 is the most important step — run it before drafting anything.

## Steps

1. **Decide reference vs workflow.** Apply `_skill-authoring-standards` §4. A workflow is a verb-noun action a human invokes via slash command. A reference is a noun-phrase standards document loaded by other skills.

2. **Composition gate.** Required first design step.
    - Search the catalog `.agents/skills/README.md` for an existing skill that already covers this concern. If found, edit that skill instead. **Stop here.**
    - Identify which existing skills the new skill will *invoke* (its children).
    - Identify which existing skills will *invoke* the new skill (its parents).
    - If the new skill would have exactly one parent and no independent trigger, it is a *section* of that parent. Inline it; do not create a new file.
    - Apply `_skill-authoring-standards` §2 extraction and merge triggers.

3. **Decide skill vs `AGENTS.md` vs both.** Apply `_agents-md-curation` §2 and §3. If the content fits both, pick the single source of truth and decide which file points to which.

4. **Pick the name and prefix.** Apply `_skill-authoring-standards` §4. The `_`-prefix rule answers one question: *will any human ever type `/<name>` to invoke this skill?* If yes (even occasionally), no prefix — even if other skills also reference it. If no, prefix.
    - If the skill is `_`-prefixed, set `user-invocable: false` in frontmatter (`_skill-authoring-standards` §3) so it stays out of Claude Code's menu, not just Cascade's autocomplete. Unprefixed workflows omit the key.

5. **Draft the `description`.** Apply `_skill-effectiveness` §1. The description must name (a) the action verb, (b) the artifact or situation, (c) what the skill is *not* for when sibling ambiguity exists.
    - Run the **triggerability test**: read your draft alongside the ten nearest sibling descriptions in the catalog. Given a representative task, would a reader pick your skill from descriptions alone? If no, rewrite.

6. **Draft the body.** Apply `_skill-authoring-standards` §6 for structure and `_skill-effectiveness` §2–§5 for content.
    - Workflow: imperative numbered steps; one action and one verifiable outcome per step.
    - Reference: lead sentence + named-bold sub-bullets; fenced code blocks for canonical forms; canonical-implementation pointers wherever a real exemplar exists.
    - Cross-reference children with `Apply <skill>` — never paraphrase.
    - Reference files and skills per `_skill-authoring-standards` §9: own bundled files as markdown links, other skills by name, source/test code as backtick pointers.

7. **Shape for progressive disclosure.** Apply `_skill-authoring-standards` §8: keep the always-needed core in `SKILL.md` and move heavy detail an invocation rarely needs into scoped Level-3 linked files (§8 Granularity governs how to split them). Outcome: the body is within the §7 budget and every linked file is referenced from it by relative path.

8. **Cross-link parents.** For each parent skill identified in step 2, edit that skill to add the dispatch line or wrap reference. A skill that no parent invokes is invisible.

9. **Add the catalog README row.** Edit `.agents/skills/README.md`: pick the right section table, alphabetical ordering within the section, one-sentence purpose. The purpose itself must satisfy `_skill-effectiveness` §1 (it is a second-tier description).

10. **Update `AGENTS.md` if applicable.** Apply `_agents-md-curation` §5 (single source of truth) and §7 (sync rules).

11. **Run `skill-review`** on the new skill. Address every finding before merging.

12. **Run `./bin/precommit.sh`.** markdownlint lints the catalog README (re-included in `.markdownlint-cli2.jsonc`'s `globs`); the rest of the `.agents/` tree, including every `SKILL.md`, is excluded. So a skills-only change is linted only through the README.

## Verification

- File exists at `.agents/skills/<name>/SKILL.md` with valid frontmatter (`name`, `description`).
- `name` in frontmatter matches the directory name.
- `_`-prefixed skills carry `user-invocable: false`; unprefixed workflows do not.
- Any Level-3 linked file is referenced from the body by a relative-path markdown link (no orphan files); references follow `_skill-authoring-standards` §9 (own files linked, other skills named, code as pointers).
- Catalog README contains a row for the new skill in the correct section.
- At least one parent skill cross-references the new skill (or, for the rare top-level human workflow, the catalog README is the only reference).
- `skill-review` of the new skill reports no findings under Architecture, Effectiveness, or Structure.
- `./bin/precommit.sh` returns 0.
