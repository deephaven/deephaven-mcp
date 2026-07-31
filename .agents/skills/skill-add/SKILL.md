---
name: skill-add
description: Add a new agent skill end-to-end — invoke when creating a new file under `.agents/skills/`. Runs the composition gate, chooses the name and `ref-` prefix, drafts a triggering description and a body that satisfies the effectiveness and structural standards, cross-links parents, registers the catalog README row, and runs `skill-review`
---

# Add an Agent Skill

End-to-end workflow for creating a new skill at `.agents/skills/<name>/SKILL.md`. Wraps `ref-skill-authoring-standards` (structure) + `ref-skill-effectiveness` (content) + `ref-agents-md-curation` (decide whether the content belongs in `AGENTS.md` instead).

A new skill that no parent invokes is invisible. The Composition gate at step 2 is the most important step — run it before drafting anything.

## Steps

1. **Decide reference vs workflow.** Apply `ref-skill-authoring-standards` *Naming and the prefix rule*. A workflow is a verb-noun action a human invokes directly. A reference is a noun-phrase standards document loaded by other skills.

2. **Composition gate.** Required first design step.
    - Search the catalog `.agents/skills/README.md` for an existing skill that already covers this concern. If found, edit that skill instead. **Stop here.**
    - Identify which existing skills the new skill will *invoke* (its children).
    - Identify which existing skills will *invoke* the new skill (its parents).
    - If the new skill would have exactly one parent and no independent trigger, it is a *section* of that parent. Inline it; do not create a new file.
    - Apply `ref-skill-authoring-standards` *Composition is the architecture* extraction and merge triggers.

3. **Decide skill vs `AGENTS.md` vs both.** Apply `ref-agents-md-curation` *What belongs in `AGENTS.md`* and *What does NOT belong in `AGENTS.md`*. If the content fits both, pick the single source of truth and decide which file points to which.

4. **Pick the name and prefix.** Apply `ref-skill-authoring-standards` *Naming and the prefix rule*. The prefix rule answers one question: *will any human ever invoke this skill directly?* If yes (even occasionally), no prefix — even if other skills also reference it. If no, prefix with `ref-`.
    - The `name` must be lowercase letters, numbers, and hyphens only, at most 64 characters, and identical to the directory name. The spec rejects anything else.
    - A `ref-`-prefixed skill also carries `user-invocable: false` (`ref-skill-authoring-standards` *Frontmatter contract*). Unprefixed workflows omit the key.

5. **Draft the `description`.** Apply `ref-skill-effectiveness` *Triggerability*. Third person; state what the skill does **and** when to use it; **front-load the trigger** so truncation cannot eat it; add the "not for X" clause when a sibling is ambiguous.
    - Run the **triggerability test**: read your draft alongside the ten nearest sibling descriptions in the catalog. Given a representative task, would a reader pick your skill from descriptions alone? If no, rewrite.

6. **Draft the body.** Apply `ref-skill-authoring-standards` *Structure of the `SKILL.md` body* for form, and `ref-skill-effectiveness` (*Actionability*, *Outcome orientation*, *Examples*, *Clarity and writing standards*) for content.
    - Workflow: imperative numbered steps; one action and one verifiable outcome per step.
    - Reference: lead sentence + named-bold sub-bullets; fenced code blocks for canonical forms; canonical-implementation pointers wherever a real exemplar exists.
    - **Match specificity to fragility** (`ref-skill-authoring-standards` *Degrees of freedom*): exact commands where the operation is fragile, objective-and-constraints where several approaches are valid. Assume the agent is competent — a rule earns its tokens by carrying a project-specific opinion or a non-obvious constraint, not general good practice.
    - Cross-reference children with `Apply <skill>` — never paraphrase. Cite another skill's section by **name**, never by number.
    - Reference files and skills per `ref-skill-authoring-standards` *Referencing files and skills*: own bundled files as markdown links, other skills by name, source/test code as backtick pointers.

7. **Shape for progressive disclosure.** Apply `ref-skill-authoring-standards` *Progressive disclosure and multi-file skills*: keep the always-needed core in `SKILL.md` and move heavy detail an invocation rarely needs into scoped linked files (its *Granularity* rule governs how to split them). Outcome: the body is under 500 lines, references stay one level deep, and every linked file is referenced from the body by relative path.

8. **Cross-link parents.** For each parent skill identified in step 2, edit that skill to add the dispatch line or wrap reference. A skill that no parent invokes is invisible.

9. **Add the catalog README row.** Edit `.agents/skills/README.md`: pick the right section table, alphabetical ordering within the section, one-sentence purpose. The purpose itself must satisfy `ref-skill-effectiveness` *Triggerability* (it is a second-tier description).

10. **Update `AGENTS.md` if applicable.** Apply `ref-agents-md-curation` *Single source of truth* and *Sync rules*.

11. **Run `skill-review`** on the new skill. Address every finding before merging.

12. **Run `uv run pytest tests/agents/test_skills_catalog.py`.** It enforces the catalog contract mechanically: frontmatter parses as YAML, `name` matches the directory and is spec-legal, `description` is a bounded single line, no unexpected keys, the `ref-`/`user-invocable` pairing, the README row, and that every section citation and canonical-implementation pointer resolves.

13. **Run `./bin/precommit.sh`.** markdownlint lints the catalog README **and every `SKILL.md`**: `.markdownlint-cli2.jsonc` excludes the `.agents/` tree, then re-includes `.agents/skills/**/*.md` with a trailing positive glob. Skill bodies lint under the relaxed set in `.agents/skills/.markdownlint.jsonc`, which turns off MD041 so a body may open with a prose callout instead of an H1.

## Verification

`uv run pytest tests/agents/test_skills_catalog.py` covers the mechanical half of this list — frontmatter validity, `name` and `description` conformance, the `ref-`/`user-invocable` pairing, the README row, section citations, and canonical-implementation pointers. Run it first; the remaining items are judgment calls it cannot make.

- File exists at `.agents/skills/<name>/SKILL.md` with valid frontmatter (`name`, `description`).
- The body is under 500 lines and every reference is one level deep.
- Any linked file is referenced from the body by a relative-path markdown link (no orphan files); references follow `ref-skill-authoring-standards` *Referencing files and skills* (own files linked, other skills named, code as pointers).
- No section is cited by number — `grep '§' <new-skill>/SKILL.md` returns nothing.
- At least one parent skill cross-references the new skill (or, for the rare top-level human workflow, the catalog README is the only reference).
- `skill-review` of the new skill reports no findings under Architecture, Effectiveness, or Structure.
- `./bin/precommit.sh` returns 0.
