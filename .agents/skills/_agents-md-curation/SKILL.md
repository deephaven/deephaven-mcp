---
name: _agents-md-curation
description: Standards for editing the project root `AGENTS.md` — what belongs there vs in a skill, format conventions, composition with skills, sync rules — invoke when adding, removing, or restructuring `AGENTS.md` content
user-invocable: false
---

# AGENTS.md Curation

`AGENTS.md` is the always-on ruleset loaded as a system memory at the start of every session. It is a distinct artifact from skills: skills are on-demand and triggered by description; `AGENTS.md` is unconditional and read by every reader. This skill defines what belongs in it, how it stays in sync with the skill catalog, and when content moves between the two.

Content quality of `AGENTS.md` bullets is governed by `_skill-effectiveness` (triggerability does not apply to `AGENTS.md` itself — every reader reads it — but actionability, outcome orientation, hedge-word audit, and clarity all do).

## 1. Role

`AGENTS.md` carries the universal facts every task needs before doing anything. It is read every session, by every reader. It is *not* a skill catalog and *not* a procedure manual.

## 2. What belongs in `AGENTS.md`

- Universal facts every task uses (Python floor pointer, server commands and ports, config directory layout, `uv run pytest` convention).
- Project-wide conventions an agent must know unconditionally (`git mv` for renames, `uv run` for tooling).
- One-line pointers to authoritative sources: `pyproject.toml`, `docs/CLI.md`, named skills.

## 3. What does NOT belong in `AGENTS.md`

- Procedures longer than two bullets → factor into a workflow skill.
- Per-domain coding standards → factor into a `_`-prefixed reference skill (`_python-coding-practices`, `_logging-standards`, …).
- Anything an agent needs only when working in a specific subsystem → load on demand via skill, not unconditionally.

## 4. Format conventions

- Terse bullets. Short H2 sections.
- When listing a fact that a skill owns, point to the skill rather than restating: "Apply `_configuration-conventions` before adding a new tunable."
- JSON and code blocks follow the markdown documentation standards in `_markdown-documentation-standards`.
- No hedge words; no decorative language (`_skill-effectiveness` §5).

## 5. Single source of truth

Every fact has exactly one home.

- If a fact lives in a skill, `AGENTS.md` *points* to the skill (`Apply <skill>` or `See <skill>`).
- If a fact lives in `AGENTS.md`, the relevant skill *points* to `AGENTS.md` (or omits the topic entirely).
- Never both. Duplication drifts within one PR cycle.

## 6. Composition with skills

`AGENTS.md` is itself an extraction target. Apply the composition triggers from `_skill-authoring-standards` §2 in both directions:

- **Extraction**: when an `AGENTS.md` topic accumulates more than two bullets, that topic is a candidate to move out into a skill, with `AGENTS.md` retaining a one-line pointer. The moved-out content must satisfy `_skill-effectiveness` at its new home.
- **Merge** (rare): a skill with fewer than ~10 lines that is invoked on every task is a candidate to fold into `AGENTS.md`. The fold loses the on-demand property; do this only when the skill is genuinely universal.

## 7. Sync rules

When a skill is added, removed, or renamed, audit `AGENTS.md` for pointers to that skill. The `skill-review` workflow performs this check; the `skill-add` workflow performs the complement.

- **Skill renamed**: update every `AGENTS.md` reference in the same commit.
- **Skill removed**: remove every `AGENTS.md` reference; verify with `grep '<old-name>' AGENTS.md`.
- **Skill added that supersedes an `AGENTS.md` section**: replace the section with a one-line pointer to the new skill.

## 8. Common failure modes

Used by `skill-review` when the target is `AGENTS.md`.

- **Wall-of-text section.** > 2 bullets on a topic; extraction candidate.
- **Duplicated guidance.** A fact stated in both `AGENTS.md` and a skill body; pick one home.
- **Outdated pointer.** Named skill no longer exists or has been renamed.
- **Per-subsystem detail.** A bullet that only applies inside one subsystem; move to a `_`-prefixed reference skill.
- **Hedge cluster.** Hedge words in always-on content are especially corrosive — every reader reads them and discounts the rule.
