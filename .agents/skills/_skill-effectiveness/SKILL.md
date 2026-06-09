---
name: _skill-effectiveness
description: Content-quality standards for any agent-facing prose — skill descriptions, skill bodies, AGENTS.md rules, MCP tool docstrings, error-code help text — covers triggerability, actionability, outcome orientation, pattern density, clarity, hedge-word audit, anti-patterns, and field failure signals — invoke when authoring or reviewing content that an AI agent reads at decision or execution time
user-invocable: false
---

# Skill Effectiveness

A skill that the agent never invokes is dead code. A skill the agent invokes but cannot act on is worse. This document defines the content properties that make agent-facing prose actually drive behavior. It is loaded by `_skill-authoring-standards`, `_agents-md-curation`, `skill-add`, and `skill-review`.

Effectiveness is the goal; structure is in service of effectiveness. A perfectly-structured skill the agent never invokes is worse than a structurally-imperfect skill that fires correctly every time.

## 1. Triggerability — does the agent recognize the moment to invoke?

The frontmatter `description` is the only text the agent sees at decision time. If it does not name the moment, the skill is dark code.

- **Required content in every description**:
  - **An action verb** (`invoke when`, `use to`, `apply for`, `review`, `add`, `run`).
  - **The artifact or situation** that triggers the skill (a file path, a verb-noun task, a named workflow).
  - **What the skill is *not* for** when ambiguity with a sibling skill exists.
- **Triggerability test**: read the skill's description alongside its ten nearest sibling descriptions in the catalog README. Given a representative task, can you predict — without reading any body — which skill should fire? If no, the description is broken.
- **Acceptable / unacceptable pairs:**
  - ✅ "Add a new command to the dh-mcp CLI — wraps the click + Pattern B + structured-error + introspect conventions; prevents the most common bugs." (Names the artifact, names the verb, names the failure mode it prevents.)
  - ❌ "CLI command guidance." (Names the topic; not the moment.)
  - ✅ "Run a single test file's tests with coverage — required for assessing per-file coverage of a single source file." (Names the verb, the artifact, and the disambiguator from `tests-run`.)
  - ❌ "Test running helper." (No verb, no disambiguator, no artifact.)
  - ✅ "Verify a markdown documentation file is factually accurate — checks commands, file paths, config keys, API names, code examples, and URLs against source code; fixes inaccuracies in place." (Names verb, artifact, scope, side effect.)
  - ❌ "Documentation review skill." (All three failures.)
- **Empirical grounding**: the longest-standing effectiveness bug in this codebase was `ErrorCode.help_text` emitting one identical string per member because the description-level `__doc__` was read as a member-level attribute — a triggerability failure at the catalog layer (`dh-mcp introspect` reported "10 codes, 1 unique help string"). The skill-level analog is a description that names the topic but not the moment.

## 2. Actionability — when invoked, can the agent act?

- **Workflow steps**: one action per step, one verifiable outcome per step. A step that combines "do X and consider Y" is two steps.
- **Reference rules**: verb-form (`do X`, `avoid Y`), never adjective-form (`X is preferred`, `Y is generally bad`). Adjective-form rules invite the agent to override them with judgement.
- **Step indirection limit**: a step should not require the reader to follow more than one cross-reference to act. If step 3 says "apply skill A which says apply skill B which says apply skill C", flatten one level.
- **Canonical implementation pointers**: when a step or rule has a real exemplar in the codebase, name it: ``Canonical implementation: `cli/_errors.py` (`ErrorCode`)``. Pointers stay accurate when prose drifts.
- **Closed checklists vs. open-ended judgement.** Different skill genres have different needs:
  - **Action-add skills** (e.g., `cli-command-add`, `mcp-tool-add`, `skill-add`) get **closed checklists** — every step is mandatory and verifiable; missing a step is a bug.
  - **Review skills** (e.g., `review-python-file`, `skill-review`, `review-changes`) get **enumerated triggers + open-ended judgement empowerment** — list common categories illustratively, then explicitly invite the reviewer to flag *anything else that looks off*. No closed checklist can enumerate every code smell, every security issue, every design problem; pretending otherwise produces false closure (see §6).
  - **Scope-completeness test for any enumerated list**: ask *"is this list comprehensive for the topic, or does it imply false closure?"* If the topic is genuinely closed (the four `OutputMode` literals, the eight `ErrorCode` members), enumerate exhaustively. If the topic is open-ended (code smells, security risks, design problems), pair the illustrative list with explicit open-ended language: *"or anything else that looks off — trust your judgement."* Canonical implementation: `review-python-file/SKILL.md` step 4.

## 3. Outcome orientation — what does success look like?

- **Workflows state the verifiable end state**: "file exists at path X", "`uv run pytest` returns 0", "`./bin/precommit.sh` returns 0", "row count in README increased by N".
- **Reference rules state the verifiable property**: "no `# type: ignore` without bracketed error code", "every error code has a unique `help_text`", "every field on a `StrictSchema` subclass carries a PEP 257 trailing docstring".
- **Anti-pattern: activity-as-outcome.** "Improve the code", "review docs", "make better" — the reviewer cannot tell when to stop. Replace with the verifiable property the activity is supposed to produce.

## 4. Pattern-matching density — does the agent learn from examples?

Agents pattern-match better than they reason from abstractions. Density of concrete examples is more valuable than density of rules.

- **Concrete examples beat abstract rules.** For every non-trivial rule, include 2–3 acceptable / unacceptable pairs.
- **Fenced code blocks for canonical forms.** Do not paraphrase syntax. The canonical `match value: case _ as unexpected: typing.assert_never(unexpected)` form belongs in a fenced block, not a sentence.
- **Canonical-implementation pointers** wherever a real exemplar exists. Three real pointers beat ten paragraphs of prose.
- **Empirical grounding**: the `_python-coding-practices` rules #17 / #18 / #19 became scannable only after canonical-form fenced blocks landed and prose paraphrase came out.

## 5. Clarity and writing standards

- **Voice**:
  - Workflows use **imperative**: "Add the click decorator", "Run `uv run pytest`", "Update the README row".
  - Reference rules use **declarative-but-prescriptive**: "f-strings are preferred over `%` and `.format()`", "every named exception inherits from `McpError`".
- **No hedge words.** Forbidden list: `might`, `could`, `generally`, `typically`, `usually`, `sometimes`, `perhaps`, `consider`, `try to`, `often`, `mostly`. Replace with imperatives or delete the rule. Hedge words leave room for the agent to skip the rule.
- **No meta-commentary.** No "this skill aims to …", no "in this section we will …", no "as noted above", no "in summary".
- **No decorative language.** No `obviously`, `simply`, `just`, `clearly`, `elegant`, `clean`. They add no information and signal that the author is justifying rather than instructing.
- **Sentence length**: target ≤ 25 words. Long sentences split into two.
- **No throat-clearing intros**: a skill opens with substantive content, not with framing. The first sentence does work.
- **Keep the body to the always-needed core.** A loaded body stays in context for the whole session, so every line is a recurring per-turn cost. Move heavy reference an invocation rarely needs to a linked file (`_skill-authoring-standards` §8).

## 6. Anti-patterns checklist (used by `skill-review`)

Walk this list against every reviewed skill. Each bullet names a real failure mode observed in real catalogs.

- **Triggerless description.** Names the topic but not the moment. Test: read it alongside ten siblings; if you can't predict which task fires it, fix it.
- **Mushy outcome.** Workflow ends in "improve …" / "make better" / "review …" with no verifiable end state.
- **Hedge cluster.** More than one hedge word per 100 lines. Each is a license for the agent to skip the rule.
- **Paraphrase of another skill.** > 3 lines on a topic another skill owns. Replace with `Apply <skill>` pointer.
- **Throat-clearing intro.** First 1–2 paragraphs say nothing actionable. Delete them; the first paragraph does work.
- **Single-use sub-skill.** `_`-prefixed skill referenced by exactly one parent. Inline it.
- **Folklore rule.** Prescriptive rule with no canonical-implementation pointer and no named failure it prevents. Either ground it or delete it.
- **Decorative prose.** `obviously`, `simply`, `just`, `clean`, `elegant` applied to design choices.
- **Adjective-form rule.** "X is preferred" when "Prefer X" or "Use X" is shorter and actionable.
- **Wrong voice.** Workflow written in declarative voice; reference written in imperative voice with imagined caller.
- **False-closure list.** An enumerated list of examples on an open-ended topic (code smells, security risks, design problems, weird code) with no "or anything else" open-end. The agent will misread the list as exhaustive and miss the long tail. Fix: add explicit open-ended language (*"or anything else that looks off — trust your judgement"*) and an *"illustrative, not exhaustive"* disclaimer. See §2 closed-checklists-vs-open-ended-judgement.

## 7. Field failure signals — how do we tell a skill isn't working?

These are the symptoms that say a skill needs a content fix, not a structural fix.

- **Agent never invokes it for tasks where it should fire.** → Triggerability failure. Fix the description.
- **Agent invokes it for tasks where it should not fire.** → Over-broad description. Narrow it; add an explicit "not for X" clause.
- **Agent invokes it but produces output the skill didn't ask for.** → Actionability failure. Tighten the steps; replace adjective-form rules with verb-form.
- **Another skill paraphrases this skill.** → Composition failure. Extract a shared sub-skill or merge; apply `_skill-authoring-standards` §2.
- **A canonical-implementation pointer goes stale.** → Empirical-grounding failure. Refresh the pointer in the same PR; if the exemplar no longer exists, the rule the pointer grounded is also stale.
- **A reviewer reads the skill end-to-end before acting.** → Length / density failure. The skill is too long, or the lead does not summarize the actionable core. Extract or rewrite the lead.

## 8. Effectiveness in non-skill contexts

These rules apply equally to:

- `AGENTS.md` bullets (always-on, every-session content; effectiveness is *more* important here because every reader is forced to read it).
- MCP tool docstrings (consumed by AI agents through `tools/list`).
- Error-code `help_text` (consumed by agents through structured-error output and `dh-mcp introspect`).
- README catalog rows (the second-tier description; agents and humans both read it).

When applying effectiveness rules outside skills, the same checks apply: name the moment, name the verb, prefer examples over abstractions, avoid hedges, ground in real exemplars.
