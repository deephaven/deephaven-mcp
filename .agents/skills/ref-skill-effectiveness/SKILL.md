---
name: ref-skill-effectiveness
description: "Content-quality standards for agent-facing prose — invoke when authoring or reviewing anything an AI agent reads at decision or execution time: skill descriptions and bodies, AGENTS.md rules, MCP tool docstrings, error-code help text. Covers triggerability, actionability, outcome orientation, examples, clarity, anti-patterns, and field failure signals"
user-invocable: false
---

# Skill Effectiveness

A skill that the agent never invokes is dead code. A skill the agent invokes but cannot act on is worse. This document defines the content properties that make agent-facing prose actually drive behavior. It is loaded by `ref-skill-authoring-standards`, `ref-agents-md-curation`, `skill-add`, and `skill-review`.

Effectiveness is the goal; structure is in service of effectiveness. A perfectly-structured skill the agent never invokes is worse than a structurally-imperfect skill that fires correctly every time. The published sources and their precedence are in `ref-skill-authoring-standards` *Sources*; rules here cite them where they derive from them.

## Triggerability

The frontmatter `description` is the only text the agent sees at decision time. If it does not name the moment, the skill is dark code.

- **Required content in every description**:
  - **Third person**, per the spec — the description is catalog metadata about the skill, not an instruction to the agent.
  - **What the skill does**, stated plainly.
  - **When to use it** — the trigger: an artifact, a verb-noun task, a named workflow, a moment in a process.
  - **What the skill is *not* for** when ambiguity with a sibling skill exists. When a skill already carries a "When to use this vs. X" line in its body, that disambiguator belongs in the description too — routing happens at Level 1, and a body-only disambiguator arrives after the routing decision is already made.
- **Front-load the trigger.** Hosts shorten long descriptions when the catalog is large, and the trigger clause is the part that must survive. A description whose "when" sits in the final clause is one truncation away from being a topic label.
- **Triggerability test**: read the skill's description alongside its ten nearest sibling descriptions in the catalog README. Given a representative task, can you predict — without reading any body — which skill should fire? If no, the description is broken.
- **Acceptable / unacceptable pairs:**
  - ✅ "Add a new command to the dhcli CLI — invoke when adding or editing a verb under cli/_commands/. Wraps the click + Pattern B + structured-error + agents-manifest conventions; prevents the most common bugs." (Names the verb, the artifact, the triggering moment, and the failure mode it prevents — with the trigger ahead of the detail.)
  - ❌ "CLI command guidance." (Names the topic; not the moment.)
  - ✅ "Run a single test file's tests with coverage — required for assessing per-file coverage of a single source file." (Names the verb, the artifact, and the disambiguator from `tests-run`.)
  - ❌ "Test running helper." (No verb, no disambiguator, no artifact.)
  - ✅ "Verify a markdown documentation file is factually accurate — invoke for surgical correctness-only fixes when the document's structure is already sound; use docs-improve for a full review. Checks commands, file paths, config keys, API names, code examples, and URLs against source code, and fixes inaccuracies in place." (Names verb, artifact, trigger, the sibling disambiguator, scope, and side effect.)
  - ❌ "Documentation review skill." (All three failures.)
- **Empirical grounding**: the longest-standing effectiveness bug in this codebase was `ErrorCode.help_text` emitting one identical string per member because the description-level `__doc__` was read as a member-level attribute — a triggerability failure at the catalog layer (`dhcli agents errors` reported "10 codes, 1 unique help string"). The skill-level analog is a description that names the topic but not the moment.

## Actionability

- **Workflow steps**: one action per step, one verifiable outcome per step. A step that combines "do X and consider Y" is two steps.
- **Reference rules**: verb-form (`do X`, `avoid Y`), never adjective-form (`X is preferred`, `Y is generally bad`). Adjective-form rules invite the agent to override them with judgment.
- **Step indirection limit**: a step should not require the reader to follow more than one cross-reference to act. If step 3 says "apply skill A which says apply skill B which says apply skill C", flatten one level.
- **Canonical implementation pointers**: when a step or rule has a real exemplar in the codebase, name it: ``Canonical implementation: `cli/_errors.py` (`ErrorCode`)``. Pointers stay accurate when prose drifts.
- **Closed checklists vs. open-ended judgment.** Different skill genres have different needs:
  - **Action-add skills** (e.g., `cli-command-add`, `mcp-tool-add`, `skill-add`) get **closed checklists** — every step is mandatory and verifiable; missing a step is a bug.
  - **Review skills** (e.g., `review-python-file`, `skill-review`, `review-changes`) get **enumerated triggers + open-ended judgment empowerment** — list common categories illustratively, then explicitly invite the reviewer to flag *anything else that looks off*. No closed checklist can enumerate every code smell, every security issue, every design problem; pretending otherwise produces false closure (see *Anti-patterns checklist*).
  - **Scope-completeness test for any enumerated list**: ask *"is this list comprehensive for the topic, or does it imply false closure?"* If the topic is genuinely closed (the `OutputMode` literals, the `ErrorCode` members), enumerate exhaustively — from the definition, not from a count written here, which drifts. If the topic is open-ended (code smells, security risks, design problems), pair the illustrative list with explicit open-ended language: *"or anything else that looks off — trust your judgment."* Canonical implementation: `review-python-file` step 4 (Code smells).
- **Match specificity to fragility** rather than writing every rule at one level — `ref-skill-authoring-standards` *Degrees of freedom* owns the test.

## Outcome orientation

- **Workflows state the verifiable end state**: "file exists at path X", "`uv run pytest` returns 0", "`./bin/precommit.sh` returns 0", "row count in README increased by N".
- **Reference rules state the verifiable property**: "no `# type: ignore` without bracketed error code", "every error code has a unique `help_text`", "every field on a `StrictSchema` subclass carries a PEP 257 trailing docstring".
- **Anti-pattern: activity-as-outcome.** "Improve the code", "review docs", "make better" — the reviewer cannot tell when to stop. Replace with the verifiable property the activity is supposed to produce.

## Examples: precise rules first, examples where they carry what prose cannot

State the rule precisely and assume the agent is competent. An example is not a quota to fill — upstream's *"concise is key"* and the blog's observation that *"giving examples actually constrains them to a certain exploration space"* are the same thesis. Add one only when it carries what a precise statement cannot: a format to match, a style to imitate, or a wrong-versus-right distinction that is not obvious from the rule.

- **Prefer a pointer to real code over a synthetic example.** Upstream: *"prefer files that are in code as it provides clear, high-fidelity instructions to Claude in a language it knows very well."* Three real `Canonical implementation:` pointers beat ten paragraphs of prose, and they cannot drift into fiction the way an invented snippet can.
- **Fenced code blocks for canonical forms.** Do not paraphrase syntax. The canonical `match value: case _ as unexpected: typing.assert_never(unexpected)` form belongs in a fenced block, not a sentence.
- **When an example earns its place, make it concrete.** Upstream's bar is *"Examples are concrete, not abstract"* — a vague illustration is worse than none, because it consumes tokens and teaches nothing.
- **Empirical grounding**: `ref-python-coding-practices` rules 17, 18, and 19 became scannable only after canonical-form fenced blocks landed and the prose paraphrase came out.

## Clarity and writing standards

- **Voice**:
  - Workflows use **imperative**: "Add the click decorator", "Run `uv run pytest`", "Update the README row".
  - Reference rules use **declarative-but-prescriptive**: "f-strings are preferred over `%` and `.format()`", "every named exception inherits from `McpError`".
- **No hedge words.** Forbidden list: `might`, `could`, `generally`, `typically`, `usually`, `sometimes`, `perhaps`, `consider`, `try to`, `often`, `mostly`. Replace with imperatives or delete the rule. Hedge words leave room for the agent to skip the rule.
- **No meta-commentary.** No "this skill aims to …", no "in this section we will …", no "as noted above", no "in summary".
- **No decorative language.** No `obviously`, `simply`, `just`, `clearly`, `elegant`, `clean`. They add no information and signal that the author is justifying rather than instructing.
- **One claim per sentence.** A sentence carrying two independent claims splits into two. The test is whether a reader can act on each clause separately, not the word count.
- **No throat-clearing intros**: a skill opens with substantive content, not with framing. The first sentence does work.
- **Keep the body to the always-needed core.** A loaded body stays in context for the whole session, so every line is a recurring per-turn cost. Move heavy reference an invocation rarely needs to a linked file (`ref-skill-authoring-standards` *Progressive disclosure and multi-file skills*).

## Anti-patterns checklist

Walk this list against every reviewed skill. Each bullet names a real failure mode observed in real catalogs.

- **Triggerless description.** Names the topic but not the moment. Test: read it alongside ten siblings; if you can't predict which task fires it, fix it.
- **Mushy outcome.** Workflow ends in "improve …" / "make better" / "review …" with no verifiable end state.
- **Hedge word.** Any occurrence, in any quantity — each one is a license for the agent to skip the rule. There is no acceptable density.
- **Paraphrase of another skill.** Narrating a topic another skill owns instead of pointing at it. Restate a rule in one line where the reader must act on it there; otherwise use an `Apply <skill>` pointer (`ref-skill-authoring-standards` *Composition is the architecture*).
- **Throat-clearing intro.** First 1–2 paragraphs say nothing actionable. Delete them; the first paragraph does work.
- **Single-use sub-skill.** A `ref-`-prefixed skill referenced by exactly one parent. Inline it.
- **Folklore rule.** Prescriptive rule with no canonical-implementation pointer and no named failure it prevents. Either ground it or delete it.
- **Decorative prose.** `obviously`, `simply`, `just`, `clean`, `elegant` applied to design choices.
- **Adjective-form rule.** "X is preferred" when "Prefer X" or "Use X" is shorter and actionable.
- **Wrong voice.** Workflow written in declarative voice; reference written in imperative voice with imagined caller.
- **False-closure list.** An enumerated list of examples on an open-ended topic (code smells, security risks, design problems, weird code) with no "or anything else" open-end. The agent will misread the list as exhaustive and miss the long tail. Fix: add explicit open-ended language (*"or anything else that looks off — trust your judgment"*) and an *"illustrative, not exhaustive"* disclaimer. See *Actionability*, closed checklists versus open-ended judgment.
- **Guardrail against bad judgment.** A rule whose only job is to stop a competent agent from doing something obviously wrong. Upstream removed most of these when models got better: *"Avoid making them overconstrained, except in highly important areas."* Keep a rule only if it encodes a project-specific opinion or a test enforces it.
- **Time-sensitive content.** "Currently", "as of now", "the new X" — these rot silently. Write timelessly; if history must be recorded, confine it to a dated provenance line or a collapsed *Old patterns* block.

## Field failure signals

These are the symptoms that say a skill needs a content fix, not a structural fix.

- **Agent never invokes it for tasks where it should fire.** → Triggerability failure. Fix the description.
- **Agent invokes it for tasks where it should not fire.** → Over-broad description. Narrow it; add an explicit "not for X" clause.
- **Agent invokes it but produces output the skill didn't ask for.** → Actionability failure. Tighten the steps; replace adjective-form rules with verb-form.
- **Another skill paraphrases this skill.** → Composition failure. Extract a shared sub-skill or merge; apply `ref-skill-authoring-standards` *Composition is the architecture*.
- **A canonical-implementation pointer goes stale.** → Empirical-grounding failure. Refresh the pointer in the same PR; if the exemplar no longer exists, the rule the pointer grounded is also stale.
- **A reviewer reads the skill end-to-end before acting.** → Length / density failure. The skill is too long, or the lead does not summarize the actionable core. Extract or rewrite the lead.

## Effectiveness in non-skill contexts

These rules apply equally to:

- `AGENTS.md` bullets (always-on, every-session content; effectiveness is *more* important here because every reader is forced to read it).
- MCP tool docstrings (consumed by AI agents through `tools/list`).
- Error-code `help_text` (consumed by agents through structured-error output and `dhcli agents errors`).
- README catalog rows (the second-tier description; agents and humans both read it).

When applying effectiveness rules outside skills, the same checks apply: name the moment, state what and when, prefer a pointer to real code over an invented illustration, avoid hedges, ground every rule in a real exemplar or a named failure.
