---
name: ref-cli-design-prior-art
description: Ground dhcli design decisions in the conventions of comparable CLIs — invoke when choosing a noun, verb, flag name, default, or interaction model, or when reviewing one. Requires citing the tool and the specific behavior being followed, and stating the reason when diverging
user-invocable: false
---

# CLI design prior art

`dhcli` is a resource-oriented CLI competing for muscle memory with `kubectl`, `docker`, `gh`, `aws`, `gcloud`, and `git`. A user who already knows those tools should be able to guess how this one behaves. Every design decision either follows an established convention or deliberately departs from one — and the difference must be visible in the artifact, not just in the author's head.

## The rule

When a design decision has an established convention in a comparable tool, **follow it, and cite the tool and the specific behavior.**

- **Cite the behavior, not the vibe.** "`kubectl delete` prompts only under `--interactive`" is a citation. "kubectl-style" is not — it conveys nothing a reader can check or apply.
- **A citation must be verifiable.** Name the flag, the subcommand, or the observable behavior, so a reader can confirm it against that tool's documentation or source. An uncheckable citation is worse than none: it borrows authority it has not earned.
- **When diverging, say so and give the reason.** A departure is a legitimate design choice; an undocumented departure reads as ignorance of the convention and invites someone to "fix" it later.
- **Do not invent a convention.** If no comparable tool does the thing, that is worth knowing — say the decision is ours and justify it on its own terms. Asserting an unattributed "this is conventional" is folklore (`ref-skill-effectiveness` *Anti-patterns checklist*).

## What is in scope

Noun and verb naming, flag naming and short forms, argument-versus-flag choices, default values, confirmation and interactivity behavior, output-format conventions, and exit-code semantics.

## Where the decisions live

This skill holds the rule. The *decisions* and their rationale live in [`docs/design/CLI_TOOL_WRAPPING.md`](../../../docs/design/CLI_TOOL_WRAPPING.md) — that is the document to read for what was already decided and why, and the document to extend when a new decision is made.

Canonical citations already in the codebase:

- **Resource-first noun-verb tree** (`docs/design/CLI_TOOL_WRAPPING.md`) — follows `kubectl`, `docker`, `gh`, `aws`, `gcloud`, all of which put the resource before the action. Type-first (`community …` / `enterprise …`) was considered and rejected; the rejection is recorded with its reason.
- **Code rides flags, not a noun** (`docs/design/CLI_TOOL_WRAPPING.md`) — follows `kubectl apply -f`, which takes a file without introducing a `file` noun.
- **Destructive confirmation is opt-in** (`ref-cli-tool-wrapping` *Destructive verbs with a defaultable target*) — follows `kubectl delete`, which prompts only when `--interactive` is passed; the default is no prompt.

## Verifying a citation

Check the tool's own documentation or source rather than recalling from memory — flag names and defaults change between releases, and a half-remembered flag is exactly the uncheckable citation this rule exists to prevent. Record what you verified against, so a later reader knows the claim was checked and not assumed.
