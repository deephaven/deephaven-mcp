---
name: dh-cli-architecture
description:
  "How code in the dh CLI is organized: commands as questions then actions,
  defineCommand, where code goes, naming, no nested ternaries. Use when adding,
  changing, reviewing or refactoring a dh command, flag, prompt (questions.ts),
  or action (src/auth), or when deciding where new code under src/ belongs."
---

# dh CLI architecture

[docs/ARCHITECTURE.md](../../../docs/ARCHITECTURE.md) is the source of truth.
Read it before writing code; this skill only says when to apply it and in what
order.

## When to use

- Adding a command, flag or prompt, or changing what one does
- Moving code between `src/commands/` and `src/auth/` (or a new area beside it)
- Splitting a long function, or reviewing a diff under `src/`

## Procedure

1. Read [docs/ARCHITECTURE.md](../../../docs/ARCHITECTURE.md).
2. Start from the closest existing command:
   [status.ts](../../../src/commands/auth/status.ts) is the smallest,
   [login.ts](../../../src/commands/auth/login.ts) the fullest.
3. Write `run` as questions and actions. Prompts go in the group's
   `questions.ts`; network and config work goes in actions under `src/auth/`.
4. Register a new command in its parent's `commands` list.
5. Go through the guide's review checklist, then run `bun run check` and
   `bun test`.
6. If `dh auth` behaves differently, update
   [docs/AUTH.md](../../../docs/AUTH.md), and `impl-docs/auth-impl.md` if that
   file exists locally.
