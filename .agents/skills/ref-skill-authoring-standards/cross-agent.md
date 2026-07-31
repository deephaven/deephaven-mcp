# Cross-agent portability detail

Per-agent discovery, invocation, and invocation-control behavior. Load this when portability is the task; the three binding wiring rules are in `SKILL.md` *Cross-agent portability*.

## Discovery and invocation

| | Cascade (Windsurf / Devin Desktop) | Claude Code | Codex | Cursor |
| --- | --- | --- | --- | --- |
| Reads `.agents/skills/` | Yes, natively | No — needs the symlink | Yes, natively | Yes, natively |
| Also scans `.claude/skills/` | Yes, when Claude Code config reading is enabled | Yes | No | Yes |
| Human invocation | `@mention` | `/slash` | `$mention` or `/skills` | `/slash` |
| Honors `user-invocable` | Not documented | Yes | Ignores unknown keys | Not documented |
| Invocation control | Not documented | `user-invocable`, `disable-model-invocation` | `agents/openai.yaml` → `allow_implicit_invocation` | `disable-model-invocation` |

Two consequences drive the naming rule in `SKILL.md`:

- **No portable field expresses "model-only, never human-invoked."** `user-invocable` is Claude-Code-only; the better-supported `disable-model-invocation` means the opposite thing (it blocks *model* invocation, not human invocation). The `ref-` name is therefore the only cross-agent signal.
- **Invocation syntax is per-agent.** Do not write "invoke with `/name`" in any skill or in `AGENTS.md` — that is true in Claude Code and Cursor, false in Cascade. Say "invoke directly" and let each agent's own UI supply the syntax.

## Slash commands are not skills in every agent

In Cascade, `/slash-command` addresses a separate **Workflows** primitive (`.windsurf/workflows/` or `.devin/workflows/`), not the skill catalog; skills are `@mention`ed. This repo ships no workflows directory. A statement that skills are "callable as slash commands" is therefore agent-specific, and stating it unconditionally is wrong.

## Duplicate-listing risk

Cascade and Cursor scan both `.agents/skills/` and `.claude/skills/`, and the symlink makes those the same tree. Observed behavior is that entries are not doubled — the resolved path dedupes. If a skill ever appears twice in an agent's catalog, that is the cause; the fix is to drop the symlink for that agent, not to duplicate the tree.

## Vendor documentation

- [Agent Skills specification](https://agentskills.io/specification)
- [Claude Code skills](https://code.claude.com/docs/en/skills)
- [Cascade skills](https://docs.devin.ai/desktop/cascade/skills)
