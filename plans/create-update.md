# Regenerating a spec without losing hand edits

`dh plugin create <tool> --update` reads the tool's help again (typically
after an upgrade) and writes a spec for the new version that keeps what
people edited in the old one: descriptions, options added or removed by
hand, commands written by hand. Everything still as the old help produced
it is replaced by what the new help produces. Design:
[plugins.md](plugins.md) ("Updating specs"). Builds on
[versions.md](versions.md) (specs side by side as `<name>@<version>.json`,
pins); comes after it, and after [ssh-hosts.md](ssh-hosts.md) for the
`--ssh-host` spelling.

Status: planned.

## Why a three-way merge

A spec is generated, then edited. Comparing old and new specs alone can't
tell an edit from a change in the tool: a description that differs might be
a person's rewrite (keep it) or the tool's new wording (take it). Each
generated command keeps the help it came from as `source.help`, so the
merge has a base: what the old help produces.

- **old**: the existing spec, as edited.
- **base**: what the generator produces today from old's `source.help`
  texts, without running the tool.
- **new**: what the generator produces from the tool's help now.

A field where old equals base wasn't edited, so new wins. A field where old
differs from base was edited, so old wins. Because base is computed with
today's generator, an improvement to the generator itself shows up as an
"edit" and is kept as it was: conservative, never loses a person's text.

## Contract

### Command

`dh plugin create <tool> --update [--name <noun>] [--exec local|ssh] [--ssh-host <host>]… [--recurse | --no-recurse]`

- Runs the help as `create` does today (locally, or over SSH with
  `--exec ssh`), then merges, then writes.
- **old** is the spec for `--name` (default: the name `create` would pick)
  that's in use for the target: the pinned spec on the profile's server for
  `--exec ssh`, the machine's pin for `local` ([versions.md](versions.md)),
  else the newest spec with that name. None loaded: `usage`,
  `` No <name> spec to update ``, hint `Run it without --update to create one.`
- `--force` isn't accepted with `--update` (`usage`,
  `--update and --force can't be combined`); `--update` never needs it.
- The written file is the new version's, as for `create`:
  `<name>@<new version>.json`. If that's old's own file (same version), it
  is replaced in place. Otherwise old's file is left untouched beside it.
- Pins as `create` does.
- `--recurse`/`--no-recurse` and the question are as for `create`. The
  question's default is yes when old's subcommands have their own
  `source.help` (old was read recursively), no otherwise.

### Merge rules

Commands are matched by their path of names (`intraday rescan`).

| Part | Rule |
| ---- | ---- |
| `name` | old's |
| `description` (each command, and the plugin's) | old's if it differs from base's, else new's |
| `options` (each leaf) | matched by `flags`. An option in old and base, equal in both: new's version, or dropped if new doesn't have it. In old but not base (added by hand), or different from base's (edited): old's. In base but not old (removed by hand): stays removed. In new only: added |
| `argv` | new's: it's how the tool is invoked |
| `source.help` | new's |
| `tool.path`, `tool.version` | new's |
| `tool.versionFrom` | old's if set, else new's |
| a command in new only | added as generated; reported |
| a command in old only, with `source.help` (generated, gone from the tool) | dropped; reported |
| a command in old only, without `source.help` (written by hand) | kept as it is; reported |
| a group in old that's a leaf in new, or the reverse | new's shape; old's `description` kept if edited; reported |

"Equal" for options compares `flags`, `description` and `required`.
Placeholder descriptions (the generated `<tool> <path>` text) count as
generated, so a tool that now describes a command replaces them.

### Output

Human, after `create`'s usual first line (`✔ Wrote <file> (from <n> help
runs[ over SSH: <host>])`):

```text
  Updated from ctl@2026.01.2g0.json: kept 5 edited descriptions and 1 option added by hand
  New in dhctl: dh ctl intraday compact
  Gone from dhctl: dh ctl logger generate
  Kept, written by hand: dh ctl metadata audit
  Pinned ctl to 2026.07.1g0 on https://dhe.example.com:8123
```

Each line only when it has something; then `create`'s usual
placeholder/no-options/truncated lines. JSON adds `"updatedFrom"` (old's
file), `"kept"` (the edited parts kept, e.g.
`["ctl intraday description", "ctl intraday rescan option --foo"]`),
`"added"`, `"removed"` and `"handWritten"` (commands as typed, e.g.
`"ctl intraday compact"`).

## Checklist

- [ ] Generator usable on stored help texts alone (base), with the same output as from live runs for the same texts
- [ ] Merge per the rules table
- [ ] `--update` on `dh plugin create`: choosing old, file, pin, flag conflicts, recurse default
- [ ] Output lines and JSON fields
- [ ] A second fake dhctl version for tests
- [ ] Tests for every automated Validation item
- [ ] `plans/plugins.md`: status, "Updating specs" matches what was built

## Validation

Automated. `tests/fixtures/plugins/fake-dhctl` gains a second version,
chosen by `FAKE_DHCTL_VERSION=2`: one command added (`intraday compact`),
one removed (`logger generate`), one leaf's help description reworded, and
one option added to `intraday rescan`.

- [ ] `bun run check` and `bun test` pass
- [ ] Base equals old for a spec just generated and never edited: `--update` against the same version changes nothing but reports nothing kept
- [ ] An edited description is kept; an unedited one takes the reworded text; a placeholder is replaced when the tool now has a description
- [ ] An option added by hand is kept; one removed by hand stays removed; one edited is kept; the tool's new option is added; an unedited one the tool dropped is dropped
- [ ] `intraday compact` is added and reported; `logger generate` is dropped and reported; a hand-written command (no `source.help`) is kept and reported
- [ ] `name`, edited plugin description, and `versionFrom` come from old; `argv`, `source.help`, `tool.version` from new
- [ ] For a new version, the file is `ctl@<new>.json` beside old's, which is unchanged; for the same version, old's file is replaced
- [ ] With profiles on two servers pinned to different specs, `--update` over SSH merges from the target server's pinned spec
- [ ] No spec for the name → `usage`; `--update --force` → `usage`
- [ ] JSON has `updatedFrom`, `kept`, `added`, `removed`, `handWritten`

Against a real DHE server:

- [ ] Hand-edit a few descriptions in a generated `ctl` spec, run `dh plugin create /usr/illumon/latest/bin/dhctl --exec ssh --update`, and check the edits survive and nothing else changed (same version)

## Relevant Files

- `src/plugins/scaffold.ts` — generate from stored help texts (base); expose the per-command results the merge needs
- `src/plugins/merge.ts` (new) — the merge and its report
- `src/commands/plugin/create.ts` — `--update`, choosing old, output
- `src/commands/plugin/questions.ts` — recurse default from old
- `tests/fixtures/plugins/fake-dhctl` — `FAKE_DHCTL_VERSION=2`
- `tests/plugin_update.test.ts` (new)
- `plans/plugins.md`
