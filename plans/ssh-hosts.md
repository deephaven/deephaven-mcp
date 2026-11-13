# Several SSH hosts per plugin

Let a plugin run on any of several nodes of a cluster (`crcat` on whichever
DIS node has the file), with a default host that `dh plugin use` changes and
a per-run `--ssh-host` override, following `dh auth`'s conventions (a list,
a default, `dh auth use`, `--profile`). Renames the SSH host flag and
setting from `host` to `ssh-host` everywhere. Design:
[the design doc](https://claude.ai/artifact/QbDVebtCpHu33kaHU8nAEV)
("New CLI surface", the `config.json` snippet, Walkthrough 1). Builds on
[ssh-plugins.md](ssh-plugins.md) (built); read its "Contract" first. This
plan comes before [versions.md](versions.md), which extends `dh plugin use`.

Status: planned.

## Contract

### Settings (`config.json`)

`servers.<origin>.plugins.<name>` (`ServerPluginSettings` in
`src/auth/store.ts`):

```json
{ "sshHosts": ["dis-1", "dis-2"], "defaultSshHost": "dis-2", "prefix": ["sudo", "-n", "-u", "irisadmin"] }
```

- `sshHosts`: non-empty list of SSH destinations, each passed to `ssh` as
  given. Unset: the server URL's hostname (`hostOf` in
  `src/plugins/settings.ts`), as one host.
- `defaultSshHost`: one of `sshHosts`. Unset, or not in the list: the first.
- `host` (written by the current build) is read as `sshHosts: [host]` when
  `sshHosts` is unset, and removed on the next write to that entry. Nothing
  writes `host` any more.

The **hosts** of a plugin on a server are `sshHosts`, else `[<hostname>]`;
its **default host** is `defaultSshHost` if listed, else the first.

### `--ssh-host` on plugin commands

- `--ssh-host <h>` or `--ssh-host=<h>`, anywhere before a `--`, is `dh`'s on
  an `ssh` plugin's commands, like `--profile` (`routePluginArgs` in
  `src/cli/plugin-args.ts` keeps it; `commandAt` in `src/cli/command.ts`
  skips its value). On a `local` plugin it goes to the tool, and before the
  noun it is the `usage` error `local` plugins already give for
  `--profile`.
- It's a hidden global option (in `GLOBAL_OPTIONS`, `.hideHelp()`), shown
  only in an `ssh` plugin's help. Global Options there, in order: `-o`,
  `--profile`, `--ssh-host`, `--no-prompt`. Help text:
  `Which of the plugin's SSH hosts to run on (default: its default host).`
- On any other command (built-ins, `local` plugins' nouns before the verb):
  `usage`, `--ssh-host only applies to commands of plugins that run over SSH`.
- The run uses `--ssh-host` if given, else the default host. A value that
  isn't one of the hosts: `usage`,
  `` <h> isn't one of <name>'s SSH hosts on <origin>: <h1>, <h2> ``, hint
  `` Add it: `dh plugin set <name> --ssh-host <h1> --ssh-host <h2> --ssh-host <h>`. ``
  Checked before anything runs; nothing is logged by the fake ssh.
- `dh agents -o json`: `pluginFlags.ssh` becomes
  `["-o, --output <format>", "-h, --help", "--profile <name>", "--ssh-host <host>", "--no-prompt"]`.
  The human summary's SSH sentence:
  `` Over SSH (`dh ctl`), they also take `--profile`, `--ssh-host` and `--no-prompt`. ``

### `dh plugin set <name> --ssh-host <host>…`

- `--host` is renamed `--ssh-host` (no alias). Repeatable (commander
  `argParser` collecting into a list): the hosts, in order, replacing the
  stored list. Each is checked as `askHost` checks `--host` today
  (non-empty, no whitespace). Duplicates are dropped, keeping the first.
- `--ssh-host ""` given alone removes `sshHosts` (back to the hostname);
  `""` together with other values is `usage`, `` --ssh-host "" can't be combined with other hosts ``.
- A `defaultSshHost` no longer in the new list is removed.
- "No flags" hint becomes `Pass --exec, --path, --ssh-host or --prefix.`
- Output, human: the `host:` line becomes
  `  ssh hosts: dis-2 (default), dis-1 (https://dhe.example.com:8123)`: the
  default first, `(default)` only when there are two or more; `(no profile)`
  as today without one. JSON: `host` is replaced by `"sshHosts"` (the hosts,
  default first; `null` for `local` or without a profile) and
  `"defaultSshHost"` (string or `null`).

### `dh plugin use <name> [--ssh-host <host>]` (new)

- `<name>`: a loaded plugin, else `usage` as in `set`. A `local` plugin:
  `usage`, `` <name> runs on this machine, so it has no SSH hosts ``, hint
  `` Run it over SSH first: `dh plugin set <name> --exec ssh`. ``
- Needs a profile (as `set --ssh-host`).
- With `--ssh-host <h>`: `h` must be one of the hosts (same `usage` error as
  for a run). Writes `defaultSshHost` with `Store.update`.
- Without it: in a terminal, a picker (`ui.select`, flag `--ssh-host`) of
  the hosts, the current default marked with hint `(default)`. Without a
  terminal or with `--no-prompt`, the prompter's usual "pass --ssh-host"
  `usage` error. With a single host, no picker: it's already the default.
- Output: human `` ✔ <name> runs on <h> by default on <origin> ``; JSON
  `{ "plugin": "crcat", "server": "<origin>", "sshHosts": [ … ], "defaultSshHost": "<h>" }`.

### `dh plugin create … --ssh-host <host>`

`--host` is renamed `--ssh-host`, repeatable as in `set`; help runs use the
first; all are saved as `sshHosts` after the spec is written. Without
`--exec ssh`: `usage`, `--ssh-host needs --exec ssh`. JSON: `host` becomes
`"sshHost"` (the one the help ran on, `null` for local).

### `dh plugin list`

The `ssh:` line shows the hosts, default first, `*` after the default when
there are two or more: `  crcat   ssh: dis-2*, dis-1 /usr/illumon/latest/bin/crcat`.
JSON: `host` is replaced by `"sshHosts"` and `"defaultSshHost"` (both `null`
without a profile, or for `local`).

### `dh plugin check <name> [--ssh-host <host>]`

- For an `ssh` plugin, the `host`, `connect`, `tool` and `prefix` steps run
  for each host in turn (default first), then `version` once on the default
  host. With `--ssh-host`, only that host (validated as for a run).
- Human: the host name follows the step when there are two or more hosts:
  `✔ connect  dis-1  in 176 ms`. JSON: `"host"` (top level) becomes
  `"sshHosts"`, and each check entry has `"sshHost"`. On failure, `checks`
  is the steps so far, as today.

### Errors and hints

- The `-o json` field `"host"` on SSH errors becomes `"sshHost"`.
- Hints naming `--host` name `--ssh-host`
  (`` Check the host, or set it: `dh plugin set <plugin> --ssh-host <host>`. `` and the `check` tool hint).

## Checklist

- [x] `sshHosts`, `defaultSshHost`, reading the old `host` (`src/auth/store.ts`, `src/plugins/settings.ts`)
- [x] `--ssh-host` global option, routing, help, the "only applies" error, `dh agents`
- [x] Run-time host choice and the not-a-host error
- [x] `dh plugin set --ssh-host` (repeatable, `""`), output
- [x] `dh plugin use` (new), picker
- [x] `dh plugin create --ssh-host`
- [x] `dh plugin list` and `dh plugin check` changes
- [x] `host` → `sshHost` in error fields, hints
- [x] Tests for every automated Validation item; existing tests updated for the rename
- [x] `plans/plugins.md` (Machine settings table, Commands table), `plans/plugins-poc-test.md` section 5 use `--ssh-host`

## Validation

Automated, with the fake `ssh` (`tests/fixtures/plugins/fake-ssh`):

- [x] `bun run check` and `bun test` pass
- [x] `dh plugin set crcat --exec ssh --ssh-host dis-1 --ssh-host dis-2 --ssh-host dis-1` writes `sshHosts: ["dis-1", "dis-2"]`; `--ssh-host ""` removes it; `--ssh-host "" --ssh-host x` → `usage`; `--host x` → `usage` (unknown option)
- [x] With no `sshHosts`, runs go to the URL hostname; with `["dis-1","dis-2"]` to `dis-1`; after `dh plugin use crcat --ssh-host dis-2`, to `dis-2` (`defaultSshHost` written)
- [x] `dh crcat x --ssh-host dis-1` runs on `dis-1` and the tool doesn't see `--ssh-host`; `--ssh-host=dis-1` too; `--ssh-host nope` → `usage` listing the hosts, empty fake ssh log
- [x] A `local` plugin passes `--ssh-host x` through to the tool; `dh --ssh-host x ctl …` on a local plugin → `usage`; `dh auth status --ssh-host x` → `usage`
- [x] A config with the old `host: "infra-1"` runs on `infra-1`; after `dh plugin set ctl --prefix "x"`, the entry has `sshHosts: ["infra-1"]` and no `host`
- [x] `dh plugin use crcat` without a terminal → `usage` naming `--ssh-host`; on a local plugin → `usage`; `--ssh-host nope` → `usage`
- [x] `dh plugin list` shows `dis-2*, dis-1`; JSON `sshHosts`, `defaultSshHost`
- [x] `dh plugin check crcat -o json` has `host`/`connect`/`tool`/`prefix` for each host, then `version`, each with `sshHost`; `--ssh-host dis-2` checks only `dis-2`
- [x] SSH errors in `-o json` carry `"sshHost"`; help for an `ssh` plugin lists `--ssh-host`, a local plugin's doesn't; `dh agents -o json` `pluginFlags.ssh` includes `--ssh-host <host>`

Against a real DHE server:

- [ ] Two hosts for one plugin (two nodes, or the same node by two `~/.ssh/config` aliases): `use`, `--ssh-host`, `check` behave as above

## Relevant Files

- `src/auth/store.ts` — `ServerPluginSettings`: `sshHosts`, `defaultSshHost`, `host` read-only
- `src/plugins/settings.ts` — hosts and default host; old `host`
- `src/plugins/ssh.ts` — `sshHost` error field; hint text
- `src/cli/command.ts` — `--ssh-host` global option; `commandAt` skips its value; `PLUGIN_GLOBAL_FLAGS.ssh`
- `src/cli/plugin-args.ts` — keep `--ssh-host` for `ssh` plugins; before-noun error for `local`
- `src/main.ts` — help Global Options order; the "only applies" check
- `src/commands/agents.ts` — summary sentence
- `src/commands/plugin/tool.ts` — host for the run; not-a-host error
- `src/commands/plugin/set.ts` — repeatable `--ssh-host`, output
- `src/commands/plugin/use.ts` (new)
- `src/commands/plugin/index.ts` — register `use`; `list` changes
- `src/commands/plugin/create.ts` — `--ssh-host`
- `src/commands/plugin/check.ts` — per-host steps, `--ssh-host`
- `src/commands/plugin/questions.ts` — host list validation, picker
- `tests/plugin_ssh.test.ts`, `tests/plugins.test.ts`
- `plans/plugins.md`, `plans/plugins-poc-test.md`
