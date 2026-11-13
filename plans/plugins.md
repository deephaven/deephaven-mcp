# `dh` plugins

Design for adding noun/verb command groups to `dh` that wrap existing CLIs, so
one `dh` binary fronts tools like `dhctl`, `dhconfig`, `iriscat` and `crcat`.
Most of these live only on Deephaven Enterprise servers, so a plugin must be
able to run its tool over SSH without the user handling it.

Status: design; the open questions are settled (see
[Decisions](#decisions)). A proof-of-concept slice of phase 1 is implemented
for the server-installed case:

- Spec files in `<config dir>/plugins/*.json` become `dh` commands, with every
  argument passed through and listed in `dh agents` as `[args...]`.
- Local execution, with `path` and `prefix` machine settings, edited by hand in
  `config.json` (`dh plugin set` isn't built).
- `tool_failed` / `tool_not_found` (exit 6, with `toolExit`), and
  `plugin_invalid` for specs that are invalid or take a built-in, reserved or
  duplicate name.
- `dh plugin` / `dh plugin list`.
- `dh plugin create <tool> [--name <noun>] [--force] [--[no-]recurse]`: a
  spec generated from the tool's help (see
  [Scaffolding](#scaffolding-from---help)).
- An example spec for `dhctl`: [examples/plugins/ctl.json](../examples/plugins/ctl.json).

The first SSH pass (phase 2, [ssh-plugins.md](ssh-plugins.md)) is
implemented too, and tested against a fake `ssh`; the checks against a real
DHE server ([plugins-poc-test.md](plugins-poc-test.md), section 5) are still
to do:

- `exec: "ssh"` runs the spec's `tool.path` on the active profile's server,
  with the server's `host` and `prefix` (`servers.<origin>.plugins.<name>`).
- `--profile` and `--no-prompt` are `dh`'s on `ssh` plugins, in help and
  `dh agents` (`pluginFlags.ssh`, `"pluginExec"`).
- `ssh_host_unknown`, `ssh_auth_failed` and `remote_unreachable`, with hints
  and the `host` in the `-o json` error.
- `dh plugin set`, `dh plugin check`, and `dh plugin create --exec ssh
  [--host]`, which also records `tool.version`.

Results over SSH from a laptop (plugins-poc-test.md section 5):

- Test 21, no usable key with `--no-prompt`: `ssh_auth_failed`, exit 4, with
  the `ssh-copy-id` hint and no password prompt. The server offered
  `publickey,gssapi-keyex,gssapi-with-mic`. The `Permission denied` line
  showed twice in human mode, passed through from `ssh` and again as the
  error's detail; now only `-o json` repeats it, in the error message.
- Tests 16–23 run through; all as expected except `crcat`.
- Timing, `dh ctl metadata list --table-name DbInternal`, one run each
  (wall clock), against `dhctl` timed on the server itself:

  | Run                          | Time   | Over `dhctl` alone |
  | ---------------------------- | ------ | ------------------ |
  | `dhctl` on the server        | 3.75 s | —                  |
  | `dh`, reusing the connection | 3.83 s | 0.09 s             |
  | plain `ssh <host> dhctl …`   | 4.50 s | 0.76 s             |
  | `dh`, new connection         | 5.29 s | 1.55 s             |

  With the connection reused, SSH and `dh` together add under 0.1 s, so the
  JVM is the whole cost. Without reuse every command pays about 0.8 s for
  the handshake and login, and `dh`'s first run about 1.5 s, as it also
  starts the master that later runs share for 60 s. `dhctl` used 9.4 s of
  CPU in its 3.75 s (a multi-threaded JVM start). `dh`'s own CPU time is
  about 0.1 s.
- `dh plugin check ctl`'s `connect` step: 992 ms, presumably on a new
  connection (between plain `ssh`'s 0.76 s and the first `dh` run's
  1.55 s, as it includes starting the master).
  Run again within 60 s, reusing the connection: 176 ms. That's opening a
  session on the existing connection plus the remote shell running `true`,
  a few round trips; single runs are noisy (the reused `dh ctl` run above
  came out 0.09 s over `dhctl`).
- Versions, from the laptop over SSH: `dh plugin check ctl` detected
  `2026.01.261102164822g2f6a8ebf11` and wrote the pin (`(pinned now)`);
  `dh plugin list` showed `(version not checked)` before. Timing
  `dh ctl metadata list --table-name DbInternal` the same day, reusing the
  connection: 3.78 s with a spec without `versionFrom` (no check), 3.83 s
  pinned without a terminal (the in-session check is free), 4.33 s pinned in
  a terminal (`-t` makes the check a separate SSH run first: about 0.5 s on
  every interactive command). After making the terminal check silent and
  in-session too (543c651): 3.86 s in a terminal.
- Pasting several lines after a `dh` SSH plugin command: the command reads
  the following lines as its stdin and sends them to the tool, as `ssh`
  does, so they never run. One command at a time, or `</dev/null`.
- `crcat` has no `--help`: it read `--help` as a checkpoint record file and
  printed `Failed to read checkpoint record from --help`, which also shows
  it starts over SSH (Java found). `create` made it one pass-through command,
  as it should, but said only "no options". It now warns when `<tool>
  --help` prints no usage line (`noUsage` in `-o json`), since the spec is
  then a guess to finish by hand. Its top-level `--help` run is the one
  `create` always makes, so a tool that acts on unknown arguments would act
  on `--help`; harmless for `crcat`.

Tried on real DHE servers, installed locally: first `dev-gplus`, then a fresh
server where every test in [plugins-poc-test.md](plugins-poc-test.md) passed.
Findings:

- `dhctl` help is Apache Commons CLI format (`usage: …`, then
  ` -x,--long <arg>  description` lines), and each subcommand has its own
  `--help`. That regular layout makes scaffolding (phase 3) feasible for the
  DHE Java tools.
- Commons CLI allows multi-letter short flags with one dash (`-pf`, `-user`).
  Declared `options` and conflict checks must treat them as one flag, not
  `-p -f`. Pass-through is unaffected.
- All flags follow the subcommand, matching the argv order `dh` produces. The
  only overlap with the flags `dh` takes on plugin commands is `-h/--help`.
- Credential flags are `-k/--key`, `-user/--user` and `-pf/--pwfile`, with
  `--key` and `--user` mutually exclusive, matching `tool.auth`.
- Some flags take several values (`--table-name A B.C`). Pass-through handles
  them; declared `options` don't say so yet.
- `metadata list -f <file>` writes the file wherever the tool runs; over SSH
  that's the server.
- An unrecognized option makes `dhctl` print `Action failed: Unrecognized
  option: …` and its usage, and exit 1, so `dh` reports `tool_failed` (exit 6).
  With the flag routing in [Global flags](#global-flags), a `--profile` after
  the command reaches `dhctl` this way instead of being swallowed.

Plugin versions ([versions.md](versions.md)) are implemented too, and tested
against the fake `ssh`; the timing and upgrade checks against a real DHE
server ([plugins-poc-test.md](plugins-poc-test.md), section 6) are still to
do:

- Specs for several versions of a tool side by side (`ctl@<version>.json`),
  each with `tool.version`, and `tool.versionFrom` saying how to find the
  tool's version (its real path, or a run such as `--version`).
- A pin per server (`ssh`) or machine (`local`) says which spec to use and
  what it was confirmed against; it's written only by detection: on first
  use, by `dh plugin create`, by a run and by `dh plugin check`.
- Every run checks the tool still matches the pin: locally at startup; over
  SSH in the same session; the `run` form once a day. A new version with a
  spec switches to it (with a notice); one without is `plugin_outdated`.
- `dh plugin list` shows the pin, the state and the other versions;
  `dh plugin check` re-pins; `dh agents` marks `pluginVersion` and
  `pluginState`.

Not built yet: `add`/`remove`, `check` comparing `helpDigest`, guided SSH setup, closing SSH masters on
logout, `json`/`csv` output, `rename` for options, `profileKey`, bundled
specs, and the 64 MiB cap on captured output.

## Goals

- `dh ctl intraday truncate …` runs `dhctl intraday truncate …` with the same
  exit codes, `-o json` and `dh agents` listing as built-in commands.
- **Primary: `dh` installed on a Deephaven server** wraps the tools installed
  there, with no SSH and no profile. An admin, or an agent with a shell on the
  server, gets one CLI for every tool.
- **Explored as an option: `dh` on a user's machine** runs the same plugins on
  the profile's server over SSH. The same spec file works in both places.
- **A wrapped tool works the way users run it today.** Anything a user can do
  running the tool directly, they can do through `dh`: every flag including
  credentials (`--key`, `--user`, `--pwfile`), stdin, prompts on a terminal,
  and however their site runs it as another user (`sudo`, `dzdo`, `pbrun`).
  `dh` adds only what this machine's settings say, and never at the expense of
  that flexibility.
- `dh` owns the agent-facing surface: help, flags, output shape and error
  codes. A wrapped tool never defines that surface at runtime.
- A plugin can be scaffolded from a tool's `--help`, then edited by hand (or by
  an agent).
- Conflicts are found when a plugin is created or added, not when a user runs a
  command.

Not goals: plugins running their own code inside `dh`, a plugin marketplace,
or fanning out to several hosts at once.

## Prior art

No existing framework fits. The useful patterns are spread across several tools.

| Tool                                          | What to borrow                                                                                                                         | What not to borrow                                                                                                                                                                                   |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Docker CLI plugins                            | Schema-versioned metadata; a plugin that duplicates a built-in is marked invalid; shadowed plugins are recorded                       | Executable plugins that parse their own argv                                                                                                                                                         |
| kubectl / gh                                  | Built-ins always win; `kubectl plugin list` reports shadowed plugins                                                                  | PATH discovery (`kubectl-foo`): `dh` would lose control of flags, help and output                                                                                                                    |
| Docker `ssh://` contexts (`connhelper`)       | Runs the system `ssh` with `-T`, `ConnectTimeout`, and a POSIX-quoted remote command                                                  | —                                                                                                                                                                                                    |
| carapace-spec `run`, Nushell `extern`         | A declarative file that maps a command to an external argv                                                                            | Completion-only scope                                                                                                                                                                                |
| [usage](https://usage.jdx.dev/) (jdx, active) | Reference for the shape of a CLI spec                                                                                                  | KDL: a new format and dependency for something JSON already covers                                                                                                                                   |
| oclif                                         | —                                                                                                                                      | Loads npm packages at runtime, which a single `bun build --compile` binary can't do cleanly. User plugins override core commands                                                                     |
| AWS CLI                                       | Commands generated from data, with hand-written commands for the important cases                                                      | Depends on every AWS service sharing one wire protocol, which wrapped CLIs don't                                                                                                                     |
| `--help` parsers (Fig, cli2mcp, carapace-parse) | —                                                                                                                                    | Unreliable. Fig's parser is abandoned, carapace recommends writing specs by hand, and the CLI-to-MCP generators name help-text quality as their main weakness. Use help parsing only to scaffold once |

Takeaway: plugins are **data files that `dh` turns into commands**, not code or
executables. That keeps the API surface in `dh`'s hands, which is what agent
ergonomics needs.

## Plugin spec

One JSON file per plugin in `<config dir>/plugins/<name>.json` (that is,
`~/.deephaven/cli/plugins/`). It's JSON for consistency with `config.json`, and
`dh` publishes a JSON Schema for it so editors and agents can validate edits.

```json
{
  "schema": 1,
  "name": "ctl",
  "description": "Manage intraday partitions and metadata indexes (dhctl).",
  "tool": {
    "path": "/usr/illumon/latest/bin/dhctl",
    "auth": { "keyFlag": "--key", "flags": ["--key", "-k", "--user", "--pwfile", "-pf"] }
  },
  "commands": [
    {
      "name": "intraday",
      "description": "Truncate, delete or rescan intraday partitions.",
      "commands": [
        {
          "name": "truncate",
          "description": "Remove a partition's data and block further ingestion.",
          "argv": ["intraday", "truncate"],
          "output": "text",
          "options": [
            { "flags": "-d, --dry-run", "description": "Print what would be done, without doing it." },
            { "flags": "-part, --partitions <arg>", "description": "Partitions to act on." }
          ],
          "source": { "help": "…`dhctl intraday truncate --help`, for reference…" }
        }
      ]
    }
  ],
  "source": {
    "createdFrom": "dhctl --help",
    "helpDigest": "sha256:…"
  }
}
```

| Field                     | Meaning                                                                                                                                                                                               |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`                    | The top-level noun: `dh ctl`. Must not conflict (see [Conflicts](#conflicts))                                                                                                                         |
| `promote`                 | Reserved; rejected in v1. Later: leaf commands to also register at the top level, each going through the same conflict check                                                                        |
| `tool.path`               | Absolute path of the executable. No `$PATH` lookup, especially over SSH, where a non-login shell's `PATH` won't include `/usr/illumon/latest/bin`                                                    |
| `tool.auth`               | Optional. `keyFlag`: the flag that takes a Deephaven private-key file, for [profile key reuse](#machine-settings). `flags`: every credential flag the tool has, so `dh` can tell when the user passed their own |
| `commands[]`              | The noun/verb tree. A group has `commands`; a leaf has `argv`                                                                                                                                         |
| leaf `argv`               | The tool's argv prefix. The user's remaining arguments are appended verbatim                                                                                                                         |
| leaf `options`            | The tool's flags (`flags` as the tool spells them, `description`, `required`). Shown in `dh`'s help and `dh agents` like any command's options. Every flag still passes through, declared or not. `rename` and validation are later additions |
| leaf `output`             | `text` (default), `json` (the tool prints JSON) or `csv` (header row; becomes an array of objects). See [Output and errors](#output-and-errors)                                                     |
| `source.help` (any command) | The tool's help the command was generated from. Reference for whoever edits the spec, e.g. to fill in a placeholder description or a missed option; never shown by `dh` |
| `source.helpDigest`       | Hash of the help text the spec was created from, so `dh plugin check` can detect when the tool has changed                                                                                           |

### Flags pass through by default

The scaffold only has to get the **command tree** right. Flags pass through
verbatim unless the spec declares them. That's what makes a scaffold from
`--help` usable even though help parsing is unreliable: a wrong guess about a
flag doesn't break anything, it just leaves the flag undocumented in
`dh agents`. `dh plugin create` declares the `options` it can read from the
help, and editing them is how a plugin gets better for agents: complete
help, and later renamed flags and validation before an SSH round trip.

## Machine settings

The spec is portable: it says what the tool is and how its commands map, and
nothing about the machine or the cluster, so it can be shared, bundled or
committed. Settings live in `config.json`, in two places:

- **This machine**, under `plugins.<name>`: how it runs the plugin, and for
  local runs where the tool is.
- **Each server (cluster)**, under `servers.<origin>.plugins.<name>`: where
  and how the tool runs on that cluster. A command finds them through its
  profile: the active profile's server's settings apply, so every profile on
  a cluster shares them. All nodes of a DHE cluster run the same version, so
  the version is a cluster setting too.

```json
{
  "plugins": {
    "ctl": { "exec": "ssh" },
    "iriscat": { "exec": "local", "path": "/usr/illumon/2026.1/bin/iriscat" }
  },
  "servers": {
    "https://dhe.example.com:8123": {
      "kind": "enterprise",
      "plugins": {
        "ctl": {
          "sshHosts": ["infra-1"],
          "prefix": ["sudo", "-n", "-u", "irisadmin"],
          "version": "2026.01.261102164822g2f6a8ebf11"
        },
        "crcat": { "sshHosts": ["dis-1", "dis-2"], "defaultSshHost": "dis-2" }
      }
    }
  }
}
```

| Field | Where | Meaning |
| ----- | ----- | ------- |
| `exec` | machine | `local` (run on this machine) or `ssh` (run on the profile's server). A plugin with no entry is `local`, so bundled specs work on a server with zero setup |
| `path` | machine | Optional. Replaces the spec's `tool.path` for local runs, e.g. a versioned install directory instead of `latest` |
| `prefix` | machine, server | Optional argv put before the tool, e.g. `["sudo", "-n", "-u", "irisadmin"]`, `["dzdo", "-u", "irisadmin"]` or `["env", "JAVA_HOME=/opt/jdk"]`. How a site runs tools as another user varies, so it's a setting, not part of the spec. For `ssh`, the server's `prefix` applies; for `local`, the machine's. Unset → the tool runs as the user `dh` runs as (or the SSH login) |
| `sshHosts` | server | For `ssh`: the SSH destinations on that cluster, each passed to `ssh` as given (an alias from `~/.ssh/config` works). A list per plugin per server, because tools may run on different nodes (`crcat` on whichever DIS node has the file). Unset → the server URL's hostname. An older single `host` is read as a one-host list and rewritten on the next change |
| `defaultSshHost` | server | For `ssh`: which of `sshHosts` runs get without `--ssh-host` (`dh plugin use <name> --ssh-host <host>`). Unset, or not listed → the first |
| `version`, `detected`, `realPath`, `checkedAt` | machine (`local`), server (`ssh`) | The pin: which spec to use (`null`: the unversioned one), and the tool version, real path or check time it was confirmed against. Written only by detection: on first use (also when the pinned spec is gone), by `dh plugin create`, a run and `dh plugin check`. See [Versions](#versions) |
| `profileKey` | machine | Optional, off by default, local `exec` only. Pass the profile's Deephaven key to the tool through `tool.auth.keyFlag`, so the tool acts as the signed-in user. Skipped when the user passed any of `tool.auth.flags` themselves |

`dh` follows these settings exactly and never guesses at run time.

Several hosts for one plugin and server ([ssh-hosts.md](ssh-hosts.md)):
runs go to the default host, or to the one `--ssh-host <host>` names; it's
`dh`'s flag on an `ssh` plugin's commands, like `--profile`.

### Versions

A tool's commands and options can differ between DHE releases, so a plugin
can have a spec per tool version, side by side: `plugins/ctl@2026.01.1g0.json`,
`plugins/ctl@2025.02….json`. A spec's identity is its `name` and
`tool.version` (unversioned: for any version); two specs with the same
identity are both rejected. The contract is in [versions.md](versions.md).

- **Detecting a version.** DHE has no network endpoint for it (checked:
  `/iris/connection.json` lists only service hosts, ports and authorities)
  and the tools have no `--version`, but `/usr/illumon/latest` is a symlink to the
  versioned install: `readlink -f /usr/illumon/latest/bin/dhctl` →
  `/usr/illumon/illumon-db-2026.01.261102164822g2f6a8ebf11/bin/dhctl`. The
  version is what follows `illumon-db-`. A spec says how to find it in
  `tool.versionFrom`: `{"path": "/illumon-db-([^/]+)/"}` (what `dh plugin
  create` writes for DHE tools), or `{"run": ["--version"], "match": "…"}`
  for a tool that can say. Without `versionFrom`, a plugin has one spec and
  no pin, as before.
- **The pin** (`version`, `detected`, and `realPath` or `checkedAt`) is a
  server setting for `ssh` plugins and a machine setting for `local` ones.
  It names the spec to use, chosen at startup so help and `dh agents`
  describe what that cluster runs. First use pins the spec for the version
  detected (else the unversioned one); `dh plugin create` pins too, and a pin
  whose spec is gone counts as none. `use` never pins a spec to a version
  it isn't for: a server's version is matched by a spec for that version
  or it's an error, fixed by getting the right spec (update `dh` for the
  tools it ships; `dh plugin create` for others). `dh` never picks a spec
  by guessing: no release line or "newest" fallback.
- **Noticing a change**, on every run: locally, `realpath` at startup; over
  SSH, a `readlink -f` comparison in the same session as the command (no
  extra round trip), or a separate run with a terminal; the `run` form once
  a day. A change to a version with a spec re-pins with a notice (over SSH,
  `dh` runs itself again); to one without, `plugin_outdated` until a spec is
  generated.
- **Unavailable plugins** (several specs with nothing to choose, no spec for
  the version, or not detected yet over SSH)
  register a bare noun; using it gives the error. `dh plugin list` shows the
  state.

### Updating specs

- Bundled Deephaven specs update when `dh` updates itself.
- A generated spec goes stale when the cluster is upgraded; see Versions
  above. `dh plugin check` re-runs the help and compares `helpDigest` for
  the thorough answer (one run per command, so on demand only).
- `dh plugin create --update` regenerates without losing hand edits, as a
  three-way merge with each command's `source.help` as the base: a
  description or option that still equals what the old help would generate
  wasn't edited, so it's replaced; anything else is kept; commands that
  disappeared are reported.

## Commands

| Command                                                             | Does                                                                                                                                                                                           |
| ------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dh plugin create <tool> [--name <noun>] [--[no-]recurse] [--force] [--exec local\|ssh] [--ssh-host <ssh host>]… [--no-check]` | Checks each `--ssh-host` (host and login; `--no-check` skips it), then runs `<tool> --help` (here, or over SSH with `--exec ssh`), recurses into subcommands if allowed, and writes a spec (`<name>@<version>.json` when the tool's version is found, with `tool.versionFrom`, pinning it) and this machine's settings. Only a spec with the same name and version conflicts (`--force` replaces it). Prints the path to edit                                             |
| `dh plugin add <file> [--exec local\|ssh] [--ssh-host <ssh host>]…` | Validates a spec, checks conflicts, copies it into the plugins directory, and writes this machine's settings                                                                                   |
| `dh plugin set <name> [--exec local\|ssh] [--path <path>] [--ssh-host <ssh host>]… [--prefix <command>] [--[no-]profile-key] [--no-check]` | Changes a plugin's settings. For an `ssh` plugin, each `--ssh-host` is checked first (host, login, tool, as `dh plugin check`; `--no-check` skips it), and nothing is written if one fails; see [hosts-and-pins.md](hosts-and-pins.md). `--exec`, `--path` and `--profile-key` are this machine's; `--ssh-host` (repeatable, replacing the list; `""` clears it) is the profile's server's (`--profile` picks another); `--prefix` is the server's for `ssh` plugins and the machine's for `local` ones. `--prefix` is split into words like a shell would, without expanding anything; `--prefix ""` clears it |
| `dh plugin use <name> [--ssh-host <host>]` | Sets which of an `ssh` plugin's hosts runs it by default on the profile's server; without `--ssh-host`, a host picker. It doesn't pin a version ([hosts-and-pins.md](hosts-and-pins.md)). See [ssh-hosts.md](ssh-hosts.md) |
| `dh plugin list`                                                    | Lists plugins (one per name) and how each runs here: `exec`, path (`(not installed here)` for a `local` tool that doesn't exist, with a footer pointing at `--exec ssh`), the SSH hosts for the profile's server (default first, starred when there are several), the spec in use and whether it's pinned, or why none can be used, and the name's other versions; then spec files that didn't load, with the reason |
| `dh plugin check <name> [--ssh-host <host>]`                        | Tests each step a run needs, stopping at the first that fails: for `ssh`, on each host in turn (or only `--ssh-host`), the host (`ssh -G`), connecting, the tool and the prefix, then the version once; locally, the tool, the prefix and the version. The `version` step re-pins when the tool changed and a spec matches. Later: re-runs `--help` to compare `helpDigest` |
| `dh plugin remove <name>`                                           | Deletes the spec                                                                                                                                                                               |
| `dh auth login [server] [--plugins ssh\|local]`                     | On an Enterprise server, with plugins that have no `exec` setting, whose tool isn't on this machine, and that weren't declined before: asks once, with the other login questions, `Run these plugins' tools (<names>) on this server over SSH?`. Yes (`--plugins ssh`) sets their `exec` to `ssh` with the sign-in; no (`--plugins local`) records them in `sshOfferDeclined`. Not asked without a terminal unless `--plugins` is given. See [laptop-setup.md](laptop-setup.md) |

`<tool>` is an absolute path, e.g. `/usr/illumon/latest/bin/dhctl`. The noun defaults to the
tool name with a leading `dh` removed (`dhctl` → `ctl`, `dhconfig` → `config`).

`exec` is `local` unless `--exec ssh` is given to `create` or `add` (or
later to `dh plugin set`); `dh` never infers it from whether the tool exists
here, since for an arbitrary tool a missing path is more likely a typo or an
uninstalled tool than a request to use SSH. `create --exec ssh [--ssh-host
<host>]` reads the help over SSH from the profile's server. Both print how
the plugin runs, e.g. `✔ Added ctl (runs locally:
/usr/illumon/latest/bin/dhctl)`.

### Scaffolding from `--help`

Built in the PoC (`src/plugins/help.ts`, `src/plugins/scaffold.ts`):

1. Run `<tool> --help`, reading stdout and stderr together and ignoring the
   exit code: `dhctl metadata --help` prints its usage as
   `Action failed: Usage: …` and exits 1. This is the only run before the
   user decides (step 3).
2. Find the subcommands, from the first of these that has any:
   - a `[a|b|c]` (dhctl) or `{a,b,c}` (argparse) list on the usage line,
     after the command's own path and any options like `[-h]`. The usage
     line is the one with `usage:` (after any prefix), or the line after a
     bare `Usage:` (Cobra);
   - a `Supported subcommands [a|b|c]` line (dhconfig's `acls users`);
   - a `Commands:`-style section (`Commands:`, `Available Commands:`,
     `SUBCOMMANDS`, …) of `name [args]  description` lines: Cobra, Click,
     clap, commander, picocli.

   `help` is skipped, but noted (step 4). Section entries, and argparse's
   `positional arguments:` entries, also give each subcommand's description.
   A one-item list right after the command (dhctl's `logger [generate]`)
   could be a placeholder, so it's tried (`x` is a subcommand only if its own
   help's usage line names it), and only when the help reads like a group's:
   no option lines of its own, and not a placeholder word (`arguments`,
   `flags`, `file`, …). Trying `[flags]` on a Cobra leaf would run the leaf.
3. Ask before going further, unless `--recurse` or `--no-recurse` was given
   (with `--no-prompt`, neither is a `usage` error). The question shows what
   `<tool> --help` listed. A careless tool might run a subcommand instead of
   printing its help, so whether that's safe is the user's call. Declining
   still gives a spec: the listed subcommands become commands, with their
   listed descriptions, and nothing else is run.
4. Recursing, each subcommand's help comes from `<tool> … help <sub>` where
   the command lists a `help` command, which shows the help without running
   the subcommand (Cobra, clap). If that doesn't print the first
   subcommand's usage, `<tool> … <sub> --help` is used for all of them
   (dhctl lists `help` but doesn't answer it this way). Four at a time,
   stopping at depth 4 or 200 runs. Only each group's own help is trusted:
   dhctl's top level lists every action of every group in its second
   bracket.
5. A description is the parent's listing for that subcommand if it has one,
   else the first sentence of prose after its own usage line, read across
   wrapped lines until it ends with `.` (at most 3 lines). dhctl puts that
   prose before the options (`metadata list`) or after them (`intraday
   truncate`); `intraday rescan` has none, and dhctl lists no group
   descriptions. Placeholders (`dhctl intraday rescan`) are used otherwise,
   and the output lists every command that has one.
6. A tool whose help lists no subcommands becomes one leaf:
   `dh iriscat <args>` → `iriscat <args>`.
7. `source.helpDigest` hashes every help text, sorted by command so parallel
   runs give the same digest, for `dh plugin check` to compare later.
8. The spec is validated and written to `<config dir>/plugins/<name>.json`;
   an existing file is replaced only with `--force`.

Beyond the DHE tools, tried in this container: a Python argparse tool (full
tree and descriptions, 5 runs), `dh` itself (commander: full tree, three
levels, all descriptions) and `bun` (with `--no-recurse`: its commands and
descriptions, from a three-column `name  example  description` list). `git`
stays a single command: its help groups commands under prose headings, not a
`Commands:` heading. Cobra, Click and clap are covered by tests written from
their default help layouts, not from real tools.

Each command's options are read from its help too: indented lines starting
with `-`, the spellings and value up to two or more spaces, then a
description that may wrap onto lines at its column (even ones starting with
`-`: dhctl wraps `-pf`'s onto a line starting `--user`). That covers Commons
CLI (`-t,--table-name <arg>`), argparse (`--port PORT`), Cobra
(`-a, --addr string`), Click (`--name TEXT`) and commander/clap. Values are
written `<value>`. A flag outside brackets on the usage line is `required`
(Commons CLI's `-t <arg>`). The usage block itself, which Commons CLI wraps
at the column after `usage: `, isn't read as options. `-h/--help` is left
out. Against the real help: every dhctl command (5–9 options each, `-t`
required exactly where the usage says so) and all 63 dhconfig commands (647
options).

Each command keeps the help it was read from as `source.help`, never shown,
and the output lists commands with placeholder descriptions or no options,
for whoever edits the spec.

Against the real `dhctl`, this finds the same tree as the hand-written spec,
plus a `logger` group (with `generate`) the hand-written one missed. Six of
seven leaves get real descriptions; `intraday rescan` and the three groups get
placeholders.

Against the real `dhconfig` (`--name cluster`), it finds 9 groups (`schemas`,
`properties`, `routing`, `dis`, `checkpoint`, `serviceregistry`, `pq`,
`acls`, `resolver`) and 63 commands, every one with a real description; only
the groups have placeholders. The first run missed the third level under
`acls` (`users`, `groups`, `publickeys`, `rows`, `columns`, `inputtables`),
which lists its subcommands on a `Supported subcommands […]` line. Reading
that line found all 23 (e.g. `acls users add`: "Create one or more users.").

## Conflicts

| Conflict                                             | Detected                     | Result                                                                                                                                                                            |
| ---------------------------------------------------- | ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Noun matches a built-in (`auth`, `agents`, `plugin`) | create, add, load            | Rejected. Built-ins always win (as in Docker, kubectl and gh, not oclif or Azure)                                                                                                 |
| Noun matches a reserved name                         | create, add, load            | Rejected. `dh` keeps a list of nouns it may add later (e.g. `query`, `pq`, `table`, `session`, `config`), so a release doesn't break someone's plugin                              |
| Noun and version match another spec                  | create, add, load            | Rejected (`create --force` replaces it). Specs sharing a noun with different `tool.version`s are versions of one plugin. If two files on disk have the same noun and version, neither loads and `dh plugin list` shows both |
| A tool flag matches a `dh` flag plugin commands take | create, check                | Warning. Plugin commands take only `-o json`/`-o human` and `-h/--help` (see [Global flags](#global-flags)), so only a tool's `-h`, or `-o` with the value `json` or `human`, needs `--`. A `rename` in `options` fixes it |

`dh` checks the reserved list against plugin names at load. When it rejects a
plugin there, it doesn't warn on every command, since stray stderr hurts
agents. `dh plugin list` and `dh agents -o json` (under `plugins`) report it.

`config` is reserved for `dh`'s own settings (see Decisions), so the bundled
`dhconfig` plugin is `dh cluster`. When `create`'s default noun is reserved or
taken, it asks for one, naming `--name`; with `--no-prompt` it fails with
`usage`.

### Help

`dh ctl intraday truncate --help` is rendered by `dh` from the spec, the same
way as every other command's help: the description, then the declared options
in the normal "Options:" section. It needs no SSH round trip. Nothing from the
tool's raw help is shown, and nothing names the tool: the CLI reads as one CLI.
`dh ctl intraday truncate -- --help` still asks the tool itself.

Commander can't spell Commons CLI's multi-letter short flags (`-pf`,
`-user`), so help shows their long forms (`--pwfile`, `--user`), which the
tool accepts too; the spec keeps the tool's spelling. A tool option spelled
like one of `dh`'s flags (`-o`, `-h`, …) isn't shown.

### Global flags

A plugin command takes only the global flags `dh` acts on for it, and every
other flag goes to the tool:

| Plugin runs | `dh` takes                                     | To the tool                                             |
| ----------- | ---------------------------------------------- | ------------------------------------------------------- |
| locally     | `-o json`/`-o human` (any spelling), `-h/--help` | everything else, including `--profile`, `--no-prompt`, `-V` |
| over SSH    | the same, plus `--profile` and `--no-prompt`    | everything else                                         |

- `-o` is `dh`'s only with a value `dh` accepts, so a tool's `-o out.csv`
  reaches it without `--`.
- `--profile` or `--no-prompt` typed *before* the plugin noun
  (`dh --profile p ctl …`) is clearly meant for `dh`, so on a local plugin it's
  a `usage` error rather than silently ignored. `DH_PROFILE` in the
  environment is ambient and stays unused.
- Commander can't express this: its `passThroughOptions` mode sends every flag
  after the command to the tool, `-o` included, and needs
  `enablePositionalOptions` on the root, which breaks `dh auth status -o json`.
  So `main.ts` rewrites the argument list before parsing: for a plugin command,
  everything but `dh`'s flags goes behind `--`, which commander hands to the
  tool, and a `--` the user typed is folded in.
- Help lists only the global flags that apply. `dh agents` doesn't repeat them
  per command: it states the rule once (`pluginFlags`) and marks each plugin's
  noun (`"plugin": "dhctl"`), which its subcommands inherit.
- Which flags `dh` takes depends on the plugin's `exec` in this machine's
  settings, so:
  - the argument rewrite reads `config.json` (alone, never failing: an
    unreadable one makes every plugin `local`) before parsing, and keeps
    `--profile` and `--no-prompt` (in any position) for `ssh` plugins;
  - the before-noun error applies only to `local` plugins;
  - plugin help shows `--profile` and `--no-prompt` for `ssh` plugins;
  - `pluginFlags` in `dh agents` gets an `ssh` entry, and each plugin's marker
    says which applies (`"pluginExec": "ssh"` beside `"plugin": "dhctl"`).

## Running a command

The spec says what to run; the [machine settings](#machine-settings) say
where. A missing tool fails with `tool_not_found`, naming the path and
suggesting `dh plugin set`.

### Local

`Bun.spawn([...prefix, path, ...argv, ...userArgs])`, with no shell. stdin,
stdout and stderr are inherited (stdout is captured only for `-o json`), so
piped input, terminal prompts and a `sudo` password prompt work as they do
when running the tool directly.

With `profileKey` on, `dh` writes the profile's key in Deephaven key-file
format (the format `src/auth/keyfile.ts` already parses) to a 0600 file in the
config directory, appends `<keyFlag> <file>`, and deletes the file when the
tool exits. A `keyFile` profile passes its own path instead. This needs
checking against the real tools first ([Phases](#phases)).

On servers, `install.sh` works as on any Linux machine. Admins who pin versions
can set `DH_AUTO_UPDATE=off`. Servers without internet access need an offline
install path (copy the binary), which the installer doesn't document yet.

### Over SSH

The SSH destination is the server's `host` setting for the plugin,
else the server URL's hostname. It needs an Enterprise profile. It is passed to the system `ssh` as given, so aliases, `ProxyJump`, keys, the
agent, Kerberos and Teleport (`ProxyCommand`) in `~/.ssh/config` all work.
`dh` doesn't hold SSH keys or passwords.

```text
ssh (-t | -T -o BatchMode=yes) -o ConnectTimeout=<DH_TIMEOUT>
    [-o ControlMaster=auto -o ControlPath=<config dir>/ssh/%C -o ControlPersist=60]
    -- <host> '<prefix>' '<tool.path>' '<argv>'… '<user args>'…
```

The remote command is one argument, built from the quoted words below. The
exact argv, the multiplexing rule and the exit classification are in
[ssh-plugins.md](ssh-plugins.md); `src/plugins/ssh.ts` implements them.

- Every argument is quoted as `'…'`, with `'` written as `'\''`, then joined
  with spaces. `ssh` joins its arguments into one string that the remote shell
  re-parses, so unquoted arguments would be split and expanded. Quoting is
  checked against quotes, `$HOME`, backticks, `;`, globs, empty strings,
  newlines and Unicode; see [Validation](#validation).
- `prefix` goes before the tool, quoted the same way.
- A TTY is allocated (`-t`) when stdin and stdout are terminals, without
  `-o json` and without `--no-prompt`. A person then gets the tool's prompts and
  a `sudo` password prompt as if they'd run `ssh -t` themselves. Otherwise
  `-T`, which keeps stdout and stderr separate and output clean for agents;
  prompts on the server then fail, the same as running the tool without a
  terminal. (`-t` merges stderr into stdout, which is why `-o json` never
  uses it.)
- `profileKey` is ignored over SSH: it would mean copying a private key to the
  server.
- `BatchMode=yes` makes a missing key fail at once instead of waiting on a
  password prompt an agent can't see. Without `--no-prompt` and with a
  terminal, `BatchMode` is left off so a person can type a password.
- `ControlMaster` reuses one connection for repeated calls, which matters for
  agents that run many commands. Windows OpenSSH doesn't support it, so it's
  omitted there.
- The remote stderr is passed through as it arrives, and its end is kept to
  classify a failure: exit 255 is an SSH failure, `ssh_host_unknown`,
  `ssh_auth_failed` or `remote_unreachable` by what `ssh` printed; 127 is
  `tool_not_found`; anything else is `tool_failed`, with a hint when a
  prefix's `sudo` wanted a password. A tool that exits 255 itself is reported
  as an SSH failure.
- `ConnectTimeout` bounds connecting only; a running tool has no time limit.
- The remote tool path is always the spec's `tool.path`; the machine's `path`
  is for local runs.

## Output and errors

| `output` | Human mode                  | `-o json` (stdout)                                                     |
| -------- | --------------------------- | ---------------------------------------------------------------------- |
| `text`   | Streams stdout and stderr   | `{"stdout": "<text>"}`. stderr still streams to stderr                 |
| `json`   | Streams                     | The tool's JSON, parsed and printed again (`internal` if it isn't JSON) |
| `csv`    | Streams                     | Array of objects keyed by the header row                               |

Captured output for `-o json` is capped at 64 MiB; past that the command fails
and suggests running without `-o json`.

New error codes (in `errors.ts`, so they're listed by `dh agents`):

| Code                 | Exit | When                                                                                                    |
| -------------------- | ---- | ------------------------------------------------------------------------------------------------------- |
| `plugin_invalid`     | 2    | The spec fails validation or conflicts                                                                  |
| `tool_not_found`     | 6    | `tool.path` doesn't exist (`ENOENT` locally, exit 127 remotely)                                         |
| `tool_failed`        | 6    | The tool exited non-zero. With `-o json` the error includes `"toolExit": <n>`                          |
| `plugin_outdated`    | 2    | No spec for the tool's version on that server (or machine). With `-o json` the error includes `"detected"`, `"pinned"` and, over SSH, `"host"` |
| `remote_unreachable` | 5    | `ssh` exited 255: no route, host key or SSH auth                                                         |

Wrapped tools' exit codes are mapped, not passed through, because their
numbers would collide with `dh`'s (a tool's exit 4 isn't `auth_*`). The
original code stays available as `toolExit`.

## Fit with the codebase

- Specs are loaded in `main.ts` before `registerCommand(root, rootCommand)` and
  converted to `CommandSpec`s, so plugin commands get the shared handler,
  `-o json`, `--profile` and the `dh agents` listing like any other command.
- A plugin leaf takes a variadic `[args...]`, calls `allowUnknownOption()`,
  and skips the "unknown command or extra argument" check that
  `registerCommand` does. With the argument rewrite in
  [Global flags](#global-flags), commander passes everything but `dh`'s flags
  through, including `--namespace=Foo` and anything after `--`.
- `config.json` gains a `plugins` map (see
  [Machine settings](#machine-settings)), read and written through the
  existing `Store` and its file lock.
- Actions go in a new `src/plugins/` beside `src/auth/`: `loadPlugins`,
  `checkConflicts`, `runTool` (local or SSH), `scaffoldPlugin`. Commands go in
  `src/commands/plugin/`.
- Deephaven's own specs (for `dhctl`, `dhconfig`, `iriscat`, `crcat`) ship
  inside the binary and load the same way, with user plugins unable to take
  their names. A command that needs more than a spec can offer is written as
  an ordinary built-in command instead.

## Not everything needs SSH

Many Deephaven users can sign in to a server but have no shell account on it,
so SSH plugins mainly serve administrators. Some tools have an alternative:
`dhctl`'s operations are also available from Core+ worker scripts ("Data
control from scripts" in the Enterprise docs). A built-in `dh` command could
call those over the profile's existing session, with no SSH account. Use SSH
plugins where nothing like that exists (`iriscat` and `crcat` read files on the
server), and consider native commands for the tools most people need.

## Phases

0. **SSH spike** against a real DHE host: run `dhctl`, `dhconfig`, `iriscat`
   and `crcat` through the quoting above. Confirm the PATH and `JAVA_HOME`
   assumptions (`crcat` needs `JAVA_HOME`), a `sudo -n -u irisadmin` prefix,
   exit codes, and `ControlMaster` timing against JVM startup. Check which
   node(s) each tool must run on. Check whether a key file written from a `dh`
   profile works with `dhctl --key`.
1. Spec schema, loader, conflict checks, local execution, `dh plugin
   add/list/check/remove`, and plugins in `dh agents`. This completes the
   server-installed use case.
2. SSH execution: per-server settings, `dh plugin set`, classified SSH
   errors, `dh plugin check`, `create` over SSH, and `tool.version`.
   Implementation plan: [ssh-plugins.md](ssh-plugins.md). Phase 0's checks against a
   real server are part of its validation.
3. `dh plugin create` scaffolding (built in the PoC; `--update` merging
   later, with phase 4).
4. Versions: several specs per plugin, the pin, the check on every run and `plugin_outdated`
   (implementation plan: [versions.md](versions.md); built); then
   `create --update` ([create-update.md](create-update.md)). Before it: several SSH hosts per plugin
   ([ssh-hosts.md](ssh-hosts.md)); after it: the SSH offer at login
   ([laptop-setup.md](laptop-setup.md)); then checked SSH hosts and no
   version overrides in `use` ([hosts-and-pins.md](hosts-and-pins.md)),
   which also records the deferred question of each tool's default host. The target surface is in the design
   doc, [dh plugins: design](https://claude.ai/artifact/QbDVebtCpHu33kaHU8nAEV).
5. `json` and `csv` output; `rename`, multi-value and validation for
   declared `options`.
6. `profileKey`, if phase 0 showed the key format works. Its `tool.auth`
   shape is settled before phase 7 either way.
7. Bundled Deephaven specs, once the spec schema is stable: phases 4–6 all
   change it, and a bundled spec makes the schema a public contract that
   can't change shortly after release.
8. Guided SSH setup (`dh auth ssh`).

`dh plugin add` and `remove` (left from phase 1) don't change the schema
and can be done at any point before phase 7.

## Decisions

1. **Exit code for tool failures:** a new exit 6 for both `tool_failed` and
   `tool_not_found`, with the tool's own code as `toolExit` in the `-o json`
   error. Exit 6 means `dh` reached the tool and the tool failed, as opposed to
   5 (couldn't reach the server) or 1 (a bug in `dh`). Rejected: reusing 1
   (agents would report tool refusals as `dh` bugs), and passing the tool's
   code through (its numbers collide with `dh`'s, e.g. a tool's 4 would read as
   an auth failure).

2. **No top-level promotion in v1.** Each plugin registers exactly one
   top-level noun (`dh ctl …`, `dh iriscat …`). A tool without subcommands is a
   leaf at its noun. `promote` is reserved in the schema so a later version can
   add it without a schema bump. Rejected for v1: promoting inner verbs
   (`dh truncate`), because generic verbs like `list` and `delete` would compete
   for the top level, and adding promotion later breaks nothing while removing
   it would. Also rejected: one umbrella noun for all plugins (`dh tools …`),
   since the reserved-names list already protects future built-ins.

3. **SSH commands are one-shot, sped up by OpenSSH multiplexing.** Each `dh`
   command runs one `ssh` and exits. `ControlMaster=auto` with
   `ControlPersist` lets later commands reuse the connection, so 2FA is entered
   once per window and the handshake isn't repeated. `dh` runs no background
   process of its own. Multiplexing is skipped when the user's
   `~/.ssh/config` already sets `ControlMaster` (checked with `ssh -G <host>`,
   which resolves config without connecting), is unavailable on Windows, and
   can be turned off. `dh auth logout` closes that server's masters
   (`ssh -O exit`). The first SSH pass has no off switch and doesn't close
   masters on logout; `ControlPersist=60` ends them a minute after the last
   use. The socket goes in a short 0700 directory using `%C`,
   since Unix socket paths are limited to about 104 characters. Rejected: a
   connection kept open by `dh` itself (a daemon lifecycle and stale-state
   errors for v1), and plain reconnecting (prompts on every command with 2FA).
   The tools are stateless anyway; each run signs in to Deephaven on its own.

4. **Where a plugin runs is machine config, not run-time guessing.** The spec
   stays portable. Each machine's `config.json` records `exec` (`local` or
   `ssh`), an optional `path`, and for `ssh` one host per plugin per server,
   because tools may run on different nodes (`dhctl` on a CONFIG node, `crcat`
   on the DIS node that has the files). A plugin without an entry is `local`.
   It's `local` unless `--exec ssh` is given to `create`/`add`/`set` (see
   decision 12). Rejected: an env var (`DH_PLUGIN_EXEC`, `DH_SSH_HOST`), and
   detecting the tool path each run. Deferred: several hosts per plugin per
   server, with a way to choose between them.

5. **`config` is reserved for `dh`'s own settings; `dhconfig` becomes
   `dh cluster`.** `dh config` is where users and agents expect a CLI's own
   settings (`git config`, `gh config`), and `dh` will likely need it as
   settings outgrow env vars. `cluster` describes what `dhconfig` manages
   (cluster-wide configuration in etcd) and reads well:
   `dh cluster schemas export`, `dh cluster pq restart`. It's a starting
   choice, open to change before the bundled spec ships.

6. **Tools authenticate the way users run them today; profile key reuse is
   opt-in.** `dh` never stands between the user and the tool: credential flags
   pass through, terminal prompts work, and how to run as another user is a
   per-machine `prefix` (`sudo`, `dzdo`, `env …`), not a spec field fixed to
   `sudo`. Unset, the tool runs as whoever runs `dh` (or the SSH login), e.g.
   `irisadmin` gets the built-in `iris` key automatically. `profileKey` (off by
   default, local runs only) passes the profile's key as `--key`, so the tool
   acts as the signed-in user with their ACLs and audit trail. It steps aside
   whenever the user passes their own credential flag, and ships after the
   SSH spike confirms the key format. Rejected: a spec-level `runAs` that
   always uses `sudo -n` (wrong for sites using `dzdo` or password sudo), and
   copying a private key to the server for SSH runs.
7. **Plugin commands take only the global flags `dh` acts on for them.**
   Locally that's `-o json|human` and `-h`; SSH adds `--profile` and
   `--no-prompt`. Everything else goes to the tool, so `dh` never accepts a
   flag and silently does nothing with it (as `--profile` and `--no-prompt`
   did in the first PoC). Inapplicable flags before the noun are a `usage`
   error. `dh agents` states the rule once plus a per-plugin marker rather
   than repeating global flags per command, which keeps the manifest small.
   Rejected: commander's `passThroughOptions` (all-or-nothing, and breaks
   global flags after built-in commands), and per-command lists of applicable
   globals in `dh agents` (token cost).
8. **The CLI reads as one CLI; a tool's raw help is reference, not output.**
   Plugin commands' help looks like any other `dh` command's: description,
   then options in the normal "Options:" section, listed in `dh agents` the
   same way. `dh plugin create` makes a first pass at parsing the tool's
   options, as it does descriptions, and the spec keeps each command's raw
   help as `source.help` so whoever edits it can fill the gaps. What can't be
   parsed confidently isn't shown at all. Rejected: showing the tool's help
   under its own heading (`upstreamHelp`, "From dhctl:"), which named the
   tool, showed a second usage line and copyable `dhctl …` examples that
   bypass `dh`, and listed flags such as `-h` that `dh` takes itself.
9. **Plugin settings are per server (cluster), found through the profile.**
   The SSH `host`, the remote `prefix` and the spec `version` are stored once
   per server under `servers.<origin>.plugins.<name>`, and a command uses the
   active profile's server's. Every profile on a cluster shares them; DHE
   requires all nodes of a cluster to run one version. `exec`, and `path` and
   `prefix` for local runs, stay machine settings, since a server install has
   no profile. This replaces decision 4's per-plugin `hosts` map. A
   per-profile override can come later if two identities on one cluster need
   different settings. Rejected: per-profile settings (each identity would
   repeat the cluster's setup and could disagree about it).
10. **dh helps with SSH access but never holds SSH credentials.** The system
    `ssh` owns authentication (`~/.ssh/config`, agents, ProxyJump, Kerberos,
    Teleport). The first SSH pass classifies SSH failures into fixable errors
    (`ssh_host_unknown`, `ssh_auth_failed`, `remote_unreachable`), each with a
    hint, and adds `dh plugin check`, which tests each step: host lookup,
    host key, non-interactive login, the tool, the prefix, the version, and
    round-trip time. Guided setup comes later, e.g. `dh auth ssh [server]`:
    create a key if needed, confirm the host key, `ssh-copy-id`, optionally
    write a `~/.ssh/config` entry (as `gcloud compute config-ssh` does; `gh
    auth login` similarly offers to upload an SSH key).
11. **Plugins have versions; specs record them; a pin chooses, and a
    version without a spec is an error, not a guess.** See
    [Versions](#versions) and [Updating specs](#updating-specs). Specs for
    several versions sit side by side; `tool.versionFrom` says how to find
    the tool's version (for DHE, where `/usr/illumon/latest` points,
    `illumon-db-<version>`, since DHE has no network endpoint for it and the
    tools have no `--version`). The spec in use is pinned per server (`ssh`)
    or machine (`local`) and chosen at startup, so help and `dh agents`
    match that cluster. Every run checks the tool against the pin (over SSH
    in the same session); a new version with a spec switches to it, one
    without fails with `plugin_outdated`. Regenerating keeps hand edits by a
    three-way merge on `source.help` ([create-update.md](create-update.md)).
    Rejected: picking the newest spec or one from the same release line,
    which would run commands the cluster's tool may not have.
12. **SSH is always explicit: `--exec ssh`.** `dh plugin create` and `add`
    use `local` unless given `--exec ssh` (with optional `--ssh-host`), the same
    flag as `dh plugin set` and the `exec` setting's own name. A tool missing
    on this machine is `tool_not_found`, with a hint mentioning `--exec ssh`
    when there's a profile. Rejected: switching to SSH when the tool isn't
    found locally; plugins wrap arbitrary tools, so a missing path more
    likely means a typo or an uninstalled tool, and connecting to a server
    shouldn't happen by surprise. `dh auth login` to an Enterprise server is one
    more explicit way: it asks once (`--plugins ssh|local`) about plugins
    with no `exec` setting whose tools aren't on this machine, and a no is
    remembered in `sshOfferDeclined` ([laptop-setup.md](laptop-setup.md)).
    Nothing switches to SSH without that answer.

## Validation

- [ ] Quoting round trip: an argument list containing `it's`, `a b`, `$HOME`,
      `` `id` ``, `; rm -rf /`, `*`, an empty string, `--x="y"`, a tab, a
      newline and `ünïcode` reaches the remote tool unchanged, through
      `sh -c` (unit test) and through a real `ssh` (spike).
- [ ] `dh ctl intraday truncate --namespace=Foo -x` runs
      `dhctl intraday truncate --namespace=Foo -x`.
- [x] `dh ctl intraday truncate -o json …` gives JSON from `dh`; the tool
      receives `-o json` only after `--`, and `-o out.csv` directly.
- [x] `dh ctl intraday rescan --profile p --no-prompt -V` passes all three to
      the tool; `dh --profile p ctl intraday rescan` is a `usage` error.
- [x] `dh ctl --help` and `dh ctl intraday rescan --help` list only `-o` under
      Global Options; built-in commands' help is unchanged.
- [x] `dh agents -o json` has `pluginFlags` once and `"plugin": "dhctl"` on
      `dh ctl` only.
- [ ] A spec named `auth`, a reserved name, or a second plugin's name is
      rejected by `dh plugin add` with `plugin_invalid`.
- [ ] A spec whose noun conflicts on disk doesn't load and appears in
      `dh plugin list` as `conflict`.
- [x] `ssh` exit 255 → `remote_unreachable`, exit 5; tool exit 3 →
      `tool_failed`, exit 6, `"toolExit": 3` in the JSON error.
- [ ] With `--no-prompt` and no SSH key, the command fails within
      `DH_TIMEOUT` instead of waiting on a password prompt.
- [ ] `dh agents -o json` lists plugin commands and a `plugins` status block.
- [ ] `dh plugin create /usr/illumon/latest/bin/dhctl` produces a spec with the
      `intraday` and `metadata` groups and their verbs, both on the server
      itself and over SSH from another machine.
- [ ] On a server with no `dh` profile, `dh ctl intraday rescan` runs the local
      `dhctl` (server-installed case).
- [ ] The same spec file, on a machine without `dhctl` and with an Enterprise
      profile, is added as `exec: ssh` and runs over SSH.
- [ ] With `dh plugin set ctl --ssh-host infra-1` and
      `dh plugin set crcat --ssh-host dis-2`, each plugin connects to its own host
      for the same profile.
- [ ] `dh plugin set iriscat --path <other path>` makes `dh iriscat` run that
      path; the spec file is unchanged.
- [ ] `dh plugin set ctl --prefix "dzdo -u irisadmin"` runs
      `dzdo -u irisadmin /usr/illumon/latest/bin/dhctl …`, locally and over SSH.
- [ ] On a terminal, a prefix of `sudo -u irisadmin` (no `-n`) shows sudo's
      password prompt, locally and over SSH; with `-o json` or no terminal it
      fails instead of hanging.
- [x] `echo data | dh <plugin> …` delivers `data` to the tool's stdin.
- [ ] With `profileKey` on, `dh ctl intraday rescan …` runs as the profile's
      user; `dh ctl intraday rescan --key <file> …` uses the user's key and
      `dh` adds none.
