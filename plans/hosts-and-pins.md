# Checked SSH hosts, and no version overrides

Two changes from the design discussion, on top of the implemented
[ssh-hosts.md](ssh-hosts.md), [versions.md](versions.md) and
[laptop-setup.md](laptop-setup.md):

1. **Hosts are checked when they're set**: `--ssh-host` on `set` and
   `create` must resolve, accept a login, and have the tool, so a typo fails
   at once instead of on a later run.
2. **`use` never pins a spec to a version it isn't for.** `dh plugin use
   <name> <version>` is removed; `use` only chooses among things that exist
   (the plugin's SSH hosts). A server's version is matched by a spec for that
   version or it's an error; the fix is always to get the right spec
   (update `dh` for the tools it ships; `dh plugin create` for others).

Design doc: [dh plugins: design](https://claude.ai/artifact/QbDVebtCpHu33kaHU8nAEV).

## Deferred: which host a tool runs on by default

Still the server URL's hostname, unless `sshHosts` is set. Considered and
left open, because which node each tool needs isn't known yet, and
`dhctl`/`dhconfig` may be wrapped in another umbrella tool:

- `/iris/connection.json` (fetched by the login probe, no sign-in needed)
  names the infra services' hosts: `auth_host`, `controller_host`,
  `configuration_host` (strings or lists). On the QA cluster
  (`qa-gplus-java17-cluster-infra-1.int.illumon.com`) all three are the one
  infra node. None of the tools talks to the controller; `dhconfig` (and
  `dhctl intraday`, through routing) use the configuration server, so
  `configuration_host` would be the candidate.
- The file-reading tools (`iriscat`, `crcat`, `dhctl metadata`) need the
  node with the data, which no service in `connection.json` names. That
  points at a per-spec (or per-command) role, e.g. `runsOn`, plus a map of
  the cluster's nodes.
- Finding nodes: on the QA cluster `dhconfig serviceregistry list` is
  empty, `dhconfig dis list` has no hosts, and data routing names DIS hosts
  only through YAML anchors (`*dh-import` → the infra node, `*qa-results` →
  an IP outside the cluster) or dynamic registration. Not reliable enough to
  build on; check `/etc/sysconfig/deephaven/cluster.cnf` on a multi-node
  cluster first.

Status: implemented, and tested against the fake `ssh`; the check against
a real DHE server is still to do.

## Contract

### 1. Checked hosts

- `dh plugin set <name> --ssh-host <h>…` and `dh plugin create … --exec ssh
  --ssh-host <h>…`: for each host given, before anything is written, run
  `dh plugin check`'s `host`, `connect` and `tool` steps against it
  (`ssh -G`, a `true` run, `test -x '<tool.path>'`; for `create`, `tool`
  is skipped because the help runs that follow need the tool anyway).
  Steps are printed through `note` as `check` prints them. The first
  failure fails the command with that step's error (`remote_unreachable`,
  `ssh_auth_failed`, `ssh_host_unknown`, `tool_not_found`), and nothing is
  written.
- Only for `ssh` plugins: on `set`, when the plugin's `exec` after the call
  is `ssh`. A `local` plugin's `--ssh-host` is stored without a check (as
  today it's only used once it runs over SSH); the human output adds
  `` ! Not checked: <name> runs on this machine. Check later with `dh plugin check <name>`. ``
- `--no-check` on `set` and `create` skips the checks (for a node that's
  down for now). Help text: `Don't check the SSH hosts before saving them.`
- `--ssh-host ""` (back to the URL hostname) checks nothing.

### 2. `use` and versions

- `dh plugin use <name> [version]` loses `[version]`:
  `dh plugin use <name> [--ssh-host <host>]` only. A second positional
  argument is the usual `usage` "Unknown command or extra argument".
  The pin's `version`/`detected` are written only by detection (first use,
  `create`, a run, `dh plugin check`).
- `plugin_outdated` hints no longer offer to keep an old spec:
  - for a run or `check`: `` Generate one: `dh plugin create <tool.path> --name <name>[ --exec ssh]`. ``
    (the "or keep using <v>: `dh plugin use …`" part is removed).
  - "Can't tell which version …": hint `` Check it: `dh plugin check <name>`. ``
- **Ambiguous** (several specs, no `versionFrom`): message unchanged; hint
  `` Keep one spec for <name>, or add "tool.versionFrom" so dh can tell them apart. ``
- **Pin-missing** (the pinned spec is gone): no longer a `usage` state. It
  is treated as no pin: first use applies (detect, pin the spec for the
  detected version or the unversioned one, else `plugin_outdated`). The
  `pin_missing` state disappears from `dh plugin list` and `dh agents`.
- A pin written by the old `use <version>` (its `detected` differs from its
  `version` while that spec has a different `tool.version`) is a mismatch:
  the next run or `check` handles it as **a change** (versions.md): the
  exact spec if there is one, else `plugin_outdated`.

### 3. Tools that run as another user (added after the first real try)

Found wrapping a tool installed in another user's home
(`/home/abaranec/deephaven/current/bin/deephaven`, readable and runnable
only by `abaranec`): the SSH user got "Permission denied" for the help, the
version lookup and the checks.

- `dh plugin create … --prefix <command>`: split as `set --prefix` is. The
  help runs and the version lookup (`readlink -f`) run through it, and it's
  saved as `set --prefix` saves it (the profile's server's for `--exec ssh`,
  the machine's for local). Without the flag, a stored prefix for that name
  is used.
- Everything that looks at the tool runs as the user the tool runs as: the
  first-use and `check` version lookup, the in-session check's `readlink`
  (`p=$(<prefix> readlink -f '<tool.path>')`), and the `tool` step
  (`<prefix> test -x '<tool.path>'`) in `check`, `set` and `create`.
- `check`'s steps become `host`, `connect`, `prefix`, `tool`, `version`:
  the prefix is checked before the tool is checked through it. `set` checks
  new hosts with the prefix in effect after the call.
- `create`'s first help run over SSH fails the command, instead of saving
  the output as help, on exit 126 (`tool_failed`, `toolExit: 126`,
  `` Can't run <path> on <host>: permission denied ``, hint
  `` Run it as a user who can, e.g. --prefix "sudo -n -u <user>". ``; with a
  prefix: `` …with the prefix `<prefix>`: permission denied ``, hint
  `Check that the prefix's user can run it.`), and when a prefix's sudo
  wants a password (`tool_failed`, the sudo hint).
- The DHE version regex covers the newer install layout too:
  `/(?:illumon-db|deephaven-enterprise)-([^/]+)/`
  (`…/deephaven-enterprise-2026.04.261104200000g00000000f6/bin/deephaven`).

## Checklist

- [x] Host checks on `set` and `create --ssh-host`, `--no-check`, the `local` note
- [x] `dh plugin use` without `[version]`
- [x] `plugin_outdated`, "can't tell" and ambiguous hints
- [x] Pin-missing becomes first use; `pin_missing` state removed
- [x] Old `use <version>` pins handled as a change
- [x] Part 3: `create --prefix`, everything through the prefix, step order, exit 126 and sudo on the first help run, the `deephaven-enterprise-` layout
- [x] Tests for every automated Validation item; existing tests for `use <version>` and pin-missing replaced
- [x] The design doc (CLI surface `use` line; walkthrough: no "keep using" lines), `plans/versions.md`, `plans/plugins.md` updated to match (the design doc is still to update: it's a claude.ai artifact)

## Validation

- [x] `bun run check` and `bun test` pass
- [x] `dh plugin set ctl --exec ssh --ssh-host good` writes the setting after `host`/`connect`/`tool` pass; with `FAKE_SSH_FAIL=dns` it fails `remote_unreachable` and writes nothing; with a spec whose tool path doesn't exist, `tool_not_found` and nothing written; `--no-check` writes without any ssh run
- [x] `set --ssh-host` on a `local` plugin stores it, makes no ssh run, and prints the "Not checked" line
- [x] `dh plugin use ctl 2026.01.1g0` → `usage` (extra argument); `dh plugin use crcat --ssh-host dis-2` still works
- [x] After an upgrade with no spec, the `plugin_outdated` hint names `dh plugin create` and not `dh plugin use`
- [x] Ambiguous gives the new hint
- [x] Deleting the pinned spec's file: the next run pins the spec for the detected version, or fails `plugin_outdated`; `dh agents` never shows `pin_missing`
- [x] A config holding an old mismatched pin (`version: V1`, `detected: V3`, specs V1 and V3): the next run switches to V3 with the notice

- [x] Part 3, with the fake ssh: `create --prefix env A=1` runs `--help` and `readlink` through it and saves it; a non-executable tool fails `create` with exit 126 and a `--prefix` hint; a prefix whose sudo wants a password fails with the sudo hint; first-use and in-session `readlink` and the `tool` step run through the prefix; `check` orders `host`, `connect`, `prefix`, `tool`; a `deephaven-enterprise-2026.04.1g0` install gets `tool.version` and `versionFrom`

Against a real DHE server:

- [x] `dh plugin create /home/abaranec/deephaven/current/bin/deephaven --exec ssh --ssh-host abaranec-d-1.int.illumon.com --name andy --prefix "sudo -n -u abaranec"` reads the real help and records `2026.04.261104200000g00000000f6`. Result: the prefix step passed, 34 help runs (14 subcommands, recursive), version found from the `deephaven-enterprise-` path, and pinned

- [ ] `dh plugin set crcat --ssh-host typo-host` fails at once with `remote_unreachable`

## Relevant Files

- `src/commands/plugin/set.ts`, `src/commands/plugin/create.ts` — host checks, `--no-check`
- `src/commands/plugin/check.ts` — share the `host`/`connect`/`tool` steps
- `src/commands/plugin/use.ts` — drop `[version]`
- `src/plugins/versions.ts` — hints; pin-missing as first use; old pins
- `src/commands/plugin/tool.ts`, `src/commands/plugin/index.ts`, `src/commands/agents.ts` — `pin_missing` removed
- `tests/plugin_versions.test.ts`, `tests/plugin_ssh.test.ts`
- `plans/versions.md`, `plans/plugins.md`, the design doc
