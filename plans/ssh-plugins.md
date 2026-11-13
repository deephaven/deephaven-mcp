# SSH plugins: first pass

Status: implemented and validated, against a fake `ssh` and a real DHE
server; results are in [plugins.md](plugins.md).

Run a plugin's tool on the active profile's server over SSH, configure where
and how per server, and diagnose SSH problems. Design and decisions:
[plugins.md](plugins.md) (decisions 3, 7, 9, 10, 11, 12; sections "Machine
settings", "Versions", "Over SSH", "Global flags"). Read
[docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) first: commands are
questions and actions, and `output.ts` is the only writer to stdout/stderr.

Today (the PoC) every plugin runs locally; an `exec: "ssh"` setting makes the
leaf command fail with "can't do yet". This plan replaces that.

Out of scope: several spec versions per plugin and choosing between them,
`dh plugin use`, the daily version check and `outdated`, guided SSH setup,
`create --update`, `dh plugin add`, `profileKey`, a per-server remote `path`.

## Contract

### Config (`config.json`)

```json
{
  "plugins": { "ctl": { "exec": "ssh" } },
  "servers": {
    "https://dhe.example.com:8123": {
      "kind": "enterprise",
      "plugins": {
        "ctl": { "host": "infra-1", "prefix": ["sudo", "-n", "-u", "irisadmin"] }
      }
    }
  }
}
```

- `plugins.<name>` (machine, `PluginSettings` in `src/auth/store.ts`):
  `exec` (`"local"` default, `"ssh"`), `path` and `prefix` (local runs
  only). Unchanged from the PoC.
- `servers.<origin>.plugins.<name>` (server; new optional `plugins` map on
  `ServerEntry`): `host` (SSH destination, passed to `ssh` as given),
  `prefix` (argv before the tool on that server), `version` (string; stored
  and shown only, no selection yet; nothing in this pass writes it).
- An `ssh` plugin uses the server's `prefix` and ignores the machine's; a
  `local` plugin the reverse. The remote tool path is always the spec's
  `tool.path`; the machine's `path` doesn't apply over SSH.

### Spec

- `tool.version`: optional string, validated in `parseSpec`. `dh plugin
  create` sets it from the tool's real path: local `fs.realpath(path)`, SSH
  remote `readlink -f '<path>'` with its output trimmed. Version = capture 1
  of `/\/illumon-db-([^/]+)\//` on that path; unset if no match or the
  lookup fails. The lookup isn't counted in `runs`.

### Which server

SSH plugins need a saved profile. The active profile, in order:

1. `--profile <name>` or `--profile=<name>` anywhere on the line before a
   `--` (see Global flags);
2. `DH_PROFILE`, if non-empty;
3. `config.defaultProfile`, which is a profile **id** (a key of
   `config.profiles`), not a name.

1 and 2 are names, looked up with `findByName` (`src/auth/profile.ts`). An
unknown name: `usage`, `` No profile named "<name>" ``, hint
`` Run `dh auth` to list profiles. `` (same as `requireAuth` in
`src/auth/session.ts`). No profile at all: `auth_required`,
`You're not logged in to a Deephaven server.`, hint
`` Run `dh auth login` to get started. ``; when `DH_SERVER` is set, the hint
is instead `` SSH plugins need a saved profile (DH_SERVER alone isn't enough). Run `dh auth login`. ``
(`DH_SERVER` is resolved over the network, so it can't key settings
offline.) No network is used. The profile's `server` is the origin that keys
`servers.<origin>`.

### Reading config at startup

`main.ts` needs each plugin's `exec` before parsing (routing, help). Read
**`config.json` only** (not `credentials.json`, whose permission check can
throw). Any failure to read or parse it means every plugin is `local`; never
fail startup over it.

### Global flags on `ssh` plugins

- `--profile <name>`, `--profile=<name>`, `--no-prompt` and the hidden
  `--no-input` are `dh`'s in any position, like `-o json|human` and `-h`:
  `routePluginArgs` keeps them before the `--` it inserts. Everything else
  still goes to the tool. `local` plugins are unchanged.
- The before-noun `usage` error applies to `local` plugins only.
- Help's "Global Options" for an `ssh` plugin's noun and subcommands lists
  `-o`, `--profile` and `--no-prompt` (in that order, as declared); for a
  `local` plugin only `-o`, as today.
- `dh agents -o json`: `pluginFlags` becomes

  ```json
  {
    "local": ["-o, --output <format>", "-h, --help"],
    "ssh": ["-o, --output <format>", "-h, --help", "--profile <name>", "--no-prompt"],
    "rest": "passed to the plugin's tool"
  }
  ```

  and each plugin noun (only nouns; subcommands inherit) has
  `"pluginExec": "local"` or `"ssh"` beside `"plugin"`.
- `dh agents` human summary, replacing today's plugin line; each sentence
  only when such plugins exist:
  `` Plugins (`dh ctl`, `dh iriscat`) take only `-o json`; every other flag goes to their tool. ``
  then `` Over SSH (`dh ctl`), they also take `--profile` and `--no-prompt`. ``

### Running over SSH

Argv, exactly:

```text
ssh (-t | -T) [-o BatchMode=yes] -o ConnectTimeout=<seconds>
    [-o ControlMaster=auto -o ControlPath=<config dir>/ssh/%C -o ControlPersist=60]
    -- <host> <remote command>
```

- `<seconds>`: `DH_TIMEOUT` as `timeoutMs()` in `src/errors.ts` reads it
  (default 30), in whole seconds. It bounds connecting only; a running tool
  has no time limit.
- `<host>`: the server's `host` setting, else `new URL(origin).hostname`
  with IPv6 brackets removed (`[::1]` → `::1`).
- `-t` when `process.stdin.isTTY` and `process.stdout.isTTY` and neither
  `-o json` nor `--no-prompt`/`--no-input`; otherwise `-T` and
  `-o BatchMode=yes`.
- Multiplexing options only when all hold:
  - not Windows (`process.platform !== "win32"`);
  - `ssh -G <host>` exits 0 and its output has the line
    `controlmaster false` (lines are `<lowercase key> <value>`; any other
    value means the user configured multiplexing). Run `ssh -G` once per
    `dh` invocation;
  - `join(configDir(), "ssh").length + 1 + 40 <= 100` (`%C` expands to 40
    hex characters; Unix socket paths are limited to about 104).

  `<config dir>/ssh` is created (recursive) with mode 0700 before use.
- `<remote command>`: `[...serverPrefix, tool.path, ...specArgv, ...userArgs]`,
  each word wrapped in `'…'` with every `'` written `'\''`, joined with
  single spaces, passed as one argv element.
- stdin inherited. stdout inherited, or captured for `-o json` (output
  `{"stdout": "…"}`, as for local). stderr piped: each chunk is written to
  `dh`'s stderr as it arrives (through a new byte-writing function in
  `output.ts`, keeping it the only writer) and the last 16 KiB are kept for
  classifying a failure.
- SIGINT is ignored by `dh` while ssh runs, as for local runs.
- `ssh` not on `PATH` (spawn `ENOENT`): `tool_not_found`, `ssh not found`,
  hint `Install OpenSSH.`

Known limitations, accepted: with `-T`, Ctrl-C stops `ssh`, and the remote
process may keep running until it next writes; a tool that itself exits 255
is reported as an SSH failure; with `-t`, the remote stderr arrives on
stdout, so the sudo hint below only works with `-T`.

### Exit codes and errors

New codes in `src/errors.ts` (listed by `dh agents`):

| Code                 | Exit | Description (for `dh agents`)          | When                                                                       |
| -------------------- | ---- | -------------------------------------- | -------------------------------------------------------------------------- |
| `ssh_host_unknown`   | 5    | SSH host key unknown or changed        | ssh exit 255, stderr matches `/Host key verification failed\|REMOTE HOST IDENTIFICATION HAS CHANGED/` |
| `ssh_auth_failed`    | 4    | SSH login to the server failed         | ssh exit 255, stderr matches `/Permission denied/`                          |
| `remote_unreachable` | 5    | SSH couldn't reach the server          | any other ssh exit 255                                                     |

| ssh exit | Result |
| -------- | ------ |
| 0        | success |
| 255      | as above. Messages: `SSH host key for <host> is unknown or changed` / `SSH login to <host> failed` / `Couldn't reach <host> over SSH`. `detail`: the last non-empty stderr line. Hints: `` Connect once with `ssh <host>` and check its host key. `` / `` Set up key-based login, e.g. `ssh-copy-id <host>`, or check ~/.ssh/config. `` / `` Check the host, or set it: `dh plugin set <plugin> --host <host>`. `` |
| 127      | `tool_not_found`, `Tool not found on <host>: <tool.path>` (a missing prefix command, e.g. no `sudo`, also gives 127) |
| other n  | `tool_failed`, `<tool> exited with <n>`, `toolExit: n`; hint `` The prefix's sudo needs a password: use `sudo -n` with NOPASSWD, or run in a terminal. `` when stderr matches `/sudo: (a password is required\|a terminal is required)/` |

Every error from an SSH run adds `"host": "<host>"` to its `-o json` fields.

### `dh plugin set <name>`

`dh plugin set <name> [--exec <local|ssh>] [--path <path>] [--host <host>] [--prefix <command>]`

- `<name>` must be a loaded plugin, else `usage`, `` No plugin named "<name>" ``,
  hint `` Run `dh plugin list`. ``. No flags: `usage`,
  `Nothing to set`, hint `Pass --exec, --path, --host or --prefix.`
- `--exec`, `--path`: machine settings. `--path` must be absolute
  (`usage`). `--host`: the active profile's server's (profile required, as
  above); non-empty, no whitespace (`usage`). `--prefix`: the server's if
  the plugin's `exec` after this call is `ssh` (profile required), else the
  machine's.
- `--prefix` is split into words like POSIX shell quoting, without
  expansion: whitespace separates words; `'…'` is literal; inside `"…"`, `\`
  escapes only `"`, `\`, `$` and `` ` ``; outside quotes, `\` escapes the
  next character. An unterminated quote is `usage`. `--prefix ""` (no
  words) removes the setting.
- Written with `Store.update` (locked).
- Output, the settings now in effect. Human:

  ```text
  ✔ Updated ctl
    exec: ssh
    host: infra-1 (https://dhe.example.com:8123)
    prefix: sudo -n -u irisadmin
  ```

  (`host` line only for `ssh`, showing the derived hostname when unset;
  `path`/`prefix` lines only when set.) JSON, every key always present,
  `null` when not applicable:
  `{ "plugin": "ctl", "exec": "ssh", "path": null, "prefix": ["sudo", "-n", "-u", "irisadmin"], "server": "https://dhe.example.com:8123", "host": "infra-1" }`.
  For a `local` plugin `server` and `host` are `null` and `prefix` is the
  machine's.

### `dh plugin list`

Needs no profile; `ssh` details shown when there is one. Each plugin's
human line:

- local: `  ctl   local: <prefix words> <path>` (as today)
- ssh: `  ctl   ssh: <host> <prefix words> <tool.path>`, `<host>` being
  `(no profile)` without one
- either, then ` (<tool.version>)` when the spec has one.

JSON entries add `"server"`, `"host"`, `"version"` (each `null` when
unknown/not applicable); `"prefix"` is the one in effect.

### `dh plugin check <name>`

Runs each step in order with `-T -o BatchMode=yes` (never prompts), printing
`✔ <step>  <detail>` through `note` for each passed step (step name padded
to 7), and fails with the first failing step's error.

| Step      | `ssh` plugin                                              | `local` plugin            | Detail on success |
| --------- | --------------------------------------------------------- | ------------------------- | ----------------- |
| `host`    | `ssh -G <host>`; non-zero → `remote_unreachable`          | skipped                   | `<hostname> as <user>, port <port>` |
| `connect` | ssh … `true` (remote command `'true'`); exit classified as in the table above | skipped | `<host> in <ms> ms` |
| `tool`    | remote `test -x '<tool.path>'`; non-zero → `tool_not_found` | `access(path, X_OK)`; failure → `tool_not_found` | the path |
| `prefix`  | only with a prefix: remote `<prefix words> true`; non-zero → `tool_failed` (with the sudo hint when it matches) | same, locally | the prefix words |
| `version` | remote `readlink -f '<tool.path>'`; never fails            | `fs.realpath`; never fails | the version or `unknown`, plus ` (spec: <tool.version>)` when the spec's differs |

The `ssh` plugin needs a profile (as above). JSON on success:
`{ "plugin": "ctl", "exec": "ssh", "host": "infra-1", "checks": [{ "step": "host", "ok": true, "detail": "…", "ms": 12 }, …] }`
(`host` `null` for local; `ms` on every step). On failure the error's JSON
adds `"checks"`: the passed steps and the failed one with `"ok": false` and
no `detail`.

### `dh plugin create` over SSH

`dh plugin create <tool> [--name <noun>] [--force] [--recurse | --no-recurse] [--exec <local|ssh>] [--host <host>]`

- `--exec local` (default): unchanged from the PoC, no config written. A
  missing tool is `tool_not_found`, `Tool not found: <tool>`; with an active
  profile the hint is
  `` Not on this machine. To read it from the profile's server over SSH, add `--exec ssh`. ``
- `--exec ssh`: needs an active profile. `readToolHelp` must not check for
  the file locally. Every help run (`--help`, `help <sub>`, `<sub> --help`)
  goes over SSH to `--host`, else the server's `host` setting, else the
  URL hostname, with `-T -o BatchMode=yes` and the multiplexing rule;
  stdout and stderr captured together (as locally); exit codes ignored,
  except 255 on any run (classified; the command fails) and 127 on the first
  run (`tool_not_found`, `Tool not found on <host>: <tool>`). After the spec
  is written, `plugins.<name>.exec` becomes `"ssh"` and `--host`, if given,
  is saved to the server's `host`.
- `--host` without `--exec ssh`: `usage`, `--host needs --exec ssh`.
- Human: `✔ Wrote <file> (from <n> help runs)` locally,
  `✔ Wrote <file> (from <n> help runs over SSH: <host>)` over SSH. JSON adds
  `"exec"` and `"host"` (`null` for local) and `"version"` (`null` when
  unknown).

## Test fixture: fake `ssh`

`tests/fixtures/plugins/fake-ssh`, put first on `PATH` as `ssh` in the
tests (Windows skipped, as for the other shell fixtures):

- `-G <host>`: prints `hostname <host>`, `user tester`, `port 22`, and
  `controlmaster ${FAKE_SSH_CONTROLMASTER:-false}`; exit 0.
- Otherwise: appends its argv, one JSON array per line, to `$FAKE_SSH_LOG`;
  skips options (`-t`, `-T`, and `-o <value>`) up to `--`, then takes the
  host and the remote command.
- `FAKE_SSH_FAIL=auth|hostkey|dns`: prints `Permission denied (publickey).`
  / `Host key verification failed.` / `ssh: Could not resolve hostname <host>: Name or service not known`
  to stderr and exits 255.
- Otherwise: `exec sh -c "<remote command>"`, so quoting, stdin, stdout,
  stderr and exit codes are real.

Since the remote command runs locally, tests that need a version build
`<tmp>/illumon/illumon-db-2026.01.1g0/bin/dhctl` (a copy of `fake-dhctl`)
and a symlink `<tmp>/illumon/latest` → `illumon-db-2026.01.1g0`, and use
`<tmp>/illumon/latest/bin/dhctl` as the tool path.

## Checklist

- [x] `ServerEntry.plugins` and its settings type in `src/auth/store.ts`
- [x] `tool.version` in `src/plugins/spec.ts`, validated as a string
- [x] Active profile and effective settings (`src/plugins/settings.ts`): order, id vs name, errors, `DH_SERVER` hint, host derivation with IPv6
- [x] Startup: `config.json`-only read that never fails; plugin commands registered with their `exec`
- [x] `routePluginArgs`: `--profile`/`--no-prompt`/`--no-input` kept for `ssh` plugins; before-noun error for `local` only
- [x] Help "Global Options" by `exec`; `dh agents` `pluginFlags`, `pluginExec`, summary lines
- [x] `src/plugins/ssh.ts`: argv, TTY/BatchMode rule, multiplexing rule, quoting, stderr pass-through via `output.ts`, SIGINT, exit classification
- [x] Error codes `ssh_host_unknown`, `ssh_auth_failed`, `remote_unreachable` with descriptions, messages, hints, `host` field
- [x] Leaf commands run local or SSH by `exec`; the PoC's "can't do yet" error removed
- [x] `dh plugin set` with the prefix splitter
- [x] `dh plugin list` columns
- [x] `dh plugin check`
- [x] `dh plugin create --exec ssh [--host]`, `tool.version`, missing-tool hint
- [x] Fake `ssh` fixture
- [x] Tests for every automated Validation item
- [x] `plans/plugins.md`: status, "Over SSH" section and decisions match what was built
- [x] `plans/plugins-poc-test.md`: SSH section (laptop → server)
- [x] `docs/ARCHITECTURE.md`: `src/plugins/` row mentions SSH

## Validation

Automated, with the fake `ssh` on `PATH`:

- [x] `bun run check` and `bun test` pass
- [x] Quoting: the arguments `it's`, `a b`, `$HOME`, `` `id` ``, `; rm -rf /`, `*`, an empty string, `--x="y"`, a tab, a newline and `ünïcode` reach the tool unchanged
- [x] Argv without a terminal: `-T -o BatchMode=yes -o ConnectTimeout=30 -o ControlMaster=auto -o ControlPath=<dir>/ssh/%C -o ControlPersist=60 -- <host> <one quoted command>`; with `DH_TIMEOUT=7`, `ConnectTimeout=7`
- [x] `FAKE_SSH_CONTROLMASTER=auto`: no multiplexing options; a `DH_CONFIG_DIR` making the socket path longer than 100: none either
- [x] Host: the server's `host` setting when set, else the profile's server URL hostname
- [x] Prefix: the server's prefix is used for `ssh`; a machine prefix is ignored
- [x] `FAKE_SSH_FAIL=auth` → `ssh_auth_failed`, exit 4; `hostkey` → `ssh_host_unknown`, exit 5; `dns` → `remote_unreachable`, exit 5; each with its hint, and `"host"` in the `-o json` error
- [x] Remote exit 127 (tool path missing) → `tool_not_found`, exit 6; remote exit 3 → `tool_failed`, exit 6, `toolExit: 3`
- [x] A remote tool printing `sudo: a password is required` and exiting 1 → `tool_failed` with the sudo hint
- [x] An `ssh` plugin with no profile → `auth_required`, exit 4, and the fake ssh log is empty; with only `DH_SERVER` set, the `DH_SERVER` hint
- [x] With profiles `p1` and `p2` on different servers, `dh ctl intraday rescan --profile p2` uses `p2`'s server's `host`; `--profile` never reaches the tool; `--profile nosuch` → `usage`; a `local` plugin still passes `--profile` through
- [x] `dh ctl intraday rescan --help` lists `--profile` and `--no-prompt` for an `ssh` plugin and only `-o` for a local one; `dh agents -o json` has `pluginFlags.ssh` and `"pluginExec": "ssh"` on `dh ctl` only; `dh agents` has the "Over SSH" sentence
- [x] `dh plugin set ctl --exec ssh --host infra-1 --prefix "sudo -n -u 'iris admin'"` writes `plugins.ctl.exec: "ssh"`, and the server's `host` and `prefix` `["sudo", "-n", "-u", "iris admin"]`; `--prefix ""` removes it; `--prefix "a 'b"` → `usage`; an unknown plugin → `usage`; no flags → `usage`
- [x] `dh plugin list -o json` shows `server`, `host`, `prefix`, `version` for the active server; without a profile, `host` is `null`
- [x] `dh plugin check ctl -o json` passes all five steps; with `FAKE_SSH_FAIL=auth` it fails at `connect` with `ssh_auth_failed` and `checks` = `host` ok, `connect` not ok
- [x] `dh plugin create <tmp>/illumon/latest/bin/dhctl --exec ssh --recurse` with a profile runs every help over the fake ssh, writes `plugins.ctl.exec: "ssh"`, and sets `tool.version` `2026.01.1g0`; without `--exec ssh` and with the tool path absent locally → `tool_not_found` with the `--exec ssh` hint and an empty fake ssh log; `--host x` alone → `usage`
- [x] Over SSH, `-o json` gives `{"stdout": …}`, and stdin piped to `dh` reaches the tool

Against a real DHE server, from a laptop (record results in `plans/plugins.md`):

- [x] `dh plugin set ctl --exec ssh --host <server>`, then `dh ctl metadata list --table-name DbInternal` works
- [x] `dh plugin check ctl` passes; record the `connect` ms, and the time of one `dh ctl` command with and without multiplexing next to JVM startup
- [x] A missing key gives `ssh_auth_failed` within `DH_TIMEOUT`, not a password prompt, with `--no-prompt`
- [x] In a terminal, a `sudo -u irisadmin` prefix (no `-n`) prompts for the password; with `--no-prompt` it fails with the sudo hint
- [x] `dh plugin create /usr/illumon/latest/bin/dhctl --name ctl3 --exec ssh --recurse` from the laptop matches the spec generated on the server, with `tool.version` set
- [x] `iriscat` and `crcat` run over SSH (PATH and `JAVA_HOME` assumptions)

## Relevant Files

- `src/auth/store.ts` — `ServerEntry.plugins` and its settings type
- `src/plugins/spec.ts` — `tool.version`
- `src/plugins/settings.ts` (new) — active profile, `config.json`-only read, effective settings per plugin, host derivation
- `src/plugins/ssh.ts` (new) — argv, quoting, multiplexing rule, exit classification
- `src/plugins/run.ts` — local and SSH runs share the result and SIGINT handling
- `src/plugins/scaffold.ts` — help capture through a local or SSH runner; no local file check for SSH; version lookup
- `src/output.ts` — byte pass-through to stderr
- `src/errors.ts` — three new codes
- `src/cli/command.ts` — plugin marker carries `exec`
- `src/cli/plugin-args.ts` — routing by `exec`
- `src/main.ts` — read config before routing; `visibleGlobalOptions` by `exec`
- `src/commands/agents.ts` — `pluginFlags`, `pluginExec`, summary lines
- `src/commands/plugin/index.ts` — `list` columns; register `set` and `check`
- `src/commands/plugin/tool.ts` — leaf runs by `exec`; remove the "can't do yet" error
- `src/commands/plugin/create.ts` — `--exec`, `--host`, version, output
- `src/commands/plugin/set.ts` (new)
- `src/commands/plugin/check.ts` (new)
- `src/commands/plugin/questions.ts` — prefix splitting, flag validation
- `tests/fixtures/plugins/fake-ssh` (new)
- `tests/plugin_ssh.test.ts` (new)
- `tests/plugins.test.ts` — `pluginFlags` shape
- `plans/plugins.md`, `plans/plugins-poc-test.md`
- `docs/ARCHITECTURE.md` — `src/plugins/` row
