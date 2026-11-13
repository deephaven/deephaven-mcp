# Plugin versions

Keep specs for several versions of a tool side by side, pin one per server
(written by `dh plugin create` or on first use), check on every run that the
server still runs the version the pin was made for, and fail clearly when it
doesn't. Design: [plugins.md](plugins.md) (section "Versions", decisions 9
and 11). Read [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) first, and
[ssh-plugins.md](ssh-plugins.md) for the settings and SSH code this builds on
(`src/plugins/settings.ts`, `src/plugins/ssh.ts`). Comes after
[ssh-hosts.md](ssh-hosts.md), which adds `dh plugin use` (for the default
SSH host) and `--ssh-host`; this plan added the version to `use`, which
[hosts-and-pins.md](hosts-and-pins.md) later removed (along with the
pin-missing state). The contract below is as amended there. The design
doc: [dh plugins: design](https://claude.ai/artifact/QbDVebtCpHu33kaHU8nAEV).

Status: implemented, and tested against the fake `ssh`; the checks against
a real DHE server are still to do.

Out of scope: `dh plugin create --update` (regenerating a spec and keeping
hand edits; [create-update.md](create-update.md)), `dh plugin add`/`remove`, bundled specs, telling
a tool changed without a version (a fingerprint of the binary).

## Model

- A spec is for one version of its tool (`tool.version`), or for any
  version (unversioned).
- The spec says how to find its tool's version (`tool.versionFrom`). Without
  it, `dh` can't, and the plugin works as today: one spec, no pin, no check.
- The **pin** says which spec to use, and records the tool version (and, for
  the `path` form, the tool's real path) it was confirmed against. It's a
  server setting for `ssh` plugins and a machine setting for `local` ones.
- Every run checks the tool still matches the pin. A change to a version
  with an exact spec switches to it; a change to a version without one is an
  error. `dh` never picks a spec by guessing.

## Contract

### Spec: `tool.version` and `tool.versionFrom`

- `tool.version`: optional string (exists). The version the spec is for.
- `tool.versionFrom`: optional, one of:
  - `{ "path": "<regex>" }`: matched against the tool's real path
    (symlinks followed); capture group 1 is the version.
  - `{ "run": ["<arg>", …], "match": "<regex>" }`: runs the tool (with the
    prefix in effect) with those arguments, stdin ignored, stdout and stderr
    captured together as for help runs; capture group 1 of the first match
    in that text, trimmed, is the version. Non-zero exits are ignored; no
    match means unknown.
- `<regex>`: a JavaScript `RegExp` source, no flags. `parseSpec` rejects
  (`plugin_invalid`) one that doesn't compile, or has no capture group
  (`new RegExp(`${source}|`).exec("")!.length < 2`), and a `run` that isn't a
  non-empty list of strings: `` "tool.versionFrom": <reason> ``.
- In JSON the DHE form is `{ "path": "/illumon-db-([^/]+)/" }`, the regex
  `versionOf` in `src/plugins/scaffold.ts` uses today.

### Specs side by side

All specs stay in `<config dir>/plugins/*.json`. A spec's identity is
`name` plus `tool.version`; the file name doesn't matter to the loader.

- Several specs may share a `name` if their `tool.version`s differ; at most
  one per name is unversioned.
- Two specs with the same identity are both rejected, `plugin_invalid`,
  reason `` "<name>" version <version> is in <file1> and <file2> `` (or
  `` "<name>" without a version is in <file1> and <file2> ``). Other specs
  with that name still load.
- Built-in and reserved names are rejected as today.
- `versionFrom` for a name: the newest spec's that has one (order below).
  Specs of one tool are expected to agree.

### Version order

For "newest" and sorting only (selection never uses it). Parse
`/^(\d+)\.(\d+)(?:\.(\d+))?(.*)$/`; compare the three numbers numerically (a
missing third is `-1`), then the rest as plain strings (`<`/`>`). A version
that doesn't parse sorts below every one that does; those compare as plain
strings. Unversioned sorts lowest.

### The pin

Machine (`plugins.<name>`, `local`) or server
(`servers.<origin>.plugins.<name>`, `ssh`), the same fields:

```json
{
  "version": "2026.01.1g0",
  "detected": "2026.01.1g0",
  "realPath": "/usr/illumon/illumon-db-2026.01.1g0/bin/dhctl",
  "checkedAt": "2026-11-04T17:03:00.000Z"
}
```

- `version`: the spec to use; `null` for the unversioned spec.
- `detected`: the tool version the pin was confirmed against.
- `realPath`: `path` form only, the real path it was confirmed against.
- `checkedAt`: `run` form only, `new Date().toISOString()` of the last check.
- Written with `Store.update`. `PluginSettings` and `ServerPluginSettings`
  in `src/auth/store.ts` get these fields; `ServerPluginSettings.version`
  (unused so far) becomes this `version`.

### Detecting

"Detect" means: the real path (`path` form: local `fs.realpath`, SSH remote
`readlink -f '<tool.path>'` on the run's SSH host (`--ssh-host`, else the
default host; see ssh-hosts.md) with `-T -o BatchMode=yes` and the
multiplexing rule, output trimmed) and the version from it; or the `run` form's run
(locally, or over SSH like a help run). Any failure: version unknown. The
tool path is the pinned spec's `tool.path`, else the newest spec's (for
`local`, the machine's `path` if set).

### Choosing the spec at startup

The active profile as in [ssh-plugins.md](ssh-plugins.md) "Which server",
except an unknown name counts as no profile (startup never fails).
`--profile` is read from the arguments as `routePluginArgs` keeps it:
`--profile <v>` or `--profile=<v>` anywhere before `--`. The **invoked
plugin**: the first argument before `--` that isn't a flag or the value of
`--profile`, `--ssh-host`, `-o` or `--output`, if it names a plugin.

For each plugin name:

1. No spec has `versionFrom`: one spec → use it, no pin, no check (today's
   behavior). Several → **ambiguous** (below), unless a pin selects one.
2. A pin (machine's for `local`; server's for `ssh`, which needs a profile;
   without one, step 4's "not detected" applies) whose `version` names a
   loaded spec; a pin whose spec is gone counts as no pin (steps 3 and 4).
   A pin whose `detected` differs from its versioned spec's version
   (written by the old `use <name> <version>`) is a mismatch: the next
   check handles it as **a change**.
   - `local` with the `path` form: detect now (free). Real path equals
     `realPath` → use the pinned spec. Otherwise apply **a change** below;
   - otherwise use the pinned spec (`ssh` is checked when it runs; the
     `run` form when its check is due, below).
3. No pin, `local` with the `path` form: detect now, then **first use**.
4. No pin otherwise (`ssh`, or the `run` form): if this is the invoked
   plugin, detect now (one SSH run, or one local tool run), then **first
   use**. If not, the plugin is **not detected**.

**First use** with a detected version `D`: the spec with `tool.version` `D`,
else the unversioned spec; write the pin
(`{version, detected: D, realPath|checkedAt}`) and use it. Neither exists →
**no spec for version**. `D` unknown → **not detected** for `ssh`/`run`, and
for `local` `path` form: one spec → use it, no pin; several → ambiguous.

### Checking on every run

- `local`, `path` form: at startup (step 2).
- `ssh`, `path` form, without a terminal (`-T`): inside the same SSH session.
  The remote command becomes `'sh' '-c' '<script>'`, where `<script>` (its
  words quoted as in [ssh-plugins.md](ssh-plugins.md), then the whole
  script quoted once more) is:

  ```sh
  p=$(readlink -f '<tool.path>'); [ "$p" = '<realPath>' ] || { printf 'dh-version-check:%s\n' "$p" >&2; exit 254; }; exec <prefix words> '<tool.path>' <argv words> <user words>
  ```

  ssh exit 254 with a stderr line starting `dh-version-check:` is a change,
  with that line's rest as the new real path. That line is never written to
  `dh`'s stderr (stderr is passed through as it arrives, so hold back a
  partial line until it's known not to be the marker). Exit 254 without the
  marker is the tool's own exit.
- `ssh`, `path` form, with a terminal (`-t`): also inside the same session,
  but silent: with `-t`, remote stderr arrives on stdout, so the marker
  can't be separated, and the script on a mismatch only exits 254:

  ```sh
  p=$(readlink -f '<tool.path>'); [ "$p" = '<realPath>' ] || exit 254; exec <prefix words> '<tool.path>' <argv words> <user words>
  ```

  ssh exit 254 then means "maybe a change": `dh` makes one separate detect
  run. A different real path is a change, handled as above; the same one
  means the 254 was the tool's own exit, reported as `tool_failed` with
  `toolExit: 254`. So an unchanged server costs nothing extra in a terminal
  either; the extra run happens only on a change or a tool's own 254.
  (Measured before this: a separate detect run before every terminal
  command cost about 0.5 s.)
- `run` form (`local` or `ssh`): when `checkedAt` is missing or 24 hours old
  or more, a detect run before the command; the result compared to
  `detected`, and `checkedAt` updated.

### A change

New real path or version `D'` against the pin:

- `D'` equals `detected` (e.g. reinstalled elsewhere): update `realPath` /
  `checkedAt`, keep the spec.
- A spec with `tool.version` `D'` exists: re-pin to it
  (`{version: D', detected: D', …}`), and print to stderr (always stderr):
  `` ! <name> on <where> is now <D'>; using its spec. ``
- Otherwise **no spec for version** (the pin is left as it was).
- At startup (`local`) the result applies directly. Over SSH the tool
  didn't run: after updating the pin, `dh` runs itself again with the same
  arguments (as `ensureCa` in `src/auth/session.ts` does), with
  `DH_PLUGIN_RECHECKED=1` set; a second change in that run is
  **no spec for version**. Gotcha: ssh may already have read and forwarded
  some of a piped stdin, so when stdin is a pipe or a file
  (`fstatSync(0).isFIFO() || .isFile()`), don't re-run: fail with
  `plugin_outdated`, `` <name> on <where> is now <D'>; switched to its spec. Run the command again. ``
  (no hint).

### Unavailable plugins

Ambiguous, no spec for version, and not detected all register
the noun **bare**: the newest spec's `description`, no subcommands, no
options. For all but not detected, `dh <name>`, `dh <name> -h|--help` and
`dh <name> <anything>` fail as below, and nothing runs the tool. Gotcha:
commander answers `-h`/`--help` itself before any `run`, and
`routePluginArgs` keeps them as `dh`'s; for a bare noun they must reach the
command (or be intercepted first) so they give the error.

| State | Code | Message | Hint |
| ----- | ---- | ------- | ---- |
| ambiguous | `usage` | `` <name> has several specs and no "tool.versionFrom" to choose between them `` | `` Keep one spec for <name>, or add "tool.versionFrom" so dh can tell them apart. `` |
| no spec for version | `plugin_outdated` | `` <name> on <where> is now <D>, but its spec is for <pinned version> `` (first use: `` <name> on <where> is <D>, and no spec is for that version ``) | `` Generate one: `dh plugin create <tool.path> --name <name>[ --exec ssh]`. `` |
| can't tell the version | `plugin_outdated` | `` Can't tell which version of <tool.path> <name> runs on <where> `` | `` Check it: `dh plugin check <name>`. `` |

`<where>`: `<origin>` for `ssh`, `this machine` for `local`. New error code
in `src/errors.ts`: `plugin_outdated`, exit 2, description
`No plugin spec for the tool's version`. Its `-o json` error adds
`"detected"`, `"pinned"` (`null` on first use) and, for `ssh`, `"host"`.

**Not detected** isn't an error: it only arises when the plugin isn't the
invoked one, so it only shows in listings (root help, `dh agents`). Running
`dh <name> …` detects instead.

### `dh plugin use <name> [--ssh-host <host>]`

Only sets the default SSH host, as in [ssh-hosts.md](ssh-hosts.md). The
`[version]` this plan added was removed by
[hosts-and-pins.md](hosts-and-pins.md): a second positional argument is
`usage`, `` Unknown command or extra argument "<arg>" ``. The pin's
`version`/`detected` are written only by detection (first use, `create`, a
run, `dh plugin check`).

### `dh plugin create`

- When the tool's real path matches `/illumon-db-([^/]+)/`, the spec gets
  `"versionFrom": { "path": "/illumon-db-([^/]+)/" }` and `tool.version`
  (as now). Otherwise neither, and the human output adds
  `` ! No version found for <tool>; add "tool.versionFrom" to the spec to pin and check versions. ``
  `create` never runs the tool except for help.
- The file is `<dir>/<name>@<version>.json` with a version, else
  `<dir>/<name>.json`.
- Only a loaded spec with the same identity conflicts: `usage`,
  `` A <name> spec for version <version> exists: <file> `` (or
  `` A <name> spec without a version exists: <file> ``), hint
  `Pass --force to replace it, or --name to choose another name.` `--force`
  replaces that file in place.
- With a version, pins the new spec (`local`: machine; `--exec ssh`: the
  profile's server), with `detected` and `realPath`.
- Human output adds `  Pinned <name> to <version> on <where>` when it pinned; JSON adds
  `"pinned": true | false`.

### `dh plugin list`

One entry per name. Human, after the line from
[ssh-plugins.md](ssh-plugins.md), whose ` (<version>)` is the spec in use:

- the state instead, when unavailable: ` (ambiguous)`,
  ` (now <D>, no spec)`, ` (version not checked)`;
- only for ` (ambiguous)`, a next line `        specs: <v1>, <v2>` (8
  spaces), newest first, `no version` for the unversioned one: the fix is to
  keep one.

(Changed after a review of the demo: the line no longer says `, pinned`,
since the version shown is the pinned one; and other specs aren't listed,
since no command chooses among them. `-o json` still has `pin` and
`versions`.)

JSON entries add `"pin"` (the pin object or `null`), `"state"` (`"ok"`,
`"ambiguous"`, `"no_spec_for_version"`, `"not_detected"`),
and `"versions"`: every spec for the name, newest first,
`[{ "version": "…" | null, "file": "…" }]`. `file` and `version` are the
spec in use (`null` when unavailable). `list` detects nothing over SSH.

### `dh plugin check <name>`

Unavailable plugins fail with their error first, except **no spec for
version** and **not detected**, which `check` can resolve. The `version`
step (on the default SSH host for `ssh`) detects afresh and never fails. If
there's no pin yet, or the pin no longer matches what it detected, it
applies **first use** or **a change** (above): it writes the pin when a
spec matches, and otherwise leaves it. Detail: the detected version or
`unknown`, then ` (pinned now)` when it wrote the pin, or
` (pinned for <detected>; no spec for this version)` when the pin still
doesn't match.

### `dh agents -o json`

A plugin noun's marker adds `"pluginVersion"` (the spec in use's
`tool.version`, omitted when unversioned or unavailable) and, when
unavailable, `"pluginState"` (as `list`'s `state`) with `"commands": []`.
For `not_detected` the noun's description gets
`` (run `dh <name> --help` to load its commands) `` appended.

## Checklist

- [x] `tool.versionFrom` in `src/plugins/spec.ts`, validated
- [x] Pin fields in `src/auth/store.ts`
- [x] Version order (`src/plugins/versions.ts`, new)
- [x] Loader: identity by name and version, duplicate rule and reasons
- [x] Detecting: `path` and `run` forms, local and SSH
- [x] Startup: profile and invoked plugin from args; steps 1–4; first use writes the pin
- [x] Bare nouns for unavailable plugins; their `--help` gives the error
- [x] `plugin_outdated` error code
- [x] In-session check script, marker handling (not passed through), the silent check with `-t` (one detect run only after exit 254), `run`-form daily check
- [x] A change: same version, switch with notice, re-run (`DH_PLUGIN_RECHECKED`), piped-stdin error
- [x] `dh plugin use <name> <version>` (added to ssh-hosts.md's `use`; removed by hosts-and-pins.md)
- [x] `dh plugin create`: `versionFrom`, file name, identity conflict, pin
- [x] `dh plugin list` and `dh plugin check` additions
- [x] `dh agents` marker fields
- [x] Tests for every automated Validation item
- [x] `plans/plugins.md`: status, "Versions" section, Commands table (`use`), decision 11 match what was built
- [x] `plans/plugins-poc-test.md`: a versions section (two versions, an upgrade)

## Validation

Automated, with the fake `ssh` (`tests/fixtures/plugins/fake-ssh`) and
versioned installs built as in [ssh-plugins.md](ssh-plugins.md) "Test
fixture": `illumon-db-2026.01.1g0` and `illumon-db-2026.01.2g0`, with
`latest` pointing at one. An "upgrade" is repointing `latest`.

- [x] `bun run check` and `bun test` pass; every existing plugin test passes unchanged except the duplicate-name test, which now uses identical versions
- [x] `parseSpec`: both `versionFrom` forms accepted; a regex that doesn't compile, one without a group, and an empty `run` rejected with `plugin_invalid`
- [x] Version order: `2026.01.10g0` > `2026.01.9g0` > `2026.01.1g0` > `2025.02.9g0` > `2025.02` > `nightly` > unversioned
- [x] `ctl@2026.01.1g0.json` and `ctl@2026.01.2g0.json` both load as one `dh ctl`; two specs with the same version are both rejected naming both files while the other version loads
- [x] A spec without `versionFrom`, alone: no pin written, no extra ssh runs (fake ssh log has exactly the command), behaves as before
- [x] Local, no pin, `latest` → `1g0`: the first `dh ctl …` writes `plugins.ctl` pin `{version: "2026.01.1g0", detected, realPath}` and uses that spec (a command only in it appears in `dh ctl --help`)
- [x] Local upgrade to `2g0`, both specs present: next run uses the `2g0` spec, re-pins, stderr has `! ctl on this machine is now 2026.01.2g0; using its spec.`
- [x] Local upgrade to `3g0` (no spec): `dh ctl …` and `dh ctl --help` exit 2, `plugin_outdated`, the hint names `dh plugin create` (and, before hosts-and-pins.md, `dh plugin use ctl 2026.01.1g0`); nothing runs; the pin is unchanged
- [x] Ssh, no pin: `dh ctl intraday rescan` makes one `readlink` run, then the command; the server's pin is written; `dh agents -o json` before that shows `"pluginState": "not_detected"` and makes no ssh run
- [x] Ssh, pinned, no change: one ssh run per command (log), its remote command is `'sh' '-c' …` containing the `readlink` comparison; output and exit codes are the tool's (exit 3 → `tool_failed`, `toolExit: 3`; quoting test from ssh-plugins.md passes through the script)
- [x] Ssh upgrade to `2g0` with both specs: the command still succeeds (dh re-runs itself), the pin is updated, the notice is on stderr once, `dh-version-check:` never appears in output
- [x] Ssh upgrade to `3g0`: `plugin_outdated`, exit 2, with `detected`, `pinned`, `host` in `-o json`; the tool didn't run
- [x] Ssh upgrade with stdin piped (`echo x | dh ctl …`) and a `2g0` spec: re-pinned, fails with "Run the command again", exit 2
- [x] A tool that exits 254 itself (no marker) → `tool_failed`, `toolExit: 254`
- [x] `run` form: a tool printing `version 1.2.3` for `--version`, spec `versionFrom: {run: ["--version"], match: "version (\\S+)"}`: first use pins `1.2.3`; a second run within 24 h makes no version run; with `checkedAt` 25 h old it does
- [x] Two profiles on two servers pin independently (`--profile p2`)
- [x] Ambiguous (two specs, no `versionFrom`, no pin) → `usage`; `dh plugin use ctl 2026.01.1g0` fixes it (superseded by hosts-and-pins.md: removing a spec fixes it)
- [x] Pin-missing → `usage`; `dh ctl --help` and `-h` give it too; `dh agents` has `"pluginState": "pin_missing"`, `"commands": []` (superseded by hosts-and-pins.md: it's first use)
- [x] (Superseded by hosts-and-pins.md, which removed `use <version>`.) `dh plugin use ctl 2026.01.1g0` after an upgrade to `3g0`: pins with `detected: "2026.01.3g0"`; next run works without error; `use ctl 9.9` → `usage` listing versions; `use crcat 2026.01.1g0 --ssh-host dis-2` sets both the pin and `defaultSshHost`
- [x] `dh plugin check ctl` after an upgrade to `2g0` with a `2g0` spec: the `version` step re-pins (`(pinned now)`); after `3g0` with no spec: `(pinned for 2026.01.2g0; no spec for this version)` and the pin unchanged
- [x] `dh plugin create` on `latest` → `ctl@2026.01.1g0.json` with `versionFrom`, pinned; after upgrading to `2g0`, the same command without `--force` writes `ctl@2026.01.2g0.json` beside it and re-pins; again → `usage` (exists); a tool not under `illumon-db-` → `ctl.json`, no `versionFrom`, the `! No version found` line
- [x] `dh plugin list`: `(2026.01.1g0)`, the state texts, the `specs:` line for ambiguous only; JSON `pin`, `state`, `versions`

Against a real DHE server:

- [x] Over SSH, with a pin, time one `dh ctl` command against the times recorded in plugins.md (the in-session check should add a few ms, not a round trip). Result, same day, reused connection: 3.78 s with a spec without `versionFrom` (no check); 3.83 s pinned, without a terminal (`</dev/null`, the in-session check: free); 4.33 s pinned, in a terminal (`-t`, the separate check run first: about 0.5 s per command). After the silent in-session check with `-t`: 3.86 s in a terminal. Detection and pinning on first use (`dh plugin check ctl`, `(pinned now)`) worked
- [ ] Fake an upgrade on a test server (repoint a symlink the spec's `tool.path` goes through, or edit `realPath` in the pin) and check the notice and the `plugin_outdated` error

## Relevant Files

- `src/plugins/spec.ts` — `tool.versionFrom`
- `src/auth/store.ts` — pin fields on `PluginSettings` and `ServerPluginSettings`
- `src/errors.ts` — `plugin_outdated`
- `src/plugins/versions.ts` (new) — version order, detecting, choosing, a change
- `src/plugins/load.ts` — identity by name and version, duplicate rule
- `src/plugins/settings.ts` — pins in effect; active profile at startup without throwing
- `src/plugins/scaffold.ts` — `versionOf` replaced by `versionFrom`; spec gets `versionFrom`
- `src/plugins/ssh.ts` — in-session check script, marker handling in stderr pass-through
- `src/plugins/run.ts` — local `run`-form detect
- `src/auth/session.ts` — `ensureCa`'s re-run reused (factored out if needed)
- `src/main.ts` — choosing at startup; profile and invoked plugin from args
- `src/cli/command.ts` — plugin marker carries version and state
- `src/cli/plugin-args.ts` — `-h`/`--help` reach a bare noun
- `src/commands/agents.ts` — `pluginVersion`, `pluginState`
- `src/commands/plugin/index.ts` — `PluginLoad` shape; `list`; register `use`
- `src/commands/plugin/use.ts` — add `<version>` (the command comes from ssh-hosts.md)
- `src/commands/plugin/tool.ts` — bare nouns; checks and re-run around SSH runs
- `src/commands/plugin/create.ts` — `versionFrom`, file name, identity conflict, pin
- `src/commands/plugin/questions.ts` — a used name alone is no longer a conflict
- `src/commands/plugin/check.ts` — unavailable states; `version` step detail
- `src/commands/plugin/set.ts` — `askLoadedPlugin` returns the spec in use
- `tests/plugin_versions.test.ts` (new)
- `tests/plugins.test.ts` — duplicate-name test
- `tests/plugin_create.test.ts` — file names, `versionFrom`
- `plans/plugins.md`, `plans/plugins-poc-test.md`
