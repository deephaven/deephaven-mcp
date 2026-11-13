# `dh` plugins PoC: install and test

Proves a `dh` plugin wrapping `dhctl` on a DHE server, with `dh` installed on
the server (sections 1–4), from a laptop over SSH (section 5), and with
specs for several versions (section 6). Design: [plugins.md](plugins.md).

## 1. Build

From the repo root, on any OS (Bun cross-compiles):

```bash
SERVER=<server>
ssh -n $SERVER uname -m      # x86_64 → linux-x64, aarch64 → linux-arm64
bun install
bun run build --target linux-x64
```

Gives `dist/dh-linux-x64`.

## 2. Copy to the server

```bash
ssh -n $SERVER 'mkdir -p ~/.local/bin ~/.deephaven/cli/plugins'
scp dist/dh-linux-x64 $SERVER:~/.local/bin/dh
scp examples/plugins/ctl.json $SERVER:~/.deephaven/cli/plugins/ctl.json
```

## 3. Install (server)

```bash
chmod +x ~/.local/bin/dh
grep -q '.local/bin' ~/.bashrc || echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
grep -q DH_AUTO_UPDATE ~/.bashrc || echo 'export DH_AUTO_UPDATE=off' >> ~/.bashrc
source ~/.bashrc
ls -l /usr/illumon/latest/bin/dhctl   # the spec's tool.path
```

`DH_AUTO_UPDATE=off` stops `dh` replacing itself with the released build,
which has no plugins.

If you aren't `irisadmin`, have `dh` run `dhctl` as `irisadmin` (only if
`~/.deephaven/cli/config.json` doesn't exist yet; otherwise add the `plugins`
key to it):

```bash
cat > ~/.deephaven/cli/config.json <<'EOF'
{ "version": 1, "servers": {}, "profiles": {},
  "plugins": { "ctl": { "prefix": ["sudo", "-u", "irisadmin"] } } }
EOF
```

## 4. Test (server)

| # | Command | Expect |
|---|---|---|
| 1 | `dh plugin list` | `ctl   local: [sudo -u irisadmin] /usr/illumon/latest/bin/dhctl` |
| 2 | `dh ctl --help` | `intraday` and `metadata` groups |
| 3 | `dh ctl intraday rescan --help` | `dh` help with dhctl's flags under "Options:" (`-t, --table-name <arg>`, `--pwfile <arg>`, …) |
| 4 | `dh ctl intraday rescan -- --help` | dhctl's own help |
| 5 | `dh ctl metadata list --table-name DbInternal` | dhctl's listing (read-only) |
| 6 | `dh -o json ctl metadata list --table-name DbInternal` | One JSON line on stdout: `{"stdout":"…"}` |
| 7 | `dh -o json ctl metadata list --bogus; echo "exit=$?"` | dhctl's error, then `{"error":"dhctl exited with N","code":"tool_failed","exit":6,"toolExit":N}` on stderr; `exit=6` |
| 8 | `dh agents \| grep ctl` | `dh ctl intraday rescan [args...]` etc. |
| 9 | `dh ctl intraday rescan --table-name DbInternal.ProcessEventLog` | Rescan runs (asks each DIS to rescan; harmless) |
| 10 | Ctrl-C during a long command | dhctl stops; prompt returns |

### 11. Tool path set wrong for this machine

`config.json` can override where this machine's `dhctl` is
(`plugins.ctl.path`). Pointed at nothing, `dh` should stop before running
anything, with a clear error:

```bash
cd ~/.deephaven/cli
cp config.json config.json.bak 2>/dev/null
cat > config.json <<'EOF'
{ "version": 1, "servers": {}, "profiles": {},
  "plugins": { "ctl": { "path": "/nope/dhctl" } } }
EOF
dh ctl intraday rescan; echo "exit=$?"
mv config.json.bak config.json 2>/dev/null || rm config.json
```

Expect `✖ Tool not found: /nope/dhctl`, a hint naming
`dh plugin set ctl --path <path>`, and `exit=6`.

### 12–13. Plugins can't take dh's names

A plugin named after a built-in (`auth`) or a reserved name (`config`) is
rejected; the real commands are unaffected:

```bash
cd ~/.deephaven/cli/plugins
sed 's/"name": "ctl"/"name": "auth"/' ctl.json > auth.json
sed 's/"name": "ctl"/"name": "config"/' ctl.json > config.json
dh plugin list
dh auth
rm auth.json config.json
```

Expect `dh plugin list` to show `ctl` as normal, then
`✖ …/auth.json: "auth" is a built-in command` and
`✖ …/config.json: "config" is reserved for dh`. `dh auth` still shows dh's
own profiles (or "No profiles").

### 14. Only `-o` and `-h` are dh's on plugin commands

```bash
dh ctl intraday rescan --help | grep -A3 "Global Options"   # only -o, --output
dh ctl intraday rescan --profile x; echo "exit=$?"           # --profile reaches dhctl
dh --profile x ctl intraday rescan; echo "exit=$?"           # before the noun: dh's
```

Expect: help lists only `-o, --output` under Global Options. The second
command shows dhctl's own complaint about an unknown `--profile`, not dh's; if
dhctl exits non-zero for it, `exit=6`.
The third prints `✖ --profile doesn't apply to …` and `exit=2`.

### 15. Generate a spec from `--help`

```bash
dh plugin create /usr/illumon/latest/bin/dhctl --name ctl2
dh ctl2 --help
dh ctl2 metadata list --help
rm ~/.deephaven/cli/plugins/ctl2.json
```

It first lists what `dhctl --help` shows and asks whether to run each
subcommand's help too; answer yes (or pass `--recurse`). Expect `✔ Wrote
…/ctl2.json` with a list of commands needing descriptions, then the
`intraday`, `metadata` and `logger` groups, with `logger generate` under
`logger`. With `--no-recurse` instead, only `dhctl --help` runs, and
`intraday`, `metadata` and `logger` become single commands.

Also try it on another tool, e.g. `dh plugin create /usr/illumon/latest/bin/dhconfig`.
Its default name `config` is reserved, so it asks for one (`--name cluster`).

## 5. Test over SSH (laptop → server)

`dh` on your laptop runs `dhctl` on the server over SSH. Needs `ssh $SERVER`
to work without a password (keys or agent; `~/.ssh/config` aliases are fine)
and a `dh` profile for the server. On the laptop, from the repo root:

```bash
bun run build                        # dist/dh for this machine
alias dh="$PWD/dist/dh"
export DH_AUTO_UPDATE=off
dh auth login https://$SERVER:8000   # if there's no profile for it yet
mkdir -p ~/.deephaven/cli/plugins
cp examples/plugins/ctl.json ~/.deephaven/cli/plugins/
dh plugin set ctl --exec ssh --ssh-host $SERVER
```

Add `--prefix "sudo -n -u irisadmin"` to the last line if you aren't
`irisadmin` on the server (needs NOPASSWD sudo; see test 19 otherwise).

| # | Command | Expect |
|---|---|---|
| 16 | `dh plugin list` | `ctl   ssh: <server> [sudo -n -u irisadmin] /usr/illumon/latest/bin/dhctl` |
| 17 | `dh plugin check ctl` | `✔` for `host`, `connect` (note the ms), `tool`, `prefix` (if set) and `version` (the `illumon-db-…` version) |
| 18 | `dh ctl metadata list --table-name DbInternal` | The same listing as test 5. Time it with `time`, twice: the second run reuses the SSH connection. Compare with `time ssh $SERVER /usr/illumon/latest/bin/dhctl metadata list --table-name DbInternal` |
| 19 | `dh plugin set ctl --prefix "sudo -u irisadmin"`, then `dh ctl metadata list --table-name DbInternal` in a terminal, then the same with `--no-prompt` | First: sudo asks for your password. With `--no-prompt`: `✖ dhctl exited with 1` and a hint about `sudo -n`. Set the prefix back afterwards |
| 20 | `dh plugin set ctl --ssh-host nosuchhost`, then `dh -o json ctl metadata list --table-name DbInternal; echo "exit=$?"` | `remote_unreachable`, `"sshHost":"nosuchhost"`, `exit=5`. Set the host back afterwards |
| 21 | Add a keyless alias to `~/.ssh/config` (`Host dh-nokey`, `HostName <server>`, `IdentityFile ~/.ssh/dh-nokey-missing` (a file that doesn't exist; `/dev/null` works too but adds `Load key "/dev/null": invalid format`), `IdentitiesOnly yes`), `dh plugin set ctl --ssh-host dh-nokey`, then `SSH_AUTH_SOCK= dh --no-prompt ctl metadata list --table-name DbInternal; echo "exit=$?"` | `ssh_auth_failed` with the `ssh-copy-id` hint, `exit=4`, within `DH_TIMEOUT` (30 s), with no password prompt. Set the host back afterwards |
| 22 | `dh plugin create /usr/illumon/latest/bin/dhctl --name ctl3 --exec ssh --recurse` | `✔ Wrote …/ctl3.json (from N help runs over SSH: <server>)`. `"tool.version"` in the file is set. Compare with a spec generated on the server (test 15): `diff <(jq 'del(.tool.version)' ctl3.json) <(jq … ctl2.json)` should show only the name |
| 23 | Specs for `iriscat` and `crcat` (`dh plugin create /usr/illumon/latest/bin/iriscat --exec ssh`), then a read-only command of each | They run over SSH; `crcat` needs `JAVA_HOME`, so note whether a non-interactive SSH login has it (a `--prefix "env JAVA_HOME=…"` fixes it) |

## 6. Versions (laptop → server)

Specs for two versions side by side, pinned per server, and an upgrade. Use a
test server where you can repoint a symlink the tool's path goes through (or
fake an upgrade by editing `realPath` in the pin).

```bash
rm -f ~/.deephaven/cli/plugins/ctl*.json
dh plugin create /usr/illumon/latest/bin/dhctl --exec ssh --recurse
dh plugin list
```

| # | Command | Expect |
|---|---|---|
| 24 | The `create` above | `✔ Wrote …/ctl@<version>.json` and `  Pinned ctl to <version> on https://<server>:8000`. The file has `"versionFrom": {"path": "/illumon-db-([^/]+)/"}`; `config.json` has the pin under `servers.<origin>.plugins.ctl` |
| 25 | `time dh ctl metadata list --table-name DbInternal`, twice | As test 18; the second run's time within a few ms of test 18's (the version check runs in the same SSH session) |
| 26 | Copy the spec to `ctl@<other version>.json` and set its `tool.version` to that version (a second version side by side), then `dh plugin list` | One `ctl` line, `(<version>, pinned)`, then `        also: <other version>` |
| 27 | On the server, point `latest` at another install (or set `realPath` in the pin to `/nowhere`), then `dh ctl metadata list --table-name DbInternal` | With a spec for the new version: `! ctl on <origin> is now <version>; using its spec.` on stderr, then the listing. Without one: `✖ ctl on <origin> is now <version>, but its spec is for <old>`, exit 2, a hint naming `dh plugin create` and `dh plugin use` |
| 28 | `dh plugin use ctl <old version>`, then the command again | `✔ ctl uses <old version> on <origin> (tool: <new version>)`; the command runs. Point `latest` back afterwards |

## 7. Report back

- Output of tests 5–7.
- Tests 17–18: the `connect` ms, and the times with and without connection
  reuse, next to the time `dhctl` itself takes (JVM startup).
- Anything where `dh ctl …` over SSH behaves differently from running it on
  the server.
- Anything where `dh ctl …` behaves differently from running `dhctl …` directly.
- Tests 25 and 27: the times, and the notice or error as printed.

## Clean up (server)

```bash
rm -rf ~/.local/bin/dh ~/.deephaven/cli
sed -i '/DH_AUTO_UPDATE=off/d' ~/.bashrc
```

On the laptop: `rm ~/.deephaven/cli/plugins/{ctl,ctl3,iriscat,crcat}*.json`,
and remove `"plugins"` from `~/.deephaven/cli/config.json` (top level and
under the server).
