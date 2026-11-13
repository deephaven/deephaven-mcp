# Cluster-provided plugins

Revision of [plugins.md](plugins.md) after the first demo. Three changes:

1. **Clusters provide plugins; `dh` discovers and caches them.** Specs no
   longer live in `<config dir>/plugins/` and get checked against the
   server. Each DHE install carries the plugins that match its tools and
   version, and `dh` discovers each kind of plugin over the channel that
   runs it (see [Two channels](#two-channels)).
2. **Three execution kinds:** a tool on this machine (`local`), a tool on a
   cluster node (`ssh`), and a TypeScript module that uses the JS API
   (`js`). The last one is new and designed below.
3. **Plugin creation is internal tooling, not a `dh` command.** Scaffolding
   from `--help` and the new build step move into a separate, unshipped
   toolkit. A user-facing subset, for `js` plugins only, is an open question.

Status: design, not started. Implementation is planned in
[steel-thread.md](steel-thread.md), which also lists where everything lives
(this repo, the Iris branch, the devcontainer). Everything in plugins.md that isn't contradicted
here still applies: command trees from data, flags passed through, the global
flag routing, SSH execution and its errors, `-o json`, and how plugins appear
in `dh agents`.

## Two channels

Users differ in what they can reach. An admin may have SSH to the cluster's
nodes, and needs their tools most when the web server is down. Most users
have no shell account, but can sign in, and some cluster operations are
available to them through the JS API. So each kind of plugin is discovered
over the channel that runs it, and a user discovers what they can use:

| Kind   | Discovered                                      | Needs                         | Runs                                  |
| ------ | ----------------------------------------------- | ----------------------------- | ------------------------------------- |
| `js`   | Over the web, from the profile's server         | A sign-in, to run them; the fetch itself is anonymous | In `dh`, through the JS API           |
| `shell` | From disk on a node, or over SSH from elsewhere | Being on a node, or SSH to one | The tool, locally or over SSH        |

- The channels are independent. A user without SSH gets the `js` plugins; an
  admin on a node with the web server down gets the `shell` plugins; an admin
  with both gets both.
- `shell` discovery never uses the web server, so admin commands keep
  working when it's down.
- `js` discovery uses the web server, which `js` plugins need anyway to run.
  The fetch is anonymous, as the web UI's own plugins are: on DHE,
  `/iriside/js-plugins/manifest.json` is served without a login (confirmed
  on a server, 2026-11-06); in Community's web UI, `PluginsBootstrap` loads
  `js-plugins/manifest.json` before `AuthBootstrap`, with a plain `fetch`.
  Only running a `js` command signs in.
- A `js` plugin can be limited to some groups (`groups`), so users don't see
  commands they can't use. That's a convenience; the server's ACLs still
  decide what a command can do.

## What this replaces

| Today (plugins.md and sub-plans)                                    | Now                                                                                                                                 |
| ------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Spec files in `<config dir>/plugins/*.json`, added by the user      | The bundle in the cluster's install, per channel: `js/` fetched over the web, `shell/` read on a node or copied over SSH. `DH_PLUGIN_BUNDLE` points at a directory for development (see [Development](#development-override)) |
| `ctl@<version>.json` side by side, `tool.versionFrom`, a pin per server, a version check on every run, `plugin_outdated` ([versions.md](versions.md)) | Removed. The install carries the specs for its own tools. An upgrade changes the bundle's `revision`, which `dh` notices (see [Noticing changes](#noticing-changes)) |
| The in-session `readlink -f` check before each SSH run              | The same mechanism, comparing the `shell/` bundle's `revision` file instead of the tool's real path                                 |
| `exec` machine setting, `--exec ssh` on `create`/`add`/`set`        | Removed. Where a `shell` plugin runs follows from where it was discovered (see [Execution](#execution)); there is no `exec` setting or `--exec` flag |
| One top-level noun per plugin, with `promote` reserved for later (plugins.md decision 2) | A spec may own several top-level nouns (`nouns`), each its own `dh` noun. See [Several nouns from one plugin](#several-nouns-from-one-plugin) |
| `dh plugin create`, `create --update` ([create-update.md](create-update.md)), `dh plugin add`/`remove` | Removed from `dh`. Scaffolding moves to the internal toolkit ([Authoring](#authoring-internal)). No `add`/`remove`: the install decides |
| Per-plugin `sshHosts`/`defaultSshHost` per server          | Per server (`servers.<origin>.ssh`), used for discovery and every `shell` plugin; per-plugin values remain as overrides, e.g. `crcat` on the DIS nodes |
| `prefix` settings (machine, server, per plugin) and `dh plugin set --prefix` | Removed. The spec says which service account a tool runs as (`tool.runAs`), and which of its flags mean the user brings their own identity (see [Running as a service account](#running-as-a-service-account)) |
| The login SSH offer for plugins whose tool isn't installed here ([laptop-setup.md](laptop-setup.md)) | Reworded: login offers SSH setup when the server says it has `shell` plugins (see [SSH setup](#ssh-setup))                          |

Kept as is: `dh plugin use`, `dh plugin check` (minus its version step),
`src/actions/plugins/ssh.ts` (argv, multiplexing, quoting, error classification), `src/cli/plugin-args.ts`, the spec-to-`CommandSpec`
conversion in `src/commands/plugin/tool.ts`, and `src/actions/auth/jsapi.ts`'s
`assertTrusted` and download code.

## The cluster bundle

A cluster's plugins are one directory, the **bundle**, with one part per
channel. Each part has its own `manifest.json` and `revision`, so each
channel fetches and checks only its own:

```text
dh-cli/
  js/                      # fetched over the web
    revision               # one line: the manifest's revision
    manifest.json
    dbserver/plugin.json   # command tree, extracted at build time
    dbserver/index.js      # bundled ESM module
  shell/                    # read on a node, or copied over SSH
    revision
    manifest.json
    ctl.json
    cluster.json
    deephaven.json
    iriscat.json
```

A worked example is in [examples/dh-cli/](../examples/dh-cli/), built by
`cli-plugin-kit build` from [examples/dh-cli-src/](../examples/dh-cli-src/):
`shell/ctl.json` and `shell/deephaven.json`, and `js/dbserver`
([index.ts](../examples/dh-cli-src/js/dbserver/index.ts)). Rebuild it after
editing a source (see [Authoring](#authoring-internal)); a test checks it's
current.

The build drops each `shell` spec's `source` fields (the tool's raw help and
`helpDigest`). They're for whoever edits the spec, so they stay in the
authoring sources, not in the installed bundle.

Deephaven's own bundle is built by each DHE release's build, so it always
matches what that release runs. That is what makes per-version specs and pins
unnecessary. The two parts ship in different places:

- **`shell/` is installed** at `/usr/illumon/illumon-db-<version>/dh-cli/shell/`,
  so `/usr/illumon/latest/dh-cli/shell/` matches the tools that node runs. Its
  files are world-readable, like the rest of the install, so reading it over
  SSH needs no `sudo`.
- **`js/` is packed into the web app** and served at
  `<origin>/iriside/cli-plugins/`, the way the web UI's own plugins are: Iris's
  `web/client-ide` build packs a pinned plugin list into the web app jar at
  `iriside/js-plugins/`, served with no login and no configuration
  (`packageJsPlugins` in `web/client-ide/build.gradle`). It's a sibling of
  `/iriside/js-plugins/`, not inside it, so the web UI never loads a CLI
  plugin. `Webapi.cliPlugins=<dir>` replaces the packed set, as
  `Webapi.jsPlugins` does for the web UI's.

The serving side is drafted in Iris (branch `DH-23907_cli-plugins`): the
`Webapi.cliPlugins` property, `cli-plugins` in `DeepLinkRedirectFilter`'s
exclusions (without it, every file but `manifest.json` is redirected to the
web app). Envoy's catch-all route already reaches it. The build packs the
bundle; see [Authoring](#authoring-internal).

Plugin names are unique across the whole bundle; the build enforces it. If
`dh` still sees one name from both channels, the `shell` one is rejected
(`plugin_invalid`).

### `manifest.json`

`js/manifest.json`:

```json
{
  "schema": 1,
  "channel": "js",
  "revision": "sha256:4c1f…",
  "cluster": { "kind": "enterprise", "version": "2026.01.261102164822g2f6a8ebf11" },
  "plugins": [
    {
      "name": "dbserver",
      "kind": "js",
      "description": "List the cluster's query and merge servers.",
      "nouns": ["dbserver"],
      "spec": "dbserver/plugin.json",
      "module": "dbserver/index.js",
      "digest": "sha256:77b2…",
      "dh": ">=3.2.0",
      "api": 1
    }
  ],
  "shell": [
    { "name": "ctl", "description": "Manage intraday partitions and metadata indexes.", "nouns": ["ctl"] },
    { "name": "deephaven", "description": "Control a Deephaven node: …", "nouns": ["service", "node", "etcd", "keys", "units", "coreplus", "tls"] }
  ]
}
```

`shell/manifest.json` has the same shape with `"channel": "shell"`, `shell`
plugins, and no `shell`.

| Field                 | Meaning                                                                                                                                                         |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `schema`              | Manifest format version. `dh` ignores a manifest with a schema it doesn't know (status `unsupported` in `dh plugin list`)                                       |
| `channel`             | `js` or `shell`; must match where it was read. Every plugin's `kind` must equal it                                                                               |
| `revision`            | `sha256:` of one `<name>:<digest>\n` line per plugin, plus (`js` part only) one `shell:<name>:<noun>,<noun>…\n` line per `shell` entry, all sorted. Changes whenever any plugin, or the advertised shell nouns, do. Also the whole content of that part's `revision` file (plus a newline) |
| `cluster.version`     | The DHE version the bundle was built for. Shown by `dh plugin list`                                                                                              |
| `plugins[].kind`      | `shell` or `js`                                                                                                                                                   |
| `plugins[].nouns`     | The top-level nouns it registers: its spec's `nouns`, else its `name`. Lets `dh` check conflicts and list plugins without reading every spec                     |
| `plugins[].spec`      | Part-relative path of the command tree. For `shell`, today's spec format minus `tool.version`/`tool.versionFrom`, plus `tool.runAs`. For `js`, see [JS plugins](#js-plugins)       |
| `plugins[].module`    | `js` only: part-relative path of the ESM module                                                                                                                  |
| `plugins[].digest`    | `sha256:` over the spec file's bytes, then the module's (if any). Checked after every fetch or copy and before every import                                    |
| `plugins[].dh`        | semver range of `dh` versions that can run it (`semver.satisfies`). Outside it: not registered, status `needs dh <range>`                                       |
| `plugins[].api`       | `js` only: the plugin API major version it was built against. `dh` supports a set; outside it, status `unsupported api <n>`                                     |
| `plugins[].groups`    | `js` only, optional: registered only for users in at least one of these groups (`getGroupsForUser()`). Unset: everyone                                         |
| `shell`               | `js` manifest only: each `shell` plugin's name, description and `nouns`. Lets a user without SSH learn which nouns SSH would add (`dh plugin list`, the login question), without the specs |

Paths must be relative and, once normalized, stay inside their part;
anything else rejects that plugin (`plugin_invalid`), not the manifest.

The name rules (built-in, reserved, duplicate) apply to every noun a plugin
registers, as they do to spec names today. Plugin names (identities) must be
unique too, separately from nouns.

## The `js` channel

Fetched from `<origin>/iriside/cli-plugins/` of the active profile's server, over
`https:` (or a local `http:` server), with `assertTrusted` on every redirect.
Requests carry no credentials.

- **Fetching:** `GET revision` (conditional, `If-None-Match`). Unchanged
  (`304`, or the same revision): done. Otherwise `GET manifest.json`, then
  each changed plugin's files, verified against its digest.
- **No bundle** (`404` on `revision`, e.g. a DHE release from before
  bundles): not an error; the cache records `none`, and `dh plugin list`
  says `this server provides no dh plugins`.
- **Groups:** at each fetch where any plugin has `groups`, `dh` signs in and
  records `getGroupsForUser()` in the cache, so startup can filter without
  connecting. A change in group membership shows up at the next fetch.
- **Community profiles:** the same fetch. A DHC server serving CLI plugins
  isn't planned (open question 4); a `404` there just means no plugins.

## The `shell` channel

### Sources

| Source       | Where                                                                                                   | Cached |
| ------------ | ------------------------------------------------------------------------------------------------------- | ------ |
| Local bundle | `/usr/illumon/latest/dh-cli/shell/` on this machine, if `manifest.json` exists there (`DH_PLUGIN_BUNDLE` overrides the bundle path). `dh` runs on a node | No: read in place at startup |
| Over SSH     | The active profile's server: its SSH host (below), path `servers.<origin>.ssh.bundle` (default `/usr/illumon/latest/dh-cli`) + `/shell` | Yes    |

Which one supplies the `shell` plugins:

| Situation                                        | Source                                       |
| ------------------------------------------------ | -------------------------------------------- |
| Local bundle, no active profile                  | local bundle                                 |
| Local bundle, active profile on this cluster     | local bundle                                 |
| Local bundle, active profile on another cluster  | over SSH                                     |
| No local bundle                                  | over SSH, or none without a profile or SSH   |

"This cluster": the profile's origin equals
`https://<FRONTEND_HOST>:<FRONTEND_PORT>`, read from `/etc/deephaven/` on
this node (see [ssh-profiles.md](ssh-profiles.md)). It only matters on a node
with profiles for several clusters.

### SSH settings move to the server

Discovery needs an SSH host before any `shell` plugin is known, so the SSH
settings are per server, with per-plugin overrides:

```json
{
  "servers": {
    "https://dhe.example.com:8123": {
      "kind": "enterprise",
      "ssh": {
        "enabled": true,
        "hosts": ["infra-1", "infra-2"],
        "defaultHost": "infra-1"
      },
      "plugins": {
        "crcat": { "sshHosts": ["dis-1", "dis-2"], "defaultSshHost": "dis-2" }
      }
    }
  }
}
```

| Field                 | Meaning                                                                                                                       |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `ssh.enabled`         | See [SSH setup](#ssh-setup). `false` records a declined offer                                                                  |
| `ssh.hosts`, `ssh.defaultHost` | Where discovery and every `shell` plugin connect. Unset → the server URL's hostname                                  |
| `ssh.bundle`          | Remote bundle path (`shell/` is under it). Unset → `/usr/illumon/latest/dh-cli`                                                |
| `plugins.<name>.sshHosts`, `defaultSshHost` | Override the server's hosts for one plugin                                                    |

Existing per-plugin hosts are kept as overrides; nothing migrates them up.
Existing `prefix` values (machine, server or plugin) and `exec` values are
ignored and dropped on the next write.

### Copying over SSH

One `ssh` run, multiplexed like tool runs (`ControlMaster`), with the
`shell/` path and the cached revision as arguments. The remote script
(quoted with `remoteCommand`, run by `sh -c`):

```sh
cd "$1" 2>/dev/null || exit 3          # no bundle at that path
r=$(cat revision) || exit 4             # not a bundle
[ "$r" = "$2" ] && { echo unchanged; exit 0; }
echo "$r"
exec tar -cf - .
```

- stdout's first line is `unchanged` or the new revision; after a revision,
  the rest is a tar stream of `shell/`.
- `dh` reads the tar itself (ustar, regular files and directories only),
  without needing `tar` locally (Windows). A symlink, an absolute path, a
  `..` component or more than 64 MiB rejects the whole copy, and the
  previous revision stays in use.
- Exit 3 or 4: no bundle at that path. Not an error: the cache records
  `none`, and `dh plugin list` says `no dh-cli bundle at <path> on <host>`.
- SSH failures are classified as for tool runs: `ssh_host_unknown`,
  `ssh_auth_failed`, `remote_unreachable`.
- `BatchMode=yes` unless it's an interactive `dh plugin sync` or `dh plugin
  ssh enable`, so a background check never prompts.

### SSH setup

`shell` plugins from a server are discovered and registered only when SSH is
enabled for it (`servers.<origin>.ssh.enabled`).

- `dh auth login` to a server whose `js` manifest lists `shell` asks once,
  with the other login questions:
  `` This server also has admin tools over SSH (ctl, cluster, iriscat). Set up SSH to <host>? ``
  (flag `--plugins ssh|none`; a no is recorded as `ssh.enabled: false` and
  not asked again). Yes checks the connection (`dh plugin check`'s steps),
  then copies `shell/`. Without a `js` manifest, or with empty `shell`, it
  isn't asked.
- `dh plugin ssh enable [--ssh-host <host>]… | disable [--profile <p>]`
  changes it any time, whatever the manifest says. Enabling checks the
  connection first, then copies.
- SSH not enabled and `shell` non-empty: `dh plugin list` ends with
  `` Over SSH, this server also has: ctl, cluster, iriscat. `dh plugin ssh enable` to use them. ``
  Nothing about them is registered or in `dh agents`.

## The cache

```text
<config dir>/plugins/cache/<encodeURIComponent(origin)>/
  js/
    state.json          # {"revision", "etag", "fetchedAt", "clusterVersion", "groups"} or {"revision": null, …}
    manifest.json
    files/<digest-hex>/dbserver/plugin.json
    files/<digest-hex>/dbserver/index.js
  shell/
    state.json          # {"revision", "host", "fetchedAt", "clusterVersion"} or {"revision": null, …}
    manifest.json
    files/<digest-hex>/ctl.json
```

- Files are stored by plugin digest, so two revisions never mix and an
  unchanged plugin's files are kept rather than refetched.
- An update writes new `files/<digest>/` directories (via a temp directory),
  verifies every digest, then writes `manifest.json` and `state.json`
  atomically (temp file, `rename`). An interrupted update leaves the
  previous revision in use.
- `files/` directories no manifest names are deleted after a successful
  update, except the previous revision's (a `dh` started before it may still
  read from it).
- Writes go through the existing `Store` lock.

### Startup never connects

`main.ts` builds plugin commands from the local bundle and the caches only:
each part's `manifest.json`, then each plugin's `spec`, filtering `js`
plugins by the cached `groups`. A `js` plugin's module is not imported at
startup; help and `dh agents` come from its `plugin.json`. A missing or
unreadable cache means no plugin commands from it, never an error.

## Noticing changes

Checks ride on connections commands make anyway, so an unchanged cluster
costs nothing extra:

| When                                 | `js` channel                                                                                                                         | `shell` channel                                                                                                                                                                                  |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Running one of its commands          | A conditional `GET revision`, in parallel with signing in. Changed → fetch, then run itself again with the same arguments (`runSelf`, as versions.ts does today) | In the SSH session that runs the tool, first: `[ "$(cat <bundle>/shell/revision)" = <cached> ] \|\| exit 254` (with today's marker line on stderr when not interactive). Replaces the `readlink -f` check in `remoteRunCommand`. On 254, copy (same multiplexed connection) and run itself again |
| The command's plugin is gone         | `plugin_unavailable`                                                                                                                 | `plugin_unavailable`                                                                                                                                                                            |
| `dh auth login`                      | Fetch. A failure is a warning, not a login failure                                                                                    | Copy, if SSH is enabled (or just enabled). A failure is a warning                                                                                                                              |
| `dh plugin sync [--profile <p>]`     | Fetch                                                                                                                                | Copy, if SSH is enabled                                                                                                                                                                         |
| Any other command                    | At most once per `DH_PLUGIN_REFRESH` hours (default 24; `0` = every run), short timeout, silent failure, like the self-updater's check | The same interval, with `BatchMode=yes` and a short `ConnectTimeout`                                                                                                                           |
| Local bundle                         | —                                                                                                                                    | Nothing: read at startup, so always current                                                                                                                                                     |

`dh plugin sync` prints, per channel, added, changed and removed plugins, or
`unchanged`. `dh plugin list` shows, per channel, the source, `fetchedAt`,
`clusterVersion` and the revision.

A cluster upgrade replaces `/usr/illumon/latest/dh-cli/`, so the next
command of either kind sees a new `revision` before it runs anything. No
stale spec or module runs against a new cluster, except a run already
started.

## Execution

| Kind   | Discovered from | Runs                                                                                                    |
| ------ | --------------- | ------------------------------------------------------------------------------------------------------- |
| `shell` | local bundle    | `local`: the spec's `tool.path` on this machine (or the machine's `path`), as `tool.runAs` says          |
| `shell` | over SSH        | `ssh`: on the plugin's host (its override, else the server's default host), as `tool.runAs` says       |
| `js`   | over the web    | In `dh`'s process, against the active profile's server                                                  |

A `shell` spec copied over SSH never runs locally: it describes what the
cluster's nodes have, not this machine. There is no `exec` setting to change
either way (decision 5).

On a node, `dh` runs every `shell` plugin on that node. A tool that needs
another node's files (`crcat` reads checkpoint records on the DIS nodes) is
run by SSHing to that node and running `dh` there; `dh` is installed on
every node. From a laptop, the plugin's own `sshHosts` send it to the right
node (`dh plugin set crcat --ssh-host dis-1`).

## Running as a service account

DHE's tools are meant to run as one of the cluster's service accounts:
`sudo -u irisadmin dhctl …` signs in with the `iris` key in
`/etc/sysconfig/deephaven/auth/`, with no credential flags. Which account is
a property of the tool, and the spec now comes from the cluster, so the spec
says it. Users no longer write a prefix.

```json
"tool": {
  "path": "/usr/illumon/latest/bin/dhctl",
  "runAs": { "account": "admin", "unlessFlags": ["-k", "--key", "-user", "--user"] }
}
```

| Field                | Meaning                                                                                                                                      |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `runAs`              | Optional. Unset: the tool runs as whoever runs it (the SSH login, or the user running `dh` on a node)                                        |
| `runAs.account`      | `admin`, `merge` or `query`: the cluster's `DH_ADMIN_USER`, `DH_MERGE_USER` or `DH_QUERY_USER`                                               |
| `runAs.unlessFlags`  | Optional. The tool's flags that mean the user brings their own identity (`dhctl`'s `--key`, `--user`). Any of them present: no elevation    |

**The account name** is read on the machine that runs the tool (locally, or
in the SSH session): the first line of `/etc/sysconfig/deephaven/cluster.cnf`
starting `DH_ADMIN_USER=` (or `DH_MERGE_USER=`, `DH_QUERY_USER=`), its value
with one pair of surrounding quotes removed. The file is parsed, never
sourced. Missing, unreadable, or no such line: the documented default,
`irisadmin`, `dbmerge` or `dbquery`.

**Each run**, in order:

1. Any of `unlessFlags` in the user's arguments: the tool runs as the user.
2. Else, the user already is that account (`id -un`): the tool runs directly.
3. Else `sudo -n -u <account> <tool> …`. With a terminal (the `-t` case in
   [SSH execution](plugins.md#over-ssh)), `sudo -u` without `-n`, so sudo can
   ask for a password.

Over SSH, steps 2 and 3 and the account lookup run in the same `sh -c` script
as the `revision` check: no extra round trip. Locally, `dh` does the same
before `Bun.spawn`.

**Matching `unlessFlags`** against the user's arguments (never the spec's
`argv`, never `dh`'s own flags):

- An argument equal to a listed flag (`--key`, `-user`), or starting with
  `<flag>=` (`--key=my.key`).
- A listed one-letter short flag (`-k`) also matches with its value attached
  (`-kmy.key`). A multi-letter short flag (`-user`) is one flag, as in
  Commons CLI, never `-u -s -e -r`.
- Scanning stops at the first `--` among the user's arguments.

**When sudo is refused** (no rule for that account, or a password needed
without a terminal): `tool_failed` with the hint
`` You can't run <tool> as <account>; pass your own credentials: <unlessFlags> `` (or
`` Run it in a terminal to enter sudo's password ``). `dh` never falls back to
running as the user: that would change who the command acts as.

**Shown to users and agents:** a plugin with `runAs` gets one help line, e.g.
`Runs as the cluster's admin account (irisadmin), or as you with --key or --user.`
(the account name from the cache's last run, else the default). `dh agents`
marks it `"runAs": {"account": "admin", "unlessFlags": [...]}`. `dh plugin
list` shows `runs as admin`.

**Scaffolding:** `cli-plugin-kit scaffold` proposes `unlessFlags` from options
whose help mentions authentication, for whoever edits the spec to confirm.

## Several nouns from one plugin

A tool may already be shaped as several nouns: the `deephaven` node tool has
`etcd`, `node`, `keys`, `units` and `coreplus` groups, and lifecycle verbs
(`start`, `stop`, `status`, …) at its top. Under one plugin noun those would
read `dh deephaven etcd status`; as nouns of their own, `dh etcd status`. A
spec says which with `nouns` instead of `commands`:

```json
{
  "schema": 1,
  "name": "deephaven",
  "description": "Control a Deephaven node: its lifecycle, its cluster membership, and its material.",
  "tool": { "path": "/opt/deephaven/current/bin/deephaven", "runAs": { "account": "admin" } },
  "nouns": [
    {
      "name": "service",
      "description": "Start, stop and inspect this node's Deephaven services.",
      "commands": [{ "name": "start", "argv": ["start"], "description": "…" }, …]
    },
    {
      "name": "node",
      "description": "This node's place in the cluster: its identities, joining, and its prerequisites.",
      "commands": [{ "name": "list", "argv": ["node", "list"], "description": "…" }, { "name": "join", "argv": ["join"], "description": "…" }, …]
    },
    { "name": "tls", "argv": ["tls"], "description": "…" }
  ]
}
```

- **One owner per noun.** A noun belongs to exactly one plugin; nothing
  merges two plugins' verbs under one noun. If two tools want the same noun,
  one is renamed when the bundle is built.
- **`name` is the plugin's identity**, used by the manifest, `dh plugin
  list`, `dh plugin set <name>`, settings (`servers.<origin>.plugins.<name>`)
  and the `"plugin"` marker in `dh agents`. It's registered as a command only
  when the spec uses `commands` or `argv` at its top (one noun, today's
  shape). A spec has `nouns`, or `commands`/`argv`, never both.
- **Each entry of `nouns`** is a command node like any other: a group with
  `commands`, or a leaf with `argv` (`dh tls`). Its leaves' `argv` map to the
  tool freely, so a noun can gather verbs from anywhere in the tool
  (`node join` is the tool's top-level `join`).
- **Conflicts per noun:** each goes through the built-in, reserved and
  duplicate checks. The build fails on a clash; if `dh` still sees one, the
  whole plugin is rejected (`plugin_invalid`), never registered in part.
- **Shared by all its nouns:** the tool, `runAs`, SSH hosts and the cache
  entry belong to the plugin.
- **`js` plugins too:** `definePlugin({ name, nouns: { … } })`, each value
  shaped like `commands`; `plugin.json` gets `nouns` the same way.
- **Scaffolding:** `cli-plugin-kit scaffold --nouns` writes each top-level
  group of the tool as its own noun; the tool's loose top-level verbs are
  placed by hand.

The example bundle's `shell/deephaven.json` maps the node tool this way
(from the real help of a build of it):

| `dh` command                                   | Tool argv                                   |
| ---------------------------------------------- | ------------------------------------------- |
| `dh service start\|stop\|restart\|status\|logs` | `start`, `stop`, `restart`, `status`, `logs` |
| `dh node create\|list\|revoke`                  | `node create\|list\|revoke`                  |
| `dh node join`, `dh node preflight`            | `join`, `preflight`                         |
| `dh etcd …`, `dh keys …`, `dh units …`, `dh coreplus …` | the same groups                    |
| `dh tls`                                       | `tls`                                       |

The tool's `create` ("Create a new cluster on this node") is left out; see
[Open questions](#open-questions).

## JS plugins

A `js` plugin is a TypeScript module that defines a command tree and, for
each leaf, a `run` function. `dh` owns the surface: arguments, options, help,
`-o json`, errors and `dh agents` come from the plugin's declared metadata,
and `run` receives parsed values and an execution context and returns data.

### Authoring shape

```ts
import { definePlugin } from "@deephaven/cli-plugin";

/** `serverClass` is on every server `getDbServers()` returns, but missing from the published types. */
interface DbServer { name: string; host: string; port: number; type: string; serverClass: string }

export default definePlugin({
  name: "dbserver",
  description: "List the cluster's query and merge servers.",
  requires: "enterprise",
  commands: {
    list: {
      description: "List the dispatchers and server groups persistent queries run on.",
      options: {
        class: { type: "string", description: "Only servers of this class, e.g. Query or Merge." },
      },
      output: {
        kind: "rows",
        fields: {
          name: { type: "string", description: "Server name." },
          host: { type: "string", description: "Host; Group for a group." },
          port: { type: "number", description: "Dispatcher port." },
          type: { type: "string", description: "DISPATCHER or GROUP." },
          class: { type: "string", description: "Server class." },
        },
      },
      async run(ctx) {
        const { client } = await ctx.enterprise();
        const servers = (await client.getDbServers()) as unknown as DbServer[];
        const wanted = (ctx.options.class as string | undefined)?.toLowerCase();
        return servers
          .filter((s) => !wanted || s.serverClass.toLowerCase() === wanted)
          .map((s) => ({ name: s.name, host: s.host, port: s.port, type: s.type, class: s.serverClass }));
      },
    },
  },
});
```

The example's API facts, from the iris source (`web/client-api/.../IrisClient.java`,
`ConsoleServerAddress.java`, `controller.proto`): `type` is `DISPATCHER` or
`GROUP` (the `ServerTypeEnum` without `STE_`); `serverClass` comes from each
server's `iris.db.<n>.class` property (`Query` and `Merge` in practice, any
string allowed), so `--class` has no fixed choices and compares ignoring
case; `getDbServers()` returns every server in the controller's
configuration, with no group check (`getConsoleServers()` is the one limited
to servers the user may start consoles on). `getDbServersForType(type)`
takes a persistent query's configuration type, not a server type. `pq` and
`pivot` were rejected as examples: `pq` will be a built-in noun, and `pivot`
belongs under a future built-in `table`.

- `commands` nests: an entry with `commands` is a group, one with `run` a
  leaf. Names follow the spec rules (`^[a-z][a-z0-9-]*$`).
- Option types in API v1: `string`, `boolean`, `number`, with `multiple`,
  `choices`, `required`, `default`. Option keys are camelCase in code and
  `--kebab-case` on the command line. `-o`, `-h`, `--profile`,
  `--no-prompt`, `--ssh-host` are taken (build error).
- `requires`: `enterprise`, `community` or `any`. A profile of the other kind
  gets `usage` (`"dbserver" needs an Enterprise profile`).
- `groups` (optional, plugin level): copied to the manifest entry; see
  [Two channels](#two-channels).
- `output.kind`: `rows` (array of objects: a table in human mode),
  `object` (key/value lines), `text` (a string, printed as is; `-o json`
  gives `{"text": …}`) or `none`. `fields` document `-o json` in help and
  `dh agents`, the same single-source schema built-in commands use.
- Optional `human(result): string | string[]` on a leaf overrides the
  default rendering. It never changes `-o json`.

### Build output

The internal build ([Authoring](#authoring-internal)) imports the module once,
writes everything but functions to `plugin.json`, and bundles the module with
its dependencies to one ESM file, `index.js` (esbuild, `platform: "node"`,
`format: "esm"`: the kit runs on Node, and the output runs in `dh`'s Bun
runtime). The JS API is **not** bundled: the context supplies the server's.
`plugin.json`:

```json
{
  "schema": 1,
  "api": 1,
  "name": "dbserver",
  "description": "List the cluster's query and merge servers.",
  "requires": "enterprise",
  "commands": [
    {
      "name": "list",
      "description": "List the dispatchers and server groups persistent queries run on.",
      "options": [
        { "key": "class", "flags": "--class <value>", "type": "string", "description": "Only servers of this class, e.g. Query or Merge." }
      ],
      "output": { "kind": "rows", "fields": { "name": { "type": "string", "description": "Server name." }, "…": {} } }
    }
  ]
}
```

`dh` validates `plugin.json` like a spec (bad file → `plugin_invalid`, listed
by `dh plugin list`) and converts it to `CommandSpec`s with real commander
options. These are ordinary `dh` commands: not `passthrough`, every global
flag applies, and none of the argument rewriting in `cli/plugin-args.ts`.

### Running a leaf

1. Parse arguments with commander (from `plugin.json`).
2. Check the `js` revision ([Noticing changes](#noticing-changes)). Signing
   in waits for the plugin's first `ctx.enterprise()`/`ctx.community()`,
   through `requireAuth`, as built-in commands sign in when they need the
   server.
3. Verify the module's digest, then
   `await import(pathToFileURL(<cache>/js/files/<digest>/dbserver/index.js).href)`,
   the same way `loadEnterpriseApi` imports the server's jsapi.
4. Find the leaf in `module.default.commands` by path. Missing, or not a
   function → `plugin_invalid` (the module and `plugin.json` disagree).
5. Call `run(ctx)`, with Ctrl-C aborting `ctx.signal`.
6. Render the result for `-o human` or `-o json`. A result that isn't JSON
   serializable → `plugin_failed`.
7. Disconnect anything `ctx` opened.

### The context (API v1)

```ts
interface PluginContext {
  /** Positional arguments by name, options by key, already parsed and validated. */
  args: Record<string, string | undefined>;
  options: Record<string, string | number | boolean | string[] | undefined>;

  /** The active profile. */
  profile: { name: string; server: string; kind: "enterprise" | "community"; user: string };

  /** Signed in with the profile's saved credentials on first call; the same connection after. */
  enterprise(): Promise<{ api: EnterpriseDhType; client: EnterpriseClient }>;
  community(): Promise<{ api: DhApi; client: CoreClient }>;

  /** Questions; without a terminal or with --no-prompt they fail naming `flag`. */
  ui: {
    confirm(question: string, opts: { flag: string; default?: boolean }): Promise<boolean>;
    select<T extends string>(question: string, choices: T[], opts: { flag: string }): Promise<T>;
  };

  /** Progress to stderr; silent with -o json. */
  progress(message: string): void;

  /** Aborted on Ctrl-C. */
  signal: AbortSignal;

  /** True with -o json. */
  json: boolean;

  /** Fails the command with one of dh's codes. */
  fail(code: "usage" | "not_found" | "conflict" | "permission_denied" | "plugin_failed",
       message: string, hint?: string): never;
}
```

- `enterprise()` and `community()` reuse `requireAuth` and the existing
  connect/login code, so expired credentials are `auth_expired` with the
  usual hint, and the plugin never sees a password or key.
- `EnterpriseDhType` and `EnterpriseClient` are
  `@deephaven-enterprise/jsapi-types`' own types, re-exported by
  `@deephaven/cli-plugin/enterprise`. `DhApi` and `CoreClient` are
  `@deephaven/cli-plugin`'s names for `typeof dh` and `dh.CoreClient` from
  `@deephaven/jsapi-types`.
- **Later, `ctx.core({ query })`**: a Core+ worker connection for a
  persistent query (auth token from the Enterprise client, then the worker's
  `dh-core.js`). Needed for plugins that run table operations; deferred
  because it means loading a second jsapi per worker. API v1 ships without it;
  adding it is a minor (non-breaking) addition.

### Errors

| Code                 | Exit | When                                                                                         |
| -------------------- | ---- | -------------------------------------------------------------------------------------------- |
| `plugin_failed`      | 6    | `run` threw anything other than a `DhError`, or `ctx.fail("plugin_failed", …)`. The message is the error's; its stack only with `DH_DEBUG` |
| `plugin_invalid`     | 2    | Bad manifest entry, `plugin.json` or spec, a path outside its part, digest mismatch, or module and `plugin.json` disagree |
| `plugin_unavailable` | 2    | The plugin was removed from the server, needs another `dh` version, or another API version. Hint: `dh plugin sync`, or update `dh` |

`ctx.fail` codes other than `plugin_failed` map to `dh`'s existing codes
(`not_found`, `conflict`, `permission_denied` are added to `errors.ts` if not
already there, so `dh agents` lists them).

### Trust

A `js` module runs in `dh`'s process with the user's permissions: it could
read `<config dir>` (other profiles' credentials included). That's the same
trust `dh` already gives the server's jsapi, which it downloads from the
same origin and imports in-process, so the rule is the same: modules load
only from the profile's origin, over `https:` (or a local `http:` server),
with `assertTrusted` on every redirect. The digest guards against a torn or
corrupted download, not a malicious server.

Hardening, later and only if needed: run `js` plugins in a child `dh`
process that receives only the active profile's session token.

### API versioning

`api` is a major version. `dh` declares the set it supports (v1 at first).
Additions to the context and to `plugin.json` that old plugins don't need are
minor and don't bump it. `@deephaven/cli-plugin` is versioned with the API, and
its `definePlugin` records `api` in the build output.

## Authoring (internal)

Creating plugins isn't part of `dh`. Two packages live in this repo, beside
`dh`, because together they are `dh`'s plugin contract (the API version,
`plugin.json`, the manifest and bundle formats) and must change in lockstep
with it. Both are published under the `@deephaven` npm scope, but nothing
needs them published to build or test (see
[Testing without published packages](#testing-without-published-packages)).

- **`@deephaven/cli-plugin`** (`packages/cli-plugin/`): `definePlugin` and the
  context types. Its main entry is product-neutral and Community-typed
  (`ctx.community()` returns `@deephaven/jsapi-types` types). Enterprise
  typing is a subpath, **`@deephaven/cli-plugin/enterprise`**, which types
  `ctx.enterprise()` with `@deephaven-enterprise/jsapi-types`, an optional
  peer dependency: a Community-only plugin never installs Enterprise types.
  The two JS APIs really differ (Enterprise's `irisapi` script and
  `EnterpriseClient` vs Community's `dh-core.js`), so a plugin's `requires`
  and its import say which one it uses.
- **`@deephaven/cli-plugin-kit`** (`packages/cli-plugin-kit/`): runs on Node
  (`npx cli-plugin-kit …`), so a Node/npm build such as Iris's needs no Bun.
  It imports each `js` plugin's bundled module once (on Node) to read its
  definition. Every manifest entry's `dh` is the kit's `DH_RANGE`
  (`>=3.0.0`): the `dh` versions that read what this kit writes. It
  validates `shell` specs with `dh`'s own parser (`src/actions/plugins/spec.ts`,
  bundled into the kit).

| Command                                   | Does                                                                                                                                        |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `cli-plugin-kit scaffold <tool> [--ssh <host>] [--name <noun>] [--nouns] [--[no-]recurse]` | Today's `dh plugin create`: prints a `shell` spec from `--help` on stdout, without version fields; with `--nouns`, each top-level group as its own noun, and the tool's other top-level commands under a noun named after the plugin, to place by hand. `src/plugins/help.ts` and `scaffold.ts` move here |
| `cli-plugin-kit build <src> <out> --cluster-version <v> [--cluster-kind enterprise\|community]` | Validates `<src>/shell/*.json` (dropping `source`) into `<out>/shell/`, builds each `<src>/js/<dir>/` (entry `index.ts` or `src/index.ts`; a symlink to a package works) into `<out>/js/<name>/` (`plugin.json` + `index.js`), and writes each part's `manifest.json` and `revision`, with `shell` in the `js` manifest. Replaces both parts of `<out>`. Fails on a plugin name or noun used twice |

**Where DHE's plugins live.** Plugins live with the product that serves them.

- **TypeScript plugins** are packages in Iris's `web/client-ui` workspace,
  `web/client-ui/packages/cli-plugin-<name>` (e.g.
  `@deephaven-enterprise/cli-plugin-dbserver`), `"private": true` (built
  into the release, never published). They type-check against the same
  release's `@deephaven-enterprise/jsapi-types` (`file:../jsapi-types`), the
  precedent being `auth-nodejs` and `jsapi-nodejs` in the same workspace,
  both Node-targeted.
- **Shell specs** live in Iris too (hand-edited, regenerated with the
  three-way merge from create-update.md, now in the kit).
- **The Iris build** gets a `dh-cli-plugins/` project (shell specs in
  `shell/`, TypeScript plugins linked under `js/`) whose `packageCliPlugins`
  task runs `cli-plugin-kit build`; `web/client-ide` packs `<out>/js/` into
  the web app at `iriside/cli-plugins/` (beside `packageJsPlugins`), and
  `distributions/tar` installs `<out>/shell/` as `dh-cli/shell/`.
- **Community plugins**, if any, would live with Deephaven Core; whether a
  Community server can serve them is open question 4.

### Testing without published packages

Every step can use local checkouts; npm publishing is only for consumers
outside these repos.

1. **`dh` from source:** `bun run dev -- <args>` in this repo runs the CLI
   with the plugin loading being worked on. No release.
2. **A bundle without a server:** `DH_PLUGIN_BUNDLE=<dir>` points `dh` at a
   bundle on disk (both parts), e.g. `examples/dh-cli/`, or the kit's
   `<out>`. Plugins run against the active profile's server as usual.
3. **The `js` fetch without DHE:** serve a bundle's `js/` at
   `/iriside/cli-plugins/` from a local server; `dh` allows `http:` for
   `localhost`. The tests do this with their fake Enterprise server.
4. **The kit without npm:** run it from the checkout:
   `node /workspaces/deephaven-mcp/packages/cli-plugin-kit/dist/cli.js build <src> <out> --cluster-version <v>`
   (after `bun run build` in that package).
5. **Iris plugin packages before `@deephaven/cli-plugin` is published:**
   build it (`bun run build` in `packages/cli-plugin`), then install it
   without saving it,
   `npm install --no-save /workspaces/deephaven-mcp/packages/cli-plugin`
   (a symlink), so `tsc` resolves `@deephaven/cli-plugin/enterprise`.
6. **The Iris build with a local kit:** `packageCliPlugins` takes
   `-PcliPluginKit=<path to the kit checkout>` and runs that instead of the
   npm package. With it unset, it uses the pinned npm version.
7. **A dev DHE server without a rebuild:** point `Webapi.cliPlugins` at a
   kit `<out>/js/` directory and restart the web API service; the packed set
   is replaced, and nothing else changes.

### User-facing subset

Undecided. If it ships, it's `js` only: `@deephaven/cli-plugin` plus `build`,
and a site directory the cluster merges into its `js` part (e.g.
`/etc/sysconfig/deephaven/dh-cli/site/`). That needs a DHE change to merge
two bundles and a rule for name clashes (Deephaven's win). Nothing in `dh`
itself changes for it.

### Development override

`DH_PLUGIN_BUNDLE=<dir>` makes `dh` read both parts from that directory:
`<dir>/shell/` as the local bundle, and `<dir>/js/` in place of the server's,
with no cache and no `groups` filter. A plugin can be tried without
installing it on a cluster. It replaces today's `<config dir>/plugins/`
directory. To try the real channels, put a bundle on a node and set
`servers.<origin>.ssh.bundle` to it (`shell`), or serve `<dir>/js/` at
`/iriside/cli-plugins/` from a local `http:` server, or point a dev server's
`Webapi.cliPlugins` at it (`js`).

## `dh plugin` commands after the change

| Command                                           | Does                                                                                                                                   |
| ------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `dh plugin list`                                  | Per channel: the source (server, SSH host, local bundle or `DH_PLUGIN_BUNDLE`), revision, cluster version, fetched time; each plugin's kind, how it runs and its state (`ready`, `needs dh <range>`, `invalid: <reason>`); or why there are none (`SSH not enabled`, `no dh-cli bundle at <path> on <host>`, `this server provides no dh plugins`). Plugins hidden by `groups` aren't listed. The SSH line from [SSH setup](#ssh-setup) last |
| `dh plugin sync [--profile <p>]`                  | Fetch `js/` and, if SSH is enabled, copy `shell/`, now                                                                                 |
| `dh plugin ssh enable [--ssh-host <host>]…\|disable [--profile <p>]` | See [SSH setup](#ssh-setup). `--ssh-host` sets `ssh.hosts`                                                          |
| `dh plugin set <name> [--ssh-host …] [--path …]` | As today, but `--ssh-host` sets the plugin's override of the server's `ssh.hosts`; no `--prefix` (see [Running as a service account](#running-as-a-service-account)), no `--exec`, nothing version-related |
| `dh plugin use <name> [--ssh-host <host>]`        | As today                                                                                                                               |
| `dh plugin check <name> [--ssh-host <host>]`      | As today, without the `version` step. For a `js` plugin: sign-in, the revision check and module load                                  |

## Decisions

1. **One bundle per cluster, built by each DHE release.** The release's
   build makes "what the cluster runs" and "what it offers `dh`" the same
   thing, so `dh` needs no version detection: `shell/` goes in the versioned
   install, `js/` into the web app. Rejected: shipping specs in `dh` (every
   version of every tool in every binary).
2. **Each kind is discovered over the channel that runs it.** `js` over the
   web, `shell` from disk or SSH. A user discovers what they can use; admin
   tools don't depend on the web server, and `js` plugins don't need SSH.
   Rejected: one channel for everything. Web only would lose admin commands
   when the web server is down; SSH only would leave users without a shell
   account with no plugins, though the JS API serves them.
3. **The bundle splits into `js/` and `shell/`, each with its own manifest and
   revision.** Each channel fetches and checks only its part, the web serves
   no `shell` specs, and SSH copies no JS modules. The `js` manifest names the
   `shell` plugins (`shell`) so SSH can be offered without the specs.
4. **Startup reads only the local bundle and caches; checks ride on
   connections commands already make.** Help and `dh agents` stay fast and
   offline. Rejected: a check on every invocation (latency on `--help`),
   and updating only at login (an upgrade would run stale specs until the
   next login).
5. **Where a `shell` plugin runs follows from where it was found.** Local
   bundle → local; over SSH → SSH. Rejected: a per-plugin `exec` setting the
   user picks (the cluster already knows), and running a spec copied from a
   server locally. On a node, a tool that needs another node runs from that
   node: the user SSHes there and runs `dh`. Rejected: a node-side setting
   or a spec field (`runOn`) that sends one plugin to another node.
6. **SSH settings are per server, with per-plugin overrides.** Discovery
   needs a host before any plugin is known, and most plugins run on the same
   nodes. `crcat`-style plugins that need other nodes keep their own
   `sshHosts`.
7. **`groups` hides `js` plugins, it doesn't secure them.** Filtering at
   discovery keeps help and `dh agents` to what a user can use; the server's
   ACLs still decide what any command can do. Rejected: running plugin code
   at discovery to decide visibility (cluster code at startup).
8. **`js` plugins are declarative metadata plus a `run` function, split at
   build time.** `dh` keeps owning help, flags, output and errors, and never
   executes server code to render help. Rejected: importing the module at
   startup to read its commands, and modules that parse their own argv
   (loses the agent-facing surface, as plugins.md's prior-art table says).
9. **`js` plugins run in-process, with the jsapi's trust.** Same origin,
   https only. Rejected for now: a subprocess sandbox (cost without a new
   trust boundary, since the jsapi already runs in-process).
10. **Creation isn't a `dh` command.** It moves to `@deephaven/cli-plugin-kit`,
    a separate package run on Node. Rejected: a hidden `dh` subcommand (the
    Iris build would need the `dh` binary, and creation would ship to every
    user).
11. **The spec says which account a tool runs as; users don't write a
    prefix.** `tool.runAs` names a DHE service account by role, resolved from
    the cluster's `cluster.cnf`, and elevates with `sudo` unless the user
    passes one of the tool's own credential flags (`unlessFlags`). Rejected:
    keeping `prefix` as a user setting (every user would have to know the
    account and the elevation command for each tool, and the cluster already
    knows), and falling back to running as the user when sudo is refused
    (changes the identity silently). Sites that elevate with something other
    than `sudo` (`dzdo`, `pbrun`) aren't supported until one asks; it would
    then be a cluster setting, not a CLI one.
12. **A plugin may own several nouns; a noun has one owner.** `nouns` lets a
    tool shaped as several nouns map to them directly (`dh etcd status`, not
    `dh deephaven etcd status`). This replaces plugins.md's reserved
    `promote` (decision 2 there). Rejected: hoisting a tool's top-level verbs
    to `dh`'s top level (generic verbs like `start` would compete), and
    letting several plugins share a noun (a reason for two similar concepts
    is a reason to name them differently).
13. **`js` plugins ship like the web UI's own plugins.** Built by the Iris
    build from private packages in `web/client-ui/packages/cli-plugin-*`,
    packed into the web app at `iriside/cli-plugins/`, with
    `Webapi.cliPlugins` as an override, as `packageJsPlugins` and
    `Webapi.jsPlugins` do for the web UI. No install path, no configuration,
    and the files only change with an upgrade. Rejected: serving them from
    the install directory (a default path, a check that it exists, and a
    web-server restart to pick up changes).
14. **The plugin SDK and kit live with `dh`, under `@deephaven`.** They're
    `dh`'s contract and change in lockstep with it; Enterprise types are a
    subpath (`@deephaven/cli-plugin/enterprise`) with an optional peer
    dependency, since the Enterprise and Community JS APIs differ. Rejected:
    web-client-ui (the `@deephaven` packages' usual home, but a web UI
    library repo on its own release cadence) and Iris (would make the SDK
    Enterprise-only).

## Open questions

1. **Serving `js/`.** The serving side is drafted (Iris branch
   `DH-23907_cli-plugins`: `Webapi.cliPlugins`, the redirect-filter
   exclusion, `packageCliPlugins`); it needs the DHE team's review. Check whether the web app's packed files get
   `ETag`s; without them, `dh` compares the `revision` body instead.
   Serving with no login matches `/iriside/js-plugins/` (confirmed
   2026-11-06).
2. **`shell/` in the install.** Confirm that the release can install
   `dh-cli/shell/` under the versioned directory, world-readable, on every
   node (not just infra).
3. **First setup while the web server is down.** Designed in
   [ssh-profiles.md](ssh-profiles.md): `dh auth login <server> --method ssh`
   creates an SSH-only profile, keyed by the origin the node reports.
4. **Community servers.** Could a DHC server serve CLI plugins (a server
   plugin)? DHC has no `/iriside/`, so it would need its own path. The `js` design doesn't assume Enterprise; only DHE's bundle is
   planned.
5. **Per-command `groups`.** v1 filters whole plugins. A plugin with some
   commands for everyone and some for admins would need per-command
   `groups`; it's an additive change.
6. **Site settings in the manifest.** Should admins be able to ship defaults
   for SSH hosts (e.g. `crcat` → the DIS nodes), so users don't
   configure them? Proposed: an optional `ssh` block per `shell` plugin, used
   when the user has no setting of their own.
7. **Checks on a real node for `runAs`.** Whether `cluster.cnf` is readable
   by an SSH user (else the defaults are always used); whether `dhconfig`,
   `iriscat` and `crcat` take credential flags (their `unlessFlags`); which
   account `iriscat` and `crcat` need to read data files (the docs suggest
   `query`).
8. **A home for the node tool's `create`.** "Create a new cluster on this
   node" reads best as `dh cluster create`, but `cluster` is planned for
   `dhconfig`. Since the node tool re-implements parts of `dhctl` and
   `dhconfig`, `cluster` may go to it instead and `dhconfig` take another
   noun, or `create` gets another name.
9. **Which operations become `js` plugins.** dhctl's operations are reachable
   from Core+ scripts. Not persistent queries or tables: `pq` and `table` will
   be built-in nouns. The example is `dbserver`; real candidates come from
   the Enterprise client's admin calls (servers, calendars, `encrypt`).

## Phases

1. `shell` channel: the bundle format, the local bundle, the copy over SSH
   (script, tar reader, cache), server-level `ssh` settings, the in-session
   `revision` check, `dh plugin sync`, `dh plugin ssh`, `DH_PLUGIN_BUNDLE`.
   Remove version pins, `versionFrom`, `plugin create`, the `<config
   dir>/plugins/` directory. Tested with the fake `ssh` the existing SSH
   tests use, against `examples/dh-cli/shell/`.
2. `packages/cli-plugin-kit/` (on Node): move scaffolding, add `build`. Produce a bundle
   for `dhctl`, `dhconfig`, `iriscat`, `crcat` from a real DHE install, and
   test the `shell` channel against it over real SSH.
3. `js` channel and plugins: the fetch and cache, `groups`, `@deephaven/cli-plugin`
   (API v1), `plugin.json` loading, module import and `run`,
   `ctx.enterprise()`/`community()`, output rendering, the new error codes,
   the login question driven by `shell`. `@deephaven/cli-plugin` with its
   `/enterprise` subpath. First plugin: `dbserver list`. Tested against a
   local `http:` server serving `examples/dh-cli/js/`, with nothing
   published ([Testing without published packages](#testing-without-published-packages)).
4. DHE integration: `web/client-ui/packages/cli-plugin-dbserver`, the
   `packageCliPlugins` task (packing `js/` into the web app), `shell/` into
   the installer, and the serving side already drafted. Tested on a dev
   server with `Webapi.cliPlugins` pointing at a local kit build before
   anything is published.
5. Later: `ctx.core({ query })`, per-command `groups`, site defaults in the
   manifest, the user-facing `js` authoring subset, subprocess isolation if
   wanted.

## Validation

- [x] With empty caches and no local bundle, `dh --help` lists no plugin
      nouns, runs no `ssh` and makes no HTTP request (a fake `ssh` that
      records calls; a server that fails every request).
- [x] `dh auth login` to a fake Enterprise server serving `examples/dh-cli/js/`
      caches it; `dh agents -o json` then lists `dbserver` and not `ctl`; `dh plugin
      list` ends with the `ctl` SSH line.
- [x] That login, in a terminal, asks the SSH question naming `ctl`; against
      a server whose `js` manifest has no `shell`, it doesn't ask. Tested on
      `askSshSetup` with a stand-in prompter; the tests have no terminal.
- [x] `dh plugin ssh enable`, with a fake `ssh` whose remote bundle is
      `examples/dh-cli/`, makes one `ssh` run for the copy; `dh agents -o
      json` then lists `ctl` too.
- [x] `dh plugin sync` with both parts unchanged prints `unchanged` for each
      and writes nothing but `fetchedAt`.
- [x] A `ctl` command with an unchanged bundle makes exactly one `ssh` run;
      a `dbserver` command makes one conditional `GET revision` and nothing else
      for discovery.
- [x] After changing `shell/ctl.json` remotely and rebuilding the bundle
      with `cli-plugin-kit build`, the next `ctl` command's run exits
      254 before the tool runs; `dh` copies, runs itself again, and uses the
      new spec.
- [x] After changing `js/dbserver/index.js` on the server and regenerating, the
      next `dbserver` command fetches only `dbserver`'s files, runs itself again, and
      imports the new module.
- [x] Removing a plugin from either part: running it gives
      `plugin_unavailable`, exit 2.
- [x] A file whose bytes don't match its `digest`, or a tar stream with a
      symlink, an absolute path or `..`: the update is rejected and the
      previous revision stays in effect.
- [x] No `shell/` bundle on the host (remote exit 3): `dh plugin list` shows
      `no dh-cli bundle at /usr/illumon/latest/dh-cli on <host>`; a `404` for
      `js/revision` shows `this server provides no dh plugins`. Other
      commands are unaffected.
- [x] A `js` plugin with `"groups": ["iris-superusers"]` is registered for a
      user in that group and absent (help, `dh agents`, `dh plugin list`) for
      one who isn't.
- [x] A manifest entry with `"dh": ">=99"` isn't registered and
      `dh plugin list` shows `needs dh >=99`.
- [x] A `spec` or `module` path with `..` rejects that plugin only; a
      manifest whose `channel` doesn't match where it was read is ignored.
- [x] With `DH_PLUGIN_BUNDLE=examples/dh-cli` and no profile, `dh ctl …`
      runs locally and no `ssh` runs.
- [x] With `examples/dh-cli/shell/deephaven.json`, `dh etcd status` runs the
      tool's `etcd status`, `dh node join` its `join`, `dh tls` its `tls`;
      `dh deephaven` isn't a command; `dh agents -o json` marks each of the
      seven nouns `"plugin": "deephaven"`.
- [x] A second spec in the bundle that also registers `node` is rejected,
      both by the build and, given such a bundle anyway, by `dh`, which
      registers neither plugin.
- [x] A spec with both `nouns` and `commands` is `plugin_invalid`.
- [x] `dh plugin set ctl --exec ssh` is a `usage` error; an `exec` left in
      `config.json` is ignored.
- [x] With `runAs: {"account": "admin", "unlessFlags": ["-k", "--key", "-user", "--user"]}`
      and a fake `ssh`, `dh ctl intraday rescan -t A.B` runs
      `sudo -n -u irisadmin /usr/illumon/latest/bin/dhctl intraday rescan -t A.B`;
      with `--user jdoe --pwfile p`, `-kmy.key` or `--key=my.key` added, no
      `sudo`; with `-- --key` after a `--`, `sudo`.
- [x] A `cluster.cnf` with `DH_ADMIN_USER="opsadmin"` makes it `sudo -n -u opsadmin`;
      an unreadable one, `irisadmin`.
- [x] Logged in over SSH as the account itself: no `sudo`.
- [x] `sudo` refused without a terminal: `tool_failed` naming the account and
      the `unlessFlags`; the tool doesn't run as the user.
- [x] `dh plugin set ctl --prefix …` is a `usage` error; a `prefix` left in
      `config.json` is ignored.
- [x] `dh dbserver list -o json` returns the rows `run` returned; human mode
      prints a table with the `fields` columns; `--class merge` keeps only
      `Merge` servers; `dh dbserver list --help` shows `--class` without
      connecting.
- [x] A `run` that throws `new Error("boom")` gives `plugin_failed`, exit 6,
      message `boom`; with `DH_DEBUG=1` the stack is on stderr.
- [x] Ctrl-C during `run` aborts `ctx.signal` and `dh` exits after `run`
      settles.
- [x] An expired profile running a `js` command gives `auth_expired` with the
      usual hint. Tested as: the same
      error `dh auth status` gives for that profile (for a saved password,
      that's `auth_failed`, as for every command that signs in).
- [x] `dh` builds without `src/plugins/help.ts`, `scaffold.ts`,
      `versions.ts` and `src/commands/plugin/create.ts`.

- [x] With nothing published: `cli-plugin-kit build` run from this repo's
      checkout (`node packages/cli-plugin-kit/dist/cli.js`) over
      `examples/dh-cli-src/` reproduces `examples/dh-cli/` byte for byte,
      and `DH_PLUGIN_BUNDLE=<out>`
      runs `dh dbserver list`.
- [ ] `npm install --no-save <this repo>/packages/cli-plugin` in an Iris
      plugin package lets `tsc` resolve `@deephaven/cli-plugin/enterprise`.
- [ ] Iris's `packageCliPlugins -PcliPluginKit=<checkout>` packs the built
      `js/` into the web app at `iriside/cli-plugins/`; on a dev server with
      no `Webapi.cliPlugins`, `curl <origin>/iriside/cli-plugins/revision`
      returns it without a login.

## Relevant Files

- `src/main.ts`: build plugin commands from the local bundle and caches.
- `src/plugins/load.ts`: replaced by bundle + cache loading for both parts;
  conflict checks per noun.
- `src/commands/plugin/tool.ts`: register each of a plugin's nouns.
- `src/actions/plugins/spec.ts`: drop `tool.version`/`versionFrom`; add `nouns`, `tool.runAs`,
  manifest and `plugin.json` parsing.
- `src/plugins/versions.ts`: removed.
- `src/actions/plugins/help.ts`, `src/actions/plugins/scaffold.ts`: move to
  `packages/cli-plugin-kit/`.
- `src/actions/plugins/settings.ts`: drop `exec` default logic, pins and `prefix`;
  add the server-level `ssh` block with per-plugin host overrides.
- `src/actions/plugins/ssh.ts`: `remoteRunCommand`'s script compares `revision`
  instead of the tool's real path, and applies `runAs`; the copy script.
- `src/actions/plugins/tar.ts`: new, a ustar reader.
- `src/actions/plugins/run.ts`: `runAs` for local runs, replacing the prefix.
- `src/actions/plugins/run-as.ts`: new, `unlessFlags` matching and the account lookup.
- `src/actions/auth/jsapi.ts`: share `assertTrusted` and the download helper with the
  `js` fetch.
- `src/actions/auth/store.ts`: config shape (`servers.<origin>.ssh`), cache directory.
- `src/commands/plugin/*.ts`: `create.ts` removed; `sync.ts`, `ssh.ts` new;
  `list.ts`, `set.ts` (no `--prefix`), `check.ts` (its prefix step becomes
  a `runAs` step: the account and `sudo -n true` as it), `questions.ts`
  updated.
- `src/commands/auth/login.ts`, `src/commands/auth/questions.ts`: the `js`
  fetch and the SSH question from `shell`.
- `src/cli/command.ts`: `js` plugin leaves as normal commands with output
  fields.
- `src/errors.ts`: `plugin_failed`, `plugin_unavailable`, and any of
  `not_found`/`conflict`/`permission_denied` not already defined.
- `src/commands/agents.ts`: plugin source/state block per channel; the
  `"plugin"` marker on each noun.
- `packages/cli-plugin-kit/` (Node CLI, `@deephaven/cli-plugin-kit`) and
  `packages/cli-plugin/` (`@deephaven/cli-plugin`, with the `/enterprise`
  subpath): new.
- In Iris (separate repo, branch `DH-23907_cli-plugins`):
  `web/server-frontend/.../AbstractIrisWebServer.java`,
  `DeepLinkRedirectFilter.java`, `configs/.../iris-defaults.prop`;
  `dh-cli-plugins/` (`packageCliPlugins`), `web/client-ide/build.gradle`,
  `distributions/tar/build.gradle` and
  `web/client-ui/packages/cli-plugin-dbserver/`.
- `examples/dh-cli/`, `examples/dh-cli-src/`: the example bundle and its
  sources.
- `examples/plugins/ctl.json`: removed once nothing reads it
  (`tests/plugins.test.ts`, `plans/plugins-poc-test.md`).
- `docs/ARCHITECTURE.md`, `README.md`: plugin sections.
- `plans/plugins.md`: point to this doc for discovery, versions and creation;
  `plans/versions.md`, `plans/create-update.md`, `plans/laptop-setup.md`,
  `plans/hosts-and-pins.md`, `plans/ssh-hosts.md`: mark superseded parts.
