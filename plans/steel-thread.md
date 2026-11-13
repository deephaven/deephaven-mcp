# Steel thread: cluster-provided plugins, end to end

Build the design in [cluster-plugins.md](cluster-plugins.md) and
[ssh-profiles.md](ssh-profiles.md) far enough to run, from a laptop, a
TypeScript plugin (`dh dbserver list`) and a shell plugin (`dh ctl …`) served
by a real DHE server (a BHS deployed by Jenkins from an Iris branch).

Status: phases 1–3 done (Iris `1bb468d616`); phase 4 (BHS demo) next. The
branch is now on `v3_console_2`, where actions live under `src/actions/`, so
the plugin code is in `src/actions/plugins/` (paths below that name
`src/plugins/` are as they were then). Read this file, then cluster-plugins.md
(the contract), before doing anything.

## Goals (from the user; they decide scope)

1. **Everything lives where it will really live.** Plugin sources in Iris,
   the bundle in the web app and the install, `config.json`, the cache: all in
   the design's final shape and place. The point is for the team to see what
   plugins look like and where they live.
2. **Nothing left over that the design removes.** No `exec`, `prefix`,
   version pins, `<config dir>/plugins/`, `dh plugin create` or old login
   offer kept around "for now". If the design replaces it, delete it in the
   same phase that adds the replacement.
3. **No unnecessary code** before the design is vetted, but builds must pass
   (Iris builds are deployed to a BHS by Jenkins). Tests: keep the suites
   green; add tests for the new behavior as each phase specifies.
4. Results get pulled into the design doc for the team.

Assume every shell plugin's tool ships with Iris (true for `dhctl`,
`dhconfig`, `iriscat`, `crcat`: Iris's `bin/`, installed by
`distributions/tar`). The `deephaven` node tool's cadence is unknown; it is
open question 1, outside this plan.

## Where things are

| What | Where |
| --- | --- |
| This repo (`dh`) | `/workspaces/deephaven-mcp.worktrees/bmingles_v3_noun_verb_extensions`, branch `bmingles_v3_noun_verb_extensions` |
| Design (contract) | `plans/cluster-plugins.md`, `plans/ssh-profiles.md` |
| Example bundle | `examples/dh-cli/`, built by the kit from `examples/dh-cli-src/` (`shell/*.json`, `js/dbserver/`) |
| Plugin packages | `packages/cli-plugin/` (SDK), `packages/cli-plugin-kit/` (kit); a Bun workspace. `bun run build` in each writes `dist/` |
| Design doc for the team | https://claude.ai/artifact/4oom9RBbay2drVrsJUYSMb (a Claude Docs doc; edit with the Claude Docs tools). Stale on one point: it says `js/` is served from the install; it's packed into the web app |
| Iris | `/workspaces/iris.worktrees/DH-23907_cli-plugins`, branch `DH-23907_cli-plugins` (Jira DH-23907) |
| Iris serving change | Committed `b6c474dafb`: `Webapi.cliPlugins` serves a directory at `/iriside/cli-plugins/` (an override of plugins to be packed into the web app); `DeepLinkRedirectFilter` excludes `cli-plugins` |
| Building Iris here | Read `/workspaces/deephaven-wksp/.devc/iris-gradle.md` first. Always `./gradlew --offline`; there are no Google credentials in this container, by design, and a `gcloud` stub answers the Artifact Registry plugin. JDK 17/21/25 installed. Never ask for, mount or store credentials |
| Committed design work | `28fa77a`, `a9dc45b`, `15c9334` in this repo |
| Uncommitted, not ours to commit unasked | `/workspaces/deephaven-wksp/.devc/` (devcontainer setup: `devc.jsonc`, `gcloud-stub/`, `iris-gradle.md`) |

Iris facts used below:

- `web/client-ide/build.gradle`: `packageJsPlugins` packs the web UI's
  plugins (`enterprisePluginList`) into the web app at `iriside/js-plugins/`.
  The CLI equivalent goes beside it.
- `distributions/tar/build.gradle`: builds `/usr/illumon/illumon-db-<version>/`
  (`usrIllumonDbDir`) from `from(…) { into "${usrIllumonDbDir}/…" }` blocks.
- `web/client-ui/packages/`: an npm workspace; `auth-nodejs`, `jsapi-nodejs`
  and `jsapi-types` are Node-targeted packages there (precedent).
- `getDbServers()` returns `{name, host, port, type, serverClass}`; `type` is
  `DISPATCHER` or `GROUP`; `serverClass` is missing from the published types.

## Decisions made while planning (not all in cluster-plugins.md yet)

1. **Shell specs live in a new Iris Gradle project, `dh-cli-plugins/`**
   (`dh-cli-plugins/shell/*.json`). It runs `cli-plugin-kit build` over those
   specs and the TypeScript packages, and exposes two outputs: `js/` (consumed
   by `web/client-ide`) and `shell/` (consumed by `distributions/tar`).
   Rejected: keeping shell specs in `web/client-ui/packages` (the web UI's
   workspace; they have nothing to do with the web).
2. **TypeScript plugin sources:** `web/client-ui/packages/cli-plugin-<name>`,
   `"private": true`, named `@deephaven-enterprise/cli-plugin-<name>`.
3. **When fully built, the Iris build runs the kit** from npm at a version
   pinned in Iris; `-PcliPluginKit=<path to this repo's packages/cli-plugin-kit>`
   runs a local checkout instead. Iris never needs the `dh` binary.
4. **Two deployment stages.** Stage A (no publishing): Jenkins deploys the
   branch with the serving change; the bundle is built locally and copied to
   the BHS over SSH (`shell/` to its real install path, `js/` behind
   `Webapi.cliPlugins`). Stage B (after the team vets the design): publish
   prereleases of both packages, pin them in Iris, and let Jenkins build and
   install everything.
5. **Package names:** `@deephaven/cli-plugin` (SDK, `packages/cli-plugin/`,
   Enterprise types at `@deephaven/cli-plugin/enterprise`) and
   `@deephaven/cli-plugin-kit` (`packages/cli-plugin-kit/`, command
   `cli-plugin-kit`, runs on Node, bundles with esbuild).

## Phases

Each phase ends with `bun run check` and `bun test` green in this repo (and,
for Iris phases, an offline `./gradlew --offline` build of the touched
projects), then a commit. Ask the user before pushing anything.

### 1. The packages (this repo)

- `packages/cli-plugin/`: `definePlugin`, the API v1 context types
  (cluster-plugins.md "The context"), the `/enterprise` subpath typing
  `ctx.enterprise()` with `@deephaven-enterprise/jsapi-types` (optional peer
  dependency). `nouns` and `groups` in `definePlugin`.
- `packages/cli-plugin-kit/`: `build <src> <out>` exactly as cluster-plugins.md
  "Authoring": shell specs validated and stripped of `source`; TypeScript
  plugins → `plugin.json` + one esbuild ESM `index.js` (JS API not bundled);
  each part's `manifest.json` (with `nouns`) and `revision` (js revision covers
  the `shell` list); the `shell` list in the js manifest; fails on a duplicate
  plugin name or noun. Move `src/plugins/help.ts`, `src/plugins/scaffold.ts`
  and `dh plugin create`'s logic here as `scaffold` (moved, not rewritten).
- Replace `examples/dh-cli-src/manifest.ts` with the kit: `examples/dh-cli/`
  becomes the kit's output from `examples/dh-cli-src/` (move the shell specs'
  sources there too). Keep the example bundle committed, regenerated by the
  kit.
- Workspace setup so both packages build with this repo's tooling; the kit's
  built CLI runs with `node packages/cli-plugin-kit/dist/cli.js`.

Done. Decided while building it (now in cluster-plugins.md):

- **`<src>` layout:** `<src>/shell/*.json` and `<src>/js/<dir>/`, each with
  `index.ts` or `src/index.ts`. Phase 3's `dh-cli-plugins/js/dbserver` can be
  a symlink to `web/client-ui/packages/cli-plugin-dbserver` (esbuild resolves
  its imports from the real path).
- **`--cluster-version <v>` (required) and `--cluster-kind`** (default
  `enterprise`) fill each manifest's `cluster`; the Iris task passes its
  version.
- **`dh` ranges:** one kit constant, `DH_RANGE = ">=3.0.0"`, for every entry.
- **Shell specs are validated with `dh`'s `src/plugins/spec.ts`**, bundled
  into the kit; it gained `nouns` and `tool.runAs` validation. The kit rejects
  `tool.version`/`versionFrom`; phase 2 drops them from the parser.
- **`scaffold` prints the spec on stdout**, notes on stderr. Without
  `--[no-]recurse` it asks in a terminal, else fails. `--nouns` puts the
  tool's loose top-level verbs under a noun named after the plugin. The
  proposed `runAs` is `account: "admin"` with the matching `unlessFlags`.
- `help.ts` and `scaffold.ts` were copied into the kit, with
  `tests/plugin_create.test.ts`'s unit tests and the fake tools
  (`packages/cli-plugin-kit/tests/`); `dh`'s originals stay until phase 2
  deletes them with `dh plugin create`.
- `examples/plugins/ctl.json` was deleted in phase 2; its source, with
  `runAs`, is `examples/dh-cli-src/shell/ctl.json`.
- The kit's `DH_RANGE` is `>=3.0.0`: no 3.x is released yet (the latest tag
  is v2.1.0), so the bundle runtime ships in 3.0.0.
- `examples/dh-cli/` is generated, so biome skips it (formatting would change
  the digests).

### 2. The `dh` runtime, legacy removed (this repo)

Implement cluster-plugins.md and ssh-profiles.md in full, except open
questions. That includes: both channels and their sources (local bundle,
`DH_PLUGIN_BUNDLE`, SSH copy, `/iriside/cli-plugins/` fetch), the cache,
change detection, `dh plugin sync`, `dh plugin ssh enable|disable`,
server-level `ssh` settings with per-plugin host overrides, `tool.runAs`,
`nouns`, `groups`, the TypeScript runtime and its errors, `dh agents` and
`dh plugin list` output, login integration (the fetch and the SSH question
from the `shell` list), and SSH-only profiles (`--method ssh`).

Remove in this phase (the design replaces them): `src/plugins/versions.ts`
and every pin (`version`, `detected`, `realPath`, `checkedAt`, `versionFrom`,
`plugin_outdated`); `<config dir>/plugins/` loading in `src/plugins/load.ts`;
`exec` and `execOf`; every `prefix` (machine, server, plugin) and
`dh plugin set --exec/--prefix`; `dh plugin create`
(`src/commands/plugin/create.ts`, `src/plugins/help.ts`, `src/plugins/scaffold.ts`
and `tests/plugin_create.test.ts`; phase 1 copied them
into the kit); the old
login offer and `sshOfferDeclined`; the `version` and `prefix` steps of
`dh plugin check` (now a `runAs` step). Old `exec`/`prefix`/pin values in a
user's `config.json` are ignored and dropped on the next write.

Files: everything under `src/plugins/` and `src/commands/plugin/`,
`src/main.ts`, `src/cli/command.ts`, `src/cli/plugin-args.ts`,
`src/auth/store.ts`, `src/auth/flows.ts`, `src/auth/session.ts`,
`src/commands/auth/*.ts`, `src/commands/agents.ts`, `src/errors.ts`; tests
`tests/plugins.test.ts`, `tests/plugin_ssh.test.ts`,
`tests/plugin_versions.test.ts` (delete or rewrite), `tests/plugin_create.test.ts`
(delete; phase 1 copied it to the kit), `tests/auth/login_plugins.test.ts`;
`docs/ARCHITECTURE.md`, `README.md`; `examples/plugins/` (delete once nothing
reads it). The validation list in cluster-plugins.md is this phase's
acceptance test, with the fake `ssh` and fake Enterprise server the existing
tests use, serving `examples/dh-cli/`.

Progress, in commits on this branch:

1. **Done: the `shell` channel, legacy removed.** Decided while building it:
   - `DH_PLUGIN_BUNDLE`'s `shell/` always runs locally, whatever the active
     profile; the "this cluster" rule applies only to the real local bundle
     (`/usr/illumon/latest/dh-cli/shell/`).
   - The bundle format (manifest types, digests, revisions, path rules) is
     `src/plugins/bundle.ts`; the kit imports it, as it does `spec.ts`.
   - A plugin whose `dh` range excludes this `dh` gets hidden nouns that give
     `plugin_unavailable`; an invalid one gets none.
   - After a 254, `dh` copies and runs itself again, unless stdin is a pipe or
     a file (ssh may have read it): then `plugin_unavailable`, "run the
     command again".
   - The periodic check runs after any command but plugin commands,
     `dh plugin …` and `dh auth login`, with a 5 s timeout.
   - `dh agents` marks a plugin's nouns with `plugin` (its name),
     `pluginTool` and `pluginExec`; `pluginVersion` and `pluginState` are
     gone.
   - Login's SSH question and `--plugins` are removed here and come back in
     commit 4, driven by the `js` manifest's `shell` list.
2. **Done: `tool.runAs`.** Decided while building it:
   - `unlessFlags` are matched against the arguments the tool gets. `dh`
     folds the user's first `--` into its own (as before), so a `--` the
     tool sees is typed twice: `dh ctl … -- -- --key` elevates.
   - Over SSH, the script tells `dh` the account it resolved on a held-back
     stderr line (`dh-ssh:as=<account>`, as `dh-ssh:revision` reports a
     changed bundle); `dh` records it in the cache's `state.json`
     (`accounts`), which the help line reads. A local bundle's help line
     reads this node's `cluster.cnf`.
   - `dh plugin check`'s `runAs` step prints `<account> (you)` or
     `<account> (sudo)`, after `sudo -n -u <account> true`.
3. **Done: the `js` channel and runtime.** Decided while building it:
   - Running a leaf checks the revision first, then signs in on the
     plugin's first `ctx.enterprise()`/`ctx.community()` through
     `requireAuth`, as built-ins do (cluster-plugins.md updated); no session
     cache.
   - `js` commands need a saved profile; `DH_SERVER` alone doesn't register
     or run them.
   - `dh plugin sync` reports each channel; if one fails, it prints both
     lines and exits with the failure's code.
   - The periodic check stamps each attempt (`<cache>/<channel>/checked`)
     before fetching, so an unreachable server is retried once per
     interval, as the self-updater's check is. It doesn't run with
     `DH_PLUGIN_BUNDLE`, which replaces both of the server's parts.
   - New error codes' exits: `plugin_failed` 6, `not_found` 2, `conflict` 2,
     `permission_denied` 4. Ctrl-C during `run` ends in `cancelled` (3)
     once `run` settles.
   - A leaf's `output` is documented under its `--help`
     (`Output (-o json): …`) and in `dh agents -o json` (`output`). Nouns
     carry `plugin` and `pluginKind` (`shell` or `js`).
4. **Done: login, `dh agents` and `dh plugin list` output.** Login
   fetches `js/` with the session's groups, asks the SSH question when the
   `js` manifest lists `shell` plugins and the server's `ssh.enabled` is
   unset (`--plugins ssh|none`), and copies `shell/` when SSH is enabled;
   failures are warnings. `dh plugin list` ends with the SSH line;
   `dh agents -o json` has `pluginChannels`.
5. **Done: SSH-only profiles** (ssh-profiles.md). Decided while building it:
   - With no `FRONTEND_*` files, the typed address is used as typed when it
     has a port, scheme included (`http://localhost:8123` stays `http:`).
   - `dh auth login --profile <ssh-only> <server>`: the server argument is
     where to sign in (another name for the web server than `FRONTEND_HOST`,
     which re-keys the server entry); for any other profile it's ignored, as
     before.
   - `--name` is for `--method ssh` only, as are `--ssh-host` and
     `--ssh-bundle`.
   - `dh auth status` on an SSH-only profile doesn't connect: server, SSH
     hosts, cluster version.
   - Logout closes `dh`'s SSH connections (`ssh -O exit`) to a server no
     profile uses any more.
   - Not tested: the interactive offer (no terminal in the tests) and the
     "this cluster" rule on a real node (phase 4).

### 3. Iris (branch `DH-23907_cli-plugins`)

- `web/client-ui/packages/cli-plugin-dbserver/`: the real source of the
  example plugin (`examples/dh-cli-src/js/dbserver/`), typed against the
  workspace's `jsapi-types`; before publishing, install the SDK with
  `npm install --no-save <this repo>/packages/cli-plugin`.
- `dh-cli-plugins/`: Gradle project; `shell/ctl.json` (and the other three
  tools' specs once scaffolded on a real node); a task running
  `cli-plugin-kit build` (`-PcliPluginKit` for a local kit); outputs `js/` and
  `shell/`.
- `web/client-ide/build.gradle`: pack `js/` into the web app at
  `iriside/cli-plugins/`, beside `packageJsPlugins`.
- `distributions/tar/build.gradle`: `shell/` into
  `${usrIllumonDbDir}/dh-cli/shell`.
- Until phase 5 the kit isn't on npm, so Jenkins can't run the build task:
  make the Iris build skip it (and pack nothing) when neither the pinned
  package nor `-PcliPluginKit` is available, so branch builds still pass.

Done in Iris `1bb468d616` (not pushed). Decisions made there:

- `packageCliPlugins` lives in `dh-cli-plugins/` (decision 1), runs the kit
  with the web build's Node (`:web-client-ui:npmSetup`), and exposes
  `ext.cliPluginsJs` / `ext.cliPluginsShell`; `web/client-ide` (`jsJar` and
  `syncWeb`) and `distributions/tar` read them via `evaluationDependsOn`.
- The task always runs and first deletes its output; without
  `-PcliPluginKit` it stops there, so a stale bundle is never packed.
- `js/dbserver` is a symlink to `web/client-ui/packages/cli-plugin-dbserver`.
  That package stays out of the root tsconfig references and `client-ui`'s
  `childPackageDirs` (CI has no SDK); its tsconfig maps
  `@deephaven-enterprise/jsapi-types` to the workspace so the linked SDK's
  own copy doesn't win.
- Any `npm ci` (including Gradle's `npmProductionInstall`) removes the
  `--no-save` SDK link; reinstall it, then run `packageCliPlugins` on its
  own, not in a build that also runs the web UI's npm install.
- A full `jsJar` + `distTar` build takes about an hour here; without the kit
  both archives have no `cli-plugins` or `dh-cli` entries.
- Formatting doesn't reach the bundle (esbuild reprints the code, and the
  kit re-serializes specs), so Prettier in `web/client-ui` is harmless. The
  module still differs from `examples/dh-cli` by esbuild's path comments:
  `src/index.ts`, and the linked SDK's real path. No gzip copies; no release
  notes yet.

### 4. Stage A demo on a BHS

1. Jenkins deploys `DH-23907_cli-plugins` to a BHS (the user starts it).
2. Locally: `./gradlew --offline :dh-cli-plugins:<build task> -PcliPluginKit=…`.
3. Copy over SSH: `shell/` to `/usr/illumon/illumon-db-<version>/dh-cli/shell/`;
   `js/` to a directory, set `Webapi.cliPlugins` to it, restart the web API
   service; check `curl <origin>/iriside/cli-plugins/revision` with no login.
4. From the laptop: `dh auth login <bhs>`, `dh plugin ssh enable --ssh-host <node>`,
   `dh plugin sync`, `dh dbserver list`, `dh ctl metadata list -t DbInternal`.
5. Confirm on the real server (and record in cluster-plugins.md): what
   `getDbServers()` returns for a group's host and port; that
   `/etc/deephaven/FRONTEND_HOST`/`FRONTEND_PORT` exist; whether
   `cluster.cnf` is readable over SSH; whether the web app's files get
   `ETag`s.
6. Update the design doc with the real files and paths.

### 5. Stage B (after the team vets the design)

Publish prereleases of `@deephaven/cli-plugin` and `@deephaven/cli-plugin-kit`
(the user publishes); pin them in Iris; Jenkins builds and installs the bundle;
redeploy with `Webapi.cliPlugins` unset.

## Validation

- [x] Phase 1: `node packages/cli-plugin-kit/dist/cli.js build examples/dh-cli-src <tmp> --cluster-version 2026.01.261102164822g2f6a8ebf11`
      reproduces `examples/dh-cli/` (it is the generator now).
- [x] Phase 2: every item in cluster-plugins.md's and ssh-profiles.md's
      Validation lists (but the two noted there as untested, and Iris's); `grep -rn "sshOfferDeclined\|execOf\|versionFrom\|\bprefix\b" src`
      finds nothing plugin-related.
- [x] Phase 3: an offline Iris build packs `iriside/cli-plugins/manifest.json`
      into the web app and `dh-cli/shell/manifest.json` into the install tar
      with `-PcliPluginKit`; without it, the build passes and packs nothing.
- [ ] Phase 4: the demo commands work against the BHS.

## Open questions

1. **Tools on their own cadence.** If the `deephaven` node tool (wrapped as
   `andy` in the old POC; `examples/dh-cli/shell/deephaven.json`) ships apart
   from Iris, each installation would bring its own `dh-cli/shell/` and `dh`
   would merge them. Ask its owner; not in this plan.
2. The open questions in cluster-plugins.md and ssh-profiles.md.
