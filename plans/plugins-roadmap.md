# Plugins roadmap (temporary)

**Scaffolding, not a persistent artifact.** This lives on the prototype branch
`bmingles_v3_noun_verb_extensions`, which won't merge. It sequences the work
that turns the plugin prototype into designed, reviewed PRs. Everything that
matters ends up in the design docs and plans those PRs produce; once they've
landed, this file is history. Agents: read it from this branch
(`/workspaces/deephaven-mcp.worktrees/bmingles_v3_noun_verb_extensions`), work
one item, and update its status here.

## Conventions

- **Docs first.** Each area gets a docs-only design PR: architecture doc, user
  docs, and a plan with exact contracts and stable Done-when IDs. Developers
  approve it as the contract; implementation PRs prove it. The model is
  `plans/cli-release-channels.md` on `bmingles_DH-24407_auto-update-from-tags`.
- **Right first time.** Greenfield: no interim fix with a "longer-term fix
  later". If the proper fix is bigger, do it or ask.
- **Names:** docs `docs/PLUGINS_<KIND>*.md`, plans `plans/plugins-<kind>.md`,
  Done-when IDs `plugins-<kind>-<area>-<n>`, so kinds sort together
  (`TS`/`ts`, `FS`/`fs`).
- **Branches:** `bmingles_DH-23907_plugins_<kind>_<step>`, created with
  `--no-track` from the branch they build on (`git worktree add --no-track -b
  <branch> <path> <base>`), so a push doesn't target the base. Iris work gets
  its own Jira, created when that work starts.
- **CLI surface:** bare nouns list (`dh plugin`, `dh dbserver`); there are no
  `list` subcommands or aliases. Cite prior art for new surface
  (`ref-cli-design-prior-art` skill).

## Work items

Status: `todo`, `in progress`, `review`, `merged`. Update the row when you
start and finish.

| # | Item | Branch | Builds on | Status | Done when |
| --- | --- | --- | --- | --- | --- |
| 1 | TypeScript plugins design | `bmingles_DH-23907_plugins_ts_design` | `v3_rewrite` | review | Approved; then merged |
| 2 | File-system plugins design | `bmingles_DH-23907_plugins_fs_design` | `v3_rewrite` | todo | `plans/plugins-fs.md` and its docs approved |
| 3 | `dh object` and `dh object table` design and PR | (owner TBD) | `v3_rewrite` | todo | Its own plan; #6 assumes the contract in `plans/plugins-ts.md`'s "Assumed: `dh object`" |
| 4 | TS PR 2: sandbox | `bmingles_DH-23907_plugins_ts_sandbox` | 1 | todo | `plugins-ts-sandbox-*` |
| 5 | TS PR 3: commands | `bmingles_DH-23907_plugins_ts_commands` | 4 | todo | `plugins-ts-cmd-*` |
| 6 | TS PR 4: object views | `bmingles_DH-23907_plugins_ts_objects` | 5 and 3 | todo | `plugins-ts-obj-*` |
| 7 | deephaven-plugins: `dh object ui` / `ui-dashboard` | in `/workspaces/deephaven-plugins` | 5 (6 to test end to end) | todo | `plugins-ts-ui-*` |
| 8 | FS PRs: local on a node first, SSH and SSH-only profiles last | per `plans/plugins-fs.md` | 2 | todo | `plugins-fs-*` |
| 9 | Iris: install the `shell` bundle | Iris, new Jira | 2 | todo | Per `plans/plugins-fs.md` |
| 10 | Follow-ups outside this repo (below) | — | — | todo | Each filed, with its link added here |

### Working an item

1. Read the item's plan and docs in full, and this file's Findings.
2. Create the branch as in Conventions; set the row to `in progress`.
3. Implement to the plan. A test proving a Done-when item names its ID in the
   test name. Where the plan is wrong or silent, change the plan and docs in
   the same PR and say so in the PR description.
4. Run `bun run check` and `bun test`; tick each Done-when box as it's
   proven.
5. Set the row to `review` with the PR link; `merged` when it lands.

## Input for the file-system design (item 2)

Scope decided: the MVP is `dh` running **on a cluster node**, reading the
`shell` bundle from local disk. SSH (discovery and runs over SSH, SSH-only
profiles via `dh auth login --method ssh`) is deprioritized: design for it so
nothing blocks it, implement it last.

- **Survives from the prototype** (this branch): the `shell` parts of
  `plans/cluster-plugins.md` (bundle `shell/` part, manifest, sources, SSH
  settings per server, copying over SSH, `tool.runAs`, several nouns per
  plugin, the cache), `plans/ssh-profiles.md`, the decisions in
  `plans/steel-thread.md`; code in `src/actions/plugins/` (`bundle.ts`,
  `spec.ts`, `run.ts`, `run-as.ts`, `ssh.ts`, `tar.ts`, `cache.ts`,
  `settings.ts`, `discover.ts`), `src/commands/plugin/` (`ssh*.ts`, `tool.ts`,
  `use.ts`, `check.ts`, `set.ts`), `src/cli/plugin-args.ts`,
  `packages/cli-plugin-kit` (shell scaffolding and build), `examples/dh-cli/shell`,
  and the Iris branch `DH-23907_cli-plugins` (`/workspaces/iris.worktrees/DH-23907_cli-plugins`:
  the `dh-cli-plugins` project and the `shell/` install at
  `/usr/illumon/latest/dh-cli/shell`).
- **The background refresh** (commit `2f1e9f5` here): a due check starts a
  detached `dh plugin sync --no-prompt --profile <active>` and never delays
  the command; `DH_PLUGIN_REFRESH=off`. The TS design reuses it; the FS
  design shares it.
- **Superseded** by the TS design: the `js` channel (`js/` bundle part,
  `/iriside/cli-plugins/`, `Webapi.cliPlugins`, `syncJs`, `js-run.ts`,
  `plugin-json.ts`, `examples/dh-cli/js`, the `groups` filter) and the Iris
  branch's `cli-plugins` serving. `packages/cli-plugin`'s `definePlugin` and
  `ctx` are a starting point for item 5's `@deephaven/cli-plugin`.
- **Lost its home:** the `js` manifest's `shell` list, which told users
  without SSH which nouns SSH would add (`dh plugin`, the login question).
  Item 2 decides whether it returns, and from where.
- **Must match** the "Shared with file-system plugins" table in
  `docs/PLUGINS_TS_ARCHITECTURE.md`: one command namespace, one `dh plugin`
  listing (sources show kind and role), one `dh plugin sync` and background
  check, cache under `<config dir>/plugins/<kind>/`, `pluginKind` in
  `dh agents`, shared error codes.

## Findings

- **QuickJS sandbox spike** (`plans/spikes/plugins-ts/`, run `bun install`
  there first):
  - `host.ts` + `plugin.js`: compile with `bun build --compile host.ts`; in
    the binary, `process`, `require`, `import()`, `fetch`, `Bun`,
    `XMLHttpRequest`, `WebSocket` and the `Function("return this")` escape are
    all blocked; `./host x loop` and `./host x memory` show the time and
    memory limits. Adds 0.59 MB to a compiled Bun binary; loads in 7–9 ms.
  - `membrane-host.ts` + `membrane-plugin.js`: the type-directed JS API
    boundary against a live Community server (`docker run -p 10000:10000 -e
    START_OPTS="-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"
    ghcr.io/deephaven/server:latest`, then
    `bun --experimental-http2-fetch membrane-host.ts membrane-plugin.js`).
    Tables, viewports, ticking `updated` events, the listener remover and
    error propagation work; `constructor`, `__proto__`, internal fields and
    unlisted methods are refused. Its allowlist is hand-written; PR 2
    generates it from the types.
  - `api-shape.ts`: classifies every member of the JS API type packages
    (`bun api-shape.ts <d.ts files>`): Community 117 types, Enterprise 35.
- **`IdeSession.runCode` hangs in Bun without `NodeHttp2gRPCTransport`**
  (`probe.ts` shows it), and the stuck command blocks every later command on
  the server's console until it restarts. `connectCommunity` doesn't use that
  transport today. Matters to `dh exec` (`plans/exec.md` on `v3_rewrite`) and
  to TS PR 2's `ctx.console()`.
- **`quietLibraries`** silences `console` while the JS API runs; debug output
  needs `writeSync`.
- **Install size:** Bun's runtime is 81.3 MB of `dh`'s 83.7 MB (linux-arm64);
  `dh`'s own code is ~2.4 MB. The runtime, not features, decides install
  size.
- **Prior art:** `/workspaces/thirdparty/dh-cli-don`'s `dh render` (renders
  `deephaven.ui` via React in jsdom). Its tree format informed the `ui` views;
  its unreachable nested tables and renumbering `@eN` refs are what `--ref`
  avoids.
- **Merging with the release-channels stack**
  (`bmingles_DH-24407_auto-update-from-tags-cli`): three trivial conflicts
  (`package.json` scripts, `src/commands/root.ts` command list,
  `src/main.ts` imports); per-channel config dirs make plugin caches per
  channel, which is fine.

## Follow-ups outside this repo (item 10)

- **deephaven-core:** serve `package.json` under development registration
  (`deephaven.jsPlugins.<name>=<dir>`), in the direction of
  deephaven-core#4817. Until then `dh` can't discover plugins registered that
  way.
- **JS API types** (deephaven-core `web/client-api/types`; Iris
  `web/client-ui/packages/jsapi-types`), one per entry in
  `types/jsapi-augment.d.ts`: literal `EVENT_*` types and per-event
  `addEventListener` overloads; `ConsoleServerAddress.serverClass`;
  `type` aliases as `interface`s; `@sensitive` tags; the narrowed
  `getAuthConfigValues` (DH-24026, DH-24027).
- **Iris team:** confirm `WorkerKind.<kind>.jsPlugins` as the admin install
  location for command plugins (served at
  `/iriside/worker-kind/<kind>/plugins/`, no login, read at web server start).

## Loose ends

- `bmingles_DH-23907_plugins_ts_design` was created tracking
  `origin/v3_rewrite`; unset it (`git branch --unset-upstream`) or set it on
  first push (`git push -u origin <branch>`).
- The TS design had one review round; a fresh review of the rewritten docs is
  pending.
- This branch was rebased onto `v3_rewrite` (old tip `0dcd335`); pushing needs
  `--force-with-lease`.
- The `ghcr.io/deephaven/server:latest` image (1.63 GB) from the spike is
  still in the local Docker store.
