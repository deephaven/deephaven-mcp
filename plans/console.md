# `dh console` and `dh workspace`

Manage Deephaven interactive consoles. Folder workspaces let projects and agents
keep separate working choices (profile, console, and creation settings), reducing
accidental interference with a lightweight middle ground between one global
configuration and a full sandbox.

## Commands

| Command | Behavior |
| --- | --- |
| `dh workspace` | Show the current workspace, why it applies, and its choices. |
| `dh workspace create` | Create a workspace in this folder, copied from the current one. |
| `dh workspace freeze` | Make the current workspace read-only. |
| `dh workspace unfreeze` | Make the current workspace editable again. |
| `dh console` | List CLI-created consoles; `●` marks the one in use. |
| `dh console show [console]` | Show every setting and status of a console; defaults to the one in use. |
| `dh console use <console>` | Set the console in use. |
| `dh console create [settings]` | Create a console from the default plus `settings` and use it. |
| `dh console default [settings]` | Show creation defaults, or set the supplied fields. |
| `dh console default --reset` | Return to the server's default. |
| `dh console destroy [console] [-f, --force]` | Destroy a console; defaults to the one in use. |

- JSON and human output carry the same content.

## Workspaces

A workspace holds choices: auth profile, console in use per profile, creation
defaults. Profile definitions, credentials, and output options stay global.

```text
~/repo $ dh workspace
Workspace: main (no .dh/ in this folder or above)
  Profile: prod
  Console: k7m2qd "IC - dh CLI - k7m2qd"
  Default: Python, 8 GB, Core+

~/repo $ dh workspace create
Created workspace ~/repo/.dh from main

~/repo $ dh auth use staging
Default profile for ~/repo/.dh: staging
```

- Workspace for a command: `DH_WORKSPACE` (folder path or `main`), else the
  nearest `.dh/` in this folder or above, else `main`.
- `main` lives in CLI config; folder workspaces live in `.dh/`. Removing the
  folder removes its workspace. `main` starts with today's saved profile.
- `dh auth use`, `login --default`, and first login set the workspace's
  profile; `dh auth` marks it.
- `--profile`, `DH_PROFILE`, and `DH_SERVER` override the profile, and its
  console, for one command; the workspace is unchanged.
- All callers in a folder share choices and can clobber each other.
- Freeze protects choices in `main` or a folder workspace. Copies start
  editable.

## Consoles

```text
$ dh console default --heap-size-gb 8
Creation default
  Heap: 8 GB  (set)

$ dh console create --language groovy
Created k7m2qd "IC - dh CLI - k7m2qd"; now in use

$ dh console
● k7m2qd  IC - dh CLI - k7m2qd                 running
  p4t8cx  IC - dh CLI - p4t8cx                 stopped
```

- `<console>`: shortcode. Consoles are named `IC - dh CLI - <shortcode>`.
- Settings: `--language python|groovy`, `--heap-size-gb <n>`, `--server <name>`,
  `--engine <name>`, or `--json '{…}'` for those plus `jvmArgs`, `environment`,
  `autoDeleteTimeoutSeconds`. Per field: flag > `--json` > default.
- Creation defaults: one set per workspace, used for every profile. Set fields
  layer over the current server's default; JSON `null` resets one field.
  A field the current server lacks → `console_unsupported` at `create`.
- `create` selects the console only after it is ready.
- `destroy` confirms; `--force` skips it and is required with `--no-prompt` or
  no terminal. Returns once deleted.
- `show` and `default` print full JVM arguments and environment values.
  Help and docs warn these are plain text, including in agent transcripts.
- Server waits use `DH_TIMEOUT`, 300 s when unset.
- Community supports `dh console` and `use`; the rest → `console_unsupported`.

### Console in use

For commands given no console:

1. The workspace's console for the current profile, set by `use` or `create`.
2. Else the profile's only running CLI console (Community: its shared
   console), saved for the profile.
3. Else `console_required`, listing running CLI consoles.

- Switching profiles restores that profile's console.
- `dh console use <console> --profile <name>` sets the workspace's profile and
  that profile's console.
- Profile switches and workspace copies or deletion leave consoles running;
  cleanup is the server's idle timeout or `destroy`. Logout keeps references.
- A stopped console in use → `console_not_ready`. A deleted one →
  `console_not_found`, and its entry is cleared.

### Agents

- Every console-taking command accepts an explicit console.
- `dh agents`, help, and docs: create in a separate workspace and pass the
  console explicitly, e.g. `dh console show k7m2qd`. The console in use can
  change between commands.

## Errors

| Code | Exit | Cause / hint |
| --- | --- | --- |
| `usage` | 2 | Bad settings. |
| `console_required` | 2 | No console in use; list running CLI consoles. |
| `console_not_found` | 2 | Unknown or deleted console; suggest `dh console`. |
| `console_not_ready` | 5 | Console stopped or failed; show its state. |
| `console_unsupported` | 5 | Server lacks the operation, language, or field. |
| `console_forbidden` | 4 | Server denied access. |
| `console_timeout` | 5 | Wait exceeded `DH_TIMEOUT`; show shortcode and last state. |
| `console_failed` | 5 | Server rejected create or delete; show its reason. |
| `workspace_not_found` | 2 | `DH_WORKSPACE` names no workspace. |
| `workspace_exists` | 2 | This folder already has `.dh/`. |
| `profile_not_found` | 2 | Workspace's profile was removed; suggest `dh auth use` or `dh auth login`. |
| `workspace_frozen` | 2 | Workspace is read-only; suggest `dh workspace create` in a folder without `.dh/`, or an explicit console. |

## Implementation

- Actions: `src/console/`, `src/workspace/`, existing `src/auth/`; command
  groups under `src/commands/`. Dependencies: none new.
- Use the `InteractiveConsole` API and its supported fields.
- Six-character lowercase alphanumeric shortcodes, allocated before create;
  interrupted creates remain discoverable by name.
- Shortcodes in `DH_CONFIG_DIR`, choices in the workspace; locked atomic
  owner-only files. Network calls outside locks.
- `.dh/.gitignore`: `*`. Profile references: server + user, surviving renames.

## Stack

| # | PR | Contents | ~Lines |
| --- | --- | --- | --- |
| 1 | Workspace store | Resolution, `main`, `.dh/`, freeze, errors | 300 |
| 2 | Auth profile in workspace | `dh auth use`/`login`/listing via the workspace, `docs/AUTH.md` | 300 |
| 3 | Console store | Shortcodes, console in use | 300 |
| 4 | Creation settings | Fields, validation, merge | 300 |
| 5 | Discovery | `dh console`, `show`, `use`, per-profile console in use, agent guidance | 450 |
| 6 | Default | `dh console default` | 300 |
| 7 | Create | `dh console create`, readiness wait | 350 |
| 8 | Destroy | `dh console destroy` | 300 |
| 9 | `dh workspace` | `workspace`, `create`, `freeze`, `unfreeze`, docs | 300 |
| 10 | Community | Shared console, real-server tests, README | 350 |

## Out of scope

- Script execution, variables, tables, logs. (DH-24138)
- Generic persistent-query lifecycle. (DH-24139)
- Community server startup and shutdown. (DH-24141)
- Consoles from other clients (web, VS Code). (follow-up, with script execution)
- Custom console names, renaming, browser links.
- Secret detection in settings.
- Strict agent isolation; use a separate `DH_CONFIG_DIR`, Deephaven identity,
  or OS/host sandbox when CLI-level isolation is insufficient.
- Future workspaces: worktree-root lookup boundaries and copy on first edit;
  per-command `--workspace <path>`.

## Done when

- [ ] (PR 1) Resolution order; frozen workspaces reject changes; git ignores
      `.dh/`.
- [ ] (PR 2) `dh auth use` stays workspace-local; `main` keeps today's
      default; renames preserve references.
- [ ] (PR 3) Concurrent creates get unique shortcodes.
- [ ] (PR 5) Profile switch restores its console; overrides leave the
      workspace unchanged; deleted console entry cleared. Enterprise API
      checked: filtering, stable IDs, fields.
- [ ] (PR 6) Enterprise API checked: server defaults, idle-timeout units.
- [ ] (PR 7) Interrupted create leaves one console listed by `dh console`;
      consoles survive CLI exit.
- [ ] (PR 9) `create` copies the current workspace, rejects existing `.dh/`;
      copies start editable.
- [ ] (PR 10) Real-server tests: Enterprise lifecycle, Community discovery.
- [ ] (Every PR) Help, agent guidance, JSON, and user docs ship together;
      `bun run check` and `bun test` pass.
