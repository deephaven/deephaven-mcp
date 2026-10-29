# `dh console`

Manage Deephaven interactive consoles, with shared command cancellation,
failure reporting, and cleanup.

## Commands

| Command | Behavior |
| --- | --- |
| `dh console` | List CLI-created consoles; `●` marks the one in use. |
| `dh console detail [console]` | Show every setting and status of a console; defaults to the one in use. |
| `dh console use <console>` | Set the console in use for its resolved profile. |
| `dh console create [settings] [--no-use]` | Create a console from the default plus `settings`; use it unless `--no-use`. |
| `dh console default [settings]` | Show creation defaults, or set the supplied fields. |
| `dh console default --reset` | Return to the server's default. |
| `dh console delete [console] [-f, --force]` | Delete a console; defaults to the one in use. |

- Commands wait for completion or caller interruption.
- Output is sparse: results only when they carry information to act on.
  `use`, setting `default`, and `delete` print nothing on success; waits print
  nothing until they finish or fail. Prompts, warnings, and errors print.
- JSON: result on stdout; warnings and errors on stderr.

## Consoles

- `<console>`: shortcode or full server console ID. Consoles are named
  `IC - dh CLI - <shortcode>`. Shortcodes are unique across all profiles in
  `DH_CONFIG_DIR`.
- Settings: `--language python|groovy`, `--heap-size-gb <n>`, `--server <name>`,
  `--engine <name>`, or `--json '{…}'` for those plus `jvmArgs`, `environment`,
  `autoDeleteTimeoutSeconds`. Per field: flag > `--json` > default.
- Creation defaults: one global set, used for every profile. Set fields layer
  over the current server's default; JSON `null` resets one field. A field the
  current server lacks → `console_unsupported` at `create`.
- `create` waits until ready, then selects the console unless `--no-use`.
- `delete` confirms; `--force` skips it and is required with `--no-prompt` or
  no terminal. Returns once deleted.
- `detail` and bare `default` print full JVM arguments and environment values.
  Help and docs warn these are plain text, including in agent transcripts.
- Community supports `dh console` and `use`; the rest → `console_unsupported`.

### Console in use

- An explicit shortcode or known full ID resolves its saved auth profile ahead
  of environment and default profiles. An explicit `--profile` must match.
  An unknown full ID uses normal profile resolution; discovery saves the mapping.
  Ambiguous full IDs require `--profile`; report the matching profiles.
- Selection: explicit console > profile's remembered console > its only running
  CLI console (remembered automatically); otherwise `console_required`.
  Community uses its shared console.
- Global auth profiles stay unchanged. Each remembers one console;
  `use --profile <name>` changes that console, leaving the default profile alone.
- `detail` and `delete` accept stopped or failed consoles, explicit or remembered.
  Only operations requiring a running console reject these with `console_not_ready`.
- Deleted selection → `console_not_found` and clear the reference.
- Consoles survive profile switches and CLI exit; logout keeps references.
- Agents: pass the console explicitly; create with `--profile` and `--no-use`.
  Remembered defaults are shared; concurrent commands can interleave.

```text
$ dh console create --profile prod --no-use
Created k7m2qd "IC - dh CLI - k7m2qd"
$ dh console detail k7m2qd
```

## Command lifecycle

```text
$ dh console create --profile prod
^C
✖ Interrupted after 95 s waiting for console k7m2qd (profile prod) to start.
  Server accepted the create; startup has not finished.
  Last state: Initializing, 5 s ago.
  The console keeps starting on the server after dh exits.
  Check whether it started: dh console detail k7m2qd
```

The same failure with `-o json`, on stderr:

```json
{"type":"error","error":"Interrupted after 95 s waiting for console k7m2qd (profile prod) to start.","code":"cancelled","exit":3,"command":"console create","target":"k7m2qd","profile":"prod","phase":"starting","elapsedSeconds":95,"outcome":"unknown","observation":{"state":"Initializing","observedAt":"2026-01-01T12:01:30Z","ageSeconds":5},"hint":"The console keeps starting on the server after dh exits. Check whether it started.","recoveryCommand":["dh","console","detail","k7m2qd"]}
```

- A terminal server state or explicit request error fails immediately;
  wait only while the operation is pending.
- Track phase, elapsed time, and the last observed state for failure reports.
- Completion, failure, SIGINT, and SIGTERM share cleanup: close
  clients, release locks, report, exit. Cleanup has a 2 s limit; a second
  Ctrl-C exits immediately.
- An uncertain create reports its shortcode; `dh console detail <shortcode>`
  establishes the result. Each `create` run starts a new console.
- SIGKILL skips cleanup and reporting; the OS releases local locks.

### Failure reports

All commands share one report contract, so an agent can act on a failure, or
relay it to the user, from the report alone. The agent decides whether to
retry, inspect, change input, or report back, using `code`, `outcome`, and its
own attempt history. `outcome: unknown` → establish the result before retrying.

Human and JSON reports carry the same fields; optional fields appear when
applicable. `observation` is `null` until the first observation.

| Fields | Content |
| --- | --- |
| `type`, `error`, `code`, `exit` | `type: "error"`, concise user-facing summary, stable code, exit status. |
| `command` | Command path, e.g. `auth login` or `console create`; excludes argument values. |
| `detail` | Additional diagnostic context: underlying validation, local system, transport, or server reason, kept separate from the summary. |
| `target`, `profile` | Resource identifier and resolved auth profile, when applicable; e.g. a console shortcode. |
| `phase`, `elapsedSeconds` | Command phase at failure and elapsed time. |
| `outcome` | Requested operation: `not_started`, `succeeded`, `failed`, or `unknown`. Describes the operation's result, distinct from the CLI's exit status; `unknown` means its result is unconfirmed. |
| `observation` | Last observed `state`, ISO 8601 `observedAt` timestamp, and `ageSeconds`; state values are command-specific. |
| `hint` | Suggested next step and its purpose; e.g. check a console's current state. |
| `recoveryCommand` | Optional JSON argument array including the executable, e.g. `["dh", "console", "detail", "k7m2qd"]`. Arguments are literal values for direct execution. Human output renders the command with shell-appropriate quoting. |

For operations that continue on the server, state whether the request was
accepted and what continues after `dh` exits.

## Errors

Auth, credential, network, and internal errors use their existing codes and exits.

| Code | Exit | Cause / hint |
| --- | --- | --- |
| `usage` | 2 | Bad settings, ambiguous full ID, or explicit profile mismatch; show valid settings or matching profiles. |
| `console_required` | 2 | No console in use; list running CLI consoles. |
| `console_not_found` | 2 | Unknown or deleted console; suggest `dh console`. |
| `cancelled` | 3 | User cancellation, SIGINT, or SIGTERM; report phase, last observed state, and recovery command. |
| `console_forbidden` | 4 | Server denied access; identify the console and profile and suggest checking permissions. |
| `console_not_ready` | 5 | Operation requires a running console; show its state. |
| `console_unsupported` | 5 | Server lacks the operation, language, or field. |
| `console_failed` | 5 | Server rejected create or delete; show its reason. |
## Implementation

- Lifecycle: `src/cli/`, `src/main.ts`, `src/errors.ts`. One `AbortSignal` in
  `Context`, honored by requests; actions record failure-report context.
- Cleanup runs once; preserve the primary outcome and report cleanup failures.
  CA child re-runs receive forwarded signals.
- `src/output.ts`: an explicit empty-success return prints nothing in either
  mode; JSON errors carry the failure-report fields. Commands used mostly by
  agents, like `dh console`, follow the sparse-output rule; auth keeps its notes.
  Extend `src/errors.ts` and the `dh agents` error listing with console errors.
- Console actions: `src/console/`; commands/questions: `src/commands/console/`.
  Use `InteractiveConsole`.
- Reserve six-character lowercase alphanumeric shortcodes and save their profile
  mappings under the store lock before create; retry collisions across all
  profiles and retain reservations after deletion. Interrupted creates remain
  discoverable by name.
- Store shortcode/full-ID-to-profile mappings, selections,
  and defaults in `DH_CONFIG_DIR`: locked atomic
  owner-only files; network calls outside locks. Profile references use
  server + login user + operate-as user and survive renames.

## Stack

| # | PR | Contents | ~Lines |
| --- | --- | --- | --- |
| 1 | Lifecycle foundation | Cancellation, cleanup, `Context`, errors; inert until integrated | 300 |
| 2 | Command lifecycle | Signals, failure reports, empty success, CA child handoff, `docs/AUTH.md` | 500 |
| 3 | Console store | Global shortcode reservations, ID/profile mappings, remembered console per profile | 300 |
| 4 | Discovery | Enterprise list, detail, use; agent guidance | 450 |
| 5 | Creation defaults | Settings validation and merge, `dh console default` | 550 |
| 6 | Create | `--no-use`, readiness wait, interruption reports | 400 |
| 7 | Delete | Confirmation, deletion wait, interruption reports | 350 |
| 8 | Community | Shared console, real-server tests, README | 350 |

## Out of scope

- Script execution, variables, tables, logs; per-console queues and background
  jobs (follow-up). (DH-24138)
- Generic persistent-query lifecycle. (DH-24139)
- Community server startup and shutdown. (DH-24141)
- Consoles from other clients (web, VS Code). (follow-up, with script execution)
- Strict isolation; use separate config, identity, or OS/host sandbox.
- Project-local profile and console defaults (follow-up).

## Done when

- [ ] (PR 1) Cleanup runs once, reports failures, and stops within 2 s.
- [ ] (PR 2) SIGINT and SIGTERM report, release resources, and exit within 2 s
      with code 3; second Ctrl-C exits immediately. CA child re-runs forward
      signals. Report fields, `error`/`detail` split, and recovery
      arguments with spaces and shell metacharacters are tested; empty success
      prints nothing in both modes.
- [ ] (PR 3) Concurrent creates across profiles get unique shortcodes, including
      forced collisions; deleted codes stay reserved. Renames preserve
      references; operate-as identities stay distinct.
- [ ] (PR 4) Selection precedence and deleted-reference cleanup work;
      shortcodes and known full IDs resolve their profiles over environment and
      defaults. Test unknown/ambiguous full IDs and explicit profile mismatches.
      `use` preserves default profile and prints nothing; `detail` accepts stopped/failed explicit and remembered targets.
      Verify API filtering and stable IDs.
- [ ] (PR 5) Settings precedence, resets, unsupported fields, and defaults
      match the API, including idle-timeout units. Setting and resetting
      print nothing.
- [ ] (PR 6) `--no-use` leaves the console in use unchanged. Interrupted create
      reports the shortcode and profile; a server-accepted
      create remains discoverable by shortcode. Terminal startup failures
      return immediately; the wait prints nothing. Consoles survive CLI exit.
- [ ] (PR 7) Test confirmation, deletion wait, empty success, stopped/failed
      targets, and `outcome: unknown` with a `detail` recovery command when
      the delete response is lost.
- [ ] (PR 8) Real-server tests: Enterprise lifecycle, Community discovery.
- [ ] (Every PR) Help, agent guidance, JSON, and user docs ship together;
      `bun run check` and `bun test` pass.
