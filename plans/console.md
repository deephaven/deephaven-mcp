# `dh console`

Manage Deephaven interactive consoles. Every `dh` command has a time limit and
shared cleanup for completion, failure, and interruption.

## Commands

| Command | Behavior |
| --- | --- |
| `dh console` | List CLI-created consoles; `●` marks the one in use. |
| `dh console show [console]` | Show every setting and status of a console; defaults to the one in use. |
| `dh console use <console>` | Set the console in use for the current profile. |
| `dh console create [settings] [--no-use]` | Create a console from the default plus `settings`; use it unless `--no-use`. |
| `dh console default [settings]` | Show creation defaults, or set the supplied fields. |
| `dh console default --reset` | Return to the server's default. |
| `dh console destroy [console] [-f, --force]` | Destroy a console; defaults to the one in use. |

- JSON and human output carry the same content.
- Global `--timeout <seconds>` sets the command's time limit; `0` disables it.

## Consoles

- `<console>`: shortcode. Consoles are named `IC - dh CLI - <shortcode>`.
- Settings: `--language python|groovy`, `--heap-size-gb <n>`, `--server <name>`,
  `--engine <name>`, or `--json '{…}'` for those plus `jvmArgs`, `environment`,
  `autoDeleteTimeoutSeconds`. Per field: flag > `--json` > default.
- Creation defaults: one global set, used for every profile. Set fields layer
  over the current server's default; JSON `null` resets one field. A field the
  current server lacks → `console_unsupported` at `create`.
- `create` waits until ready, then selects the console unless `--no-use`.
- `destroy` confirms; `--force` skips it and is required with `--no-prompt` or
  no terminal. Returns once deleted.
- `show` and `default` print full JVM arguments and environment values.
  Help and docs warn these are plain text, including in agent transcripts.
- Community supports `dh console` and `use`; the rest → `console_unsupported`.

### Console in use

- Selection: explicit shortcode > profile's remembered console > its only running
  CLI console (remembered automatically); otherwise `console_required`.
  Community uses its shared console.
- Global auth profiles stay unchanged. Each remembers one console;
  `use --profile <name>` changes that console, leaving the default profile alone.
- `show` and `destroy` accept stopped or failed consoles, explicit or remembered.
  Only operations requiring a running console reject these with `console_not_ready`.
- Deleted selection → `console_not_found` and clear the reference.
- Consoles survive profile switches and CLI exit; logout keeps references.
- Agents: pass profile and console explicitly; create with `--no-use`.
  Remembered defaults are shared; concurrent commands can interleave.

```text
$ dh console create --profile prod --no-use
Created k7m2qd "IC - dh CLI - k7m2qd"
$ dh console show k7m2qd --profile prod
```

## Command lifecycle

```text
$ dh console create --profile prod
Timed out after 300 s waiting for k7m2qd to start (last state: Initializing).
The server may still finish it. Check: dh console show k7m2qd --profile prod
Raise the limit with --timeout <seconds>; 0 waits indefinitely.
```

- One deadline covers the whole command. Defaults: 60 s; 300 s for
  `auth login`, `console create`, and `console destroy`.
- These are initial budgets; validate them against successful operation
  timings and adjust before release. A terminal server state or explicit
  request error fails immediately; wait only while the operation is pending.
- `DH_TIMEOUT` retains its 30 s default for individual connections and requests,
  capped by the remaining command time. `--timeout 0` removes the command
  deadline only. Global `--timeout` replaces the SSO-only login option.
- Long waits report phase changes and periodic progress on stderr, including
  elapsed time and time remaining.
- Completion, failure, timeout, SIGINT, and SIGTERM share cleanup: close clients,
  release locks, report, exit. Cleanup has a 2 s limit; a second Ctrl-C exits
  immediately.
- Reports include phase, elapsed time, last successful observation and its age,
  plus a recovery command where available. JSON exposes these as structured
  fields. Server work may continue; check before retrying.
- Persist create/destroy state per shortcode: `not_sent`, `unconfirmed`, or
  `confirmed`. On retry, look up an `unconfirmed` operation on the server first;
  if its outcome remains unknown, stop and show how to inspect it rather than
  resubmitting.
- SIGKILL skips cleanup and reporting; the OS releases local locks.

## Errors

| Code | Exit | Cause / hint |
| --- | --- | --- |
| `usage` | 2 | Bad settings. |
| `console_required` | 2 | No console in use; list running CLI consoles. |
| `console_not_found` | 2 | Unknown or deleted console; suggest `dh console`. |
| `console_not_ready` | 5 | Operation requires a running console; show its state. |
| `console_unsupported` | 5 | Server lacks the operation, language, or field. |
| `console_forbidden` | 4 | Server denied access. |
| `console_failed` | 5 | Server rejected create or delete; show its reason. |
| `timeout` | 6 | Command deadline expired; report progress, recovery command, and `--timeout`. |
| `cancelled` | 3 | User cancellation, SIGINT, or SIGTERM; report progress and recovery command. |

## Implementation

- Lifecycle: `src/cli/`, `src/main.ts`, `src/errors.ts`; adapt `src/auth/`
  and login's timeout option. One `AbortSignal` in `Context`, honored by
  `withTimeout` and requests; actions record progress.
- Cleanup runs once; preserve the primary outcome and report cleanup failures.
  CA child re-runs inherit remaining time and forwarded signals.
- Console actions: `src/console/`; commands/questions: `src/commands/console/`.
  Use `InteractiveConsole`; no new dependencies.
- Allocate six-character lowercase alphanumeric shortcodes before create;
  interrupted creates remain discoverable by name.
- Store shortcodes, selections, and defaults in `DH_CONFIG_DIR`: locked atomic
  owner-only files; network calls outside locks. Profile references use
  server + login user + operate-as user and survive renames.

## Stack

| # | PR | Contents | ~Lines |
| --- | --- | --- | --- |
| 1 | Lifecycle foundation | Deadline, cancellation, cleanup, `Context`, errors; inert until integrated | 350 |
| 2 | Command lifecycle | Global flag, signals, auth waits, CA child handoff, `docs/AUTH.md` | 600 |
| 3 | Console store | Shortcodes, remembered console per profile | 300 |
| 4 | Discovery | Enterprise list, show, use; agent guidance | 450 |
| 5 | Creation defaults | Settings validation and merge, `dh console default` | 550 |
| 6 | Create | `--no-use`, readiness wait, interruption recovery | 400 |
| 7 | Destroy | Confirmation, deletion wait, interruption recovery | 350 |
| 8 | Community | Shared console, real-server tests, README | 350 |

## Out of scope

- Script execution, variables, tables, logs. (DH-24138)
- Generic persistent-query lifecycle. (DH-24139)
- Community server startup and shutdown. (DH-24141)
- Consoles from other clients (web, VS Code). (follow-up, with script execution)
- Custom names, renaming, browser links, secret detection, remote cancellation.
- Strict isolation; use separate config, identity, or OS/host sandbox.
- Per-console queue (follow-up with script execution): FIFO, on by default,
  `--no-queue` opt-out. Resolve the target before queuing; queue wait counts
  toward the deadline. Global records keyed by canonical server + stable console
  ID coordinate local `dh` processes; FIFO requires queue records alongside locks.
- Background jobs (follow-up): any command via `--job`, one worker per job.
- Project-local profile and console defaults (follow-up).

## Done when

- [ ] (PR 1) Cleanup runs once, reports failures, and stops within 2 s.
- [ ] (PR 2) Slow-wait tests: timeout/SIGINT/SIGTERM report progress, release
      resources, and exit within 2 s with codes 6/3/3; second Ctrl-C exits
      immediately. Test shared deadlines, `--timeout 0`, CA handoff, and auth
      compatibility. Validate defaults against representative successful timings.
- [ ] (PR 3) Concurrent creates get unique shortcodes; renames preserve
      references; operate-as identities stay distinct.
- [ ] (PR 4) Selection precedence and deleted-reference cleanup work;
      `use --profile` preserves default profile; show accepts stopped/failed
      explicit and remembered targets. Verify API filtering and stable IDs.
- [ ] (PR 5) Settings precedence, resets, unsupported fields, and defaults
      match the API, including idle-timeout units.
- [ ] (PR 6) `--no-use` leaves the console in use unchanged. Timed-out or
      interrupted create reports the shortcode and profile; a server-accepted
      create remains discoverable without resubmission. Re-running after an
      uncertain create reconciles before retrying and never silently duplicates
      it. Terminal startup failures return immediately. Consoles survive CLI exit.
- [ ] (PR 7) Test destroy confirmation, deletion wait, interruption recovery,
      stopped/failed explicit and remembered targets, and reconciliation when
      the delete response is lost.
- [ ] (PR 8) Real-server tests: Enterprise lifecycle, Community discovery.
- [ ] (Every PR) Help, agent guidance, JSON, and user docs ship together;
      `bun run check` and `bun test` pass.
