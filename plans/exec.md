# `dh exec`

Run a local script, inline code, or stdin on a Deephaven console and report the variables it changed.

## Commands

| Surface | Behavior |
| --- | --- |
| `dh exec <file>` | Run a local script; path resolves from the invocation directory. |
| `dh exec -c '<code>'` | Run inline code. |
| `dh exec -c` | Read code from piped or redirected stdin. |
| `dh console exec ...` | Canonical command; `dh exec` is its top-level alias. |
| `--console <shortcode-or-id>` | Target a console explicitly. |
| `-o silent` | Suppress success output and progress; keep warnings, failures, and exit codes. |

```sh
dh exec analysis.py
dh exec -c 'from deephaven import empty_table; a = empty_table(5)'
dh exec -c < analysis.py
printf '%s\n' 'a = 1' 'b = 2' | dh exec -c
dh exec -c <<'PY'
from deephaven import empty_table
a = empty_table(5)
b = a.update("X = i")
PY
```

```text
$ dh exec analysis.py
Created console k7m2qd
a:table,b:table

$ dh exec analysis.py
a:table,b:table

$ dh exec analysis.py -o json
{"console":{"shortcode":"k7m2qd","id":"example-console-id","created":false},"changes":{"created":[{"name":"a","type":"Table"},{"name":"b","type":"Table"}],"updated":[],"removed":[]}}
```

- One code source per call, validated before any remote work.
- Source sent verbatim; quoted heredocs keep multiline input intact.
- Target: `--console` → current console → sole running console → new console (Enterprise). Community → shared console.
- Existing profile and console-default rules apply.
- Both entry points listed in help and `dh agents`.
- Human output: `Created console <shortcode>` only for a new console, then changed exports.
- JSON: console identity, `created`, and changes with API type strings; stdout holds only the result.
- Silent: success prints nothing, including a new shortcode; failures include the shortcode when known.

## Errors

| Code | Cause | Exit |
| --- | --- | --- |
| `usage` | Bad input, multiple sources, ambiguous target | 2 |
| `console_not_found` | Stale target | 2 |
| `console_not_ready` | Stopped target | 5 |
| `console_unsupported` | Worker can't run scripts; fails before user code | 5 |
| `cancelled` | Interrupted; reports console and known outcome, warns remote may continue | 3 |
| `exec_failed` | Script error; includes server traceback and partial changes | 5 |

Auth, access, and transport failures keep existing codes.

## Implementation

| Path | Contents |
| --- | --- |
| `src/commands/console/`, `src/commands/root.ts` | Exec command, questions, alias registration |
| `src/actions/console/` | Target resolution, console creation, execution |
| `src/cli/command.ts`, `src/output.ts`, `src/errors.ts` | `-o silent`, `exec_failed` |
| `tests/`, `docs/CONSOLE.md` | Contract tests, examples |

- Existing authenticated client and `IdeSession.runCode`.
- `runCode` resolves with `error` set on script failure → map to `exec_failed`, keep `changes`.

## Stack

| # | PR | Contents | ~Lines |
| --- | --- | --- | --- |
| 1 | Exec | Source forms, alias, console reuse/creation, results, errors, `-o silent`, tests/docs | 1,000 |

## Out of scope

- Worker log streaming; server logging tools cover extra diagnostics.
- Automatic dependency installation.
- Local imports and data files (follow-up). Design open:
  - Surface options: per-console `dh console attach <dir>`; central `dh files` registry with mark/unmark like [VS Code](https://deephaven.io/vscode/1.0.8/docs/python-remote-file-sourcing/#marking-and-unmarking-folders); remote file source plugin extended to arbitrary files.
  - Python imports: remote file source plugin serves modules on demand while the client is connected.
  - Other files: `CoreClient.getStorageService().saveFile(...)` on Community and Core+ (via EnterpriseCorePlusManager); whole-file unary upload, content hash skips unchanged files.
  - Server-side path: storage API paths are relative to `storage.path` (default `<DataDir>/storage`); the only current way to get the physical path is reading server config via `jpy`. Needs a supported mapping.
  - Script access options: absolute `DH_DATA_DIR`, package/plugin helper. Changing the working directory affects the whole worker process.
  - Unsolved: persistent sync state, worker restart, replacement/cleanup, shared-console coordination.

## Done when

- [ ] File, inline, and stdin sources run verbatim from both entry points, including shell-quoted and heredoc input.
- [ ] Console reuse, auto-creation, and explicit targeting work on Community and Enterprise.
- [ ] Changed exports reported in human and JSON output.
- [ ] Script failures return server traceback, partial changes, and exit 5.
- [ ] Cancellation reports console and outcome.
- [ ] `-o silent` suppresses success output and keeps warnings, failures, identifiers, and exit codes.
