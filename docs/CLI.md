# `dh-mcp` CLI

The `dh-mcp` command-line tool is a thin local client for the
multiplexed [Deephaven](https://deephaven.io) MCP systems server. It
manages a per-user background daemon, lists registered MCP tools, and
dispatches tool calls — without requiring you to run the server yourself.

## Table of Contents

- [Installation](#installation)
- [Shell completion](#shell-completion)
- [Quick start](#quick-start)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Runtime directory](#runtime-directory)
- [Command tree](#command-tree)
  - [`dh-mcp daemon`](#dh-mcp-daemon)
  - [`dh-mcp tool`](#dh-mcp-tool)
  - [`dh-mcp session`](#dh-mcp-session)
  - [`dh-mcp system`](#dh-mcp-system)
  - [`dh-mcp table`](#dh-mcp-table)
  - [`dh-mcp script`](#dh-mcp-script)
  - [`dh-mcp catalog`](#dh-mcp-catalog)
  - [`dh-mcp pq`](#dh-mcp-pq)
  - [`dh-mcp config`](#dh-mcp-config)
  - [`dh-mcp introspect`](#dh-mcp-introspect)
- [Top-level flags](#top-level-flags)
- [Output modes](#output-modes)
- [Exit codes](#exit-codes)
- [Structured errors (for AI agents)](#structured-errors-for-ai-agents)
- [Troubleshooting](#troubleshooting)
- [Security model](#security-model)

## Installation

`dh-mcp` ships with the `deephaven-mcp` package; installing the
package wires the entry point automatically:

```bash
pip install deephaven-mcp
# or, in this repo:
uv sync --all-extras
```

After installation `dh-mcp` is on `$PATH`. Verify with:

```bash
dh-mcp --help
```

## Shell completion

`dh-mcp` supports tab completion for commands, subcommands, and options via
`click`'s built-in mechanism — no extra package or command. Enable it for the
current shell by evaluating the generated script, or add the line to your shell
startup file to make it permanent:

```bash
# bash (requires bash >= 4.4); add to ~/.bashrc
eval "$(_DH_MCP_COMPLETE=bash_source dh-mcp)"

# zsh; add to ~/.zshrc
eval "$(_DH_MCP_COMPLETE=zsh_source dh-mcp)"

# fish; add to ~/.config/fish/completions/dh-mcp.fish
_DH_MCP_COMPLETE=fish_source dh-mcp | source
```

## Quick start

```bash
# Show subcommands.
dh-mcp --help

# Start the daemon (idempotent; spawns one if absent).
dh-mcp daemon start

# List the tools the daemon registers.
dh-mcp tool list

# Inspect one tool's input schema.
dh-mcp tool show sessions_list

# Invoke a tool. Arguments are key=value, JSON values are auto-decoded.
dh-mcp tool call sessions_list

# Tail the daemon log file.
dh-mcp daemon logs -n 200

# Stop the daemon.
dh-mcp daemon stop

# Discover the full command tree as structured JSON (for AI agents).
dh-mcp introspect | jq .
```

## Architecture

The CLI talks to a per-user **daemon process** — `dh-mcp-systems-server
--daemon` — over a loopback HTTP transport. The daemon binds
`127.0.0.1` on a random port, generates a fresh PSK per startup, and
writes a registry file the CLI consults to discover the connection
parameters:

```text
<runtime_dir>/daemon/
    daemon.json     # registry: pid, host, port, psk, started_at, ...
    daemon.lock     # advisory lock used to serialize spawn races
    daemon.log      # captured daemon stdout/stderr
```

The daemon is auto-started by the CLI when no live registry is
present, runs in the background, and gracefully exits after a
configurable idle window (default: 1 hour of no traffic).

## Configuration

The CLI shares the multi-server configuration directory with
`dh-mcp-systems-server`. Override the directory via:

- `--config-dir <path>`
- `$DH_MCP_DATA_DIR/config/` (the env var moves the user-data
  root; the per-subdir override is the CLI flag)
- (otherwise) the platform default: `~/.deephaven/ai/config/` on
  POSIX, `%APPDATA%/Deephaven/ai/config/` on Windows.

Two files in that directory are CLI-relevant:

- `cli.json` — CLI defaults (output mode, request timeout, ...).
- `server.json` — daemon tunables (under the `daemon` block).

Use `dh-mcp config show` to inspect the resolved tree (with secrets
redacted) and `dh-mcp config validate` to confirm it is valid.

### Configuration loading

Configuration is loaded **once, eagerly, on every invocation**.
Any malformed file under the configuration directory fails fast
with `config_invalid` (exit code `2`) before any subcommand body
runs. There is no recovery mode — fix the file the error message
names, then retry.

The single load runs in `dh-mcp`'s root callback:

1. Resolves `config_dir` and `runtime_dir` (CLI flags →
   `$DH_MCP_DATA_DIR/{config,runtime}` → platform default).
2. Creates `runtime_dir` at mode `0o700`.
3. Audits `config_dir` permissions and parses `server.json`,
   `cli.json`, `community/`, and `enterprise/` into a single
   validated `Runtime`.
4. Applies top-level CLI flag overrides (`-o`, `--timeout`,
   `--no-auto-start`) onto `runtime.config.cli`.

The only verbs that bypass the load are help and self-introspection:

- `dh-mcp --help`, `dh-mcp <noun> --help`, `dh-mcp <noun> <verb> --help` — the help text is rendered without touching configuration so you can navigate the CLI surface against a broken tree.
- `dh-mcp introspect` — emits the command tree as JSON for agents that need to learn the surface before any config exists.

#### Recovering from a broken configuration

Eager validation means a typo in any config file blocks every
subcommand body. Workarounds when you can't run `dh-mcp` itself:

- **Diagnose the error**: `dh-mcp config validate` surfaces the
  structured `config_invalid` payload identifying the offending
  file. (Validation runs in the eager load before the verb body,
  so the error appears even though the verb itself never executes.)
- **Stuck daemon**: read `daemon.json` directly under
  `<runtime_dir>/daemon/`, then `kill <pid>`. Optionally `rm`
  the registry file once the process is gone.
- **Edit the offending file by hand** based on the error message,
  then re-run the command.

### `cli.json`

The schema is organized into three top-level domain sections. Each section
is its own object (Pydantic model); sections that hold time-shaped knobs
carry a `timeouts:` sub-section reserved from day one, so future timeouts
slot in without a breaking schema change.

#### `output.*` — presentation

| Field                | Type    | Default | Notes                                                                       |
|----------------------|---------|---------|-----------------------------------------------------------------------------|
| `output.format`      | string  | `human` | One of `human`, `json`, `yaml`. Override per invocation with `-o/--output`. |

#### `daemon.*` — CLI-side daemon lifecycle

| Field                                       | Type    | Default | Notes                                                                          |
|---------------------------------------------|---------|---------|--------------------------------------------------------------------------------|
| `daemon.auto_start`                         | bool    | `true`  | When `false`, `dh-mcp` exits if no daemon is running.                          |
| `daemon.timeouts.startup_deadline_seconds`  | integer | `30`    | How long the CLI waits for a freshly-spawned daemon to publish its registry.   |
| `daemon.timeouts.kill_after_seconds`        | integer | `10`    | How long `daemon stop`/`restart` waits after `SIGTERM` before escalating to `SIGKILL`. |

#### `request.*` — outbound MCP requests

| Field                                  | Type    | Default | Notes                                            |
|----------------------------------------|---------|---------|--------------------------------------------------|
| `request.timeouts.default_seconds`     | integer | `60`    | Per-request timeout. Override with `--timeout`.  |

Example (JSON5; `//` comments are accepted):

```json5
{
  "output": {
    "format": "json"
  },
  "daemon": {
    "auto_start": true,
    "timeouts": {
      "startup_deadline_seconds": 30,
      "kill_after_seconds": 10
    }
  },
  "request": {
    "timeouts": {
      "default_seconds": 30
    }
  }
}
```

Every section and sub-section is optional; an empty `{}` at any level yields
all-defaults for that level. A missing or empty `cli.json` yields a fully
default `CliConfig`.

### `server.json` — `daemon` block

The background daemon's behavior is tuned by the optional `daemon`
block in `server.json` (`idle_shutdown_seconds`, `process_name`).
Those are server-side configuration fields, so their schema and
defaults live in
[`docs/CONFIGURATION.md`](CONFIGURATION.md#serverjson-daemon-block).

## Runtime directory

Daemon state lives under the runtime directory, controlled by:

- `--runtime-dir <path>` (top-level CLI flag)
- `$DH_MCP_DATA_DIR/runtime/` (the env var moves the user-data
  root; the per-subdir override is the CLI flag)
- (otherwise) the platform default: `~/.deephaven/ai/runtime/` on
  POSIX, `%APPDATA%/Deephaven/ai/runtime/` on Windows.

On POSIX the CLI applies the following modes on every invocation
(idempotent tightening; stricter modes set by the operator would be
unusable anyway, looser modes are silently corrected):

- `<runtime_dir>` itself: `0700` (re-applied by `load_runtime`).
- `<runtime_dir>/daemon/`: `0700` (re-applied by the daemon's
  `DaemonDirectory.ensure`).
- `<runtime_dir>/daemon/daemon.json`: `0600` (re-applied on every
  registry write).

The CLI does *not* refuse to operate when it observes looser
modes — it logs and corrects them. On Windows none of these mode
changes apply; ACL hardening for the runtime tree is tracked
under the Windows-support follow-up.

## Command tree

The CLI is organized noun-verb. Each noun is a `click` group; each
verb honors the top-level `-o/--output` flag.

### `dh-mcp daemon`

| Verb       | Purpose                                                                                       |
|------------|-----------------------------------------------------------------------------------------------|
| `start`    | Idempotently spawn the daemon (or print the existing handle). Reports pid/host/port.          |
| `stop`     | Idempotent SIGTERM (escalating to SIGKILL); removes the registry file.                        |
| `status`   | Reports whether a daemon is running and surfaces the registered host, port, pid, started_at, server_name, and config_dir. |
| `restart`  | `stop` then `start` in one shot; reports the new handle.                                      |
| `reset`    | Quarantines a corrupt `daemon.json` (renames it to `daemon.json.corrupt-<UTC>`) so a fresh `start` can write a clean registry. Refuses while a live daemon is still registered (`daemon_registry_live`). |
| `logs`     | Tails `daemon.log`. `-n/--lines N` controls the initial tail (default 100); `-f/--follow` follows the file (Ctrl-C to exit). |

### `dh-mcp tool`

| Verb               | Purpose                                                                                                 |
|--------------------|---------------------------------------------------------------------------------------------------------|
| `list`             | Lists registered tools. Internal tools (`_`-prefixed) are hidden unless `--all` is supplied.            |
| `show <name>`      | Prints one tool's name, description, and JSON input schema.                                             |
| `call <name>`      | Invokes a tool. Pass arguments as `--arg key=value` (repeatable); JSON-decoded when possible.           |

`dh-mcp tool call` returns:

- `0` — success.
- `2` — client-side failure (connection, timeout, malformed argument, ...).
- `3` — the tool returned `isError=true`.

Examples:

```bash
dh-mcp -o json tool list
dh-mcp tool show sessions_list
dh-mcp tool call sessions_list --arg type=community
```

### `dh-mcp session`

Sessions are addressed by a fully qualified id `type:system:name`
(`type` is `community` or `enterprise`). Verbs that take an id route to
the right backend by the id's prefix; `create` chooses the backend from
`--system`. Type is never a subgroup.

| Verb                          | Purpose                                                                                       |
|-------------------------------|-----------------------------------------------------------------------------------------------|
| `list`                        | Lists sessions (both types) as a JSON array. Filters: `--type community\|enterprise`, `--system NAME`, `--origin static\|dynamic`. Wraps `sessions_list`. |
| `show <id>`                   | Shows one session's detail object. `--connect` actively verifies liveness. Wraps `session_details`. |
| `create [NAME] --system SYS`  | Creates a session. `--system community` (default) → local Community worker (`NAME` required); any other system → an Enterprise worker on that named system, `NAME` optional/auto-generated (discover system names with `system list`). Wraps `session_community_create` / `session_enterprise_create`. |
| `delete <id>`                 | Deletes a session, routing by the id prefix. Wraps `session_community_delete` / `session_enterprise_delete`. |
| `credentials <id>`            | Prints a Community session's browser-login credentials (`auth_type`, `auth_token`, `connection_url`, `connection_url_with_auth`). Wraps `session_community_credentials`. |
| `url <id>`                    | Prints only the authenticated browser URL (`connection_url_with_auth`) — pipe-friendly. |
| `open <id>`                   | Opens the authenticated URL in the default browser; `--print` prints it instead (headless-safe). |

`create` options are split by type and mutually exclusive: Community
takes `--launch-method`, `--auth-token`, `--docker-image`,
`--docker-memory-limit-gb`, `--docker-cpu-limit`, `--docker-volume`
(repeatable), `--python-venv-path`; Enterprise takes `--server`,
`--engine`, `--auto-delete-timeout`, `--admin-group`/`--viewer-group`
(repeatable), `--session-arg KEY=VALUE` (repeatable, JSON values).
Shared: `--language` (`Python`/`Groovy`), `--heap-size-gb`, `--jvm-arg`
(repeatable), `--env KEY=VALUE` (repeatable). Supplying a wrong-type option
exits `2` (`option_not_applicable`).

`credentials`, `url`, and `open` are Community-only and share the
`session_community_credentials` tool, whose output contains a
**plaintext auth token by design**. Retrieval is gated by
`security.credential_retrieval_mode` in `community/settings.json`
(default `none`); when disabled — or the session is missing or not a
Community session — they exit `3`. All session verbs exit `0` on
success, `2` on client-side/daemon failure, and `3` when the wrapped
tool reports an error.

Examples:

```bash
dh-mcp session list --type community
dh-mcp session create dev --launch-method python --env LOG_LEVEL=DEBUG
dh-mcp session create rpt --system prod --engine DeephavenEnterprise
dh-mcp session delete community:community:dev
dh-mcp session open community:community:dev --print
```

### `dh-mcp system`

A *system* is the source dimension of every fully qualified session id
(`type:system:name`): the single Community umbrella (named `community`)
plus every configured Enterprise (Core+) system.

| Verb       | Purpose                                                                                       |
|------------|-----------------------------------------------------------------------------------------------|
| `list`     | Lists every configured system as `{name, type}`. Wraps `list_systems`. Output is a JSON array — use the names with `session create --system NAME`. |
| `status`   | Reports Enterprise (Core+) system health (`liveness_status`, `is_alive`, redacted `config`). Wraps `enterprise_systems_status`. Enterprise-only: an all-Community deployment returns an empty list. `--system NAME` scopes to one system; `--connect` actively verifies connectivity instead of reading cached state. Exits `3` if the tool reports failure. |

Examples:

```bash
dh-mcp system list
dh-mcp -o json system list | jq '.[].name'
dh-mcp system status --system prod --connect
```

### `dh-mcp table`

Inspects tables in a session. All verbs take a fully qualified
`SESSION_ID`.

| Verb                          | Purpose                                                                                       |
|-------------------------------|-----------------------------------------------------------------------------------------------|
| `list <id>`                   | Emits the session's table names as a JSON array. Wraps `session_tables_list`. |
| `schema <id> [TABLE...]`      | Column definitions for the named tables (all tables when none named). Wraps `session_tables_schema`. |
| `data <id> <table>`           | Row data: `--max-rows N`, `--head/--tail` (default head). Wraps `session_table_data`. |

```bash
dh-mcp table list community:community:dev
dh-mcp table data community:community:dev trades --max-rows 50 --tail
```

### `dh-mcp script`

Runs code and inspects the package environment in a session.

| Verb                          | Purpose                                                                                       |
|-------------------------------|-----------------------------------------------------------------------------------------------|
| `run <id>`                    | Executes a script via `--script TEXT` or `--script-path PATH` (supply one). Wraps `session_script_run`. |
| `pip-list <id>`               | Lists installed pip packages as a `{package, version}` array. Wraps `session_pip_list`. |

```bash
dh-mcp script run community:community:dev --script 'print(1)'
dh-mcp script pip-list community:community:dev
```

### `dh-mcp catalog`

**Enterprise (Core+) only.** Queries an enterprise session's catalog
(database); `SESSION_ID` must name an enterprise session.

| Verb                                  | Purpose                                                                               |
|---------------------------------------|---------------------------------------------------------------------------------------|
| `tables <id>`                         | Catalog table metadata. `--max-rows`, `--filter` (repeatable). Wraps `catalog_tables_list`. |
| `namespaces <id>`                     | Catalog namespaces. Same options as `tables`. Wraps `catalog_namespaces_list`. |
| `schema <id> [TABLE...]`              | Catalog table schemas. `--namespace`, `--filter` (repeatable), `--max-tables`. Wraps `catalog_tables_schema`. |
| `sample <id> <namespace> <table>`    | Sample rows. `--max-rows`, `--head/--tail`, `--filter` (repeatable). Wraps `catalog_table_sample`. |

```bash
dh-mcp catalog tables enterprise:prod:rpt
dh-mcp catalog sample enterprise:prod:rpt Market Trades --max-rows 20
```

### `dh-mcp pq`

**Enterprise (Core+) only.** Manages Persistent Queries, addressed by
serial id (`pq name-to-id` resolves a name within a system).

| Verb                                  | Purpose                                                                               |
|---------------------------------------|---------------------------------------------------------------------------------------|
| `list <system>`                       | Lists PQs configured on a system. Wraps `pq_list`. |
| `details <id>`                        | Configuration + status for one PQ. Wraps `pq_details`. |
| `name-to-id <system> <name>`          | Resolves a PQ name to its serial id. Wraps `pq_name_to_id`. |
| `create <name> --system S --heap-size-gb N` | Creates a PQ on `--system` with `--heap-size-gb` of heap. Script via `--script-body`/`--script-path`; see the config flags below. Unset flags use controller defaults. Wraps `pq_create`. |
| `modify <id>`                         | Updates only the fields passed; everything else is left unchanged. `--restart` restarts the PQ after applying the change. Wraps `pq_modify`. |
| `delete <id>...`                      | Deletes one or more PQs. `--max-concurrent N`. Wraps `pq_delete`. |
| `start <id>...`                       | Starts one or more PQs. `--wait/--no-wait`, `--max-concurrent`. Wraps `pq_start`. |
| `stop <id>...`                        | Stops one or more PQs. Same options as `start`. Wraps `pq_stop`. |
| `restart <id>...`                     | Restarts one or more PQs. Same options as `start`. Wraps `pq_restart`. |

`create` and `modify` share a large optional config flag set; only the flags you
pass take effect (on `modify`, everything else is left unchanged). `create`
additionally requires `--system` and `--heap-size-gb` and accepts
`--enabled/--disabled` (default enabled); `modify` takes `PQ_ID` and accepts
`--pq-name`, `--heap-size-gb`, `--enabled/--disabled`, and `--restart`. The
shared flags are: `--script-body`/`--script-path`, `--language`
(`Python`/`Groovy`), `--configuration-type` (`Script`/`RunAndDone`), `--schedule`
(repeatable), `--server`, `--engine`, `--jvm-profile`, `--jvm-arg` (repeatable),
`--class-path` (repeatable), `--python-venv`, `--env` (repeatable),
`--init-timeout-nanos`, `--auto-delete-timeout`, `--admin-group`/`--viewer-group`
(repeatable), `--restart-users`, and `--owner`. Run `dh-mcp pq create --help`
(or `modify`) for the full per-flag detail.

`delete` / `start` / `stop` / `restart` are best-effort across multiple ids:
exit `0` means the batch ran, not that every id succeeded — check the
`summary` and per-item `results` in the payload for failures.

```bash
dh-mcp pq create nightly --system prod --heap-size-gb 4 --script-path /pq/n.py
dh-mcp pq restart 1234567890 --no-wait
```

### `dh-mcp config`

| Verb        | Purpose                                                                                       |
|-------------|-----------------------------------------------------------------------------------------------|
| `show`      | Prints the resolved configuration with secrets redacted.                                      |
| `validate`  | Confirms the configuration is valid; exits `0`, or `2` with `config_invalid` if any file is malformed. Validation is eager, so this is CI-friendly. |

### `dh-mcp introspect`

Emits the full command tree as a manifest: every command,
every option (with envvar / choices / defaults), every error code.
AI agents should prefer this over scraping `--help`. The manifest
defaults to JSON (so `dh-mcp introspect | jq .` works without `-o`)
but honors the root `-o/--output` flag and `DH_MCP_OUTPUT` (`json`,
`yaml`, or `human`).

## Top-level flags

| Flag                | Envvar               | Purpose                                                                              |
|---------------------|----------------------|--------------------------------------------------------------------------------------|
| `--config-dir PATH` |                      | Override the configuration directory. No per-subdir env var; use `DH_MCP_DATA_DIR` to move both `config/` and `runtime/` together. |
| `--runtime-dir PATH`|                      | Override the runtime directory (where `daemon.json` lives). No per-subdir env var.   |
|                     | `DH_MCP_DATA_DIR`    | Override the **user-data root**; `config/` and `runtime/` resolve under it. |
| `-o`, `--output`    | `DH_MCP_OUTPUT`      | One of `human`, `json`, `yaml`. Overrides `cli.json`'s `output.format`.              |
| `--timeout SECS`    |                      | Per-request timeout. Overrides `cli.json`'s `request.timeouts.default_seconds`.      |
| `-v`, `--verbose`   |                      | Increase logging verbosity (`-v`=INFO, `-vv`=DEBUG). Mutually exclusive with `-q`.   |
| `-q`, `--quiet`     |                      | Suppress non-error logging (root logger at ERROR). Mutually exclusive with `-v`.     |
| `--no-auto-start`   |                      | Fail rather than spawn a daemon when none is running.                                |
| `--version`         |                      | Print the package version and exit.                                                  |

## Output modes

Every verb honors `-o/--output`, selecting how its result is rendered. The mode
is resolved per invocation: `-o/--output` flag → `DH_MCP_OUTPUT` → `cli.json`'s
`output.format` (default `human`).

- `human` (default) — terminal-friendly. Row/tabular data and `tool list` render
  as aligned, header-topped tables (sized to the terminal width, falling back to
  80 columns when output is not a TTY); objects render as `key: value` lines;
  plain lists render one item per line; an empty list renders as `(none)`.
- `json` — a single document via `json.dumps(..., indent=2, sort_keys=True)`:
  indented and key-sorted, so output is stable and diff-/`jq`-friendly.
- `yaml` — block style with sorted keys (`yaml.safe_dump`), for `yq` or
  human-readable structured output.

Errors and warnings always go to **stderr**, leaving stdout clean for piping. In
`human` mode an error prints as a single line `<command>: <message>`; in `json` /
`yaml` mode it prints as the structured payload described under
[Structured errors](#structured-errors-for-ai-agents).

## Exit codes

| Code  | Meaning                                                                  |
|-------|--------------------------------------------------------------------------|
| `0`   | Success.                                                                 |
| `2`   | User-facing failure (CLI argument error, daemon-spawn timeout, etc.).    |
| `3`   | The invoked MCP tool returned `isError=true`.                            |

For **batch / vector verbs** (e.g. `pq delete`, `pq start` / `stop` /
`restart` given multiple ids) exit `0` means the operation *executed*, not
that every item succeeded — these are best-effort. Read the payload's
`summary` (`succeeded` / `failed`) and per-item `results` for individual
outcomes; exit `3` is reserved for a tool that did not execute at all
(`isError=true`).

Some verbs also emit **non-fatal warnings on stderr** while returning their
result on stdout — e.g. `session list` warns about enterprise systems that
failed discovery, yet still lists the sessions it found. Warnings go to
stderr so `-o json` / `-o yaml` stdout stays clean for piping.

## Structured errors (for AI agents)

When run with `-o json` or `-o yaml`, any user-facing failure is
emitted on **stderr** as a structured payload with these keys:

```json
{
  "error": "<human-readable message>",
  "error_code": "<stable identifier; see registry below>",
  "exit_code": 2,
  "command": "daemon start"
}
```

The `error_code` values are stable across releases. Get the full
registry programmatically via `dh-mcp introspect` (look under
`error_codes`). Current set:

| `error_code`                  | Meaning                                                            |
|-------------------------------|--------------------------------------------------------------------|
| `daemon_startup_timeout`      | Daemon was spawned but did not publish a registry entry in time.   |
| `daemon_not_running`          | No running daemon was found: either none is registered and `--no-auto-start` was specified, or a command that needs the daemon's files (e.g. `daemon logs`) found none yet. |
| `daemon_client_error`         | A client-side daemon-management failure (signal denied, etc.).     |
| `daemon_registry_corrupt`     | `daemon.json` exists but cannot be parsed. Recover with `dh-mcp daemon reset`. |
| `daemon_registry_live`        | `dh-mcp daemon reset` refused to quarantine `daemon.json` because a live daemon is still registered; run `dh-mcp daemon stop` first. |
| `mcp_request_failed`          | The MCP transport reported an error (connect, timeout, parse).     |
| `tool_not_found`              | `dh-mcp tool show/call` referenced an unknown tool name.           |
| `tool_returned_error`         | The invoked tool returned `isError=true`. Exit code `3`.           |
| `arg_parse_error`             | A `key=value` token (`--arg`, `--env`, `--session-arg`) was malformed. |
| `option_not_applicable`       | An option/argument is invalid for the selected `--system` type (an inapplicable option, or a missing required one such as a Community session name). |
| `browser_launch_failed`       | `dh-mcp session open` could not launch a browser; the URL is included in the error message to open manually. |
| `config_invalid`              | The configuration tree failed validation.                          |
| `internal_error`              | An unexpected internal failure not attributable to a specific subsystem. |

## Troubleshooting

**Daemon refuses to start / `dh-mcp daemon start` times out.**

Inspect `<runtime_dir>/daemon/daemon.log`. The daemon writes its
stdout and stderr there; configuration errors and import failures
appear in plain text. Once you've fixed the cause, the registry
will be written on the next successful spawn.

**Stale registry after a crash.**

If the daemon crashes without cleaning up, `dh-mcp` detects the
mismatch between the registered PID and the running process names,
purges the registry, and (when auto-start is enabled) spawns a
fresh daemon. You can also delete `<runtime_dir>/daemon/daemon.json`
manually.

**Corrupt registry (`daemon_registry_corrupt`).**

If `daemon.json` exists but cannot be parsed, every lifecycle verb
(`start`, `stop`, `status`, `restart`) fails with
`daemon_registry_corrupt` rather than silently auto-recovering —
the daemon process (if any) is unaffected, but a corrupt file
co-existing with a still-live daemon would otherwise produce a
confusing port-bind timeout. Recovery:

```bash
dh-mcp daemon status   # confirm whether a daemon is still running
dh-mcp daemon stop     # if it is, stop it first
dh-mcp daemon reset    # quarantine the corrupt file (renames to daemon.json.corrupt-<UTC>)
dh-mcp daemon start    # fresh spawn
```

`daemon reset` refuses to run while a live daemon is still
registered (`daemon_registry_live`) so you cannot accidentally
orphan a running process.

**Permission errors on the runtime directory.**

The CLI auto-tightens `runtime_dir`, `daemon/`, and `daemon.json`
on every invocation (see [Runtime directory](#runtime-directory)).
If you still see `PermissionError`, the issue is almost always
*ownership*, not mode — e.g., a daemon spawned by `sudo` left a
root-owned registry. Repair with:

```bash
sudo chown -R $USER ~/.deephaven/ai/runtime
```

**`No daemon is running and auto-start is disabled.`**

You set `daemon.auto_start: false` in `cli.json`. Run `dh-mcp
daemon start` explicitly, or flip the config back to `true`.

## Security model

- The daemon binds loopback only (`127.0.0.1`). Non-loopback bind
  is rejected at startup.
- A fresh PSK is generated per daemon startup, stored in the
  registry, and never written elsewhere. The CLI sends it in the
  `X-Deephaven-PSK` header on every request.
- On POSIX the CLI re-applies `0700` to `runtime_dir` and
  `daemon/`, and `0600` to `daemon.json`, on every invocation.
  Looser modes are silently corrected; the CLI does not refuse to
  operate on the basis of mode alone. Ownership is *not* audited
  — keep the parent `runtime_dir` outside of any shared
  filesystem path.
- The daemon is per-user: cross-user use is intentionally not
  supported; running the CLI as a different user from the daemon
  process produces a clear permission-denied error.
