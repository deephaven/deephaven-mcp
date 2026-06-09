# `dh-mcp` CLI

The `dh-mcp` command-line tool is a thin local client for the
multiplexed Deephaven MCP systems server. It manages a per-user
background daemon, lists registered MCP tools, and dispatches tool
calls — without requiring you to run the server yourself.

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
| `logs`     | Tails `daemon.log`. `-n N` controls the initial tail; `-f` follows the file (Ctrl-C to exit). |

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
|                     | `DH_MCP_DATA_DIR`    | Override the **user-data root**; ``config/`` and ``runtime/`` resolve under it. |
| `-o`, `--output`    | `DH_MCP_OUTPUT`      | One of `human`, `json`, `yaml`. Overrides `cli.json`'s `output.format`.              |
| `--timeout SECS`    |                      | Per-request timeout. Overrides `cli.json`'s `request.timeouts.default_seconds`.      |
| `-v`, `--verbose`   |                      | Increase logging verbosity (`-v`=INFO, `-vv`=DEBUG). Mutually exclusive with `-q`.   |
| `-q`, `--quiet`     |                      | Suppress non-error logging (root logger at ERROR). Mutually exclusive with `-v`.     |
| `--no-auto-start`   |                      | Fail rather than spawn a daemon when none is running.                                |
| `--version`         |                      | Print the package version and exit.                                                  |

## Exit codes

| Code  | Meaning                                                                  |
|-------|--------------------------------------------------------------------------|
| `0`   | Success.                                                                 |
| `2`   | User-facing failure (CLI argument error, daemon-spawn timeout, etc.).    |
| `3`   | The invoked MCP tool returned `isError=true`.                            |

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
| `arg_parse_error`             | A `--arg key=value` token was malformed.                           |
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
