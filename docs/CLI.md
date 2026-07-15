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
  - [Configuration loading](#configuration-loading)
  - [`cli.json`](#clijson)
  - [`server.json` — `daemon` block](#serverjson--daemon-block)
- [Runtime directory](#runtime-directory)
- [Command tree](#command-tree)
  - [`dh-mcp daemon`](#dh-mcp-daemon)
  - [`dh-mcp tool`](#dh-mcp-tool)
  - [`dh-mcp session`](#dh-mcp-session)
  - [`dh-mcp system`](#dh-mcp-system)
  - [`dh-mcp table`](#dh-mcp-table)
  - [`dh-mcp catalog`](#dh-mcp-catalog)
  - [`dh-mcp pq`](#dh-mcp-pq)
  - [`dh-mcp docs`](#dh-mcp-docs)
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
dh-mcp introspect tree | jq .

# Machine-readable description of one command (twin of --help).
dh-mcp daemon start --introspect
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

The only invocations that bypass the load are help and self-introspection:

- `dh-mcp --help`, `dh-mcp <noun> --help`, `dh-mcp <noun> <verb> --help` — the help text is rendered without touching configuration so you can navigate the CLI surface against a broken tree.
- `dh-mcp introspect <verb>` — emits machine-readable metadata as JSON for agents that need to learn the surface before any config exists.
- `--introspect` at any depth (`dh-mcp --introspect`, `dh-mcp <noun> <verb> --introspect`) — the machine-readable twin of `--help`, likewise rendered without touching configuration.

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

The schema is organized into four top-level domain sections. Each section
is its own object (Pydantic model); sections that hold time-shaped knobs
carry a `timeouts:` sub-section reserved from day one, so future timeouts
slot in without a breaking schema change.

#### `output.*` — presentation

| Field                | Type    | Default | Notes                                                                       |
|----------------------|---------|---------|-----------------------------------------------------------------------------|
| `output.format`      | string  | `json` | One of `human`, `json`, `yaml`. Machine-first default; set `human` for interactive use. Override per invocation with `-o/--output`. |

#### `daemon.*` — CLI-side daemon lifecycle

| Field                                       | Type    | Default | Notes                                                                          |
|---------------------------------------------|---------|---------|--------------------------------------------------------------------------------|
| `daemon.auto_start`                         | bool    | `true`  | When `false`, `dh-mcp` exits if no daemon is running.                          |
| `daemon.reuse.version`                      | string  | `refuse`| Action when the running daemon's package version differs from the CLI's. One of `ignore`, `warn`, `restart`, `refuse`. |
| `daemon.reuse.venv`                         | string  | `refuse`| Action when the daemon's virtualenv (`sys.prefix`) differs from the CLI's. One of `ignore`, `warn`, `restart`, `refuse`. |
| `daemon.reuse.fingerprint`                  | string  | `warn`  | Action when only the source fingerprint differs (an in-place code edit at the same version + venv). One of `ignore`, `warn`, `restart`, `refuse`. |
| `daemon.timeouts.startup_deadline_seconds`  | integer | `30`    | How long the CLI waits for a freshly-spawned daemon to publish its registry.   |
| `daemon.timeouts.kill_after_seconds`        | integer | `10`    | How long `daemon stop`/`restart` waits after `SIGTERM` before escalating to `SIGKILL`. |

The CLI verifies a running daemon is the *same build* it ships from — not
merely alive — before reusing it, comparing three identity fields:
package **version**, **venv** (`sys.prefix`), and a **source fingerprint**
(a hash of the installed package's `*.py` files). Each differing field
maps to an action via `daemon.reuse`; when several differ the **most
severe** action wins, ordered `ignore < warn < restart < refuse`. `restart`
degrades to `refuse` when `auto_start` is `false` (a restart implies a spawn).
The defaults refuse on a version or venv change (real drift) and merely warn
on a fingerprint-only change (an in-place edit), so developers iterate without
restarting while end users are still protected.

#### `request.*` — outbound MCP requests

| Field                                  | Type    | Default | Notes                                            |
|----------------------------------------|---------|---------|--------------------------------------------------|
| `request.timeouts.default_seconds`     | integer | `60`    | Per-request timeout. Override with `--timeout`.  |

#### `docs.*` — docs MCP server

| Field                          | Type    | Default | Notes                                                                        |
|--------------------------------|---------|---------|-------------------------------------------------------------------------------|
| `docs.url`                     | string  | `https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/mcp` | Streamable-HTTP endpoint of the docs MCP server the `docs` commands query. Defaults to the Deephaven-hosted production docs server; point it at a self-hosted `dh-mcp-docs-server` to query that instead. |
| `docs.timeouts.request_seconds`| integer | `120`   | Per-request timeout for docs server calls. Higher than the daemon request default because docs queries are LLM-backed. Override with `--timeout`. |

Example (JSON5; `//` comments are accepted):

```json5
{
  "output": {
    "format": "json"
  },
  "daemon": {
    "auto_start": true,
    "reuse": {
      "version": "refuse",        // ignore | warn | restart | refuse
      "venv": "refuse",
      "fingerprint": "warn"
    },
    "timeouts": {
      "startup_deadline_seconds": 30,
      "kill_after_seconds": 10
    }
  },
  "request": {
    "timeouts": {
      "default_seconds": 30
    }
  },
  "docs": {
    "url": "https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/mcp",
    "timeouts": {
      "request_seconds": 120
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
| `start`    | Idempotently spawn the daemon (or report the existing one). Returns the shared `{state, message, daemon, paths}` envelope with `state: "running"`. |
| `stop`     | Idempotent SIGTERM (escalating to SIGKILL); removes the registry file.                        |
| `status`   | Reports the daemon's `state` (`running`/`stopped`/`crashed`) and a human `message`. Includes a `daemon` object — the running daemon's registry entry, redacted: `pid`, `create_time_ns`, `process_name`, `host`, `port`, redacted `psk`, `started_at`, `config_dir`, `server_name`, and a `build_identity` sub-object with `version`, `venv`, `fingerprint` — **only when running**, and always includes `paths` (config, runtime, registry, log). Read-only: a `crashed` entry is reported, not cleaned up — use `start` or `repair`. Exits 0 in all three states. |
| `restart`  | `stop` then `start` in one shot; returns the same `{state, message, daemon, paths}` envelope as `start`. |
| `repair`   | Recovers from a corrupt `daemon.json` by moving it aside to `daemon.json.corrupt-<UTC>` so a fresh `start` can write a clean registry. Refuses while a live daemon is still registered (`daemon_registry_live`). |
| `logs`     | Tails `daemon.log`. `-n/--lines N` controls the initial tail (default 100); `-f/--follow` follows the file (Ctrl-C to exit); `--path` prints the absolute log-file path and exits (works even if the daemon has never started). |

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
dh-mcp tool list
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
| `list`                        | Lists sessions (both types). Filters: `--type community\|enterprise`, `--system NAME`, `--origin static\|dynamic\|discovered`. Wraps `sessions_list`. |
| `show <id>`                   | Shows one session's detail object. `--connect` actively verifies liveness. Wraps `session_details`. |
| `create [NAME] --system SYS`  | Creates a session. `--system community` (default) → local Community worker (`NAME` required); any other system → an Enterprise worker on that named system, `NAME` optional/auto-generated (discover system names with `system list`). Wraps `session_community_create` / `session_enterprise_create`. |
| `delete <id>`                 | Deletes a session, routing by the id prefix. Wraps `session_community_delete` / `session_enterprise_delete`. |
| `exec <id>`                   | Runs a script in the session via `--script TEXT`, `--script-path PATH` (read by the CLI), or `--script-path -` (stdin); supply exactly one. Wraps `session_script_run`. |
| `pip-list <id>`               | Lists the session's installed pip packages as a `{package, version}` array. Wraps `session_pip_list`. |
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

For an Enterprise system, `create` is *create-and-connect*: it provisions
a Persistent Query and connects immediately, and `delete` also deletes
the underlying PQ (equivalent to `pq delete` with the same id). To define
a durable PQ without connecting — scheduled, RunAndDone, or disabled —
use `pq create` instead; see [`dh-mcp pq`](#dh-mcp-pq).

`exec` takes exactly one script source: `--script TEXT` (inline),
`--script-path PATH` (a local file), or `--script-path -` (standard
input). The file is read by the CLI itself, so a relative path resolves
against your working directory; an unreadable file exits `2`
(`file_read_failed`). Supplying no source or several exits `2`
(`missing_argument` / `mutually_exclusive_options`); a script error
exits `3`.

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
dh-mcp session exec community:community:dev --script 'print(1)'
cat job.py | dh-mcp session exec community:community:dev --script-path -
dh-mcp session pip-list community:community:dev
dh-mcp session delete community:community:dev
dh-mcp session open community:community:dev --print
```

### `dh-mcp system`

A *system* is the source dimension of every fully qualified session id
(`type:system:name`): the single Community umbrella (named `community`)
plus every configured Enterprise (Core+) system.

| Verb         | Purpose                                                                                       |
|--------------|-----------------------------------------------------------------------------------------------|
| `list`       | Lists every configured system as `{name, type}` pairs — use the names with `session create --system NAME`. Wraps `list_systems`. |
| `status`     | Reports Enterprise (Core+) system health as a compact array of per-system records (`name`, `type`, `liveness_status`, `is_alive`, `liveness_detail`). Wraps `enterprise_systems_status`. Health only — use `dh-mcp config show` for configuration. Enterprise-only: an all-Community deployment returns an empty list. `--system NAME` scopes to one system; `--connect` actively verifies connectivity instead of reading cached state. `liveness_detail` is a short reason code: when `--connect` probed the system, the probe's own message; otherwise, when discovery recorded an error, the kubectl-style exception-type prefix (e.g. `DeephavenConnectionError`). When discovery is still running or has failed, a phase-summary warning is written to stderr; when `partial_result.errors` is present, stderr also includes a per-system details map with the full failure messages. The completed-phase banner may be suppressed when reasons are already in each row's `liveness_detail`. Exits `3` if the tool reports failure. |
| `url <name>` | Prints an Enterprise system's web console URL — pipe-friendly. |
| `open <name>`| Opens the Enterprise system's web console in the default browser; `--print` prints the URL instead (headless-safe). |

`url` and `open` are Enterprise-only and computed locally from
configuration — they do **not** contact the daemon. The URL is the
system's `connection_json_url` origin with the `/iriside` path (e.g.
`https://dhe.example.com:8123/iriside`). Unlike `session url`, the URL is
**unauthenticated**: you log in interactively in the browser (Deephaven
Enterprise has no token-in-URL). They exit `2` (`system_not_found`) when
no Enterprise system has that name — including `community`, which has no
web console — and `open` exits `2` (`browser_launch_failed`, with the URL
in the message) when no browser can be launched.

Examples:

```bash
dh-mcp system list
dh-mcp system list | jq '.[].name'
dh-mcp system status --system prod --connect
dh-mcp system url prod
dh-mcp system open prod --print
```

### `dh-mcp table`

Inspects tables in a session. All verbs take a fully qualified
`ID`.

| Verb                          | Purpose                                                                                       |
|-------------------------------|-----------------------------------------------------------------------------------------------|
| `list <id>`                   | Lists the session's table names. Wraps `session_tables_list`. |
| `schema <id> [TABLE...]`      | Column definitions for the named tables (all tables when none named). Wraps `session_tables_schema`. |
| `data <id> <table>`           | Row data: `--max-rows N`, `--head/--tail` (default head). Wraps `session_table_data`. |

```bash
dh-mcp table list community:community:dev
dh-mcp table data community:community:dev trades --max-rows 50 --tail
```

### `dh-mcp catalog`

**Enterprise (Core+) only.** Queries an enterprise session's catalog
(database); `ID` must name an enterprise session.

| Verb                                  | Purpose                                                                               |
|---------------------------------------|---------------------------------------------------------------------------------------|
| `tables <id>`                         | Catalog table metadata. `--max-rows`, `--filter` (repeatable). Wraps `catalog_tables_list`. |
| `namespaces <id>`                     | Lists the catalog's namespace names. Same options as `tables`. Wraps `catalog_namespaces_list`. When the list is truncated by `--max-rows`, a warning is written to stderr. |
| `schema <id> [TABLE...]`              | Catalog table schemas. `--namespace`, `--filter` (repeatable), `--max-tables`. Wraps `catalog_tables_schema`. |
| `sample <id> <namespace> <table>`    | Sample rows. `--max-rows`, `--head/--tail`, `--filter` (repeatable). Wraps `catalog_table_sample`. |

```bash
dh-mcp catalog tables enterprise:prod:42
dh-mcp catalog sample enterprise:prod:42 Market Trades --max-rows 20
```

### `dh-mcp pq`

**Enterprise (Core+) only.** Manages Persistent Queries, addressed by
their fully qualified id `enterprise:<system>:<serial>` — the same id the
`session` verbs use (`pq name-to-id` resolves a name within a system).

A PQ and an enterprise session are two lenses on the same controller
object, addressed by the same id: `pq` manages the durable definition
and lifecycle (create, configure, schedule, start/stop), while `session`
interacts with a live worker (scripts, tables, credentials). Pass a
running PQ's id to either noun's verbs verbatim. Community sessions have
no PQ counterpart (local workers are ephemeral).

| Verb                                  | Purpose                                                                               |
|---------------------------------------|---------------------------------------------------------------------------------------|
| `list <system>`                       | Lists PQs configured on a system. Wraps `pq_list`. |
| `details <id>`                        | Configuration + status for one PQ. Wraps `pq_details`. |
| `name-to-id <system> <name>`          | Resolves a PQ name to its fully qualified id. Wraps `pq_name_to_id`. |
| `create <name> --system S --heap-size-gb N` | Creates a PQ on `--system` with `--heap-size-gb` of heap. Script via `--script-body`/`--script-body-path`/`--git-script-path`; see the config flags below. Unset flags use controller defaults. Wraps `pq_create`. |
| `modify <id>`                         | Updates only the fields passed; everything else is left unchanged. `--restart` restarts the PQ after applying the change. Wraps `pq_modify`. |
| `delete <id>...`                      | Deletes one or more PQs. `--max-concurrent N`. Wraps `pq_delete`. |
| `start <id>...`                       | Starts one or more PQs. `--wait/--no-wait`, `--max-concurrent`. Wraps `pq_start`. |
| `stop <id>...`                        | Stops one or more PQs. Same options as `start`. Wraps `pq_stop`. |
| `restart <id>...`                     | Restarts one or more PQs. Same options as `start`. Wraps `pq_restart`. |

`create` and `modify` share a large optional config flag set; only the flags you
pass take effect (on `modify`, everything else is left unchanged). `create`
additionally requires `--system` and `--heap-size-gb` and accepts
`--enabled/--disabled` (default enabled); `modify` takes `ID` and accepts
`--pq-name`, `--heap-size-gb`, `--enabled/--disabled`, and `--restart`. The
shared flags are: `--script-body`/`--script-body-path`/`--git-script-path`,
`--language` (`Python`/`Groovy`), `--configuration-type` (`Script`/`RunAndDone`),
`--schedule` (repeatable), `--server`, `--engine`, `--jvm-profile`, `--jvm-arg`
(repeatable), `--class-path` (repeatable), `--python-venv`, `--env KEY=VALUE`
(repeatable), `--init-timeout-nanos`, `--auto-delete-timeout`,
`--admin-group`/`--viewer-group` (repeatable), `--restart-users`, and `--owner`.
Run `dh-mcp pq create --help` (or `modify`) for the full per-flag detail.
The three script sources and `--auto-delete-timeout`/`--schedule` are each
mutually exclusive; combining them exits `2` with `mutually_exclusive_options`.

The script sources differ in where the file lives: `--script-body TEXT` is
inline source stored in the PQ definition; `--script-body-path PATH|-` is a
**local** file (or stdin) read by the CLI — like `session exec --script-path`
— and stored as the inline body (unreadable file exits `2`,
`file_read_failed`); `--git-script-path PATH` is a path into the Enterprise
controller's Git-backed script repository, resolved **on the server** each
time the PQ starts (use it for version-controlled scripts). `--python-venv`
and `--class-path` also name resources on the Enterprise server, not this
machine.

`delete` / `start` / `stop` / `restart` are best-effort across multiple ids:
exit `0` means the batch ran, not that every id succeeded — check the
`summary` and per-item `results` in the payload for failures.

```bash
dh-mcp pq create nightly --system prod --heap-size-gb 4 --script-body-path ./n.py
dh-mcp pq create weekly --system prod --heap-size-gb 4 --git-script-path IrisQueries/py/weekly.py
dh-mcp pq restart enterprise:prod:1234567890 --no-wait
```

### `dh-mcp docs`

Queries the Deephaven documentation MCP server. These verbs connect
**directly** to the docs server configured as `docs.url` in `cli.json`
(default: the Deephaven-hosted production docs server at
`https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/mcp`) — the local
daemon is not involved and is never started.

| Verb     | Purpose                                                                                       |
|----------|-----------------------------------------------------------------------------------------------|
| `ask`    | Sends a one-shot question (`PROMPT`) to the documentation assistant and prints its answer (an object with a `response` field). Wraps the docs server's `docs_chat` tool. |
| `status` | Checks that the configured docs server is reachable: initializes an MCP session, lists its tools, and reports `{url, reachable, tools, latency_ms}`. Exits `2` with `mcp_request_failed` when unreachable. |

`ask` accepts `--language` (`python`/`groovy`) to tailor code examples,
`--core-version VERSION` / `--enterprise-version VERSION` to tailor the
answer to a Deephaven release, and `--history JSON` — a JSON array of
objects, each with exactly two fields, `role` (`user` or `assistant`)
and a string `content`, oldest first — to carry a prior exchange into a
follow-up question. A malformed `--history` exits `2` with
`arg_parse_error`; an assistant-reported failure exits `3` with
`tool_returned_error`.

The assistant is LLM-backed, so answers typically take several seconds;
the docs request timeout defaults to `docs.timeouts.request_seconds`
(120) and honors the `--timeout` flag.

```bash
dh-mcp docs ask "How do I join two tables?"
dh-mcp docs ask "Show me a ring table example" --language python
dh-mcp docs ask "What is a liveness scope?" | jq -r .response
dh-mcp docs status
```

### `dh-mcp config`

| Verb        | Purpose                                                                                       |
|-------------|-----------------------------------------------------------------------------------------------|
| `show`      | Prints the resolved configuration with secrets redacted.                                      |
| `validate`  | Confirms the configuration is valid; exits `0`, or `2` with `config_invalid` if any file is malformed. Validation is eager, so this is CI-friendly. |

### `dh-mcp introspect`

Machine-readable CLI metadata for AI-agent self-discovery — prefer it
over scraping `--help`. There are two complementary ways to reach it:
the `--introspect` flag for one command in place, and the `introspect`
group for whole-system views. Both honor the root `-o/--output` flag
and `DH_MCP_OUTPUT` (`human`, `json`, or `yaml`) and, like every
command, **default to `json`** — pass `-o human` for terminal-friendly
output. Both run without a valid configuration tree, so they work even
when `config validate` fails; that same bypass means they cannot read
`cli.json`'s `output.format` (use `-o`/`DH_MCP_OUTPUT` instead).

#### The `--introspect` flag (twin of `--help`)

Append `--introspect` to any command, at any depth, to emit just that
command's node — the machine-readable counterpart of `--help`:

```bash
dh-mcp daemon start --introspect    # the start verb node
dh-mcp daemon --introspect          # the daemon group node (its verbs included)
dh-mcp --introspect                 # the whole-tree manifest
```

A command's node carries its `name`, help text, options and arguments
(`params`), any `subcommands`, its `output` shape, and which MCP tool it
`wraps`. Like click's own `--help`, the universal `--help` and
`--introspect` flags are *not* listed under any command's `params`; the
whole-tree manifest discloses them once under `universal_options`.
`dh-mcp --introspect` (on the root) instead emits the whole-tree manifest
described below.

#### The `introspect` group

| Verb                 | Output                                                                                  |
|----------------------|-----------------------------------------------------------------------------------------|
| `tree`               | The whole-tree manifest: package `version`, the full `commands` tree, `global_options`, `universal_options` (the every-command flags `--help` and `--introspect`), the project-wide `error_codes`, and the other top-level fields. Identical to `dh-mcp --introspect`. |
| `command PATH...`    | One command's node, resolved from `PATH` (one or more command-name tokens; required). Identical to appending `--introspect` to that command. A path that does not resolve exits `2` with `command_not_found`. |
| `errors`             | The stable `error_code` registry (`code` + `help`) — also the `error_codes` key of `tree`. |

```bash
dh-mcp introspect tree | jq '.commands | keys'
dh-mcp introspect command daemon start    # == dh-mcp daemon start --introspect
dh-mcp introspect errors | jq '.[].code'
```

The `command` node is byte-identical to the object found at
`.commands.<path…>` in `tree` — e.g. `dh-mcp introspect command daemon
start` equals `dh-mcp introspect tree | jq
'.commands.daemon.subcommands.start'`.

The whole-tree fields (`version`, `error_codes`, `universal_options`,
...) appear only in `tree` (and `dh-mcp --introspect`); a single
command's node never carries them.

> **Migration:** `dh-mcp introspect` alone is now a command group and
> lists its verbs (it no longer emits the manifest). Use `dh-mcp
> introspect tree` (or `dh-mcp --introspect`) for the old whole-tree
> JSON.

## Top-level flags

| Flag                | Envvar               | Purpose                                                                              |
|---------------------|----------------------|--------------------------------------------------------------------------------------|
| `--config-dir PATH` |                      | Override the configuration directory. No per-subdir env var; use `DH_MCP_DATA_DIR` to move both `config/` and `runtime/` together. |
| `--runtime-dir PATH`|                      | Override the runtime directory (where `daemon.json` lives). No per-subdir env var.   |
|                     | `DH_MCP_DATA_DIR`    | Override the **user-data root**; `config/` and `runtime/` resolve under it. |
| `-o`, `--output`    | `DH_MCP_OUTPUT`      | One of `human`, `json`, `yaml`. Overrides `cli.json`'s `output.format`.              |
| `--timeout SECS`    |                      | Per-request timeout. Overrides `cli.json`'s `request.timeouts.default_seconds` (and `docs.timeouts.request_seconds` for the `docs` commands). |
| `--introspect`      |                      | Emit the command's manifest node and exit (machine-readable twin of `--help`); available on every command. Rendered in the `-o`/`DH_MCP_OUTPUT` mode, `json` by default. On the root, emits the whole-tree manifest. |
| `-v`, `--verbose`   |                      | Increase logging verbosity (`-v`=INFO, `-vv`=DEBUG). Mutually exclusive with `-q`.   |
| `-q`, `--quiet`     |                      | Suppress non-error logging (root logger at ERROR). Mutually exclusive with `-v`.     |
| `--no-auto-start`   |                      | Fail rather than spawn a daemon when none is running.                                |
| `--version`         |                      | Print the package version and exit.                                                  |

These flags accept any position on the command line — before the
noun group, between the noun and the verb, or after the verb.
For example `dh-mcp -o json config show`, `dh-mcp config -o json
show`, and `dh-mcp config show -o json` are all equivalent. The
CLI rewrites argv to lift recognized top-level options to the
front before `click` parses it. `--help` and `--version` are
*not* lifted: Click resolves them per-command, so `dh-mcp daemon
--help` correctly renders the `daemon` group's help (not the
root's). Use the POSIX `--` sentinel to force a literal token
later in the command line (everything after `--` is preserved
verbatim).

## Output modes

Every verb honors `-o/--output`, selecting how its result is rendered. The mode
is resolved per invocation: `-o/--output` flag → `DH_MCP_OUTPUT` → `cli.json`'s
`output.format` (default `json`). The CLI is **machine-first** (primarily driven
by AI agents), so the default is `json`; for human-readable output, pass
`-o human`, set `DH_MCP_OUTPUT=human`, or set `output.format: "human"` in
`cli.json`. `dh-mcp introspect`, the `--introspect` flag, and error output run
without the validated config, so they skip the `cli.json` step — use
`-o`/`DH_MCP_OUTPUT` for those (so `DH_MCP_OUTPUT=human` is the most complete way
to get human output everywhere).

- `json` (default) — a single document via `json.dumps(..., indent=2, sort_keys=True)`:
  indented and key-sorted, so output is stable and diff-/`jq`-friendly.
- `human` — terminal-friendly. Row/tabular data and `tool list` render
  as aligned, header-topped tables (sized to the terminal width, falling back to
  80 columns when output is not a TTY); objects render as an indented tree
  (nested keys indented two spaces under their parent, scalar leaves as
  `key: value`, scalar lists as `- item` bullets); plain lists render one item
  per line; an empty list renders as `(none)`.
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
registry programmatically via `dh-mcp introspect errors` (or the
`error_codes` key of `dh-mcp introspect tree`). Current set:

| `error_code`                  | Meaning                                                            |
|-------------------------------|--------------------------------------------------------------------|
| `daemon_startup_timeout`      | Daemon was spawned but did not publish a registry entry in time.   |
| `daemon_not_running`          | No running daemon was found: either none is registered and `--no-auto-start` was specified, or a command that needs the daemon's files (e.g. `daemon logs`) found none yet. |
| `daemon_client_error`         | A client-side daemon-management failure (signal denied, etc.).     |
| `daemon_registry_corrupt`     | `daemon.json` exists but cannot be parsed. Recover with `dh-mcp daemon repair`. |
| `daemon_registry_live`        | `dh-mcp daemon repair` refused to move `daemon.json` aside because a live daemon is still registered; run `dh-mcp daemon stop` first. |
| `daemon_reuse_refused`        | The running daemon is a different build than the CLI (version, venv, or source fingerprint differs) and `daemon.reuse` resolved to `refuse`. Run `dh-mcp daemon restart`, or adjust `daemon.reuse` in `cli.json`. |
| `mcp_request_failed`          | The MCP transport reported an error (connect, parse, server failure). |
| `mcp_request_timeout`         | The MCP request timed out. The server may still finish the operation — verify the resulting state before retrying. Allow more time with `--timeout`, or raise the timeout in `cli.json`: `request.timeouts.default_seconds` (`docs.timeouts.request_seconds` for the `docs` commands). |
| `tool_not_found`              | `dh-mcp tool show/call` referenced an unknown tool name.           |
| `tool_returned_error`         | The invoked tool returned `isError=true`. Exit code `3`.           |
| `arg_parse_error`             | A `key=value` token (`--arg`, `--env`, `--session-arg`) was malformed. |
| `command_not_found`           | `dh-mcp introspect command PATH` referenced a command path that does not exist. |
| `missing_argument`            | A required positional argument or option was not provided.         |
| `mutually_exclusive_options`  | Two or more options that cannot be combined were supplied together. |
| `file_read_failed`            | A local file passed on the command line could not be read.          |
| `option_not_applicable`       | An option/argument is invalid for the selected `--system` type (an inapplicable option, or a missing required one such as a Community session name). |
| `browser_launch_failed`       | `dh-mcp session open` / `system open` could not launch a browser; the URL is included in the error message to open manually. |
| `system_not_found`            | `dh-mcp system url/open NAME` named an Enterprise system that is not configured (`community` included — it has no web console). |
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
dh-mcp daemon status    # confirm whether a daemon is still running
dh-mcp daemon stop      # if it is, stop it first
dh-mcp daemon repair    # move the corrupt file aside (renames to daemon.json.corrupt-<UTC>)
dh-mcp daemon start     # fresh spawn
```

`daemon repair` refuses to run while a live daemon is still
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

**Build mismatch (`daemon_reuse_refused`).**

A daemon is a persistent process running a specific build of the code; the
CLI verifies the running daemon matches the build it ships from before
reusing it. After an upgrade (`pip install -U`), or when a different
virtualenv's daemon is still running, the CLI refuses (by default) rather
than silently driving a stale daemon. Replace it:

```bash
dh-mcp daemon restart   # stop the stale daemon and spawn a fresh one
```

To relax the policy per field, set `daemon.reuse` in `cli.json`
(see the `daemon.*` configuration table above). For parallel
development across multiple checkouts, give each its own daemon by pointing
`DH_MCP_DATA_DIR` at a per-worktree directory (e.g.
`export DH_MCP_DATA_DIR="$PWD/.dh-mcp-data"`), so switching checkouts never
tears down another's daemon.

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
