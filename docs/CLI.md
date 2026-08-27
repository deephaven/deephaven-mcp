# `dhcli` — the Deephaven CLI

`dhcli` is the [Deephaven](https://deephaven.io) command-line tool,
designed for humans and especially AI agents. It inspects and operates
Deephaven systems from the shell — sessions, tables, catalogs,
persistent queries, documentation Q&A — with machine-first structured
output on every command. Today its runtime commands are backed by a
per-user background daemon hosting the multiplexed Deephaven MCP
systems server, which `dhcli` manages for you — that is the current
mechanism, not the tool's scope.

> **`dhcli` is under rapid development.** Command names, flags, and
> output shapes can change without notice, so an upgrade may break
> scripts written against them — pin a version if you need stability.
> AI agents need no such care: `dhcli` describes itself at runtime
> through [`dhcli agents tree`](#dhcli-agents) and `--agents`, so an
> agent that reads that manifest adapts to the changes on its own.

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
  - [Choosing a target](#choosing-a-target)
  - [`dhcli daemon`](#dhcli-daemon)
  - [`dhcli tool`](#dhcli-tool)
  - [`dhcli session`](#dhcli-session)
  - [`dhcli system`](#dhcli-system)
  - [`dhcli table`](#dhcli-table)
  - [`dhcli catalog`](#dhcli-catalog)
  - [`dhcli pq`](#dhcli-pq)
  - [`dhcli context`](#dhcli-context)
  - [`dhcli docs`](#dhcli-docs)
  - [`dhcli config`](#dhcli-config)
  - [`dhcli agents`](#dhcli-agents)
  - [`dhcli self`](#dhcli-self)
- [Top-level flags](#top-level-flags)
- [Output modes](#output-modes)
- [Exit codes](#exit-codes)
- [Structured errors (for AI agents)](#structured-errors-for-ai-agents)
- [Troubleshooting](#troubleshooting)
- [Security model](#security-model)

## Installation

`dhcli` ships with the `deephaven-mcp` package; installing the
package wires the entry point automatically:

```bash
uv tool install --python-preference managed "deephaven-mcp"
```

That is the recommended path, and it puts `dhcli` alongside
`dh-mcp-systems-server` and `dh-mcp-docs-server` on your `$PATH` with no
venv to manage. The venv-based and standalone-binary alternatives are in
the project [`README.md`](../README.md#installation-methods). Working in a
clone of this repository, `uv sync --all-extras` then `uv run dhcli ...`
also works.

Verify with:

```bash
dhcli --help
```

## Shell completion

`dhcli` supports tab completion for commands, subcommands, and options
in bash, zsh, and fish. Enable it for the current shell by evaluating
the script printed by `dhcli self completion <shell>`, or add the line to
your shell startup file to make it permanent:

```bash
# bash (requires bash >= 4.4); add to ~/.bashrc
eval "$(dhcli self completion bash)"

# zsh; add to ~/.zshrc
eval "$(dhcli self completion zsh)"

# fish; add to ~/.config/fish/completions/dhcli.fish
dhcli self completion fish | source
```

See [`dhcli self`](#dhcli-self) for details.

## Quick start

Starting from nothing, in the order a new user needs them:

```bash
# 1. Show subcommands.
dhcli --help

# 2. Write a working configuration. No prompts; enables session creation.
dhcli config init

# 3. Confirm it is valid. Exit 0 means good; 2 prints the offending file.
dhcli config validate

# 4. Create a session to work in. The daemon auto-starts on first use.
dhcli session create dev

# 5. Do something with it.
dhcli table list
dhcli session exec --script 'print(1)'
```

Steps 4 and 5 omit the session id: `session create` records the new
session as the sticky default, and later verbs fall back to it. See
[`dhcli context`](#dhcli-context).

Diagnostics and self-discovery:

```bash
# Which files hold the configuration, and is any of them broken?
dhcli config files

# Daemon lifecycle (auto-started, but manageable by hand).
dhcli daemon status
dhcli daemon logs -n 200
dhcli daemon stop

# Discover the command tree as structured JSON (for AI agents).
dhcli agents tree | jq .

# Machine-readable description of one command (twin of --help).
dhcli daemon start --agents
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
- `$DH_AI_DATA_DIR/config/` (the env var moves the user-data
  root; the per-subdir override is the CLI flag)
- (otherwise) the platform default: `~/.deephaven/ai/config/` on
  POSIX, `%APPDATA%/Deephaven/ai/config/` on Windows.

Two files in that directory are CLI-relevant:

- `cli.json` — CLI defaults (output mode, request timeout, ...).
- `server.json` — daemon tunables (under the `daemon` block).

Use `dhcli config show` to inspect the resolved tree (with secrets
redacted) and `dhcli config validate` to confirm it is valid.

### Configuration loading

For every **runtime-dependent** command, configuration is loaded
**once per invocation, just before the command body runs**. Any
malformed file under the configuration directory fails fast with
`config_invalid` (exit code `2`) before the body runs. A tree whose
files are all valid but that declares no systems is itself **valid**
and loads cleanly; the no-systems invariant is enforced only where a
system is actually required — systems-server startup and the daemon
acquisition performed by the tool-wrapping verbs (`system`, `session`,
`table`, `catalog`, `pq`, `tool`), which fail with
`no_systems_configured`. The two discovery verbs `system list` and
`session list` are the exception: on a zero-system tree they return an
empty list (exit `0`) with the same guidance on **stderr** instead of
failing, since the result is provably empty. System-independent verbs
(`docs`, `daemon stop`, and every `config` verb) work against a
zero-system tree.

The offline authoring verbs — `config files`, `config get`,
`config set`, `config unset`, `config keys`, `config edit`,
`config init`, and the `config session` / `config system`
sub-groups — are exempt: they operate on the raw files without the
full-tree load, so they keep working against a broken or empty
tree. They are the recovery mode (see below).

The single load runs when a command is dispatched (after its
arguments parse, before its body):

1. Resolves `config_dir` and `runtime_dir` (CLI flags →
   `$DH_AI_DATA_DIR/{config,runtime}` → platform default).
2. Creates `runtime_dir` at mode `0o700`.
3. Audits `config_dir` permissions and parses `server.json`,
   `cli.json`, `community/`, and `enterprise/` into a single
   validated `Runtime`.
4. Applies top-level CLI flag overrides (`-o`, `--timeout`,
   `--no-auto-start`) onto `runtime.config.cli`.

Help and agent self-discovery therefore work against a broken (or
absent) configuration tree, with no special-casing:

- `dhcli --help`, `dhcli <noun> --help`, `dhcli <noun> <verb> --help` — help exits while arguments are being parsed, before the load would run, so you can navigate the CLI surface against a broken tree.
- `--agents` at any depth (`dhcli --agents`, `dhcli <noun> <verb> --agents`) — the machine-readable twin of `--help`, exits at the same parse-time point.
- `dhcli agents <verb>` — the agents verbs are declared config-free and never trigger the load; they emit machine-readable metadata for agents that need to learn the surface before any config exists.

#### Recovering from a broken configuration

Pre-body validation means a typo in any config file blocks every
runtime-dependent subcommand body. The offline authoring verbs stay
available:

- **Locate the failure**: `dhcli config files` lists every
  configuration file with its validity status and first error;
  `dhcli config validate` surfaces the full structured
  `config_invalid` payload. (Validation runs in the pre-body load,
  so the error appears even though the verb itself never executes.)
- **Fix it in place**: `dhcli config set` / `dhcli config unset`
  rewrite one field, `dhcli config edit <path>` opens the whole
  file in your editor (`$VISUAL`, else `$EDITOR`), and `dhcli config
  get` reads raw on-disk contents even from a partial or invalid tree.
- **Stuck daemon**: read `daemon.json` directly under
  `<runtime_dir>/daemon/`, then `kill <pid>`. Optionally `rm`
  the registry file once the process is gone.

### `cli.json`

The schema is organized into five top-level domain sections. Each section
is its own object (Pydantic model); sections that hold time-shaped knobs
carry a `timeouts:` sub-section reserved from day one, so future timeouts
slot in without a breaking schema change.

#### `output.*` — presentation

| Field                | Type    | Default | Notes                                                                       |
|----------------------|---------|---------|-----------------------------------------------------------------------------|
| `output.format`      | string  | `json` | One of `human`, `json`, `json-pretty`, `yaml`. Machine-first default (`json` is compact single-line); set `human` for interactive use or `json-pretty` for indented JSON. Override per invocation with `-o/--output`. |

#### `daemon.*` — CLI-side daemon lifecycle

| Field                                       | Type    | Default | Notes                                                                          |
|---------------------------------------------|---------|---------|--------------------------------------------------------------------------------|
| `daemon.auto_start`                         | bool    | `true`  | When `false`, `dhcli` exits if no daemon is running.                          |
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
| `docs.url`                     | string  | `https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/mcp` | Streamable-HTTP endpoint of the docs MCP server the `docs` commands query. Defaults to the Deephaven-hosted production docs server; point it at a self-hosted `dh-mcp-docs-server` to query that instead. Validated eagerly: must be `http://` or `https://` with a host and a well-formed port, and must not contain userinfo credentials (`user:password@`) — the URL is echoed in output and error messages. |
| `docs.timeouts.request_seconds`| integer | `120`   | Per-request timeout for docs server calls. Higher than the daemon request default because docs queries are LLM-backed. Override with `--timeout`. |

#### `context.*` — sticky context

| Field              | Type | Default | Notes                                                                                   |
|--------------------|------|---------|------------------------------------------------------------------------------------------|
| `context.enabled`  | bool | `true`  | Whether commands that take a session, system, or PQ id fall back to the sticky context in `context.json` when the argument is omitted. `false` disables the fallback; an omitted argument then fails with `context_not_set`. `context.json` is still readable and writable — `context show` reports each stored value with provenance `disabled` — so only the automatic fallback is affected. Override per invocation with `--no-context`. |
| `context.confirm_destructive` | bool | `false` | Whether a verb that executes, destroys, or disrupts (`session exec`/`delete`, `pq delete`/`stop`/`restart`/`modify`) asks for confirmation before acting on a target that came from `context.json` rather than the command line. An explicitly named target is never confirmed. Skip per invocation with `--yes`; declining exits `2` (`operation_canceled`). Skipped silently when prompting is unavailable (no TTY, or `--no-input`), so enabling it never breaks a non-interactive caller. |

The sticky context's *values* (the session/system/PQ ids themselves) are
never stored here — they are ephemeral per-user state in
`<runtime_dir>/context.json`, managed with `dhcli context set` /
`dhcli context unset`. See the `dhcli context` command-tree entry below.

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
  },
  "context": {
    "enabled": true,
    "confirm_destructive": false  // ask before acting on a context-supplied target
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
- `$DH_AI_DATA_DIR/runtime/` (the env var moves the user-data
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
verb honors the top-level `-o/--output` flag (except `self
completion`, which prints raw shell source).

### Choosing a target

Most verbs act on a session, system, or PQ named by an id. Two rules
decide which id is legitimate, and they matter most for AI agents:

1. **Act on an id you were given, or one you created yourself** with
   `session create` or `pq create`. Creating your own is the normal way
   to get something to work in.
2. **A listing is discovery, not a menu.** `session list` and `pq list`
   span every user's resources on the configured systems, production
   included. A returned id is a *candidate*, not a target: never run a
   script in, stop, restart, or delete one you did not create and were
   not pointed at. `session list --origin dynamic` narrows the listing
   to tool-created sessions. (`system list` is different — it reports
   the systems *your* configuration declares.)

The [sticky context](#dhcli-context) interacts with both: a verb whose
id is omitted acts on the persisted default, which is not visible on
the command line. Run `dhcli context show` before any consequential
verb you intend to invoke without an explicit id. The verbs that
execute or destroy (`session exec`, `session delete`, `pq modify`,
`pq delete`, `pq stop`, `pq restart`) confirm a context-supplied id on
a terminal; `--yes` skips the question, and with no terminal they
proceed unprompted.

`session credentials`, `session url`, and `session open` are the quiet
case: they destroy nothing, so nothing confirms them, but each fetches a
live auth token for whatever session was named. Aim one at a session you
were not given and you have disclosed that session's credentials.
`credentials` and `url` print the token — that is what they are for.
`session open` hands it to the browser instead and keeps it out of
stdout unless you pass `--reveal-secrets`, the same opt-in `config get`
uses.

### `dhcli daemon`

| Verb       | Purpose                                                                                       |
|------------|-----------------------------------------------------------------------------------------------|
| `start`    | Idempotently spawn the daemon (or report the existing one). Returns the shared `{state, message, daemon, paths}` envelope with `state: "running"`. |
| `stop`     | Idempotent SIGTERM (escalating to SIGKILL); removes the registry file.                        |
| `status`   | Reports the daemon's `state` (`running`/`stopped`/`crashed`) and a human `message`. Includes a `daemon` object — the running daemon's registry entry, redacted: `pid`, `create_time_ns`, `process_name`, `host`, `port`, redacted `psk`, `started_at`, `config_dir`, `server_name`, and a `build_identity` sub-object with `version`, `venv`, `fingerprint` — **only when running**, and always includes `paths` (config, runtime, registry, log). Read-only: a `crashed` entry is reported, not cleaned up — use `start` or `repair`. Exits 0 in all three states. |
| `restart`  | `stop` then `start` in one shot; returns the same `{state, message, daemon, paths}` envelope as `start`. |
| `repair`   | Recovers from a corrupt `daemon.json` by moving it aside to `daemon.json.corrupt-<UTC>` so a fresh `start` can write a clean registry. Refuses while a live daemon is still registered (`daemon_registry_live`). |
| `logs`     | Tails `daemon.log`. `-n/--lines N` controls the initial tail (default 100); `-f/--follow` follows the file (Ctrl-C to exit); `--path` prints the absolute log-file path and exits (works even if the daemon has never started). Auto-generated session tokens appear in the output in plaintext (see [`docs/SECURITY.md`](SECURITY.md#secret-handling)). |

### `dhcli tool`

| Verb               | Purpose                                                                                                 |
|--------------------|---------------------------------------------------------------------------------------------------------|
| `list`             | Lists registered tools. Internal tools (`_`-prefixed) are hidden unless `--all` is supplied.            |
| `show <name>`      | Prints one tool's name, description, and JSON input schema.                                             |
| `call <name>`      | Invokes a tool. Pass arguments as `--arg key=value` (repeatable); JSON-decoded when possible.           |

`tool` is the escape hatch, not the main road. It reaches every tool,
including ones no verb fronts, but `tool call` is a raw passthrough:
no sticky-context resolution, no confirmation before a destructive
call, no `--yes`, no documented output schema, and the result is the
unwrapped MCP envelope rather than a verb's shaped payload. Prefer the
first-class verbs (`session`, `table`, `catalog`, `pq`, `docs`)
wherever one exists.

`dhcli tool call` returns:

- `0` — success.
- `2` — client-side failure (connection, timeout, malformed argument, ...).
- `3` — the tool returned `isError=true`.

Examples:

```bash
dhcli tool list
dhcli tool show sessions_list
dhcli tool call sessions_list --arg type=community
```

### `dhcli session`

Sessions are addressed by a fully qualified id `type:system:name`
(`type` is `community` or `enterprise`). Verbs that take an id route to
the right backend by the id's prefix; `create` chooses the backend from
`--system`. Type is never a subgroup.

| Verb                          | Purpose                                                                                       |
|-------------------------------|-----------------------------------------------------------------------------------------------|
| `list`                        | Lists sessions (both types) — every user's, not just yours; see [Choosing a target](#choosing-a-target). Filters: `--type community\|enterprise`, `--system NAME`, `--origin static\|dynamic\|discovered`. The list can be incomplete while Enterprise discovery runs or a system is unreachable, in which case a warning naming the phase and failing systems goes to stderr. Wraps `sessions_list`. |
| `show [ID]`                   | Shows one session's detail object. `--connect` actively verifies liveness. Wraps `session_details`. |
| `create [NAME] --system SYS`  | Creates a session. `--system community` (default) → local Community worker (`NAME` required); any other system → an Enterprise worker on that named system, `NAME` optional/auto-generated (discover system names with `system list`). Wraps `session_community_create` / `session_enterprise_create`. |
| `delete [ID]`                 | Deletes a session, routing by the id prefix. Only a session `create` made (`origin: dynamic`) is eligible; others exit `3`. `--yes` skips the context confirmation. Wraps `session_community_delete` / `session_enterprise_delete`. |
| `exec [ID]`                   | Runs a script in the session via `--script TEXT`, `--script-path PATH` (read by the CLI), or `--script-path -` (stdin); supply exactly one. `--yes` skips the context confirmation. Wraps `session_script_run`. |
| `pip-list [ID]`               | Lists the session's installed pip packages as a `{package, version}` array. Wraps `session_pip_list`. |
| `credentials [ID]`            | Prints a Community session's browser-login credentials (`id`, `auth_type`, `auth_token`, `connection_url`, `connection_url_with_auth`). Wraps `session_community_credentials`. |
| `url [ID]`                    | Prints only the authenticated browser URL (`connection_url_with_auth`) — pipe-friendly. |
| `open [ID]`                   | Opens the authenticated URL in the default browser; `--print` prints it instead (headless-safe). |

`create` options are split by type and mutually exclusive: Community
takes `--launch-method`, `--auth-token`, `--docker-image`,
`--docker-memory-limit-gb`, `--docker-cpu-limit`, `--docker-volume`
(repeatable), `--python-venv-path`; Enterprise takes `--server`,
`--engine`, `--auto-delete-timeout`, `--admin-group`/`--viewer-group`
(repeatable), `--session-arg KEY=VALUE` (repeatable, JSON values).
Shared: `--language` (`Python`/`Groovy`), `--heap-size-gb`, `--jvm-arg`
(repeatable), `--env KEY=VALUE` (repeatable), and `--no-set-context`
(client-side; suppresses the automatic sticky-context set on success).
Supplying a wrong-type option exits `2` (`option_not_applicable`).

For an Enterprise system, `create` is *create-and-connect*: it provisions
a Persistent Query and connects immediately, and `delete` also deletes
that PQ. Because `delete` destroys the PQ, it accepts only a session
`create` made; a session that already existed (`origin: static` or
`discovered` in `session list`) is refused with exit `3` — use
`pq delete` instead.

To define a durable PQ without connecting — scheduled, RunAndDone, or
disabled — use `pq create` instead; see [`dhcli pq`](#dhcli-pq).

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
`community.settings.security.credential_retrieval_mode`
(default `dynamic_only`, which permits every dynamically launched
session — whichever client created it — but withholds statically
configured credentials);
when refused — or the session is missing or not a Community session —
they exit `3`. `session open` additionally keeps the token out of its
output unless `--reveal-secrets` is passed — including on the
`browser_launch_failed` path, where the URL offered for manual opening
is the token-free one. All session verbs exit `0` on
success, `2` on client-side/daemon failure, and `3` when the wrapped
tool reports an error.

Examples:

```bash
dhcli session list --type community
dhcli session create dev --launch-method python --env LOG_LEVEL=DEBUG
dhcli session create rpt --system prod --engine DeephavenEnterprise
dhcli session exec community:community:dev --script 'print(1)'
cat job.py | dhcli session exec community:community:dev --script-path -
dhcli session pip-list community:community:dev
dhcli session delete community:community:dev
dhcli session open community:community:dev --print --reveal-secrets
```

### `dhcli system`

A *system* is the source dimension of every fully qualified session id
(`type:system:name`): the single Community umbrella (named `community`)
plus every configured Enterprise (Core+) system.

| Verb         | Purpose                                                                                       |
|--------------|-----------------------------------------------------------------------------------------------|
| `list`       | Lists every configured system as `{name, type}` pairs — use the names with `session create --system NAME`. Wraps `list_systems`. |
| `status`     | Reports Enterprise (Core+) system health as a compact array of per-system records (`name`, `type`, `liveness_status`, `is_alive`, `liveness_detail`). Wraps `enterprise_systems_status`. Health only — use `dhcli config show` for configuration. Enterprise-only: an all-Community deployment returns an empty list. `--system NAME` scopes to one system; `--connect` actively verifies connectivity instead of reading cached state. `liveness_detail` is a short reason code: when `--connect` probed the system, the probe's own message; otherwise, when discovery recorded an error, the kubectl-style exception-type prefix (e.g. `DeephavenConnectionError`). When discovery is still running or has failed, a phase-summary warning is written to stderr; when `partial_result.errors` is present, stderr also includes a per-system details map with the full failure messages. The completed-phase banner may be suppressed when reasons are already in each row's `liveness_detail`. Exits `3` if the tool reports failure. |
| `url [NAME]` | Prints an Enterprise system's web console URL — pipe-friendly. |
| `open [NAME]`| Opens the Enterprise system's web console in the default browser; `--print` prints the URL instead (headless-safe). |

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
dhcli system list
dhcli system list | jq '.[].name'
dhcli system status --system prod --connect
dhcli system url prod
dhcli system open prod --print
```

### `dhcli table`

Inspects tables in a session. All verbs take a fully qualified
session id; `list` falls back to the sticky context when it is omitted.

| Verb                          | Purpose                                                                                       |
|-------------------------------|-----------------------------------------------------------------------------------------------|
| `list [ID]`                   | Lists the session's table names. Wraps `session_tables_list`. |
| `schema <id> <table>`         | Column definitions for one table: name and type per column, plus `column_type` where meaningful. Wraps `session_table_schema`. |
| `data <id> <table>`           | Row data as JSON objects keyed by column name: `--max-rows N` (default 1000), `--head/--tail` (default head). Wraps `session_table_data`. |

`schema` and `data` require an explicit `ID`: a table name follows it,
so a single argument would be ambiguous and they cannot fall back to
the sticky context the way `list` does.

`data` truncates silently on stdout when the table is larger than
`--max-rows`: `is_complete` reports `false` and no warning is printed.
Check that field before drawing a conclusion about the table as a
whole.

```bash
dhcli table list community:community:dev
dhcli table schema community:community:dev trades
dhcli table data community:community:dev trades --max-rows 50 --tail
dhcli table data community:community:dev trades | jq '.row_count, .is_complete'
```

### `dhcli catalog`

**Enterprise (Core+) only.** Queries an enterprise data catalog (database).
The two halves of this noun address differently.

`tables` and `namespaces` take a `SYSTEM` and need no worker of your own: they
read the listing through the system's shared `WebClientData` persistent query,
which builds the catalog with the ACLs of the Enterprise principal the server
is configured with for that system. That is not your CLI identity — every
caller of a given system sees the same listing. `WebClientData` must be
running on the system.

`schema` and `sample` take a session `ID`. Reading a catalog table's schema or
rows is a data access, and the server admits that only on a worker you
administer; asking the shared worker for it is refused with a permission
error. Point them at a session you own.

| Verb                                  | Purpose                                                                               |
|---------------------------------------|---------------------------------------------------------------------------------------|
| `tables [SYSTEM]`                     | Lists `{namespace, table_name}` entries. `--max-rows`, `--filter` (repeatable). When the list is truncated by `--max-rows`, a warning is written to stderr. Wraps `catalog_tables_list`. |
| `namespaces [SYSTEM]`                 | Lists the catalog's namespace names. Same options as `tables`. When the list is truncated by `--max-rows`, a warning is written to stderr. Wraps `catalog_namespaces_list`. |
| `schema <id> <namespace> <table>`     | Column definitions for one catalog table: name and type per column, plus `column_type` where meaningful. Wraps `catalog_table_schema`. |
| `sample <id> <namespace> <table>`     | Sample rows. `--max-rows` (default 100), `--head/--tail`, `--filter` (repeatable). Wraps `catalog_table_sample`. |

`tables` and `namespaces` fall back to the sticky context system when `SYSTEM`
is omitted. `schema` and `sample` cannot fall back, because a namespace and
table name follow the id.

`sample` is a preview, not a query. Partitioned tables would return
nothing without a partition filter, so with no `--filter` the tool
detects the table's partition columns and samples the most recent
partition holding data; passing `--filter` replaces that with your own
expressions. `catalog schema` marks partition columns with
`column_type: Partitioning`.

```bash
dhcli catalog tables prod
dhcli catalog namespaces prod
dhcli catalog schema enterprise:prod:42 Market Trades
dhcli catalog sample enterprise:prod:42 Market Trades --max-rows 20
```

### `dhcli pq`

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
| `list [SYSTEM]`                       | Lists PQs configured on a system — every user's, including production; see [Choosing a target](#choosing-a-target). Wraps `pq_list`. |
| `details [ID]`                        | Configuration + status for one PQ. Wraps `pq_details`. |
| `name-to-id <system> <name>`          | Resolves a PQ name to its fully qualified id. Wraps `pq_name_to_id`. |
| `create <name> --system S --heap-size-gb N` | Creates a PQ on `--system` with `--heap-size-gb` of heap. Script via `--script-body`/`--script-body-path`/`--git-script-path`; see the config flags below. Unset flags use controller defaults. Wraps `pq_create`. |
| `modify [ID]`                         | Updates only the fields passed; everything else is left unchanged. A repeatable option **replaces** the PQ's existing list rather than appending. `--restart` restarts the PQ after applying the change. `--yes` skips the context confirmation. Wraps `pq_modify`. |
| `delete [ID...]`                      | Deletes one or more PQs. `--max-concurrent N`, `--yes`. Wraps `pq_delete`. |
| `start [ID...]`                       | Starts one or more PQs. `--wait/--no-wait`, `--max-concurrent`. Wraps `pq_start`. |
| `stop [ID...]`                        | Stops one or more PQs. Same options as `start`, plus `--yes`. Wraps `pq_stop`. |
| `restart [ID...]`                     | Restarts one or more PQs. Same options as `stop`. Wraps `pq_restart`. |

`create` and `modify` share a large optional config flag set; only the flags you
pass take effect (on `modify`, everything else is left unchanged). `create`
additionally requires `--system` and `--heap-size-gb` and accepts
`--enabled/--disabled` (default enabled); `modify` takes an optional `ID`
(sticky context otherwise) and accepts `--pq-name`, `--heap-size-gb`,
`--enabled/--disabled`, and `--restart`. The shared flags are:
`--script-body`/`--script-body-path`/`--git-script-path`,
`--language` (`Python`/`Groovy`), `--configuration-type` (`Script`/`RunAndDone`),
`--schedule` (repeatable), `--server`, `--engine`, `--jvm-profile`, `--jvm-arg`
(repeatable), `--class-path` (repeatable), `--python-venv`, `--env KEY=VALUE`
(repeatable), `--init-timeout-nanos`, `--auto-delete-timeout`,
`--admin-group`/`--viewer-group` (repeatable), `--restart-users`, and `--owner`.
`create` also takes `--no-set-context` (client-side; suppresses the automatic
sticky-context set on success). Run `dhcli pq create --help` (or `modify`) for
the full per-flag detail.
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
`summary` and per-item `results` in the payload for failures. All ids in
one invocation must belong to the same Enterprise system.

None of these verbs report a settled state:

- `create` does not wait. Its `state` is always the placeholder
  `UNINITIALIZED` — the state at the instant the controller accepted the
  definition, not the live state. Poll `pq details` for what the PQ is
  actually doing.
- `start` / `restart` succeed on *acceptance*, not readiness. Branch on
  each `results[].state_category`: `TRANSITIONAL` (state `CONNECTING` or
  `INITIALIZING`) is a normal outcome with `--no-wait` or a short wait,
  and only `ACTIVE` means the id is usable with the `session`, `table`,
  and `catalog` verbs.
- A `--wait` that runs out is reported as a per-item failure even though
  the controller keeps going in the background. Treat a timeout as
  *unknown* rather than failed: re-read `pq details` instead of retrying
  blindly. The wait duration is
  `timeouts.client.pq_state_change_timeout_seconds` in
  `enterprise/settings.json` (120 seconds unless set), and
  `--max-concurrent` defaults to `pq_tools.default_max_concurrent` there
  (20 unless set).

Modifying a running PQ without `--restart` stores the new definition
while the live worker keeps serving the old one; the response then
carries a `warning` field, and `pq restart` applies it.

```bash
dhcli pq create nightly --system prod --heap-size-gb 4 --script-body-path ./n.py
dhcli pq create weekly --system prod --heap-size-gb 4 --git-script-path IrisQueries/py/weekly.py
dhcli pq restart enterprise:prod:1234567890 --no-wait
```

### `dhcli context`

Manages the **sticky context**: a persisted default `session` id,
`system` name, and `pq` id in `<runtime_dir>/context.json`. The `session`,
`system`, `table`, `catalog`, and `pq` verbs that take one of these ids
fall back to it when the argument is omitted, resolved in order: the
explicit argument, then `context.json`. `session create` and
`pq create` set the relevant key(s) automatically on success (pass
`--no-set-context` to skip this). A command that falls through every
step with the id still unset exits `2` (`context_not_set`).

| Verb                  | Purpose                                                                               |
|-----------------------|----------------------------------------------------------------------------------------|
| `show`                | Reports the stored value and provenance (`file`, `unset`, or `disabled`) for each of `session`, `system`, `pq`. Never contacts the daemon. |
| `set KEY VALUE`       | Persists `VALUE` as the sticky default for `KEY`. `session`/`pq` are confirmed to exist via the daemon; `system` is checked against `'community'` and the configured Enterprise systems, with no daemon contact. |
| `unset [KEY...] [--all]` | Clears one or more keys, or every key with `--all`. Pass one or the other, never both — combining them exits `2` (`mutually_exclusive_options`). Idempotent. |

The `source` each key reports is one of:

| `source`   | Meaning                                                                              |
|------------|--------------------------------------------------------------------------------------|
| `file`     | The key holds a value in `context.json`, and the fallback is on, so an omitted argument will use it. |
| `unset`    | The key holds no value; a command that omits the argument exits `2` (`context_not_set`). |
| `disabled` | The fallback is off (`--no-context`, or `cli.context.enabled` set to `false`), so no command will consult the context. `value` still shows what is stored. |

`set` and `unset` write to `context.json` even when the fallback is off,
printing a one-line warning on stderr.

Set by hand, the three keys are independent: a PQ and its running session
share an id, but setting one never changes another. The `create` and
`delete` verbs write or clear several at once:

| Verb                          | Keys written or cleared                        |
|-------------------------------|-------------------------------------------------|
| `session create` (Community)  | sets `session`                                  |
| `session create` (Enterprise) | sets `session`, `system`, `pq` (the session id *is* its PQ id) |
| `pq create`                   | sets `pq`, `system`                             |
| `session delete` / `pq delete`| clears `session` and `pq` when either pointed at a deleted id |

Disable the fallback for one invocation with `--no-context`, or
permanently via `cli.context.enabled` (see the `context.*`
configuration table above).

The two flags govern opposite directions and are easy to confuse:
`--no-context` disables *reading* the context (an omitted id fails
instead of falling back), while `--no-set-context` on `session create` /
`pq create` disables *writing* it. `--no-context` alone still lets a
successful `create` record the new id; pass both to leave `context.json`
untouched in either direction.

#### Safety: check the context before anything consequential

The sticky target is the one input a command's own command line does not
show. Acting on an unintended context **executes or destroys in the wrong
worker or system** — `session exec` runs your script in whatever session
the context names, and `pq delete` is irreversible. Run `dhcli context
show` first whenever you intend to omit an id and are not certain what
the context holds. Every consequential verb repeats this warning in its
own `--help` and `--agents` output.

By default dhcli acts on the context without asking. Set
`cli.context.confirm_destructive` to `true` to be asked first:

```text
Run script in session 'community:community:dev' (from sticky context)? [y/N]
```

That prompt appears only when **both** conditions hold: the verb is one
of the six that execute, destroy, or disrupt (`session exec`,
`session delete`, `pq delete`, `pq stop`, `pq restart`, `pq modify`) and
the id came from `context.json` rather than the command line. Naming the
id explicitly is already a statement of intent and is never confirmed;
read-only verbs never prompt. Pass `--yes` to skip a prompt; declining
exits `2` with `operation_canceled`.

When prompting is unavailable — stdin is not a TTY, or `--no-input` was
given — the command proceeds without asking rather than failing, so
enabling the setting never breaks a script. In practice an interactive
human gets the prompt and a non-interactive caller does not.

Five verbs keep a **required** leading positional despite the fallback:
`table schema`, `table data`, `catalog schema`, `catalog sample` (each an
`ID`), and `pq name-to-id` (a `SYSTEM`). Each takes a further required
positional after it, and a leading optional argument followed by a
required one is ambiguous to parse, so the first stays mandatory. The
verb tables above mark this difference: bracketed (`[ID]`) means the
sticky context can supply it, angled (`<id>`) means you must pass it.

Examples:

```bash
dhcli context show
dhcli context set system prod
dhcli session create rpt --system prod    # auto-sets session, system, pq
dhcli session exec                        # falls back to the sticky session
dhcli context unset --all
```

### `dhcli docs`

Queries the Deephaven documentation MCP server. These verbs connect
**directly** to the docs server named by `cli.docs.url`
(default: the Deephaven-hosted production docs server at
`https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/mcp`) — the local
daemon is not involved and is never started.

| Verb     | Purpose                                                                                       |
|----------|-----------------------------------------------------------------------------------------------|
| `ask`    | Sends a one-shot question (`PROMPT`) to the documentation assistant and prints its answer (an object with a `response` field). Wraps the docs server's `docs_chat` tool. |
| `status` | Checks that the configured docs server is reachable: initializes an MCP session, lists its tools, and reports `{url, reachable, tools, latency_ms}`. Exits `2` with `mcp_request_failed` when unreachable, or with `mcp_request_timeout` when the probe exceeds the request timeout. |

`ask` accepts `--language` (`python`/`groovy`) to tailor code examples,
`--core-version VERSION` / `--enterprise-version VERSION` to tailor the
answer to a Deephaven release, and `--history JSON` — a JSON array of
message objects, each with a string `role` (e.g. `user` or `assistant`)
and a string `content`, oldest first — to carry a prior exchange into a
follow-up question. A `--history` value that does not decode to a JSON
array of string-valued objects exits `2` with `arg_parse_error`; entry
semantics (which keys are required, which roles are meaningful) are
validated by the docs server, and an assistant-reported failure exits
`3` with `tool_returned_error`.

The assistant is LLM-backed, so answers typically take several seconds;
the docs request timeout defaults to `docs.timeouts.request_seconds`
(120) and honors the `--timeout` flag. For both verbs the budget bounds
the complete operation — connect, MCP initialize, and the tool call or
tool list — so a server that accepts the connection but stalls still
fails within the configured time.

```bash
dhcli docs ask "How do I join two tables?"
dhcli docs ask "Show me a ring table example" --language python
dhcli docs ask "What is a liveness scope?" | jq -r .response
dhcli docs status
```

### `dhcli config`

The configuration is one **logical JSON document** addressed by
dot-separated paths (`cli.output.format`,
`community.settings.session_creation`,
`enterprise.systems.prod.auth.credentials`), stored across several
files under the configuration directory. A path segment may be
double-quoted to contain literal dots (`defaults.session_arguments."my.key"`,
TOML dotted-key style); session/system names themselves can never
contain dots. File boundaries surface only in `config files`.

| Verb        | Purpose                                                                                       |
|-------------|-----------------------------------------------------------------------------------------------|
| `show`      | Prints the resolved configuration with secrets redacted (requires a valid tree).             |
| `validate`  | Confirms the configuration is valid; exits `0`, or `2` with `config_invalid` if any file is malformed. A zero-system tree is valid (the no-systems invariant is enforced only where a system is required, not by this check). Validation runs before every command body, so this is CI-friendly. |
| `files`     | Lists every configuration file: logical path, absolute file path, exists, valid, first validation error, template-resolution warnings. Works even when the configuration is broken or empty — the first command to run when diagnosing configuration problems. |
| `init`      | Creates a working configuration in one step: afterwards `dhcli session create dev` starts a local Deephaven worker, with no Docker and nothing further to install. Asks no questions and contacts no server, so it is safe to script or run from an agent. Will not touch a configuration that is already there — if `community/settings.json` exists it stops with `already_exists` and changes nothing, so use `config set` to adjust an existing file. To use a Deephaven server you already run, or an Enterprise system, use `session add` / `system add` instead. |
| `get [PATH]` | Prints the raw on-disk value at `PATH` (or the whole tree when omitted): a JSON object for a subtree, the bare scalar for a leaf. Works on a broken or partial tree and shows raw values with templating refs unresolved — unlike `show`, which prints the validated, template-resolved view. Secrets are `[REDACTED]` unless `--reveal-secrets` is passed, which additionally warns on stderr naming how many values it disclosed. |
| `set ASSIGNMENT...` | Sets one or more fields via `PATH=VALUE` tokens (VALUE parsed as JSON first, falling back to a plain string). Intermediate objects are created as needed; a `PATH` naming a whole file takes a JSON object that **replaces** the file's contents (assignment, not a merge). Assignments spanning multiple files are validated first, then each file is written atomically, with the already-written files rolled back if a later write fails (per-file atomic with batch rollback, not cross-file atomic: a concurrent reader may briefly see a partial multi-file update). Cannot create a new session/system (use `session/system add`); refuses to rewrite a file with JSON5-only syntax (`config_not_rewritable` — use `config edit` instead). |
| `unset PATH...` | Removes one or more fields, reverting each to its schema default. Cannot unset a whole file (use `session/system remove`); same JSON5-rewrite refusal as `set`. |
| `keys [PATH]` | Lists every settable logical path below `PATH` (or the whole tree), with type and description — the discovery companion to `get`/`set`. Not exhaustive by design: a discriminated union (e.g. `auth.credentials`) and an open/free-form object (e.g. `session_arguments`) each collapse to a single `object` entry, so their variant- or user-chosen children (such as `auth.credentials.token`) are not listed even though `config set` accepts them. A whole file (its logical path; run `config files`) is likewise omitted. |
| `edit PATH` | Opens the whole file named by `PATH` in the editor from `$VISUAL`, else `$EDITOR`, else a platform default ([`docs/ENV.md`](ENV.md#visual-and-editor)) and writes back exactly what was saved, comments and formatting included — the only authoring verb that can touch a JSON5-only file. Parses and schema-validates before writing; a failure leaves the file untouched. Interactive only (`no_tty` without a TTY or with `--no-input`). |
| `session add NAME` | Declares a community session file (`community/sessions/<NAME>.json`). Flags: `--host`, `--port`, `--language`, `--auth anonymous\|psk\|password\|custom` plus the matching credential flags (`--token`; `--username`/`--password`/`--effective-user`; `--auth-type`/`--auth-token`). Missing values are prompted for on a terminal (stderr); non-interactive runs fail with `missing_required_option`. Refuses to overwrite (`already_exists`). |
| `session remove NAME` | Deletes the session file. Confirms on a terminal; requires `--yes` otherwise.       |
| `session list` | Lists declared session files with per-file validity. Contrast: `dhcli session list` shows *live* sessions. |
| `system add NAME` | Declares an enterprise system file (`enterprise/systems/<NAME>.json`). Flags: `--url` (connection.json URL), `--auth password\|private_key` plus the matching credential flags (`--username`/`--password`/`--effective-user`; `--key` — use `${file:/path/key.pem}`), optional `--max-sessions`, `--heap-gb`. `community` is a reserved name. |
| `system remove NAME` | Deletes the system file. Confirms on a terminal; requires `--yes` otherwise.        |
| `system list` | Lists declared system files with per-file validity. Contrast: `dhcli system list` shows what the daemon serves. |

Secret-bearing flags (`--token`, `--password`, `--key`, `--auth-token`)
accept templating refs — `${env:VAR}`, `${env:VAR:-default}`,
`${file:/path}` — stored verbatim in the file and resolved when the
server loads it; a literal value is accepted with a stderr hint
recommending a ref. Authoring verbs never write an invalid file: every
change is schema-validated and written atomically, and a ref that does
not resolve in *your* shell (e.g. an env var only the daemon has) is a
warning, not an error — even on a typed field such as `port`, whose
type is checked when the server resolves the ref.

Every `config` verb except `show` and `validate` operates on the raw
files without loading the runtime, and so honors the root
`-o/--output` flag and `DHCLI_OUTPUT` but not `cli.output.format`
(the configuration may be the thing being inspected or repaired).

### `dhcli agents`

Machine-readable CLI metadata for AI-agent self-discovery — the
`--help` for agents; prefer it over scraping help text. There are two
complementary ways to reach it: the `--agents` flag for one command in
place, and the `agents` group for whole-system views. Both honor the
root `-o/--output` flag and `DHCLI_OUTPUT` (`human`, `json`,
`json-pretty`, or `yaml`) and, like every command, **default to
`json`** — compact single-line JSON; pass `-o json-pretty` for
indented JSON or `-o human` for terminal-friendly output. Both run
without a valid configuration tree, so they work even when `config
validate` fails; that same bypass means they cannot read
`cli.output.format` (use `-o`/`DHCLI_OUTPUT` instead).

Which surface to use:

- **Orienting** — `dhcli agents tree`.
- **Running one command** — `<cmd> --agents`.
- **Decoding a failure** — `dhcli agents errors`, fetched once.
- **Everything at once** — `dhcli agents tree --full`; prefer the three
  above unless you need the whole surface in one payload.

#### The `--agents` flag (twin of `--help`)

Append `--agents` to any command, at any depth, to emit just that
command's node — the machine-readable counterpart of `--help`:

```bash
dhcli daemon start --agents    # the start verb's full node
dhcli daemon --agents          # the daemon group node (verb summaries)
dhcli --agents                 # the summary tree (== agents tree)
```

Like click's own `--help`, the universal `--help` and `--agents` flags
are *not* listed under any command's `params`; the complete manifest
discloses them once under `universal_options`.

#### The `agents` group

| Verb                 | Output                                                                                  |
|----------------------|-----------------------------------------------------------------------------------------|
| `tree`               | The **summary tree** by default — every command path with its one-line summary, plus the project-wide `conventions`. Identical to `dhcli --agents`. With `--full`, the complete manifest instead: full nodes for every command plus the project-wide metadata. Both field lists are under [Node schema](#node-schema). |
| `command PATH...`    | One command's **full node**, resolved from `PATH` (one or more command-name tokens; required). Identical to appending `--agents` to that command. A group's node lists its subcommands as one-line summaries; `--full` expands them into full nested nodes. A path that does not resolve exits `2` with `command_not_found`. |
| `errors`             | The stable `error_code` registry (`code` + `help`) — also the `error_codes` key of `tree --full`. **The decoder for every node's `error_codes`**, which name codes without their meanings; fetch it once and cache it. |

```bash
dhcli agents tree | jq '.commands | keys'
dhcli agents command daemon start    # == dhcli daemon start --agents
dhcli agents command session         # group: verb summaries, one level
dhcli agents errors | jq '.[].code'
```

#### Node schema

A command's node carries, as **sparse keys** (an absent key means
false, empty, or the default):

| Key           | Content                                                                                     |
|---------------|----------------------------------------------------------------------------------------------|
| `name`        | The command's own name, e.g. `stop` (always present).                                        |
| `path`        | The full invocation path, e.g. `dhcli pq stop` — what to actually run. Omitted inside `tree --full`, where a node's position in the nested map already gives its path. |
| `usage`       | Usage line in click's own form, e.g. `dhcli pq stop [OPTIONS] [ID]...`, giving the positional order. Omitted inside `tree --full` (`params` gives the order). |
| `summary`     | One-line summary (always present).                                                           |
| `description` | What the command does and when to use it.                                                    |
| `params`      | Options **and** positional arguments: `name`, `kind` (`option`/`argument`), `type`, `help`, plus (sparse) `required`, `nargs` (when not 1), `choices`, `opts`, `secondary_opts`, `is_flag`, `multiple`, `envvar`, `default`. |
| `output`      | The structured output shape: `mode` (`object`/`list`/`text`), `fields` (`{name, type, help}`), `note`. |
| `examples`    | Shell snippets.                                                                              |
| `see_also`    | Related commands.                                                                            |
| `error_codes` | The stable codes the command can emit, as bare strings. Decode them with `dhcli agents errors` (or the root `error_codes` key of `tree --full`); a failure also carries its own message next to the code. |
| `exit_codes`  | The process codes the command can return — `{code, help}`, or bare integers inside `tree --full` (see `default_exit_codes`). |
| `environment` | Environment variables honored (`{name, help}`); inside `tree --full`, omitted when the command leaves it unset (it then inherits `default_environment`). |
| `wraps`       | The wrapped MCP tool binding (`tools`, `intentionally_unsupported`, `router_params`, `client_only_params`). |
| `subcommands` | Groups only: `{name: summary}` one level down, or full nested nodes with `--full` / inside `tree --full`. |

A node requested on its own (`agents command PATH`, `<cmd> --agents` —
the two are byte-identical) carries what the command's `--help` renders,
except error-code meanings: get those from `dhcli agents errors`. The
same node nested inside `tree --full` drops `path` and `usage`, plus any
`exit_codes` / `environment` the root already states once as
`default_exit_codes` / `default_environment`.

The two whole-tree surfaces carry different top-level fields, and a
single command's node never carries any of them:

- **Summary tree** (`agents tree`, `dhcli --agents`): `version`,
  `prog`, `summary`, `description`, `conventions`, `hint`, and a nested
  `{name: {summary, commands?}}` map under `commands`. `conventions`
  states the rules that hold for *every* command — output mode and exit
  codes, the sticky-context fallback, and target selection. Read it
  before your first consequential command; a hazard specific to one
  command is stated on that command instead.
- **Full manifest** (`agents tree --full`): `version`, `prog`,
  `summary` (plus the root's `description` / `examples`),
  `global_options`, `universal_options`, full command nodes under
  `commands`, `default_environment`, `default_exit_codes`, and the
  `error_codes` registry.

> **Migration:** this surface was previously `dh-mcp introspect` /
> `--introspect`; use `agents` / `--agents`, which has no alias. `tree`
> once emitted the full manifest by default — that is now `tree --full`,
> and the rendered `help` / `short_help` node fields are replaced by the
> structured `summary`, `description`, and section keys above. `-o json`
> now emits compact single-line JSON; use `-o json-pretty` for the
> previous indented form.

### `dhcli self`

Verbs that operate on the `dhcli` installation itself rather than on
Deephaven resources (the `rustup self` / `uv self` pattern), and the
intended home for future tool-self-management verbs.

| Verb         | Purpose                                                        |
|--------------|----------------------------------------------------------------|
| `completion` | Print the shell tab-completion script for `bash`, `zsh`, or `fish`. |

`self completion SHELL` prints the tab-completion script for one
supported shell — `bash` (>= 4.4), `zsh`, or `fish` — as raw shell
source (not subject to `-o/--output`; the script *is* the output).
Evaluate it to enable completion in the current shell, or add the
line to your shell startup file (see
[Shell completion](#shell-completion)):

```bash
eval "$(dhcli self completion bash)"    # ~/.bashrc
eval "$(dhcli self completion zsh)"     # ~/.zshrc
dhcli self completion fish | source     # ~/.config/fish/completions/dhcli.fish
```

The scripts are generated and maintained by `click`; the supported set
is exactly click's native set. The command reads no configuration and
works without a valid configuration tree. An unsupported shell fails
argument parsing and exits `2`.

## Top-level flags

Environment variables listed here are the ones bound to a flag.
[`ENV.md`](ENV.md) is the canonical inventory of every environment
variable the project reads, with the reasoning for each.

| Flag                | Envvar               | Purpose                                                                              |
|---------------------|----------------------|--------------------------------------------------------------------------------------|
| `--config-dir PATH` |                      | Override the configuration directory. No per-subdir env var; use `DH_AI_DATA_DIR` to move both `config/` and `runtime/` together. |
| `--runtime-dir PATH`|                      | Override the runtime directory (where `daemon.json` lives). No per-subdir env var.   |
|                     | `DH_AI_DATA_DIR`    | Override the **user-data root**; `config/` and `runtime/` resolve under it. |
| `-o`, `--output`    | `DHCLI_OUTPUT`      | One of `human`, `json`, `json-pretty`, `yaml`. Overrides `cli.output.format`. |
| `--timeout SECS`    |                      | Per-request timeout. Overrides `cli.request.timeouts.default_seconds` (and `cli.docs.timeouts.request_seconds` for the `docs` commands). |
| `--agents`          |                      | Print the command's machine-readable description, tuned for AI agents, and exit (the machine twin of `--help`); available on every command. Honors the `-o`/`DHCLI_OUTPUT` mode, compact `json` by default. On the root, emits the summary tree; `dhcli agents tree` prints the whole surface. |
| `-v`, `--verbose`   |                      | Increase logging verbosity (`-v`=INFO, `-vv`=DEBUG). Mutually exclusive with `-q`.   |
| `-q`, `--quiet`     |                      | Suppress non-error logging (root logger at ERROR). Mutually exclusive with `-v`.     |
| `--no-auto-start`   |                      | Fail rather than spawn a daemon when none is running.                                |
| `--no-context`      |                      | Disable the sticky context for this invocation: when a session, system, or PQ id is omitted, the command fails with `context_not_set` instead of falling back to the stored context. Overrides `cli.context.enabled`. Governs only the read. See [`dhcli context`](#dhcli-context) below. |
| `--no-input`        |                      | Never prompt interactively; a command missing a required value fails with a structured `missing_required_option` error naming the flag to supply. Prompting is already disabled when stdin is not a TTY. |
| `--version`         |                      | Print the package version and exit.                                                  |

These flags accept any position on the command line — before the
noun group, between the noun and the verb, or after the verb.
For example `dhcli -o json config show`, `dhcli config -o json
show`, and `dhcli config show -o json` are all equivalent. The
CLI rewrites argv to lift recognized top-level options to the
front before `click` parses it. `--help` and `--version` are
*not* lifted: Click resolves them per-command, so `dhcli daemon
--help` correctly renders the `daemon` group's help (not the
root's). Use the POSIX `--` sentinel to force a literal token
later in the command line (everything after `--` is preserved
verbatim).

## Output modes

Every verb honors `-o/--output`, selecting how its result is rendered. The mode
is resolved per invocation: `-o/--output` flag → `DHCLI_OUTPUT` →
`cli.output.format` (default `json`). The CLI is **machine-first** (primarily
driven by AI agents), so the default is `json`; for human-readable output, pass
`-o human`, set `DHCLI_OUTPUT=human`, or run `dhcli config set
cli.output.format=human`. `dhcli agents`, the `--agents` flag, and error output
run without the validated config, so they skip the configured step — use
`-o`/`DHCLI_OUTPUT` for those (so `DHCLI_OUTPUT=human` is the most complete way
to get human output everywhere).

- `json` (default) — a **compact single-line** document
  (`json.dumps(..., separators=(",", ":"), sort_keys=True)`): key-sorted and
  deterministic, the machine-optimal form — it parses identically to the
  indented form but costs the fewest tokens in an agent's context. Pipe
  through `jq .` to eyeball it, or use `json-pretty`.
- `json-pretty` — the same document via `json.dumps(..., indent=2,
  sort_keys=True)`: indented and key-sorted, for human reading and
  line-oriented diffs.
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

When run with `-o json`, `-o json-pretty`, or `-o yaml`, any
user-facing failure is emitted on **stderr** as a structured payload
with these keys:

```json
{
  "error": "<human-readable message>",
  "error_code": "<stable identifier; see registry below>",
  "exit_code": 2,
  "command": "daemon start"
}
```

The `error_code` values are stable across releases. Get the full
registry programmatically via `dhcli agents errors` (or the
`error_codes` key of `dhcli agents tree --full`). Current set:

| `error_code`                  | Meaning                                                            |
|-------------------------------|--------------------------------------------------------------------|
| `daemon_startup_timeout`      | Daemon was spawned but did not publish a registry entry in time.   |
| `daemon_not_running`          | No running daemon was found: either none is registered and `--no-auto-start` was specified, or a command that needs the daemon's files (e.g. `daemon logs`) found none yet. |
| `daemon_client_error`         | A client-side daemon-management failure (signal denied, etc.).     |
| `daemon_registry_corrupt`     | `daemon.json` exists but cannot be parsed. Recover with `dhcli daemon repair`. |
| `daemon_registry_live`        | `dhcli daemon repair` refused to move `daemon.json` aside because a live daemon is still registered; run `dhcli daemon stop` first. |
| `daemon_reuse_refused`        | The running daemon is a different build than the CLI (version, venv, or source fingerprint differs) and `cli.daemon.reuse` resolved to `refuse`. Run `dhcli daemon restart` to replace it, or adjust `cli.daemon.reuse`. |
| `mcp_request_failed`          | The MCP transport reported an error (connect, parse, server failure). |
| `mcp_request_timeout`         | The MCP request timed out. The server may still finish processing the request — if the operation changes state, verify the result before retrying. Allow more time with `--timeout`, or raise `cli.request.timeouts.default_seconds` (`cli.docs.timeouts.request_seconds` for the `docs` commands). |
| `tool_not_found`              | `dhcli tool show/call` referenced an unknown tool name.           |
| `tool_returned_error`         | The invoked tool returned `isError=true`. Exit code `3`.           |
| `arg_parse_error`             | A `key=value` token (`--arg`, `--env`, `--session-arg`) was malformed. |
| `command_not_found`           | `dhcli agents command PATH` referenced a command path that does not exist. |
| `missing_argument`            | A required positional argument or option was not provided, or one was supplied blank (an empty or whitespace-only string). |
| `mutually_exclusive_options`  | Two or more options that cannot be combined were supplied together. |
| `file_read_failed`            | A local file passed on the command line could not be read.          |
| `option_not_applicable`       | An option/argument is invalid for the selected `--system` type (an inapplicable option, or a missing required one such as a Community session name). |
| `browser_launch_failed`       | `dhcli session open` / `system open` could not launch a browser; the URL is included in the error message to open manually. For `session open` that URL omits the auth token unless `--reveal-secrets` was passed — use `dhcli session url` for one that logs in. |
| `system_not_found`            | `dhcli system url/open NAME` named an Enterprise system that is not configured (`community` included — it has no web console). |
| `context_not_set`             | A session/system/PQ id was omitted and no sticky context supplies one. Pass it explicitly, run `dhcli context set KEY VALUE`, or check `dhcli context show`. `--no-context` / `cli.context.enabled=false` disables the sticky-context fallback step, making this code more likely, not less. |
| `config_invalid`              | The configuration tree failed validation.                          |
| `no_systems_configured`       | A system-dependent verb (`system`, `session`, `table`, `catalog`, `pq`, `tool`) ran against a valid tree that declares no system (no community session file, no `session_creation` block, no enterprise system file); the daemon serves systems, so acquiring one is refused. (The systems server likewise refuses to start on a zero-system tree.) The discovery verbs `system list` / `session list` are exempt — they return an empty list with this guidance on stderr instead of exiting non-zero. Add one (`dhcli config session add`, `dhcli config system add`, or `dhcli config init`), or check that `--config-dir` / `DH_AI_DATA_DIR` points at the intended directory. |
| `config_path_invalid`         | A configuration path argument is malformed or does not name a known location. Run `dhcli config files` to list files and `dhcli config keys` to list settable paths. |
| `missing_required_option`     | A required option was not provided and interactive prompting is unavailable (stdin is not a TTY, or `--no-input` was given). The error message names the exact flag(s) to supply. |
| `already_exists`              | The target configuration file already exists and the command refuses to overwrite it. For a session or system, remove it first with `dhcli config session/system remove`; for `config init` (which will not replace `community/settings.json`), edit the existing file with `dhcli config set` or `dhcli config edit`. |
| `not_found`                   | The named configuration entity or file does not exist, or the addressed field has no value set. |
| `config_not_rewritable`       | `config set`/`unset` refused to touch a file that uses JSON5-only syntax (comments, trailing commas) a programmatic rewrite would destroy; edit the file directly, or with `dhcli config edit`. |
| `no_tty`                      | The command is interactive-only (`config edit`) but stdin is not a TTY or `--no-input` was given; use the non-interactive equivalents (`config set`, or `config session add` with flags). |
| `config_locked`               | Another process holds the per-directory configuration write lock, so a `config` authoring verb (`add`/`set`/`unset`/`remove`/`edit`/`init`) could not acquire it before timing out. Retry once the other invocation finishes. |
| `operation_canceled`         | The operator answered no to an interactive confirmation prompt, so a destructive action was not performed. A deliberate decline (exit `2`), distinct from a Ctrl-C interruption (exit `130`). |
| `internal_error`              | An unexpected internal failure not attributable to a specific subsystem. |

## Troubleshooting

**Daemon refuses to start / `dhcli daemon start` times out.**

Inspect `<runtime_dir>/daemon/daemon.log`. The daemon writes its
stdout and stderr there; configuration errors and import failures
appear in plain text. Once you've fixed the cause, the registry
will be written on the next successful spawn.

**Stale registry after a crash.**

If the daemon crashes without cleaning up, `dhcli` detects the
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
dhcli daemon status    # confirm whether a daemon is still running
dhcli daemon stop      # if it is, stop it first
dhcli daemon repair    # move the corrupt file aside (renames to daemon.json.corrupt-<UTC>)
dhcli daemon start     # fresh spawn
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

You set `cli.daemon.auto_start` to `false`. Run `dhcli daemon start`
explicitly, or run `dhcli config set cli.daemon.auto_start=true`.

**Build mismatch (`daemon_reuse_refused`).**

A daemon is a persistent process running a specific build of the code; the
CLI verifies the running daemon matches the build it ships from before
reusing it. After an upgrade (`pip install -U`), or when a different
virtualenv's daemon is still running, the CLI refuses (by default) rather
than silently driving a stale daemon. Replace it:

```bash
dhcli daemon restart   # stop the stale daemon and spawn a fresh one
```

To relax the policy per field, set `cli.daemon.reuse`
(see the `daemon.*` configuration table above). For parallel
development across multiple checkouts, give each its own daemon by pointing
`DH_AI_DATA_DIR` at a per-worktree directory (e.g.
`export DH_AI_DATA_DIR="$PWD/.dhcli-data"`), so switching checkouts never
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
