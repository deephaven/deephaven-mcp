# Deephaven MCP Configuration Reference

This document is the single source of truth for configuring the
Deephaven MCP servers. It covers:

- The on-disk configuration directory layout.
- The JSON5 file formats and the Pydantic schemas behind them.
- The `${env:VAR}` / `${env:VAR:-default}` / `${file:PATH}`
  templating engine that lets a JSON file pull values from
  environment variables or other files on disk.
- The default values that apply when a field is omitted.

## Table of Contents

- [Configuration directory](#configuration-directory)
- [Templating](#templating)
- [`server.json`](#serverjson)
- [`community/settings.json`](#communitysettingsjson)
- [`community/sessions/<name>.json`](#communitysessionsnamejson)
- [`enterprise/settings.json`](#enterprisesettingsjson)
- [`enterprise/systems/<name>.json`](#enterprisesystemsnamejson)
- [Reloading](#reloading)
- [See also](#see-also)

## Configuration directory

`dh-mcp-systems-server` reads a *directory tree* (not a single
file). The default location is:

- POSIX: `~/.deephaven/ai/config/`
- Windows: `%APPDATA%/Deephaven/ai/config/`

Override with the `DH_AI_DATA_DIR` environment variable or the
`--config-dir` CLI flag. **`DH_AI_DATA_DIR` is the only
environment variable the systems server itself reads at startup**
(plus `PYTHONLOGLEVEL` for log verbosity). Every other knob lives
inside the JSON files (and the [templating engine](#templating)
below pulls env-var values *into* those JSON values when you want
them).

`dh-mcp-docs-server` does **not** read this directory tree; it has
its own (small) set of environment variables documented in
[`docs/ENV.md`](ENV.md).

Layout:

```text
$DH_AI_DATA_DIR/config/
  server.json                       # optional; HTTP transport + PSK
  cli.json                          # optional; dhcli CLI defaults (see docs/CLI.md)
  community/
    settings.json                   # optional; per-system defaults
    sessions/
      <name>.json                   # zero or more static sessions
  enterprise/
    settings.json                   # optional; per-system defaults
    systems/
      <name>.json                   # zero or more enterprise systems
```

All files are JSON5 (so comments and trailing commas are allowed)
and validated by Pydantic. Unknown fields are rejected with a
clear error.

You can hand-edit these files, or let the CLI write them: the
`dhcli config` verbs (`init`, `session add`, `system add`, `set`,
`unset`, `get`, `keys`, `edit`, `files`, `show`, `validate`) address the
tree by dot-separated logical path, schema-validate every change before
an atomic write, and apply the permissions the startup audit requires.
`dhcli config validate` is the standalone check for a tree you edited by
hand. See [`docs/CLI.md`](CLI.md).

One caveat if you rely on JSON5 syntax: `config set` / `config unset`
refuse to rewrite a file containing comments or trailing commas, since a
programmatic rewrite would discard them. Use `dhcli config edit`, which
preserves the file verbatim, or edit it directly.

## Templating

Anywhere a string value appears in a JSON file the server expands
three placeholder forms before handing the tree to the Pydantic
validators:

| Syntax                       | Result                                                                                       |
| ---------------------------- | -------------------------------------------------------------------------------------------- |
| `${env:VAR}`                 | Value of the environment variable `VAR`. Error if `VAR` is unset or empty.                   |
| `${env:VAR:-default}`        | Value of `VAR` when set and non-empty, otherwise the literal `default` (which may be empty). |
| `${file:/absolute/path}`     | UTF-8 contents of the named file, returned verbatim. Error if the file is missing.           |

A `${file:...}` path may be absolute (used as-is, so a system trust
store such as `/etc/ssl/cert.pem` works), home-relative (a leading `~`
expands to the user's home directory), or relative (resolved against
the configuration directory). Symlinks are followed and the file must
be UTF-8 and under 1 MiB.

Placeholders may appear anywhere inside a string value (substring
expansion), but nesting is not supported. Keys are never expanded.
Errors surface with the source file and JSON path of the offending
value, e.g.:

```text
In community/sessions/local.json at credentials.token:
env var DH_MCP_PSK is not set
```

This is the **only** mechanism for env-var or file indirection.
The legacy `<field>_env_var` / `<field>_path` shadow fields are
gone — use the templating syntax inside the value of the real
field instead.

### Examples

```json5
// server.json — load PSK from the environment.
{
  "psk": "${env:DH_MCP_PSK}"
}
```

```json5
// community/sessions/local.json — anonymous session.
{
  "host": "localhost",
  "port": 10000,
  "auth": {
    "credentials": {"type": "anonymous"}
  }
}
```

```json5
// community/sessions/secured.json — PSK with env-var indirection.
{
  "host": "localhost",
  "port": 10000,
  "auth": {
    "credentials": {
      "type": "psk",
      "token": "${env:DH_COMMUNITY_PSK}"
    }
  }
}
```

```json5
// community/sessions/mtls.json — TLS material loaded from disk.
{
  "host": "secure.example.com",
  "port": 10000,
  "tls": {
    "root_certs": "${file:/etc/ssl/dh-ca.pem}",
    "client_certificate": {
      "cert_chain": "${file:/etc/ssl/client.pem}",
      "private_key": "${file:/etc/ssl/client.key}"
    }
  },
  "auth": {
    "credentials": {"type": "anonymous"}
  }
}
```

```json5
// enterprise/systems/prod.json — private-key auth.
{
  "connection_json_url": "https://dhe.example.com/iris/connection.json",
  "auth": {
    "credentials": {
      "type": "private_key",
      "key_text": "${file:/etc/dh/prod_id.pem}"
    }
  }
}
```

## `server.json`

Every operator-tunable knob for the systems-server process lives here.
All fields are optional and carry schema-level defaults. CLI flags
(`--transport`, `--host`, `--port`, `--psk`) override the
corresponding JSON value when supplied.

| Field             | Type                       | Default                  | Description                                                                                |
| ----------------- | -------------------------- | ------------------------ | ------------------------------------------------------------------------------------------ |
| `transport`       | `"stdio"` \| `"http"`      | `"stdio"`                | Default transport. Overridden by `--transport`.                                            |
| `host`            | str                        | `"127.0.0.1"`            | HTTP bind address (must be loopback). Overridden by `--host`.                              |
| `port`            | int (1–65535)              | `8000`                   | HTTP TCP port. Overridden by `--port`.                                                     |
| `server_name`     | str                        | `"deephaven-mcp-systems"`| FastMCP server name advertised in MCP handshakes.                                          |
| `psk`             | string (secret)            | `null`                   | PSK required by the HTTP transport (omit for stdio). Overridden by `--psk`.                |

Community- and enterprise-side timeouts live alongside the other
per-section settings in `community/settings.json` and
`enterprise/settings.json`; see those sections below. PQ-tool
defaults are enterprise-only and live on `enterprise/settings.json:
pq_tools` (see [below](#pq_tools)).

Use `${env:...}` to pull any field's value from an environment
variable without committing it to disk.

### `server.json`: `daemon` block

Optional sub-block consulted only when the systems server runs in
daemon mode (`--daemon`, as spawned by the `dhcli` CLI). Ignored
under stdio and the foreground HTTP transport. All fields are
optional with schema-level defaults.

| Field                   | Type        | Default                   | Description                                                                                                                                                                                 |
| ----------------------- | ----------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `idle_shutdown_seconds` | int (>= 0)  | `3600`                    | Seconds of MCP inactivity after which the daemon gracefully exits. `0` disables auto-shutdown (the daemon runs until killed). Activity is any successful, PSK-authenticated MCP request; failed PSK checks do not reset the timer. |
| `process_name`          | str         | `"dh-mcp-systems-server"` | Expected process-name token the CLI's liveness check matches against the recorded PID (or command line on Linux/macOS); a mismatch marks the registry entry stale and discards it.          |

The `dhcli` CLI commands that manage this daemon's lifecycle
(`daemon start` / `stop` / `status` / `restart` / `repair` / `logs`)
are documented in [`docs/CLI.md`](CLI.md) — that link is for the CLI
command reference, not for these field definitions.

## `community/settings.json`

Tunables that apply to *every* dynamic community session this
server creates. All fields are optional; omitted fields fall back
to the schema-level defaults shown below.

| Field                                  | Type   | Default  | Description                                                |
| -------------------------------------- | ------ | -------- | ---------------------------------------------------------- |
| `security.credential_retrieval_mode`   | enum   | `"dynamic_only"` | One of `none` / `dynamic_only` / `static_only` / `all`. The default returns credentials for every session this server launched dynamically — the check is the session's origin, not which client created it, and it includes sessions created with an explicit `auth_token` — while withholding operator-authored static credentials. The wrapping `security` block may be omitted, which is equivalent to writing it empty; it may not be `null`. |
| `session_creation.max_concurrent_sessions` | int \| null | `5` | Hard cap on concurrent dynamic sessions. `null` disables the cap (unbounded). |
| `session_creation.defaults`            | object | —        | Per-session defaults (see next table).                     |
| `timeouts`                             | object | `{}` (all defaults) | All operator-tunable durations, grouped under `client` and `eviction`. See next two tables. |
| `response_limits`                      | object | `{}` (all defaults) | Tool-side response-size guard thresholds. See [`response_limits`](#response_limits). |

### `community/settings.json` `timeouts.client`

Timeouts the Deephaven Community client layer applies to outbound
RPCs. All fields are optional.

| Field                             | Type  | Default |
| --------------------------------- | ----- | ------- |
| `session_connect_timeout_seconds` | float | `60.0`  |

### `community/settings.json` `timeouts.eviction`

MCP-side idle-session eviction sweeper for the community registry.
All fields are optional.

| Field                          | Type  | Default  | Description                                                            |
| ------------------------------ | ----- | -------- | ---------------------------------------------------------------------- |
| `session_idle_timeout_seconds` | float | `3600.0` | Seconds of inactivity after which an MCP-cached session is evicted.    |
| `sweep_interval_seconds`       | float | `60.0`   | Cadence at which the eviction sweep runs.                              |

### `session_creation.defaults`

Mode-agnostic fields (apply to every dynamic session regardless of
`launch_method`):

| Field                            | Type    | Default                                                            |
| -------------------------------- | ------- | ------------------------------------------------------------------ |
| `launch_method`                  | enum    | `"docker"` (also `"python"`)                                       |
| `auth.credentials`               | object  | `null` (PSK auto-generated when needed)                            |
| `programming_language`           | enum    | `"Python"` (also `"Groovy"`)                                       |
| `heap_size_gb`                   | float   | `4.0`                                                              |
| `extra_jvm_args`                 | list    | `null`                                                             |
| `environment_vars`               | dict    | `null`                                                             |
| `startup_timeout_seconds`        | float   | `60.0`                                                             |
| `startup_check_interval_seconds` | float   | `2.0`                                                              |
| `startup_retries`                | int     | `3`                                                                |

Mode-specific defaults live in two nested blocks. Both blocks are
always present (default-constructed when omitted from the JSON), so
operators can set defaults for both modes and flip `launch_method`
per call without rewriting `community/settings.json`.

#### `docker` (consulted when `launch_method == "docker"`)

| Field                          | Type   | Default                                  |
| ------------------------------ | ------ | ---------------------------------------- |
| `docker.images.python`         | str    | `"ghcr.io/deephaven/server:latest"`      |
| `docker.images.groovy`         | str    | `"ghcr.io/deephaven/server-slim:latest"` |
| `docker.memory_limit_gb`       | float  | `null`                                   |
| `docker.cpu_limit`             | float  | `null`                                   |
| `docker.volumes`               | list   | `null`                                   |

#### `python` (consulted when `launch_method == "python"`)

| Field                | Type | Default |
| -------------------- | ---- | ------- |
| `python.venv_path`   | str  | `null`  |

The launcher picks `docker.images.python` or `docker.images.groovy`
based on the resolved `programming_language`. The MCP tool's
per-call `docker_image` parameter overrides the schema choice.
`python.venv_path == null` falls back to the MCP server's own venv.

The worker-side authentication handler is **derived** from
`auth.credentials.type`; it is not a separate JSON knob. The
mapping is fixed:

- `psk` (or `null` credentials) → PSK handler
  (`io.deephaven.authentication.psk.PskAuthenticationHandler`).
- `anonymous` → `Anonymous`.
- `custom` → the FQCN carried on `auth.credentials.auth_type`.
- `password` is rejected for dynamically-launched workers (no
  pre-configured user database for Basic to validate against);
  use `password` only on `community/sessions/<name>.json` files
  declaring a pre-existing worker, or on enterprise systems.

## `community/sessions/<name>.json`

One file per static community session. The filename stem is the
session name: ASCII letters, digits, `_`, and `-` only, starting with a
letter or digit (dots are not allowed — the name must work as one
segment of a dotted `dhcli config` path). Fields:

| Field                | Type    | Required | Description                                                                 |
| -------------------- | ------- | -------- | --------------------------------------------------------------------------- |
| `host`               | str     | no       | Hostname; defaults to whatever the SDK derives.                             |
| `port`               | int (1–65535) | no | Port number.                                                                |
| `programming_language` | enum  | no       | Exactly `"Python"` or `"Groovy"`.                                            |
| `never_timeout`      | bool    | no       | Disables client-side idle timeout.                                          |
| `tls`                | object  | no       | Presence enables TLS. See [TLS](#tls).                                      |
| `auth.credentials`   | object  | **yes**  | One of the credential kinds below.                                          |

The filename stem must match the optional `session_name` field
when present.

### Credential kinds (`auth.credentials.type`)

| `type`        | Required fields              | Notes                                                            |
| ------------- | ---------------------------- | ---------------------------------------------------------------- |
| `anonymous`   | —                            | No bearer material.                                              |
| `psk`         | `token`                      | Pre-shared key for Deephaven Community PSK.                      |
| `password`    | `username`, `password`       | Optional `effective_user` for sudo-style delegation.             |
| `private_key` | `key_text`                   | Enterprise private-key auth; `key_text` is the PEM contents.     |
| `custom`      | `auth_type`, `auth_token`    | Escape hatch for arbitrary Java auth handlers.                   |

Every secret field accepts `${env:NAME}` indirection. Every file
field accepts `${file:PATH}` indirection.

### TLS

```json5
{
  "tls": {
    "root_certs":          "${file:/etc/ssl/dh-ca.pem}",       // optional
    "client_certificate": {                                      // optional (mTLS)
      "cert_chain":  "${file:/etc/ssl/client.pem}",
      "private_key": "${file:/etc/ssl/client.key}"
    }
  }
}
```

Presence of the `tls` block (even `{}`) enables TLS. The
`client_certificate` sub-block requires *both* halves when set.

## `enterprise/settings.json`

Tunables that apply to *every* enterprise system this server manages.
The file is optional; omit it to accept all defaults.

| Field                                  | Type   | Default             | Description                                                |
| -------------------------------------- | ------ | ------------------- | ---------------------------------------------------------- |
| `timeouts`                             | object | `{}` (all defaults) | All operator-tunable durations, grouped under `client` and `eviction`. See next two tables. |
| `pq_tools`                             | object — see [below](#pq_tools) | `{}` (all defaults) | Defaults applied by the persistent-query MCP tools.        |
| `response_limits`                      | object | `{}` (all defaults) | Tool-side response-size guard thresholds. See [`response_limits`](#response_limits). |

### `pq_tools`

Defaults applied by the persistent-query MCP tools
(`pq_start`, `pq_stop`, `pq_restart`, `pq_delete`). PQ tools are
enterprise-only, which is why this block lives on
`enterprise/settings.json` rather than `server.json`.

| Field                    | Type | Default |
| ------------------------ | ---- | ------- |
| `default_max_concurrent` | int  | `20`    |

Timeout durations for PQ lifecycle operations are operator-tuned via
`enterprise/settings.json: timeouts.client.*` (next table). The PQ tools do
not expose numeric `timeout_seconds` parameters to AI agents;
`pq_start`, `pq_stop`, and `pq_restart` accept a `wait: bool`
parameter (default `True`) so agents can choose
wait-for-completion vs. fire-and-forget without picking a timeout.

### `response_limits`

Thresholds applied by the tool-side response-size guard. The same
schema is consumed by both `community/settings.json` and
`enterprise/settings.json` — each section carries its own copy so
community and enterprise deployments can be tuned independently.
All fields are optional.

| Field                       | Type | Default               | Description                                                                                                  |
| --------------------------- | ---- | --------------------- | ------------------------------------------------------------------------------------------------------------ |
| `max_response_bytes`        | int  | `52428800` (50 MiB)   | Hard ceiling on the estimated serialized response size. Tools refuse to serialize and return a structured error asking the caller to reduce `max_rows`. |
| `warning_response_bytes`    | int  | `5242880` (5 MiB)     | Threshold above which a warning is logged but the response is still served. Must be ≤ `max_response_bytes`.  |
| `estimated_bytes_per_cell`  | int  | `50`                  | Conservative bytes-per-cell estimate used to project response size before serialization.                     |

### `enterprise/settings.json` `timeouts.client`

Timeouts the enterprise client layer (factory + controller) applies
to outbound Deephaven calls and persistent-query state waits. All
fields are optional. Most fields are floats (seconds);
`pq_state_change_timeout_seconds` is an `int` because the upstream
Java API requires it.

| Field                             | Type  | Default |
| --------------------------------- | ----- | ------- |
| `session_connect_timeout_seconds` | float | `60.0`  |
| `worker_creation_timeout_seconds` | float | `60.0`  |
| `pq_connection_timeout_seconds`   | float | `60.0`  |
| `auth_timeout_seconds`            | float | `60.0`  |
| `saml_auth_timeout_seconds`       | float | `120.0` |
| `quick_operation_timeout_seconds` | float | `5.0`   |
| `subscribe_timeout_seconds`       | float | `30.0`  |
| `controller_resubscribe_recreate_interval_seconds` | float | `30.0`  |
| `pq_management_timeout_seconds`   | float | `60.0`  |
| `pq_state_change_timeout_seconds` | int   | `120`   |
| `no_wait_seconds`                 | float | `0.0`   |

### `enterprise/settings.json` `timeouts.eviction`

MCP-side idle-session eviction sweeper, applied uniformly to every
enterprise system. All fields are optional.

| Field                          | Type  | Default  | Description                                                            |
| ------------------------------ | ----- | -------- | ---------------------------------------------------------------------- |
| `session_idle_timeout_seconds` | float | `3600.0` | Seconds of inactivity after which an MCP-cached session is evicted.    |
| `sweep_interval_seconds`       | float | `60.0`   | Cadence at which the eviction sweep runs.                              |

## `enterprise/systems/<name>.json`

One file per Deephaven Enterprise system. The filename stem is the
system name, under the same character rule as session names (letters,
digits, `_`, `-`; no dots); `community` is reserved for the community
umbrella system. Fields:

| Field                                 | Type   | Required | Default | Description                                                                 |
| ------------------------------------- | ------ | -------- | ------- | --------------------------------------------------------------------------- |
| `connection_json_url`                 | str    | **yes**  | —       | URL of the Enterprise `connection.json`.                                    |
| `auth.credentials`                    | object | **yes**  | —       | `password` or `private_key`. (`anonymous`/`psk` are community-only.)        |
| `session_creation`                    | object | no       | —       | When present, enables session creation; see next table.                     |

Idle/sweep timers are system-wide and live on
`enterprise/settings.json` under `timeouts.eviction.*` (mirroring the
community side); they are *not* per-system fields. Likewise the
connection timeout for factory construction is the global
`enterprise/settings.json: timeouts.client.session_connect_timeout_seconds`;
there is no per-system override.

Enterprise systems do **not** accept a `tls` block. The Deephaven
Enterprise `SessionManager` fetches its truststore via the
`connection.json`'s `truststore_url`.

### `session_creation`

| Field                     | Type        | Required | Default | Description                                                                                                  |
| ------------------------- | ----------- | -------- | ------- | ------------------------------------------------------------------------------------------------------------ |
| `max_concurrent_sessions` | int \| null | no       | `5`     | Per-system cap on concurrent enterprise sessions MCP may run against this system. `null` disables the cap (unbounded). No aggregate enterprise-wide cap exists; the effective total is the sum of per-system values. |
| `defaults`                | object      | no       | `{}`    | Per-session defaults (see next table).                                                                       |

### `session_creation.defaults`

| Field                  | Type      | Required | Default              |
| ---------------------- | --------- | -------- | -------------------- |
| `heap_size_gb`         | float     | no       | `4.0`                |
| `auto_delete_timeout`  | positive int \| null | no | `null`           |
| `server`               | str \| null | no     | `null`               |
| `engine`               | enum      | no       | `"DeephavenCommunity"` (also `"DeephavenEnterprise"`) |
| `extra_jvm_args`       | list      | no       | `null`               |
| `environment_vars`     | dict      | no       | `null`               |
| `admin_groups`         | list      | no       | `null`               |
| `viewer_groups`        | list      | no       | `null`               |
| `session_arguments`    | dict      | no       | `null`               |
| `programming_language` | enum      | no       | `"Python"`           |

## Reloading

Configuration is read **once at server startup**. There is no
hot-reload path; restart the server to pick up changes.

This applies to the `dhcli` daemon too: a running daemon keeps the tree
it loaded, so a config change does not reach it until you run
`dhcli daemon stop` (the next command starts a fresh one) or
`dhcli daemon restart`. The `dhcli config` authoring verbs print this
reminder after every successful write, without checking whether a daemon
is actually running.

## See also

- [`docs/ENV.md`](ENV.md) — the (small) set of environment variables the
  server processes themselves consume.
- [`docs/CLI.md`](CLI.md) — the `dhcli config` verbs that author,
  inspect, and validate this tree.
- [`docs/DEVELOPER_GUIDE.md`](DEVELOPER_GUIDE.md) — building and running
  the servers.
- [`docs/SECURITY.md`](SECURITY.md) — secret-handling guarantees.
- [`config-samples/ai/config/`](../config-samples/ai/config/) —
  copy-paste-ready example configuration directory.
