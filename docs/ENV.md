# Environment Variables Reference

Deephaven MCP ships three binaries — `dh-mcp-systems-server`,
`dhcli` (the CLI), and `dh-mcp-docs-server` — and reads a small,
fixed set of environment variables. This file is the canonical
reference. Every other operator-tunable knob lives in the JSON
configuration tree; see [`docs/CONFIGURATION.md`](CONFIGURATION.md).

## Table of Contents

- [Variables at a glance](#variables-at-a-glance)
- [Minimum setup](#minimum-setup)
- [All binaries](#all-binaries)
- [Systems server and CLI](#systems-server-and-cli)
- [CLI (`dhcli`)](#cli-dhcli)
- [Docs server](#docs-server)
- [Templating from environment variables](#templating-from-environment-variables)

## Variables at a glance

| Variable | Consumed by | Purpose |
|---|---|---|
| `PYTHONLOGLEVEL` | all three binaries | Python log level. |
| `DH_AI_DATA_DIR` | systems server, CLI | User-data root (`config/` + `runtime/`). |
| `APPDATA` | systems server, CLI | Windows only: base for the default user-data root. |
| `DHCLI_OUTPUT` | CLI | Default output mode (mirrors `-o/--output`). |
| `VISUAL` / `EDITOR` | CLI | Editor launched by `dhcli config edit`. |
| `INKEEP_API_KEY` | docs server | **Required.** Inkeep LLM API key. |
| `MCP_DOCS_HOST` | docs server | HTTP bind host. |
| `MCP_DOCS_PORT` | docs server | HTTP bind port. |
| `PORT` | docs server | Cloud Run / PaaS fallback for `MCP_DOCS_PORT`. |

To inject *individual* JSON values from the environment, use the
configuration templating engine — see
[Templating from environment variables](#templating-from-environment-variables)
below.

## Minimum setup

Most users do not need to set any of these variables. The cases
that actually require action:

- **Running the docs server (`dh-mcp-docs-server`).** Set
  `INKEEP_API_KEY`. The server refuses to start without it.
- **Keeping your config tree in a non-standard location** (a
  shared install path, a container volume, a chroot, a test
  fixture). Set `DH_AI_DATA_DIR` to that root. The platform
  default (`~/.deephaven/ai/` on POSIX, `%APPDATA%/Deephaven/ai/`
  on Windows) works for everyone else.
- **Running the docs server outside Cloud Run / a managed PaaS.**
  Optionally set `MCP_DOCS_HOST` / `MCP_DOCS_PORT` if you need
  something other than `127.0.0.1:8001`.

Everything else is opt-in tuning (`PYTHONLOGLEVEL`, `DHCLI_OUTPUT`,
`VISUAL` / `EDITOR`, the templating engine) or set for you by the
operating system (`APPDATA`).

## All binaries

### `PYTHONLOGLEVEL`

Standard Python logging-level override, honored at startup by all
three binaries.

| | |
|---|---|
| Default | `INFO` |
| Values | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

## Systems server and CLI

### `DH_AI_DATA_DIR`

Override for the **user-data root** shared by the systems server
and the CLI. (The docs server does not consume this variable.) Two
subdirectories live under this root:

- `config/` — read-only configuration tree consumed by
  `dh-mcp-systems-server` and (for `cli.json`) by `dhcli`. The
  systems server reads a directory tree, not a single file.
- `runtime/` — mutable per-user state owned by the running daemon
  (registry, lock, log).

Leave the env var unset to use the platform default; set it only
when your config tree lives somewhere else (shared install path,
container volume, chroot, test fixture).

| | |
|---|---|
| Default (POSIX) | `~/.deephaven/ai/` |
| Default (Windows) | `%APPDATA%/Deephaven/ai/` (standard Windows roaming-app-data location) |
| Example | `/opt/deephaven/ai` |

### `APPDATA`

Standard Windows roaming-app-data path. Read **only on Windows**, and
only when `DH_AI_DATA_DIR` is unset, to build the default user-data root
`%APPDATA%/Deephaven/ai/`. Windows always sets it; if it is unset or
empty, the resolver falls back to `~/.deephaven/ai/`. Not read on POSIX,
and never set this yourself to relocate Deephaven data — use
`DH_AI_DATA_DIR`, which is the supported knob.

| | |
|---|---|
| Consumed by | systems server, CLI (Windows only) |
| Set by | the operating system |

**Propagating it to MCP-client subprocesses.** When an AI client
(Claude Desktop, etc.) spawns `dh-mcp-systems-server`, the client
must include `DH_AI_DATA_DIR` in its MCP server `env` block;
otherwise the spawned server falls back to the platform default and
won't see your override:

```json5
// Example: inside your MCP client config
{
  "env": {
    "DH_AI_DATA_DIR": "/full/path/to/your/deephaven-ai-data"
  }
}
```

**No per-subdir env vars.** `DH_AI_CONFIG_DIR` and
`DH_AI_RUNTIME_DIR` do not exist by design: a single knob moves
both subdirectories, which matches the operator use cases that
motivated it (containers, chroots, custom install paths). For
targeted per-subdir overrides — e.g. a test pointing only the
config tree elsewhere — use the CLI flags:

```bash
dhcli --config-dir /tmp/test-config --runtime-dir /tmp/test-runtime tool list
```

`dh-mcp-systems-server` exposes the same `--config-dir` and
`--runtime-dir` flags, both honored in every transport (stdio,
HTTP, and daemon); the `dhcli` CLI also passes them when spawning
the daemon. These flags **bypass** `$DH_AI_DATA_DIR` for the
subdirectory they target; the env var still applies to whichever
subdirectory was not overridden.

## CLI (`dhcli`)

### `DHCLI_OUTPUT`

Default output mode for `dhcli`, mirroring the `-o/--output` flag.
(This is the only `dhcli` flag with an env-var binding — output
format varies often enough across shells, pipelines, and AI tools
to justify it.)

Precedence: **CLI flag** > **`DHCLI_OUTPUT`** > **`cli.json`'s `output.format`** > **schema default**.

| | |
|---|---|
| Values | `human`, `json`, `json-pretty`, `yaml` |
| CLI flag | `-o`, `--output` |
| `cli.json` field | `output.format` |

### `VISUAL` and `EDITOR`

Which editor `dhcli config edit` launches. **Set either one; if you only
ever set `EDITOR`, that is enough.**

Deephaven MCP does not implement this lookup — `dhcli config edit` calls
[`click`](https://click.palletsprojects.com/)'s `click.edit()`, which
honors the usual Unix convention: `VISUAL`, then `EDITOR`, then a
platform default (`notepad` on Windows; otherwise the first of
`sensible-editor`, `vim`, `nano` found on `PATH`, falling back to `vi`).
`VISUAL` wins when both are set.

Neither has a `cli.json` field: your editor is a property of your shell,
not of this project's configuration. No other command reads them.

`dhcli config edit` is interactive-only: with `--no-input`, or when stdin
is not a TTY, it fails with `no_tty` rather than launching an editor.

| | |
|---|---|
| Precedence | `VISUAL` > `EDITOR` > platform default |
| Example | `export EDITOR=nvim` |

## Docs server

The docs server (`dh-mcp-docs-server`) has no JSON config; the
variables below are its entire configuration surface.

### `INKEEP_API_KEY`

API key for the [Inkeep](https://inkeep.com)-powered documentation
LLM backend. The docs server refuses to start without it.

| | |
|---|---|
| Required | **Yes** — server will not start without it |

### `MCP_DOCS_HOST`

Host interface the docs server HTTP listener binds to.

| | |
|---|---|
| Default | `127.0.0.1` (loopback only) |
| Example | `0.0.0.0` (all interfaces, e.g. inside a container) |

### `MCP_DOCS_PORT` (and `PORT` fallback)

Port the docs server HTTP listener binds to. Resolved first-match-wins:

1. `MCP_DOCS_PORT` — the Deephaven-specific knob.
2. `PORT` — the standard Cloud Run / PaaS variable, set
   automatically by managed platforms.
3. `8001` — the built-in default.

Operators running outside a managed platform should set
`MCP_DOCS_PORT` directly; `PORT` exists only so Cloud Run / similar
deployments work without configuration.

| | |
|---|---|
| Default | `8001` |

## Templating from environment variables

Anywhere a string value is accepted in the JSON configuration tree,
operators can pull the value from an environment variable using
either of these placeholders:

- `"${env:NAME}"` — substitute the value of `$NAME`; fail if unset.
- `"${env:NAME:-default}"` — substitute the value of `$NAME`; if
  unset or empty, use `default`.

The templating engine resolves the placeholder once at file-load
time. Placeholders may appear anywhere inside a string value, but
**nesting is not supported** and there is no escape syntax. See the
*Templating* section of [`docs/CONFIGURATION.md`](CONFIGURATION.md) for
the full syntax, including the `${file:PATH}` form.

Example:

```json5
// $DH_AI_DATA_DIR/config/server.json
{
  "transport": "http",
  "port": 8000,
  "psk": "${env:DH_MCP_PSK}"                          // value from env
}

// $DH_AI_DATA_DIR/config/enterprise/settings.json
{
  "timeouts": {
    "auth_timeout_seconds": "${env:DH_AUTH_TIMEOUT:-60}"  // env or default
  }
}
```

This is the supported way to make any individual JSON field
env-overridable. Pick whatever variable name suits your environment;
the names in the example are just illustrations, not reserved.
