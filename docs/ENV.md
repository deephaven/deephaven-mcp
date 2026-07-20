# Environment Variables Reference

Deephaven MCP ships three binaries — `dh-mcp-systems-server`,
`dh-mcp` (the CLI), and `dh-mcp-docs-server` — and reads a small,
fixed set of environment variables. This file is the canonical
reference. Every other operator-tunable knob lives in the JSON
configuration tree; see [`docs/CONFIGURATION.md`](CONFIGURATION.md).

## Variables at a glance

| Variable | Consumed by | Purpose |
|---|---|---|
| `PYTHONLOGLEVEL` | all three binaries | Python log level. |
| `DH_MCP_DATA_DIR` | systems server, CLI | User-data root (`config/` + `runtime/`). |
| `DH_MCP_OUTPUT` | CLI | Default output mode (mirrors `-o/--output`). |
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
  fixture). Set `DH_MCP_DATA_DIR` to that root. The platform
  default (`~/.deephaven/ai/` on POSIX, `%APPDATA%/Deephaven/ai/`
  on Windows) works for everyone else.
- **Running the docs server outside Cloud Run / a managed PaaS.**
  Optionally set `MCP_DOCS_HOST` / `MCP_DOCS_PORT` if you need
  something other than `127.0.0.1:8001`.

Everything else (`PYTHONLOGLEVEL`, `DH_MCP_OUTPUT`, the templating
engine) is opt-in tuning.

## All binaries

### `PYTHONLOGLEVEL`

Standard Python logging-level override, honored at startup by all
three binaries.

| | |
|---|---|
| Default | `INFO` |
| Values | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

## Systems server and CLI

### `DH_MCP_DATA_DIR`

Override for the **user-data root** shared by the systems server
and the CLI. (The docs server does not consume this variable.) Two
subdirectories live under this root:

- `config/` — read-only configuration tree consumed by
  `dh-mcp-systems-server` and (for `cli.json`) by `dh-mcp`. The
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
| Example | `/opt/deephaven/mcp` |

**Propagating it to MCP-client subprocesses.** When an AI client
(Claude Desktop, etc.) spawns `dh-mcp`, the client must include
`DH_MCP_DATA_DIR` in its MCP server `env` block; otherwise the
spawned `dh-mcp` falls back to the platform default and won't see
your override:

```json5
// Example: inside your MCP client config
{
  "env": {
    "DH_MCP_DATA_DIR": "/full/path/to/your/dh-mcp-data"
  }
}
```

**No per-subdir env vars.** `DH_MCP_CONFIG_DIR` and
`DH_MCP_RUNTIME_DIR` do not exist by design: a single knob moves
both subdirectories, which matches the operator use cases that
motivated it (containers, chroots, custom install paths). For
targeted per-subdir overrides — e.g. a test pointing only the
config tree elsewhere — use the CLI flags:

```bash
dh-mcp --config-dir /tmp/test-config --runtime-dir /tmp/test-runtime tool list
```

`dh-mcp-systems-server` exposes the same `--config-dir` and
`--runtime-dir` flags, both honored in every transport (stdio,
HTTP, and daemon); the `dh-mcp` CLI also passes them when spawning
the daemon. These flags **bypass** `$DH_MCP_DATA_DIR` for the
subdirectory they target; the env var still applies to whichever
subdirectory was not overridden.

## CLI (`dh-mcp`)

### `DH_MCP_OUTPUT`

Default output mode for `dh-mcp`, mirroring the `-o/--output` flag.
(This is the only `dh-mcp` flag with an env-var binding — output
format varies often enough across shells, pipelines, and AI tools
to justify it.)

Precedence: **CLI flag** > **`DH_MCP_OUTPUT`** > **`cli.json`'s `output`** > **schema default**.

| | |
|---|---|
| Values | `human`, `json`, `json-pretty`, `yaml` |
| CLI flag | `-o`, `--output` |
| `cli.json` field | `output` |

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
time. See the *Templating* section of `docs/CONFIGURATION.md` for
the full syntax including escapes and nested placeholders.

Example:

```json5
// $DH_MCP_DATA_DIR/config/server.json
{
  "transport": "http",
  "port": 8000,
  "psk": "${env:DH_MCP_PSK}"                          // value from env
}

// $DH_MCP_DATA_DIR/config/enterprise/settings.json
{
  "timeouts": {
    "auth_timeout_seconds": "${env:DH_AUTH_TIMEOUT:-60}"  // env or default
  }
}
```

This is the supported way to make any individual JSON field
env-overridable. Pick whatever variable name suits your environment;
the names in the example are just illustrations, not reserved.
