# Environment Variables Reference

The Deephaven MCP servers consume **one** environment variable directly:
`DH_MCP_CONFIG_DIR`. Everything else lives in the JSON configuration
tree — see [`docs/CONFIGURATION.md`](CONFIGURATION.md) for the full
reference.

Operators who want to pull individual JSON values from environment
variables write `"${env:NAME}"` (or `"${env:NAME:-default}"`) anywhere
a string value is accepted. The templating engine resolves the
placeholder when the file is loaded. See the *Templating* section of
`docs/CONFIGURATION.md` for the full syntax.

## Table of Contents

- [Environment Variables Reference](#environment-variables-reference)
  - [Table of Contents](#table-of-contents)
  - [Systems Server](#systems-server)
    - [`DH_MCP_CONFIG_DIR`](#dh_mcp_config_dir)
    - [`PYTHONLOGLEVEL`](#pythonloglevel)
  - [Docs Server](#docs-server)
    - [`INKEEP_API_KEY`](#inkeep_api_key)
    - [`MCP_DOCS_HOST`](#mcp_docs_host)
    - [`MCP_DOCS_PORT`](#mcp_docs_port)
    - [`PORT`](#port)
  - [Where the systems-server env vars went](#where-the-systems-server-env-vars-went)

## Systems Server

### `DH_MCP_CONFIG_DIR`

Override for the configuration *directory* (`dh-mcp-systems-server`
reads a directory tree, not a single file). When unset, the server
falls back to the platform default.

| | |
|---|---|
| Required | No (a default location is used when unset) |
| Default (POSIX) | `~/.deephaven/ai/config/` |
| Default (Windows) | `%APPDATA%/Deephaven/ai/config/` |
| Example | `/etc/deephaven/mcp` |

The CLI flag `--config-dir` takes precedence over this env var.

Set it in your AI tool's MCP server `env` block:

```json5
// Example: inside your MCP client config (e.g. Claude Desktop)
{
  "env": {
    "DH_MCP_CONFIG_DIR": "/full/path/to/your/config"
  }
}
```

### `PYTHONLOGLEVEL`

Standard Python logging-level override. Controls the verbosity of log
output from the MCP servers.

| | |
|---|---|
| Required | No |
| Default | `INFO` |
| Values | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

## Docs Server

The Docs Server (`dh-mcp-docs-server`) is an optional component that
provides AI-powered Deephaven documentation search. It has its own
set of environment variables.

### `INKEEP_API_KEY`

**Required** for the Docs Server. API key for the
[Inkeep](https://inkeep.com)-powered documentation LLM backend.

| | |
|---|---|
| Required | Yes (Docs Server only) |
| Default | *(none — server will not start without this)* |

### `MCP_DOCS_HOST`

Host interface the Docs Server HTTP server binds to.

| | |
|---|---|
| Required | No |
| Default | `127.0.0.1` (localhost only) |
| Example | `0.0.0.0` (all interfaces, for Docker/remote access) |

### `MCP_DOCS_PORT`

Port the Docs Server HTTP server listens on.

| | |
|---|---|
| Required | No |
| Default | `MCP_DOCS_PORT` → `PORT` (Cloud Run) → `8001` |

### `PORT`

Standard Cloud Run port variable. Used as a fallback when
`MCP_DOCS_PORT` is not set.

## Where the systems-server env vars went

The legacy `MCP_HOST`, `MCP_PORT`, `DH_MCP_HTTP_PORT`,
`DH_MCP_PSK`, `DH_MCP_SERVER_NAME`, the entire
`DH_MCP_*_TIMEOUT_SECONDS` family, `DH_MCP_NO_WAIT_SECONDS`,
`DH_MCP_TIMEOUT_WARNING_THRESHOLD`, `DH_MCP_DEFAULT_PQ_TIMEOUT`,
and `DH_MCP_DEFAULT_MAX_CONCURRENT` have all been retired. Every
one of those knobs now lives on `ServerConfig` in `server.json`;
see [`docs/CONFIGURATION.md`](CONFIGURATION.md).

Operators who want to pull any individual field from an environment
variable can do so with the JSON templating syntax — for example:

```json5
// $DH_MCP_CONFIG_DIR/server.json
{
  "transport": "http",
  "port": 8000,
  "psk": "${env:DH_MCP_PSK}"                        // value from env
}

// $DH_MCP_CONFIG_DIR/enterprise/settings.json
{
  "timeouts": {
    "auth_timeout_seconds": "${env:DH_AUTH_TIMEOUT:-60}"  // env or default
  }
}
```
