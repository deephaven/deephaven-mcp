# Environment Variables Reference

This document describes all environment variables recognized by Deephaven MCP.
Variables are grouped by server component.

> **Security note**: Never commit credentials to version control. Use
> `*_env_var` fields in `deephaven_mcp.json` to reference environment variables
> instead of embedding secrets directly in config files.

---

## Table of Contents

- [Systems Server](#systems-server)
  - [Core](#core)
  - [Credential variables (user-defined names)](#credential-variables-user-defined-names)
  - [Timeout tuning](#timeout-tuning)
- [Docs Server](#docs-server)

---

## Systems Server

### Core

#### `DH_MCP_CONFIG_FILE`

**Required.** Path to your `deephaven_mcp.json` configuration file.

| | |
|---|---|
| Required | Yes |
| Default | *(none — server will not start without this)* |
| Example | `/home/user/.config/deephaven_mcp.json` |

Set this in your AI tool's MCP server `env` block:

```json
"env": {
  "DH_MCP_CONFIG_FILE": "/full/path/to/your/deephaven_mcp.json"
}
```

See [docs/DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) for the full configuration
file format.

---

#### `PYTHONLOGLEVEL`

Controls the verbosity of log output from the MCP servers.

| | |
|---|---|
| Required | No |
| Default | `INFO` |
| Values | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| Example | `DEBUG` |

Set to `DEBUG` to get detailed per-request logs when troubleshooting connection
or authentication problems. Set to `WARNING` or `ERROR` to reduce noise in
production.

---

### Credential variables (user-defined names)

These are not fixed variable names — you choose the names and reference them
from your `deephaven_mcp.json` configuration. Deephaven MCP reads the value of
the named variable at runtime, keeping secrets out of your config file.

#### Community session: `auth_token_env_var`

Any variable name you choose. Holds the authentication token for a community
session.

```json5
{
  "community_sessions": {
    "my_session": {
      "auth_type": "password",
      "auth_token_env_var": "MY_DH_TOKEN"  // set MY_DH_TOKEN=user:password
    }
  }
}
```

#### Enterprise session: `password_env_var`

Any variable name you choose. Holds the password for an enterprise session
configured with `"auth_type": "password"`.

```json5
{
  "enterprise_sessions": {
    "prod": {
      "connection_json_url": "https://your-server.example.com/iris/connection.json",
      "auth_type": "password",
      "username": "your-username",
      "password_env_var": "PROD_DH_PASSWORD"  // set PROD_DH_PASSWORD=your-password
    }
  }
}
```

#### Enterprise session: `api_key_env_var`

Any variable name you choose. Holds the API key for an enterprise session
configured with `"auth_type": "api_key"`.

```json5
{
  "enterprise_sessions": {
    "prod": {
      "connection_json_url": "https://your-server.example.com/iris/connection.json",
      "auth_type": "api_key",
      "api_key_env_var": "PROD_DH_API_KEY"  // set PROD_DH_API_KEY=your-api-key
    }
  }
}
```

---

### Timeout tuning

These variables override the built-in timeout defaults. They are optional and
rarely need to be changed. All values are in **seconds** and must be parseable
as a float (or integer where noted). Invalid values raise an error at startup.

| Variable | Default | Description |
|---|---|---|
| `DH_MCP_SESSION_CONNECT_TIMEOUT_SECONDS` | `60.0` | Timeout for establishing the initial connection to a Deephaven server. Increase on slow or high-latency networks. |
| `DH_MCP_SUBSCRIBE_TIMEOUT_SECONDS` | `30.0` | Timeout for receiving the initial PQ state snapshot from the Enterprise controller. Increase if the controller manages a very large number of persistent queries. |
| `DH_MCP_PQ_CONNECTION_TIMEOUT_SECONDS` | `60.0` | Timeout for opening a session to a running persistent query worker. Distinct from the initial server connection timeout. |
| `DH_MCP_WORKER_CREATION_TIMEOUT_SECONDS` | `60.0` | Timeout for provisioning and connecting to a new on-demand worker. Increase on systems where worker JVM startup is slow. |
| `DH_MCP_AUTH_TIMEOUT_SECONDS` | `60.0` | Timeout for standard authentication (password, private key). |
| `DH_MCP_SAML_AUTH_TIMEOUT_SECONDS` | `120.0` | Timeout for SAML authentication. Longer than standard auth to accommodate the browser redirect roundtrip. |
| `DH_MCP_PQ_MANAGEMENT_TIMEOUT_SECONDS` | `60.0` | Timeout for PQ management operations (add, delete, modify, stop). Does not cover waiting for a worker to reach a target state — see `DH_MCP_PQ_STATE_CHANGE_TIMEOUT_SECONDS`. |
| `DH_MCP_QUICK_OPERATION_TIMEOUT_SECONDS` | `5.0` | Timeout for lightweight network round-trips (ping, key management). A timeout here typically indicates a connectivity problem. |
| `DH_MCP_PQ_STATE_CHANGE_TIMEOUT_SECONDS` | `120.0` | Timeout for waiting on a PQ to reach a target state after a start or restart. Increase for PQs with large heaps or slow initialization scripts. |
| `DH_MCP_NO_WAIT_SECONDS` | `0.0` | Sentinel value passed to controller methods to mean "return immediately without waiting". Overriding this is rarely useful. |
| `DH_MCP_TIMEOUT_WARNING_THRESHOLD` | `60` *(int)* | MCP tool operations exceeding this many seconds generate a warning, because MCP clients may time out before the operation completes. |
| `DH_MCP_DEFAULT_PQ_TIMEOUT` | `30` *(int)* | Default timeout (seconds) used by PQ lifecycle MCP tools (start, stop, restart) when the caller does not supply an explicit value. |
| `DH_MCP_DEFAULT_MAX_CONCURRENT` | `20` *(int)* | Default cap on the number of concurrent PQ operations within a single batch MCP tool call. |

---

## Docs Server

The Docs Server (`dh-mcp-docs-server`) is an optional component that provides
AI-powered Deephaven documentation search. It has its own set of environment
variables.

### `INKEEP_API_KEY`

**Required** for the Docs Server. API key for the Inkeep-powered documentation
LLM backend.

| | |
|---|---|
| Required | Yes (Docs Server only) |
| Default | *(none — server will not start without this)* |
| Obtained from | Your Inkeep account or Deephaven support |

---

### `MCP_DOCS_HOST`

Host interface the Docs Server HTTP server binds to.

| | |
|---|---|
| Required | No |
| Default | `127.0.0.1` (localhost only) |
| Example | `0.0.0.0` (all interfaces, for Docker/remote access) |

---

### `MCP_DOCS_PORT`

Port the Docs Server HTTP server listens on.

| | |
|---|---|
| Required | No |
| Default | `8001` (falls back to `PORT` for Cloud Run compatibility) |
| Example | `9000` |

The server checks `MCP_DOCS_PORT` first, then `PORT` (the standard Cloud Run
port variable), then defaults to `8001`.

---

### `PORT`

Standard Cloud Run port variable. Used as a fallback when `MCP_DOCS_PORT` is
not set. You do not normally need to set this manually.

| | |
|---|---|
| Required | No |
| Default | *(falls through to `8001`)* |
