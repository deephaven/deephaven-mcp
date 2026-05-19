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
  - [Transport-security variables](#transport-security-variables)
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

```json5
// Example: inside your MCP client config (e.g. Claude Desktop)
{
  "env": {
    "DH_MCP_CONFIG_FILE": "/full/path/to/your/deephaven_mcp.json"
  }
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

#### `MCP_HOST`

Host interface the Community or Enterprise server HTTP server binds to.

| | |
|---|---|
| Required | No |
| Default | `127.0.0.1` (localhost only) |
| Example | `0.0.0.0` (all interfaces, for Docker/remote access) |

Can also be set via the `--host` CLI argument (CLI takes precedence).

> **Security:** Authentication headers (`X-Deephaven-Password`,
> `X-Deephaven-Private-Key`, `X-Deephaven-PSK`) carry secrets in
> cleartext on the wire. When `MCP_HOST` is set to a non-loopback
> address (such as `0.0.0.0`), the server **refuses to start** unless
> at least one of `MCP_SSL_KEYFILE` + `MCP_SSL_CERTFILE` (paired),
> `MCP_TRUST_FORWARDED_PROTO`, or `MCP_ALLOW_CLEARTEXT` is set. See
> the transport-security section below for details.

---

#### `MCP_PORT`

Port the Community or Enterprise server HTTP server listens on.

| | |
|---|---|
| Required | No |
| Default | `8003` for Community server, `8002` for Enterprise server |
| Example | `9000` |

Can also be set via the `--port` CLI argument (CLI takes precedence). Precedence order: CLI argument → `MCP_PORT` env var → server default.

---

### Transport-security variables

Auth headers (`X-Deephaven-Password`, `X-Deephaven-Private-Key`,
`X-Deephaven-PSK`) **must** travel over an encrypted transport. On
startup, when `MCP_HOST` (or `--host`) is set to a non-loopback
address, the server requires exactly one of:

1. Native TLS terminated by the server itself
   (`MCP_SSL_KEYFILE` + `MCP_SSL_CERTFILE`).
2. TLS terminated by a trusted reverse proxy that sets
   `X-Forwarded-Proto: https`
   (`MCP_TRUST_FORWARDED_PROTO=1` + `MCP_FORWARDED_ALLOW_IPS=<peer-CIDRs>`).
3. An explicit cleartext opt-out for trusted private networks
   (`MCP_ALLOW_CLEARTEXT=1`, logs a loud warning every startup).

If none is configured, startup fails fast with an actionable error
message. At request time, the same policy is enforced by an ASGI
middleware: cleartext traffic from a non-loopback peer that lacks the
appropriate signal is rejected with HTTP `426 Upgrade Required`.

The `/health` endpoint bypasses this check so health probes can
succeed over either HTTP or HTTPS.

> **See also:** [`docs/SECURITY.md`](SECURITY.md) for the security model and
> hardening checklist; [Developer Guide → Transport Security (TLS)](DEVELOPER_GUIDE.md#transport-security-tls)
> for the full decision matrix, deployment patterns (native TLS,
> reverse-proxy termination, loopback-only), and rationale.

---

#### `MCP_SSL_KEYFILE`

Path to a PEM-encoded private key file for native TLS.

| | |
|---|---|
| Required | No (required only for native TLS; must be paired with `MCP_SSL_CERTFILE`) |
| Default | unset |
| Example | `/etc/ssl/private/deephaven-mcp.key` |

Can also be set via the `--ssl-keyfile` CLI argument (CLI takes precedence). When this and `MCP_SSL_CERTFILE` are both set, the server binds with TLS via [uvicorn](https://www.uvicorn.org/)'s native support. Setting only one of the two is a startup error.

---

#### `MCP_SSL_CERTFILE`

Path to a PEM-encoded certificate file for native TLS.

| | |
|---|---|
| Required | No (required only for native TLS; must be paired with `MCP_SSL_KEYFILE`) |
| Default | unset |
| Example | `/etc/ssl/certs/deephaven-mcp.crt` |

Can also be set via the `--ssl-certfile` CLI argument. The certificate file may include the full chain.

---

#### `MCP_TRUST_FORWARDED_PROTO`

Trust the `X-Forwarded-Proto: https` header from a fronting reverse proxy as proof that the client connection used TLS.

| | |
|---|---|
| Required | No |
| Default | `0` (do not trust) |
| Values | `1`, `true`, `yes` (case-insensitive) treat as truthy; everything else is falsy |
| Example | `1` |

Can also be set via the `--trust-forwarded-proto` CLI flag. Use this when terminating TLS at a reverse proxy (nginx, Envoy, Cloud Run, ALB, etc.). To prevent header spoofing, the trust only applies when the request comes from a peer in `MCP_FORWARDED_ALLOW_IPS`.

---

#### `MCP_FORWARDED_ALLOW_IPS`

Comma-separated list of peer IPs/CIDRs that are allowed to set the `X-Forwarded-Proto` header.

| | |
|---|---|
| Required | No |
| Default | `127.0.0.1` (loopback only) |
| Example | `10.0.0.0/8,192.168.0.0/16` or `*` (any peer — risky) |

Can also be set via the `--forwarded-allow-ips` CLI argument. Only honored when `MCP_TRUST_FORWARDED_PROTO=1`. Setting this to `*` disables the spoofing defense entirely and is logged as a `WARNING` at startup; use only when an L4 firewall already restricts who can reach the server.

---

#### `MCP_ALLOW_CLEARTEXT`

Emergency opt-out: explicitly accept cleartext HTTP traffic on a non-loopback bind, even with auth headers in flight.

| | |
|---|---|
| Required | No |
| Default | `0` (do not allow) |
| Values | `1`, `true`, `yes` (case-insensitive) treat as truthy; everything else is falsy |
| Example | `1` |

Can also be set via the `--allow-cleartext` CLI flag. Intended only for trusted private networks (LAN-only, air-gapped) where an out-of-band control prevents cleartext exposure. Logs a loud `WARNING` banner at startup and a periodic per-request reminder. **Do not use in production over the public internet.**

---

### Credential variables (user-defined names)

These are not fixed variable names — you choose the names and reference them
from your `deephaven_mcp.json` configuration. Deephaven MCP reads the value of
the named variable at runtime, keeping secrets out of your config file.

#### Community MCP server gate: `auth.psk_env_var`

Any variable name you choose. Holds the pre-shared key that gates access to
the **community MCP server itself** (i.e. controls who is allowed to connect
to `dh-mcp-community-server`). Mutually exclusive with the inline `auth.psk`
field.

```json5
{
  "auth": {
    "enabled": true,
    "psk_env_var": "DH_MCP_COMMUNITY_PSK"  // set DH_MCP_COMMUNITY_PSK=your-secret
  }
}
```

#### Community session: `auth_token_env_var`

Any variable name you choose. Holds the authentication token used by the MCP
server to connect **out to an individual community Deephaven worker** (a
pydeephaven client parameter — distinct from the server-gate PSK above).
Mutually exclusive with the inline `auth_token` field.

```json5
{
  "sessions": {
    "my_session": {
      "auth_type": "PSK",
      "auth_token_env_var": "MY_DH_TOKEN"  // set MY_DH_TOKEN=your-psk-token
    }
  }
}
```

#### Enterprise server: no user credentials in env vars

> **Note:** As of the authentication redesign, the enterprise server no
> longer reads user credentials from the config file or environment
> variables. The server-side config only declares the allowed auth
> backends; every MCP request must carry the caller's own Deephaven
> credentials in `X-Deephaven-*` HTTP headers (`X-Deephaven-Username`,
> `X-Deephaven-Password`, `X-Deephaven-Private-Key`, and — when
> `auth.allow_effective_user: true` — `X-Deephaven-Effective-User`).
>
> The legacy fields `auth_type`, `username`, `password`,
> `password_env_var`, and `private_key_path` have been removed from the
> enterprise config schema. Configs that still contain them will fail
> validation at startup.

```json5
// Current enterprise config - no secrets, just the allowed auth backends.
{
  "system_name": "prod",
  "connection_json_url": "https://your-server.example.com/iris/connection.json",
  "auth": {
    "backends": ["password", "private_key"],
    "allow_effective_user": false
  }
}
```

How clients supply credentials is documented in the main
[README](../README.md#client-authentication-headers) and the
[Developer Guide](./DEVELOPER_GUIDE.md#enterprise-auth-model).

---

### Timeout tuning

These variables override the built-in timeout defaults. They are optional and
rarely need to be changed. Most values are in **seconds** and must be parseable
as a float; entries marked *(int)* must be parseable as an integer. Invalid
values raise a `ValueError` at startup.

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
| `DH_MCP_PQ_STATE_CHANGE_TIMEOUT_SECONDS` | `120` *(int)* | Timeout for waiting on a PQ to reach a target state after a start or restart. Increase for PQs with large heaps or slow initialization scripts. |
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

**Required** for the Docs Server. API key for the [Inkeep](https://inkeep.com)-powered
documentation LLM backend.

| | |
|---|---|
| Required | Yes (Docs Server only) |
| Default | *(none — server will not start without this)* |
| Obtained from | Your [Inkeep](https://inkeep.com) account or Deephaven support |

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
| Default | `PORT` env var if set, otherwise `8001` (the `PORT` fallback exists for [Cloud Run](https://cloud.google.com/run) compatibility) |
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
