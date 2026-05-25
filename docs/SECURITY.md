# Deephaven MCP Security Guide

**Read this once before deploying.** The defaults are safe for local
development; non-loopback deployments require a few explicit
operator decisions. This page is the single source of truth for the
runtime security model. For the project's vulnerability-disclosure
policy, see the root [`SECURITY.md`](../SECURITY.md).

## Am I in scope for this guide?

> **If you only run `dh-mcp-systems-server` on your own machine, use
> stdio transport (the default), and have not exposed any TCP port
> from the server process, the defaults are already safe and this
> guide does not apply to you.** You can stop reading here.

This guide is for operators who want **other clients on the same
machine** to talk to a running `dh-mcp-systems-server` over HTTP
(via `mcp-proxy`, a TLS-terminating reverse proxy, a Cloud Run
deployment, etc.). The HTTP transport requires explicit decisions
about authentication and a fronting TLS layer; the rest of this
page walks through those decisions.

## Trust model in one paragraph

`dh-mcp-systems-server` is a **single-binary multiplexed MCP server**
with two transports:

- **stdio** (default): the OS pipe is the trust boundary. There is
  no network surface and no authentication.
- **HTTP** (streamable-HTTP): a single pre-shared key (PSK) gates
  every request via the `X-Deephaven-PSK` header. The transport
  binds to **loopback only** (`127.0.0.1`, `::1`, or `localhost`)
  and refuses to start on any other host. There is **no native TLS**;
  operators who need TLS terminate it at a reverse proxy on the same
  host and forward to `127.0.0.1:<port>`.

Outbound credentials (the secrets the server uses to talk to
Deephaven Community workers and Deephaven Enterprise controllers)
are configured in the JSON tree under `$DH_MCP_CONFIG_DIR` and
resolved once at startup. Restart to rotate.

## Hardening checklist

Walk through these items before exposing the HTTP transport to
anything beyond the local process group.

- [ ] **Use stdio if you can.** stdio has no network surface, no
      auth required, and is the recommended transport for
      local-IDE integrations (Claude Desktop, Cursor, mcp-proxy
      bridges that already terminate transport security upstream).
- [ ] **Configure a non-empty PSK** for HTTP. Set the `psk` field in
      `server.json` (or pass `--psk` on the CLI). The server refuses
      to start the HTTP transport without one. Use
      `${env:NAME}` templating to source it from an environment
      variable rather than committing the literal value to disk.
- [ ] **Bind to loopback only.** The default `host` is `127.0.0.1`
      and the server refuses to bind anything else. Do not bypass
      this; expose the port through a reverse proxy if you need to
      reach it from another host.
- [ ] **Terminate TLS upstream.** When a non-local client needs to
      reach the server, deploy a TLS-terminating reverse proxy
      (nginx, Caddy, Envoy, AWS ALB, Cloud Run) on the same host and
      forward to `127.0.0.1:<port>`. The MCP transport itself is
      cleartext; do not expose the loopback port directly to a
      non-local network.
- [ ] **Use `${env:NAME}` (or `${file:PATH}`) for every secret in
      the config tree.** Credential `password`, `key_text`,
      `auth_token`, and the server `psk` field are all
      `pydantic.SecretStr` and accept templating. Never commit
      literal secrets to a config file checked into source control.
- [ ] **Lock down `$DH_MCP_CONFIG_DIR` permissions.** On POSIX:
      `chmod 700` the directory and `chmod 600` every file inside.
      The startup audit refuses to start the server if any file or
      subdirectory under the configuration root is owned by another
      UID or has any group/other permission bits set
      (`stat & 0o077 != 0`). On Windows, the audit requires the
      configuration directory be located under the current user
      profile (`%APPDATA%/Deephaven/ai/config/` satisfies this).
- [ ] **Leave `security.credential_retrieval_mode` at `null` /
      `"none"` unless you specifically need AI agents to read
      community session tokens.** When set to `"dynamic_only"`,
      `"static_only"`, or `"all"`, the
      `session_community_credentials` MCP tool will return plaintext
      tokens to its caller. Audit logs every call.
- [ ] **Rotate** any PSK or outbound credential that has appeared in
      a log file, shell history, or version-control commit. PSK
      rotation requires a server restart (there is no
      `mcp_reload`).

## Threat model

**What this server protects against, when configured per the
checklist:**

- **Unauthorized HTTP access.** Every non-`/health` HTTP request is
  rejected with `401 Unauthorized` unless the
  `X-Deephaven-PSK` header is present and matches the configured
  value (compared with `hmac.compare_digest`).
- **Network exposure of the MCP transport.** The HTTP server refuses
  to bind any non-loopback host, so the cleartext MCP traffic never
  leaves the kernel without operator action.
- **Credential leakage via logs / config dumps.** Every secret-
  bearing field on the typed credential and TLS schemas is a
  `pydantic.SecretStr`. The default `repr` masks the value
  (`SecretStr('**********')`). Configuration summaries logged at
  startup use `model_dump(context={"redact": True})` and emit the
  project's `[REDACTED]` sentinel for every secret field.
- **Tampered or world-readable config tree.** The startup permission
  audit refuses to start the server if any file or subdirectory
  under `$DH_MCP_CONFIG_DIR` is owned by another UID or accessible
  to anyone other than the running user.

**What this server does NOT protect against:**

- **Compromised host or insider access** to environment variables,
  the configuration directory, or process memory.
- **Cleartext PSK and MCP traffic on the loopback interface.** The
  PSK is sent in clear over the `X-Deephaven-PSK` header. The
  server expects loopback (or a TLS-terminating proxy on the same
  host) to provide confidentiality.
- **Inline secrets committed to a config file.** Operators must use
  `${env:NAME}` or `${file:PATH}` indirection for every secret;
  inlining a literal PSK or password into a tracked config file is
  outside the threat model.
- **Vulnerabilities in upstream Deephaven workers or in MCP clients.**

## Authentication

### Inbound (clients calling the systems server)

- **stdio transport:** no authentication. The OS process boundary
  is the trust boundary.
- **HTTP transport:** every request must carry the configured PSK in
  the `X-Deephaven-PSK` header. Comparison uses
  `hmac.compare_digest` to prevent timing attacks. Empty PSKs are
  refused at startup. The `/health` endpoint is exempt from the gate
  so external liveness probes work without sharing the PSK.
- A failed gate produces an HTTP 401 response with a structured
  JSON body and a stable machine-readable `code`:

  ```json
  {
    "error": "Unauthorized",
    "code": "psk_missing",
    "detail": "Authentication required: ..."
  }
  ```

  Possible codes are `psk_missing` (header absent) and
  `psk_invalid` (header present but value did not match). The
  response also carries `WWW-Authenticate: Deephaven-PSK realm="mcp"`.

### Outbound (the systems server calling Deephaven workers)

The systems server is a long-lived process that holds outbound
credentials for every configured Community session and Enterprise
system. These credentials are loaded **once at startup** from the
JSON config tree and reused for the lifetime of the process. There
is no per-request credential resolution and no `X-Deephaven-*`
credential-forwarding header model.

Credentials are typed `pydantic` models under
`auth.credentials.type`, parsed as a discriminated union:

| Kind | Required fields | Notes |
| --- | --- | --- |
| `anonymous` | — | No bearer material; community-only. |
| `psk` | `token` | Pre-shared key for Deephaven Community PSK auth; community-only. |
| `password` | `username`, `password` | Optional `effective_user` for Enterprise sudo-style delegation. |
| `private_key` | `key_text` | PEM contents (use `${file:/path/to/key.pem}` to load from disk). |
| `custom` | `auth_type`, `auth_token` | Escape hatch for arbitrary Java auth handler classes; community-only. |

Enterprise systems accept only `password` and `private_key`; the
other kinds are rejected. See
[`docs/CONFIGURATION.md`](CONFIGURATION.md) for the full schema.

## Transport security

The HTTP transport itself is **cleartext**. The systems server does
not perform TLS termination; it relies on one of:

| Pattern | When to use |
| --- | --- |
| **Loopback only** (default) | Local development; same-host tooling that already lives inside the trust boundary. |
| **TLS-terminating reverse proxy** | Production. Run nginx / Caddy / Envoy / AWS ALB / Cloud Run on the same host (or in front of it) so it terminates TLS and forwards to `127.0.0.1:<port>`. The proxy is responsible for verifying its peer; the systems server trusts everything that arrives on the loopback port (subject to the PSK gate). |
| **mcp-proxy / IDE bridge** | When an IDE-side MCP client speaks stdio but the server runs over HTTP, run an stdio-↔-HTTP bridge on the client host and forward the PSK in the `X-Deephaven-PSK` header. |

Outbound TLS for Community sessions is configured per-session under
the optional `tls` block on each `community/sessions/<name>.json`
file. Presence of the block (even `"tls": {}`) enables TLS for that
session; the `client_certificate` sub-block enables mTLS. PEM text
is read by the templating engine via `${file:/path/to/file.pem}`
at config-load time. Enterprise systems do **not** accept a `tls`
block — the upstream Enterprise SessionManager fetches its
truststore via the connection.json's `truststore_url`. See
[`docs/CONFIGURATION.md`](CONFIGURATION.md#tls) for the full
schema.

## Secret handling

- **Templating, not shadow fields.** Every secret-bearing field
  accepts `"${env:NAME}"` / `"${env:NAME:-default}"` / `"${file:PATH}"`
  inside its string value. The legacy `<field>_env_var` /
  `<field>_path` shadow fields have been removed.
- **Resolution order.** The templating engine resolves placeholders
  *before* Pydantic validation, so the validated model carries only
  literal, fully-resolved values. Failures (missing env var,
  unreadable file, malformed placeholder) surface as
  `ConfigurationError` at startup, naming the source file and the
  JSON path of the offending value.
- **In-memory representation.** Resolved secrets are stored in
  `pydantic.SecretStr` fields on `RedactableSchema` subclasses. The
  default `repr` masks the value; logs that dump configuration use
  `model_dump(context={"redact": True})` so every secret renders as
  the project's `[REDACTED]` sentinel. Code paths that need the
  plaintext value (the registry-to-manager handoff, the session
  factory) call `.get_secret_value()` explicitly.
- **Filesystem permissions.** `verify_config_directory_permissions`
  runs at startup and aborts with a single aggregated error message
  listing every offending path. POSIX-strict
  (`stat & 0o077 == 0`, owner-must-match-UID); Windows best-effort
  (configuration directory must be under the current user profile).

## Rotation

- **PSK rotation:** update `psk` in `server.json` (or the env var
  it references) and **restart the server**. There is no
  `mcp_reload` tool; configuration is read once at startup.
- **Outbound credential rotation:** update the `${env:NAME}`
  source or the `${file:PATH}` target and **restart the server**.
  In-memory credentials are not re-resolved at runtime.

## Further reading

- [`docs/CONFIGURATION.md`](CONFIGURATION.md) — complete configuration
  schema reference (server, community, enterprise sections; templating;
  defaults).
- [`docs/ENV.md`](ENV.md) — every environment variable the server
  processes themselves consume.
- [`docs/DEVELOPER_GUIDE.md`](DEVELOPER_GUIDE.md) — building, running,
  and integrating the systems server; includes the
  [HTTP Transport Security](DEVELOPER_GUIDE.md#http-transport-security)
  section with concrete deployment commands.
- Root [`SECURITY.md`](../SECURITY.md) — vulnerability-disclosure
  policy.
