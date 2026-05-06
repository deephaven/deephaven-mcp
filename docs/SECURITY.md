# Deephaven MCP Security Guide

**Read this once before deploying.** The defaults are safe for local
development; production deployments require a few explicit decisions.
This page is the single source of truth for the security model. For the
vulnerability-disclosure policy, see the root [`SECURITY.md`](../SECURITY.md).

## Am I in scope for this guide?

> **If you only run Deephaven MCP on your own computer for use with your
> own AI tool — and you have not changed any host, port, or security
> settings from the defaults — the defaults are already safe and this
> guide does not apply to you.** You can stop reading here.

This guide is for people who expose an MCP server so other people or
other machines can connect to it (over a LAN, a reverse proxy, the
public internet, a Docker network, etc.). That requires explicit
decisions about authentication and transport security; the rest of the
page walks through those decisions.

## Hardening checklist

Walk through these items before exposing an MCP server to anything beyond
loopback. Each is enforced or recommended by the server itself.

- [ ] **Bind to loopback OR enable TLS** (encrypted connections, like
      HTTPS). The default `--host 127.0.0.1` is *loopback* — only
      programs on the same machine can connect, so traffic never leaves
      the kernel and is safe. For non-loopback binds (any other host,
      including `0.0.0.0`), pick one of:
  - **Native TLS** — `--ssl-keyfile` + `--ssl-certfile`. The server
    terminates TLS itself. Simplest secure option.
  - **Trusted reverse proxy** — `--trust-forwarded-proto` +
    `--forwarded-allow-ips <CIDR>`. A fronting server (nginx, Envoy,
    AWS ALB, Cloud Run) terminates TLS and forwards requests.
    `<CIDR>` is an IP-address range, e.g. `10.0.0.0/8`.
  - **`--allow-cleartext`** — explicit opt-out. Auth headers travel
    unencrypted; trusted private networks only.
- [ ] **Set `--forwarded-allow-ips` to your proxy's CIDR** when using
      `--trust-forwarded-proto`. The default is `127.0.0.1`, so a proxy on
      any other host is rejected with `426` until you set this. Never use
      `*` over the public internet — it disables the spoofing defense.
- [ ] **Never use `--allow-cleartext` over the public internet.** It is
      intended for trusted private networks (LAN, air-gapped) only.
- [ ] **(Community)** Set `auth.enabled: true` with a `psk` or `psk_env_var`.
      Disabling auth is allowed only on loopback binds; if you do, the
      server prints a prominent WARNING banner at startup.
- [ ] **(Community)** Use `*_env_var` indirection for every secret. Never
      inline `auth_token` or `psk` in committed configs.
- [ ] **(Community)** Leave `security.credential_retrieval_mode` at
      `"none"` unless you specifically need AI agents to read tokens.
      Never enable on a non-loopback bind.
- [ ] **(Enterprise)** Send credentials in `X-Deephaven-*` headers per
      request. Server-side config holds no user credentials.
- [ ] **(Enterprise)** Set `auth.allow_effective_user: true` only when
      operator clients are trusted to act as other users.
- [ ] **`chmod 600`** every config file. Even when secrets are env-var
      indirected, the file controls connection URLs and auth backends.
- [ ] **Rotate (replace)** any auth token or PSK that has appeared in a
      log file, shell history, or version control.

## Threat model

**What this software protects against, when configured per the checklist:**

- Cleartext exposure of authentication headers on the wire (TLS enforcement
  at startup and per-request).
- Header spoofing from untrusted peers behind a TLS-terminating proxy
  (`--forwarded-allow-ips` allowlist).
- Accidental credential leaks via logs (auth tokens are redacted when
  truthy; binary TLS material is redacted).

**What this software does NOT protect against:**

- Compromised hosts or insider access to environment variables, config
  files, or process memory.
- Inline secrets in committed config files (use `*_env_var` indirection).
- Anything reachable when `--allow-cleartext` is set — you have explicitly
  opted out of transport security.
- Vulnerabilities in upstream Deephaven workers or the MCP client itself.

## Authentication

### Community server

- A single PSK controls who is allowed to connect to the MCP server.
  Configured via `auth.psk` (inline) or `auth.psk_env_var` (env-var
  indirection — preferred).
- Every request must carry the PSK in the `X-Deephaven-PSK` header.
- `auth.enabled: false` is permitted **only** on loopback binds; the server
  refuses to start on a non-loopback host with auth disabled.

### Enterprise server

- The server holds **no** user credentials. Every MCP request must carry
  the caller's own Deephaven credentials in HTTP headers:

  | Header | Backend | Required |
  |---|---|---|
  | `X-Deephaven-Username` | both | yes |
  | `X-Deephaven-Password` | password | yes (password) |
  | `X-Deephaven-Private-Key` | private_key | yes (private_key) |
  | `X-Deephaven-Effective-User` | password | only when `allow_effective_user: true` |

- The config file declares which backends (`password`, `private_key`) are
  accepted. Set `allow_effective_user: true` only for trusted operator
  clients that legitimately act as other users.

## Transport security

The community and enterprise servers refuse to start on a non-loopback host
without one of the four mechanisms below, and reject cleartext non-loopback
requests at runtime with HTTP `426 Upgrade Required`.

| Pattern | When to use |
|---|---|
| **Loopback** (default) | Local development; single-host deployments. |
| **Native TLS** (`--ssl-keyfile` + `--ssl-certfile`) | Server terminates TLS itself. Simplest secure option. |
| **Reverse proxy** (`--trust-forwarded-proto` + `--forwarded-allow-ips`) | Proxy (nginx, Envoy, Cloud Run, ALB) terminates TLS; server trusts `X-Forwarded-Proto: https` only from peers in the allowlist. |
| **`--allow-cleartext`** | Trusted private networks only (LAN, air-gapped). Logs a loud warning and a per-request reminder. |

Every server (community, enterprise, docs) exposes a `/health` endpoint that
returns `200 OK` with `{"status": "ok"}`. On the systems servers (community
and enterprise), `/health` bypasses **both** the TLS-enforcement layer and
the authentication layer, so liveness/readiness probes from arbitrary peers
succeed over cleartext with no credentials. The docs server has no
TLS-enforcement or authentication layer and serves `/health` directly.

For the full decision matrix, deployment commands, and CLI/env-var reference,
see [`DEVELOPER_GUIDE.md#transport-security-tls`](DEVELOPER_GUIDE.md#transport-security-tls)
and [`ENV.md#transport-security-variables`](ENV.md#transport-security-variables).

## Secret handling

- **Env-var indirection.** Use `auth.psk_env_var` (community gate),
  `sessions[*].auth_token_env_var` (per-worker token), or any of the
  documented `*_env_var` fields instead of inlining secrets.
- **Log redaction.** `auth_token` values are redacted when truthy.
  Binary TLS key material (`tls_root_certs`, `client_cert_chain`,
  `client_private_key`) is redacted when stored as `bytes` /
  `bytearray`; string paths are not redacted (they're filesystem
  references, not secrets).
- **Docs server.** `INKEEP_API_KEY` is required and read once at startup;
  the value never appears in MCP tool responses.

## Further reading

- [`docs/ENV.md`](ENV.md) — full reference for every environment variable
  the servers respect.
- [`docs/DEVELOPER_GUIDE.md#transport-security-tls`](DEVELOPER_GUIDE.md#transport-security-tls)
  — decision matrix and concrete deployment patterns.
- [`docs/DEVELOPER_GUIDE.md#enterprise-auth-model`](DEVELOPER_GUIDE.md#enterprise-auth-model)
  — auth-backend internals and credential lifecycle.
- Root [`SECURITY.md`](../SECURITY.md) — vulnerability disclosure policy.
