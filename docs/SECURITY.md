# Deephaven MCP Security Guide

**Read this once before deploying.** The defaults are safe for local
development; non-loopback deployments require a few explicit
operator decisions. This page is the single source of truth for the
runtime security model. For the project's vulnerability-disclosure
policy, see the root [`SECURITY.md`](../SECURITY.md).

## Table of Contents

- [Am I in scope for this guide?](#am-i-in-scope-for-this-guide)
- [Trust model](#trust-model)
- [The `dhcli` daemon](#the-dhcli-daemon)
- [Hardening checklist](#hardening-checklist)
- [Threat model](#threat-model)
- [Authentication](#authentication)
- [Transport security](#transport-security)
- [Secret handling](#secret-handling)
- [Rotation](#rotation)
- [Docs server](#docs-server)
- [Further reading](#further-reading)

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

If instead you deploy `dh-mcp-docs-server` (the public
documentation Q&A service), skip to [Docs server](#docs-server) —
it has a deliberately different, unauthenticated posture.

If you use the `dhcli` CLI, it starts a local server on your behalf.
That is a loopback-only surface with its own secret on disk; see
[The `dhcli` daemon](#the-dhcli-daemon).

## Trust model

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
are configured in the JSON tree under the resolved configuration
directory (see [`docs/ENV.md`](ENV.md) for the resolution order)
and resolved once at startup. Restart to rotate.

## The `dhcli` daemon

The `dhcli` CLI runs its commands against a **per-user background
daemon** it starts automatically — `dh-mcp-systems-server --daemon`.
This is the same HTTP transport described above, pinned to a safe
shape you do not configure:

- **Loopback only, ephemeral port.** The daemon binds `127.0.0.1` on a
  kernel-chosen port. Combining `--daemon` with `--transport`,
  `--host`, or `--port` is rejected.
- **A fresh PSK per startup**, generated with
  `secrets.token_urlsafe(32)`. `server.json`'s `psk` is *ignored* in
  daemon mode, so the daemon never shares your HTTP-transport key.
- **The PSK is stored in plaintext** in the registry file
  `<runtime_dir>/daemon/daemon.json`, because the CLI must read it back
  to authenticate. The file is written owner-only (`0o600`) via an
  atomic replace, and the directory is created owner-only (`0o700`)
  before the first write.

What this means for you:

- **Anyone who can read `daemon.json` can drive your daemon**, and
  through it every Deephaven session and system your configuration
  declares. Keep `<runtime_dir>` private; the defaults already are.
- **Do not relocate the runtime directory to a shared path.** A
  world-readable or group-readable `--runtime-dir` (or
  `DH_AI_DATA_DIR`) discloses the PSK. Unlike the configuration
  directory, the runtime directory has no startup permission audit.
- **Stop the daemon when you are done** on a shared or long-lived host:
  `dhcli daemon stop` terminates it and removes the registry file. It
  also exits on its own after an idle window
  (`server.json`'s `daemon.idle_shutdown_seconds`).
- **`daemon.json` is safe to share only after redaction.** `dhcli
  daemon status` prints the same entry with the PSK masked; prefer it
  when pasting diagnostics into a bug report.

## Hardening checklist

Walk through these items before exposing the HTTP transport to
anything beyond the local process group.

- [ ] **Use stdio if you can.** stdio has no network surface and is
      the recommended transport for local-IDE integrations.
- [ ] **Configure a PSK of at least 16 characters** for HTTP. Set
      `psk` in `server.json` (or pass `--psk`); the server refuses
      to start the HTTP transport without one, and rejects any PSK
      shorter than 16 characters (including the empty string) so the
      gate is not brute-forceable. Use `${env:NAME}` templating to
      source it from an environment variable.
- [ ] **Bind to loopback only.** Default `host` is `127.0.0.1`; do
      not change it. Reach the server from another host through a
      reverse proxy on the same host.
- [ ] **Terminate TLS upstream.** Run nginx / Caddy / Envoy / a
      cloud load balancer on the same host and forward to
      `127.0.0.1:<port>`.
- [ ] **Use `${env:NAME}` or `${file:PATH}` for every secret** in
      the config tree. Never commit literal secrets.
- [ ] **Lock down configuration-directory permissions.** POSIX:
      `chmod 700` the directory, `chmod 600` every file inside.
      Windows: place the directory under the current user profile
      (`%APPDATA%/Deephaven/ai/config/` satisfies this). The startup
      audit aborts otherwise.
- [ ] **Set `security.credential_retrieval_mode` to `"none"`** if AI
      agents must never read community session tokens. The default,
      `"dynamic_only"`, returns the token for any session this server
      launched at runtime — the check is the session's origin, not which
      client created it, so one agent can read the token of a session
      another agent created. `"static_only"` and `"all"` additionally
      hand operator-authored credentials to the caller of the
      `session_community_credentials` MCP tool.
- [ ] **Keep the runtime directory private.** The `dhcli` daemon's
      registry (`<runtime_dir>/daemon/daemon.json`) holds a live PSK in
      plaintext. The defaults are owner-only; do not point
      `--runtime-dir` / `DH_AI_DATA_DIR` at a shared location, and note
      that no startup audit checks this directory.
- [ ] **Rotate** any PSK or outbound credential that has appeared in
      a log file, shell history, or version-control commit. PSK
      rotation requires a server restart.

## Threat model

**Protects against (when configured per the checklist):**

- **Unauthorized HTTP access.** Every non-`/health` request without
  a valid `X-Deephaven-PSK` header is rejected `401 Unauthorized`
  (compared with `hmac.compare_digest`).
- **Network exposure.** The HTTP server refuses to bind any
  non-loopback host, so cleartext MCP traffic never leaves the
  kernel without operator action.
- **Credential leakage via logs.** Every secret-bearing field is a
  `pydantic.SecretStr`; the default `repr` masks the value, and
  configuration dumps emit `[REDACTED]` for every secret field.
- **Tampered or world-readable config tree.** The startup permission
  audit refuses to start the server if any file or subdirectory
  under the resolved configuration directory is owned by another
  UID or accessible to anyone other than the running user.

**Does NOT protect against:**

- A compromised host or insider access to environment variables,
  the configuration directory, or process memory.
- Cleartext PSK and MCP traffic on the loopback interface — provide
  confidentiality with a TLS-terminating proxy on the same host.
- Inline secrets committed to a config file. Operators must use
  `${env:NAME}` / `${file:PATH}` indirection.
- Vulnerabilities in upstream Deephaven workers or in MCP clients.

## Authentication

### Inbound (clients calling the systems server)

- **stdio transport:** no authentication. The OS process boundary
  is the trust boundary.
- **HTTP transport:** every request must carry the configured PSK in
  the `X-Deephaven-PSK` header. Comparison uses
  `hmac.compare_digest`. PSKs shorter than 16 characters (including
  the empty string) are refused at startup. The `/health` endpoint
  is exempt so external liveness probes work without sharing the
  PSK.

A failed gate produces an HTTP 401 with this body:

```json
{
  "error": "Unauthorized",
  "code": "psk_missing",
  "detail": "Authentication required: ..."
}
```

`code` is `psk_missing` (header absent) or `psk_invalid` (header
present but value did not match). The response also carries
`WWW-Authenticate: Deephaven-PSK realm="mcp"`.

### Outbound (the systems server calling Deephaven workers)

The systems server holds outbound credentials for every configured
Community session and Enterprise system. Credentials are loaded
**once at startup** from the JSON config tree and reused for the
process lifetime; there is no per-request credential resolution.

Credentials are typed `pydantic` models discriminated by
`auth.credentials.type`:

| Kind | Required fields | Notes |
| --- | --- | --- |
| `anonymous` | — | No bearer material; community-only. |
| `psk` | `token` | Pre-shared key for Deephaven Community PSK auth; community-only. |
| `password` | `username`, `password` | Optional `effective_user` for Enterprise sudo-style delegation. |
| `private_key` | `key_text` | PEM contents (use `${file:/path/to/key.pem}` to load from disk). |
| `custom` | `auth_type`, `auth_token` | Escape hatch for arbitrary Java auth handler classes; community-only. |

Enterprise systems accept only `password` and `private_key`. The
full schema lives in [`docs/CONFIGURATION.md`](CONFIGURATION.md).

## Transport security

The HTTP transport is **cleartext**. The systems server does not
perform TLS termination; choose one of:

| Pattern | When to use |
| --- | --- |
| **Loopback only** (default) | Local development; same-host tooling already inside the trust boundary. |
| **TLS-terminating reverse proxy** | Production. nginx / Caddy / Envoy / cloud LB on the same host terminates TLS and forwards to `127.0.0.1:<port>`. The proxy verifies its peer; the server trusts everything that arrives on the loopback port (subject to the PSK gate). |
| **mcp-proxy / IDE bridge** | When an IDE-side MCP client speaks stdio but the server runs over HTTP, run an stdio-↔-HTTP bridge on the client host and forward the PSK in `X-Deephaven-PSK`. |

Whichever reverse proxy you choose, ensure it forwards the client's
`X-Deephaven-PSK` header through to the loopback port — the server still
applies the PSK gate to proxied requests.

Outbound TLS for Community sessions is configured per session under
the optional `tls` block on each `community/sessions/<name>.json`
file. Presence of the block (even `"tls": {}`) enables TLS; the
`client_certificate` sub-block enables mTLS. PEM text is read by the
templating engine via `${file:/path/to/file.pem}`. Enterprise systems
do **not** accept a `tls` block — the upstream Enterprise
`SessionManager` fetches its truststore via the connection.json's
`truststore_url`.

## Secret handling

- **Templating, not shadow fields.** Every secret-bearing field
  accepts `"${env:NAME}"` / `"${env:NAME:-default}"` / `"${file:PATH}"`
  inside its string value. The legacy `<field>_env_var` /
  `<field>_path` shadow fields have been removed.
- **Resolution order.** The templating engine resolves placeholders
  *before* Pydantic validation, so the validated model carries only
  literal, fully-resolved values. Failures (missing env var,
  unreadable file, malformed placeholder) surface as
  `ConfigurationError` at startup, naming the source file and JSON
  path of the offending value.
- **In-memory representation.** Resolved secrets live in
  `pydantic.SecretStr` fields on `RedactableSchema` subclasses. The
  default `repr` masks the value; logs that dump configuration use
  `model_dump(context={"redact": True})` so every secret renders as
  `[REDACTED]`. Code that needs the plaintext calls
  `.get_secret_value()` explicitly.
- **Filesystem permissions.** `verify_config_directory_permissions`
  runs at startup and aborts with a single aggregated error message
  listing every offending path. POSIX-strict
  (`stat & 0o077 == 0`, owner-must-match-UID); Windows best-effort
  (configuration directory must be under the current user profile).
  This audit covers the *configuration* directory only — the runtime
  directory is created owner-only but is not re-audited on startup.
- **Community session tokens.** `session_community_credentials`, and
  the `dhcli session credentials` / `session url` / `session open`
  verbs that wrap it, return a **plaintext auth token** and a URL
  containing it. All are gated by
  `security.credential_retrieval_mode` (default `dynamic_only`, which
  covers every dynamically launched session — whichever client created
  it — but withholds operator-authored static credentials); every
  retrieval is logged. `session open` is the exception that does not
  print the token: it hands the authenticated URL to the browser and
  reports the token-free one, on success and on launch failure alike,
  unless `--reveal-secrets` is passed. Treat the output like a
  password: a session URL pasted into a chat log grants access to that
  session.
- **Inspecting configuration.** Both read verbs redact secret values by
  default; `dhcli config get --reveal-secrets` is the only way to print
  one, and it warns on stderr naming how many it disclosed. A field
  holding only a `${env:NAME}` / `${file:PATH}` reference is shown as
  written, so a whole-value `[REDACTED]` means the field holds an
  **on-disk value** rather than a bare reference — usually a literal
  secret, though redaction fails closed, so an invalid value (a number,
  `null`) at a secret field is replaced too. A reference with a fallback keeps its variable
  name but loses the literal (`${env:NAME:-[REDACTED]}`), since the
  fallback is itself a stored secret. Two gaps: `dhcli config edit` opens the file
  verbatim, and redaction is schema-guided, so a secret placed in a
  free-form map (`environment_vars`, `session_arguments`) is not
  recognized as one.
- **`daemon.log` holds auto-generated session tokens.** A session
  created without an explicit auth token gets one minted by the server
  and logged with a ready-to-use `?psk=<token>` URL. `dhcli daemon logs`
  prints it unredacted, so review that output before sharing it; the
  file is protected only by the `0700` daemon directory around it. The
  log is not the only route to that token — under the default
  `dynamic_only` mode, `session_community_credentials` and its CLI
  wrappers return it too.

## Rotation

- **PSK rotation:** update `psk` in `server.json` (or the env var
  it references) and **restart the server**. There is no
  `mcp_reload` tool; configuration is read once at startup.
- **Outbound credential rotation:** update the `${env:NAME}` source
  or the `${file:PATH}` target and **restart the server**. In-memory
  credentials are not re-resolved at runtime.
- **`dhcli` daemon PSK rotation:** automatic. Every daemon start
  generates a fresh key, so `dhcli daemon restart` rotates it. Run it
  if you believe `daemon.json` was exposed.

## Docs server

Everything above describes `dh-mcp-systems-server`. The second
binary, `dh-mcp-docs-server`, has a deliberately different posture:
it is an **intentionally public, unauthenticated** documentation
Q&A service.

- **No PSK gate, by design.** The docs server exposes a single
  read-only tool (`docs_chat`) that forwards questions to an
  upstream LLM documentation API. It carries no inbound secrets and
  hosts no Deephaven session, so there is no PSK middleware. The
  shipped container image (`ops/docker/mcp-docs/Dockerfile`) binds
  `0.0.0.0` on purpose so it can run behind a public ingress
  (e.g. Cloud Run).
- **Abuse control belongs to the fronting deployment.** Because
  `docs_chat` consumes a metered upstream API key
  (`INKEEP_API_KEY`), rate limiting and request shaping are the
  responsibility of the deployment in front of the server (ingress,
  API gateway, or load balancer). The server does not rate-limit
  itself.
- **The only secret is outbound.** `INKEEP_API_KEY` is read from the
  environment and used solely for the upstream call; it is never
  returned to clients. Keep it in the deployment's secret store, not
  in an image layer.

If you run the docs server only on loopback for local development,
none of this applies — treat it like any other local process.

## Further reading

- [`docs/CONFIGURATION.md`](CONFIGURATION.md) — complete configuration
  schema reference.
- [`docs/ENV.md`](ENV.md) — environment variables the server
  processes themselves consume.
- [`docs/DEVELOPER_GUIDE.md`](DEVELOPER_GUIDE.md) — building, running,
  and integrating the systems server.
- Root [`SECURITY.md`](../SECURITY.md) — vulnerability-disclosure
  policy.
