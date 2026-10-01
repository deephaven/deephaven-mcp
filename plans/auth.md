# `dh auth`

Sign in to Deephaven Enterprise (DHE) and Community (DHC) servers. Each sign-in
is saved as a profile, and one profile is the default.

## Commands

| Command                           | Behavior                                                                                                                            |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `dh auth`                         | List profiles. Mark the default.                                                                                                    |
| `dh auth login [server]`          | Sign in. Save the profile. First profile → default; otherwise prompt (§ Login).                                                     |
| `dh auth use [profile]`           | Set the default. Picker if no profile is given.                                                                                     |
| `dh auth rename <profile> <name>` | Rename a profile.                                                                                                                   |
| `dh auth logout [profile]`        | Revoke the key on the server if `dh` generated it, and remove the profile. Picker if no profile is given and there are 2+ profiles. |
| `dh auth logout --all`            | Same, for every profile.                                                                                                            |
| `dh auth status`                  | Check the profile against its server.                                                                                               |
| `dh auth import [--from <path>]`  | Import legacy config.                                                                                                               |

Global flags: `--profile <name>` (env `DH_PROFILE`), `-o json`, `--no-input`.
Env: `DH_TIMEOUT` (connect + non-SSO login, default 30 s →
`server_unreachable`).

## Login

```text
$ dh auth login
? What server do you want to log in to?
❯ https://dhe.example.com:8123   (Enterprise · dhcli "prod")
  Other…

? How do you want to sign in?
❯ Google (SSO)
  Username and password

Opening browser. If it doesn't open:
  https://dhe.example.com:9032/dh-saml/?key=…
Waiting for sign-in… (Ctrl-C to cancel)
✔ Signed in as user-a@example.com
✔ Authorized this computer (key "dh CLI · my-laptop · 2026-10-01")

Logged in. Default profile: dhe.example.com:user-a@example.com
Run `dh --help` to learn more.
```

- Server choices: imported config, previous logins, and **Other…**.
- The method prompt only appears if the server offers 2+ methods.
- SSO wait: `--timeout` (default 300 s). Not bounded by `DH_TIMEOUT`.
- DHE: after SSO or password login, upload a key. Later logins use the key. The
  password is not saved.
- DHE rejects the key upload → `key_upload_denied`.
- DHC methods: anonymous, PSK, username/password, custom handler.
- The profile is saved only after a successful login.
- Default: first profile → default. Otherwise `Make this the default? (y/N)`.
  `--default` / `--no-default` skip the prompt. No TTY and no flag → not
  default.

Already logged in:

```text
$ dh auth login
  ● dhe.example.com:user-a   (default)
    dhe-qa.example.com:user-a
? What would you like to do?
❯ Log in to another server
  Log in as a different user
  Re-authenticate a profile
  Remove a profile
```

### Server address

- Input: a host, a URL, or a URL copied from the browser.
- No scheme → `https://` (`http://` for localhost). Drop the path.
- No port → try, in order, and use the first Deephaven server that answers:
  - `https`: 443, 8000, 8123.
  - `http` (localhost or explicit `http://`): 10000, 8000, 8123, 80.
- No server found → `server_unreachable`, listing the addresses tried.
- Never downgrade `https` to `http` for a non-local host.
- `--ca-cert <file>`: trust this CA for the server. The path is saved with the
  server.
- `HTTPS_PROXY` / `HTTP_PROXY` / `NO_PROXY` apply to all traffic.

## Profiles

- Profile = server + user, plus an optional operate-as user.
- Default name: `<host>:<user>`, or `<host>:<user>/<operateAs>`. Add the port if
  two names collide. Names can be renamed.
- Profile used by a command: `--profile` → `DH_PROFILE` → `DH_SERVER` (§
  Environment-only auth) → default. None of them → `auth_required`.
- Re-login replaces a `dh`-generated key and deletes the old one on the server.
- Only `dh`-generated keys are revoked or deleted. `--private-key-file` keys are
  never touched on the server.
- Logging out of the default makes another profile the default: one on the same
  server first, otherwise the first remaining.
- Logout: if revoking the key fails, warn and still remove the profile locally.

| Method                   | Stored                                                                   |
| ------------------------ | ------------------------------------------------------------------------ |
| DHE SSO / password       | Generated key pair                                                       |
| DHE `--private-key-file` | The path (read on each login), or the key with `--copy-key`              |
| DHC anonymous            | Nothing                                                                  |
| DHC PSK                  | The PSK, or an env var name (`--psk-env`)                                |
| DHC username/password    | The password (warning shown once), or an env var name (`--password-env`) |
| DHC custom               | Handler and token, or an env var name (`--token-env`)                    |

- Directory: `~/.deephaven/cli/` (`%APPDATA%\Deephaven\cli\` on Windows).
  Override with `DH_CONFIG_DIR`.
- `config.json`: servers and profiles.
- `credentials.json`: secrets, owner-only (mode 0600; Windows: owner-only ACL).
  Refused if others can read it.

## Errors

| Code                     | Cause                                 | Hint                             |
| ------------------------ | ------------------------------------- | -------------------------------- |
| `auth_required`          | No profile                            | `dh auth login`                  |
| `auth_failed`            | Server rejected credentials at login  | Check the credentials            |
| `auth_expired`           | Server rejected the stored credential | `dh auth login --profile <name>` |
| `credential_unavailable` | Profile's env var or key file missing | The var or path                  |
| `key_upload_denied`      | DHE rejected the key upload           | Ask an administrator             |
| `server_unreachable`     | Network or TLS failure                | The URL and the cause            |

Exit codes: 0 ok, 1 internal error, 2 usage, 3 cancelled, 4 auth, 5 server. With
`-o json`, errors go to stderr as `{"error", "code", "exit"}`.

## Non-interactive use

- No TTY, or `--no-input`: never prompt. Missing input → usage error naming the
  flag. Exception: a server whose only method is anonymous logs in without
  flags.
- Secrets only from stdin or an env var, never from argv.

| `dh auth login` flag                                                  | Purpose                                         |
| --------------------------------------------------------------------- | ----------------------------------------------- |
| `--method saml\|password\|private-key\|psk\|basic\|anonymous\|custom` | Sign-in method                                  |
| `--username <u>`                                                      | Username                                        |
| `--password-stdin`, `--password-env <VAR>`                            | Password                                        |
| `--psk-stdin`, `--psk-env <VAR>`                                      | PSK                                             |
| `--handler <class>`, `--token-stdin`, `--token-env <VAR>`             | DHC custom handler and token                    |
| `--private-key-file <path>`, `--copy-key`                             | Existing DHE key: store the path / the contents |
| `--ca-cert <file>`                                                    | CA cert for the server                          |
| `--timeout <seconds>`                                                 | SSO wait (default 300)                          |
| `--operate-as <u>`                                                    | DHE operate-as user                             |
| `--no-browser`                                                        | Print the SSO URL without opening a browser     |
| `--default`, `--no-default`                                           | Set / don't set as default; skip the prompt     |
| `--yes`                                                               | Accept confirmation prompts                     |

- Library logs only appear with `DH_DEBUG`.
- `dh` (no args): help and login status.
- `dh agents`: command tree, flags and error codes, as JSON.

### Environment-only auth

For CI and containers. Applies when `DH_SERVER` is the selected source (§
Profiles). Then only these variables are used, and the config directory is not
read or written.

| Var                   | Purpose                                 |
| --------------------- | --------------------------------------- |
| `DH_SERVER`           | Server address (§ Server address rules) |
| `DH_USERNAME`         | Username                                |
| `DH_PASSWORD`         | Password (DHE or DHC)                   |
| `DH_PSK`              | DHC PSK                                 |
| `DH_PRIVATE_KEY_FILE` | DHE key file                            |
| `DH_AUTH_HANDLER`     | DHC custom handler class                |
| `DH_AUTH_TOKEN`       | DHC custom token                        |
| `DH_OPERATE_AS`       | DHE operate-as user                     |
| `DH_CA_CERT`          | CA cert file                            |

- Method: from the secret var that is set. None set → anonymous.
- More than one secret var set → usage error.
- DHE password: logs in with the password on every command. No key is uploaded.

## Legacy config import

- Runs on the first `dh auth login` with no profiles (offered once), or on
  `dh auth import`.
- Sources: `$DH_MCP_CONFIG_FILE` (v1); `$DH_AI_DATA_DIR/config` or
  `~/.deephaven/ai/config` (v2).
- The prompt lists each item and the action for it. Items can be deselected.
  `--yes` imports everything.
- Legacy files are not modified.

| Legacy                                              | Result                                                                                                                                                                                     |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| DHE `connection_json_url`                           | Server (drop `/iris/connection.json`)                                                                                                                                                      |
| DHE `password` (+ `effective_user`)                 | Log in once, generate a key. `effective_user` → operate-as.                                                                                                                                |
| DHE `private_key` (`key_text` / `private_key_path`) | Key file lines `user`/`operateas`/`public`/`private`. `EC:` prefix → strip it and use the key. No prefix (DSA) → needs re-login. `private_key_path` → path reference; `key_text` → copied. |
| DHC `anonymous` / v1 `Anonymous`                    | Anonymous                                                                                                                                                                                  |
| DHC `psk` / v1 `PSK`                                | PSK. `${env:VAR}` and `auth_token_env_var` → env var reference.                                                                                                                            |
| DHC `password` / v1 `Basic user:pass`               | Username/password                                                                                                                                                                          |
| DHC `custom` / v1 Java class                        | Custom handler and token                                                                                                                                                                   |
| `${file:/path}`                                     | The file's contents                                                                                                                                                                        |
| v2 `context.json` default system                    | Default profile                                                                                                                                                                            |
| Session creation, Docker, timeouts                  | Not imported                                                                                                                                                                               |

## Implementation

| Path                 | Contents                                              |
| -------------------- | ----------------------------------------------------- |
| `src/auth/`          | Address resolution, store, DHE/DHC login, SSO, import |
| `src/commands/auth/` | Cliffy commands                                       |
| `src/ui/`            | Ink prompts (TTY only, lazy-loaded)                   |

Dependencies: `@deephaven-enterprise/auth-nodejs`,
`@deephaven-enterprise/jsapi-nodejs`, `@deephaven/jsapi-nodejs`, `ink`.

- SSO:
  - The login URL comes from the server's auth config. It may be on another
    port.
  - Nonce: ≥ 96 CSPRNG bytes.
  - One `login({type: "saml"})` call. The server blocks until the browser
    sign-in completes.
- Key upload:
  - Target: the ACL writer host from `getServerConfigValues()`.
  - Check the HTTP status.
  - Log in with the new key before saving it.
- Key label: `dh CLI · <hostname> · <date>`.
- jsapi:
  - Load it as ESM.
  - Load it only from the resolved origin, over TLS or loopback.
  - Cache it per origin.
  - Runs with `dh`'s permissions. Logging in to a server trusts it to run code,
    as the web UI does.
- Login rejections aren't `Error`s. Map any rejection to an auth code.
- After a failed login, discard the client. `disconnect()` throws.
- `credentials.json`: atomic writes, in an owner-only directory (0700; Windows:
  owner-only ACL).
- `config.json` and `credentials.json`: read-modify-write under one lock
  (`config.lock`).
- CA cert and proxy: apply to address probing, jsapi download, and the jsapi
  transport (`fetch` via `Deno.createHttpClient`; node `http2`/`ws` options for
  the transport).
- Timeouts: race connect + non-SSO login against `DH_TIMEOUT`, and the SSO wait
  against `--timeout`. On timeout, discard the client.
- Command name: `"bin"` in `deno.json`, the only place it is set.
- Build: add `--allow-run` and `--allow-sys=hostname`.

## Out of scope

- OIDC and device-code login.
- TLS client certificates.
- Managing other users' keys.
- Inbound auth for an MCP HTTP server (old `server.json` PSK).
- An SSO "close this tab" page (follow-up).
- An OS keychain backend (follow-up).
- `dh auth token`: print a DHC credential for scripts and agents (follow-up).
- `dh auth keys`: list and revoke your own `dh CLI` keys from other machines
  (follow-up).
- Reusing a session across commands. Each command logs in (follow-up).

## Done when

- [ ] `dh auth login`, then any server command, works on all 5 release targets.
- [ ] Ink prompts work on all 5 targets.
- [ ] No prompt without a TTY. Every prompt has a flag.
- [ ] No DHE password is stored. No secret appears in output.
- [ ] `logout` revokes `dh`-generated keys only.
- [ ] The v1 and v2 fixtures from `main` import.
- [ ] CI: DHC anonymous and PSK against a real server.
- [ ] DHE test server: SSO, password, key login and revocation.
- [ ] `--ca-cert` and proxy env vars apply to the address check, the jsapi
      download and the connection.
- [ ] Environment-only auth works with no config directory.
- [ ] Concurrent `dh` processes don't lose config writes.
