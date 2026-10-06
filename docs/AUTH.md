# Signing in with `dh auth`

`dh auth login` signs you in to a Deephaven Enterprise or Community server and
saves the result as a profile. Other `dh` commands sign in with the default
profile, or with the one you name.

```sh
dh auth login dhe.example.com      # Enterprise: SSO in the browser, once
dh auth login localhost:10000      # Community: whatever the server offers
dh auth                            # list profiles; ● marks the default
```

## Commands

| Command                           | What it does                                                                      |
| --------------------------------- | --------------------------------------------------------------------------------- |
| `dh auth`                         | List profiles                                                                     |
| `dh auth login [server]`          | Sign in and save a profile                                                        |
| `dh auth status`                  | Sign in with the selected profile and report who you are                          |
| `dh auth use [profile]`           | Set the default profile                                                           |
| `dh auth rename <profile> <name>` | Rename a profile                                                                  |
| `dh auth logout [profile]`        | Delete this computer's key on the server, if `dh` made it, and remove the profile |
| `dh auth logout --all`            | The same for every profile                                                        |
| `dh auth import [--from <path>]`  | Import servers from an old dhcli / Deephaven MCP config                           |

Global flags, accepted by every command:

| Flag               | Effect                                                     |
| ------------------ | ---------------------------------------------------------- |
| `--profile <name>` | Use this profile instead of the default (env `DH_PROFILE`) |
| `-o json`          | Print results as JSON ([JSON output](#json-output))        |
| `--no-input`       | Never prompt; fail with the flag to pass instead           |

## Server addresses

`dh auth login` accepts a host name, `host:port`, or a URL copied from the
browser. It ignores the path.

| You type                            | `dh` tries                          |
| ----------------------------------- | ----------------------------------- |
| `dhe.example.com`, `https://…`      | https on ports 443, 8000, 8123      |
| `localhost`, `http://…`             | http on ports 10000, 8000, 8123, 80 |
| a port, e.g. `dhe.example.com:8123` | that port only                      |

It uses the first address in the list that answers as a Deephaven server, and
reports whether that is Enterprise or Community. An `https` address never falls
back to `http`.

If nothing answers, the error lists every address tried. Check the address, your
VPN, and whether the server is up.

## Enterprise

| Method            | Flags                                      | What `dh` saves                                      |
| ----------------- | ------------------------------------------ | ---------------------------------------------------- |
| SSO (SAML)        | `--method saml`                            | A key pair it generates and uploads                  |
| Username/password | `--method password --username <u>`         | A key pair it generates and uploads                  |
| Your own key file | `--private-key-file <path>` [`--copy-key`] | The file's path, or the key itself with `--copy-key` |

`dh` never saves an Enterprise password. After an SSO or password sign-in it
generates a key pair, uploads the public key to the server for your user, and
checks that the key works. Later commands sign in with that key, without a
browser or password.

The uploaded key's comment is `dh CLI - <hostname> - <date>`, so administrators
can tell which computer it belongs to. It stays valid until `dh auth logout` or
an administrator deletes it ([Logging out](#logging-out)).

If the upload fails, login fails with `key_upload_failed`:

- `Could not reach <host>:<port> …`: the server sends key uploads to its ACL
  write server at that address, and this computer can't reach it. Connect to the
  network it's on (for example, the VPN).
- `… rejected this computer's key (HTTP 400)`: the hint shows the server's
  reason.
- HTTP 401: the server didn't accept the session for key uploads. This is a
  server configuration problem.

While uploads fail, a password account can still sign in with
[environment variables](#environment-only-sign-in), which uploads nothing. An
existing key file works with `--private-key-file`.

`--operate-as <user>` signs in as you and operates as another user. Each
operate-as user gets its own profile.

### SSO sign-in

1. `dh` opens the server's sign-in page in your browser and prints the URL. With
   `--no-browser`, it only prints it.
2. Sign in with your identity provider. `dh` waits up to `--timeout` seconds
   (default 300).
3. `dh` confirms the account:
   - If the server provides a confirmation page, you confirm in the browser.
   - Otherwise the terminal asks `Use alice@example.com? (Y/n)`. Answer `n` and
     nothing is saved.
   - `--expect-user <user>` checks the account without asking. A different
     account fails with `auth_failed` and nothing is saved.

Browsers often sign you in again with whatever account they already have a
session for. To use a different account, pick it in your identity provider's
account chooser, or sign out of the identity provider first.

## Community

`dh` asks the server which sign-in methods it accepts. If it accepts only one,
`dh` uses it without asking.

| Method            | Flags                                                                          | What `dh` saves                      |
| ----------------- | ------------------------------------------------------------------------------ | ------------------------------------ |
| Anonymous         | `--method anonymous`                                                           | Nothing                              |
| Pre-shared key    | `--method psk` + `--psk-stdin` or `--psk-env <VAR>`                            | The key, or the variable's name      |
| Username/password | `--method basic --username <u>` + `--password-stdin` or `--password-env <VAR>` | The password, or the variable's name |
| Custom handler    | `--method custom --handler <class>` + `--token-stdin` or `--token-env <VAR>`   | The token, or the variable's name    |

Secrets are never accepted as command-line arguments. Pass them on stdin, name
an environment variable, or type them at the prompt.

- `--*-env <VAR>` saves only the variable's name. `dh` reads the variable each
  time the profile is used, so it must be set then too.
- A secret typed at the prompt or read from stdin is saved in
  `credentials.json`, which only you can read.

```sh
dh auth login localhost:10000 --method psk --psk-env DH_PSK
printf '%s' "$PSK" | dh auth login localhost:10000 --method psk --psk-stdin
```

## Profiles

A profile is one combination of server, user and operate-as user. Signing in
again as the same combination updates that profile instead of adding a new one.

- **Name:** `<host>:<user>` by default, e.g. `dhe.example.com:alice`. Rename it
  with `dh auth rename`.
- **Default:** your first profile becomes the default. Later logins ask
  `Make this the default? (y/N)`. `--default` or `--no-default` answers without
  asking. Without a terminal, the new profile isn't made the default.
- **Which profile a command uses:** `--profile`, then `DH_PROFILE`, then
  `DH_SERVER` ([Environment-only sign-in](#environment-only-sign-in)), then the
  default.
- **Several accounts on one server:** sign in once per account. Each gets its
  own profile, e.g. `dhe.example.com:alice` and `dhe.example.com:bob`. Switch
  with `dh auth use` or `--profile`.

In a terminal, `dh auth login` with no server and existing profiles offers a
menu: log in to another server, log in as a different user, sign in again with a
profile, or remove one.

### Where profiles are stored

| Path                                | Contents                               |
| ----------------------------------- | -------------------------------------- |
| `~/.deephaven/cli/config.json`      | Servers and profiles. No secrets       |
| `~/.deephaven/cli/credentials.json` | Keys and secrets. Readable only by you |

On Windows the directory is `%APPDATA%\Deephaven\cli`. Set `DH_CONFIG_DIR` to
use another directory.

`dh` refuses to read `credentials.json` if other users can read it. Fix the
permissions with `chmod 600 ~/.deephaven/cli/credentials.json`.

## Logging out

`dh auth logout <profile>` removes the profile from this computer. If `dh`
generated the profile's Enterprise key, it first deletes that key on the server.
It never deletes a key you supplied with `--private-key-file`.

If the server can't be reached, `dh` warns, removes the profile anyway, and the
key stays on the server. An administrator can list and delete keys with
`dhconfig acl publickey` (run as the Deephaven admin user, e.g.
`sudo -u irisadmin`). Keys made by `dh` have comments starting with `dh CLI -`.

Signing in again to an existing Enterprise profile replaces its key and deletes
the old one.

## Scripts, CI and agents

Without a terminal, or with `--no-input`, `dh` never prompts. When it needs an
answer, it fails with exit code 2 and names the flag to pass:

```text
$ dh auth login --no-input
✖ Input needed: pass <server>
  Prompts are off without a TTY or with --no-input.
```

| Prompt                          | Flag that answers it                                       |
| ------------------------------- | ---------------------------------------------------------- |
| Which server                    | the `server` argument                                      |
| Which method                    | `--method`                                                 |
| Username                        | `--username`                                               |
| Password, key, token            | `--password-*`, `--psk-*`, `--token-*`                     |
| `Use alice@example.com?` (SSO)  | `--expect-user alice@example.com`                          |
| `Make this the default?`        | `--default` / `--no-default` (without either: not default) |
| Import an old config?           | `--yes` (without it: skipped)                              |
| Which profile (`use`, `logout`) | the `profile` argument, or `logout --all`                  |

`dh agents` prints every command, flag and error code as JSON.

### JSON output

With `-o json`, stdout holds exactly one JSON value. Progress messages and
errors go to stderr.

| Command          | stdout                                                       |
| ---------------- | ------------------------------------------------------------ |
| `dh auth`        | `[{"name", "server", "kind", "user", "method", "default"}]`  |
| `dh auth login`  | `{"profile", "server", "user", "default"}`                   |
| `dh auth status` | `{"profile", "server", "kind", "user", "operateAs"}`         |
| `dh auth use`    | `{"default"}`                                                |
| `dh auth rename` | `{"from", "to"}`                                             |
| `dh auth logout` | `{"removed": [{"profile", "server", "revoked"}], "default"}` |
| `dh auth import` | `{"imported"}`                                               |

`profile` is `null` in `status` when signed in from environment variables.

### Errors and exit codes

With `-o json`, an error is one line of JSON on stderr:

```json
{
  "error": "You're not logged in to a Deephaven server.",
  "code": "auth_required",
  "exit": 4,
  "hint": "Run `dh auth login` to get started."
}
```

| Exit | Code                     | Meaning                                                       | What to do                              |
| ---- | ------------------------ | ------------------------------------------------------------- | --------------------------------------- |
| 1    | `internal`               | Bug in `dh`                                                   | Rerun with `DH_DEBUG=1` and report it   |
| 2    | `usage`                  | Bad arguments, or input needed without a terminal             | Pass the flag named in the message      |
| 3    | `cancelled`              | You pressed Ctrl-C or declined a confirmation                 | —                                       |
| 4    | `auth_required`          | No profile                                                    | `dh auth login`                         |
| 4    | `auth_failed`            | The server rejected the credentials during login              | Check the credentials and try again     |
| 4    | `auth_expired`           | The server rejected a saved profile's credentials             | `dh auth login --profile <name>`        |
| 4    | `credential_unavailable` | The profile's env var or key file is missing                  | Set the variable or restore the file    |
| 4    | `key_upload_failed`      | Uploading this computer's key to the Enterprise server failed | See [Enterprise](#enterprise)           |
| 5    | `server_unreachable`     | Network or TLS failure, or no server at the address           | Check the address, VPN and certificates |
| 5    | `server_unsupported`     | Not a supported Deephaven server, or unsafe to load from      | Use `https://` or a local server        |

### Environment-only sign-in

For CI and containers: set `DH_SERVER` and the credentials, and `dh` signs in
from those without reading or writing the config directory.

```sh
DH_SERVER=localhost:10000 DH_PSK=… dh auth status -o json
```

| Variable              | Use                                                    |
| --------------------- | ------------------------------------------------------ |
| `DH_SERVER`           | Server address ([Server addresses](#server-addresses)) |
| `DH_USERNAME`         | Username (Enterprise, Community basic)                 |
| `DH_PASSWORD`         | Password                                               |
| `DH_PSK`              | Community pre-shared key                               |
| `DH_PRIVATE_KEY_FILE` | Enterprise key file                                    |
| `DH_AUTH_HANDLER`     | Community custom handler class                         |
| `DH_AUTH_TOKEN`       | Community custom token                                 |
| `DH_OPERATE_AS`       | Enterprise operate-as user                             |
| `DH_CA_CERT`          | CA certificate file                                    |

- The method follows from which secret is set. None set → anonymous.
- Setting more than one of `DH_PASSWORD`, `DH_PSK`, `DH_PRIVATE_KEY_FILE` and
  `DH_AUTH_TOKEN` is a usage error.
- An Enterprise password is used to sign in on every command.
- `--profile` or `DH_PROFILE` takes priority over `DH_SERVER`.

## Importing an old config

If you used the earlier dhcli / Deephaven MCP tools, `dh` can import their
servers. Your first `dh auth login` offers it, or run `dh auth import`.

`dh` looks for `$DH_MCP_CONFIG_FILE`, then `$DH_AI_DATA_DIR/config` or
`~/.deephaven/ai/config`. `--from <path>` points it at another file or
directory. The old files are not changed.

Importing signs in to each server to check its credentials, so **each server
must be reachable at the time** (for example, connect to the VPN first). `dh`
prints what it's waiting on for each server:

```text
  … prod: signing in to dhe.example.com:8123 as alice, then authorizing this computer (gives up after 30s if unreachable)
  ✔ prod → dhe.example.com:alice  authorized this computer
  ! staging: can't reach dhe-staging.example.com:8123 (fetch failed)
1 not imported. Fix the problems above (for example, connect to the servers' network), then run `dh auth import` to retry. Imported servers are skipped.
```

- An Enterprise password is used once to upload a key, then discarded.
- A secret written as `${env:VAR}` stays an environment-variable reference. If
  the variable isn't set, the profile is saved without being checked.
- Servers with a custom CA certificate are skipped. Sign in to them with
  `dh auth login <server> --ca-cert <file>`.
- Session creation, Docker and timeout settings aren't used by `dh`; they are
  listed as not imported.

## Certificates and proxies

- `--ca-cert <file>` trusts that CA for the server, and `dh` remembers it with
  the server. `DH_CA_CERT` does the same for environment-only sign-in.
- `HTTPS_PROXY` and `NO_PROXY` apply to finding the server and downloading its
  client library. The connection to the server itself doesn't use a proxy.

## Environment variables

| Variable                               | Effect                                                          |
| -------------------------------------- | --------------------------------------------------------------- |
| `DH_PROFILE`                           | Profile to use, like `--profile`                                |
| `DH_CONFIG_DIR`                        | Config directory (default `~/.deephaven/cli`)                   |
| `DH_TIMEOUT`                           | Seconds to wait for connecting and non-SSO sign-in (default 30) |
| `DH_DEBUG`                             | Show client library logs and internal error details             |
| `DH_SERVER`, …                         | Environment-only sign-in ([above](#environment-only-sign-in))   |
| `DH_MCP_CONFIG_FILE`, `DH_AI_DATA_DIR` | Where to look for an old config to import                       |

## Troubleshooting

| Message                                                       | Fix                                                                                        |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `No Deephaven server found at …`                              | Check the address and VPN. Add the port if the server uses a non-standard one              |
| `Could not reach <host>:<port> to upload this computer's key` | Connect to the network the server's ACL write server is on (for example, the VPN)          |
| `… rejected this computer's key (HTTP 400)`                   | The hint shows the server's reason                                                         |
| `… rejected the saved credentials for "<profile>"`            | The key was deleted or the user disabled. Run `dh auth login --profile <profile>`          |
| `Profile "<profile>" needs DH_PSK, which is not set`          | Set the variable the profile was saved with, or sign in again without `--psk-env`          |
| `… can be read by other users`                                | `chmod 600 ~/.deephaven/cli/credentials.json`                                              |
| `Input needed: pass <flag>`                                   | Running without a terminal; pass that flag                                                 |
| SSO keeps signing in as the wrong account                     | Choose the account in your identity provider, or sign out of it first; use `--expect-user` |
| `Timed out waiting for sign-in`                               | Finish signing in within `--timeout` seconds, or raise it                                  |
