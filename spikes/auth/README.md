# Auth checks

Standalone Deno scripts that exercise the libraries `dh auth` depends on, under
`deno run` and as compiled binaries.

## Setup

```sh
cd spikes/auth
deno task compile   # bin/{crypto,dhc,dhe,ink}, same permissions as scripts/build.ts
```

Run a check with `deno task <check> …` or `./bin/<check> …`.

## Checks

| Check                | Server | Verifies                                                                                                          |
| -------------------- | ------ | ----------------------------------------------------------------------------------------------------------------- |
| `crypto`             | —      | auth-nodejs EC P-256 keygen, DER signature, `EC:` prefix. 96-byte SAML nonce survives URL encoding.               |
| `dhc <url>`          | DHC    | Download + import `dh-core.js`, read auth handlers, log in (PSK or anonymous), run code. PSK: wrong PSK rejected. |
| `dhe probe <url>`    | DHE    | Download + import irisapi, read auth config. SAML login with an unused nonce waits instead of failing. No login.  |
| `dhe saml <url>`     | DHE    | Browser SSO, then key round trip.                                                                                 |
| `dhe password <url>` | DHE    | Password login, then key round trip.                                                                              |
| `ink`                | —      | Ink picker: arrows, Enter, `q`, Ctrl-C. Skipped without a TTY.                                                    |

Key round trip: upload a new key (label `dh auth check <host> <time>`), log in
with it on a new connection, delete it, confirm it's refused. `--keep-key`
leaves it on the server.

## Options

| Flag / env       | Check                 | Effect                                                        |
| ---------------- | --------------------- | ------------------------------------------------------------- |
| `DH_PSK`         | `dhc`                 | PSK for PSK servers                                           |
| `--groovy`       | `dhc`                 | Groovy session (default Python)                               |
| `--http2`        | `dhc`                 | Node HTTP/2 gRPC transport                                    |
| `--cjs`          | `dhc`                 | Load the jsapi as CJS (fails under Deno; ESM required)        |
| `DH_USER`, stdin | `dhe password`        | Username; password on stdin                                   |
| `--keep-key`     | `dhe saml`/`password` | Don't delete the uploaded key                                 |
| `--no-browser`   | `dhe saml`            | Print the sign-in URL only (e.g. open it in a private window) |
| `DH_TIMEOUT`     | all                   | Per-check timeout in seconds (default 30)                     |

## Examples

```sh
docker run --rm -p 10000:10000 -e START_OPTS="-Dauthentication.psk=my-psk" ghcr.io/deephaven/server
DH_PSK=my-psk ./bin/dhc http://localhost:10000

./bin/dhe probe https://dhe.example.com:8123
./bin/dhe saml https://dhe.example.com:8123
read -s P; printf %s "$P" | DH_USER=user-a ./bin/dhe password https://dhe.example.com:8123

./bin/ink
./bin/ink < /dev/null
```

## Output

One line per step: `PASS|FAIL <step> (<ms> ms): <detail>`, then
`RESULT: PASS|FAIL`. Exit 1 on failure. Downloaded jsapi files are cached in
`$TMPDIR/dh-auth-check/`.
