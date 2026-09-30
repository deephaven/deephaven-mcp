# dh — Deephaven CLI

A single compiled binary, built with Deno, that updates itself from GitHub
releases.

```sh
curl -fsSL https://github.com/deephaven/deephaven-mcp/releases/latest/download/install.sh | sh
```

On Windows (PowerShell):

```powershell
irm https://github.com/deephaven/deephaven-mcp/releases/latest/download/install.ps1 | iex
```

This installs to `~/.local/bin/dh` (Windows:
`%LOCALAPPDATA%\Programs\dh\dh.exe`, added to the user `PATH` unless
`DH_INSTALL_NO_MODIFY_PATH` is set). Override with `DH_INSTALL_DIR`. The
directory must be writable by the user, or `dh` can't update itself.

To install a specific release (including prereleases), set `DH_INSTALL_VERSION`.
`dh` still updates itself to the latest release unless `DH_AUTO_UPDATE=off`:

```sh
curl -fsSL https://github.com/deephaven/deephaven-mcp/releases/latest/download/install.sh | DH_INSTALL_VERSION=3.0.1 sh
```

## Auto-update

`dh` checks for a newer release at most once per `DH_UPDATE_INTERVAL` (default
24 hours) and, if one exists, replaces itself after verifying its SHA-256.

| Env var              | Effect                                                |
| -------------------- | ----------------------------------------------------- |
| `DH_UPDATE_URL`      | Manifest URL (defaults to this repo's latest release) |
| `DH_UPDATE_INTERVAL` | Hours between update checks (default 24; 0 = always)  |
| `DH_AUTO_UPDATE=off` | Disable auto-update                                   |
| `DH_DEBUG=1`         | Print update errors (otherwise updates fail silently) |

Update and install URLs, including every redirect, must use HTTPS. Plain HTTP is
allowed only for loopback (`localhost`, `127.0.0.1`, `[::1]`) test servers;
`install.sh` allows it only for the starting URL, so its redirects must still be
HTTPS.

## Development

Building, testing and releasing are covered in the
[Developer Guide](docs/DEVELOPER_GUIDE.md).
