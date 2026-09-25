# dh — Deephaven CLI

A single compiled binary, built with Deno, that updates itself from GitHub
releases.

```sh
curl -fsSL https://github.com/deephaven/deephaven-mcp/releases/latest/download/install.sh | sh
```

This installs to `~/.local/bin/dh` (override with `DH_INSTALL_DIR`). The
directory must be writable by the user, or `dh` can't update itself.

```sh
deno task build          # compile for this platform -> dist/ (--all for every target)
deno task test           # installs via install.sh from a fake GitHub and proves dh updates itself
deno task check          # fmt + lint + type-check
```

## Auto-update

After a run, `dh` fetches
`https://github.com/deephaven/deephaven-mcp/releases/latest/download/manifest.json`,
at most once per `DH_UPDATE_INTERVAL` (default 24 hours). If the manifest lists
a newer version for this platform, `dh` downloads it, checks the SHA-256, and
swaps the new binary in place of its own executable. The time of the last check
is kept in `<executable>.last-update-check`.

```json
{
  "version": "3.0.1",
  "binaries": {
    "aarch64-apple-darwin": {
      "url": "https://github.com/deephaven/deephaven-mcp/releases/download/v3.0.1/dh-aarch64-apple-darwin",
      "sha256": "..."
    }
  }
}
```

Every shipped binary reads this URL and format, so keep both backward
compatible.

| Env var              | Effect                                                |
| -------------------- | ----------------------------------------------------- |
| `DH_UPDATE_URL`      | Manifest URL (default in `src/updater.ts`)            |
| `DH_UPDATE_INTERVAL` | Hours between update checks (default 24; 0 = always)  |
| `DH_AUTO_UPDATE=off` | Disable auto-update                                   |
| `DH_DEBUG=1`         | Print update errors (otherwise updates fail silently) |

Update URLs must use HTTPS, except `localhost`/`127.0.0.1`. When `dh` runs from
source (`deno task dev`), it never updates itself.

## Releasing

Bump `version` in `deno.json`, then push a matching tag:

```sh
git tag v3.0.1 && git push origin v3.0.1
```

`.github/workflows/release.yml` runs the checks and tests, cross-compiles every
target (`deno task release <tag>`), and publishes the binaries, `manifest.json`,
`SHA256SUMS` and `install.sh` as a GitHub release.

Tags with a suffix (`v3.0.1-rc.1`) are published as prereleases, which
`releases/latest` ignores. Use them to test against real GitHub without
affecting users:

```sh
DH_UPDATE_URL=https://github.com/deephaven/deephaven-mcp/releases/download/v3.0.1-rc.1/manifest.json \
  DH_UPDATE_INTERVAL=0 DH_DEBUG=1 dh
```

Any later release not meant for `dh` (e.g. a v2.x Python patch) must be
published with `--latest=false`, or `dh` will stop finding its manifest.
