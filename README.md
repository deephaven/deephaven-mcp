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

```sh
deno task build          # compile for this platform -> dist/ (--all for every target)
deno task test           # installs via install.sh/install.ps1 from a fake GitHub and proves dh updates itself
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

| Env var              | Effect                                                  |
| -------------------- | ------------------------------------------------------- |
| `DH_UPDATE_URL`      | Manifest URL (default from `repository` in `deno.json`) |
| `DH_UPDATE_INTERVAL` | Hours between update checks (default 24; 0 = always)    |
| `DH_AUTO_UPDATE=off` | Disable auto-update                                     |
| `DH_DEBUG=1`         | Print update errors (otherwise updates fail silently)   |

Update URLs must use HTTPS, except `localhost`/`127.0.0.1`. When `dh` runs from
source (`deno task dev`), it never updates itself.

## Releasing

Bump `version` in `deno.json`, then either push a matching tag:

```sh
git tag v3.0.1 && git push origin v3.0.1
```

or, on GitHub, open **Actions → Release → Run workflow** and pick the branch.
The run tags that branch's latest commit as `v<version from deno.json>`, and
fails if that tag already exists. GitHub only shows this button for workflows on
the default branch.

`.github/workflows/release.yml` runs CI (`.github/workflows/ci.yml`, which also
runs on pull requests and pushes to `main`), cross-compiles every target
(`deno task release <tag>`), and publishes the binaries, `manifest.json`,
`SHA256SUMS`, `install.sh` and `install.ps1` as a GitHub release.

Tags with a suffix (`v3.0.1-rc.1`) are published as prereleases, which
`releases/latest` ignores. Use them to test against real GitHub without
affecting users:

```sh
DH_UPDATE_URL=https://github.com/deephaven/deephaven-mcp/releases/download/v3.0.1-rc.1/manifest.json \
  DH_UPDATE_INTERVAL=0 DH_DEBUG=1 dh
```

Python v2.x patches are tagged from the Python code, which doesn't contain
`release.yml` (GitHub runs the workflows of the tagged commit), so they never
trigger a `dh` release. They must still be published with `--latest=false`, or
`dh` will stop finding its manifest.

### Testing on a fork

The release workflow takes the repo from `GITHUB_REPOSITORY`, so a tag pushed to
a fork (with Actions enabled) produces binaries, a manifest and `install.sh`
that all point at the fork. Install from the fork's
`releases/latest/download/install.sh`, push a newer tag, and run
`DH_UPDATE_INTERVAL=0 DH_DEBUG=1 dh` to see it update, with no other overrides.
