# dh — Developer Guide

For installing and using `dh`, see the [README](../README.md).

## Prerequisites

[Bun](https://bun.sh); the version is pinned in `package.json`'s
`packageManager` field, which CI also reads. Run `bun install` first.

## Common tasks

```sh
bun run dev              # run from source (never auto-updates)
bun run build            # compile for this platform -> dist/ (--all for every target)
bun test                 # installs via install.sh/install.ps1 from a fake GitHub and proves dh updates itself
bun run check            # Biome format + lint, and tsc type-check
```

CI (`.github/workflows/ci.yml`) runs `check` on Ubuntu and `test` on Ubuntu,
macOS and Windows for every pull request and push to `main`.

## How auto-update works

After a run, `dh` fetches
`https://github.com/deephaven/deephaven-mcp/releases/latest/download/manifest.json`
(from `repository` in `package.json`), at most once per `DH_UPDATE_INTERVAL`. If
the manifest lists a newer version for this platform, `dh` downloads it, checks
the SHA-256, and swaps the new binary in place of its own executable. The time
of the last check is kept in `<executable>.last-update-check`. Only compiled
binaries update themselves (the build bakes in the version with `--define`);
`bun run dev` never does.

```json
{
  "version": "3.0.1",
  "binaries": {
    "darwin-arm64": {
      "url": "https://github.com/deephaven/deephaven-mcp/releases/download/v3.0.1/dh-darwin-arm64",
      "sha256": "..."
    }
  }
}
```

Every shipped binary reads this URL and format, so keep both backward
compatible.

## Releasing

Bump `version` in `package.json`, commit it, and push the commit to the branch
first, so the tag points at a commit that's on the remote. Then either push a
matching tag:

```sh
git tag v3.0.1 && git push origin v3.0.1
```

or, on GitHub, open **Actions → Release → Run workflow** and pick the branch.
The run tags that branch's latest commit as `v<version from package.json>`, and
fails if that tag already exists. GitHub only shows this button for workflows on
the default branch.

`.github/workflows/release.yml` runs CI, cross-compiles every target
(`bun run release <tag>`), and publishes the binaries, `manifest.json`,
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
a fork (with Actions enabled) produces binaries, a manifest and install scripts
that all point at the fork. Install from the fork's
`releases/latest/download/install.sh`, push a newer tag, and run
`DH_UPDATE_INTERVAL=0 DH_DEBUG=1 dh` to see it update, with no other overrides.
