# dh — Developer Guide

For installing and using `dh`, see the [README](../README.md). For how the code
is organized and the rules for adding commands, see
[Architecture](ARCHITECTURE.md).

## Prerequisites

[Bun](https://bun.sh); the version is pinned in `package.json`'s
`packageManager` field, which CI also reads. Run `bun install` first.

## Common tasks

```sh
bun run dev              # run from source (never auto-updates)
bun run build            # compile for this platform -> dist/ (--all for every target)
bun test                 # unit and CLI tests, plus an install from a fake GitHub that proves dh updates itself
bun run check            # Biome format + lint, and tsc type-check
```

CI (`.github/workflows/ci.yml`) runs `check` on Ubuntu and `test` on Ubuntu,
macOS and Windows for every pull request and push to `main`.

## How auto-update works

After a run, `dh` reads its channel from `dh.meta` beside the executable (else
`release`) and fetches that channel release's manifest, e.g.
`https://github.com/deephaven/deephaven-mcp/releases/download/cli-channel-rc/manifest.json`
(the repo comes from `repository` in `package.json`), at most once per
`DH_UPDATE_INTERVAL`. If the manifest's version differs from its own (higher or
lower), `dh` downloads the build for this platform, checks the SHA-256, and swaps
it in place of its own executable. The time of the last check is kept in
`dh.meta` too. Only compiled binaries on a published channel update themselves:
the build bakes in the version and channel with `--define`, and `bun run dev` and
local `bun run build` binaries are `dev` builds, which never update.

`dh.meta` (`dh.exe.meta` on Windows) is a JSON object, `{"channel":"rc",
"lastUpdateCheck":"<ISO 8601>"}`. The install scripts write it fresh; the CLI
changes only its own keys, keeps unknown ones, and treats a bad file as empty.

```json
{
  "version": "3.0.1",
  "binaries": {
    "darwin-arm64": {
      "url": "https://github.com/deephaven/deephaven-mcp/releases/download/cli-v3.0.1/dh-darwin-arm64",
      "sha256": "..."
    }
  }
}
```

Every shipped binary reads this URL and format, so keep both backward
compatible.

## Releasing

Releases are published from `cli-channel/<channel>` branches: every push or
merge to one publishes the next build on that channel. The design, with
diagrams, is in [plans/cli-release-channels.md](../plans/cli-release-channels.md).

| Branch                   | Channel      | Fed by                                  |
| ------------------------ | ------------ | --------------------------------------- |
| `cli-channel/release`    | `release`    | PRs from `cli-channel/rc` or `hotfix/*` |
| `cli-channel/rc`         | `rc`         | Merges from `v3_rewrite`                |
| `cli-channel/alpha-<id>` | `alpha-<id>` | A feature branch, for experiments       |

`.github/workflows/release.yml` runs CI, computes the version
(`bun run next-version <channel>`), cross-compiles every target
(`bun run release <tag>`), and publishes (`bun run publish <tag> dist`):

1. A versioned release, `cli-vX.Y.Z` on `release` or
   `cli-vX.Y.Z-<channel>.<build>` elsewhere, with the binaries, `manifest.json`,
   `SHA256SUMS` and install scripts. Never changed afterwards.
2. If the commit is still the branch's newest, the channel release
   `cli-channel-<channel>`: its tag moves to the commit, its title becomes
   `<channel> channel (currently <tag>)`, and its `manifest.json` is replaced
   (plus `install.sh`/`install.ps1` on `release`). Installed CLIs and the install
   scripts read only this release. Never delete it.

Publishes to `release` wait for approval in the `cli-release` environment; other
channels use `cli-prerelease`. None of these releases is marked latest, so the
repo's `releases/latest` stays with the Python MCP releases.

**Versions.** The base is the higher of the last `cli-v` release plus a patch and
`package.json`'s `version`; channels other than `release` add
`-<channel>.<build>`, counting from 0 for each base. For a minor or major bump,
set `version` in `package.json` (e.g. `3.2.0`) in a PR; the next publish on every
channel picks it up.

**Promoting.** Merge `v3_rewrite` into `cli-channel/rc` to publish a release
candidate; open a PR from `cli-channel/rc` into `cli-channel/release` to release
it. Two required checks (`.github/workflows/release-checks.yml`) guard that PR:
`release-source` (it must come from `cli-channel/rc` or `hotfix/*`) and
`release-contains` (nothing already on `release` may be missing; cherry-picks
count).

**Hotfixes.** Branch `hotfix/<name>` from `cli-channel/release`, fix, and open a
PR into `cli-channel/release`. Then merge the fix into `v3_rewrite` and
`v3_rewrite` into `cli-channel/rc`; until then, `release-contains` fails the
next rc PR.

To test a published channel without switching an install:

```sh
DH_UPDATE_URL=https://github.com/deephaven/deephaven-mcp/releases/download/cli-channel-rc/manifest.json \
  DH_UPDATE_INTERVAL=0 DH_DEBUG=1 dh
```

### Repo setup

Done once by a repo admin:

- Create `cli-channel/release` and `cli-channel/rc` from `v3_rewrite`.
- Branch ruleset on `cli-channel/**`: block force pushes and deletion.
- Branch ruleset on `cli-channel/release`: require a pull request, allow only
  merge commits, and require the checks `release-source` and `release-contains`.
- Tag ruleset on `cli-v*`: block updates and deletion.
- Tag ruleset on `cli-channel-*`: block updates and deletion, with GitHub Actions
  as the only bypass actor.
- Environments: `cli-release` (required reviewers) and `cli-prerelease` (none).

### Testing on a fork

The release workflow takes the repo from `GITHUB_REPOSITORY`, so a push to a
fork's `cli-channel/<channel>` branch (with Actions enabled) produces binaries, a
manifest and install scripts that all point at the fork. Push to
`cli-channel/release` once so the install scripts exist, install from the fork's
`releases/download/cli-channel-release/install.sh` with `DH_CHANNEL=rc`, push to
`cli-channel/rc` twice, and run `DH_UPDATE_INTERVAL=0 DH_DEBUG=1 dh` to see it
update, with no other overrides.
