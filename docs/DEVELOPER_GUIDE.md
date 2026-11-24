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

Channels, publishing and versions are described in
[Release architecture](RELEASE_ARCHITECTURE.md); this section covers what the
binary does.

Updates come from **channel releases**: one GitHub release per channel, named
`cli-channel-<channel>` (`cli-channel-release`, `cli-channel-rc`, …), whose
`manifest.json` always names that channel's current build. Every publish to the
channel replaces the manifest and moves the release's tag; the builds it names
live in the versioned `cli-v…` releases, which never change.

After each command finishes and prints its output (except `--help` and
`--version`), `dh` checks for an update, at most once per `DH_UPDATE_INTERVAL`.
It reads its channel from `dh.meta` beside the executable (else `release`) and
fetches that channel release's manifest, e.g.
`https://github.com/deephaven/deephaven-mcp/releases/download/cli-channel-rc/manifest.json`
(the repo is baked in at build time: `GITHUB_REPOSITORY` on CI, so a fork's
builds update from the fork). If the manifest's version differs from its own
(higher or lower), `dh` downloads the build for this platform, checks the
SHA-256, and swaps it in place of its own executable. The time of the last check
is kept in `dh.meta` too. Only compiled binaries on a published channel update
themselves: the build bakes in the version and channel with `--define`, and
`bun run dev` and local `bun run build` binaries are `dev` builds, which never
update.

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

Every push or merge to a `cli-channel/<channel>` branch publishes the next build
on that channel; CI computes the version. Why it works this way, and what each
publish creates, is in [Release architecture](RELEASE_ARCHITECTURE.md).

- **Release candidate:** open a PR from `v3_rewrite` into `cli-channel/rc` and
  merge it with a merge commit. PRs into every `cli-channel/*` branch use merge
  commits, never squash.
- **Release:** open a PR from `cli-channel/rc` into `cli-channel/release`. The
  `release-source` and `release-contains` checks must pass; merging publishes.
- **Experiment:** push a new `cli-channel/alpha-<id>` branch to create the
  channel, then merge a feature branch into it with a PR.
- **Hotfix:** follow [Hotfixes](RELEASE_ARCHITECTURE.md#hotfixes).
- **Minor or major bump:** set `version` in `package.json` (e.g. `3.2.0`) in a PR
  into `v3_rewrite`. It takes effect when `v3_rewrite` is next merged into
  `cli-channel/rc` (`3.2.0-rc.0`), then promoted to release (`3.2.0`); see
  [Minor and major bumps](RELEASE_ARCHITECTURE.md#minor-and-major-bumps).
- **Failed publish:** a run can fail partway through publishing. It may leave a
  versioned release that no channel points to, which is harmless, or a channel
  release without its `manifest.json`, so updates and installs on that channel
  fail until it's republished. Check the run's log, fix the cause if needed, and
  use "Re-run jobs" on the failed run in Actions: it publishes the next version
  and completes the channel release. Re-running an older commit's run never
  moves the channel back.

To try a published channel's build in an existing install without changing its
saved channel (this replaces the binary when the versions differ, and the next
normal update puts the saved channel's build back):

```sh
DH_UPDATE_URL=https://github.com/deephaven/deephaven-mcp/releases/download/cli-channel-rc/manifest.json \
  DH_UPDATE_INTERVAL=0 DH_DEBUG=1 dh
```

To leave the main install untouched, install a second copy instead:
`DH_CHANNEL=rc DH_INSTALL_DIR=~/dh-rc`.

### Repo setup

Done once by a repo admin:

- Create `cli-channel/release` and `cli-channel/rc` from `v3_rewrite`.
- Branch ruleset on `cli-channel/**`: block force pushes and deletion; require
  a pull request with 0 approvals and allow only merge commits (no squash or
  rebase).
- Branch ruleset on `cli-channel/release`: require a pull request with an
  approving review, and require the checks `release-source` and
  `release-contains`.
- Tag ruleset on `cli-v*`: block updates and deletion.
- Tag ruleset on `cli-channel-*`: block updates and deletion, with GitHub Actions
  as the only bypass actor.

### Testing on a fork

The release workflow takes the repo from `GITHUB_REPOSITORY`, so merging into a
fork's `cli-channel/<channel>` branch (with Actions enabled) produces binaries,
a manifest and install scripts that all point at the fork. Apply the repo setup
to the fork too, to test the rulesets. Make every PR within the fork: GitHub's
new-PR page defaults the base to the upstream repo, and the release checks
reject a PR that crosses repos. Merge into `cli-channel/release` once so
the install scripts exist, install from the fork's
`releases/download/cli-channel-release/install.sh` with `DH_CHANNEL=rc`, merge
into `cli-channel/rc` twice, and run `DH_UPDATE_INTERVAL=0 DH_DEBUG=1 dh` to see
it update, with no other overrides.
