# `dh` release channels

## Strategy

- **Versions are git tags.** `cli-v3.1.0` is a release, and
  `cli-v3.1.0-pre.2` is build 2 of 3.1.0 on the `pre` channel. Any lowercase
  name can be a channel, for example `cli-v3.1.1-experiment324.0`.
- **Each install follows one channel,** `release` by default. Opt in with
  `DH_CHANNEL=pre`. The install script saves the choice next to the binary,
  so `dh` keeps following it even where the variable isn't set (for example,
  when an MCP client starts `dh`).
- **`dh` installs its channel's latest build.** It doesn't compare versions:
  if the channel's latest differs from what is running, `dh` switches to it.
- **Channels are independent.** Publishing to one channel never changes what
  another channel serves.
- **Each channel is a straight line in git.** A new tag's commit must include
  the channel's latest tag, so moving to a channel's next build never drops
  work that was in its previous one.
- **Releases come only from `pre`.** A release is a promoted `pre` tag on the
  same commit. Experiments reach a release by going out on `pre` first.
- **People release with one local script, and CI publishes.** You compute and
  push a tag; pushing the tag runs the release workflow, which builds,
  publishes the GitHub release, and points the channel at it.

```sh
bun run tag pre                  # dry run: shows cli-v3.1.1-pre.0 and the commit
bun run tag pre --push           # tag HEAD and push → CI publishes to `pre`
bun run tag pre --bump minor --push   # start cli-v3.2.0-pre.0 instead
bun run tag experiment324 --push # any other channel works the same way
bun run tag release --push       # promote the `pre` tag on HEAD → cli-v3.1.1
```

A typical history, oldest at the top (the `cli-v` prefix is omitted). `pre`
tags go on commits on the main line, and a release is a second tag on a commit
that already has a `pre` tag:

```text
 main   pre                      release
 ────   ───                      ───────
  ●                              3.0.0
  ●     3.0.1-pre.0               ← tag pre
  ●     3.0.1-pre.1  ══════════▶ 3.0.1
  │                               ← tag release --from cli-v3.0.1-pre.1,
  │                                 run later, after 3.1.0-pre.1 was tagged
  ●     3.1.0-pre.0               ← tag pre --bump minor
  ●     3.1.0-pre.1
  ●     3.1.0-pre.2               ← tag pre (first one after the release)
  ●     4.0.0-pre.0               ← tag pre --bump major (breaking change)
```

The next `pre` tag is the later of these two:

- **The last release reachable from the commit, plus the bump.** The bump is
  `patch` unless `--bump` says otherwise.
- **The `pre` line already in progress.** If that line is later, the tag
  continues it with the next `.N`.

So releasing an older `pre` build never pulls `pre` backwards, and `--bump`
only matters when it goes past the line in progress. For the commit tagged
`3.1.0-pre.2` above, the last release is `3.0.1` and the line in progress is
`3.1.0`:

| Command | Last release + bump | Line in progress | Result |
| --- | --- | --- | --- |
| `tag pre` | `3.0.2` | `3.1.0` | `3.1.0-pre.2` |
| `tag pre --bump minor` | `3.1.0` | `3.1.0` | `3.1.0-pre.2` (already a minor ahead) |
| `tag pre --bump major` | `4.0.0` | `3.1.0` | `4.0.0-pre.0` |

When the release is an older build of the same version, the next `pre` tag
starts a new patch line. The builds that weren't released continue in it:

```text
  ●     3.1.0-pre.2  ══════════▶ 3.1.0   (the tool warns that pre.3 and pre.4 exist)
  ●     3.1.0-pre.3
  ●     3.1.0-pre.4
  ●     3.1.1-pre.0               ← next tag pre: carries pre.3 and pre.4 forward
```

How a client finds its channel's latest: every channel has a fixed GitHub
release, `cli-channel-<channel>`, whose `manifest.json` CI replaces on each
publish. `dh` and the install scripts fetch
`https://github.com/<repo>/releases/download/cli-channel-<channel>/manifest.json`.
`dh` never reads or sets GitHub's `releases/latest`, which stays with the
Python MCP releases.

The rest of this doc is the detailed spec.

## Tags

```text
cli-v<MAJOR>.<MINOR>.<PATCH>                 release
cli-v<MAJOR>.<MINOR>.<PATCH>-<channel>.<n>   prerelease on <channel>
```

- `<channel>` matches `^[a-z][a-z0-9]*$`. `release` is reserved and is never a
  suffix. There are no dots or hyphens, so a tag always has exactly two
  prerelease identifiers.
- `<n>` is a non-negative integer without leading zeros. It counts up per
  `(X.Y.Z, channel)`.
- Any other `cli-v*` tag is invalid. Scripts ignore it with a warning, and the
  workflow rejects it.
- Releases are created only by **promoting** a `pre` tag. The release tag goes
  on the same commit. `pre` is the one special prerelease channel: it is the
  release-candidate line. Other channels (experiments) can never be promoted.
  Their work reaches a release by merging and going out on `pre` first.
- Version source of truth: the tag. `package.json` `version` is no longer
  checked against it and is used only for local builds (`bun run build`,
  `bun run dev`). A promotion needs no bump commit.
- Versions are never compared across channels. Each channel is its own
  sequence. The tagging script makes every new tag in a channel the latest one
  in that channel, so "latest" is unambiguous.

## Channel pointers (how clients find "latest on channel X")

Every channel has a **pointer release**. Its tag is `cli-channel-<channel>` and
it is created once by the workflow (`--prerelease --latest=false`). The workflow
replaces that release's assets on every publish. Clients fetch one fixed URL:

```text
https://github.com/<repo>/releases/download/cli-channel-<channel>/manifest.json
```

A pointer's manifest is **the latest build on that channel**. Publishing a tag
points that tag's own channel at it, and touches no other channel.

The client makes one fetch and installs whatever the pointer names, if that
differs from the version it is running. It does no version comparison and has
no notion of "higher" or "lower".

Gotcha: `gh release upload --clobber` deletes the asset and then uploads the
new one, so the manifest can briefly return 404. The updater already treats a
failed fetch as "try again next interval". Upload `manifest.json` last.

## Tagging script: `bun run tag`

Runs locally with the user's git credentials. The workflow does the
publishing; the script only creates and pushes the tag.

```text
bun run tag <channel> [--bump patch|minor|major] [--ref <commit>] [--push]
bun run tag release   [--from <cli-vX.Y.Z-pre.N>]               [--push]
```

Without `--push`, the script prints the computed tag, the commit, the base tag
it reasoned from, and any warnings, then exits 0 (a dry run). With `--push`, it
creates an annotated tag (`-m "dh <version>"`) and runs
`git push origin refs/tags/<tag>`.

**Always first:** `git fetch origin --tags --force`. Then:

**Prerelease (`<channel>` ≠ `release`).** `--ref` defaults to `HEAD`.

1. The ref must be on the remote: `git branch -r --contains <ref>` must be
   non-empty. Otherwise exit 1 with "push the commit first".
2. Collect the tags reachable from ref: `git tag --merged <ref> --list 'cli-v*'`.
   - `lastRelease` = the semver max of the reachable release tags. Use
     `0.0.0` if there are none.
   - `current` = the semver max of the reachable `-<channel>.n` tags whose
     `X.Y.Z` is greater than `lastRelease`. It may not exist.
3. `floor = inc(lastRelease, bump)`. `bump` defaults to `patch`. Use
   `semver.inc` on the release version, which has no prerelease part, so the
   result is clean.
4. `target = max(floor, current?.X.Y.Z)`.
   - Base `1.4.0` (no current): patch → `1.4.1`, minor → `1.5.0`.
   - Last release `1.1.2`, current `1.1.3-pre.2`: patch → `1.1.3`, minor →
     `1.2.0`.
   - Last release `1.1.2`, current `1.2.0-pre.0`: patch → `1.2.0`. The bump is
     a floor, so it never goes backwards.
5. `n` = 1 + the max `n` over **all** tags `cli-v<target>-<channel>.*`
   (global, not just reachable), or `0` if there are none. Collisions with
   tags on other branches are impossible.
6. Errors (exit 1):
   - `cli-v<target>` already exists anywhere. The message suggests `--bump`.
   - The new tag would not be the latest on its channel: some
     `-<channel>.*` tag anywhere has a higher version (for example, tagging
     from an old branch). This keeps each channel a single forward sequence.
   - **Lineage:** the ref does not include the channel's latest tag, checked
     with `git merge-base --is-ancestor <latest channel tag> <ref>`. The
     latest tag is the highest version among all `-<channel>.*` tags. The
     first tag on a channel is exempt. Without this, a branch that split off
     earlier could tag a higher version (for example with `--bump major`) and
     followers would silently lose the work tagged on the channel since the
     split. Message:

     ```text
     error: HEAD does not include cli-v3.1.0-pre.2, the latest tag on `pre`.
            Merge or rebase onto it first, or publish to a separate channel
            (e.g. `bun run tag bigrefactor`).
     ```
   - The ref already carries a `-<channel>.*` tag. There is nothing new to
     publish.
   - The channel name is invalid, or is `release`.

The script uses semver only to compute the next tag *within* the target
channel (and from the last reachable release, which is where every channel
line starts).

**Release (`release`):**

1. `--from` defaults to the single `-pre.*` tag that points at `HEAD`. If
   there are 0 or 2+ such tags, exit 1 and list the candidates.
2. The source must be a valid `cli-vX.Y.Z-pre.N` tag. The release is
   `cli-v<source X.Y.Z>`, on the source's commit (`<tag>^{commit}`), not on
   `HEAD`.
3. Only the release tag is created. The source channel's followers are already
   running this exact commit, so they need nothing new. The next
   `bun run tag <channel>` starts a fresh line at `X.Y.(Z+1)-<channel>.0` (or a
   bumped `.0`), because `lastRelease` is now `X.Y.Z`.

   Because a release always comes from `pre`, `pre` never needs to catch up
   to a release. Either `pre` is still on the released commit, or it has
   already moved ahead with newer work that a catch-up would drop.
4. Errors: the release tag already exists, the source is not a `pre` tag
   (the message says to ship the work on `pre` first), or
   the release would not be the latest release (a hotfix on an old line).
   Hotfixing old lines is out of scope. **Lineage:** the source commit must
   include the latest release tag (`git merge-base --is-ancestor`), unless
   there is no release yet. Linear `pre` already implies this; the check
   makes the release line's guarantee explicit.
5. Warning: a higher `n` exists for the same `X.Y.Z` and channel. You may be
   promoting a stale build.

Keep tag parsing, validation and next-tag computation as pure functions, and
test them without git. They share the tag grammar module with the updater (see
Updater).

## Workflow (`.github/workflows/release.yml`)

- Trigger: `push: tags: ["cli-v*"]` only. Remove `workflow_dispatch`, because
  the version now comes from the tag and dispatch has no way to choose one.
  Re-run a failed publish with "Re-run jobs".
- GitHub runs the workflow file **of the tagged commit**, so tags work from
  `v3_rewrite` before anything merges to `main`.
- Steps: `ci` → checkout → bun install → `bun run release "$TAG" dist` →
  `bun run publish "$TAG" dist` (`GH_TOKEN`).
- `scripts/release.ts`: parse and validate the tag with the shared grammar, and
  take the version from it (`cli-v3.1.0-pre.2` → `3.1.0-pre.2`). Drop the
  `package.json` version check. `baseUrl` stays tagged
  (`releases/download/<tag>/`).
- New `scripts/publish.ts <tag> <dir>`:
  1. `gh release create <tag> <dir>/* --verify-tag --latest=false
     --generate-notes --notes-start-tag <prev>`, plus `--prerelease` for
     channel tags. `<prev>` is the previous tag in the same channel for
     prereleases (falling back to the last release), and the previous release
     for releases. It must be passed explicitly, or GitHub may diff against a
     Python `v*` tag.
  2. Update the tag's own channel pointer (`cli-channel-release` for a release,
     `cli-channel-C` for a prerelease), and no other. Guard: skip if the tag is
     not the latest `cli-v*` tag on its channel (`git tag --list`, same channel
     only). The tagging script already guarantees that new tags are the latest,
     so this only protects "Re-run jobs" on an old run from pointing the
     channel back. If the pointer is missing, create it with `gh release create cli-channel-C
     --target <sha> --prerelease --latest=false --title "dh C channel" --notes
     "Moving pointer to the newest dh build on the C channel. Assets are
     replaced on each publish. Do not delete."`.
  3. Upload `manifest.json` to the pointer. Only `cli-channel-release` also
     gets `install.sh` and `install.ps1`, so there is one install URL. Upload
     the install scripts first and `manifest.json` last.
- Keep `concurrency: release` so publishes are serialized.
- Gotcha: tags created with `GITHUB_TOKEN` (the pointer tags) never trigger
  workflows, and `cli-channel-*` doesn't match `cli-v*` anyway.

**Repo settings (manual, record in DEVELOPER_GUIDE):**

- Point the `release` environment's deployment rule at tag pattern `cli-v*`.
- Add a tag ruleset that restricts creating, updating and deleting `cli-v*` and
  `cli-channel-*` to maintainers. Pushing a tag is now a release.

## Updater (`src/updater.ts`)

- Channel resolution, first match wins:
  1. `DH_CHANNEL` (non-empty). This is the same variable the install
     scripts read, so setting it once means the same thing to both.
  2. the sidecar file `<execPath>.channel`, trimmed (next to the existing
     `.last-update-check`)
  3. `release`

  An invalid name throws, which shows under `DH_DEBUG`.
- Default URL:
  `${releasesUrl(REPOSITORY)}/download/cli-channel-${channel}/manifest.json`.
  `DH_UPDATE_URL` still overrides it entirely.
- Replace `gt(manifest.version, VERSION)` with
  `manifest.version !== VERSION`. Keep validating `manifest.version` with
  `semver.valid` so a malformed manifest throws. Remove the `gt` import if it
  is no longer used. The sha256 check and swap stay the same.
- Switching channels takes effect at the next check: the binary becomes that
  channel's latest, whatever its version.
- Don't bake the channel into the binary. The channel belongs to the install
  (sidecar or env), not the build.
- Shared tag grammar module `src/channel.ts` (used by the updater and the
  scripts): the channel regex, `cli-v` and `cli-channel-` prefixes, and
  parse/format for tags.

## Install scripts (`install.sh`, `install.ps1`)

- Read `DH_CHANNEL`, default `release`, validated with the same regex.
- Default resolution: fetch
  `$REPO_URL/releases/download/cli-channel-$CHANNEL/manifest.json`.
  - ps1 already installs from the manifest.
  - sh reads the version with
    `sed -n 's/^ *"version": *"\([^"]*\)".*/\1/p' | head -n 1` (only the
    top-level key is named `version`), sets `TAG=cli-v$version`, and continues
    as today with `SHA256SUMS`.
  - A 404 means "no channel `<C>` published": exit 1 with that message.
- `DH_INSTALL_VERSION` accepts `3.1.0`, `v3.1.0` or `cli-v3.1.0`, and
  normalizes to `cli-v…`.
- The tag check becomes `case "$TAG" in cli-v*)`.
- If `DH_CHANNEL` is set, write the sidecar `<installed exe>.channel`
  (`dh.channel`; on Windows `dh.exe.channel`). If it is set to `release`,
  remove the sidecar. If it is unset, leave any existing sidecar untouched.
- One-liners become
  `…/releases/download/cli-channel-release/install.sh` (and `.ps1`). Prerelease
  installs use the same URL with `DH_CHANNEL=pre`.

## Out of scope (follow-ups)

- A `dh` command to show or set the channel (it would write the sidecar).
- CI-driven tagging (`workflow_dispatch` with channel/bump inputs that call the
  same pure functions).
- Hotfix releases on an old line (a release that isn't the latest release).
- Reusing the tested prerelease binaries for the release. Promotion rebuilds
  from the same commit and lockfile, because the version is baked in.

## Background

**Why change.** `dh` currently reads
`releases/latest/download/manifest.json`, but the Python MCP (v2) releases
compete for that same `latest` slot. Today `latest` is `v2.1.0`, which has no
manifest. The `v*` tag namespace is shared too: once `release.yml` reaches
`main`, a Python `v2.x` tag would start a `dh` release. The `cli-v*` prefix and
the `--latest=false` flag on every `dh` release separate the two completely.
This also removes the DEVELOPER_GUIDE rule that Python releases must pass
`--latest=false`.

**No compatibility constraint.** No `dh` binary has shipped yet (the remote has
no `v3*` tag), so neither the update URL nor the manifest location needs to
stay compatible with old builds.

**Pointer alternatives considered and rejected:**

- GitHub REST `/releases`: 60 requests/hour per IP without auth (a shared
  company NAT would hit it), it paginates through every Python release, and its
  failures are silent.
- Raw files on a branch: needs pushes to a protected branch, and has CDN
  staleness.
- Discovering tags directly with `ls-remote`: a tag exists before its assets do.

**Decided against:**

- Comparing versions across channels, or letting a release move other
  channels' pointers. A release isn't guaranteed to contain another channel's
  work, so experiment followers could silently lose what they opted in to test.
- Creating a follow-on `pre` tag on each release ("catching `pre` up").
  Because releases come only from `pre`, `pre` is either already on the
  released commit or ahead of it, and a catch-up would drop newer work.

## Checklist

- [ ] `src/channel.ts`: tag grammar, channel regex, prefixes, parse/format
- [ ] `src/updater.ts`: channel resolution (env > sidecar > `release`),
      pointer URL, update when the version differs (not when it is greater)
- [ ] `scripts/tag.ts` + `package.json` `"tag"` script: prerelease and
      promotion rules above, dry run by default, `--push`
- [ ] `scripts/release.ts`: version from tag, drop `package.json` check
- [ ] `scripts/publish.ts` + `package.json` `"publish"` script: create the
      release with explicit `--notes-start-tag`, point only the tag's own
      channel at it, skip if it isn't the channel's latest tag
- [ ] `.github/workflows/release.yml`: `cli-v*` trigger, no dispatch, call
      `publish`
- [ ] `install.sh` / `install.ps1`: `DH_CHANNEL`, pointer resolution,
      `cli-v` tags, sidecar
- [ ] Tests: `tests/channel.test.ts` (grammar + next-tag table),
      `tests/auto_update.test.ts` (fake GitHub serves `cli-channel-*` pointers;
      env and sidecar channel; installer channel + sidecar)
- [ ] Docs: README install URLs + channel env vars; DEVELOPER_GUIDE Releasing,
      auto-update, fork testing, repo settings; remove the Python
      `--latest=false` caveat

## Validation

- [ ] `bun run check` and `bun test` pass.
- [ ] Next-tag table (unit, from tag lists):

  | Reachable tags | Command | Result |
  | --- | --- | --- |
  | none | `pre` | `cli-v0.0.1-pre.0` |
  | `cli-v1.4.0` | `pre` | `cli-v1.4.1-pre.0` |
  | `cli-v1.4.0` | `pre --bump minor` | `cli-v1.5.0-pre.0` |
  | `cli-v1.4.0` | `pre --bump major` | `cli-v2.0.0-pre.0` |
  | `cli-v1.1.2`, `cli-v1.1.3-pre.2` | `pre` | `cli-v1.1.3-pre.3` |
  | `cli-v1.1.2`, `cli-v1.1.3-pre.2` | `pre --bump minor` | `cli-v1.2.0-pre.0` |
  | `cli-v1.1.2`, `cli-v1.2.0-pre.0` | `pre` | `cli-v1.2.0-pre.1` |
  | `cli-v1.4.0`, `cli-v1.4.1-pre.0` | `experiment324` | `cli-v1.4.1-experiment324.0` |
  | reachable `cli-v1.1.3-pre.2`, unreachable `cli-v1.1.3-pre.3` | `pre` | exit 1 (lineage: HEAD lacks `cli-v1.1.3-pre.3`) |
  | `cli-v1.1.3-pre.2`; branch split before it | `pre --bump major` on the branch | exit 1 (lineage), even though `2.0.0` is higher |
  | same, after merging the commit tagged `cli-v1.1.3-pre.2` into the branch | `pre --bump major` | `cli-v2.0.0-pre.0` |
  | `cli-v1.1.3-pre.2` on `pre`; branch split before it | `bigrefactor` on the branch | `cli-v1.1.3-bigrefactor.0` (first tag on a channel is exempt) |
  | `cli-v1.1.3-pre.2` on HEAD | `release` | `cli-v1.1.3` on the same commit, and no other tag |
  | `cli-v1.1.3-exp.1` on HEAD | `release` | exit 1 (only `pre` tags can be promoted) |
  | any | `release --from cli-v1.1.3-exp.1` | exit 1 |
  | `cli-v1.1.3` (promoted) | `pre` | `cli-v1.1.4-pre.0` |
  | reachable `cli-v1.1.2`, unreachable `cli-v1.2.0-pre.0` | `pre` | exit 1 (not the latest on `pre`, and lineage) |
  | `cli-v1.1.3` exists | `release --from cli-v1.1.3-pre.1` | exit 1 |
  | `cli-v1.2.0` exists | `release --from cli-v1.1.4-pre.0` | exit 1 (not the latest release) |
  | any | `Pre`, `pre-1`, `release` as channel name | exit 1 |
  | `v2.1.0`, `cli-vfoo` | `pre` | ignored; same as no tags (warning for `cli-vfoo`) |

- [ ] The lineage rows above run against a temporary git repo (real commits and
      tags), not just tag lists.
- [ ] `bun run tag pre` without `--push` creates no tag
      (`git tag -l 'cli-v*'` is unchanged).
- [ ] Pointer update (unit, `gh` stubbed): a release updates only
      `cli-channel-release`. A prerelease updates only its own channel. A tag
      that isn't its channel's latest updates nothing. A missing pointer is
      created. `manifest.json` is uploaded last.
- [ ] Updater (fake GitHub): default fetches
      `/releases/download/cli-channel-release/manifest.json`. With
      `DH_CHANNEL=pre` it fetches `cli-channel-pre`. A sidecar
      containing `pre` fetches `cli-channel-pre`, and the env var overrides the
      sidecar. An invalid channel performs no fetch and logs under `DH_DEBUG`.
      `DH_UPDATE_URL` still wins. A manifest with a *lower* version than the
      running binary is installed, and one with an equal version is not.
- [ ] Installer (fake GitHub): the default installs from
      `cli-channel-release`. `DH_CHANNEL=pre` installs the `pre`
      pointer's version and writes `dh.channel` = `pre`.
      `DH_CHANNEL=release` removes the sidecar. An unknown channel
      exits non-zero with "no channel". `DH_INSTALL_VERSION=cli-v9.9.9` and
      `9.9.9` both work.
- [ ] End to end on a fork: `bun run tag pre --push` produces
      `cli-v…-pre.0`. The release is a prerelease, not marked latest, and
      `cli-channel-pre` exists. `bun run tag release --push` produces the
      release only. The `release` pointer shows it, and the `pre` pointer is
      unchanged. The repo's
      `releases/latest` is unchanged throughout. An installed `dh` with
      `DH_UPDATE_INTERVAL=0 DH_DEBUG=1` updates along its channel.

## Relevant Files

- `src/channel.ts` (new)
- `src/updater.ts`
- `src/version.ts` (doc comment only: `package.json` version is for local
  builds)
- `scripts/tag.ts` (new)
- `scripts/publish.ts` (new)
- `scripts/release.ts`
- `scripts/build.ts` (CLI log line only, if it still says `v${pkg.version}`)
- `package.json` (`tag`, `publish` scripts)
- `.github/workflows/release.yml`
- `install.sh`
- `install.ps1`
- `tests/channel.test.ts` (new)
- `tests/tag.test.ts` (new; next-tag table + pointer-update logic)
- `tests/auto_update.test.ts`
- `README.md`
- `docs/DEVELOPER_GUIDE.md`
