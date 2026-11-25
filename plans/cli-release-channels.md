# CLI release channels

Publish the CLI on release channels from `cli-channel/<channel>` branches, and
have each install follow one channel. The design is in
[Release architecture](../docs/RELEASE_ARCHITECTURE.md); the user-facing
surface is in [Release channels](../docs/CHANNELS.md), the README,
`docs/AUTH.md` and the Developer Guide. Those docs ship in PR 1 and describe
the finished behavior: the implementation must match them, or change them in
the same PR and say so.

Names, formats and output below are exact; internal function and module names
are up to the implementer, matching existing patterns. No CLI has shipped yet,
so nothing needs migrating: the old `releases/latest` URL and
`<exe>.last-update-check` file are simply replaced.

## Implementation

### 1. Channel and version rules (PR 2)

New module (e.g. `src/channel.ts`), imported by the CLI and the scripts.

**Channel names**

- Valid: `/^[a-z][a-z0-9-]*$/`, at most 40 characters.
- `release` is the default channel; its versions have no suffix.
- `dev` is reserved for builds run from source or built locally. It can never
  be published or followed: publishing to `cli-channel/dev`, `DH_CHANNEL=dev`
  and `dh channel use dev` are all errors.

**Tags**

| Kind | Format | Regex |
| --- | --- | --- |
| Release | `cli-vX.Y.Z` | `/^cli-v(0\|[1-9]\d*)\.(0\|[1-9]\d*)\.(0\|[1-9]\d*)$/` |
| Pre-release | `cli-vX.Y.Z-<channel>.<build>` | `/^cli-v((?:0\|[1-9]\d*)\.(?:0\|[1-9]\d*)\.(?:0\|[1-9]\d*))-([a-z][a-z0-9-]*)\.(0\|[1-9]\d*)$/` |
| Channel release | `cli-channel-<channel>` | — (never parsed as a version) |

- The pre-release regex splits on the **last** dot, so `alpha-1.0` is channel
  `alpha-1`, build `0`. Tags matching neither regex (including the Python
  `v*` tags) are ignored.
- The version is the tag minus `cli-v`. It is valid semver;
  `semver.parse("3.1.1-alpha-bar.0").prerelease` is `["alpha-bar", 0]`.

**Next version** (a pure function of: all tag names, `package.json` `version`,
channel):

1. `last` = the semver maximum of release tags, or `0.0.0` if none.
2. `pkg` = `package.json` `version`; it must be plain `X.Y.Z` (no pre-release),
   else error.
3. `base` = the higher of `semver.inc(last, "patch")` and `pkg`.
4. Channel `release` → `base`. Any other channel →
   `base-<channel>.<build>`, where `<build>` = 1 + the highest build among
   existing `cli-v<base>-<channel>.*` tags, or `0`.

Expose it as a script for the workflow: `bun run next-version <channel>` prints
the version (no `cli-v`, no trailing text) to stdout and exits 0; an invalid
channel or invalid `pkg` prints an error to stderr and exits 1.
It reads tags with `git tag --list 'cli-v*'`, so it needs a checkout with all
tags (see the workflow's `fetch-depth: 0`).

**Release-notes start tag** (a pure function of: all tag names, the tag being
published): for a release, the highest release tag below it; for a pre-release,
the highest tag of the same channel below it, falling back to the highest
release tag below its `X.Y.Z`. None → omit.

Next-version cases (from tag lists). Base says where `base` came from: `tag`
(last release + patch), `pkg` (`package.json`), or `err`:

| Tags | `package.json` | Base | Channel | Result | Shows |
| --- | --- | --- | --- | --- | --- |
| none | `3.0.0` | pkg | `release` | `3.0.0` | With no release tags, `package.json` is the first release |
| none | `3.0.0` | pkg | `rc` | `3.0.0-rc.0` | A channel's first build is `.0` |
| `cli-v3.1.0` | `3.1.0` | tag | `rc` | `3.1.1-rc.0` | `package.json` equal to the last release: base is last + patch |
| `cli-v3.1.0`, `cli-v3.1.1-rc.0` | `3.1.0` | tag | `rc` | `3.1.1-rc.1` | Build is 1 + the channel's highest build on that base |
| `cli-v3.1.0`, `cli-v3.1.1-rc.1` | `3.1.0` | tag | `alpha-bar` | `3.1.1-alpha-bar.0` | Build counts only this channel's tags: `rc.1` doesn't make this `alpha-bar.2` |
| `cli-v3.1.0`, `cli-v3.1.1-rc.1` | `3.1.0` | tag | `release` | `3.1.1` | Pre-release tags aren't releases, so they don't move the base: counting `3.1.1-rc.1` would give `3.1.2` |
| `cli-v3.1.1` | `3.1.0` | tag | `rc` | `3.1.2-rc.0` | A new release moves the base on; build restarts at `.0` |
| `cli-v3.1.1` | `3.2.0` | pkg | `rc` | `3.2.0-rc.0` | `package.json` above last + patch wins (a bump) |
| `cli-v3.2.0` | `3.2.0` | tag | `rc` | `3.2.1-rc.0` | Once the bump is released, patches resume |
| `cli-v3.2.0` | `3.1.0` | tag | `alpha-bar` | `3.2.1-alpha-bar.0` | `package.json` below the last release is ignored (old branch) |
| `v2.1.0`, `cli-vfoo`, `cli-v03.1.1-rc.0`, `cli-v3.1.0` | `3.1.0` | tag | `rc` | `3.1.1-rc.0` | Non-CLI, malformed and leading-zero tags are ignored: parsing `cli-v03.1.1-rc.0` would give `rc.1` |
| any | `3.2.0-beta` | err | any | error | `package.json` must be plain `X.Y.Z` |
| any | any | — | `Rc`, `dev`, `-x` | error | Invalid channel name (`Rc`, `-x`), or the reserved `dev` |

### 2. Publishing (PR 2)

**Build** (`scripts/build.ts`, `src/version.ts`)

- Bake the channel in with `--define DH_BUILD_CHANNEL=…`, beside
  `DH_BUILD_VERSION`. `src/version.ts` exports `CHANNEL`: the baked value, else
  `"dev"`.
- `build()` takes a `channel` option, default `"dev"`. Local
  `bun run build` therefore makes `dev` builds.

**`scripts/release.ts`**: takes a full tag (`cli-v3.1.1-rc.2`), derives version
and channel from it (`release` when there is no pre-release part), passes both
to `build()`, and drops the `package.json` version check. Base URLs stay
tagged (`releases/download/<tag>/`). Install scripts are still attached to every
versioned release.

**`scripts/publish.ts <tag> <dir>`**, run by the workflow with `GH_TOKEN`. Tags
and the branch-head check use `git` against the checkout's `origin`, so they can
be tested against a local bare repository; only release objects use `gh`:

1. Versioned tag: `git push origin $GITHUB_SHA:refs/tags/<tag>`.
2. Versioned release: `gh release create <tag> <dir>/* --verify-tag
   --latest=false --generate-notes [--notes-start-tag <start>]`, plus
   `--prerelease` unless the channel is `release`.
3. Newest-commit check: `git ls-remote origin refs/heads/cli-channel/<channel>`
   must name `$GITHUB_SHA`. If not, print
   `cli-channel/<channel> moved on; channel release unchanged` and exit 0.
4. Channel tag: `git push --force origin $GITHUB_SHA:refs/tags/cli-channel-<channel>`.
5. Channel release `cli-channel-<channel>`:
   - Missing: `gh release create cli-channel-<channel> --verify-tag
     --prerelease --latest=false --title "<title>" --notes "<notes>"`.
   - Exists: `gh release edit cli-channel-<channel> --title "<title>"`.
   - Upload: for channel `release` only, `install.sh` and `install.ps1` first;
     then always `manifest.json` last, each with `gh release upload … --clobber`.
   - `<title>`: `<channel> channel (currently <tag>)`, e.g.
     `rc channel (currently cli-v3.1.1-rc.2)`.
   - `<notes>`: `Moving release for the <channel> channel. Its tag and
     manifest.json are replaced on every publish. Do not delete.`

`git push` authentication: the checkout keeps no credentials
(`persist-credentials: false`), so when `GH_TOKEN` is set, `publish.ts` passes
it only to its own `git` commands, as an `http.https://github.com/.extraheader`
of `AUTHORIZATION: basic <base64 of x-access-token:$GH_TOKEN>` through
`GIT_CONFIG_COUNT`/`GIT_CONFIG_KEY_0`/`GIT_CONFIG_VALUE_0`. Without `GH_TOKEN`
(tests against a local bare repository) it adds nothing.

Notes:
- `gh release upload --clobber` deletes before it uploads, so the manifest URL
  can 404 for a moment. That is expected; the updater treats it as a failed
  check.
- GitHub dates a release by its tag's commit, so moving the tag keeps the
  channel release next to its current build (verified on a scratch repo).
- Tags pushed and releases made with `GITHUB_TOKEN` never start workflows (GitHub's
  protection against loops), so publishing can't retrigger itself. A future
  workflow that should react to new CLI tags or releases won't run for these
  without a GitHub App or personal token.

**`.github/workflows/release.yml`** (replace the current tag-triggered one):

- `on: push: branches: ["cli-channel/**"]`. No `workflow_dispatch`, no tag
  trigger.
- `concurrency: { group: "cli-release-${{ github.ref }}", cancel-in-progress: false }`.
  GitHub keeps only the newest pending run per group and cancels older pending
  ones, which is what we want: the newest commit is the one that should publish.
- Jobs: `ci` (reuse `ci.yml`), then `release` on `macos-latest` (darwin
  codesign), `timeout-minutes: 15`, `permissions: { contents: write,
  pull-requests: read }`. No `environment`: the job publishes as soon as the
  merge lands, with no deployment approval. Review happens on the PR (required
  on `release`).
- Steps:
  1. Checkout with `fetch-depth: 0` (all tags) and `persist-credentials: false`.
  2. Only on `cli-channel/release`, before anything is built or published:
     `bun scripts/release-check.ts origin "$GITHUB_SHA"`
     (`GH_TOKEN: ${{ github.token }}`). It reads
     `gh api repos/$GITHUB_REPOSITORY/commits/$GITHUB_SHA/pulls` and passes
     only if one is merged, based on `cli-channel/release`, has
     `merge_commit_sha` equal to the pushed commit, and comes from this repo's
     (`head.repo.full_name` equals `GITHUB_REPOSITORY`) `cli-channel/rc` or
     `hotfix/*`; otherwise it fails with
     `<sha> is not a merged PR from cli-channel/rc or hotfix/*; nothing published`.
     This catches accidental direct pushes, which bypass the PR checks.
  3. `CHANNEL=${GITHUB_REF_NAME#cli-channel/}`; fail if it isn't a valid,
     non-`dev` channel name.
  4. `VERSION=$(bun run next-version "$CHANNEL")`, `TAG=cli-v$VERSION`.
  5. `bun run release "$TAG" dist`.
  6. `bun run publish "$TAG" dist` with `GH_TOKEN: ${{ github.token }}`.

**`.github/workflows/release-checks.yml`** (new): on `pull_request` to
`cli-channel/release`, two jobs whose names are the required-check names. Both
run `scripts/release-check.ts`, which imports nothing, so the jobs only set up
Bun (no `bun install`); the workflow in `release.yml` uses it too:

- `release-source`: `bun scripts/release-check.ts source "$HEAD_REF" "$HEAD_REPO"`
  with `github.head_ref` and `github.event.pull_request.head.repo.full_name`;
  fails unless the head repo equals `GITHUB_REPOSITORY` and the ref is
  `cli-channel/rc` or starts with `hotfix/`. Error: `PRs into
  cli-channel/release must come from cli-channel/rc or a hotfix/* branch in
  this repo`. Comparing with `GITHUB_REPOSITORY` rather than a fixed name keeps
  the check working on a fork, for PRs within that fork.
- `release-contains`: checkout the PR head with `fetch-depth: 0`, then
  `bun scripts/release-check.ts contains`, which runs

  ```sh
  git log --no-merges --cherry-pick --right-only --format='%h %s' HEAD...origin/cli-channel/release
  ```

  and fails if the output is non-empty, printing the list and
  `Merge the hotfix into v3_rewrite, then v3_rewrite into cli-channel/rc.`
  This matches commits by patch; it doesn't catch a fix undone by a later
  conflict resolution, and isn't meant to.

### 3. Install scripts (PR 2)

**`install.sh` / `install.ps1`**

- `DH_CHANNEL`, default `release`, validated like a channel name (`dev`
  rejected) before any download.
  - Gotcha: macOS `sh` is bash, where `[a-z]` in a `case` pattern follows the
    locale and matches uppercase. Spell the letters out
    (`abcdefghijklmnopqrstuvwxyz`) so `Rc` is rejected.
- Without `DH_INSTALL_VERSION`: fetch
  `$REPO_URL/releases/download/cli-channel-$CHANNEL/manifest.json`. 404 → exit
  non-zero with `dh: no channel <channel> at <repo>`.
  - `sh`: read the version with
    `sed -n 's/^ *"version": *"\([^"]*\)".*/\1/p' | head -n 1` (build.ts writes
    the manifest with `JSON.stringify(m, null, 2)`, one key per line), set
    `TAG=cli-v$version`, then continue as today with the binary and
    `SHA256SUMS` from `releases/download/$TAG/`.
  - `ps1`: install from the manifest as today.
- `DH_INSTALL_VERSION` accepts `3.1.1`, `v3.1.1` or `cli-v3.1.1` (pre-releases
  too) and normalizes to `cli-v…`.
- After installing, always write the metadata file fresh:
  `{"channel":"<channel>"}` to `<dir>/dh.meta` (`dh.exe.meta` on Windows), via a
  temp file and rename.
  - Gotcha: in Windows PowerShell 5.1, `Set-Content -Encoding utf8` writes a
    BOM, which `JSON.parse` rejects. Use
    `[IO.File]::WriteAllText($path, $json, [Text.UTF8Encoding]::new($false))`.
- The header comments' one-liners become
  `…/releases/download/cli-channel-release/install.sh` (and `.ps1`).

### 4. CLI (PR 3)

**Install metadata file**

- Path: `process.execPath + ".meta"` (`dh.meta`, `dh.exe.meta`).
- Format: a JSON object. Keys used now: `channel` (string) and
  `lastUpdateCheck` (ISO 8601 string, from `new Date().toISOString()`). Unknown
  keys are kept on every write. The install scripts write it too, so this
  format is a contract with them.
- Missing, unparseable, not an object, or an invalid `channel` → treat as `{}`:
  channel `release`, check due. Never fail a command over it; log the reason
  under `DH_DEBUG`.
- Writes: hold `<path>.lock` (the `withFileLock` helper `Store.update()` uses)
  around the whole read, change and rename, so the updater's
  `lastUpdateCheck` write can't undo a concurrent `dh channel use`. Read,
  change only the caller's keys, write `<path>.<pid>.tmp`, rename over
  `<path>`. If the existing file was unreadable, write a fresh object. The
  auto-updater skips its write if the lock is busy instead of waiting;
  `dh channel use` waits. The install scripts write without the lock.

**Updater** (`src/updater.ts`)

- Builds with `CHANNEL === "dev"` never auto-update (like running from source
  today).
- Channel: `channel` from the metadata file, else `release`. `DH_CHANNEL` is
  not read.
- Manifest URL: `DH_UPDATE_URL` if non-empty (unchanged), else
  `https://github.com/<REPOSITORY>/releases/download/cli-channel-<channel>/manifest.json`.
- Throttle on `lastUpdateCheck` instead of `<exe>.last-update-check`, still
  written before fetching. `DH_UPDATE_INTERVAL` and `DH_AUTO_UPDATE=off` are
  unchanged.
- Install when `manifest.version !== VERSION`; `manifest.version` must still be
  valid semver (`semver.valid`), else throw. No higher/lower comparison.
- Replacing the executable: download outside the lock, then hold the metadata
  file's lock for the swap.
  - Auto-update: under the lock, re-read `channel` and replace the executable
    only if it still matches the channel the manifest came from; otherwise (a
    concurrent `dh channel use`) skip the swap and log it under `DH_DEBUG`. If
    the lock is busy, skip rather than wait. With `DH_UPDATE_URL` set, the
    manifest's channel is the saved one.
  - `dh channel use`: under the lock (waiting for it), save `channel` and
    replace the executable together.
  - `channel` and the executable only change under that lock, and a failed
    download changes neither, so the installed build always belongs to the
    saved channel.
- Expose a forced check for `dh channel use`: ignores the throttle and
  `DH_AUTO_UPDATE`, reports `{ from, to }` or "already current", throws on
  failure.

**Per-channel config** (`configDir()` in `src/actions/auth/store.ts`)

- `DH_CONFIG_DIR` if set (unchanged).
- Else the base folder (`~/.deephaven`, or `%APPDATA%\Deephaven` on Windows)
  plus `cli` when `CHANNEL === "release"`, else `cli-<CHANNEL>`
  (`cli-rc`, `cli-alpha-bar`, `cli-dev`).
- No copying between folders.

**`dh channel`** (new group, `src/commands/channel/`)

- `dh channel`: lists channels, like `dh auth` lists profiles.
  - Rows: `release`, then `rc`, then the followed channel if it is neither.
    No other channel is ever listed, and the releases API is never called:
    alpha channels must not be advertised to people or agents.
  - Versions: fetch each row's channel manifest (the updater's
    `cli-channel-<channel>/manifest.json` URL, ignoring `DH_UPDATE_URL`) in
    parallel, with the updater's manifest timeout. 404 → `not published`; any
    other failure → `?`.
  - Human: one line per row, `● ` (green, as in `dh auth`) on the followed
    channel and two spaces otherwise, the name padded to the longest, three
    spaces, then the version. If any row shows `?`, add
    `! Couldn't check <names, comma-separated>.` If the running build's
    `CHANNEL` differs from the followed channel, add
    `! Running <VERSION> from the <CHANNEL> channel, not <channel>.`
    (`…, not <channel>; auto-update is off.` with `DH_AUTO_UPDATE=off`) and the
    indented hint `` Run `dh channel use <channel>` to install <channel>'s build now. ``
    Always exits 0.
  - On a `dev` build: `dev (never updates)`, without reading the metadata file
    or fetching anything.
  - JSON: `{ "channels": [{ "channel": "release", "version": "3.1.1", "following": true }, …], "build": "<CHANNEL>", "version": "<VERSION>" }`,
    rows in the human order; `version` is `null` for `not published` and `?`.
    On a dev build, `channels` is `[]`.
  - Example, following `alpha-bar`:

    ```
      release     3.1.1
      rc          3.2.0-rc.3
    ● alpha-bar   3.2.1-alpha-bar.0
    ```

  - Help and the `dh agents` manifest may describe `alpha-<id>` channels, but
    never name a specific one; example channels are `release` and `rc`.
- `dh channel use <name>`:
  1. Invalid name, or the reserved name `dev` → `usage` error.
  2. A dev build (`CHANNEL === "dev"`: from source or a local `bun run build`)
     → `usage` error `dh channel use needs an installed dh`, with a hint to
     install it. Dev builds are never replaced, like auto-update.
  3. Fetch the channel's manifest. 404 → new error code `channel_not_found`
     (exit 2, "No published build on that channel"); network failure →
     `server_unreachable`. Nothing is saved on failure.
  4. If the version differs, download and verify the build; a failure saves
     nothing. Then, under the metadata lock, save `channel` and replace the
     executable together (just save `channel` if the version is the same).
  5. Human: `✔ Following rc`, then an indented `Updated 3.1.0 → 3.1.1-rc.2`
     or `Already on 3.1.1-rc.2`. Auto-update prints `Updated dh 3.1.0 → 3.1.1`.
     JSON: `{ "channel": "rc", "from": "3.1.0", "to": "3.1.1-rc.2" }`
     (`from` equals `to` when already current).
- Add `channel` to the root command's `commands`; `channel_not_found` goes in
  `src/errors.ts` so `dh agents` lists it.

**Help and root output**

- In `src/main.ts`'s `formatHelp`, after the `Version:` line, when
  `CHANNEL !== "release"`: `Channel: <CHANNEL> (config <configDir()>)`, with
  the home directory shown as `~`.
- Root command JSON gains `"channel": "<CHANNEL>"`.

### Repo setup

Manual, by a repo admin; the Developer Guide records it. Do it on the real repo
only after PR 3 merges: builds published before then lack per-channel config,
so an `rc` build would share the `release` config folder. Until then, publish
only to a fork.

- Create `cli-channel/release` and `cli-channel/rc` from `v3_rewrite`.
- Branch ruleset on `cli-channel/**`: block force pushes and deletion; require
  a pull request with 0 approvals and allow only merge commits (no squash or
  rebase).
- Branch ruleset on `cli-channel/release`: require a pull request with an
  approving review, and require the checks `release-source` and
  `release-contains`.
- Tag ruleset on `cli-v*`: block updates and deletion.
- Tag ruleset on `cli-channel-*`: block updates and deletion, with GitHub
  Actions as the only bypass actor. Verify on a fork that the workflow can
  still move the tag; if it can't, use a GitHub App token for that step.
- Delete the old `release` environment; the workflow no longer references it.

## Stack

| # | PR | Contents |
| --- | --- | --- |
| 1 | Design and surface | Release architecture, user docs, this plan |
| 2 | Publishing | Sections 1–3: channel rules, `next-version`, build channel, `release.ts`, `publish.ts`, `release-check.ts`, `release.yml`, `release-checks.yml`, install scripts |
| 3 | CLI | Section 4: `dh.meta`, updater, per-channel config, `dh channel`, help line |

## Out of scope

- A pin saved in `dh.meta`; pinning is `DH_INSTALL_VERSION` plus
  `DH_AUTO_UPDATE=off`.
- A `dh update` command; `dh channel use <channel>` updates on demand.
- Markdown linting for the v3 docs.

## Done when

Each item is a behavior the implementation must show. IDs (`channels-<pr>-<n>`)
are stable so tests can reference them: numbers are append-only, and a removed
or moved item's ID isn't reused.

Each PR's group ends with a Proof table: where a reviewer can confirm each
item without reading the implementation. Tests carry their item's ID in their
name, so `bun test -t channels-<pr>` runs them all.

### PR 2: Publishing

- [x] **channels-pub-1** Next versions match the next-version table in section
      1, and tags parse as in Tags: `cli-v3.1.1-alpha-1.0` is channel `alpha-1`,
      build `0`; the same tag with a leading zero, `cli-v03.1.1-alpha-1.0` or
      `cli-v3.01.1-alpha-1.0`, is ignored (not valid semver).
- [x] **channels-pub-2** Release notes start where Release-notes start tag
      (section 1) says: at `cli-v3.1.0` for `cli-v3.1.1`, and for
      `cli-v3.1.1-rc.0` when only `cli-v3.1.0` exists; at `cli-v3.1.1-rc.0` for
      `cli-v3.1.1-rc.1`.
- [x] **channels-pub-3** A publish creates the versioned tag and release first.
      If the branch has moved on, it stops there; otherwise it moves
      `cli-channel-<channel>` to the commit, creates or retitles that release,
      and uploads `manifest.json` last (after the install scripts on `release`).
- [x] **channels-pub-4** `cli-channel/release` publishes only the merge of a PR
      from this repo's `cli-channel/rc` or `hotfix/*`; any other commit,
      including a merged PR from another repo's branch of the same name,
      publishes nothing.
- [x] **channels-pub-5** `release-source` passes only PRs from this repo's
      `cli-channel/rc` or `hotfix/*`, so a PR from a fork's `cli-channel/rc`
      fails. `release-contains` fails while a change already on
      `release` is missing from the PR's branch, such as a hotfix not yet merged
      through `v3_rewrite` into `rc`, or an earlier hotfix missing from a later
      hotfix's branch, and passes once it's there.
- [x] **channels-pub-6** The install scripts (`install.sh`, `install.ps1`) match
      Installing on a channel in `docs/CHANNELS.md`: `release` by default;
      `DH_CHANNEL` picks the channel and is saved in `dh.meta`; an unpublished
      channel fails with `no channel`; an invalid name or `dev` fails before any
      download; `DH_INSTALL_VERSION` accepts `3.1.1`, `v3.1.1` and `cli-v3.1.1`.
- [ ] **channels-pub-7** (Manual) Publishing works end to end on a fork with the
      repo setup applied, with every PR made within the fork (GitHub's new-PR
      page defaults the base to the upstream repo; switch it to the fork):
  1. Merge a PR into `cli-channel/rc` → `cli-v<x>-rc.0` released as a
     pre-release, not latest; `cli-channel-rc` exists, titled
     `rc channel (currently …)`, its tag on the merge commit.
  2. Merge another → `rc.1`; `cli-channel-rc` moved to the new commit and sits
     next to `rc.1` on the Releases page. This proves the `cli-channel-*` tag
     ruleset lets the workflow move the tag; if the push is rejected, switch
     that step to a GitHub App token (see Repo setup).
  3. PR `cli-channel/rc` → `cli-channel/release`: both checks pass; merge
     → `cli-v<x>` released with no deployment approval, `cli-channel-release`
     carries the install scripts.
  4. A PR from any other branch, or from another repo's `cli-channel/rc`,
     fails `release-source`. A hotfix PR into
     release passes; the next rc PR fails `release-contains` until the fix is
     merged into `v3_rewrite` and then `rc`. A second hotfix branched before
     the first reached `v3_rewrite` fails `release-contains`; branched after,
     it passes.
  5. An admin's direct push to `cli-channel/release` fails the release job's
     PR check and publishes nothing.
  6. The repo's `releases/latest` never changes.
- [x] **channels-pub-8** The docs match the implementation.
- [ ] **channels-pub-9** `bun run check` passes; `bun test` passes on Linux,
      macOS and Windows.

#### Proof

| Item | Proven by |
| --- | --- |
| pub-1 | `tests/channel.test.ts`: `[channels-pub-1]` parseTag, nextVersion, next-version script |
| pub-2 | `tests/channel.test.ts`: `[channels-pub-2]` notesStartTag |
| pub-3 | `tests/publish.test.ts`: `[channels-pub-3]` publish (Unix only) |
| pub-4 | `tests/release_check.test.ts`: `[channels-pub-4]` release origin (Unix only). That `release.yml` runs this check before publishing is shown by pub-7 step 5. |
| pub-5 | `tests/release_check.test.ts`: `[channels-pub-5]` release-source; release-contains (Unix only) |
| pub-6 | `tests/auto_update.test.ts`: `[channels-pub-6]`; runs `install.sh` on Linux and macOS, `install.ps1` on Windows |
| pub-7 | Manual, on a fork with the repo setup applied; record the results in the PR. Step 2 also proves the `cli-channel-*` tag ruleset's Actions bypass. |
| pub-8 | Review `docs/CHANNELS.md` and `docs/RELEASE_ARCHITECTURE.md` against the code |
| pub-9 | CI: the `check` job and the `test` job's ubuntu, macos and windows runs (`.github/workflows/ci.yml`) |

Unix only: these tests use a fake `gh` written in sh and are skipped on
Windows.

### PR 3: CLI

- [ ] **channels-cli-1** Auto-update matches How the CLI updates in
      `docs/RELEASE_ARCHITECTURE.md`: it follows the channel in `dh.meta`, else
      `release`; with `DH_UPDATE_URL` set, installs from that manifest and leaves
      `dh.meta`'s channel unchanged; installs whenever the version differs,
      lower included; treats a
      bad `dh.meta` as `release`; throttles on `lastUpdateCheck`; re-checks the
      channel under the `dh.meta` lock before replacing the executable (unit
      tests: a stubbed download writes `{"channel":"rc"}` to `dh.meta` partway
      through a `release` update, and the executable isn't replaced; with the
      lock held by the test, the swap is skipped, not waited on); and never runs
      on a dev build.
- [ ] **channels-cli-2** `dh channel` and `dh channel use` match
      `docs/CHANNELS.md`: output, JSON and errors (`usage`, `channel_not_found`,
      `server_unreachable`).
      - `dh channel` lists `release` and `rc` with their manifests' versions
        and adds the followed channel only when it is neither (an install
        following `alpha-bar` shows three rows; one following `rc` shows two);
        an unpublished channel shows `not published` and an unreachable one `?`
        with the note, and the command still exits 0; `DH_UPDATE_URL` doesn't
        change the listed versions.
      - `dh channel use`: a failed manifest fetch or a failed download leaves
        `dh.meta` and the executable unchanged (unit test: a stubbed download
        fails, and `dh.meta` still has the old channel); dev builds refuse it.
      - No help text or `dh agents` entry names a specific alpha channel
        (nothing matches `/alpha-[a-z0-9]/`; the `alpha-<id>` placeholder is
        fine).
- [ ] **channels-cli-3** Config folders match Config per channel in
      `docs/RELEASE_ARCHITECTURE.md`: `cli` on `release`, `cli-<channel>` on
      other channels, `cli-dev` on dev builds; `DH_CONFIG_DIR` overrides all of
      them.
- [ ] **channels-cli-4** Non-release builds show `Channel: <channel> (config
      <dir>)` in `dh --help`; `dh -o json` includes `channel`; `dh agents` lists
      `channel`, `channel use` and `channel_not_found`.
- [ ] **channels-cli-5** (Manual) Installing and updating work end to end on the
      fork from channels-pub-7:
  1. Install with `DH_CHANNEL=rc`; `dh --help` shows `Channel: rc`; merge
     another PR into rc and `DH_UPDATE_INTERVAL=0 dh` updates to the new build.
  2. `dh channel use release` on the rc install switches immediately.
- [ ] **channels-cli-6** (Manual) Repo setup is applied to the real repo.
- [ ] **channels-cli-7** The docs match the implementation.
- [ ] **channels-cli-8** `bun run check` passes; `bun test` passes on Linux,
      macOS and Windows.
