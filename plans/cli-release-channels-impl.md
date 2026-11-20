# CLI release channels: implementation

Implements the design in [cli-release-channels.md](cli-release-channels.md):
read that first for the why. This file is the contract: names, formats and
behavior here are exact; internal function and module names are up to the
implementer, matching existing patterns. If every Validation item passes, the
implementation is correct.

Four phases, each shippable on its own and committed separately:

1. Channel and version rules (pure code, no I/O)
2. Publishing: build, workflow, required checks
3. CLI: install metadata, updater, per-channel config, `dh channel`, help line
4. Install scripts and docs

Repo setup (branches, rulesets, environments) is manual; see the last section.
No CLI has shipped yet, so nothing needs migrating: the old
`releases/latest` URL and `<exe>.last-update-check` file are simply replaced.

## 1. Channel and version rules

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
| Prerelease | `cli-vX.Y.Z-<channel>.<build>` | `/^cli-v(\d+\.\d+\.\d+)-([a-z][a-z0-9-]*)\.(0\|[1-9]\d*)$/` |
| Channel release | `cli-channel-<channel>` | — (never parsed as a version) |

- The prerelease regex splits on the **last** dot, so `alpha-1.0` is channel
  `alpha-1`, build `0`. Tags matching neither regex (including the Python
  `v*` tags) are ignored.
- The version is the tag minus `cli-v`. It is valid semver;
  `semver.parse("3.1.1-alpha-bar.0").prerelease` is `["alpha-bar", 0]`.

**Next version** (a pure function of: all tag names, `package.json` `version`,
channel):

1. `last` = the semver maximum of release tags, or `0.0.0` if none.
2. `pkg` = `package.json` `version`; it must be plain `X.Y.Z` (no prerelease),
   else error.
3. `base` = the higher of `semver.inc(last, "patch")` and `pkg`.
4. Channel `release` → `base`. Any other channel →
   `base-<channel>.<build>`, where `<build>` = 1 + the highest build among
   existing `cli-v<base>-<channel>.*` tags, or `0`.
5. If the resulting `cli-v…` tag already exists, error.

Expose it as a script for the workflow: `bun run next-version <channel>` prints
the version (no `cli-v`, no trailing text) to stdout and exits 0; invalid
channel, invalid `pkg` or an existing tag print an error to stderr and exit 1.
It reads tags with `git tag --list 'cli-v*'`, so it needs a checkout with all
tags (see the workflow's `fetch-depth: 0`).

**Release-notes start tag** (also pure): for a release, the highest release tag
below it; for a prerelease, the highest tag of the same channel below it,
falling back to the highest release tag below its `X.Y.Z`. None → omit.

### Checklist

- [x] Channel name validation, with `dev` reserved
- [x] Tag parse and format for release and prerelease tags
- [x] Next-version computation and the `next-version` script
- [x] Release-notes start tag
- [x] Unit tests (`tests/channel.test.ts`)

## 2. Publishing

**Build** (`scripts/build.ts`, `src/version.ts`)

- Bake the channel in with `--define DH_BUILD_CHANNEL=…`, beside
  `DH_BUILD_VERSION`. `src/version.ts` exports `CHANNEL`: the baked value, else
  `"dev"`.
- `build()` takes a `channel` option, default `"dev"`. Local
  `bun run build` therefore makes `dev` builds.

**`scripts/release.ts`**: takes a full tag (`cli-v3.1.1-rc.2`), derives version
and channel from it (`release` when there is no prerelease part), passes both
to `build()`, and drops the `package.json` version check. Base URLs stay
tagged (`releases/download/<tag>/`). Install scripts are still attached to every
versioned release.

**`scripts/publish.ts <tag> <dir>`**, run by the workflow with `GH_TOKEN`:

1. `gh release create <tag> <dir>/* --target $GITHUB_SHA --latest=false
   --generate-notes [--notes-start-tag <start>]`, plus `--prerelease` unless the
   channel is `release`.
2. Newest-commit check:
   `gh api repos/$GITHUB_REPOSITORY/git/ref/heads/cli-channel/<channel> --jq .object.sha`
   must equal `$GITHUB_SHA`. If not, print
   `cli-channel/<channel> moved on; channel release unchanged` and exit 0.
3. Channel release `cli-channel-<channel>`:
   - Missing: `gh release create cli-channel-<channel> --target $GITHUB_SHA
     --prerelease --latest=false --title "<title>" --notes "<notes>"`.
   - Exists: move its tag with
     `gh api -X PATCH repos/$GITHUB_REPOSITORY/git/refs/tags/cli-channel-<channel> -f sha=$GITHUB_SHA -F force=true`,
     then `gh release edit cli-channel-<channel> --title "<title>"`.
   - Upload: for channel `release` only, `install.sh` and `install.ps1` first;
     then always `manifest.json` last, each with `gh release upload … --clobber`.
   - `<title>`: `<channel> channel (currently <tag>)`, e.g.
     `rc channel (currently cli-v3.1.1-rc.2)`.
   - `<notes>`: `Moving release for the <channel> channel. Its tag and
     manifest.json are replaced on every publish. Do not delete.`

Gotchas:
- `gh release upload --clobber` deletes before it uploads, so the manifest URL
  can 404 for a moment. That is expected; the updater treats it as a failed
  check.
- GitHub dates a release by its tag's commit, so moving the tag keeps the
  channel release next to its current build (verified on a scratch repo).
- Tags created or moved with `GITHUB_TOKEN` never trigger workflows, and this
  workflow triggers on branches anyway.

**`.github/workflows/release.yml`** (replace the current tag-triggered one):

- `on: push: branches: ["cli-channel/**"]`. No `workflow_dispatch`, no tag
  trigger.
- `concurrency: { group: "cli-release-${{ github.ref }}", cancel-in-progress: false }`.
  Gotcha: GitHub keeps only the newest pending run per group and cancels older
  pending ones. That is fine: the newest commit is the one that should publish.
- Jobs: `ci` (reuse `ci.yml`), then `release` on `macos-latest` (darwin
  codesign), `timeout-minutes: 15`, `permissions: contents: write`,
  `environment: ${{ github.ref_name == 'cli-channel/release' && 'cli-release' || 'cli-prerelease' }}`.
- Steps:
  1. Checkout with `fetch-depth: 0` (all tags) and `persist-credentials: false`.
  2. `CHANNEL=${GITHUB_REF_NAME#cli-channel/}`; fail if it isn't a valid,
     non-`dev` channel name.
  3. `VERSION=$(bun run next-version "$CHANNEL")`, `TAG=cli-v$VERSION`.
  4. `bun run release "$TAG" dist`.
  5. `bun run publish "$TAG" dist` with `GH_TOKEN: ${{ github.token }}`.

**`.github/workflows/release-checks.yml`** (new): on `pull_request` to
`cli-channel/release`, two jobs whose names are the required-check names:

- `release-source`: fail unless `github.head_ref` is `cli-channel/rc` or starts
  with `hotfix/`. Error: `PRs into cli-channel/release must come from
  cli-channel/rc or a hotfix/* branch`.
- `release-contains`: checkout the PR head with `fetch-depth: 0`, then

  ```sh
  missing=$(git log --no-merges --cherry-pick --right-only --format='%h %s' HEAD...origin/cli-channel/release)
  ```

  and fail if it is non-empty, printing the list and
  `Merge the hotfix into v3_rewrite, then v3_rewrite into cli-channel/rc.`

### Checklist

- [x] `CHANNEL` baked into builds; `build()` channel option
- [x] `scripts/release.ts` takes the version and channel from the tag
- [x] `scripts/publish.ts` and `package.json` scripts `next-version` and `publish`
- [x] `release.yml` rewritten as above
- [x] `release-checks.yml` with `release-source` and `release-contains`

## 3. CLI

**Install metadata file**

- Path: `process.execPath + ".meta"` (`dh.meta`, `dh.exe.meta`).
- Format: a JSON object. Keys used now: `channel` (string) and
  `lastUpdateCheck` (ISO 8601 string, from `new Date().toISOString()`). Unknown
  keys are kept on every write. The install scripts write it too, so this
  format is a contract with them.
- Missing, unparseable, not an object, or an invalid `channel` → treat as `{}`:
  channel `release`, check due. Never fail a command over it; log the reason
  under `DH_DEBUG`.
- Writes: read, change only the caller's keys, write
  `<path>.<pid>.tmp`, rename over `<path>`. If the existing file was unreadable,
  write a fresh object.

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

- `dh channel`: shows the followed channel.
  - Human: `rc (saved)` or `release (default)`. If the running build's
    `CHANNEL` differs, add a line:
    `! Running the <CHANNEL> build; the <channel> build installs at the next update.`
    On a `dev` build the line is `! Running a dev build, which never updates.`
  - JSON: `{ "channel": "rc", "source": "saved" | "default", "build": "<CHANNEL>" }`.
- `dh channel use <name>`:
  1. Invalid name or `dev` → `usage` error.
  2. Running from source (`STANDALONE` false) → `usage` error:
     `dh channel use needs an installed dh` with hint to install it.
  3. Fetch the channel's manifest. 404 → new error code `channel_not_found`
     (exit 2, "No published build on that channel"); network failure →
     `server_unreachable`. Nothing is saved on failure.
  4. Save `channel` in the metadata file, then install if the version differs.
  5. Human: `✔ Following rc; updated 3.1.0 → 3.1.1-rc.2` or
     `✔ Following rc; already on 3.1.1-rc.2`.
     JSON: `{ "channel": "rc", "from": "3.1.0", "to": "3.1.1-rc.2" }`
     (`from` equals `to` when already current).
- Add `channel` to the root command's `commands`; `channel_not_found` goes in
  `src/errors.ts` so `dh agents` lists it.

**Help and root output**

- In `src/main.ts`'s `formatHelp`, after the `Version:` line, when
  `CHANNEL !== "release"`: `Channel: <CHANNEL> (config <configDir()>)`, with
  the home directory shown as `~`.
- Root command JSON gains `"channel": "<CHANNEL>"`.

### Checklist

- [x] Install metadata file read/write, atomic, tolerant of bad files
- [x] Updater: channel URL, metadata throttle, version-differs install, no
      updates for `dev` builds, forced check
- [x] `configDir()` per build channel
- [x] `dh channel` and `dh channel use`, `channel_not_found` error code
- [x] `Channel:` help line and root JSON `channel`
- [x] Tests updated and added (see Relevant Files)

## 4. Install scripts and docs

**`install.sh` / `install.ps1`**

- `DH_CHANNEL`, default `release`, validated like a channel name (`dev`
  rejected) before any download.
- Without `DH_INSTALL_VERSION`: fetch
  `$REPO_URL/releases/download/cli-channel-$CHANNEL/manifest.json`. 404 → exit
  non-zero with `dh: no channel <channel> at <repo>`.
  - `sh`: read the version with
    `sed -n 's/^ *"version": *"\([^"]*\)".*/\1/p' | head -n 1` (build.ts writes
    the manifest with `JSON.stringify(m, null, 2)`, one key per line), set
    `TAG=cli-v$version`, then continue as today with the binary and
    `SHA256SUMS` from `releases/download/$TAG/`.
  - `ps1`: install from the manifest as today.
- `DH_INSTALL_VERSION` accepts `3.1.1`, `v3.1.1` or `cli-v3.1.1` (prereleases
  too) and normalizes to `cli-v…`.
- After installing, always write the metadata file fresh:
  `{"channel":"<channel>"}` to `<dir>/dh.meta` (`dh.exe.meta` on Windows), via a
  temp file and rename.
  - Gotcha: in Windows PowerShell 5.1, `Set-Content -Encoding utf8` writes a
    BOM, which `JSON.parse` rejects. Use
    `[IO.File]::WriteAllText($path, $json, [Text.UTF8Encoding]::new($false))`.
- The header comments' one-liners become
  `…/releases/download/cli-channel-release/install.sh` (and `.ps1`).

**Docs**

- `README.md`: install one-liners via `cli-channel-release`; installing on a
  channel (`DH_CHANNEL`); `dh channel` / `dh channel use`; the auto-update table
  (`DH_UPDATE_URL` default is now the channel release); `DH_INSTALL_VERSION`
  examples with `cli-v` tags.
- `docs/DEVELOPER_GUIDE.md`: "How auto-update works" (channel release URL,
  `dh.meta`, version-differs rule, `dev` builds never update); "Releasing"
  rewritten for channel branches, promotion, hotfixes and `package.json` bumps;
  "Testing on a fork" with `cli-channel/*` branches; remove the note about Python
  releases needing `--latest=false`.
- `docs/AUTH.md`: config folder per channel (`cli`, `cli-<channel>`,
  `cli-dev`) wherever `~/.deephaven/cli` is described.

### Checklist

- [ ] `install.sh`: `DH_CHANNEL`, channel manifest, `cli-v` versions, metadata file
- [ ] `install.ps1`: same
- [ ] README, DEVELOPER_GUIDE and AUTH docs updated

## Repo setup (manual, by a repo admin)

Not code; record it in the DEVELOPER_GUIDE's Releasing section.

- [ ] Create `cli-channel/release` and `cli-channel/rc` from `v3_rewrite`.
- [ ] Branch ruleset on `cli-channel/**`: block force pushes and deletion.
- [ ] Branch ruleset on `cli-channel/release`: require a pull request, allow
      only merge commits, require checks `release-source` and
      `release-contains`.
- [ ] Tag ruleset on `cli-v*`: block updates and deletion.
- [ ] Tag ruleset on `cli-channel-*`: block updates and deletion, with GitHub
      Actions as the only bypass actor. Verify on a fork that the workflow can
      still move the tag; if it can't, use a GitHub App token for that step.
- [ ] Environments: `cli-release` (required reviewers) and `cli-prerelease`
      (none).
- [ ] Delete the old `release` environment once nothing references it.

## Validation

- [ ] `bun run check` passes.
- [ ] `bun test` passes on Linux, macOS and Windows (CI).
- [ ] Next-version table (unit tests, from tag lists):

  | Tags | `package.json` | Channel | Result |
  | --- | --- | --- | --- |
  | none | `3.0.0` | `release` | `3.0.0` |
  | none | `3.0.0` | `rc` | `3.0.0-rc.0` |
  | `cli-v3.1.0` | `3.1.0` | `rc` | `3.1.1-rc.0` |
  | `cli-v3.1.0`, `cli-v3.1.1-rc.0` | `3.1.0` | `rc` | `3.1.1-rc.1` |
  | `cli-v3.1.0`, `cli-v3.1.1-rc.1` | `3.1.0` | `alpha-bar` | `3.1.1-alpha-bar.0` |
  | `cli-v3.1.0`, `cli-v3.1.1-rc.1` | `3.1.0` | `release` | `3.1.1` |
  | `cli-v3.1.1` | `3.1.0` | `rc` | `3.1.2-rc.0` |
  | `cli-v3.1.1` | `3.2.0` | `rc` | `3.2.0-rc.0` |
  | `cli-v3.2.0` | `3.2.0` | `rc` | `3.2.1-rc.0` |
  | `v2.1.0`, `cli-vfoo`, `cli-v3.1.0` | `3.1.0` | `rc` | `3.1.1-rc.0` (others ignored) |
  | any | `3.2.0-beta` | any | error |
  | any | any | `Rc`, `dev`, `-x` | error |

- [ ] Tag parsing: `cli-v3.1.1-alpha-1.0` → version `3.1.1-alpha-1.0`, channel
      `alpha-1`, build `0`.
- [ ] Notes start tag: `cli-v3.1.1` after `cli-v3.1.0` → `cli-v3.1.0`;
      `cli-v3.1.1-rc.1` after `cli-v3.1.1-rc.0` → `cli-v3.1.1-rc.0`;
      `cli-v3.1.1-rc.0` with only `cli-v3.1.0` → `cli-v3.1.0`.
- [ ] Updater (fake GitHub in `tests/auto_update.test.ts`):
  - Default fetch is `/releases/download/cli-channel-release/manifest.json`;
    with `dh.meta` `{"channel":"rc"}` it is `cli-channel-rc`.
  - A manifest with a **lower** version installs; an equal one doesn't.
  - An unreadable `dh.meta` behaves as `release` and is rewritten.
  - `lastUpdateCheck` throttles; `DH_UPDATE_INTERVAL=0` doesn't.
  - A `dev` build never fetches.
- [ ] Installer (fake GitHub): default installs from `cli-channel-release` and
      writes `dh.meta` `{"channel":"release"}`; `DH_CHANNEL=rc` installs the rc
      manifest's version and writes `rc`; `DH_CHANNEL=nope` fails with
      `no channel`; `DH_CHANNEL=dev` and `DH_CHANNEL=Rc` fail before any
      request; `DH_INSTALL_VERSION` works as `9.9.9` and `cli-v9.9.9`.
- [ ] CLI (`tests/cli.test.ts`, from source so `CHANNEL` is `dev`):
  - `dh --help` contains `Channel: dev (config `.
  - `dh -o json` includes `"channel":"dev"`.
  - `dh channel` prints `release (default)`.
  - `dh channel use rc` fails with `usage` (not installed).
  - `dh agents -o json` lists `channel`, `channel use` and `channel_not_found`.
- [ ] `configDir()` (`tests/auth/store.test.ts`): `release` → `…/cli`, `rc` →
      `…/cli-rc`, `dev` → `…/cli-dev`, `DH_CONFIG_DIR` wins.
- [ ] End to end on a fork with the repo setup applied:
  1. Push to `cli-channel/rc` → `cli-v<x>-rc.0` released as a prerelease, not
     latest; `cli-channel-rc` exists, titled `rc channel (currently …)`, its tag
     on the pushed commit.
  2. Push again → `rc.1`; `cli-channel-rc` moved to the new commit and sits
     next to `rc.1` on the Releases page.
  3. Install with `DH_CHANNEL=rc`; `dh --help` shows `Channel: rc`; push to rc
     again and `DH_UPDATE_INTERVAL=0 dh` updates to the new build.
  4. PR `cli-channel/rc` → `cli-channel/release`: both checks pass; merge
     → `cli-v<x>` released (needs `cli-release` approval), `cli-channel-release`
     carries the install scripts.
  5. A PR from any other branch fails `release-source`. A hotfix PR into
     release passes; the next rc PR fails `release-contains` until the fix is
     merged into `v3_rewrite` and then `rc`.
  6. `dh channel use release` on the rc install switches immediately; the
     repo's `releases/latest` never changes.

## Relevant Files

- `src/channel.ts` (new): channel names, tags, next version, notes start tag
- `src/version.ts`: `CHANNEL`
- `src/updater.ts`
- install metadata module (new, e.g. `src/install-meta.ts`)
- `src/actions/auth/store.ts`: `configDir()`
- `src/main.ts`: `Channel:` help line
- `src/commands/root.ts`: `channel` command, JSON `channel`
- `src/commands/channel/index.ts`, `src/commands/channel/use.ts` (new)
- `src/errors.ts`: `channel_not_found`
- `scripts/build.ts`, `scripts/release.ts`
- `scripts/next-version.ts`, `scripts/publish.ts` (new)
- `package.json`: `next-version` and `publish` scripts
- `.github/workflows/release.yml`
- `.github/workflows/release-checks.yml` (new)
- `install.sh`, `install.ps1`
- `tests/channel.test.ts` (new)
- `tests/auto_update.test.ts`, `tests/cli.test.ts`, `tests/auth/store.test.ts`
- `README.md`, `docs/DEVELOPER_GUIDE.md`, `docs/AUTH.md`
- `plans/cli-release-channels.md`: update only if a contract here changes
