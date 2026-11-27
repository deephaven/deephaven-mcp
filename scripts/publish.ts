/**
 * Publishes a built release from CI.
 *
 *   bun run publish <tag> <dir>
 *
 *   <tag>  `cli-vX.Y.Z` or `cli-vX.Y.Z-<channel>.<build>`
 *   <dir>  the output of `bun run release <tag> <dir>`
 *
 * 1. Creates the `<tag>` release attaching every file in `<dir>`. GitHub
 *    creates the `<tag>` tag itself, on `GITHUB_SHA`, as part of the release.
 * 2. Moves the `cli-channel-<channel>` release to this build, only if
 *    `GITHUB_SHA` is still the head of `cli-channel/<channel>`:
 *    - force-pushes the `cli-channel-<channel>` tag to `GITHUB_SHA`
 *    - retitles the release, or creates it on the channel's first publish
 *    - replaces its `manifest.json` (plus `install.sh` and `install.ps1` on
 *      `release`)
 *
 * For example, publishing `cli-v3.1.0-rc.2` creates its versioned release:
 *
 *   cli-v3.1.0-rc.2    <Pre-release>
 *   tag: cli-v3.1.0-rc.2
 *   ----
 *
 *   <generated notes>
 *
 *   Assets: dh-linux-x64, dh-linux-arm64, dh-darwin-x64, dh-darwin-arm64,
 *   dh-windows-x64.exe, manifest.json, SHA256SUMS, install.sh, install.ps1
 *
 * and updates the `rc` channel release:
 *
 *   rc channel (cli-v3.1.0-rc.2)    <Pre-release>
 *   tag: cli-channel-rc
 *   ----
 *
 *   Always points at the most recent rc build on the cli-channel/rc branch.
 *   Its tag and manifest.json are replaced on every publish. Do not delete.
 *
 *   Assets: manifest.json
 *
 * The channel tag and the branch check use `git` against the checkout's
 * `origin`; releases (and the versioned tag) use `gh`. Needs
 * `GITHUB_REPOSITORY` and `GITHUB_SHA`, and `GH_TOKEN` on CI (also used to
 * authenticate `git push`).
 */
import { readdir } from "node:fs/promises";
import { join, resolve } from "node:path";
import { notesStartTag, parseTag, RELEASE_CHANNEL } from "../src/channel.ts";

const [tag, dir] = process.argv.slice(2);
const parsed = tag ? parseTag(tag) : undefined;
if (!tag || !parsed || !dir) {
  console.error("usage: bun run publish <tag> <dir>");
  process.exit(1);
}
const {
  GITHUB_REPOSITORY: repo,
  GITHUB_SHA: sha,
  GH_TOKEN: token,
} = process.env;
if (!repo || !sha) {
  console.error("GITHUB_REPOSITORY and GITHUB_SHA must be set");
  process.exit(1);
}
const outDir = resolve(dir);
const { channel } = parsed;

/**
 * Auth for this script's own `git` commands only: the checkout keeps no
 * credentials (`persist-credentials: false`), and nothing is written to config.
 */
const gitEnv: Record<string, string> = token
  ? {
      GIT_CONFIG_COUNT: "1",
      GIT_CONFIG_KEY_0: "http.https://github.com/.extraheader",
      GIT_CONFIG_VALUE_0: `AUTHORIZATION: basic ${Buffer.from(
        `x-access-token:${token}`,
      ).toString("base64")}`,
    }
  : {};

/** Runs `cmd`, echoing it; returns stdout, or throws with its stderr. */
function run(
  cmd: string[],
  { quiet = false, env = {} as Record<string, string> } = {},
): string {
  console.log(`+ ${cmd.join(" ")}`);
  const proc = Bun.spawnSync(cmd, {
    stderr: "pipe",
    env: { ...process.env, ...env },
  });
  const stderr = proc.stderr.toString();
  if (!proc.success) {
    if (!quiet) process.stderr.write(stderr);
    throw new Error(`${cmd.slice(0, 3).join(" ")} failed: ${stderr.trim()}`);
  }
  return proc.stdout.toString().trim();
}

const git = (...args: string[]) => run(["git", ...args], { env: gitEnv });

function cliTags(): string[] {
  return git("tag", "--list", "cli-v*").split("\n").filter(Boolean);
}

// 1. The versioned tag and release.
const start = notesStartTag([...cliTags(), tag], tag);
const files = (await readdir(outDir)).map((name) => join(outDir, name));
// GitHub creates the tag with the release, so a failed create leaves no tag:
// the cli-v* ruleset would block deleting one, and it would count as released.
run([
  "gh",
  "release",
  "create",
  tag,
  ...files,
  "--target",
  sha,
  "--latest=false",
  "--generate-notes",
  ...(start ? ["--notes-start-tag", start] : []),
  ...(channel === RELEASE_CHANNEL ? [] : ["--prerelease"]),
]);

// 2. Only the newest commit on the branch may move the channel release.
const head = git(
  "ls-remote",
  "origin",
  `refs/heads/cli-channel/${channel}`,
).split(/\s/)[0];
if (head !== sha) {
  console.log(`cli-channel/${channel} moved on; channel release unchanged`);
  process.exit(0);
}

// 3. The channel tag.
const channelTag = `cli-channel-${channel}`;
git("push", "--force", "origin", `${sha}:refs/tags/${channelTag}`);

// 4. The channel release.
const title = `${channel} channel (${tag})`;
const notes = `Always points at the most recent ${channel} build on the cli-channel/${channel} branch. Its tag and manifest.json are replaced on every publish. Do not delete.`;
let exists = true;
try {
  run(["gh", "release", "view", channelTag, "--json", "tagName"], {
    quiet: true,
  });
} catch (error) {
  // Only a missing release; anything else (auth, rate limit) must not look like one.
  if (!String(error).includes("release not found")) throw error;
  exists = false;
}
if (exists) {
  run(["gh", "release", "edit", channelTag, "--title", title]);
} else {
  run([
    "gh",
    "release",
    "create",
    channelTag,
    "--verify-tag",
    "--prerelease",
    "--latest=false",
    "--title",
    title,
    "--notes",
    notes,
  ]);
}

// The manifest goes last: it is what installed CLIs and the install scripts read.
const uploads = [
  ...(channel === RELEASE_CHANNEL ? ["install.sh", "install.ps1"] : []),
  "manifest.json",
];
for (const name of uploads) {
  run(["gh", "release", "upload", channelTag, join(outDir, name), "--clobber"]);
}
console.log(`${channelTag} now points at ${tag}`);
