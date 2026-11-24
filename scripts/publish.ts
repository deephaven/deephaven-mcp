/**
 * Publishes a built release from CI: `bun run publish <tag> <dir>`.
 *
 * Pushes the versioned tag `<tag>` and creates its release from every file in
 * `<dir>`. Then, if this commit is still the newest on
 * `cli-channel/<channel>`, moves the `cli-channel-<channel>` tag to it and
 * points that channel release at it: retitles it and replaces its
 * `manifest.json` (plus the install scripts on `release`).
 *
 * Tags and the branch check use `git` against the checkout's `origin`; only
 * release objects use `gh`. Needs `GITHUB_REPOSITORY` and `GITHUB_SHA`, and
 * `GH_TOKEN` on CI (also used to authenticate `git push`).
 */
import { readdir } from "node:fs/promises";
import { join, resolve } from "node:path";
import { notesStartTag, parseTag, RELEASE_CHANNEL } from "../src/channel.ts";

const [tag, dir] = process.argv.slice(2);
const parsed = tag ? parseTag(tag) : undefined;
if (!tag || !parsed || !dir) {
  console.error("usage: bun run publish <cli-v tag> <dir>");
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

// 1–2. The versioned tag and release.
const start = notesStartTag([...cliTags(), tag], tag);
git("push", "origin", `${sha}:refs/tags/${tag}`);
const files = (await readdir(outDir)).map((name) => join(outDir, name));
run([
  "gh",
  "release",
  "create",
  tag,
  ...files,
  "--verify-tag",
  "--latest=false",
  "--generate-notes",
  ...(start ? ["--notes-start-tag", start] : []),
  ...(channel === RELEASE_CHANNEL ? [] : ["--prerelease"]),
]);

// 3. Only the newest commit on the branch may move the channel release.
const head = git(
  "ls-remote",
  "origin",
  `refs/heads/cli-channel/${channel}`,
).split(/\s/)[0];
if (head !== sha) {
  console.log(`cli-channel/${channel} moved on; channel release unchanged`);
  process.exit(0);
}

// 4. The channel tag.
const channelTag = `cli-channel-${channel}`;
git("push", "--force", "origin", `${sha}:refs/tags/${channelTag}`);

// 5. The channel release.
const title = `${channel} channel (currently ${tag})`;
const notes = `Moving release for the ${channel} channel. Its tag and manifest.json are replaced on every publish. Do not delete.`;
let exists = true;
try {
  run(["gh", "release", "view", channelTag, "--json", "tagName"], {
    quiet: true,
  });
} catch {
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
