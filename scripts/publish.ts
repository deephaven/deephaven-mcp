/**
 * Publishes a built release from CI: `bun run publish <tag> <dir>`.
 *
 * Creates the versioned release `<tag>` from every file in `<dir>`, then, if
 * this commit is still the newest on `cli-channel/<channel>`, points the
 * channel release `cli-channel-<channel>` at it: moves its tag, retitles it,
 * and replaces its `manifest.json` (plus the install scripts on `release`).
 * Needs `gh`, `GH_TOKEN`, `GITHUB_REPOSITORY` and `GITHUB_SHA`.
 */
import { readdir } from "node:fs/promises";
import { join, resolve } from "node:path";
import { notesStartTag, parseTag, RELEASE_CHANNEL } from "../src/channel.ts";

const [tag, dir] = process.argv.slice(2);
const parsed = tag ? parseTag(tag) : undefined;
if (!parsed || !dir) {
  console.error("usage: bun run publish <cli-v tag> <dir>");
  process.exit(1);
}
const { GITHUB_REPOSITORY: repo, GITHUB_SHA: sha } = process.env;
if (!repo || !sha) {
  console.error("GITHUB_REPOSITORY and GITHUB_SHA must be set");
  process.exit(1);
}
const outDir = resolve(dir);
const { channel } = parsed;

/** Runs `cmd`, echoing it; returns stdout, or throws with its stderr. */
function run(cmd: string[], { quiet = false } = {}): string {
  console.log(`+ ${cmd.join(" ")}`);
  const proc = Bun.spawnSync(cmd, { stderr: "pipe" });
  const stderr = proc.stderr.toString();
  if (!proc.success) {
    if (!quiet) process.stderr.write(stderr);
    throw new Error(`${cmd.slice(0, 3).join(" ")} failed: ${stderr.trim()}`);
  }
  return proc.stdout.toString().trim();
}

function tagList(): string[] {
  const out = run(["git", "tag", "--list", "cli-v*"]);
  return out.split("\n").filter(Boolean);
}

// 1. The versioned release.
const files = (await readdir(outDir)).map((name) => join(outDir, name));
const start = notesStartTag([...tagList(), tag as string], tag as string);
run([
  "gh",
  "release",
  "create",
  tag as string,
  ...files,
  "--target",
  sha,
  "--latest=false",
  "--generate-notes",
  ...(start ? ["--notes-start-tag", start] : []),
  ...(channel === RELEASE_CHANNEL ? [] : ["--prerelease"]),
]);

// 2. Only the newest commit on the branch may move the channel release.
const head = run([
  "gh",
  "api",
  `repos/${repo}/git/ref/heads/cli-channel/${channel}`,
  "--jq",
  ".object.sha",
]);
if (head !== sha) {
  console.log(`cli-channel/${channel} moved on; channel release unchanged`);
  process.exit(0);
}

// 3. The channel release.
const channelTag = `cli-channel-${channel}`;
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
  run([
    "gh",
    "api",
    "-X",
    "PATCH",
    `repos/${repo}/git/refs/tags/${channelTag}`,
    "-f",
    `sha=${sha}`,
    "-F",
    "force=true",
  ]);
  run(["gh", "release", "edit", channelTag, "--title", title]);
} else {
  run([
    "gh",
    "release",
    "create",
    channelTag,
    "--target",
    sha,
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
