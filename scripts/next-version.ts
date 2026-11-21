/**
 * Prints the version CI publishes next on a channel: `bun run next-version <channel>`.
 * Reads tags from git, so the checkout needs all of them (`fetch-depth: 0`).
 */
import pkg from "../package.json" with { type: "json" };
import { nextVersion } from "../src/channel.ts";

const [channel] = process.argv.slice(2);
if (!channel) {
  console.error("usage: bun run next-version <channel>");
  process.exit(1);
}

const git = Bun.spawnSync(["git", "tag", "--list", "cli-v*"], {
  stderr: "inherit",
});
if (!git.success) process.exit(1);
const tags = git.stdout.toString().split("\n").filter(Boolean);

try {
  console.log(nextVersion(tags, pkg.version, channel));
} catch (e) {
  console.error((e as Error).message);
  process.exit(1);
}
