/** Builds every target for a GitHub release: `bun run release <tag> [outDir]`. */
import { readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import pkg from "../package.json" with { type: "json" };
import { releasesUrl } from "../src/updater.ts";
import { build, TARGETS } from "./build.ts";

const ROOT = resolve(import.meta.dir, "..");
const [tag, out] = process.argv.slice(2);
// Set by GitHub Actions, so a fork's release installs and updates from the fork.
const repository = process.env.GITHUB_REPOSITORY ?? pkg.repository;

if (tag !== `v${pkg.version}`) {
  console.error(
    `Tag ${tag} does not match package.json version ${pkg.version}`,
  );
  process.exit(1);
}
if (process.platform !== "darwin") {
  console.error("Release on macOS: the darwin binaries must be re-signed.");
  process.exit(1);
}

const outDir = resolve(out ?? join(ROOT, "dist"));
// Tagged URLs, so a release published mid-update can't mix binaries.
await build(outDir, {
  repository,
  targets: TARGETS,
  baseUrl: `${releasesUrl(repository)}/download/${tag}/`,
});

const upstream = `github.com/${pkg.repository}`;
for (const name of ["install.sh", "install.ps1"]) {
  const script = await readFile(join(ROOT, name), "utf8");
  if (!script.includes(upstream)) {
    throw new Error(`${name} does not reference ${upstream}`);
  }
  await writeFile(
    join(outDir, name),
    script.replaceAll(upstream, `github.com/${repository}`),
  );
}
console.log(`release assets for ${repository} ${tag} in ${outDir}`);
