/** Builds every target for a GitHub release: `deno task release <tag> [outDir]`. */
import { fromFileUrl, join, resolve } from "@std/path";
import config from "../deno.json" with { type: "json" };
import { releasesUrl } from "../src/updater.ts";
import { build, TARGETS } from "./build.ts";

const ROOT = fromFileUrl(new URL("..", import.meta.url));
const [tag, out] = Deno.args;
// Set by GitHub Actions, so a fork's release installs and updates from the fork.
const repository = Deno.env.get("GITHUB_REPOSITORY") ?? config.repository;

if (tag !== `v${config.version}`) {
  console.error(
    `Tag ${tag} does not match deno.json version ${config.version}`,
  );
  Deno.exit(1);
}

const outDir = resolve(out ?? join(ROOT, "dist"));
// Tagged URLs, so a release published mid-update can't mix binaries.
await build(outDir, {
  repository,
  targets: TARGETS,
  baseUrl: `${releasesUrl(repository)}/download/${tag}/`,
});

const upstream = `github.com/${config.repository}`;
for (const name of ["install.sh", "install.ps1"]) {
  const script = await Deno.readTextFile(join(ROOT, name));
  if (!script.includes(upstream)) {
    throw new Error(`${name} does not reference ${upstream}`);
  }
  await Deno.writeTextFile(
    join(outDir, name),
    script.replaceAll(upstream, `github.com/${repository}`),
  );
}
console.log(`release assets for ${repository} ${tag} in ${outDir}`);
