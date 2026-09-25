/** Builds every target for a GitHub release: `deno task release <tag> [outDir]`. */
import { copy } from "@std/fs";
import { fromFileUrl, join, resolve } from "@std/path";
import config from "../deno.json" with { type: "json" };
import { RELEASES_URL } from "../src/updater.ts";
import { build, TARGETS } from "./build.ts";

const ROOT = fromFileUrl(new URL("..", import.meta.url));
const [tag, out] = Deno.args;

if (tag !== `v${config.version}`) {
  console.error(
    `Tag ${tag} does not match deno.json version ${config.version}`,
  );
  Deno.exit(1);
}

const outDir = resolve(out ?? join(ROOT, "dist"));
// Tagged URLs, so a release published mid-update can't mix binaries.
await build(outDir, {
  targets: TARGETS,
  baseUrl: `${RELEASES_URL}/download/${tag}/`,
});
await copy(join(ROOT, "install.sh"), join(outDir, "install.sh"), {
  overwrite: true,
});
console.log(`release assets for ${tag} in ${outDir}`);
