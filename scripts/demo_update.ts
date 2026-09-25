/** Manual auto-update check: installs the current build and serves a bumped one. */
import { emptyDir } from "@std/fs";
import { serveDir } from "@std/http/file-server";
import { fromFileUrl, join } from "@std/path";
import { format, increment, parse } from "@std/semver";
import { VERSION } from "../src/version.ts";
import { BINARY_NAME, build } from "./build.ts";

const DEMO = fromFileUrl(new URL("../.demo", import.meta.url));
const PORT = 8787;
const next = Deno.args[0] ?? format(increment(parse(VERSION), "patch"));

await emptyDir(DEMO);
await build(join(DEMO, "installed"));
await build(join(DEMO, "published"), next);

Deno.serve(
  { hostname: "127.0.0.1", port: PORT, onListen() {} },
  (req) => serveDir(req, { fsRoot: join(DEMO, "published"), quiet: true }),
);

const exe = join(DEMO, "installed", BINARY_NAME);
console.log(`
Serving v${next}. In another terminal:

  export DH_UPDATE_URL=http://127.0.0.1:${PORT}/manifest.json DH_DEBUG=1
  "${exe}" --version
  "${exe}"
  "${exe}" --version

Re-run to reset. Ctrl+C to stop.`);
