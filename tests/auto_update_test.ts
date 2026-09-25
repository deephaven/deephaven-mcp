import { assert, assertStringIncludes } from "@std/assert";
import { serveDir } from "@std/http/file-server";
import { join } from "@std/path";
import { BINARY_NAME, build } from "../scripts/build.ts";
import { VERSION } from "../src/version.ts";

const NEXT = "9.9.9";

async function run(exe: string, args: string[], env: Record<string, string>) {
  const out = await new Deno.Command(exe, { args, env }).output();
  const decode = (b: Uint8Array) => new TextDecoder().decode(b);
  return { stdout: decode(out.stdout), stderr: decode(out.stderr) };
}

Deno.test("compiled dh updates itself to the published version", async () => {
  const tmp = await Deno.makeTempDir({ prefix: "dh-update-test-" });
  const published = join(tmp, "published");
  const server = Deno.serve(
    { hostname: "127.0.0.1", port: 0, onListen() {} },
    (req) => serveDir(req, { fsRoot: published, quiet: true }),
  );
  try {
    await build(join(tmp, "installed"));

    const exe = join(tmp, "installed", BINARY_NAME);
    const env = {
      DH_UPDATE_URL: `http://127.0.0.1:${server.addr.port}/manifest.json`,
      DH_DEBUG: "1",
    };
    const now = { ...env, DH_UPDATE_INTERVAL: "0" };
    const version = async () => (await run(exe, ["--version"], env)).stdout;

    assertStringIncludes(await version(), VERSION);

    // Nothing published yet: this check fails, but still starts the interval.
    const early = await run(exe, [], env);
    assertStringIncludes(early.stderr, "auto-update failed");

    await build(published, NEXT);

    const throttled = await run(exe, [], env);
    assert(!throttled.stderr.includes("Updated dh"), throttled.stderr);
    assertStringIncludes(await version(), VERSION, "within interval");

    await run(exe, [], { ...now, DH_AUTO_UPDATE: "off" });
    assertStringIncludes(await version(), VERSION, "DH_AUTO_UPDATE=off");

    const first = await run(exe, [], now);
    assertStringIncludes(first.stderr, `Updated dh ${VERSION} -> ${NEXT}`);
    assertStringIncludes(await version(), NEXT);

    const second = await run(exe, [], now);
    assert(!second.stderr.includes("Updated dh"), second.stderr);
  } finally {
    await server.shutdown();
    await Deno.remove(tmp, { recursive: true });
  }
});
