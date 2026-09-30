import { assert, assertStringIncludes } from "@std/assert";
import { serveDir } from "@std/http/file-server";
import { fromFileUrl, join } from "@std/path";
import { binaryName, build } from "../scripts/build.ts";
import { VERSION } from "../src/version.ts";

const NEXT = "9.9.9";
// Every asset of this release redirects to plain HTTP (never resolves: .invalid).
const INSECURE_TAG = "v0.0.0-insecure";
const WINDOWS = Deno.build.os === "windows";
const installer = (name: string) =>
  fromFileUrl(new URL(`../${name}`, import.meta.url));
const INSTALL = WINDOWS
  ? {
    cmd: "powershell",
    args: [
      "-NoProfile",
      "-ExecutionPolicy",
      "Bypass",
      "-File",
      installer("install.ps1"),
    ],
  }
  : { cmd: "sh", args: [installer("install.sh")] };

async function run(exe: string, args: string[], env: Record<string, string>) {
  const out = await new Deno.Command(exe, { args, env }).output();
  const decode = (b: Uint8Array) => new TextDecoder().decode(b);
  return {
    success: out.success,
    stdout: decode(out.stdout),
    stderr: decode(out.stderr),
  };
}

/** Serves `root` with GitHub's `releases/latest` redirects pointing at `latest.tag`. */
function fakeGitHub(root: string, latest: { tag: string }) {
  return Deno.serve(
    { hostname: "127.0.0.1", port: 0, onListen() {} },
    (req) => {
      const url = new URL(req.url);
      const path = url.pathname;
      if (path === "/releases/latest") {
        return Response.redirect(new URL(`/releases/tag/${latest.tag}`, url));
      }
      const asset = path.match(/^\/releases\/latest\/download\/(.+)$/);
      if (asset) {
        const to = `/releases/download/${latest.tag}/${asset[1]}`;
        return Response.redirect(new URL(to, url));
      }
      if (path.startsWith(`/releases/download/${INSECURE_TAG}/`)) {
        return Response.redirect(`http://example.invalid${path}`);
      }
      return serveDir(req, { fsRoot: root, quiet: true });
    },
  );
}

Deno.test({
  name:
    "dh installed with the install script updates itself from GitHub releases",
  async fn() {
    const tmp = await Deno.makeTempDir({ prefix: "dh-update-test-" });
    const latest = { tag: `v${VERSION}` };
    const server = fakeGitHub(tmp, latest);
    const repo = `http://127.0.0.1:${server.addr.port}`;
    const release = (tag: string) => join(tmp, "releases", "download", tag);
    const baseUrl = (tag: string) => `${repo}/releases/download/${tag}/`;
    try {
      await build(release(latest.tag), { baseUrl: baseUrl(latest.tag) });

      const bin = join(tmp, "bin");
      const installEnv = {
        DH_INSTALL_DIR: bin,
        DH_INSTALL_NO_MODIFY_PATH: "1",
      };
      const insecure = await run(INSTALL.cmd, INSTALL.args, {
        ...installEnv,
        DH_INSTALL_REPO_URL: "http://example.com/dh",
      });
      assert(!insecure.success, "installer accepted a plaintext remote URL");
      assertStringIncludes(insecure.stderr, "insecure URL");

      const redirected = await run(INSTALL.cmd, INSTALL.args, {
        ...installEnv,
        DH_INSTALL_REPO_URL: repo,
        DH_INSTALL_VERSION: INSECURE_TAG,
      });
      assert(!redirected.success, "installer followed a redirect to HTTP");
      assertStringIncludes(
        redirected.stderr,
        WINDOWS ? "insecure URL" : 'Protocol "http" disabled',
      );

      const install = await run(INSTALL.cmd, INSTALL.args, {
        ...installEnv,
        DH_INSTALL_REPO_URL: repo,
      });
      assert(install.success, install.stderr);
      assertStringIncludes(install.stdout, `Installed dh v${VERSION}`);

      const exe = join(bin, WINDOWS ? "dh.exe" : "dh");
      const env = {
        DH_UPDATE_URL: `${repo}/releases/latest/download/manifest.json`,
        DH_DEBUG: "1",
      };
      const now = { ...env, DH_UPDATE_INTERVAL: "0" };
      const version = async () => (await run(exe, ["--version"], env)).stdout;

      assertStringIncludes(await version(), VERSION);

      // Tag exists but its assets aren't uploaded: the check fails, but still starts the interval.
      latest.tag = `v${NEXT}`;
      const early = await run(exe, [], env);
      assertStringIncludes(early.stderr, "auto-update failed");

      await build(release(latest.tag), {
        version: NEXT,
        baseUrl: baseUrl(latest.tag),
      });

      const throttled = await run(exe, [], env);
      assert(!throttled.stderr.includes("Updated dh"), throttled.stderr);
      assertStringIncludes(await version(), VERSION, "within interval");

      await run(exe, [], { ...now, DH_AUTO_UPDATE: "off" });
      assertStringIncludes(await version(), VERSION, "DH_AUTO_UPDATE=off");

      const asset = join(release(latest.tag), binaryName(Deno.build.target));
      const good = await Deno.readFile(asset);
      const corrupt = good.slice();
      corrupt[0] ^= 0xff;
      await Deno.writeFile(asset, corrupt);
      const tampered = await run(exe, [], now);
      assertStringIncludes(tampered.stderr, "Checksum mismatch");
      assertStringIncludes(await version(), VERSION, "corrupt binary");
      await Deno.writeFile(asset, good);

      const first = await run(exe, [], now);
      assertStringIncludes(first.stderr, `Updated dh ${VERSION} -> ${NEXT}`);
      assertStringIncludes(await version(), NEXT);

      const second = await run(exe, [], now);
      assert(!second.stderr.includes("Updated dh"), second.stderr);
      if (WINDOWS) {
        const old = await Deno.stat(`${exe}.old`).catch(() => null);
        assert(!old, "dh.exe.old should be cleaned up on the next run");
      }

      const redirect = await run(exe, [], {
        ...now,
        DH_UPDATE_URL:
          `${repo}/releases/download/${INSECURE_TAG}/manifest.json`,
      });
      assertStringIncludes(redirect.stderr, "insecure URL");
      assertStringIncludes(await version(), NEXT, "redirect to HTTP");
    } finally {
      await server.shutdown();
      await Deno.remove(tmp, { recursive: true });
    }
  },
});
