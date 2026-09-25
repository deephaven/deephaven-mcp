import { encodeHex } from "@std/encoding/hex";
import { copy, ensureDir } from "@std/fs";
import { fromFileUrl, join, resolve } from "@std/path";
import config from "../deno.json" with { type: "json" };
import type { Manifest } from "../src/updater.ts";

const ROOT = fromFileUrl(new URL("..", import.meta.url));

export const BINARY_NAME = `dh-${Deno.build.target}${
  Deno.build.os === "windows" ? ".exe" : ""
}`;

/** Compiles dh for the current platform into `outDir` with a `manifest.json`. */
export async function build(outDir: string, version?: string): Promise<void> {
  // VERSION is read from deno.json at compile time, so other versions build from a copy.
  let root = ROOT;
  if (version) {
    root = await Deno.makeTempDir({ prefix: "dh-build-" });
    await copy(join(ROOT, "src"), join(root, "src"));
    await copy(join(ROOT, "deno.lock"), join(root, "deno.lock"));
    await Deno.writeTextFile(
      join(root, "deno.json"),
      JSON.stringify({ ...config, version }),
    );
  }

  try {
    await ensureDir(outDir);
    const output = join(outDir, BINARY_NAME);
    const { success } = await new Deno.Command(Deno.execPath(), {
      args: [
        "compile",
        "--quiet",
        "--allow-env",
        "--allow-net",
        "--allow-read",
        "--allow-write",
        "--output",
        output,
        join(root, "src/main.ts"),
      ],
      cwd: root,
    }).spawn().status;
    if (!success) throw new Error("deno compile failed");

    const digest = await crypto.subtle.digest(
      "SHA-256",
      await Deno.readFile(output),
    );
    const manifest: Manifest = {
      version: version ?? config.version,
      binaries: {
        [Deno.build.target]: { url: BINARY_NAME, sha256: encodeHex(digest) },
      },
    };
    await Deno.writeTextFile(
      join(outDir, "manifest.json"),
      JSON.stringify(manifest, null, 2) + "\n",
    );
  } finally {
    if (root !== ROOT) await Deno.remove(root, { recursive: true });
  }
}

if (import.meta.main) {
  const outDir = resolve(Deno.args[0] ?? join(ROOT, "dist"));
  await build(outDir);
  console.log(`built ${join(outDir, BINARY_NAME)} (v${config.version})`);
}
