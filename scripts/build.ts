import { encodeHex } from "@std/encoding/hex";
import { copy, ensureDir } from "@std/fs";
import { fromFileUrl, join, resolve } from "@std/path";
import config from "../deno.json" with { type: "json" };
import type { Manifest } from "../src/updater.ts";

const ROOT = fromFileUrl(new URL("..", import.meta.url));

export const TARGETS = [
  "x86_64-unknown-linux-gnu",
  "aarch64-unknown-linux-gnu",
  "x86_64-apple-darwin",
  "aarch64-apple-darwin",
  "x86_64-pc-windows-msvc",
];

export function binaryName(target: string): string {
  return `${config.bin}-${target}${target.includes("windows") ? ".exe" : ""}`;
}

export interface BuildOptions {
  version?: string;
  /** GitHub `owner/repo` baked in as the default update source. */
  repository?: string;
  targets?: string[];
  /** Prefix for binary URLs in the manifest; names stay relative if omitted. */
  baseUrl?: string;
}

/** Compiles the CLI into `outDir` with a `manifest.json` and `SHA256SUMS`. */
export async function build(
  outDir: string,
  { version, repository, targets = [Deno.build.target], baseUrl = "" }:
    BuildOptions = {},
): Promise<void> {
  // deno.json is embedded at compile time, so overrides build from a copy.
  let root = ROOT;
  try {
    if (version || repository) {
      root = await Deno.makeTempDir({ prefix: "dh-build-" });
      await copy(join(ROOT, "src"), join(root, "src"));
      await copy(join(ROOT, "deno.lock"), join(root, "deno.lock"));
      await Deno.writeTextFile(
        join(root, "deno.json"),
        JSON.stringify({
          ...config,
          version: version ?? config.version,
          repository: repository ?? config.repository,
        }),
      );
    }

    await ensureDir(outDir);
    const manifest: Manifest = {
      version: version ?? config.version,
      binaries: {},
    };
    let sums = "";
    for (const target of targets) {
      const name = binaryName(target);
      const output = join(outDir, name);
      const { success } = await new Deno.Command(Deno.execPath(), {
        args: [
          "compile",
          "--quiet",
          "--allow-env",
          "--allow-net",
          "--allow-read",
          "--allow-write",
          "--allow-run",
          "--allow-sys=hostname",
          "--target",
          target,
          "--output",
          output,
          join(root, "src/main.ts"),
        ],
        cwd: root,
      }).spawn().status;
      if (!success) throw new Error(`deno compile failed for ${target}`);

      const sha256 = encodeHex(
        await crypto.subtle.digest("SHA-256", await Deno.readFile(output)),
      );
      manifest.binaries[target] = { url: baseUrl + name, sha256 };
      sums += `${sha256}  ${name}\n`;
    }
    await Deno.writeTextFile(
      join(outDir, "manifest.json"),
      JSON.stringify(manifest, null, 2) + "\n",
    );
    await Deno.writeTextFile(join(outDir, "SHA256SUMS"), sums);
  } finally {
    if (root !== ROOT) await Deno.remove(root, { recursive: true });
  }
}

if (import.meta.main) {
  const targets = Deno.args.includes("--all") ? TARGETS : [Deno.build.target];
  const dir = Deno.args.find((arg) => arg !== "--all");
  const outDir = resolve(dir ?? join(ROOT, "dist"));
  await build(outDir, { targets });
  console.log(
    `built v${config.version} for ${targets.join(", ")} in ${outDir}`,
  );
}
