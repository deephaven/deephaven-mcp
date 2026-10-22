import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import pkg from "../package.json" with { type: "json" };
import { type Manifest, PLATFORM } from "../src/updater.ts";

const ROOT = resolve(import.meta.dir, "..");

/**
 * Bun flags dh always runs with. Enterprise's key service (ACL write server) is
 * HTTP/2-only, and Bun's fetch offers HTTP/2 only with this.
 */
export const RUNTIME_FLAGS = ["--experimental-http2-fetch"];

/** Release keys, matching the updater's PLATFORM. */
export const TARGETS = [
  "linux-x64",
  "linux-arm64",
  "darwin-x64",
  "darwin-arm64",
  "windows-x64",
];

export function binaryName(target: string): string {
  return `${pkg.name}-${target}${target.startsWith("windows") ? ".exe" : ""}`;
}

async function exec(cmd: string[]): Promise<void> {
  const proc = Bun.spawn(cmd, { stdout: "ignore", stderr: "inherit" });
  if ((await proc.exited) !== 0) throw new Error(`${cmd[0]} failed: ${cmd}`);
}

// Bun appends the app to its own signed runtime, which invalidates that
// signature for darwin-x64 (macOS may kill it at launch); codesign is macOS-only.
async function adhocSign(file: string): Promise<void> {
  if (process.platform !== "darwin") {
    console.warn(`warning: ${file} not re-signed; codesign needs macOS`);
    return;
  }
  await exec(["codesign", "--force", "--sign", "-", file]);
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
  {
    version = pkg.version,
    repository = pkg.repository,
    targets = [PLATFORM],
    baseUrl = "",
  }: BuildOptions = {},
): Promise<void> {
  await mkdir(outDir, { recursive: true });
  const manifest: Manifest = { version, binaries: {} };
  let sums = "";
  for (const target of targets) {
    const name = binaryName(target);
    const output = join(outDir, name);
    await exec([
      process.execPath,
      "build",
      "--compile",
      // Otherwise the binary loads .env/bunfig.toml from the CWD: a repo dh runs in
      // could set DH_UPDATE_URL or preload its own code.
      "--no-compile-autoload-dotenv",
      "--no-compile-autoload-bunfig",
      `--compile-exec-argv=${RUNTIME_FLAGS.join(" ")}`,
      // Ink's react-devtools-core is a devDependency only so this can resolve it;
      // it loads only when DEV=true (marking it external breaks startup).
      `--target=bun-${target}`,
      "--define",
      `DH_BUILD_VERSION=${JSON.stringify(version)}`,
      "--define",
      `DH_BUILD_REPOSITORY=${JSON.stringify(repository)}`,
      "--outfile",
      output,
      join(ROOT, "src/main.ts"),
    ]);
    if (target.startsWith("darwin")) await adhocSign(output);

    const sha256 = createHash("sha256")
      .update(await readFile(output))
      .digest("hex");
    manifest.binaries[target] = { url: baseUrl + name, sha256 };
    sums += `${sha256}  ${name}\n`;
  }
  await writeFile(
    join(outDir, "manifest.json"),
    `${JSON.stringify(manifest, null, 2)}\n`,
  );
  await writeFile(join(outDir, "SHA256SUMS"), sums);
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const targets = args.includes("--all") ? TARGETS : [PLATFORM];
  const dir = args.find((arg) => arg !== "--all");
  const outDir = resolve(dir ?? join(ROOT, "dist"));
  await build(outDir, { targets });
  console.log(`built v${pkg.version} for ${targets.join(", ")} in ${outDir}`);
}
