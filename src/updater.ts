import { encodeHex } from "@std/encoding/hex";
import { basename } from "@std/path";
import { greaterThan, parse } from "@std/semver";
import { VERSION } from "./version.ts";

// TODO: point at the real central update location.
const DEFAULT_UPDATE_URL = "https://updates.example.com/dh/manifest.json";
const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);
const DEFAULT_INTERVAL_HOURS = 24;

export interface Manifest {
  version: string;
  /** Keyed by `Deno.build.target`; `url` is relative to the manifest. */
  binaries: Record<string, { url: string; sha256: string }>;
}

function assertTrusted(url: URL): void {
  if (url.protocol === "https:") return;
  if (url.protocol === "http:" && LOCAL_HOSTS.has(url.hostname)) return;
  throw new Error(`Refusing to update over insecure URL: ${url}`);
}

async function fetchOk(url: URL, timeoutMs: number): Promise<Response> {
  assertTrusted(url);
  const res = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
  if (!res.ok) {
    await res.body?.cancel();
    throw new Error(`HTTP ${res.status} fetching ${url}`);
  }
  return res;
}

async function replaceExecutable(binary: Uint8Array): Promise<void> {
  const exe = Deno.execPath();
  const staged = `${exe}.new`;
  await Deno.writeFile(staged, binary, { mode: 0o755 });
  if (Deno.build.os === "windows") {
    // A running .exe can't be overwritten, but it can be renamed.
    await Deno.remove(`${exe}.old`).catch(() => {});
    await Deno.rename(exe, `${exe}.old`);
  } else {
    await Deno.chmod(staged, 0o755);
  }
  await Deno.rename(staged, exe);
}

function intervalMs(): number {
  const hours = Number(Deno.env.get("DH_UPDATE_INTERVAL") ?? NaN);
  const valid = Number.isFinite(hours) && hours >= 0;
  return (valid ? hours : DEFAULT_INTERVAL_HOURS) * 3_600_000;
}

/** Records a check attempt; returns false if the last one was too recent. */
async function claimCheck(): Promise<boolean> {
  const stamp = `${Deno.execPath()}.last-update-check`;
  const last = Number(await Deno.readTextFile(stamp).catch(() => "0"));
  const elapsed = Date.now() - last;
  if (elapsed >= 0 && elapsed < intervalMs()) return false;
  // Written before fetching so failed or slow checks are throttled too.
  await Deno.writeTextFile(stamp, String(Date.now()));
  return true;
}

export async function autoUpdate(): Promise<void> {
  if (Deno.env.get("DH_AUTO_UPDATE") === "off") return;
  // Running from source via `deno run`; never replace the deno executable.
  if (/^deno(\.exe)?$/i.test(basename(Deno.execPath()))) return;
  if (!await claimCheck()) return;

  const manifestUrl = new URL(
    Deno.env.get("DH_UPDATE_URL") ?? DEFAULT_UPDATE_URL,
  );
  const manifest: Manifest = await (await fetchOk(manifestUrl, 5_000)).json();
  if (!greaterThan(parse(manifest.version), parse(VERSION))) return;

  const asset = manifest.binaries[Deno.build.target];
  if (!asset) return;

  const url = new URL(asset.url, manifestUrl);
  const binary = new Uint8Array(
    await (await fetchOk(url, 5 * 60_000)).arrayBuffer(),
  );
  const digest = encodeHex(await crypto.subtle.digest("SHA-256", binary));
  if (digest !== asset.sha256.toLowerCase()) {
    throw new Error(`Checksum mismatch for ${url}`);
  }

  await replaceExecutable(binary);
  console.error(`Updated dh ${VERSION} -> ${manifest.version}`);
}
