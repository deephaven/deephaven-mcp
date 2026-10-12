import { createHash } from "node:crypto";
import { chmod, readFile, rename, rm, writeFile } from "node:fs/promises";
import { gt } from "semver";
import { REPOSITORY, STANDALONE, VERSION } from "./version.ts";

export const releasesUrl = (repository: string): string =>
  `https://github.com/${repository}/releases`;
// Baked into every shipped binary; keep this URL and the manifest format stable.
const DEFAULT_UPDATE_URL = `${releasesUrl(
  REPOSITORY,
)}/latest/download/manifest.json`;
const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);
const DEFAULT_INTERVAL_HOURS = 24;
const MAX_REDIRECTS = 5;

/** This machine's release key, e.g. `darwin-arm64` or `windows-x64`. */
export const PLATFORM = `${
  process.platform === "win32" ? "windows" : process.platform
}-${process.arch}`;

export interface Manifest {
  version: string;
  /** Keyed by {@link PLATFORM}; `url` may be relative to the manifest. */
  binaries: Record<string, { url: string; sha256: string }>;
}

function assertTrusted(url: URL): void {
  if (url.protocol === "https:") return;
  if (url.protocol === "http:" && LOCAL_HOSTS.has(url.hostname)) return;
  throw new Error(`Refusing to update over insecure URL: ${url}`);
}

/** Fetches `url`, following redirects by hand so every hop must be trusted. */
async function fetchOk(url: URL, timeoutMs: number): Promise<Response> {
  const signal = AbortSignal.timeout(timeoutMs);
  for (let hop = 0; hop <= MAX_REDIRECTS; hop++) {
    assertTrusted(url);
    const res = await fetch(url, { signal, redirect: "manual" });
    const location = res.headers.get("location");
    if (res.status >= 300 && res.status < 400 && location) {
      await res.body?.cancel();
      url = new URL(location, url);
      continue;
    }
    if (!res.ok) {
      await res.body?.cancel();
      throw new Error(`HTTP ${res.status} fetching ${url}`);
    }
    return res;
  }
  throw new Error(`Too many redirects fetching ${url}`);
}

async function replaceExecutable(binary: Uint8Array): Promise<void> {
  const exe = process.execPath;
  // Per-process name, so concurrent updates never rename each other's half-written file.
  const staged = `${exe}.${process.pid}.new`;
  try {
    await writeFile(staged, binary, { mode: 0o755 });
    if (process.platform !== "win32") {
      await chmod(staged, 0o755);
      await rename(staged, exe);
      return;
    }
    // A running .exe can't be overwritten, but it can be renamed.
    const old = `${exe}.old`;
    await rm(old).catch(() => {});
    await rename(exe, old);
    try {
      await rename(staged, exe);
    } catch (e) {
      await rename(old, exe);
      throw e;
    }
  } catch (e) {
    await rm(staged).catch(() => {});
    throw e;
  }
}

function intervalMs(): number {
  const raw = process.env.DH_UPDATE_INTERVAL;
  const hours = raw ? Number(raw) : NaN;
  const valid = Number.isFinite(hours) && hours >= 0;
  return (valid ? hours : DEFAULT_INTERVAL_HOURS) * 3_600_000;
}

/** Records a check attempt; returns false if the last one was too recent. */
async function claimCheck(): Promise<boolean> {
  const stamp = `${process.execPath}.last-update-check`;
  const last = Number(await readFile(stamp, "utf8").catch(() => "0"));
  const elapsed = Date.now() - last;
  if (elapsed >= 0 && elapsed < intervalMs()) return false;
  // Written before fetching so failed or slow checks are throttled too.
  await writeFile(stamp, String(Date.now()));
  return true;
}

export async function autoUpdate(): Promise<void> {
  // From source, execPath is the bun runtime, which must never be replaced.
  if (!STANDALONE) return;
  if (process.platform === "win32") {
    // Left by the previous update; deletable once that process has exited.
    await rm(`${process.execPath}.old`).catch(() => {});
  }
  if (process.env.DH_AUTO_UPDATE === "off") return;
  if (!(await claimCheck())) return;

  const manifestUrl = new URL(process.env.DH_UPDATE_URL ?? DEFAULT_UPDATE_URL);
  const manifest = (await (
    await fetchOk(manifestUrl, 5_000)
  ).json()) as Manifest;
  // Strict: throws on an invalid version (unlike Bun.semver, which accepts "3.0").
  if (!gt(manifest.version, VERSION)) return;

  const asset = manifest.binaries[PLATFORM];
  if (!asset) return;

  const url = new URL(asset.url, manifestUrl);
  const binary = new Uint8Array(
    await (await fetchOk(url, 5 * 60_000)).arrayBuffer(),
  );
  const digest = createHash("sha256").update(binary).digest("hex");
  if (digest !== asset.sha256.toLowerCase()) {
    throw new Error(`Checksum mismatch for ${url}`);
  }

  await replaceExecutable(binary);
  console.error(`Updated dh ${VERSION} -> ${manifest.version}`);
}
