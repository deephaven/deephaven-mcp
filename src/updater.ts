import { createHash } from "node:crypto";
import { writeSync } from "node:fs";
import { chmod, rename, rm, writeFile } from "node:fs/promises";
import { valid } from "semver";
import { DEV_CHANNEL } from "./channel.ts";
import { followedChannel, readMeta, updateMeta } from "./install-meta.ts";
import { isTrustedSource } from "./net.ts";
import { BIN, CHANNEL, REPOSITORY, STANDALONE, VERSION } from "./version.ts";

export const releasesUrl = (repository: string): string =>
  `https://github.com/${repository}/releases`;
/**
 * Where a channel's CLIs update from: its channel release's `manifest.json`,
 * or `DH_UPDATE_URL` if set. Baked into every shipped binary; keep this URL and
 * the manifest format stable.
 */
export function manifestUrl(channel: string): URL {
  return new URL(
    process.env.DH_UPDATE_URL ||
      `${releasesUrl(REPOSITORY)}/download/cli-channel-${channel}/manifest.json`,
  );
}
const DEFAULT_INTERVAL_HOURS = 24;
const MAX_REDIRECTS = 5;

/** This machine's release key, e.g. `darwin-arm64` or `windows-x64`. */
export const PLATFORM = `${
  process.platform === "win32" ? "windows" : process.platform
}-${process.arch}`;

/** `manifest.json` in each release; every shipped binary reads it, so keep it backward compatible. */
export interface Manifest {
  /** The release's version, without a `v`. */
  version: string;
  /** Keyed by {@link PLATFORM}; `url` may be relative to the manifest. */
  binaries: Record<string, { url: string; sha256: string }>;
}

/** A non-2xx response; `status` lets callers tell "not published" from other failures. */
export class HttpError extends Error {
  constructor(
    readonly status: number,
    url: URL,
  ) {
    super(`HTTP ${status} fetching ${url}`);
    this.name = "HttpError";
  }
}

/** Updates only over https, or http to this machine (for tests). */
function assertTrusted(url: URL): void {
  if (isTrustedSource(url)) return;
  throw new Error(`Refusing to update over insecure URL: ${url}`);
}

/** Fetches `url`, following redirects by hand so every hop must be trusted. `cancel` aborts it early. */
async function fetchOk(
  url: URL,
  timeoutMs: number,
  cancel?: AbortSignal,
): Promise<Response> {
  const timeout = AbortSignal.timeout(timeoutMs);
  const signal = cancel ? AbortSignal.any([timeout, cancel]) : timeout;
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
      throw new HttpError(res.status, url);
    }
    return res;
  }
  throw new Error(`Too many redirects fetching ${url}`);
}

/** Swaps `binary` in for this executable; on Windows the old one is renamed aside first. */
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

/** Minimum time between update checks: `$DH_UPDATE_INTERVAL` hours, else the default. */
function intervalMs(): number {
  const raw = process.env.DH_UPDATE_INTERVAL;
  const hours = raw ? Number(raw) : NaN;
  const valid = Number.isFinite(hours) && hours >= 0;
  return (valid ? hours : DEFAULT_INTERVAL_HOURS) * 3_600_000;
}

/** Records a check attempt in the install metadata; returns false if the last one was too recent. */
async function claimCheck(lastUpdateCheck: unknown): Promise<boolean> {
  const last =
    typeof lastUpdateCheck === "string" ? Date.parse(lastUpdateCheck) : NaN;
  const elapsed = Date.now() - last;
  if (elapsed >= 0 && elapsed < intervalMs()) return false;
  // Written before fetching so failed or slow checks are throttled too.
  await updateMeta({ lastUpdateCheck: new Date().toISOString() });
  return true;
}

/** Fetches the manifest at `url`; throws if its version isn't valid semver. */
export async function fetchManifest(
  url: URL,
  cancel?: AbortSignal,
): Promise<Manifest> {
  const res = await fetchOk(url, 5_000, cancel);
  const manifest = (await res.json()) as Manifest;
  // Strict: rejects "3.0" and "v3.0.0" (unlike Bun.semver).
  if (typeof manifest.version !== "string" || !valid(manifest.version)) {
    throw new Error(`Invalid version in ${url}: ${manifest.version}`);
  }
  return manifest;
}

/**
 * Installs `manifest`'s build for this platform if its version differs from
 * this one, higher or lower. Returns the version installed, or undefined.
 */
export async function install(
  manifest: Manifest,
  manifestUrl: URL,
  cancel?: AbortSignal,
): Promise<string | undefined> {
  if (manifest.version === VERSION) return undefined;
  const asset = manifest.binaries[PLATFORM];
  if (!asset) throw new Error(`No ${PLATFORM} build in ${manifestUrl}`);

  const url = new URL(asset.url, manifestUrl);
  const binary = new Uint8Array(
    await (await fetchOk(url, 5 * 60_000, cancel)).arrayBuffer(),
  );
  const digest = createHash("sha256").update(binary).digest("hex");
  if (digest !== asset.sha256.toLowerCase()) {
    throw new Error(`Checksum mismatch for ${url}`);
  }
  await replaceExecutable(binary);
  return manifest.version;
}

/**
 * Runs after every command in a compiled binary: if a check is due, fetches
 * the followed channel's manifest and installs its build if the version
 * differs. `DH_AUTO_UPDATE=off` disables it; `dev` builds never update.
 */
export async function autoUpdate(): Promise<void> {
  // From source, execPath is the bun runtime, which must never be replaced.
  if (!STANDALONE) return;
  if (process.platform === "win32") {
    // Left by the previous update; deletable once that process has exited.
    await rm(`${process.execPath}.old`).catch(() => {});
  }
  if (CHANNEL === DEV_CHANNEL) return;
  if (process.env.DH_AUTO_UPDATE === "off") return;
  const meta = await readMeta();
  if (!(await claimCheck(meta.lastUpdateCheck))) return;

  const url = manifestUrl(followedChannel(meta).channel);
  const manifest = await fetchManifest(url);
  // A build only for other platforms isn't an error for this one.
  if (!manifest.binaries[PLATFORM]) return;
  const installed = await install(manifest, url);
  // Not console: it is silenced while the Deephaven client libraries run.
  if (installed) writeSync(2, `Updated ${BIN} ${VERSION} -> ${installed}\n`);
}
