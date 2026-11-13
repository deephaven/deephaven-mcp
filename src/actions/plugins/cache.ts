/**
 * The plugin cache (plans/cluster-plugins.md, "The cache"):
 *
 *   <config dir>/plugins/cache/<encodeURIComponent(origin)>/<channel>/
 *     state.json        what was last fetched or copied, and when
 *     manifest.json
 *     files/<digest-hex>/<part-relative path>
 *
 * Files are stored by plugin digest, so revisions never mix. An update
 * writes and verifies every new plugin's files before it replaces
 * `manifest.json` and `state.json`, so an interrupted one leaves the
 * previous revision in use.
 */
import { randomUUID } from "node:crypto";
import {
  mkdir,
  readdir,
  readFile,
  rename,
  rm,
  writeFile,
} from "node:fs/promises";
import { dirname, join } from "node:path";
import { DhError, isErrno } from "../../errors.ts";
import { configDir, type Store } from "../auth/store.ts";
import {
  type Channel,
  digestOf,
  filesOf,
  type Manifest,
  type ManifestEntry,
  type ManifestRead,
  readManifest,
} from "./bundle.ts";
import type { RunAs } from "./spec.ts";

/** What a part's cache records about its last fetch or copy. */
export interface PartState {
  /** The manifest's revision; null when the server has no bundle. */
  revision: string | null;
  /** ISO timestamp of the last check, changed or not. */
  fetchedAt: string;
  clusterVersion?: string;
  /** `shell`: the SSH host it was copied from. */
  host?: string;
  /** `shell`, with no bundle: the path looked at. */
  path?: string;
  /** `js`: the `revision` response's ETag, for a conditional request. */
  etag?: string;
  /** `js`: the user's groups at the last fetch, when a plugin has `groups`. */
  groups?: string[];
  /** `shell`: the service accounts runs last resolved, by role. */
  accounts?: Partial<Record<RunAs["account"], string>>;
}

/** A part's cache as read at startup. */
export interface CachedPart {
  dir: string;
  state: PartState;
  /** Unset when `state.revision` is null, or the manifest can't be read. */
  manifest?: ManifestRead;
}

/** `<config dir>/plugins/cache/<origin>/<channel>/`. */
export function cacheDir(origin: string, channel: Channel): string {
  return join(
    configDir(),
    "plugins",
    "cache",
    encodeURIComponent(origin),
    channel,
  );
}

/** Where a cached plugin's file is. */
export function cachedFile(
  dir: string,
  entry: ManifestEntry,
  path: string,
): string {
  return join(dir, "files", hexOf(entry.digest), path);
}

/** The part's cache, or undefined if there's none or it can't be read. */
export async function readCachedPart(
  origin: string,
  channel: Channel,
): Promise<CachedPart | undefined> {
  const dir = cacheDir(origin, channel);
  const state = await readJson<PartState>(join(dir, "state.json"));
  if (!state) return undefined;
  if (state.revision === null) return { dir, state };
  const manifest = await readJson<unknown>(join(dir, "manifest.json"));
  return {
    dir,
    state,
    ...(manifest === undefined
      ? {}
      : { manifest: readManifest(manifest, channel) }),
  };
}

/**
 * Replaces the part's cache with `manifest` (`raw` is written, so entries it
 * left out are still reported), whose plugins' files `read` gives (by
 * part-relative path). Files of plugins already cached are kept.
 * Every new plugin's files must match its digest, else nothing changes and
 * `plugin_invalid` is thrown.
 */
export async function writeCachedPart(
  store: Store,
  origin: string,
  channel: Channel,
  manifest: Manifest,
  raw: unknown,
  state: Omit<PartState, "revision" | "clusterVersion">,
  read: (path: string) => Promise<Uint8Array>,
): Promise<void> {
  const dir = cacheDir(origin, channel);
  await store.withLock(async () => {
    const before = await readCachedPart(origin, channel);
    const files = join(dir, "files");
    for (const entry of manifest.plugins) {
      const target = join(files, hexOf(entry.digest));
      if (await exists(target)) continue;
      const tmp = join(files, `.tmp-${randomUUID()}`);
      try {
        const bytes: Uint8Array[] = [];
        for (const path of filesOf(entry)) {
          const data = await read(path);
          bytes.push(data);
          await mkdir(dirname(join(tmp, path)), { recursive: true });
          await writeFile(join(tmp, path), data);
        }
        if (digestOf(bytes) !== entry.digest) {
          throw new DhError(
            "plugin_invalid",
            `${entry.name}: its files don't match its digest`,
          );
        }
        await rename(tmp, target);
      } finally {
        await rm(tmp, { recursive: true, force: true });
      }
    }
    await writeAtomic(join(dir, "manifest.json"), raw);
    await writeAtomic(join(dir, "state.json"), {
      revision: manifest.revision,
      ...state,
      clusterVersion: manifest.cluster.version,
    });
    const kept = new Set(
      [
        ...manifest.plugins,
        ...(before?.manifest?.ok ? before.manifest.manifest.plugins : []),
      ].map((e) => hexOf(e.digest)),
    );
    await removeOthers(files, kept);
  });
}

/** Records that the server has no bundle: the part's plugins are gone. */
export async function writeNoBundle(
  store: Store,
  origin: string,
  channel: Channel,
  state: Omit<PartState, "revision">,
): Promise<void> {
  const dir = cacheDir(origin, channel);
  await store.withLock(async () => {
    await rm(join(dir, "manifest.json"), { force: true });
    await rm(join(dir, "files"), { recursive: true, force: true });
    await writeAtomic(join(dir, "state.json"), { revision: null, ...state });
  });
}

/** Records an unchanged check: `fetchedAt` changes, and anything in `patch`. */
export async function touchCachedPart(
  store: Store,
  origin: string,
  channel: Channel,
  patch: Partial<PartState> = {},
): Promise<void> {
  const dir = cacheDir(origin, channel);
  await store.withLock(async () => {
    const state = await readJson<PartState>(join(dir, "state.json"));
    if (!state) return;
    await writeAtomic(join(dir, "state.json"), {
      ...state,
      ...patch,
      fetchedAt: new Date().toISOString(),
    });
  });
}

/** Records the account `role` resolved to in the last run on `origin`, if it changed. */
export async function recordAccount(
  store: Store,
  origin: string,
  role: RunAs["account"],
  account: string,
): Promise<void> {
  const file = join(cacheDir(origin, "shell"), "state.json");
  const known = await readJson<PartState>(file);
  if (!known || known.accounts?.[role] === account) return;
  await store.withLock(async () => {
    const state = await readJson<PartState>(file);
    if (!state) return;
    await writeAtomic(file, {
      ...state,
      accounts: { ...state.accounts, [role]: account },
    });
  });
}

/** Deletes `origin`'s caches. */
export async function removeCache(origin: string): Promise<void> {
  await rm(dirname(cacheDir(origin, "shell")), {
    recursive: true,
    force: true,
  });
}

/** Moves `from`'s caches to `to`, replacing any there. */
export async function moveCache(from: string, to: string): Promise<void> {
  const source = dirname(cacheDir(from, "shell"));
  const target = dirname(cacheDir(to, "shell"));
  if (!(await exists(source))) return;
  await rm(target, { recursive: true, force: true });
  await rename(source, target);
}

/** The hex of a `sha256:<hex>` digest. */
function hexOf(digest: string): string {
  return digest.slice("sha256:".length);
}

/** Deletes the entries of `dir` not in `kept`, temp directories included. */
async function removeOthers(dir: string, kept: Set<string>): Promise<void> {
  let names: string[];
  try {
    names = await readdir(dir);
  } catch (e) {
    if (isErrno(e, "ENOENT")) return;
    throw e;
  }
  for (const name of names) {
    if (!kept.has(name)) {
      await rm(join(dir, name), { recursive: true, force: true });
    }
  }
}

async function exists(path: string): Promise<boolean> {
  try {
    await readdir(path);
    return true;
  } catch {
    return false;
  }
}

/** Parsed JSON at `path`; undefined if it's missing or not JSON. */
async function readJson<T>(path: string): Promise<T | undefined> {
  try {
    return JSON.parse(await readFile(path, "utf8")) as T;
  } catch {
    return undefined;
  }
}

/** Writes `value` as JSON via a temp file and rename. */
async function writeAtomic(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const tmp = `${path}.${process.pid}.${randomUUID()}.tmp`;
  try {
    await writeFile(tmp, `${JSON.stringify(value, null, 2)}\n`);
    await rename(tmp, path);
  } catch (e) {
    await rm(tmp, { force: true });
    throw e;
  }
}
