/**
 * The install metadata file: `<exe>.meta` beside the executable (`dh.meta`,
 * `dh.exe.meta`), a JSON object of install-level data. The install scripts
 * write it too, so its format is a contract with them.
 */
import { writeSync } from "node:fs";
import { readFile, rename, rm, writeFile } from "node:fs/promises";
import { tryWithFileLock, withFileLock } from "./actions/auth/lock.ts";
import { isChannel, RELEASE_CHANNEL } from "./channel.ts";
import { BIN } from "./version.ts";

/** What the CLI reads from the file; unknown keys are kept on every write. */
export interface InstallMeta {
  /** The channel this install follows. */
  channel?: string;
  /** When the updater last checked, as an ISO 8601 string. */
  lastUpdateCheck?: string;
  [key: string]: unknown;
}

/** `<exe>.meta`, named after the running executable. */
export const metaPath = (): string => `${process.execPath}.meta`;

const LOCK_WAIT_MS = 10_000;

/**
 * Runs `fn` holding the file's cross-process lock (`<path>.lock`). Every change
 * to the file, and every replacement of the executable, happens under it.
 * With `wait: false`, returns undefined without running `fn` if another
 * process holds it.
 */
export async function withMetaLock<T>(
  fn: () => Promise<T>,
  { wait }: { wait: boolean },
  path = metaPath(),
): Promise<T | undefined> {
  const lock = `${path}.lock`;
  if (wait) return await withFileLock(lock, LOCK_WAIT_MS, fn);
  const result = await tryWithFileLock(lock, async () => ({
    value: await fn(),
  }));
  if (!result) debug(`${lock} is busy; skipping`);
  return result?.value;
}

function debug(text: string): void {
  // Not console: it is silenced while the Deephaven client libraries run.
  if (process.env.DH_DEBUG) writeSync(2, `${BIN}: ${text}\n`);
}

/**
 * The file's contents, or `{}` if it is missing or unusable (unparseable, not
 * an object, or an invalid `channel`). Never throws.
 */
export async function readMeta(path = metaPath()): Promise<InstallMeta> {
  let text: string;
  try {
    text = await readFile(path, "utf8");
  } catch {
    return {};
  }
  try {
    const value: unknown = JSON.parse(text);
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      throw new Error("not a JSON object");
    }
    const meta = value as InstallMeta;
    if (
      meta.channel !== undefined &&
      (typeof meta.channel !== "string" || !isChannel(meta.channel))
    ) {
      throw new Error(`invalid channel ${JSON.stringify(meta.channel)}`);
    }
    return meta;
  } catch (e) {
    debug(`ignoring ${path}: ${(e as Error).message}`);
    return {};
  }
}

/** The channel to follow: the saved one, else `release`. */
export function followedChannel(meta: InstallMeta): string {
  return meta.channel ?? RELEASE_CHANNEL;
}

/**
 * Sets `changes` in the file, keeping every other key, via a temp file renamed
 * into place. An unusable file is replaced with a fresh object. The caller
 * must hold {@link withMetaLock}.
 */
export async function writeMeta(
  changes: Partial<InstallMeta>,
  path = metaPath(),
): Promise<void> {
  const next = { ...(await readMeta(path)), ...changes };
  // Per-process name, so concurrent writers never rename each other's file.
  const tmp = `${path}.${process.pid}.tmp`;
  try {
    await writeFile(tmp, `${JSON.stringify(next, null, 2)}\n`);
    await rename(tmp, path);
  } catch (e) {
    await rm(tmp).catch(() => {});
    throw e;
  }
}

/** {@link writeMeta} under the lock; with `wait: false`, skipped if the lock is busy. */
export async function updateMeta(
  changes: Partial<InstallMeta>,
  { wait }: { wait: boolean },
  path = metaPath(),
): Promise<void> {
  await withMetaLock(() => writeMeta(changes, path), { wait }, path);
}
