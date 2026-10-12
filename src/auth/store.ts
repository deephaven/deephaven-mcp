import {
  chmod,
  mkdir,
  open,
  readFile,
  rename,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { hostname } from "node:os";
import { join } from "node:path";
import { DhError, isErrno, reasonOf } from "./errors.ts";

export type Kind = "enterprise" | "community";
export type Method =
  | "saml"
  | "password"
  | "private-key"
  | "psk"
  | "basic"
  | "anonymous"
  | "custom";

export interface ServerEntry {
  kind: Kind;
  caCert?: string;
  importedFrom?: string;
}

export interface Profile {
  name: string;
  server: string;
  user: string;
  method: Method;
  operateAs?: string;
  /** DHC custom auth handler class. */
  handler?: string;
  /** Legacy config entries this profile was migrated from (a string in older configs). */
  importedFrom?: string[];
  createdAt: string;
}

export type Credential =
  | { type: "none" }
  | {
      type: "keyPair";
      keyType: string;
      publicKey: string;
      privateKey: string;
      /** Created and uploaded by this CLI, so it may revoke it. */
      generated: boolean;
    }
  | { type: "keyFile"; path: string }
  | { type: "secret"; value: string }
  /** `basicToken`: the variable holds `user:password` (legacy v1 Basic). */
  | { type: "envRef"; env: string; basicToken?: boolean };

export interface Config {
  version: 1;
  defaultProfile?: string;
  legacyImport?: "done" | "declined";
  servers: Record<string, ServerEntry>;
  profiles: Record<string, Profile>;
}

export interface Credentials {
  version: 1;
  credentials: Record<string, Credential>;
  /** Generated keys whose deletion failed; retried at the next sign-in as `user`. */
  orphans?: OrphanKey[];
}

export interface OrphanKey {
  server: string;
  user: string;
  keyType: string;
  publicKey: string;
}

export interface State {
  config: Config;
  credentials: Credentials;
}

const WINDOWS = process.platform === "win32";
const LOCK_WAIT_MS = 10_000;
const LOCK_STALE_MS = 30_000;

export function configDir(): string {
  const override = process.env.DH_CONFIG_DIR;
  if (override) return override;
  if (WINDOWS) {
    const appData = process.env.APPDATA;
    if (appData) return join(appData, "Deephaven", "cli");
  }
  const home = WINDOWS ? process.env.USERPROFILE : process.env.HOME;
  if (!home) throw new DhError("internal", "Cannot find the home directory");
  return join(home, ".deephaven", "cli");
}

export function emptyState(): State {
  return {
    config: { version: 1, servers: {}, profiles: {} },
    credentials: { version: 1, credentials: {} },
  };
}

export class Store {
  constructor(readonly dir: string = configDir()) {}

  get configPath(): string {
    return join(this.dir, "config.json");
  }

  get credentialsPath(): string {
    return join(this.dir, "credentials.json");
  }

  get cacheDir(): string {
    return join(this.dir, "cache");
  }

  async read(): Promise<State> {
    const state = emptyState();
    const config = await readJson<Config>(this.configPath);
    if (config) state.config = { ...state.config, ...config };
    const credentials = await readJson<Credentials>(this.credentialsPath);
    if (credentials) {
      await this.assertPrivate();
      state.credentials = { ...state.credentials, ...credentials };
    }
    return state;
  }

  /** Read-modify-write of both files under one lock. */
  async update<T>(fn: (state: State) => T | Promise<T>): Promise<T> {
    await this.makeDir(false);
    return await withLock(join(this.dir, "config.lock"), async () => {
      const state = await this.read();
      const value = await fn(state);
      await writeAtomic(this.configPath, state.config, 0o644);
      await writeAtomic(this.credentialsPath, state.credentials, 0o600);
      return value;
    });
  }

  /** Owner-only directory, re-securing an existing one. Call once per command, before writing. */
  async ensureDir(): Promise<void> {
    await this.makeDir(true);
  }

  private async makeDir(resecure: boolean): Promise<void> {
    const existed = await stat(this.dir).then(
      () => true,
      () => false,
    );
    if (!existed) await mkdir(this.dir, { recursive: true });
    if (!WINDOWS) await chmod(this.dir, 0o700);
    // Editing ACLs races with other processes' writes, so not on every update.
    else if (!existed || resecure) {
      await restrictWindowsAcl(this.dir, existed ? "reset-dir" : "dir");
    }
  }

  private async assertPrivate(): Promise<void> {
    if (WINDOWS) return;
    const { mode } = await stat(this.credentialsPath);
    if ((mode & 0o077) !== 0) {
      throw new DhError(
        "credential_unavailable",
        `${this.credentialsPath} can be read by other users`,
        `Run: chmod 600 ${this.credentialsPath}`,
      );
    }
  }
}

async function readJson<T>(path: string): Promise<T | undefined> {
  let text: string;
  try {
    text = await readFile(path, "utf8");
  } catch (e) {
    if (isErrno(e, "ENOENT")) return undefined;
    throw e;
  }
  try {
    return JSON.parse(text) as T;
  } catch (e) {
    throw new DhError(
      "internal",
      `${path} is not valid JSON`,
      undefined,
      reasonOf(e),
    );
  }
}

async function writeAtomic(
  path: string,
  value: unknown,
  mode: number,
): Promise<void> {
  const tmp = `${path}.${process.pid}.${crypto.randomUUID()}.tmp`;
  try {
    const file = await open(tmp, "wx", mode);
    try {
      await file.writeFile(`${JSON.stringify(value, null, 2)}\n`);
      await file.sync();
    } finally {
      await file.close();
    }
    if (!WINDOWS) await chmod(tmp, mode);
    // The temp file is this process's own, so its ACL can change without racing other writers.
    else if (mode === 0o600) await restrictWindowsAcl(tmp, "file");
    await rename(tmp, path);
  } catch (e) {
    await rm(tmp, { force: true }).catch(() => {});
    throw e;
  }
}

/**
 * Owner-only. `dir`: inherited by new files; `reset-dir`: also drops explicit
 * grants on an existing folder first. `file`: just that file.
 */
async function restrictWindowsAcl(
  path: string,
  kind: "dir" | "reset-dir" | "file",
): Promise<void> {
  const user = process.env.USERNAME;
  const icacls = async (args: string[]) =>
    (await Bun.spawn(["icacls", path, ...args, "/Q"], {
      stdout: "ignore",
      stderr: "ignore",
    }).exited) === 0;
  const grant = kind === "file" ? `${user}:F` : `${user}:(OI)(CI)F`;
  const ok =
    (kind !== "reset-dir" || (await icacls(["/reset"]))) &&
    (await icacls(["/inheritance:r", "/grant:r", grant]));
  if (!ok) {
    throw new DhError("internal", `Could not restrict access to ${path}`);
  }
}

/** The lock file names its holder, so only the holder removes it and a dead holder's lock can be broken. */
async function withLock<T>(path: string, fn: () => Promise<T>): Promise<T> {
  const token = `${hostname()} ${process.pid} ${crypto.randomUUID()}`;
  const deadline = Date.now() + LOCK_WAIT_MS;
  for (;;) {
    try {
      await writeFile(path, token, { flag: "wx" });
      break;
    } catch (e) {
      if (!isErrno(e, "EEXIST")) throw e;
      const stale = await staleLock(path);
      if (stale !== undefined) {
        // Only if unchanged: another process may have broken it and locked since.
        if ((await readFile(path, "utf8").catch(() => undefined)) === stale) {
          await rm(path, { force: true }).catch(() => {});
        }
        continue;
      }
      if (Date.now() > deadline) {
        throw new DhError(
          "internal",
          `Timed out waiting for ${path}`,
          "Delete it if no other process is running.",
        );
      }
      await new Promise((r) => setTimeout(r, 10 + Math.random() * 40));
    }
  }
  try {
    return await fn();
  } finally {
    // Only our own: another process may have broken ours and taken the lock.
    if ((await readFile(path, "utf8").catch(() => "")) === token) {
      await rm(path, { force: true }).catch(() => {});
    }
  }
}

/**
 * The lock's contents if its holder on this computer has exited, or if another
 * computer's lock is old; undefined if it's live or already gone.
 */
async function staleLock(path: string): Promise<string | undefined> {
  const info = await stat(path).catch(() => null);
  const text = await readFile(path, "utf8").catch(() => undefined);
  if (!info || text === undefined) return undefined;
  const [host, pid] = text.split(" ");
  if (host === hostname() && Number(pid) > 0) {
    return isRunning(Number(pid)) ? undefined : text;
  }
  // Still being written, or from another computer sharing the folder.
  return Date.now() - info.mtime.getTime() > LOCK_STALE_MS ? text : undefined;
}

function isRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return isErrno(e, "EPERM");
  }
}
