import {
  chmod,
  mkdir,
  open,
  readFile,
  rename,
  rm,
  stat,
} from "node:fs/promises";
import { join } from "node:path";
import { DhError, isErrno, reasonOf } from "../errors.ts";
import { withFileLock } from "./lock.ts";

/** Which Deephaven product a server is. */
export type Kind = "enterprise" | "community";
/** How a profile signs in. `saml`, `password` and `private-key` are Enterprise; the rest are Community. */
export type Method =
  | "saml"
  | "password"
  | "private-key"
  | "psk"
  | "basic"
  | "anonymous"
  | "custom";

/** What's known about a server, keyed by origin in {@link Config.servers}. */
export interface ServerEntry {
  kind: Kind;
  /** Absolute path of a CA certificate to trust for it (`--ca-cert`). */
  caCert?: string;
  /** The legacy config entry it was first migrated from. */
  importedFrom?: string;
}

/** One saved identity on one server. Its credential is stored separately, under the same id. */
export interface Profile {
  /** Unique, user-facing; `<host>:<user>` unless renamed. */
  name: string;
  /** The server's origin, a key of {@link Config.servers}. */
  server: string;
  user: string;
  method: Method;
  /** Enterprise user to act as, if not `user`. */
  operateAs?: string;
  /** DHC custom auth handler class. */
  handler?: string;
  /** Legacy config entries this profile was migrated from (a string in older configs). */
  importedFrom?: string[];
  /** ISO timestamp of the first save. */
  createdAt: string;
}

/** What a profile signs in with, kept in `credentials.json`. */
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
  /** A pre-shared key, password or token, saved as is. */
  | { type: "secret"; value: string }
  /** Read from the env var `env` at each use. `basicToken`: the variable holds `user:password` (legacy v1 Basic). */
  | { type: "envRef"; env: string; basicToken?: boolean };

/** `config.json`: servers and profiles, nothing secret. */
export interface Config {
  version: 1;
  /** Profile id. */
  defaultProfile?: string;
  /** Set once login has offered to migrate a legacy config; unset means offer again. */
  legacyImport?: "done" | "declined";
  /** By origin. */
  servers: Record<string, ServerEntry>;
  /** By profile id. */
  profiles: Record<string, Profile>;
}

/** `credentials.json`: owner-only. */
export interface Credentials {
  version: 1;
  /** By profile id. */
  credentials: Record<string, Credential>;
  /** Generated keys whose deletion failed; retried at the next sign-in as `user`. */
  orphans?: OrphanKey[];
}

/** A generated public key still on `server` for `user`; the private half is gone. */
export interface OrphanKey {
  server: string;
  user: string;
  keyType: string;
  publicKey: string;
}

/** Both files, as read or about to be written. */
export interface State {
  config: Config;
  credentials: Credentials;
}

const WINDOWS = process.platform === "win32";
const LOCK_WAIT_MS = 10_000;

/** `$DH_CONFIG_DIR`, else `~/.deephaven/cli` (`%APPDATA%\Deephaven\cli` on Windows). */
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

/** `config.json` and `credentials.json` in `dir`, always read and written together. */
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

  /** Both files; missing files read as empty. Refuses a `credentials.json` other users can read. */
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
    const lock = join(this.dir, "config.lock");
    return await withFileLock(lock, LOCK_WAIT_MS, async () => {
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

/** Parsed JSON at `path`, or undefined if the file doesn't exist. */
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

/** Writes `value` as JSON via a temp file and rename, so readers never see half a file. */
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
