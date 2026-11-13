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
import { DhError, isErrno, reasonOf } from "../../errors.ts";
import { withFileLock } from "./lock.ts";

/** Which Deephaven product a server is. */
export type Kind = "enterprise" | "community";
/**
 * How a profile signs in. `saml`, `password` and `private-key` are
 * Enterprise; `ssh` is SSH access only, with no sign-in; the rest are
 * Community.
 */
export type Method =
  | "saml"
  | "password"
  | "private-key"
  | "psk"
  | "basic"
  | "anonymous"
  | "custom"
  | "ssh";

/** What's known about a server, keyed by origin in {@link Config.servers}. */
export interface ServerEntry {
  kind: Kind;
  /** Absolute path of a CA certificate to trust for it (`--ca-cert`). */
  caCert?: string;
  /** The legacy config entry it was first migrated from. */
  importedFrom?: string;
  /** SSH access to the cluster's nodes: where `shell` plugins are discovered and run. */
  ssh?: ServerSsh;
  /** Per-plugin overrides of `ssh`, by plugin name. */
  plugins?: Record<string, ServerPluginSettings>;
}

/** A server's SSH settings, shared by every profile on it. */
export interface ServerSsh {
  /** `shell` plugins are discovered and run only when true; false records a declined offer. */
  enabled?: boolean;
  /** SSH destinations, each passed to `ssh` as given. Unset: the server URL's hostname. */
  hosts?: string[];
  /** One of `hosts`, where runs go without `--ssh-host`. Unset, or not listed: the first. */
  defaultHost?: string;
  /** The bundle's path on the nodes; `shell/` is under it. Unset: `/usr/illumon/latest/dh-cli`. */
  bundle?: string;
}

/** One server's overrides for one plugin, shared by every profile on that server. */
export interface ServerPluginSettings {
  /** The SSH destinations the plugin runs on instead of the server's `ssh.hosts`. */
  sshHosts?: string[];
  /** One of `sshHosts`, where runs go without `--ssh-host`. Unset, or not listed: the first. */
  defaultSshHost?: string;
  /**
   * The one SSH destination, as older builds wrote it. Read as `sshHosts:
   * [host]` when `sshHosts` is unset; removed on the next write to the entry.
   */
  host?: string;
}

/** One saved identity on one server. Its credential is stored separately, under the same id. */
export interface Profile {
  /** Unique, user-facing; `<host>:<user>` unless renamed. */
  name: string;
  /** The server's origin, a key of {@link Config.servers}. */
  server: string;
  /** Unset for an SSH-only profile (`method: "ssh"`). */
  user?: string;
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
  /** This machine's settings for `shell` plugins from a local bundle, by plugin name. */
  plugins?: Record<string, PluginSettings>;
}

/** This machine's settings for one plugin. */
export interface PluginSettings {
  /** Replaces the spec's `tool.path` on this machine; local runs only. */
  path?: string;
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
  /** Set while a sign-in is still authorizing the key; it's left alone until then. */
  pendingUntil?: string;
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
  #dir: string | undefined;

  /** `dir` defaults to {@link configDir}, found on first use so env-only sign-in needs no home directory. */
  constructor(dir?: string) {
    this.#dir = dir;
  }

  get dir(): string {
    this.#dir ??= configDir();
    return this.#dir;
  }

  get configPath(): string {
    return join(this.dir, "config.json");
  }

  get credentialsPath(): string {
    return join(this.dir, "credentials.json");
  }

  get consoleDataPath(): string {
    return join(this.dir, "consoles.json");
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
      dropRemovedSettings(state.config);
      await writeAtomic(this.configPath, state.config, 0o644);
      await writeAtomic(this.credentialsPath, state.credentials, 0o600);
      return value;
    });
  }

  /** Reads the owner-only console registry, or a fresh copy of `empty`. */
  async readConsoleData<T extends object>(empty: T): Promise<T> {
    const data = await readJson<T>(this.consoleDataPath);
    if (!data) return structuredClone(empty);
    await this.assertPrivatePath(this.consoleDataPath);
    return Object.assign(structuredClone(empty), data);
  }

  /** Updates the owner-only console registry under the profile-store lock. */
  async updateConsoleData<T extends object, R = void>(
    empty: T,
    fn: (data: T) => R | Promise<R>,
  ): Promise<R> {
    await this.makeDir(false);
    const lock = join(this.dir, "config.lock");
    return await withFileLock(lock, LOCK_WAIT_MS, async () => {
      const data = await this.readConsoleData(empty);
      const result = await fn(data);
      await writeAtomic(this.consoleDataPath, data, 0o600);
      return result;
    });
  }

  /** `fn` under the lock {@link update} takes, for other files in the config directory. */
  async withLock<T>(fn: () => Promise<T>): Promise<T> {
    await this.makeDir(false);
    return await withFileLock(join(this.dir, "config.lock"), LOCK_WAIT_MS, fn);
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
    await this.assertPrivatePath(this.credentialsPath);
  }

  private async assertPrivatePath(path: string): Promise<void> {
    if (WINDOWS) return;
    const { mode } = await stat(path);
    if ((mode & 0o077) !== 0) {
      throw new DhError(
        "credential_unavailable",
        `${path} can be read by other users`,
        `Run: chmod 600 ${path}`,
      );
    }
  }
}

/**
 * Removes plugin settings older builds wrote and nothing reads any more:
 * `exec`, `prefix`, version pins and the declined SSH offer.
 */
function dropRemovedSettings(config: Config): void {
  const removed = [
    "exec",
    "prefix",
    "version",
    "detected",
    "realPath",
    "checkedAt",
  ];
  const clean = (settings: Record<string, Record<string, unknown>>) => {
    for (const [name, s] of Object.entries(settings)) {
      for (const key of removed) delete s[key];
      if (Object.keys(s).length === 0) delete settings[name];
    }
  };
  delete (config as { sshOfferDeclined?: unknown }).sshOfferDeclined;
  if (config.plugins) {
    clean(config.plugins as Record<string, Record<string, unknown>>);
    if (Object.keys(config.plugins).length === 0) delete config.plugins;
  }
  for (const server of Object.values(config.servers)) {
    if (!server.plugins) continue;
    clean(server.plugins as Record<string, Record<string, unknown>>);
    if (Object.keys(server.plugins).length === 0) delete server.plugins;
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
