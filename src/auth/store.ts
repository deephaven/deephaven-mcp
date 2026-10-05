import { join } from "@std/path";
import { DhError } from "./errors.ts";

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
  | { type: "envRef"; env: string };

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
}

export interface State {
  config: Config;
  credentials: Credentials;
}

const WINDOWS = Deno.build.os === "windows";
const LOCK_WAIT_MS = 10_000;
const LOCK_STALE_MS = 30_000;

export function configDir(): string {
  const override = Deno.env.get("DH_CONFIG_DIR");
  if (override) return override;
  if (WINDOWS) {
    const appData = Deno.env.get("APPDATA");
    if (appData) return join(appData, "Deephaven", "cli");
  }
  const home = Deno.env.get(WINDOWS ? "USERPROFILE" : "HOME");
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
    await this.ensureDir();
    return await withLock(join(this.dir, "config.lock"), async () => {
      const state = await this.read();
      const value = await fn(state);
      await writeAtomic(this.configPath, state.config, 0o644);
      await writeAtomic(this.credentialsPath, state.credentials, 0o600);
      return value;
    });
  }

  /** Creates the directory owner-only. Call before anything writes into it. */
  async ensureDir(): Promise<void> {
    const existed = await Deno.stat(this.dir).then(() => true, () => false);
    if (!existed) await Deno.mkdir(this.dir, { recursive: true });
    if (!WINDOWS) await Deno.chmod(this.dir, 0o700);
    else if (!existed) await restrictWindowsAcl(this.dir);
  }

  private async assertPrivate(): Promise<void> {
    if (WINDOWS) return;
    const { mode } = await Deno.stat(this.credentialsPath);
    if (mode !== null && (mode & 0o077) !== 0) {
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
    text = await Deno.readTextFile(path);
  } catch (e) {
    if (e instanceof Deno.errors.NotFound) return undefined;
    throw e;
  }
  try {
    return JSON.parse(text) as T;
  } catch {
    throw new DhError("internal", `${path} is not valid JSON`);
  }
}

async function writeAtomic(
  path: string,
  value: unknown,
  mode: number,
): Promise<void> {
  const tmp = `${path}.${Deno.pid}.${crypto.randomUUID()}.tmp`;
  try {
    const file = await Deno.open(tmp, { createNew: true, write: true, mode });
    try {
      await file.write(
        new TextEncoder().encode(JSON.stringify(value, null, 2) + "\n"),
      );
      await file.sync();
    } finally {
      file.close();
    }
    if (!WINDOWS) await Deno.chmod(tmp, mode);
    await Deno.rename(tmp, path);
  } catch (e) {
    await Deno.remove(tmp).catch(() => {});
    throw e;
  }
}

/** Owner-only, inherited by everything created in `dir`. */
async function restrictWindowsAcl(dir: string): Promise<void> {
  const user = Deno.env.get("USERNAME");
  const { success } = await new Deno.Command("icacls", {
    args: [dir, "/inheritance:r", "/grant:r", `${user}:(OI)(CI)F`],
    stdout: "null",
    stderr: "null",
  }).output();
  if (!success) {
    throw new DhError("internal", `Could not restrict access to ${dir}`);
  }
}

async function withLock<T>(path: string, fn: () => Promise<T>): Promise<T> {
  const deadline = Date.now() + LOCK_WAIT_MS;
  for (;;) {
    try {
      (await Deno.open(path, { createNew: true, write: true })).close();
      break;
    } catch (e) {
      if (!(e instanceof Deno.errors.AlreadyExists)) throw e;
      const mtime = (await Deno.stat(path).catch(() => null))?.mtime;
      if (mtime && Date.now() - mtime.getTime() > LOCK_STALE_MS) {
        await Deno.remove(path).catch(() => {});
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
    await Deno.remove(path).catch(() => {});
  }
}
