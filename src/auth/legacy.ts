import { readdir, readFile, stat } from "node:fs/promises";
import { dirname, join } from "node:path";
import { DhError, isErrno, reasonOf } from "../errors.ts";
import type { Kind } from "./store.ts";

/** Where a legacy config kept a secret: inline, in an env var, or in a file. */
export type SecretRef =
  | { literal: string }
  | { env: string }
  | {
      file: string;
    };

/** How a legacy entry signed in, as its config describes it. */
export type LegacyPlan =
  | {
      method: "password";
      username: string;
      password: SecretRef;
      operateAs?: string;
    }
  | { method: "private-key"; keyPath?: string; keyText?: string }
  | { method: "anonymous" }
  | { method: "psk"; secret: SecretRef }
  /** Without `username`, v1 keeps `user:password` in `secret`. */
  | { method: "basic"; username?: string; secret: SecretRef }
  | { method: "custom"; handler: string; secret: SecretRef };

/** One Community session or Enterprise system from a legacy config. */
export interface LegacyItem {
  /** The session or system name in the old config. */
  name: string;
  /** Where in the config it came from, e.g. `community.sessions.prod`; part of the migration marker. */
  source: string;
  kind: Kind;
  origin: string;
  /** CA certificate path the old config trusted. */
  caCert?: string;
  plan: LegacyPlan;
}

/** A legacy config, found and parsed. */
export interface LegacyConfig {
  /** The file (v1) or directory (v2) it was read from. */
  path: string;
  items: LegacyItem[];
  /** Entries that need no import (session creation, Docker, timeouts). */
  notImported: string[];
  /** The default system or session; names can repeat across the two. */
  defaultItem?: { name: string; kind: Kind };
}

// biome-ignore lint/suspicious/noExplicitAny: untyped legacy config files.
type Json = any;

const WINDOWS = process.platform === "win32";

/** The Deephaven MCP data folder: `$DH_AI_DATA_DIR`, else `~/.deephaven/ai` (`%APPDATA%\Deephaven\ai` on Windows). */
export function legacyDataRoot(): string {
  const override = process.env.DH_AI_DATA_DIR;
  if (override) return override;
  const appData = process.env.APPDATA;
  if (WINDOWS && appData) return join(appData, "Deephaven", "ai");
  return join(
    (WINDOWS ? process.env.USERPROFILE : process.env.HOME) ?? "",
    ".deephaven",
    "ai",
  );
}

/** `${env:VAR}` / `${file:/path}` templates, or a literal. */
export function secretRef(value: unknown): SecretRef | undefined {
  if (typeof value !== "string" || value === "") return undefined;
  const env = value.match(/^\$\{env:([^}]+)\}$/)?.[1];
  if (env) return { env };
  const file = value.match(/^\$\{file:([^}]+)\}$/)?.[1];
  if (file) return { file };
  return { literal: value };
}

/** A Community session's origin from its host, optional port and TLS setting. */
function origin(host: string, port: unknown, tls: boolean): string {
  const scheme = tls ? "https" : "http";
  return new URL(`${scheme}://${host}${port ? `:${port}` : ""}`).origin;
}

/** An Enterprise system's origin from its `connection_json_url`; `where` names the entry in errors. */
function enterpriseOrigin(url: string, where: string): string {
  try {
    return new URL(url).origin;
  } catch {
    throw new DhError("usage", `${where}: invalid connection_json_url ${url}`);
  }
}

/** A file path from a config value (a plain path or `${file:...}`); env refs aren't paths. */
function fileRef(value: unknown): string | undefined {
  const ref = secretRef(value);
  if (ref && "file" in ref) return ref.file;
  if (ref && "literal" in ref) return ref.literal;
  return undefined;
}

// --- v1: one JSON5 file -----------------------------------------------------

/** One v1 `community.sessions` entry, named `name`. */
function v1Community(name: string, cfg: Json): LegacyItem {
  const where = `community.sessions.${name}`;
  const authType = String(cfg.auth_type ?? "Anonymous");
  const token: SecretRef | undefined = cfg.auth_token_env_var
    ? { env: cfg.auth_token_env_var }
    : secretRef(cfg.auth_token);
  const tls = Boolean(cfg.use_tls || cfg.tls_root_certs);
  const base = {
    name,
    source: where,
    kind: "community" as const,
    origin: origin(cfg.host ?? "localhost", cfg.port, tls),
    caCert: fileRef(cfg.tls_root_certs),
  };
  switch (authType.toLowerCase()) {
    case "anonymous":
      return { ...base, plan: { method: "anonymous" } };
    case "psk":
      return {
        ...base,
        plan: { method: "psk", secret: token ?? { literal: "" } },
      };
    case "basic":
      return {
        ...base,
        plan: { method: "basic", secret: token ?? { literal: "" } },
      };
    default:
      return {
        ...base,
        plan: {
          method: "custom",
          handler: authType,
          secret: token ?? { literal: "" },
        },
      };
  }
}

/** One v1 `enterprise.systems` entry, named `name`. */
function v1Enterprise(name: string, cfg: Json): LegacyItem {
  const where = `enterprise.systems.${name}`;
  const base = {
    name,
    source: where,
    kind: "enterprise" as const,
    origin: enterpriseOrigin(cfg.connection_json_url, where),
  };
  if (cfg.auth_type === "private_key") {
    return {
      ...base,
      plan: { method: "private-key", keyPath: cfg.private_key_path },
    };
  }
  return {
    ...base,
    plan: {
      method: "password",
      username: cfg.username,
      password: cfg.password_env_var
        ? { env: cfg.password_env_var }
        : (secretRef(cfg.password) ?? { literal: "" }),
      operateAs: cfg.effective_user,
    },
  };
}

/** Parses a v1 config (one JSON5 file, read from `path`) into the entries to migrate. */
export function parseV1(text: string, path: string): LegacyConfig {
  let cfg: Json;
  try {
    cfg = Bun.JSONC.parse(text);
  } catch (e) {
    throw new DhError(
      "usage",
      `${path} is not valid JSON5`,
      undefined,
      reasonOf(e),
    );
  }
  const items = [
    ...Object.entries(cfg?.community?.sessions ?? {}).map(([n, c]) =>
      v1Community(n, c),
    ),
    ...Object.entries(cfg?.enterprise?.systems ?? {}).map(([n, c]) =>
      v1Enterprise(n, c),
    ),
  ];
  const notImported = [
    ...(cfg?.community?.session_creation ? ["community session creation"] : []),
    ...Object.entries(cfg?.enterprise?.systems ?? {})
      .filter(([, c]: [string, Json]) => c.session_creation)
      .map(([n]) => `enterprise.systems.${n} session creation`),
  ];
  return { path, items, notImported };
}

// --- v2: a directory tree ----------------------------------------------------

/** Every `*.json` file in `dir`, parsed, with its path; a missing `dir` is empty. */
async function readJsonFiles(dir: string): Promise<[string, Json][]> {
  const out: [string, Json][] = [];
  try {
    for (const entry of await readdir(dir, { withFileTypes: true })) {
      if (!entry.isFile() || !entry.name.endsWith(".json")) continue;
      const file = join(dir, entry.name);
      out.push([file, JSON.parse(await readFile(file, "utf8"))]);
    }
  } catch (e) {
    if (!isErrno(e, "ENOENT")) throw e;
  }
  return out.sort(([a], [b]) => a.localeCompare(b));
}

/** One v2 `community/sessions/*.json` file; the name defaults to the file's. */
function v2Community(file: string, cfg: Json): LegacyItem {
  const name = cfg.session_name ?? file.replace(/^.*[\\/]|\.json$/g, "");
  const cred = cfg.auth?.credentials ?? { type: "anonymous" };
  const base = {
    name,
    source: `community/sessions/${name}.json`,
    kind: "community" as const,
    origin: origin(cfg.host ?? "localhost", cfg.port, Boolean(cfg.tls)),
    caCert: fileRef(cfg.tls?.root_certs),
  };
  const ref = (v: unknown) => secretRef(v) ?? { literal: "" };
  switch (cred.type) {
    case "psk":
      return { ...base, plan: { method: "psk", secret: ref(cred.token) } };
    case "password":
      return {
        ...base,
        plan: {
          method: "basic",
          username: cred.username,
          secret: ref(cred.password),
        },
      };
    case "custom":
      return {
        ...base,
        plan: {
          method: "custom",
          handler: cred.auth_type,
          secret: ref(cred.auth_token),
        },
      };
    default:
      return { ...base, plan: { method: "anonymous" } };
  }
}

/** One v2 `enterprise/systems/*.json` file; the name defaults to the file's. */
function v2Enterprise(file: string, cfg: Json): LegacyItem {
  const name = cfg.system_name ?? file.replace(/^.*[\\/]|\.json$/g, "");
  const source = `enterprise/systems/${name}.json`;
  const cred = cfg.auth?.credentials ?? {};
  const base = {
    name,
    source,
    kind: "enterprise" as const,
    origin: enterpriseOrigin(cfg.connection_json_url, source),
    caCert: fileRef(cfg.tls?.root_certs),
  };
  if (cred.type === "private_key") {
    const ref = secretRef(cred.key_text);
    if (ref && "file" in ref) {
      return { ...base, plan: { method: "private-key", keyPath: ref.file } };
    }
    const keyText = ref && "literal" in ref ? ref.literal : undefined;
    return { ...base, plan: { method: "private-key", keyText } };
  }
  return {
    ...base,
    plan: {
      method: "password",
      username: cred.username,
      password: secretRef(cred.password) ?? { literal: "" },
      operateAs: cred.effective_user,
    },
  };
}

/** Parses a v2 config tree (one file per session or system under `configDir`). */
export async function parseV2(configDir: string): Promise<LegacyConfig> {
  const sessions = await readJsonFiles(
    join(configDir, "community", "sessions"),
  );
  const systems = await readJsonFiles(join(configDir, "enterprise", "systems"));
  const items = [
    ...sessions.map(([f, c]) => v2Community(f, c)),
    ...systems.map(([f, c]) => v2Enterprise(f, c)),
  ];
  const settings = await readFile(
    join(configDir, "community", "settings.json"),
    "utf8",
  ).then(JSON.parse, () => ({}));
  const notImported = [
    ...(settings.session_creation ? ["community session creation"] : []),
    ...systems
      .filter(([, c]) => c.session_creation)
      .map(([, c]) => `enterprise/systems/${c.system_name} session creation`),
  ];
  const context = await readFile(
    join(dirname(configDir), "runtime", "context.json"),
    "utf8",
  ).then(JSON.parse, () => ({}));
  return {
    path: configDir,
    items,
    notImported,
    defaultItem: defaultFromContext(context),
  };
}

/** The v2 runtime context's current system (preferred) or session, if any. */
function defaultFromContext(context: Json): LegacyConfig["defaultItem"] {
  if (context.system) return { name: context.system, kind: "enterprise" };
  if (context.session) return { name: context.session, kind: "community" };
  return undefined;
}

/** Whether `path` is a file, a directory, or missing. */
async function kindOf(path: string): Promise<"file" | "dir" | undefined> {
  const info = await stat(path).catch(() => undefined);
  if (info?.isDirectory()) return "dir";
  if (info?.isFile()) return "file";
  return undefined;
}

/** `from`, else `$DH_MCP_CONFIG_FILE` (v1), else the v2 tree. */
export async function findLegacy(
  from?: string,
): Promise<LegacyConfig | undefined> {
  const candidates = from
    ? [from]
    : [process.env.DH_MCP_CONFIG_FILE, join(legacyDataRoot(), "config")].filter(
        (p): p is string => Boolean(p),
      );
  for (const path of candidates) {
    const kind = await kindOf(path);
    if (kind === "file") return parseV1(await readFile(path, "utf8"), path);
    if (kind === "dir") return await parseV2(path);
  }
  if (from) throw new DhError("usage", `Not found: ${from}`);
  return undefined;
}

/** The secret's value now; undefined if it's empty or its env var isn't set. */
export async function resolveSecret(
  ref: SecretRef,
): Promise<string | undefined> {
  if ("literal" in ref) return ref.literal || undefined;
  if ("env" in ref) return process.env[ref.env] || undefined;
  return (await readFile(ref.file, "utf8")).trim();
}

/** Where a secret comes from, for {@link describePlan}: ` from $VAR`, ` from <file>`, or nothing. */
function secretSource(ref: SecretRef): string {
  if ("env" in ref) return ` from $${ref.env}`;
  if ("file" in ref) return ` from ${ref.file}`;
  return "";
}

/** A short description of how `item` will sign in, for the migrate picker and progress lines. */
export function describePlan(item: LegacyItem): string {
  const plan = item.plan;
  switch (plan.method) {
    case "password":
      return "sign in with the saved password, then authorize this computer";
    case "private-key":
      return plan.keyPath ? `key file ${plan.keyPath}` : "saved key";
    case "anonymous":
      return "anonymous";
    case "psk":
      return `PSK${secretSource(plan.secret)}`;
    case "basic":
      return `username/password${secretSource(plan.secret)}`;
    case "custom":
      return `${plan.handler}${secretSource(plan.secret)}`;
  }
}
