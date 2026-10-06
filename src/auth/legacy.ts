import { dirname, join } from "@std/path";
import { parse as parseJsonc } from "@std/jsonc";
import { DhError } from "./errors.ts";
import type { Kind } from "./store.ts";

export type SecretRef = { literal: string } | { env: string } | {
  file: string;
};

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
  | { method: "basic"; username?: string; secret: SecretRef }
  | { method: "custom"; handler: string; secret: SecretRef };

export interface LegacyItem {
  name: string;
  source: string;
  kind: Kind;
  origin: string;
  caCert?: string;
  plan: LegacyPlan;
}

export interface LegacyConfig {
  path: string;
  items: LegacyItem[];
  /** Entries that need no import (session creation, Docker, timeouts). */
  notImported: string[];
  /** Legacy name of the default system or session. */
  defaultName?: string;
}

// deno-lint-ignore no-explicit-any
type Json = any;

const WINDOWS = Deno.build.os === "windows";

export function legacyDataRoot(): string {
  const override = Deno.env.get("DH_AI_DATA_DIR");
  if (override) return override;
  const appData = Deno.env.get("APPDATA");
  if (WINDOWS && appData) return join(appData, "Deephaven", "ai");
  return join(
    Deno.env.get(WINDOWS ? "USERPROFILE" : "HOME") ?? "",
    ".deephaven",
    "ai",
  );
}

/** `${env:VAR}` / `${file:/path}` templates, or a literal. */
export function secretRef(value: unknown): SecretRef | undefined {
  if (typeof value !== "string" || value === "") return undefined;
  const env = value.match(/^\$\{env:([^}]+)\}$/);
  if (env) return { env: env[1] };
  const file = value.match(/^\$\{file:([^}]+)\}$/);
  if (file) return { file: file[1] };
  return { literal: value };
}

function origin(host: string, port: unknown, tls: boolean): string {
  const scheme = tls ? "https" : "http";
  return new URL(`${scheme}://${host}${port ? `:${port}` : ""}`).origin;
}

function enterpriseOrigin(url: string, where: string): string {
  try {
    return new URL(url).origin;
  } catch {
    throw new DhError("usage", `${where}: invalid connection_json_url ${url}`);
  }
}

function fileRef(value: unknown): string | undefined {
  const ref = secretRef(value);
  if (!ref) return undefined;
  return "file" in ref ? ref.file : "literal" in ref ? ref.literal : undefined;
}

// --- v1: one JSON5 file -----------------------------------------------------

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
        : secretRef(cfg.password) ?? { literal: "" },
      operateAs: cfg.effective_user,
    },
  };
}

export function parseV1(text: string, path: string): LegacyConfig {
  let cfg: Json;
  try {
    cfg = parseJsonc(text);
  } catch {
    throw new DhError("usage", `${path} is not valid JSON5`);
  }
  const items = [
    ...Object.entries(cfg?.community?.sessions ?? {}).map(([n, c]) =>
      v1Community(n, c)
    ),
    ...Object.entries(cfg?.enterprise?.systems ?? {}).map(([n, c]) =>
      v1Enterprise(n, c)
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

async function readJsonFiles(dir: string): Promise<[string, Json][]> {
  const out: [string, Json][] = [];
  try {
    for await (const entry of Deno.readDir(dir)) {
      if (!entry.isFile || !entry.name.endsWith(".json")) continue;
      const file = join(dir, entry.name);
      out.push([file, JSON.parse(await Deno.readTextFile(file))]);
    }
  } catch (e) {
    if (!(e instanceof Deno.errors.NotFound)) throw e;
  }
  return out.sort(([a], [b]) => a.localeCompare(b));
}

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
    return {
      ...base,
      plan: ref && "file" in ref
        ? { method: "private-key", keyPath: ref.file }
        : {
          method: "private-key",
          keyText: ref && "literal" in ref ? ref.literal : undefined,
        },
    };
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

export async function parseV2(configDir: string): Promise<LegacyConfig> {
  const sessions = await readJsonFiles(
    join(configDir, "community", "sessions"),
  );
  const systems = await readJsonFiles(join(configDir, "enterprise", "systems"));
  const items = [
    ...sessions.map(([f, c]) => v2Community(f, c)),
    ...systems.map(([f, c]) => v2Enterprise(f, c)),
  ];
  const settings = await Deno.readTextFile(
    join(configDir, "community", "settings.json"),
  ).then(JSON.parse, () => ({}));
  const notImported = [
    ...(settings.session_creation ? ["community session creation"] : []),
    ...systems
      .filter(([, c]) => c.session_creation)
      .map(([, c]) => `enterprise/systems/${c.system_name} session creation`),
  ];
  const context = await Deno.readTextFile(
    join(dirname(configDir), "runtime", "context.json"),
  ).then(JSON.parse, () => ({}));
  return {
    path: configDir,
    items,
    notImported,
    defaultName: context.system ?? context.session,
  };
}

async function kindOf(path: string): Promise<"file" | "dir" | undefined> {
  const stat = await Deno.stat(path).catch(() => undefined);
  return stat?.isDirectory ? "dir" : stat?.isFile ? "file" : undefined;
}

/** `from`, else `$DH_MCP_CONFIG_FILE` (v1), else the v2 tree. */
export async function findLegacy(
  from?: string,
): Promise<LegacyConfig | undefined> {
  const candidates = from
    ? [from]
    : [Deno.env.get("DH_MCP_CONFIG_FILE"), join(legacyDataRoot(), "config")]
      .filter((p): p is string => Boolean(p));
  for (const path of candidates) {
    const kind = await kindOf(path);
    if (kind === "file") return parseV1(await Deno.readTextFile(path), path);
    if (kind === "dir") return await parseV2(path);
  }
  if (from) throw new DhError("usage", `Not found: ${from}`);
  return undefined;
}

export async function resolveSecret(
  ref: SecretRef,
): Promise<string | undefined> {
  if ("literal" in ref) return ref.literal || undefined;
  if ("env" in ref) return Deno.env.get(ref.env) || undefined;
  return (await Deno.readTextFile(ref.file)).trim();
}

export function describePlan(item: LegacyItem): string {
  const plan = item.plan;
  const ref = (r: SecretRef) =>
    "env" in r ? ` from $${r.env}` : "file" in r ? ` from ${r.file}` : "";
  switch (plan.method) {
    case "password":
      return "sign in with the saved password, then authorize this computer";
    case "private-key":
      return plan.keyPath ? `key file ${plan.keyPath}` : "saved key";
    case "anonymous":
      return "anonymous";
    case "psk":
      return `PSK${ref(plan.secret)}`;
    case "basic":
      return `username/password${ref(plan.secret)}`;
    case "custom":
      return `${plan.handler}${ref(plan.secret)}`;
  }
}
