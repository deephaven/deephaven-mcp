/**
 * Which plugins this run has, read at startup from the local bundle and the
 * caches only: no network, no `ssh` (plans/cluster-plugins.md, "Startup never
 * connects"). Never fails; what can't be used is listed with its reason.
 */
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { DhError, reasonOf } from "../../errors.ts";
import { VERSION } from "../../version.ts";
import type { Config } from "../auth/store.ts";
import {
  type Channel,
  DEFAULT_BUNDLE,
  dhSatisfies,
  type EntryProblem,
  type Manifest,
  type ManifestEntry,
  type ManifestRead,
  readManifest,
} from "./bundle.ts";
import { cachedFile, type PartState, readCachedPart } from "./cache.ts";
import {
  type PluginJson,
  parsePluginJson,
  SUPPORTED_APIS,
} from "./plugin-json.ts";
import { accountIn, CLUSTER_CNF } from "./run-as.ts";
import {
  type ActiveProfile,
  type Exec,
  localClusterOrigin,
  serverSsh,
} from "./settings.ts";
import { type PluginSpec, parseSpec, type RunAs } from "./spec.ts";

/** Nouns `dh` may add itself later; a plugin can't take them, so a release can't break it. */
export const RESERVED_NAMES = [
  "help",
  "config",
  "query",
  "pq",
  "table",
  "session",
];

/** Where a part comes from. */
export type SourceKind =
  /** `DH_PLUGIN_BUNDLE`. */
  | "env"
  /** This node's install. */
  | "local"
  /** Copied over SSH from the profile's server, and cached. */
  | "ssh"
  /** Fetched from the profile's server over the web, and cached. */
  | "web";

/** One part (channel) of a bundle, as found at startup. */
export interface PluginSource {
  channel: Channel;
  kind: SourceKind;
  /** The part's directory: in place for `env` and `local`, the cache's for `ssh`. */
  dir: string;
  /** For `ssh`: the profile's server. */
  origin?: string;
  /** For `ssh`: the cache's record of the last copy. */
  state?: PartState;
  manifest?: Manifest;
  /** Entries of `manifest` too broken to use. */
  problems?: EntryProblem[];
  /** Why there's no manifest, for `dh plugin list`. */
  missing?: string;
  /** The service accounts its tools run as, by role, where known. */
  accounts: Partial<Record<RunAs["account"], string>>;
}

/** Why a part has no plugins, when there's no source at all. */
export interface NoSource {
  channel: Channel;
  reason: string;
}

/** A plugin from a manifest, usable or not. */
export interface Plugin {
  name: string;
  channel: Channel;
  source: PluginSource;
  /** The manifest entry; unset only for an entry too broken to read. */
  entry?: ManifestEntry;
  /** Where it runs: a `shell` plugin's tool here or over SSH, which follows from where it was found; a `js` plugin in `dh`. */
  exec: Exec | "js";
  /** `shell`: its spec, once read. */
  spec?: PluginSpec;
  /** `js`: its `plugin.json`, once read. */
  pluginJson?: PluginJson;
  /** `ready`: registered. `unavailable`: its nouns give `plugin_unavailable`. `invalid`: not registered. */
  state: "ready" | "unavailable" | "invalid";
  /** Why it isn't ready, as `dh plugin list` shows it. */
  reason?: string;
}

export interface Discovery {
  sources: PluginSource[];
  noSource: NoSource[];
  /** An SSH-only profile: the `js` plugins a sign-in would add (from the cached manifest; none limited by `groups`). */
  signIn?: { profile: string; plugins: string[] };
  plugins: Plugin[];
  /** `dh`'s own commands, which plugins can't be named. */
  builtins: string[];
}

/** The plugins this run has; `active` is the active profile, if any. */
export async function discoverPlugins(
  config: Config,
  active: ActiveProfile | undefined,
  builtins: string[],
): Promise<Discovery> {
  const noSource: NoSource[] = [];
  const shell = await shellSource(config, active, noSource);
  const signIn = await sshOnly(active, noSource);
  const js = signIn ? undefined : await jsSource(active, noSource);
  const sources = [shell, js].filter((s) => s !== undefined);
  const plugins: Plugin[] = [];
  for (const source of sources) plugins.push(...(await pluginsOf(source)));
  checkNames(plugins, builtins);
  return {
    sources,
    noSource,
    plugins,
    builtins,
    ...(signIn ? { signIn } : {}),
  };
}

/**
 * For an SSH-only profile: no `js` plugins (they need a sign-in), but the
 * names its server's cached manifest lists, except those limited by groups.
 */
async function sshOnly(
  active: ActiveProfile | undefined,
  noSource: NoSource[],
): Promise<Discovery["signIn"]> {
  if (active?.profile.method !== "ssh") return undefined;
  noSource.push({ channel: "js", reason: "no sign-in" });
  const cached = await readCachedPart(active.profile.server, "js");
  const plugins = cached?.manifest?.ok
    ? cached.manifest.manifest.plugins
        .filter((p) => !p.groups)
        .map((p) => p.name)
    : [];
  return { profile: active.profile.name, plugins };
}

/**
 * The `shell` part: `DH_PLUGIN_BUNDLE`'s; else this node's install, unless
 * the active profile is another cluster's; else the cache of the profile's
 * server, when SSH is enabled for it.
 */
async function shellSource(
  config: Config,
  active: ActiveProfile | undefined,
  noSource: NoSource[],
): Promise<PluginSource | undefined> {
  const env = process.env.DH_PLUGIN_BUNDLE;
  if (env) return await partInPlace("env", join(env, "shell"), "shell");
  const local = await partInPlace(
    "local",
    join(DEFAULT_BUNDLE, "shell"),
    "shell",
  );
  const onThisCluster =
    !active || (await localClusterOrigin()) === active.profile.server;
  if (local.manifest && onThisCluster) return local;
  if (!active) {
    noSource.push({ channel: "shell", reason: "no profile" });
    return undefined;
  }
  const origin = active.profile.server;
  if (!serverSsh(config, origin).enabled) {
    noSource.push({ channel: "shell", reason: "SSH not enabled" });
    return undefined;
  }
  const cached = await readCachedPart(origin, "shell");
  if (!cached) {
    return {
      channel: "shell",
      kind: "ssh",
      dir: "",
      origin,
      missing: "not copied yet",
      accounts: {},
    };
  }
  return {
    channel: "shell",
    kind: "ssh",
    dir: cached.dir,
    origin,
    state: cached.state,
    accounts: cached.state.accounts ?? {},
    ...fromRead(cached.manifest, cached.state),
  };
}

/**
 * The `js` part: `DH_PLUGIN_BUNDLE`'s, read in place; else the cache of the
 * profile's server.
 */
async function jsSource(
  active: ActiveProfile | undefined,
  noSource: NoSource[],
): Promise<PluginSource | undefined> {
  const env = process.env.DH_PLUGIN_BUNDLE;
  if (env) return await partInPlace("env", join(env, "js"), "js");
  if (!active) {
    noSource.push({ channel: "js", reason: "no profile" });
    return undefined;
  }
  const origin = active.profile.server;
  const cached = await readCachedPart(origin, "js");
  if (!cached) {
    return {
      channel: "js",
      kind: "web",
      dir: "",
      origin,
      missing: "not fetched yet",
      accounts: {},
    };
  }
  return {
    channel: "js",
    kind: "web",
    dir: cached.dir,
    origin,
    state: cached.state,
    accounts: {},
    ...fromRead(cached.manifest, cached.state),
  };
}

/** A part read in place, from `dir`. */
async function partInPlace(
  kind: SourceKind,
  dir: string,
  channel: Channel,
): Promise<PluginSource> {
  const cnf = await readFile(CLUSTER_CNF, "utf8").catch(() => undefined);
  const accounts = Object.fromEntries(
    (["admin", "merge", "query"] as const).map((r) => [r, accountIn(cnf, r)]),
  );
  const base = { channel, kind, dir, accounts };
  let text: string;
  try {
    text = await readFile(join(dir, "manifest.json"), "utf8");
  } catch {
    return { ...base, missing: `no manifest.json in ${dir}` };
  }
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch (e) {
    return { ...base, missing: `invalid: ${reasonOf(e)}` };
  }
  return { ...base, ...fromRead(readManifest(value, channel)) };
}

/** A source's manifest, or why there's none. */
function fromRead(
  read: ManifestRead | undefined,
  state?: PartState,
): Pick<PluginSource, "manifest" | "problems" | "missing"> {
  if (state?.revision === null) {
    return {
      missing: state.path
        ? `no dh-cli bundle at ${state.path} on ${state.host}`
        : "this server provides no dh plugins",
    };
  }
  if (!read) return { missing: "the cache is unreadable" };
  if (!read.ok) return { missing: `${read.status}: ${read.reason}` };
  return { manifest: read.manifest, problems: read.problems };
}

/** Where a plugin's file is: in place, or in the cache by digest. */
export function pluginFile(plugin: Plugin, path: string): string {
  const { source, entry } = plugin;
  const cached = source.kind === "ssh" || source.kind === "web";
  if (cached && entry) return cachedFile(source.dir, entry, path);
  return join(source.dir, path);
}

/** Each entry of `source`'s manifest, with its spec read. */
async function pluginsOf(source: PluginSource): Promise<Plugin[]> {
  const manifest = source.manifest;
  if (!manifest) return [];
  const shellExec: Exec = source.kind === "ssh" ? "ssh" : "local";
  const exec = source.channel === "js" ? "js" : shellExec;
  const plugins: Plugin[] = (source.problems ?? []).map((p) => ({
    name: p.name ?? "?",
    channel: source.channel,
    source,
    exec,
    state: "invalid",
    reason: p.reason,
  }));
  for (const entry of manifest.plugins) {
    // Hidden from users outside its groups: not registered, not listed.
    if (!inGroups(entry, source)) continue;
    const plugin: Plugin = {
      name: entry.name,
      channel: source.channel,
      source,
      entry,
      exec,
      state: "ready",
    };
    plugins.push(plugin);
    if (!dhSatisfies(entry, VERSION)) {
      plugin.state = "unavailable";
      plugin.reason = `needs dh ${entry.dh}`;
      continue;
    }
    try {
      const file = pluginFile(plugin, entry.spec);
      const value = JSON.parse(await readFile(file, "utf8"));
      if (source.channel === "js") {
        const json = parsePluginJson(value, file);
        if (json.name !== entry.name) {
          throw new DhError(
            "plugin_invalid",
            `its plugin.json is named "${json.name}"`,
          );
        }
        if (jsNouns(json).join() !== entry.nouns.join()) {
          throw new DhError(
            "plugin_invalid",
            "its plugin.json's nouns differ from the manifest's",
          );
        }
        plugin.pluginJson = json;
        if (!SUPPORTED_APIS.includes(json.api)) {
          plugin.state = "unavailable";
          plugin.reason = `unsupported api ${json.api}`;
        }
        continue;
      }
      const spec = parseSpec(value, file);
      if (spec.name !== entry.name) {
        throw new DhError("plugin_invalid", `its spec is named "${spec.name}"`);
      }
      if (nounsOf(spec).join() !== entry.nouns.join()) {
        throw new DhError(
          "plugin_invalid",
          "its spec's nouns differ from the manifest's",
        );
      }
      plugin.spec = spec;
    } catch (e) {
      plugin.state = "invalid";
      plugin.reason = e instanceof DhError ? e.message : reasonOf(e);
    }
  }
  return plugins;
}

/**
 * Whether the user may see `entry`: it has no `groups`, it's read in place
 * (`DH_PLUGIN_BUNDLE`), or the user was in one of them at the last fetch.
 */
function inGroups(entry: ManifestEntry, source: PluginSource): boolean {
  if (!entry.groups || source.kind !== "web") return true;
  const mine = new Set(source.state?.groups ?? []);
  return entry.groups.some((g) => mine.has(g));
}

/** The top-level nouns a `plugin.json` registers: its `nouns`, else its name. */
export function jsNouns(json: PluginJson): string[] {
  return json.nouns ? json.nouns.map((n) => n.name) : [json.name];
}

/** The top-level nouns a spec registers: its `nouns`, else its name. */
export function nounsOf(spec: PluginSpec): string[] {
  return spec.nouns ? spec.nouns.map((n) => n.name) : [spec.name];
}

/**
 * The name rules, per noun: a built-in or reserved noun rejects its plugin;
 * a noun two plugins register rejects both; a name used in both channels
 * rejects the `shell` one.
 */
function checkNames(plugins: Plugin[], builtins: string[]): void {
  const usable = () => plugins.filter((p) => p.state !== "invalid" && p.entry);
  for (const p of usable()) {
    if (p.channel !== "shell") continue;
    if (usable().some((o) => o.channel === "js" && o.name === p.name)) {
      reject(p, `"${p.name}" is also a js plugin`);
    }
  }
  const owners = new Map<string, Plugin[]>();
  for (const p of usable()) {
    for (const noun of p.entry?.nouns ?? []) {
      if (builtins.includes(noun)) reject(p, `"${noun}" is a built-in command`);
      else if (RESERVED_NAMES.includes(noun)) {
        reject(p, `"${noun}" is reserved for dh`);
      }
      owners.set(noun, [...(owners.get(noun) ?? []), p]);
    }
  }
  for (const [noun, ps] of owners) {
    if (ps.length < 2) continue;
    const names = ps.map((p) => p.name).join(" and ");
    for (const p of ps) reject(p, `"${noun}" is registered by ${names}`);
  }
}

function reject(plugin: Plugin, reason: string): void {
  if (plugin.state === "invalid") return;
  plugin.state = "invalid";
  plugin.reason = reason;
}
