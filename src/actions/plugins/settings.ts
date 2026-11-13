import { readFile, stat } from "node:fs/promises";
import { join } from "node:path";
import { DhError, isErrno, reasonOf } from "../../errors.ts";
import { BIN } from "../../version.ts";
import { findByName } from "../auth/profile.ts";
import {
  type Config,
  configDir,
  emptyState,
  type Profile,
  type ServerPluginSettings,
  type Store,
} from "../auth/store.ts";
import { DEFAULT_BUNDLE } from "./bundle.ts";
import type { PluginSpec } from "./spec.ts";

/** Where a `shell` plugin's tool runs: on this machine, or on a node over SSH. */
export type Exec = "local" | "ssh";

/** Where the DHE installer writes the address clients use, on every node. */
const FRONTEND_DIR = "/etc/deephaven";

/** `config.json` alone; `credentials.json` isn't needed for plugin settings. Missing reads as empty. */
export async function readConfig(store: Store): Promise<Config> {
  return await readConfigAt(store.configPath);
}

/**
 * `config.json`, for routing and help before parsing. Never fails: if it
 * can't be read, there's no profile and no SSH.
 */
export async function readConfigQuietly(): Promise<Config> {
  try {
    return await readConfigAt(join(configDir(), "config.json"));
  } catch {
    return emptyState().config;
  }
}

async function readConfigAt(path: string): Promise<Config> {
  const empty = emptyState().config;
  let text: string;
  try {
    text = await readFile(path, "utf8");
  } catch (e) {
    if (isErrno(e, "ENOENT")) return empty;
    throw e;
  }
  try {
    return { ...empty, ...(JSON.parse(text) as Config) };
  } catch (e) {
    throw new DhError(
      "internal",
      `${path} is not valid JSON`,
      undefined,
      reasonOf(e),
    );
  }
}

/** The active profile and its id. */
export interface ActiveProfile {
  id: string;
  profile: Profile;
}

/**
 * The active profile: `--profile`, else `DH_PROFILE` (if non-empty), else the
 * default profile; undefined if none. Names are looked up; an unknown one is
 * `usage`. No network: `DH_SERVER` alone doesn't count.
 */
export function activeProfile(
  config: Config,
  profileFlag: string | undefined,
): ActiveProfile | undefined {
  const name = profileFlag ?? (process.env.DH_PROFILE || undefined);
  if (name !== undefined) {
    const found = findByName(config, name);
    if (!found) {
      throw new DhError(
        "usage",
        `No profile named "${name}"`,
        `Run \`${BIN} auth\` to list profiles.`,
      );
    }
    return { id: found[0], profile: found[1] };
  }
  const id = config.defaultProfile;
  const profile = id ? config.profiles[id] : undefined;
  return id && profile ? { id, profile } : undefined;
}

/** {@link activeProfile}, or `auth_required` if there's none. */
export function requireProfile(
  config: Config,
  profileFlag: string | undefined,
): ActiveProfile {
  const active = activeProfile(config, profileFlag);
  if (active) return active;
  throw requireProfileError();
}

/** `auth_required`, for a plugin over SSH without a profile. */
export function requireProfileError(): DhError {
  return new DhError(
    "auth_required",
    "You're not logged in to a Deephaven server.",
    process.env.DH_SERVER
      ? `SSH plugins need a saved profile (DH_SERVER alone isn't enough). Run \`${BIN} auth login\`.`
      : `Run \`${BIN} auth login\` to get started.`,
  );
}

/** A server's SSH settings, with defaults filled in. */
export interface ServerSshSettings {
  enabled: boolean;
  /** `ssh.hosts`, else the URL's hostname. */
  hosts: string[];
  /** `ssh.defaultHost` if it's one of `hosts`, else the first. */
  defaultHost: string;
  /** The bundle's path on the nodes. */
  bundle: string;
}

/** `origin`'s SSH settings. */
export function serverSsh(config: Config, origin: string): ServerSshSettings {
  const ssh = config.servers[origin]?.ssh ?? {};
  const hosts =
    ssh.hosts && ssh.hosts.length > 0 ? ssh.hosts : [hostOf(origin)];
  return {
    enabled: ssh.enabled === true,
    hosts,
    defaultHost: pick(ssh.defaultHost, hosts),
    bundle: ssh.bundle ?? DEFAULT_BUNDLE,
  };
}

/** Where a plugin runs over SSH, for one server. */
export interface SshSettings {
  /** The server's origin, which keys `servers.<origin>`. */
  server: string;
  /** The plugin's own `sshHosts` (or the old `host`), else the server's. */
  hosts: string[];
  /** Where runs go without `--ssh-host`: the plugin's default, else the server's, if listed; else the first. */
  defaultHost: string;
  /** Whether `hosts` are the plugin's own. */
  override: boolean;
  /** The server's settings for the plugin, as stored. */
  stored: ServerPluginSettings;
}

/** The SSH settings for `plugin` on `profile`'s server. */
export function sshSettings(
  config: Config,
  profile: Profile,
  plugin: string,
): SshSettings {
  const server = serverSsh(config, profile.server);
  const stored = config.servers[profile.server]?.plugins?.[plugin] ?? {};
  const own = storedHosts(stored);
  const hosts = own ?? server.hosts;
  const wanted = stored.defaultSshHost;
  return {
    server: profile.server,
    hosts,
    defaultHost:
      wanted !== undefined && hosts.includes(wanted)
        ? wanted
        : pick(server.defaultHost, hosts),
    override: own !== undefined,
    stored,
  };
}

/** `wanted` if it's one of `hosts`, else the first. */
function pick(wanted: string | undefined, hosts: string[]): string {
  return wanted !== undefined && hosts.includes(wanted)
    ? wanted
    : (hosts[0] ?? "");
}

/** `sshHosts` if non-empty, else the old single `host`, else undefined. */
function storedHosts(stored: ServerPluginSettings): string[] | undefined {
  if (stored.sshHosts && stored.sshHosts.length > 0) return stored.sshHosts;
  return stored.host === undefined ? undefined : [stored.host];
}

/** `ssh`'s hosts with the default first, as shown and checked. */
export function hostsDefaultFirst(ssh: {
  hosts: string[];
  defaultHost: string;
}): string[] {
  return [ssh.defaultHost, ...ssh.hosts.filter((h) => h !== ssh.defaultHost)];
}

/**
 * `settings` as written from now on: an old `host` becomes `sshHosts`
 * (unless that's set) and is removed. Changes `settings` in place.
 */
export function upgradeServerPluginSettings(
  settings: ServerPluginSettings,
): ServerPluginSettings {
  if (settings.host !== undefined) {
    if (!settings.sshHosts || settings.sshHosts.length === 0) {
      settings.sshHosts = [settings.host];
    }
    delete settings.host;
  }
  return settings;
}

/** `origin`'s hostname, without an IPv6 address's brackets: `https://[::1]:8000` → `::1`. */
export function hostOf(origin: string): string {
  return new URL(origin).hostname.replace(/^\[(.*)\]$/, "$1");
}

/** The tool's path for a run on this machine: the machine's `path`, else the spec's. */
export function localPath(config: Config, spec: PluginSpec): string {
  return config.plugins?.[spec.name]?.path ?? spec.tool.path;
}

/** Whether a `local` tool is on this machine: false only when `stat` says it doesn't exist. */
export async function toolInstalled(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch (e) {
    return !isErrno(e, "ENOENT");
  }
}

/**
 * This node's cluster origin, `https://<FRONTEND_HOST>:<FRONTEND_PORT>`, from
 * the installer's files; undefined if they're missing or unreadable.
 */
export async function localClusterOrigin(): Promise<string | undefined> {
  try {
    const read = (f: string) =>
      readFile(join(FRONTEND_DIR, f), "utf8").then((t) => t.trim());
    const [host, port] = await Promise.all([
      read("FRONTEND_HOST"),
      read("FRONTEND_PORT"),
    ]);
    if (!host || !/^\d+$/.test(port)) return undefined;
    return new URL(`https://${host}:${port}`).origin;
  } catch {
    return undefined;
  }
}

/**
 * `config` with `origin`'s `ssh` changed: `enabled`, and `hosts` (replacing
 * the list) if given; a default host no longer listed is dropped.
 */
export function setServerSsh(
  config: Config,
  origin: string,
  change: { enabled: boolean; hosts?: string[] },
): Config {
  const entry = config.servers[origin];
  if (!entry) {
    throw new DhError(
      "internal",
      `The profile's server ${origin} is missing from config.json`,
      `Run \`${BIN} auth login\` again.`,
    );
  }
  const ssh = { ...entry.ssh, enabled: change.enabled };
  if (change.hosts !== undefined) {
    ssh.hosts = change.hosts;
    if (
      ssh.defaultHost !== undefined &&
      !change.hosts.includes(ssh.defaultHost)
    ) {
      delete ssh.defaultHost;
    }
  }
  return {
    ...config,
    servers: { ...config.servers, [origin]: { ...entry, ssh } },
  };
}
