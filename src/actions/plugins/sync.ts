/**
 * Bringing a server's cached `shell/` part up to date over SSH, and what
 * changed: for `dh plugin sync`, `dh plugin ssh enable`, login, a run that
 * found the bundle changed, and the periodic check.
 */

import type { CommandLifecycle } from "../../cli/lifecycle.ts";
import { DhError, reasonOf, timeoutMs } from "../../errors.ts";
import { fetchTrusted } from "../auth/jsapi.ts";
import { requireAuth } from "../auth/session.ts";
import { disconnectServer, type Session } from "../auth/signin.ts";
import type { Config, Store } from "../auth/store.ts";
import { type Manifest, readManifest } from "./bundle.ts";
import {
  readCachedPart,
  touchCachedPart,
  writeCachedPart,
  writeNoBundle,
} from "./cache.ts";
import { serverSsh } from "./settings.ts";
import { copyShellOverSsh, type ShellCopy } from "./ssh.ts";

/** What a sync of one part found. */
export interface PartSync {
  channel: "js" | "shell";
  /** `unchanged`, `updated`, or `none` (the server has no bundle). */
  status: "unchanged" | "updated" | "none";
  revision: string | null;
  /** Where it came from: the SSH host, or the server. */
  from: string;
  /** `shell`: the bundle's path on the host. */
  path?: string;
  added: string[];
  changed: string[];
  removed: string[];
}

/**
 * Copies `origin`'s `shell/` part over SSH from `host` (default: the
 * server's default host) into the cache, unless its revision is unchanged.
 * `prompt`: ssh may ask for a password (an interactive command); else
 * `BatchMode`.
 */
export async function syncShell(
  store: Store,
  config: Config,
  origin: string,
  opts: { host?: string; prompt: boolean; timeoutMs?: number },
): Promise<PartSync> {
  const ssh = serverSsh(config, origin);
  const host = opts.host ?? ssh.defaultHost;
  const shellDir = `${ssh.bundle}/shell`;
  const before = await readCachedPart(origin, "shell");
  const old = before?.manifest?.ok ? before.manifest.manifest : undefined;
  const copy = await copyShellOverSsh(
    host,
    shellDir,
    old?.revision,
    opts.prompt ? "prompt" : "batch",
    opts.timeoutMs,
  );
  const result = (status: PartSync["status"], now?: Manifest): PartSync => ({
    channel: "shell",
    status,
    revision: now?.revision ?? null,
    from: host,
    path: ssh.bundle,
    ...diff(old, now),
  });
  if (copy.kind === "unchanged") {
    await touchCachedPart(store, origin, "shell");
    return result("unchanged", old);
  }
  const fetchedAt = new Date().toISOString();
  if (copy.kind === "none") {
    await writeNoBundle(store, origin, "shell", {
      fetchedAt,
      host,
      path: ssh.bundle,
    });
    return result("none");
  }
  const manifest = await storeShellCopy(store, origin, host, copy, fetchedAt);
  return result("updated", manifest);
}

/**
 * Checks a copied `shell/` part and writes it to `origin`'s cache: its
 * manifest must be valid and match the revision, and every new plugin's
 * files their digest; else `plugin_invalid` and the cache is unchanged.
 */
export async function storeShellCopy(
  store: Store,
  origin: string,
  host: string,
  copy: ShellCopy & { kind: "copied" },
  fetchedAt = new Date().toISOString(),
): Promise<Manifest> {
  const rejected = (reason: string) =>
    new DhError(
      "plugin_invalid",
      `The dh plugins copied from ${host} were rejected: ${reason}`,
      undefined,
      undefined,
      { sshHost: host },
    );
  const text = copy.files.get("manifest.json");
  if (!text) throw rejected("no manifest.json");
  let raw: unknown;
  try {
    raw = JSON.parse(new TextDecoder().decode(text));
  } catch (e) {
    throw rejected(`manifest.json: ${reasonOf(e)}`);
  }
  const read = readManifest(raw, "shell");
  if (!read.ok) throw rejected(`manifest.json: ${read.reason}`);
  if (read.manifest.revision !== copy.revision) {
    throw rejected("manifest.json doesn't match the revision file");
  }
  await writeCachedPart(
    store,
    origin,
    "shell",
    read.manifest,
    raw,
    { fetchedAt, host },
    async (path) => {
      const bytes = copy.files.get(path);
      if (!bytes) throw rejected(`${path} is missing`);
      return bytes;
    },
  );
  return read.manifest;
}

/** Where a server serves its `js/` part. */
export function jsBase(origin: string): URL {
  return new URL("/iriside/cli-plugins/", origin);
}

/** `revision`'s state on the server: unchanged, gone (404), or this new revision. */
export type RevisionCheck =
  | { kind: "unchanged"; etag?: string }
  | { kind: "none" }
  | { kind: "changed"; revision: string; etag?: string };

/**
 * A conditional `GET revision` (no credentials), compared with `cached`:
 * a `304` or the same body is unchanged; a `404` means no bundle.
 */
export async function checkJsRevision(
  origin: string,
  cached: { revision?: string | null; etag?: string } | undefined,
  timeout = timeoutMs(),
): Promise<RevisionCheck> {
  const url = new URL("revision", jsBase(origin));
  const headers: Record<string, string> =
    cached?.revision && cached.etag ? { "If-None-Match": cached.etag } : {};
  const { res } = await fetchTrusted(
    url,
    AbortSignal.timeout(timeout),
    headers,
  );
  const etag = res.headers.get("etag") ?? undefined;
  if (res.status === 304) {
    await res.body?.cancel();
    return { kind: "unchanged", etag };
  }
  if (res.status === 404) {
    await res.body?.cancel();
    return { kind: "none" };
  }
  if (!res.ok) {
    await res.body?.cancel();
    throw new DhError(
      "server_unsupported",
      `HTTP ${res.status} fetching ${url}`,
    );
  }
  const revision = (await res.text()).trim();
  return revision === cached?.revision
    ? { kind: "unchanged", etag }
    : { kind: "changed", revision, etag };
}

/**
 * Fetches `origin`'s `js/` part into the cache, unless its revision is
 * unchanged: the manifest, then only the files of plugins not cached yet,
 * each verified against its digest. Where a plugin has `groups`, signs in
 * (as `profileFlag`'s profile) to record the user's groups; `groups: false`
 * skips that when nothing changed.
 */
export async function syncJs(
  store: Store,
  origin: string,
  opts: {
    profileFlag?: string;
    groups: boolean;
    timeoutMs?: number;
    /** Where the user's groups come from; default: signing in as `profileFlag`'s profile. */
    groupsOf?: () => Promise<string[]>;
    /** The command's, for signing in. */
    lifecycle?: CommandLifecycle;
  },
): Promise<PartSync & { manifest?: Manifest }> {
  const groupsOf =
    opts.groupsOf ??
    (() => userGroups(store, opts.profileFlag, opts.lifecycle));
  const timeout = opts.timeoutMs ?? timeoutMs();
  const before = await readCachedPart(origin, "js");
  const old = before?.manifest?.ok ? before.manifest.manifest : undefined;
  const result = (status: PartSync["status"], now?: Manifest) => ({
    channel: "js" as const,
    status,
    revision: now?.revision ?? null,
    from: origin,
    ...diff(old, now),
    ...(now ? { manifest: now } : {}),
  });
  const check = await checkJsRevision(
    origin,
    old && { revision: old.revision, etag: before?.state.etag },
    timeout,
  );
  const fetchedAt = new Date().toISOString();
  if (check.kind === "none") {
    await writeNoBundle(store, origin, "js", { fetchedAt });
    return result("none");
  }
  const grouped = (m: Manifest | undefined) =>
    m?.plugins.some((p) => p.groups) === true;
  if (check.kind === "unchanged") {
    const groups = opts.groups && grouped(old) ? await groupsOf() : undefined;
    await touchCachedPart(store, origin, "js", {
      ...(check.etag ? { etag: check.etag } : {}),
      ...(groups ? { groups } : {}),
    });
    return result("unchanged", old);
  }
  const base = jsBase(origin);
  const get = async (path: string): Promise<Uint8Array> => {
    const url = new URL(path, base);
    const { res } = await fetchTrusted(url, AbortSignal.timeout(timeout));
    if (!res.ok) {
      await res.body?.cancel();
      throw new DhError("plugin_invalid", `HTTP ${res.status} fetching ${url}`);
    }
    return new Uint8Array(await res.arrayBuffer());
  };
  let raw: unknown;
  try {
    raw = JSON.parse(new TextDecoder().decode(await get("manifest.json")));
  } catch (e) {
    if (e instanceof DhError) throw e;
    throw new DhError(
      "plugin_invalid",
      `${origin}'s manifest.json isn't JSON`,
      undefined,
      reasonOf(e),
    );
  }
  const read = readManifest(raw, "js");
  if (!read.ok) {
    throw new DhError(
      "plugin_invalid",
      `${origin}'s manifest.json: ${read.reason}`,
    );
  }
  if (read.manifest.revision !== check.revision) {
    throw new DhError(
      "plugin_invalid",
      `${origin}'s manifest.json doesn't match its revision file`,
    );
  }
  const groups = grouped(read.manifest) ? await groupsOf() : undefined;
  await writeCachedPart(
    store,
    origin,
    "js",
    read.manifest,
    raw,
    {
      fetchedAt,
      ...(check.etag ? { etag: check.etag } : {}),
      ...(groups ? { groups } : {}),
    },
    get,
  );
  return result("updated", read.manifest);
}

/** The signed-in user's groups (`getGroupsForUser()`); none on Community. */
async function userGroups(
  store: Store,
  profileFlag: string | undefined,
  lifecycle?: CommandLifecycle,
): Promise<string[]> {
  let session: Session;
  try {
    session = await requireAuth(profileFlag, store, lifecycle);
  } catch (e) {
    // An SSH-only profile, or none: no groups, so grouped plugins stay hidden.
    if (e instanceof DhError && e.code === "auth_required") return [];
    throw e;
  }
  try {
    const client = session.enterprise?.client;
    return client ? ((await client.getGroupsForUser()) as string[]) : [];
  } finally {
    disconnectServer(session);
  }
}

/** Plugins added, changed (another digest) and removed between two manifests. */
function diff(
  before: Manifest | undefined,
  after: Manifest | undefined,
): Pick<PartSync, "added" | "changed" | "removed"> {
  const was = new Map((before?.plugins ?? []).map((p) => [p.name, p.digest]));
  const now = new Map((after?.plugins ?? []).map((p) => [p.name, p.digest]));
  const names = (m: Map<string, string>, keep: (n: string) => boolean) =>
    [...m.keys()].filter(keep).sort();
  return {
    added: names(now, (n) => !was.has(n)),
    changed: names(now, (n) => was.has(n) && was.get(n) !== now.get(n)),
    removed: names(was, (n) => !now.has(n)),
  };
}
