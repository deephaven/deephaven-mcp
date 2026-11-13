/**
 * The cluster bundle's format (plans/cluster-plugins.md, "The cluster
 * bundle"): each part's `manifest.json`, its entries, digests and revision.
 * Shared with `cli-plugin-kit`, which writes what this reads.
 */
import { createHash } from "node:crypto";
import { posix } from "node:path";
import semver from "semver";

/** The bundle's path on a node; `shell/` is under it. */
export const DEFAULT_BUNDLE = "/usr/illumon/latest/dh-cli";

export type Channel = "js" | "shell";

export interface Cluster {
  kind: "enterprise" | "community";
  /** The version the bundle was built for, e.g. a DHE release's. */
  version: string;
}

/** One `plugins` entry of a part's `manifest.json`. */
export interface ManifestEntry {
  name: string;
  kind: Channel;
  description: string;
  /** The top-level nouns it registers. */
  nouns: string[];
  /** Part-relative path of the command tree. */
  spec: string;
  /** `js` only: part-relative path of the ESM module. */
  module?: string;
  /** `sha256:` over the spec file's bytes, then the module's. */
  digest: string;
  /** The `dh` versions that can run it, a semver range. */
  dh: string;
  /** `js` only: the plugin API major version. */
  api?: number;
  /** `js` only: registered only for users in one of these groups. */
  groups?: string[];
}

/** A `shell` plugin as the `js` manifest names it: what SSH would add. */
export interface ShellHint {
  name: string;
  description: string;
  nouns: string[];
}

export interface Manifest {
  schema: 1;
  channel: Channel;
  revision: string;
  cluster: Cluster;
  plugins: ManifestEntry[];
  /** `js` part only. */
  shell?: ShellHint[];
}

/** A manifest entry `dh` can't use, and why. */
export interface EntryProblem {
  name?: string;
  reason: string;
}

/** What reading a manifest gave: the manifest with its usable entries, or why there's none. */
export type ManifestRead =
  | { ok: true; manifest: Manifest; problems: EntryProblem[] }
  | { ok: false; status: "unsupported" | "invalid"; reason: string };

const NAME = /^[a-z][a-z0-9-]*$/;
const DIGEST = /^sha256:[0-9a-f]{64}$/;

/**
 * Checks a parsed `manifest.json` read from `channel`'s part. An unknown
 * `schema` is `unsupported`; a wrong `channel` or a bad shape is `invalid`.
 * A bad entry is left out with its reason, the rest kept.
 */
export function readManifest(value: unknown, channel: Channel): ManifestRead {
  const m = asObject(value);
  if (!m) return { ok: false, status: "invalid", reason: "not a JSON object" };
  if (m.schema !== 1) {
    return {
      ok: false,
      status: "unsupported",
      reason: `schema ${JSON.stringify(m.schema)}`,
    };
  }
  if (m.channel !== channel) {
    return {
      ok: false,
      status: "invalid",
      reason: `"channel" is ${JSON.stringify(m.channel)}, read from ${channel}/`,
    };
  }
  const cluster = asObject(m.cluster);
  if (
    typeof m.revision !== "string" ||
    !DIGEST.test(m.revision) ||
    !cluster ||
    typeof cluster.version !== "string" ||
    !Array.isArray(m.plugins)
  ) {
    return {
      ok: false,
      status: "invalid",
      reason: `needs "revision", "cluster" and "plugins"`,
    };
  }
  const plugins: ManifestEntry[] = [];
  const problems: EntryProblem[] = [];
  for (const value of m.plugins) {
    const e = asObject(value);
    const reason = entryProblem(e, channel);
    const name = typeof e?.name === "string" ? e.name : undefined;
    if (reason) problems.push({ name, reason });
    else plugins.push(e as unknown as ManifestEntry);
  }
  const shell = channel === "js" ? hintsOf(m.shell) : undefined;
  return {
    ok: true,
    manifest: {
      ...(m as unknown as Manifest),
      plugins,
      ...(shell ? { shell } : {}),
    },
    problems,
  };
}

/** Why `e` isn't a usable entry of `channel`'s manifest, or undefined. */
function entryProblem(
  e: Record<string, unknown> | undefined,
  channel: Channel,
): string | undefined {
  if (!e || typeof e.name !== "string" || !NAME.test(e.name)) {
    return `an entry's "name" must be lowercase letters, digits and dashes`;
  }
  if (e.kind !== channel) return `"kind" must be "${channel}"`;
  if (typeof e.description !== "string") return `missing "description"`;
  if (
    !Array.isArray(e.nouns) ||
    e.nouns.length === 0 ||
    !e.nouns.every((n) => typeof n === "string" && NAME.test(n))
  ) {
    return `"nouns" must be a non-empty list of names`;
  }
  if (insidePart(e.spec) === undefined) {
    return `"spec" must be a path inside ${channel}/`;
  }
  if (channel === "js" && insidePart(e.module) === undefined) {
    return `"module" must be a path inside js/`;
  }
  if (typeof e.digest !== "string" || !DIGEST.test(e.digest)) {
    return `"digest" must be "sha256:<hex>"`;
  }
  if (typeof e.dh !== "string" || semver.validRange(e.dh) === null) {
    return `"dh" must be a semver range`;
  }
  if (channel === "js" && typeof e.api !== "number") {
    return `"api" must be a number`;
  }
  if (
    e.groups !== undefined &&
    !(Array.isArray(e.groups) && e.groups.every((g) => typeof g === "string"))
  ) {
    return `"groups" must be a list of strings`;
  }
  return undefined;
}

/** The `js` manifest's `shell` list, keeping well-formed entries. */
function hintsOf(value: unknown): ShellHint[] {
  if (!Array.isArray(value)) return [];
  return value.filter((v): v is ShellHint => {
    const h = asObject(v);
    return (
      typeof h?.name === "string" &&
      typeof h.description === "string" &&
      Array.isArray(h.nouns) &&
      h.nouns.every((n) => typeof n === "string")
    );
  });
}

/**
 * `path` normalized, if it's relative and stays inside its part once
 * normalized; else undefined.
 */
export function insidePart(path: unknown): string | undefined {
  if (typeof path !== "string" || path === "" || path.includes("\\")) {
    return undefined;
  }
  if (posix.isAbsolute(path)) return undefined;
  const normal = posix.normalize(path);
  if (normal === "." || normal === ".." || normal.startsWith("../")) {
    return undefined;
  }
  return normal;
}

/** The entry's files, in digest order: its spec, then its module. */
export function filesOf(entry: ManifestEntry): string[] {
  return entry.module === undefined ? [entry.spec] : [entry.spec, entry.module];
}

/** `sha256:` over `parts`' bytes, in order. */
export function digestOf(parts: (string | Uint8Array)[]): string {
  const hash = createHash("sha256");
  for (const p of parts) hash.update(p);
  return `sha256:${hash.digest("hex")}`;
}

/**
 * `sha256:` of one `<name>:<digest>\n` line per plugin, plus one
 * `shell:<name>:<noun>,<noun>…\n` line per `shell` entry, all sorted.
 */
export function revisionOf(
  plugins: { name: string; digest: string }[],
  shell: { name: string; nouns: string[] }[] = [],
): string {
  const lines = [
    ...plugins.map((p) => `${p.name}:${p.digest}\n`),
    ...shell.map((p) => `shell:${p.name}:${p.nouns.join(",")}\n`),
  ]
    .sort()
    .join("");
  return digestOf([lines]);
}

/** Whether `dh` version `version` can run `entry`. */
export function dhSatisfies(entry: ManifestEntry, version: string): boolean {
  return semver.satisfies(version, entry.dh, { includePrerelease: true });
}

function asObject(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}
