/**
 * `cli-plugin-kit build <src> <out>`: a cluster's bundle (plans/cluster-plugins.md,
 * "The cluster bundle") from its sources.
 *
 *   <src>/shell/*.json    shell specs, with their `source` fields
 *   <src>/js/<dir>/       one TypeScript plugin each: index.ts or src/index.ts
 *
 * Writes `<out>/shell/` and `<out>/js/`, replacing them: each plugin's files,
 * and each part's `manifest.json` and `revision`.
 */
import {
  mkdir,
  readdir,
  readFile,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { join } from "node:path";
import {
  type Cluster,
  digestOf,
  type Manifest,
  type ManifestEntry,
  revisionOf,
  type ShellHint,
} from "../../../src/actions/plugins/bundle.ts";
import {
  type PluginSpec,
  parseSpec,
} from "../../../src/actions/plugins/spec.ts";
import { buildJsPlugin } from "./js.ts";

/** The `dh` versions that read the bundles this kit writes. */
export type { Cluster, Manifest };

export const DH_RANGE = ">=3.0.0";

/** A plugin's files, by part-relative path, before they're written. */
interface Planned {
  name: string;
  description: string;
  nouns: string[];
  files: { path: string; bytes: string }[];
  /** Where it came from, for errors. */
  from: string;
  api?: number;
  groups?: string[];
}

/** Builds the bundle; returns both manifests. */
export async function build(
  src: string,
  out: string,
  cluster: Cluster,
): Promise<{ js: Manifest; shell: Manifest }> {
  const shell = await planShell(join(src, "shell"));
  const js = await planJs(join(src, "js"));
  checkUnique([...shell, ...js]);
  const shellManifest = await writePart(out, "shell", shell, cluster);
  const hints = shellManifest.plugins.map((p) => ({
    name: p.name,
    description: p.description,
    nouns: p.nouns,
  }));
  const jsManifest = await writePart(out, "js", js, cluster, hints);
  return { js: jsManifest, shell: shellManifest };
}

/** Each spec in `dir`, validated, without its `source` fields. */
async function planShell(dir: string): Promise<Planned[]> {
  const planned: Planned[] = [];
  for (const file of await list(
    dir,
    (f, isDir) => !isDir && f.endsWith(".json"),
  )) {
    const path = join(dir, file);
    const value = JSON.parse(await readFile(path, "utf8"));
    const tool = value?.tool ?? {};
    if (tool.version !== undefined || tool.versionFrom !== undefined) {
      throw new Error(
        `${path}: a bundle's specs have no "tool.version" or "tool.versionFrom"`,
      );
    }
    const spec = parseSpec(value, path);
    const stripped = withoutSource(spec) as PluginSpec;
    planned.push({
      name: spec.name,
      description: spec.description,
      nouns: spec.nouns ? spec.nouns.map((n) => n.name) : [spec.name],
      files: [{ path: `${spec.name}.json`, bytes: json(stripped) }],
      from: path,
    });
  }
  return planned;
}

/** Each plugin directory in `dir`, bundled, with its `plugin.json`. */
async function planJs(dir: string): Promise<Planned[]> {
  const planned: Planned[] = [];
  for (const entry of await list(dir, (_, isDir) => isDir)) {
    const path = join(dir, entry);
    const p = await buildJsPlugin(path);
    planned.push({
      name: p.name,
      description: p.description,
      nouns: p.nouns,
      files: [
        { path: `${p.name}/plugin.json`, bytes: json(p.pluginJson) },
        { path: `${p.name}/index.js`, bytes: p.module },
      ],
      from: path,
      api: p.api,
      ...(p.groups ? { groups: p.groups } : {}),
    });
  }
  return planned;
}

/** Plugin names are unique across the bundle, and so are nouns. */
function checkUnique(plugins: Planned[]): void {
  const names = new Map<string, string>();
  const nouns = new Map<string, string>();
  for (const p of plugins) {
    const name = names.get(p.name);
    if (name) {
      throw new Error(
        `Plugin name "${p.name}" is used twice: ${name}, ${p.from}`,
      );
    }
    names.set(p.name, p.from);
    for (const noun of p.nouns) {
      const owner = nouns.get(noun);
      if (owner) {
        throw new Error(
          `Noun "${noun}" is registered twice: ${owner}, ${p.from}`,
        );
      }
      nouns.set(noun, p.from);
    }
  }
}

/** Replaces `<out>/<channel>/` with the plugins' files, manifest and revision. */
async function writePart(
  out: string,
  channel: "js" | "shell",
  plugins: Planned[],
  cluster: Cluster,
  shell?: ShellHint[],
): Promise<Manifest> {
  const dir = join(out, channel);
  await rm(dir, { recursive: true, force: true });
  const entries: ManifestEntry[] = [];
  for (const p of plugins) {
    for (const f of p.files) {
      const path = join(dir, f.path);
      await mkdir(join(path, ".."), { recursive: true });
      await writeFile(path, f.bytes);
    }
    const [spec, module] = p.files;
    entries.push({
      name: p.name,
      kind: channel,
      description: p.description,
      nouns: p.nouns,
      spec: spec?.path ?? "",
      ...(module ? { module: module.path } : {}),
      digest: digestOf(p.files.map((f) => f.bytes)),
      dh: DH_RANGE,
      ...(p.api !== undefined ? { api: p.api } : {}),
      ...(p.groups ? { groups: p.groups } : {}),
    });
  }
  const manifest: Manifest = {
    schema: 1,
    channel,
    revision: revisionOf(entries, shell ?? []),
    cluster,
    plugins: entries,
    ...(shell ? { shell } : {}),
  };
  await mkdir(dir, { recursive: true });
  await writeFile(join(dir, "manifest.json"), json(manifest));
  await writeFile(join(dir, "revision"), `${manifest.revision}\n`);
  return manifest;
}

/** `value` without any `source` key, at any depth. */
function withoutSource(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(withoutSource);
  if (typeof value !== "object" || value === null) return value;
  return Object.fromEntries(
    Object.entries(value)
      .filter(([k]) => k !== "source")
      .map(([k, v]) => [k, withoutSource(v)]),
  );
}

/** The names in `dir` that `keep` accepts, sorted; none if `dir` doesn't exist. Symlinks are followed. */
async function list(
  dir: string,
  keep: (name: string, isDir: boolean) => boolean,
): Promise<string[]> {
  try {
    const names = (await readdir(dir)).sort();
    const kept: string[] = [];
    for (const name of names) {
      if (keep(name, (await stat(join(dir, name))).isDirectory()))
        kept.push(name);
    }
    return kept;
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw e;
  }
}

function json(value: unknown): string {
  return `${JSON.stringify(value, null, 2)}\n`;
}
