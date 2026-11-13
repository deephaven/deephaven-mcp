import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { digestOf, revisionOf } from "../src/actions/plugins/bundle.ts";

export const CLUSTER = { kind: "enterprise", version: "2026.01.1g0" };

/** Overrides for one manifest entry, by plugin name. */
export type EntryOverrides = Record<string, Record<string, unknown>>;

/**
 * Writes a `shell/` part under `bundle` from spec objects, as
 * `cli-plugin-kit build` would: each spec, the manifest and the revision.
 * `entries` overrides manifest fields per plugin (e.g. `dh`, `spec`).
 */
export async function writeShellPart(
  bundle: string,
  specs: Record<string, unknown>[],
  entries: EntryOverrides = {},
): Promise<string> {
  const dir = join(bundle, "shell");
  await mkdir(dir, { recursive: true });
  const plugins = [];
  for (const spec of specs) {
    const name = spec.name as string;
    const bytes = `${JSON.stringify(spec, null, 2)}\n`;
    await writeFile(join(dir, `${name}.json`), bytes);
    const nouns = Array.isArray(spec.nouns)
      ? (spec.nouns as { name: string }[]).map((n) => n.name)
      : [name];
    plugins.push({
      name,
      kind: "shell",
      description: spec.description,
      nouns,
      spec: `${name}.json`,
      digest: digestOf([bytes]),
      dh: ">=3.0.0",
      ...entries[name],
    });
  }
  return await writeManifest(dir, "shell", plugins);
}

/** Writes `dir`'s manifest and revision from `plugins` (with digests); returns the revision. */
export async function writeManifest(
  dir: string,
  channel: "js" | "shell",
  plugins: { name: string; digest: string }[],
  extra: Record<string, unknown> = {},
): Promise<string> {
  const revision = revisionOf(plugins);
  const manifest = {
    schema: 1,
    channel,
    revision,
    cluster: CLUSTER,
    plugins,
    ...extra,
  };
  await writeFile(join(dir, "manifest.json"), JSON.stringify(manifest));
  await writeFile(join(dir, "revision"), `${revision}\n`);
  return revision;
}

/** Rewrites a spec in a `shell/` part and its manifest, as a rebuild would. */
export async function updateShellSpec(
  bundle: string,
  spec: Record<string, unknown>,
): Promise<string> {
  const dir = join(bundle, "shell");
  const manifest = JSON.parse(
    await readFile(join(dir, "manifest.json"), "utf8"),
  );
  const bytes = `${JSON.stringify(spec, null, 2)}\n`;
  await writeFile(join(dir, `${spec.name}.json`), bytes);
  for (const p of manifest.plugins) {
    if (p.name === spec.name) p.digest = digestOf([bytes]);
  }
  return await writeManifest(dir, "shell", manifest.plugins);
}
