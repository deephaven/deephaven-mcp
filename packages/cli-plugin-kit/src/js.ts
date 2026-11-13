/**
 * Builds one `js` plugin: bundles its module to one ESM file with esbuild,
 * imports it once, and writes everything but functions to `plugin.json`.
 */
import { access, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { build as esbuild } from "esbuild";

const NAME = /^[a-z][a-z0-9-]*$/;
const KEY = /^[a-z][a-zA-Z0-9]*$/;
/** `dh`'s global flags; a plugin's option can't take one. */
const TAKEN_FLAGS = [
  "--help",
  "--output",
  "--profile",
  "--no-prompt",
  "--no-input",
  "--ssh-host",
];
const REQUIRES = ["enterprise", "community", "any"];
const OPTION_TYPES = ["string", "number", "boolean"];
const OUTPUT_KINDS = ["rows", "object", "text", "none"];
const FIELD_TYPES = ["string", "number", "boolean", "object", "array"];

/** A built `js` plugin: its `plugin.json` value and its bundled module. */
export interface BuiltJsPlugin {
  name: string;
  description: string;
  /** The top-level nouns it registers. */
  nouns: string[];
  api: number;
  groups?: string[];
  pluginJson: Record<string, unknown>;
  module: string;
}

/** The module's entry in `dir`: `index.ts`, else `src/index.ts`. */
async function entryOf(dir: string): Promise<string> {
  for (const file of ["index.ts", "src/index.ts"]) {
    const path = resolve(dir, file);
    if (
      await access(path).then(
        () => true,
        () => false,
      )
    )
      return path;
  }
  throw new Error(`${dir}: no index.ts or src/index.ts`);
}

/** Bundles and describes the plugin in `dir`. */
export async function buildJsPlugin(dir: string): Promise<BuiltJsPlugin> {
  const result = await esbuild({
    entryPoints: [await entryOf(dir)],
    // Path comments in the output are relative to this, so the bytes (and the
    // digest) don't depend on where the build runs.
    absWorkingDir: resolve(dir),
    bundle: true,
    platform: "node",
    format: "esm",
    target: "es2022",
    write: false,
    logLevel: "silent",
  });
  const module = result.outputFiles[0]?.text;
  if (module === undefined) throw new Error(`${dir}: esbuild wrote nothing`);
  const plugin = await importOnce(module);
  return describe(plugin, dir, module);
}

/** The bundled module's default export, from a temporary copy. */
async function importOnce(module: string): Promise<unknown> {
  const tmp = await mkdtemp(join(tmpdir(), "cli-plugin-kit-"));
  try {
    const file = join(tmp, "index.js");
    await writeFile(file, module);
    return (await import(pathToFileURL(file).href)).default;
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
}

/** Checks the default export and converts it to `plugin.json`. */
function describe(value: unknown, dir: string, module: string): BuiltJsPlugin {
  const fail = (reason: string): never => {
    throw new Error(`${dir}: ${reason}`);
  };
  const p =
    asObject(value) ??
    fail("the module's default export must be definePlugin(…)");
  if (typeof p.api !== "number") {
    fail(`no "api": export default definePlugin(…) from @deephaven/cli-plugin`);
  }
  if (typeof p.name !== "string" || !NAME.test(p.name)) {
    fail(`"name" must be lowercase letters, digits and dashes`);
  }
  if (typeof p.description !== "string") fail(`missing "description"`);
  if (!REQUIRES.includes(p.requires as string)) {
    fail(`"requires" must be "enterprise", "community" or "any"`);
  }
  if (p.groups !== undefined && !isStrings(p.groups)) {
    fail(`"groups" must be a list of strings`);
  }
  if ((p.commands === undefined) === (p.nouns === undefined)) {
    fail(`needs "commands" or "nouns", not both`);
  }
  const key = p.nouns === undefined ? "commands" : "nouns";
  const tree = nodes(p[key], String(p.name), fail);
  const name = p.name as string;
  return {
    name,
    description: p.description as string,
    nouns: key === "nouns" ? tree.map((n) => n.name as string) : [name],
    api: p.api as number,
    ...(p.groups ? { groups: p.groups as string[] } : {}),
    pluginJson: {
      schema: 1,
      api: p.api,
      name,
      description: p.description,
      requires: p.requires,
      ...(p.groups ? { groups: p.groups } : {}),
      [key]: tree,
    },
    module,
  };
}

/** `commands` (an object by name) as `plugin.json`'s list; `at` names it in errors. */
function nodes(
  value: unknown,
  at: string,
  fail: (reason: string) => never,
): Record<string, unknown>[] {
  const commands =
    asObject(value) ?? fail(`"${at}": commands must be an object`);
  const entries = Object.entries(commands);
  if (entries.length === 0) fail(`"${at}": no commands`);
  return entries.map(([name, def]) => node(name, def, `${at} ${name}`, fail));
}

/** One command as `plugin.json` has it: a group with `commands`, or a leaf. */
function node(
  name: string,
  value: unknown,
  at: string,
  fail: (reason: string) => never,
): Record<string, unknown> {
  if (!NAME.test(name)) {
    fail(`"${at}": names are lowercase letters, digits and dashes`);
  }
  const c = asObject(value) ?? fail(`"${at}" must be an object`);
  if (typeof c.description !== "string") fail(`"${at}": missing "description"`);
  if (c.commands !== undefined) {
    if (c.run !== undefined) fail(`"${at}": has both "commands" and "run"`);
    return {
      name,
      description: c.description,
      commands: nodes(c.commands, at, fail),
    };
  }
  if (typeof c.run !== "function") {
    fail(`"${at}": needs "commands" or a "run" function`);
  }
  if (c.human !== undefined && typeof c.human !== "function") {
    fail(`"${at}": "human" must be a function`);
  }
  const args = c.args === undefined ? [] : argsOf(c.args, at, fail);
  const options = c.options === undefined ? [] : optionsOf(c.options, at, fail);
  return {
    name,
    description: c.description,
    ...(args.length > 0 ? { args } : {}),
    ...(options.length > 0 ? { options } : {}),
    output: outputOf(c.output, at, fail),
  };
}

function argsOf(
  value: unknown,
  at: string,
  fail: (reason: string) => never,
): Record<string, unknown>[] {
  const args = asObject(value) ?? fail(`"${at}": "args" must be an object`);
  return Object.entries(args).map(([name, def]) => {
    const a = asObject(def);
    if (!KEY.test(name) || typeof a?.description !== "string") {
      fail(`"${at}": arg "${name}" needs a camelCase name and a "description"`);
    }
    if (a?.required !== undefined && typeof a.required !== "boolean") {
      fail(`"${at}": arg "${name}": "required" must be true or false`);
    }
    return {
      name,
      description: a?.description,
      ...(a?.required !== undefined ? { required: a.required } : {}),
    };
  });
}

function optionsOf(
  value: unknown,
  at: string,
  fail: (reason: string) => never,
): Record<string, unknown>[] {
  const options =
    asObject(value) ?? fail(`"${at}": "options" must be an object`);
  return Object.entries(options).map(([key, def]) => {
    const where = `"${at}": option "${key}"`;
    if (!KEY.test(key)) fail(`${where}: keys are camelCase`);
    const o = asObject(def) ?? fail(`${where} must be an object`);
    if (!OPTION_TYPES.includes(o.type as string)) {
      fail(`${where}: "type" must be "string", "number" or "boolean"`);
    }
    if (typeof o.description !== "string")
      fail(`${where}: missing "description"`);
    if (o.choices !== undefined && !isStrings(o.choices)) {
      fail(`${where}: "choices" must be a list of strings`);
    }
    for (const k of ["multiple", "required"]) {
      if (o[k] !== undefined && typeof o[k] !== "boolean") {
        fail(`${where}: "${k}" must be true or false`);
      }
    }
    const long = `--${key.replace(/[A-Z]/g, (c) => `-${c.toLowerCase()}`)}`;
    if (TAKEN_FLAGS.includes(long))
      fail(`${where}: ${long} is one of dh's flags`);
    const value = o.multiple ? " <value...>" : " <value>";
    return {
      key,
      flags: o.type === "boolean" ? long : `${long}${value}`,
      type: o.type,
      description: o.description,
      ...pick(o, ["multiple", "choices", "required", "default"]),
    };
  });
}

function outputOf(
  value: unknown,
  at: string,
  fail: (reason: string) => never,
): Record<string, unknown> {
  const o = asObject(value) ?? fail(`"${at}": missing "output"`);
  if (!OUTPUT_KINDS.includes(o.kind as string)) {
    fail(`"${at}": "output.kind" must be "rows", "object", "text" or "none"`);
  }
  if (o.fields === undefined) return { kind: o.kind };
  const fields =
    asObject(o.fields) ?? fail(`"${at}": "output.fields" must be an object`);
  for (const [name, def] of Object.entries(fields)) {
    const f = asObject(def);
    if (
      !FIELD_TYPES.includes(f?.type as string) ||
      typeof f?.description !== "string"
    ) {
      fail(`"${at}": output field "${name}" needs a "type" and "description"`);
    }
  }
  return { kind: o.kind, fields };
}

function pick(
  o: Record<string, unknown>,
  keys: string[],
): Record<string, unknown> {
  return Object.fromEntries(
    keys.filter((k) => o[k] !== undefined).map((k) => [k, o[k]]),
  );
}

function asObject(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function isStrings(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((v) => typeof v === "string");
}
