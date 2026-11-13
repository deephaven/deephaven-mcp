import { DhError } from "../../errors.ts";

/** A plugin spec file: top-level nouns whose commands run an external tool. Machine-independent. */
export interface PluginSpec {
  schema: 1;
  /** The plugin's identity; also its noun (`ctl` for `dh ctl …`) unless it has `nouns`. */
  name: string;
  description: string;
  tool: {
    /** Absolute path of the executable; never looked up on `PATH`. */
    path: string;
    /** Which of the cluster's service accounts the tool runs as; unset, as whoever runs it. */
    runAs?: RunAs;
  };
  /** Several top-level nouns, each a command node; instead of `commands`/`argv` at the top. */
  nouns?: SpecCommand[];
  /** The noun's subcommands; without them, the noun itself runs the tool with `argv`. */
  commands?: SpecCommand[];
  argv?: string[];
  options?: SpecOption[];
  /** Set by `dh plugin create`: what the spec was generated from, and the tool's help for the noun. */
  source?: { createdFrom?: string; helpDigest?: string; help?: string };
}

/** `account`: the cluster's `DH_ADMIN_USER`, `DH_MERGE_USER` or `DH_QUERY_USER`. `unlessFlags`: the tool's flags that mean the user brings their own identity. */
export interface RunAs {
  account: "admin" | "merge" | "query";
  unlessFlags?: string[];
}

/** A group (with `commands`) or a leaf (with `argv`) in a plugin's command tree. */
export interface SpecCommand {
  name: string;
  description: string;
  commands?: SpecCommand[];
  /** The tool's argv prefix; the user's arguments are appended verbatim. */
  argv?: string[];
  /** The tool's options for this command, shown in `dh`'s help and `dh agents`; every flag still passes through. */
  options?: SpecOption[];
  /** Reference for whoever edits the spec, never shown: the tool's help this command was generated from. */
  source?: { help?: string };
}

/** One of the tool's options, as `dh plugin create` read it from the help, or as written by hand. */
export interface SpecOption {
  /** Spellings, then the value if it takes one, as the tool prints them: `-t, --table-name <arg>`. */
  flags: string;
  description: string;
  required?: boolean;
}

const NAME = /^[a-z][a-z0-9-]*$/;

/** Checks `value` (a parsed spec file) and returns it typed; `file` names it in errors. */
export function parseSpec(value: unknown, file: string): PluginSpec {
  const fail = (reason: string): never => {
    throw new DhError("plugin_invalid", `${file}: ${reason}`);
  };
  const spec = asObject(value) ?? fail("not a JSON object");
  if (spec.schema !== 1) fail(`unsupported "schema" (expected 1)`);
  const tool = asObject(spec.tool) ?? fail(`missing "tool"`);
  if (typeof tool.path !== "string" || !tool.path.startsWith("/")) {
    fail(`"tool.path" must be an absolute path`);
  }
  if (tool.runAs !== undefined) {
    const reason = runAsProblem(tool.runAs);
    if (reason) fail(`"tool.runAs": ${reason}`);
  }
  if (spec.nouns === undefined) checkNode(spec, "", fail);
  else checkNouns(spec, fail);
  return spec as unknown as PluginSpec;
}

/** A spec with `nouns`: its name and description, and each noun as a command node. */
function checkNouns(
  spec: Record<string, unknown>,
  fail: (reason: string) => never,
): void {
  if (typeof spec.name !== "string" || !NAME.test(spec.name)) {
    fail(`the spec: "name" must be lowercase letters, digits and dashes`);
  }
  if (typeof spec.description !== "string") {
    fail(`the spec: missing "description"`);
  }
  for (const key of ["commands", "argv", "options"]) {
    if (spec[key] !== undefined) fail(`has both "nouns" and "${key}"`);
  }
  if (!Array.isArray(spec.nouns) || spec.nouns.length === 0) {
    fail(`"nouns" must be a non-empty list`);
  }
  const seen = new Set<string>();
  for (const value of spec.nouns as unknown[]) {
    const noun = asObject(value) ?? fail(`each of "nouns" must be an object`);
    checkNode(noun, "", fail);
    if (seen.has(noun.name as string)) fail(`duplicate noun "${noun.name}"`);
    seen.add(noun.name as string);
  }
}

/** Why `value` isn't a valid `tool.runAs`, or undefined. */
function runAsProblem(value: unknown): string | undefined {
  const v = asObject(value);
  if (!v || !["admin", "merge", "query"].includes(v.account as string)) {
    return `"account" must be "admin", "merge" or "query"`;
  }
  if (
    v.unlessFlags !== undefined &&
    (!isStrings(v.unlessFlags) || v.unlessFlags.some((f) => !f.startsWith("-")))
  ) {
    return `"unlessFlags" must be a list of flags starting with "-"`;
  }
  return undefined;
}

/** Checks one node of the command tree and its children; `at` is its path for errors. */
function checkNode(
  node: Record<string, unknown>,
  at: string,
  fail: (reason: string) => never,
): void {
  const where = at ? `"${at}"` : "the spec";
  if (typeof node.name !== "string" || !NAME.test(node.name)) {
    fail(`${where}: "name" must be lowercase letters, digits and dashes`);
  }
  if (typeof node.description !== "string") {
    fail(`${where}: missing "description"`);
  }
  if (node.source !== undefined) {
    const source = asObject(node.source);
    if (
      !source ||
      (source.help !== undefined && typeof source.help !== "string")
    ) {
      fail(`${where}: "source.help" must be a string`);
    }
  }
  const path = at ? `${at} ${node.name}` : String(node.name);
  if (node.commands === undefined) {
    if (!isStrings(node.argv)) fail(`"${path}": needs "commands" or "argv"`);
    checkOptions(node.options, path, fail);
    return;
  }
  if (node.options !== undefined)
    fail(`"${path}": a group can't have "options"`);
  if (node.argv !== undefined)
    fail(`"${path}": has both "commands" and "argv"`);
  if (!Array.isArray(node.commands) || node.commands.length === 0) {
    fail(`"${path}": "commands" must be a non-empty list`);
  }
  const seen = new Set<string>();
  for (const child of node.commands as unknown[]) {
    const c =
      asObject(child) ?? fail(`"${path}": each command must be an object`);
    checkNode(c, path, fail);
    if (seen.has(c.name as string))
      fail(`"${path}": duplicate command "${c.name}"`);
    seen.add(c.name as string);
  }
}

/** `options`, if given: a list of `{flags, description, required?}`, flags starting with `-`. */
function checkOptions(
  options: unknown,
  path: string,
  fail: (reason: string) => never,
): void {
  if (options === undefined) return;
  if (!Array.isArray(options)) fail(`"${path}": "options" must be a list`);
  for (const value of options as unknown[]) {
    const o = asObject(value);
    if (!o || typeof o.flags !== "string" || !o.flags.startsWith("-")) {
      fail(`"${path}": each option needs "flags" starting with "-"`);
    }
    if (typeof o?.description !== "string") {
      fail(`"${path}": option ${String(o?.flags)} needs a "description"`);
    }
    if (o?.required !== undefined && typeof o.required !== "boolean") {
      fail(
        `"${path}": option ${String(o.flags)}: "required" must be true or false`,
      );
    }
  }
}

function asObject(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function isStrings(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((v) => typeof v === "string");
}
