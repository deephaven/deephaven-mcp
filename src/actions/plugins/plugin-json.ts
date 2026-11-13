/**
 * A `js` plugin's `plugin.json`: its command tree, written by
 * `cli-plugin-kit build` from the module's `definePlugin(…)`
 * (plans/cluster-plugins.md, "JS plugins").
 */
import { DhError } from "../../errors.ts";

/** The plugin API major versions this `dh` runs. */
export const SUPPORTED_APIS = [1];

export type Requires = "enterprise" | "community" | "any";
export type OutputKind = "rows" | "object" | "text" | "none";

export interface JsArg {
  name: string;
  description: string;
  required?: boolean;
}

export interface JsOption {
  /** camelCase, as `ctx.options` has it. */
  key: string;
  /** Commander syntax: `--class <value>`, `--dry-run`, `--tag <value...>`. */
  flags: string;
  type: "string" | "number" | "boolean";
  description: string;
  multiple?: boolean;
  choices?: string[];
  required?: boolean;
  default?: string | number | boolean | string[];
}

export interface JsOutput {
  kind: OutputKind;
  fields?: Record<string, { type: string; description: string }>;
}

/** A group (with `commands`) or a leaf (with `output`). */
export interface JsNode {
  name: string;
  description: string;
  commands?: JsNode[];
  args?: JsArg[];
  options?: JsOption[];
  output?: JsOutput;
}

export interface PluginJson {
  schema: 1;
  api: number;
  name: string;
  description: string;
  requires: Requires;
  groups?: string[];
  commands?: JsNode[];
  nouns?: JsNode[];
}

const NAME = /^[a-z][a-z0-9-]*$/;
const KEY = /^[a-z][a-zA-Z0-9]*$/;

/** Checks a parsed `plugin.json`; `file` names it in errors (`plugin_invalid`). */
export function parsePluginJson(value: unknown, file: string): PluginJson {
  const fail = (reason: string): never => {
    throw new DhError("plugin_invalid", `${file}: ${reason}`);
  };
  const p = asObject(value) ?? fail("not a JSON object");
  if (p.schema !== 1) fail(`unsupported "schema" (expected 1)`);
  if (typeof p.api !== "number") fail(`"api" must be a number`);
  if (typeof p.name !== "string" || !NAME.test(p.name)) {
    fail(`"name" must be lowercase letters, digits and dashes`);
  }
  if (typeof p.description !== "string") fail(`missing "description"`);
  if (!["enterprise", "community", "any"].includes(p.requires as string)) {
    fail(`"requires" must be "enterprise", "community" or "any"`);
  }
  if ((p.commands === undefined) === (p.nouns === undefined)) {
    fail(`needs "commands" or "nouns", not both`);
  }
  const tree = (p.commands ?? p.nouns) as unknown;
  checkNodes(tree, String(p.name), fail);
  return p as unknown as PluginJson;
}

function checkNodes(
  value: unknown,
  at: string,
  fail: (reason: string) => never,
): void {
  if (!Array.isArray(value) || value.length === 0) {
    fail(`"${at}": commands must be a non-empty list`);
  }
  const seen = new Set<string>();
  for (const v of value as unknown[]) {
    const n = asObject(v) ?? fail(`"${at}": each command must be an object`);
    if (typeof n.name !== "string" || !NAME.test(n.name)) {
      fail(
        `"${at}": a command's "name" must be lowercase letters, digits and dashes`,
      );
    }
    const path = `${at} ${n.name}`;
    if (seen.has(n.name as string)) fail(`"${at}": duplicate "${n.name}"`);
    seen.add(n.name as string);
    if (typeof n.description !== "string")
      fail(`"${path}": missing "description"`);
    if (n.commands !== undefined) {
      checkNodes(n.commands, path, fail);
      continue;
    }
    const output = asObject(n.output);
    if (
      !output ||
      !["rows", "object", "text", "none"].includes(output.kind as string)
    ) {
      fail(`"${path}": needs "commands" or an "output"`);
    }
    for (const a of (n.args ?? []) as unknown[]) {
      const arg = asObject(a);
      if (!arg || typeof arg.name !== "string" || !KEY.test(arg.name)) {
        fail(`"${path}": each arg needs a camelCase "name"`);
      }
    }
    for (const o of (n.options ?? []) as unknown[]) {
      const opt = asObject(o);
      if (
        !opt ||
        typeof opt.key !== "string" ||
        !KEY.test(opt.key) ||
        typeof opt.flags !== "string" ||
        !opt.flags.startsWith("--") ||
        !["string", "number", "boolean"].includes(opt.type as string)
      ) {
        fail(`"${path}": each option needs "key", "flags" and "type"`);
      }
    }
  }
}

function asObject(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}
