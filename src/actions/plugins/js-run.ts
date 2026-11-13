/**
 * Running a `js` plugin's leaf (plans/cluster-plugins.md, "Running a leaf"):
 * import its module after checking its digest, call `run(ctx)`, and render
 * what it returns.
 */
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import type { Context } from "../../cli/command.ts";
import { DhError, type ErrorCode } from "../../errors.ts";
import { err } from "../../output.ts";
import { BIN } from "../../version.ts";
import { requireAuth } from "../auth/session.ts";
import { disconnectServer, type Session } from "../auth/signin.ts";
import { digestOf, filesOf } from "./bundle.ts";
import { type Plugin, pluginFile } from "./discover.ts";
import type { JsNode, JsOutput } from "./plugin-json.ts";
import type { ActiveProfile } from "./settings.ts";

/** The codes `ctx.fail` may give. */
const FAIL_CODES: ErrorCode[] = [
  "usage",
  "not_found",
  "conflict",
  "permission_denied",
  "plugin_failed",
];

// biome-ignore lint/suspicious/noExplicitAny: the plugin's own module.
type Module = any;

/** A leaf's definition in the module: what `plugin.json` left out. */
interface LeafCode {
  run(ctx: unknown): unknown;
  human?(result: unknown): string | string[];
}

/** Imports the plugin's module, after checking its files against the manifest's digest. */
export async function importPlugin(plugin: Plugin): Promise<Module> {
  const entry = plugin.entry;
  if (!entry?.module) {
    throw new DhError("plugin_invalid", `${plugin.name} has no module`);
  }
  const files = filesOf(entry);
  const bytes = await Promise.all(
    files.map((f) => readFile(pluginFile(plugin, f))),
  );
  if (digestOf(bytes) !== entry.digest) {
    throw new DhError(
      "plugin_invalid",
      `${plugin.name}'s files don't match its digest`,
      `Run \`${BIN} plugin sync\`.`,
    );
  }
  const mod = await import(
    pathToFileURL(pluginFile(plugin, entry.module)).href
  );
  return mod.default;
}

/**
 * The leaf at `path` (top-level noun first) in the module's default export;
 * `plugin_invalid` if the module and `plugin.json` disagree.
 */
export function leafIn(
  plugin: Plugin,
  definition: Module,
  path: string[],
): LeafCode {
  const mismatch = () =>
    new DhError(
      "plugin_invalid",
      `${plugin.name}'s module has no command "${path.join(" ")}"`,
      "Its module and plugin.json disagree.",
    );
  const nouns = definition?.nouns;
  // A plugin with `commands` is one noun named after it.
  let node: Module = nouns
    ? { commands: nouns }
    : { commands: { [plugin.name]: definition } };
  for (const name of path) {
    node = node?.commands?.[name];
    if (!node) throw mismatch();
  }
  if (typeof node.run !== "function") throw mismatch();
  return node as LeafCode;
}

/** What a leaf's run needs from its command. */
export interface LeafRun {
  ctx: Context;
  plugin: Plugin;
  path: string[];
  node: JsNode;
  active: ActiveProfile;
  args: Record<string, string | undefined>;
  options: Record<string, string | number | boolean | string[] | undefined>;
}

/**
 * Calls the leaf's `run` with a fresh context, Ctrl-C aborting its signal,
 * then disconnects whatever the context opened. A non-`DhError` thrown is
 * `plugin_failed`, its stack shown with `DH_DEBUG`.
 */
export async function runLeaf(
  leaf: LeafCode,
  r: LeafRun,
): Promise<{ human: string[]; json: unknown }> {
  const controller = new AbortController();
  const onSigint = () => controller.abort();
  process.on("SIGINT", onSigint);
  let session: Promise<Session> | undefined;
  const signIn = () => {
    session ??= requireAuth(r.ctx.profileFlag, r.ctx.store, r.ctx.lifecycle);
    return session;
  };
  const server = r.active.profile.server;
  const kind = (await r.ctx.store.read()).config.servers[server]?.kind;
  const ctx = {
    args: r.args,
    options: r.options,
    profile: {
      name: r.active.profile.name,
      server,
      kind,
      user: r.active.profile.user,
    },
    async enterprise() {
      const s = await signIn();
      if (!s.enterprise) throw needs(r.plugin.name, "an Enterprise");
      return { api: s.enterprise.dhe, client: s.enterprise.client };
    },
    async community() {
      const s = await signIn();
      if (!s.community) throw needs(r.plugin.name, "a Community");
      return { api: s.community.dh, client: s.community.client };
    },
    ui: {
      confirm: (q: string, o: { flag: string; default?: boolean }) =>
        r.ctx.ui.confirm(q, o.default ?? false, o.flag),
      select: <T extends string>(
        q: string,
        choices: T[],
        o: { flag: string },
      ) =>
        r.ctx.ui.select(
          q,
          choices.map((c) => ({ label: c, value: c })),
          o.flag,
        ),
    },
    progress(message: string) {
      if (!r.ctx.json) err(message);
    },
    signal: controller.signal,
    json: r.ctx.json,
    fail(code: ErrorCode, message: string, hint?: string): never {
      throw new DhError(
        FAIL_CODES.includes(code) ? code : "plugin_failed",
        message,
        hint,
      );
    },
  };
  let result: unknown;
  try {
    result = await leaf.run(ctx);
  } catch (e) {
    if (e instanceof DhError) throw e;
    if (process.env.DH_DEBUG && e instanceof Error && e.stack) err(e.stack);
    throw new DhError(
      "plugin_failed",
      e instanceof Error ? e.message : String(e),
    );
  } finally {
    process.off("SIGINT", onSigint);
    const opened = await session?.catch(() => undefined);
    if (opened) disconnectServer(opened);
  }
  if (controller.signal.aborted) {
    throw new DhError("cancelled", `${r.plugin.name} was cancelled`);
  }
  return render(r.plugin.name, r.node.output ?? { kind: "none" }, result, leaf);
}

/** `usage`: the plugin needs another kind of profile. */
function needs(plugin: string, kind: string): DhError {
  return new DhError("usage", `"${plugin}" needs ${kind} profile`);
}

/**
 * The result for `-o json` (`text` as `{"text": …}`, `none` as null) and for
 * people: the leaf's `human`, else a table (`rows`), `key: value` lines
 * (`object`), or the text.
 */
function render(
  plugin: string,
  output: JsOutput,
  result: unknown,
  leaf: LeafCode,
): { human: string[]; json: unknown } {
  const json = jsonOf(output, result);
  let serial: string | undefined;
  try {
    serial = JSON.stringify(json);
  } catch {
    serial = undefined;
  }
  if (serial === undefined) {
    throw new DhError(
      "plugin_failed",
      `${plugin} returned a value that isn't JSON`,
    );
  }
  if (leaf.human) {
    const h = leaf.human(result);
    return { human: Array.isArray(h) ? h : [h], json };
  }
  return { human: humanLines(output, result), json };
}

/** `-o json`'s value: `text` as `{"text": …}`, `none` as null, else the result. */
function jsonOf(output: JsOutput, result: unknown): unknown {
  if (output.kind === "text") return { text: result };
  if (output.kind === "none") return null;
  return result;
}

function humanLines(output: JsOutput, result: unknown): string[] {
  switch (output.kind) {
    case "none":
      return [];
    case "text":
      return [String(result ?? "")];
    case "object":
      return Object.entries((result ?? {}) as Record<string, unknown>).map(
        ([k, v]) => `${k}: ${cell(v)}`,
      );
    case "rows": {
      const rows = (Array.isArray(result) ? result : []) as Record<
        string,
        unknown
      >[];
      const columns = output.fields
        ? Object.keys(output.fields)
        : Object.keys(rows[0] ?? {});
      const cells = rows.map((row) => columns.map((c) => cell(row[c])));
      const widths = columns.map((c, i) =>
        Math.max(c.length, ...cells.map((r) => (r[i] ?? "").length)),
      );
      const line = (values: string[]) =>
        values
          .map((v, i) => v.padEnd(widths[i] ?? 0))
          .join("  ")
          .trimEnd();
      return [line(columns.map((c) => c.toUpperCase())), ...cells.map(line)];
    }
  }
}

function cell(value: unknown): string {
  if (value === undefined || value === null) return "";
  return typeof value === "string" ? value : JSON.stringify(value);
}
