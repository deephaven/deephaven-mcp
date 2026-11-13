import { type Command, InvalidArgumentError, Option } from "commander";
import { runSelf } from "../../actions/auth/session.ts";
import type { Plugin } from "../../actions/plugins/discover.ts";
import { importPlugin, leafIn, runLeaf } from "../../actions/plugins/js-run.ts";
import type { JsNode, JsOption } from "../../actions/plugins/plugin-json.ts";
import { readConfig, requireProfile } from "../../actions/plugins/settings.ts";
import { checkJsRevision, syncJs } from "../../actions/plugins/sync.ts";
import {
  type CommandSpec,
  type Context,
  defineCommand,
} from "../../cli/command.ts";
import { DhError } from "../../errors.ts";
import { BIN } from "../../version.ts";
import { unavailableCommand } from "./tool.ts";

/** Set in a run again after the bundle changed, so a second change fails instead of running again. */
const RECHECKED = "DH_PLUGIN_RECHECKED";

/**
 * The top-level commands a `js` plugin registers, from its `plugin.json`:
 * ordinary `dh` commands with real options, each leaf running the module's
 * `run`. An unavailable plugin's nouns are hidden and give
 * `plugin_unavailable`.
 */
export function jsCommands(plugin: Plugin): CommandSpec<never>[] {
  if (plugin.state === "unavailable") {
    return (plugin.entry?.nouns ?? []).map((noun) =>
      unavailableCommand(plugin, noun),
    );
  }
  const json = plugin.pluginJson;
  if (plugin.state !== "ready" || !json) return [];
  const marker = { name: plugin.name, kind: "js" as const };
  const nouns: JsNode[] = json.nouns ?? [
    { name: json.name, description: json.description, commands: json.commands },
  ];
  return nouns.map((noun) => ({
    ...nodeCommand(plugin, noun, []),
    plugin: marker,
  }));
}

function nodeCommand(
  plugin: Plugin,
  node: JsNode,
  above: string[],
): CommandSpec<never> {
  const path = [...above, node.name];
  if (node.commands) {
    const commands = node.commands.map((c) => nodeCommand(plugin, c, path));
    return defineCommand({
      name: node.name,
      description: node.description,
      commands,
      async run(ctx) {
        return {
          human: findCommand(ctx.cli, path).helpInformation().trimEnd(),
          json: {
            commands: commands.map((c) => ({
              name: c.name,
              description: c.description,
            })),
          },
        };
      },
    });
  }
  const args = node.args ?? [];
  return defineCommand<Record<string, never>>({
    name: node.name,
    description: node.description,
    args: args.map((a) => ({
      name: a.required ? `<${kebab(a.name)}>` : `[${kebab(a.name)}]`,
      description: a.description,
    })),
    options: (node.options ?? []).map(optionOf),
    ...(node.output ? { output: node.output } : {}),
    async run(ctx, values, opts) {
      const named = Object.fromEntries(args.map((a, i) => [a.name, values[i]]));
      return await runJs(ctx, plugin, path, node, named, opts);
    },
  }) as CommandSpec<never>;
}

/** An option of `plugin.json` as commander parses it. */
function optionOf(o: JsOption): Option {
  const option = new Option(o.flags, o.description);
  if (o.type === "number") {
    option.argParser((value: string, previous: unknown) => {
      const n = Number(value);
      if (!Number.isFinite(n)) throw new InvalidArgumentError("Not a number.");
      return o.multiple ? [...(Array.isArray(previous) ? previous : []), n] : n;
    });
  }
  if (o.choices) option.choices(o.choices);
  if (o.default !== undefined) option.default(o.default);
  if (o.required) option.makeOptionMandatory();
  return option;
}

/**
 * Checks the `js` part's revision (one conditional request; not for
 * `DH_PLUGIN_BUNDLE`), then imports the module and runs the leaf. A changed
 * part is fetched and `dh` runs again; a plugin it no longer has is
 * `plugin_unavailable`.
 */
async function runJs(
  ctx: Context,
  plugin: Plugin,
  path: string[],
  node: JsNode,
  args: Record<string, string | undefined>,
  opts: Record<string, unknown>,
) {
  const config = await readConfig(ctx.store);
  const active = requireProfile(config, ctx.profileFlag);
  const requires = plugin.pluginJson?.requires ?? "any";
  const kind = config.servers[active.profile.server]?.kind;
  if (requires !== "any" && kind !== requires) {
    const article = requires === "enterprise" ? "an Enterprise" : "a Community";
    throw new DhError(
      "usage",
      `"${plugin.name}" needs ${article} profile`,
      `Run \`${BIN} auth\` to list profiles.`,
    );
  }
  const source = plugin.source;
  if (source.kind === "web" && source.origin) {
    const check = await checkJsRevision(source.origin, {
      revision: source.manifest?.revision,
      etag: source.state?.etag,
    });
    if (check.kind !== "unchanged") {
      await afterChange(ctx, plugin, source.origin);
    }
  }
  const leaf = leafIn(plugin, await importPlugin(plugin), path);
  return await runLeaf(leaf, {
    ctx,
    plugin,
    path,
    node,
    active,
    args,
    options: opts as Record<string, string | number | boolean | string[]>,
  });
}

/** The server's `js/` part changed: fetch it, then run `dh` again, which loads it. */
async function afterChange(
  ctx: Context,
  plugin: Plugin,
  origin: string,
): Promise<never> {
  if (process.env[RECHECKED] !== undefined) {
    throw new DhError(
      "plugin_unavailable",
      `The dh plugins on ${origin} changed again while ${plugin.name} was starting`,
      `Run \`${BIN} plugin sync\`, then the command again.`,
    );
  }
  const sync = await syncJs(ctx.store, origin, {
    profileFlag: ctx.profileFlag,
    groups: false,
    lifecycle: ctx.lifecycle,
  });
  if (sync.status === "none" || sync.removed.includes(plugin.name)) {
    throw new DhError(
      "plugin_unavailable",
      `${plugin.name} was removed from ${origin}'s dh plugins`,
      `Run \`${BIN} plugin list\` for what's there now.`,
    );
  }
  process.exit(
    await runSelf(process.argv.slice(2), { [RECHECKED]: "1" }, ctx.lifecycle),
  );
}

/** `serverName` → `server-name`. */
function kebab(name: string): string {
  return name.replace(/[A-Z]/g, (c) => `-${c.toLowerCase()}`);
}

/** The commander command at `path` under `root`. */
function findCommand(root: Command, path: string[]): Command {
  let cmd = root;
  for (const name of path) {
    const sub = cmd.commands.find((c) => c.name() === name);
    if (!sub) break;
    cmd = sub;
  }
  return cmd;
}
