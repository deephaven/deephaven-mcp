import { fstatSync } from "node:fs";
import { basename } from "node:path";
import { type Command, Option } from "commander";
import { runSelf } from "../../actions/auth/session.ts";
import { recordAccount } from "../../actions/plugins/cache.ts";
import type { Plugin } from "../../actions/plugins/discover.ts";
import { runTool } from "../../actions/plugins/run.ts";
import {
  bringsOwnIdentity,
  isCurrentUser,
  localAccount,
  runAsLine,
} from "../../actions/plugins/run-as.ts";
import {
  localPath,
  readConfig,
  requireProfile,
  serverSsh,
  sshSettings,
} from "../../actions/plugins/settings.ts";
import type {
  PluginSpec,
  RunAs,
  SpecCommand,
  SpecOption,
} from "../../actions/plugins/spec.ts";
import { runOverSsh } from "../../actions/plugins/ssh.ts";
import { syncShell } from "../../actions/plugins/sync.ts";
import {
  type CommandSpec,
  type Context,
  defineCommand,
} from "../../cli/command.ts";
import { DhError } from "../../errors.ts";
import { BIN } from "../../version.ts";
import { askSshHost } from "./questions.ts";

/**
 * The top-level commands a `shell` plugin registers: one per noun of its
 * spec. A plugin that's unavailable (it needs another `dh`) gets hidden nouns
 * whose every use is `plugin_unavailable`; an invalid one gets none.
 */
export function pluginCommands(plugin: Plugin): CommandSpec<never>[] {
  if (plugin.state === "unavailable") {
    return (plugin.entry?.nouns ?? []).map((noun) =>
      unavailableCommand(plugin, noun),
    );
  }
  const spec = plugin.spec;
  if (plugin.state !== "ready" || !spec) return [];
  const runAs = spec.tool.runAs;
  if (plugin.exec === "js") return [];
  const marker = {
    name: plugin.name,
    kind: "shell" as const,
    tool: basename(spec.tool.path),
    exec: plugin.exec,
    ...(runAs ? { runAs } : {}),
  };
  const help = runAs
    ? runAsLine(runAs, plugin.source.accounts[runAs.account])
    : undefined;
  const nouns: (PluginSpec | SpecCommand)[] = spec.nouns ?? [spec];
  return nouns.map((noun) => ({
    ...commandFor(plugin, spec, noun, [], help),
    plugin: marker,
  }));
}

/** A noun of a plugin this `dh` can't run: hidden, and every use gives why. */
export function unavailableCommand(
  plugin: Plugin,
  noun: string,
): CommandSpec<never> {
  return defineCommand({
    name: noun,
    description: plugin.entry?.description ?? "",
    hidden: true,
    passthrough: true,
    async run() {
      throw new DhError(
        "plugin_unavailable",
        `${plugin.name} ${plugin.reason ?? "can't run here"}`,
        `Update ${BIN}, or run \`${BIN} plugin sync\`.`,
      );
    },
  });
}

/** A group for a node with `commands`, else a leaf that runs the tool. `path` is the names above `node`. */
function commandFor(
  plugin: Plugin,
  spec: PluginSpec,
  node: PluginSpec | SpecCommand,
  path: string[],
  help: string | undefined,
): CommandSpec<never> {
  const here = [...path, node.name];
  const command = node.commands
    ? groupCommand(
        node,
        here,
        node.commands.map((c) => commandFor(plugin, spec, c, here, help)),
      )
    : leafCommand(plugin, spec, node);
  return help ? { ...command, helpText: help } : command;
}

/** `dh ctl` alone: its help, or its subcommands with `-o json`. */
function groupCommand(
  node: PluginSpec | SpecCommand,
  path: string[],
  commands: CommandSpec<never>[],
): CommandSpec<never> {
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

/** Runs the tool, here or over SSH, with the node's `argv` and every argument the user typed. */
function leafCommand(
  plugin: Plugin,
  spec: PluginSpec,
  node: PluginSpec | SpecCommand,
): CommandSpec<never> {
  return defineCommand({
    name: node.name,
    description: node.description,
    args: [
      {
        name: "[args...]",
        description: "Arguments and flags, passed on as typed.",
      },
    ],
    // Shown in help and `dh agents`; never parsed, since the argument rewrite
    // in cli/plugin-args.ts puts every one of them behind `--`.
    options: (node.options ?? []).flatMap(displayOption),
    passthrough: true,
    async run(ctx, args) {
      const user = args.filter((a) => a !== undefined);
      const argv = [...(node.argv ?? []), ...user];
      // The tool's own credential flags among the user's arguments: no elevation.
      const runAs =
        spec.tool.runAs && !bringsOwnIdentity(user, spec.tool.runAs.unlessFlags)
          ? spec.tool.runAs
          : undefined;
      const stdout =
        plugin.exec === "ssh"
          ? await runSsh(ctx, plugin, spec, argv, runAs)
          : await runLocal(ctx, spec, argv, runAs);
      return { human: [], json: { stdout } };
    },
  });
}

/** Set in a run again after the bundle changed, so a second change fails instead of running again. */
const RECHECKED = "DH_PLUGIN_RECHECKED";

/**
 * The tool on this machine, at the machine's `path`, else the spec's; with
 * `runAs`, as that service account (through `sudo` unless this is it).
 */
async function runLocal(
  ctx: Context,
  spec: PluginSpec,
  argv: string[],
  runAs: RunAs | undefined,
): Promise<string | undefined> {
  const config = await readConfig(ctx.store);
  const account = runAs ? await localAccount(runAs.account) : undefined;
  return await runTool({
    plugin: spec.name,
    path: localPath(config, spec),
    args: argv,
    captureStdout: ctx.json,
    lifecycle: ctx.lifecycle,
    ...(runAs && account && !isCurrentUser(account)
      ? { sudo: { account, runAs, interactive: isInteractive(ctx) } }
      : {}),
  });
}

/** A terminal on both ends, and prompts allowed: the tool and `sudo` may ask. */
function isInteractive(ctx: Context): boolean {
  return (
    process.stdin.isTTY === true &&
    process.stdout.isTTY === true &&
    !ctx.json &&
    !ctx.noPrompt
  );
}

/**
 * The spec's `tool.path` on the active profile's server, on `--ssh-host` or
 * the default host. The same session first checks the cached bundle's
 * revision; if it changed, the tool doesn't run: the bundle is copied and
 * `dh` runs again.
 */
async function runSsh(
  ctx: Context,
  plugin: Plugin,
  spec: PluginSpec,
  argv: string[],
  runAs: RunAs | undefined,
): Promise<string | undefined> {
  const config = await readConfig(ctx.store);
  const { profile } = requireProfile(config, ctx.profileFlag);
  const settings = sshSettings(config, profile, plugin.name);
  const host = askSshHost(settings, plugin.name, ctx.sshHost);
  const interactive = isInteractive(ctx);
  const revision = plugin.source.manifest?.revision;
  const result = await runOverSsh({
    plugin: plugin.name,
    host,
    toolPath: spec.tool.path,
    words: [spec.tool.path, ...argv],
    interactive,
    captureStdout: ctx.json,
    lifecycle: ctx.lifecycle,
    ...(runAs ? { runAs } : {}),
    ...(revision
      ? {
          checkRevision: {
            shellDir: `${serverSsh(config, profile.server).bundle}/shell`,
            revision,
          },
        }
      : {}),
  });
  if (result.changed) return await afterBundleChanged(ctx, plugin, host);
  if (result.maybeChanged) {
    // In a terminal the check is silent: exit 254 is the check or the tool's own.
    const sync = await syncShell(ctx.store, config, profile.server, {
      host,
      prompt: false,
    });
    if (sync.status === "unchanged") throw result.maybeChanged.failure;
    return await afterBundleChanged(ctx, plugin, host, sync.removed);
  }
  if (runAs && result.account) {
    await recordAccount(
      ctx.store,
      profile.server,
      runAs.account,
      result.account,
    );
  }
  return result.stdout;
}

/**
 * The bundle on `host` changed, so the tool didn't run: copy it (unless
 * `removed` says that's done) and run `dh` again, which loads it. A plugin
 * the new bundle doesn't have is `plugin_unavailable`.
 */
async function afterBundleChanged(
  ctx: Context,
  plugin: Plugin,
  host: string,
  removed?: string[],
): Promise<never> {
  if (process.env[RECHECKED] !== undefined) {
    throw new DhError(
      "plugin_unavailable",
      `The dh plugins on ${host} changed again while ${plugin.name} was starting`,
      `Run \`${BIN} plugin sync\`, then the command again.`,
    );
  }
  let gone = removed;
  if (gone === undefined) {
    const config = await readConfig(ctx.store);
    const { profile } = requireProfile(config, ctx.profileFlag);
    gone = (
      await syncShell(ctx.store, config, profile.server, {
        host,
        prompt: false,
      })
    ).removed;
  }
  if (gone.includes(plugin.name)) {
    throw new DhError(
      "plugin_unavailable",
      `${plugin.name} was removed from ${host}'s dh plugins`,
      `Run \`${BIN} plugin list\` for what's there now.`,
    );
  }
  if (stdinMayBeRead()) {
    throw new DhError(
      "plugin_unavailable",
      `${plugin.name}'s spec changed on ${host} and was updated; its input was already read, so run the command again`,
    );
  }
  process.exit(await runSelf(process.argv.slice(2), { [RECHECKED]: "1" }));
}

/** Whether stdin is a pipe or a file, which a run over SSH may have read from. */
function stdinMayBeRead(): boolean {
  try {
    const s = fstatSync(0);
    return s.isFIFO() || s.isFile();
  } catch {
    return false;
  }
}

/** Flags `dh` itself takes on plugin commands, or everywhere; a tool option spelled the same isn't shown. */
const DH_FLAGS = new Set([
  "-o",
  "--output",
  "-h",
  "--help",
  "-V",
  "--version",
  "--profile",
  "--ssh-host",
  "--no-prompt",
  "--no-input",
]);

/**
 * A spec option as a commander `Option`, for display. Commander can't spell
 * Commons CLI's multi-letter short flags (`-pf`), so only `-x` and `--long`
 * spellings are shown; the tool accepts those too. None if nothing's left, or
 * it would read as one of `dh`'s own flags.
 */
function displayOption(option: SpecOption): Option[] {
  const tokens = option.flags.split(/,\s*|\s+/).filter(Boolean);
  const names = tokens.filter((t) => t.startsWith("-"));
  const value = tokens.find((t) => !t.startsWith("-"));
  const shown = names.filter(
    (n) => /^-[a-z0-9]$/i.test(n) || n.startsWith("--"),
  );
  if (shown.length === 0 || names.some((n) => DH_FLAGS.has(n))) return [];
  const description = option.required
    ? `${option.description} (required)`
    : option.description;
  // Commander wants `<value>`; a hand-written spec may say `VALUE`.
  const arg =
    value && !/^[<[]/.test(value) ? `<${value.toLowerCase()}>` : value;
  try {
    return [
      new Option(`${shown.join(", ")}${arg ? ` ${arg}` : ""}`, description),
    ];
  } catch {
    // Spelled in a way commander can't show; it still passes through.
    return [];
  }
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
