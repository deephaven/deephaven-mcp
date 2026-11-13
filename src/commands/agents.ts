import type { Command, Option } from "commander";
import type { Discovery } from "../actions/plugins/discover.ts";
import {
  defineCommand,
  isHidden,
  outputOf,
  PLUGIN_GLOBAL_FLAGS,
  pluginOf,
} from "../cli/command.ts";
import { ERROR_CODES } from "../errors.ts";
import { BIN, VERSION } from "../version.ts";
import { channelsOf } from "./plugin/index.ts";

/** One command in the `dh agents -o json` listing. */
interface Described {
  /** The full command, e.g. `dh auth login`. */
  command: string;
  description: string;
  /** `variadic`: takes any number of values, e.g. a plugin command's arguments for its tool. */
  arguments: { name: string; optional: boolean; variadic: boolean }[];
  options: { flags: string; description: string }[];
  /** On a plugin's noun only: the plugin it belongs to (one plugin may register several nouns). */
  plugin?: string;
  /** Beside `plugin`: `shell` (runs a tool) or `js` (runs in dh). */
  pluginKind?: "shell" | "js";
  /** Beside a `shell` plugin: the tool it and its subcommands run. */
  pluginTool?: string;
  /** Beside `plugin`: where it runs, which picks the `pluginFlags` entry. */
  pluginExec?: "local" | "ssh";
  /** Beside `plugin`: the service account its tool runs as, unless the user passes one of `unlessFlags`. */
  runAs?: { account: string; unlessFlags: string[] };
  /** A `js` plugin's leaf: what `-o json` gives (`rows`, `object`, `text` or `none`) and its fields. */
  output?: { kind: string; fields?: Record<string, unknown> };
  commands: Described[];
}

const describeOption = (o: Option) => ({
  flags: o.flags,
  description: o.description,
});

/** `cmd` and its subcommands as data; `path` is the parent command names. */
function describeCommand(cmd: Command, path: string[]): Described {
  const plugin = pluginOf(cmd);
  return {
    command: [...path, cmd.name()].join(" "),
    description: cmd.description(),
    arguments: cmd.registeredArguments.map((a) => ({
      name: a.name(),
      optional: !a.required,
      variadic: a.variadic,
    })),
    options: cmd.options.filter((o) => !o.hidden).map(describeOption),
    ...(plugin?.kind === "shell"
      ? {
          plugin: plugin.name,
          pluginKind: plugin.kind,
          pluginTool: plugin.tool,
          pluginExec: plugin.exec,
        }
      : {}),
    ...(plugin?.kind === "js"
      ? { plugin: plugin.name, pluginKind: plugin.kind }
      : {}),
    ...(outputOf(cmd) ? { output: outputOf(cmd) } : {}),
    ...(plugin?.kind === "shell" && plugin.runAs
      ? {
          runAs: {
            account: plugin.runAs.account,
            unlessFlags: plugin.runAs.unlessFlags ?? [],
          },
        }
      : {}),
    commands: visible(cmd).map((c) =>
      describeCommand(c, [...path, cmd.name()]),
    ),
  };
}

/** `cmd`'s subcommands, without hidden ones (an unavailable plugin's nouns). */
function visible(cmd: Command): Command[] {
  return cmd.commands.filter((c) => !isHidden(c));
}

/** One line per command, e.g. `dh auth login [server]`, depth-first. */
function usageLines(commands: Described[]): [string, string][] {
  return commands.flatMap((c) => [
    [
      [
        c.command,
        ...c.arguments.map((a) => {
          const name = a.variadic ? `${a.name}...` : a.name;
          return a.optional ? `[${name}]` : `<${name}>`;
        }),
      ].join(" "),
      c.description,
    ],
    ...usageLines(c.commands),
  ]);
}

/**
 * Command tree and error codes: a summary, or everything with `-o json`.
 * `found`: where this run's plugins came from, per channel.
 */
export const agentsCommand = (found?: Discovery) =>
  defineCommand({
    name: "agents",
    description: "Summarize commands and error codes; -o json for every flag.",

    async run(ctx) {
      const manifest = {
        name: BIN,
        version: VERSION,
        globalOptions: ctx.cli.options
          .filter((o) => !o.hidden)
          .map(describeOption),
        commands: visible(ctx.cli).map((c) => describeCommand(c, [BIN])),
        // Stated once instead of per command: plugin commands take only these
        // global flags, by the noun's `pluginExec`.
        pluginFlags: {
          ...PLUGIN_GLOBAL_FLAGS,
          rest: "passed to the plugin's tool",
        },
        errors: Object.entries(ERROR_CODES).map(([code, e]) => ({
          code,
          exit: e.exit,
          description: e.description,
        })),
        errorFormat: {
          stream: "stderr",
          when: "-o json",
          fields: ["error", "code", "exit", "hint"],
        },
        // Where the plugin commands above came from; `dh plugin list` has more.
        pluginChannels: found
          ? channelsOf(found, null).map((c) => ({
              channel: c.channel,
              source: c.source,
              server: c.server,
              revision: c.revision,
              reason: c.reason,
            }))
          : [],
      };
      const commands = usageLines(manifest.commands);
      const width = Math.max(...commands.map(([usage]) => usage.length));
      const codeWidth = Math.max(...manifest.errors.map((e) => e.code.length));
      // `js` plugins' commands are ordinary commands; the flag rule is the `shell` ones'.
      const plugins = manifest.commands.filter((c) => c.pluginKind === "shell");
      const overSsh = plugins.filter((c) => c.pluginExec === "ssh");
      const named = (cs: Described[]) =>
        cs.map((c) => `\`${c.command}\``).join(", ");
      return {
        human: [
          `${BIN} ${VERSION}. Every command takes \`-o json\` for JSON output and \`--no-prompt\` to fail instead of prompting.`,
          ...(plugins.length > 0
            ? [
                `Plugins (${named(plugins)}) take only \`-o json\`; every other flag goes to their tool.`,
              ]
            : []),
          ...(overSsh.length > 0
            ? [
                `Over SSH (${named(overSsh)}), they also take \`--profile\`, \`--ssh-host\` and \`--no-prompt\`.`,
              ]
            : []),
          "",
          "Commands:",
          ...commands.map(
            ([usage, text]) => `  ${usage.padEnd(width)}  ${text}`,
          ),
          "",
          "Errors (exit code; with -o json, stderr is {error, code, exit, hint}):",
          ...manifest.errors.map(
            (e) => `  ${e.code.padEnd(codeWidth)}  ${e.exit}  ${e.description}`,
          ),
          "",
          `Flags: \`${BIN} <command> --help\`. Everything above, with flags, as JSON: \`${BIN} agents -o json\`.`,
        ],
        json: manifest,
      };
    },
  });
