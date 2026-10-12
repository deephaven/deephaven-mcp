import type { Command, Option } from "commander";
import { ERROR_CODES } from "../auth/errors.ts";
import { defineCommand } from "../cli/command.ts";
import { BIN, VERSION } from "../version.ts";

interface Described {
  command: string;
  description: string;
  arguments: { name: string; optional: boolean }[];
  options: { flags: string; description: string }[];
  commands: Described[];
}

const describeOption = (o: Option) => ({
  flags: o.flags,
  description: o.description,
});

function describeCommand(cmd: Command, path: string[]): Described {
  return {
    command: [...path, cmd.name()].join(" "),
    description: cmd.description(),
    arguments: cmd.registeredArguments.map((a) => ({
      name: a.name(),
      optional: !a.required,
    })),
    options: cmd.options.filter((o) => !o.hidden).map(describeOption),
    commands: cmd.commands.map((c) =>
      describeCommand(c, [...path, cmd.name()]),
    ),
  };
}

/** One line per command, e.g. `dh auth login [server]`, depth-first. */
function usageLines(commands: Described[]): [string, string][] {
  return commands.flatMap((c) => [
    [
      [
        c.command,
        ...c.arguments.map((a) => (a.optional ? `[${a.name}]` : `<${a.name}>`)),
      ].join(" "),
      c.description,
    ],
    ...usageLines(c.commands),
  ]);
}

/** Command tree and error codes: a summary, or everything with `-o json`. */
export const agents = defineCommand({
  name: "agents",
  description: "Summarize commands and error codes; -o json for every flag.",

  async run(ctx) {
    const manifest = {
      name: BIN,
      version: VERSION,
      globalOptions: ctx.cli.options
        .filter((o) => !o.hidden)
        .map(describeOption),
      commands: ctx.cli.commands.map((c) => describeCommand(c, [BIN])),
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
    };
    const commands = usageLines(manifest.commands);
    const width = Math.max(...commands.map(([usage]) => usage.length));
    const codeWidth = Math.max(...manifest.errors.map((e) => e.code.length));
    return {
      human: [
        `${BIN} ${VERSION}. Every command takes \`-o json\` for JSON output and \`--no-prompt\` to fail instead of prompting.`,
        "",
        "Commands:",
        ...commands.map(([usage, text]) => `  ${usage.padEnd(width)}  ${text}`),
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
