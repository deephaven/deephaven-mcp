import type { Command, Option } from "commander";
import { ERROR_CODES } from "../auth/errors.ts";
import { out } from "../output.ts";
import { BIN, VERSION } from "../version.ts";

const describeOption = (o: Option) => ({
  flags: o.flags,
  description: o.description,
});

function describeCommand(cmd: Command, path: string[]): unknown {
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

/** Machine-readable command tree, flags, and error codes. */
export function agents(root: Command): void {
  out(
    JSON.stringify({
      name: BIN,
      version: VERSION,
      globalOptions: root.options.filter((o) => !o.hidden).map(describeOption),
      commands: root.commands.map((c) => describeCommand(c, [BIN])),
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
    }),
  );
}
