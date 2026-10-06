import type { Command } from "@cliffy/command";
import { ERROR_CODES } from "../auth/errors.ts";
import { out } from "../output.ts";
import { BIN, VERSION } from "../version.ts";

// deno-lint-ignore no-explicit-any
type AnyCommand = Command<any, any, any, any, any, any, any, any>;

function describeCommand(cmd: AnyCommand, path: string[]): unknown {
  return {
    command: [...path, cmd.getName()].join(" "),
    description: cmd.getDescription(),
    arguments: cmd.getArguments().map((a) => ({
      name: a.name,
      optional: a.optional,
    })),
    options: cmd.getBaseOptions(false)
      .filter((o) => !o.global)
      .map((o) => ({ flags: o.flags, description: o.description })),
    commands: cmd.getCommands(false)
      .map((c) => describeCommand(c, [...path, cmd.getName()])),
  };
}

/** Machine-readable command tree, flags, and error codes. */
export function agents(root: AnyCommand): void {
  out(JSON.stringify({
    name: BIN,
    version: VERSION,
    globalOptions: root.getBaseOptions(false)
      .filter((o) => o.global)
      .map((o) => ({ flags: o.flags, description: o.description })),
    commands: root.getCommands(false)
      .map((c) => describeCommand(c, [BIN])),
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
  }));
}
