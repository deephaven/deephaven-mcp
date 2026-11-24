import { Option } from "commander";
import { type ExecResult, execInConsole } from "../../actions/console/exec.ts";
import { defineCommand } from "../../cli/command.ts";
import { askExecSource } from "./questions.ts";

interface ExecOptions {
  /** Inline code, or true when `-c` reads from stdin. */
  code?: string | true;
  /** Explicit console shortcode or server ID. */
  console?: string;
}

export const exec = defineCommand<ExecOptions>({
  name: "exec",
  description: "Run a local script or inline code on a console.",
  args: [{ name: "[file]", description: "Local script path." }],
  options: [
    new Option("-c, --code [code]", "Inline code, or read code from stdin."),
    new Option("--console <shortcode-or-id>", "Console to execute on."),
  ],

  async run(ctx, [file], opts) {
    const source = await askExecSource(file, opts.code);
    const result = await execInConsole(
      ctx.store,
      source,
      opts.console,
      ctx.profileFlag,
      ctx.lifecycle,
    );
    const human = execHuman(result);
    return {
      human,
      json: result,
    };
  },
});

/** Format changed exports as one human-readable line per category. */
function execHuman(result: ExecResult): string[] {
  const lines: string[] = [];
  if (result.console.created) {
    lines.push(`Created console ${result.console.shortcode}`);
  }
  const visible = [...result.changes.created, ...result.changes.updated];
  if (visible.length > 0) {
    lines.push(
      visible
        .map((change) => `${change.name}:${change.type.toLowerCase()}`)
        .join(","),
    );
  }
  if (result.changes.removed.length > 0) {
    lines.push(
      `removed ${result.changes.removed
        .map((change) => `${change.name}:${change.type.toLowerCase()}`)
        .join(",")}`,
    );
  }
  return lines;
}
