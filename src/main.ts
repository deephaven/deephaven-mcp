import { writeSync } from "node:fs";
import { styleText } from "node:util";
import { Command, CommanderError, Help } from "commander";
import { DhError } from "./auth/errors.ts";
import { attach, GLOBAL_OPTIONS } from "./cli/command.ts";
import { dh } from "./commands/root.ts";
import { err, reportError, setFormat } from "./output.ts";
import { autoUpdate } from "./updater.ts";
import { BIN, VERSION } from "./version.ts";

const blue = (s: string) => styleText("blue", s);

/** Known before parsing, so even argument errors honor `-o json`. Last one wins, as in commander. */
function formatFromArgs(args: string[]): "human" | "json" {
  let format: "human" | "json" = "human";
  for (let i = 0; i < args.length; i++) {
    const a = args[i] ?? "";
    if (a === "--") break;
    const value =
      a === "-o" || a === "--output"
        ? args[i + 1]
        : a.startsWith("--output=")
          ? a.slice("--output=".length)
          : a.startsWith("-o") && a.length > 2
            ? a.slice(2)
            : undefined;
    if (value === "json" || value === "human") format = value;
  }
  return format;
}

const root = new Command()
  .name(BIN)
  .version(`${BIN} ${VERSION}`, "-V, --version", "Show the version number.")
  .helpOption("-h, --help", "Show this help.")
  .configureHelp({
    styleTitle: (s) => styleText("bold", s),
    styleCommandText: (s) => styleText(["bold", "magenta"], s),
    styleOptionTerm: blue,
    styleSubcommandTerm: blue,
    styleArgumentTerm: blue,
    // Show the version under "Usage:"; it matters because the CLI updates itself.
    formatHelp(cmd, helper) {
      const [usage = "", ...rest] = Help.prototype.formatHelp
        .call(helper, cmd, helper)
        .split("\n");
      const version = `${helper.styleTitle("Version:")} ${styleText("yellow", VERSION)}`;
      return [usage, version, ...rest].join("\n");
    },
  })
  // Synchronous, so nothing is lost when the process exits right after.
  .configureOutput({
    writeOut: (s) => writeSync(1, s),
    writeErr: (s) => writeSync(2, s),
    // Errors are printed by reportError, as text or JSON.
    outputError: () => {},
  })
  .exitOverride();
for (const option of GLOBAL_OPTIONS) root.addOption(option);
// After the settings above, so subcommands inherit them.
attach(root, dh);

setFormat(formatFromArgs(process.argv.slice(2)));
let code = 0;
try {
  await root.parseAsync();
} catch (e) {
  // --help and --version: answered, with no update check.
  if (e instanceof CommanderError && e.exitCode === 0) process.exit(0);
  code = reportError(
    e instanceof CommanderError
      ? new DhError(
          "usage",
          e.message.replace(/^error: /, ""),
          `Run \`${BIN} --help\` for usage.`,
        )
      : e,
  );
}

try {
  await autoUpdate();
} catch (e) {
  if (process.env.DH_DEBUG) err(`${BIN}: auto-update failed: ${e}`);
}
// The Deephaven client libraries keep sockets open.
process.exit(code);
