import { styleText } from "node:util";
import { Command, Help } from "commander";
import { autoUpdate } from "./updater.ts";
import { VERSION } from "./version.ts";

const blue = (s: string) => styleText("blue", s);

await new Command()
  .name("dh")
  .version(`dh ${VERSION}`, "-V, --version", "Show the version number.")
  .helpOption("-h, --help", "Show this help.")
  .description("Deephaven command line interface.")
  .configureHelp({
    styleTitle: (s) => styleText("bold", s),
    styleCommandText: (s) => styleText(["bold", "magenta"], s),
    styleOptionTerm: blue,
    styleSubcommandTerm: blue,
    styleArgumentTerm: blue,
    // Show the version under "Usage:"; it matters because dh updates itself.
    formatHelp(cmd, helper) {
      const [usage = "", ...rest] = Help.prototype.formatHelp
        .call(helper, cmd, helper)
        .split("\n");
      const version = `${helper.styleTitle("Version:")} ${styleText("yellow", VERSION)}`;
      return [usage, version, ...rest].join("\n");
    },
  })
  // outputHelp, not help: help() exits, which would skip the update check below.
  .action((_options, command: Command) => command.outputHelp())
  .showHelpAfterError()
  // Usage errors exit 2 (the CLI convention, and what the Cliffy version did); help/version exit 0.
  .exitOverride((err) => process.exit(err.exitCode === 1 ? 2 : err.exitCode))
  .parseAsync();

try {
  await autoUpdate();
} catch (e) {
  if (process.env.DH_DEBUG) console.error(`dh: auto-update failed: ${e}`);
}
