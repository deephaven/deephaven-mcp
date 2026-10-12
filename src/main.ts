import { writeSync } from "node:fs";
import { styleText } from "node:util";
import { Command, CommanderError, Help, Option } from "commander";
import { DhError } from "./auth/errors.ts";
import { type Profile, Store } from "./auth/store.ts";
import { agents } from "./commands/agents.ts";
import { addAuthCommand } from "./commands/auth/index.ts";
import { err, reportError, result, setFormat } from "./output.ts";
import { autoUpdate } from "./updater.ts";
import { BIN, VERSION } from "./version.ts";

const blue = (s: string) => styleText("blue", s);

/** Known before parsing, so even argument errors honor `-o json`. */
function formatFromArgs(args: string[]): "human" | "json" {
  for (let i = 0; i < args.length; i++) {
    const a = args[i] ?? "";
    const value =
      a === "-o" || a === "--output"
        ? args[i + 1]
        : a.startsWith("--output=")
          ? a.slice("--output=".length)
          : a.startsWith("-o") && a.length > 2
            ? a.slice(2)
            : undefined;
    if (value === "json") return "json";
  }
  return "human";
}

async function rootStatus(command: Command): Promise<void> {
  let p: Profile | undefined;
  let readable = true;
  try {
    const { config } = await new Store().read();
    p = config.defaultProfile
      ? config.profiles[config.defaultProfile]
      : undefined;
  } catch {
    readable = false;
  }
  result(
    () => {
      command.outputHelp();
      return [
        !readable
          ? `Run \`${BIN} auth\` to check your profiles.`
          : p
            ? `Logged in as ${p.user} @ ${new URL(p.server).host} (default)`
            : `Not logged in. Run \`${BIN} auth login\` to get started.`,
        `For agents: \`${BIN} agents\` summarizes commands and error codes (\`-o json\` for everything).`,
      ];
    },
    {
      version: VERSION,
      default: p ? { profile: p.name, server: p.server, user: p.user } : null,
    },
  );
}

const root = new Command()
  .name(BIN)
  .version(`${BIN} ${VERSION}`, "-V, --version", "Show the version number.")
  .helpOption("-h, --help", "Show this help.")
  .description("Deephaven command line interface.")
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
  .exitOverride()
  .option("--profile <name>", "Profile to use (env DH_PROFILE).")
  .addOption(
    new Option("-o, --output <format>", "Output format.")
      .choices(["human", "json"])
      .default("human"),
  )
  .option("--no-prompt", "Never prompt; fail with the flag to pass instead.")
  // The older name; agents trained on other CLIs may still reach for it.
  .addOption(new Option("--no-input").hideHelp())
  .action((_options, command: Command) => rootStatus(command));
// Added after the settings above, so subcommands inherit them.
addAuthCommand(root);
root
  .command("agents")
  .description("Summarize commands and error codes; -o json for every flag.")
  .action(() => agents(root));

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
