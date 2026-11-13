import { writeSync } from "node:fs";
import { styleText } from "node:util";
import { Command, CommanderError, Help } from "commander";
import { type Discovery, discoverPlugins } from "./actions/plugins/discover.ts";
import { refreshPlugins } from "./actions/plugins/refresh.ts";
import {
  type ActiveProfile,
  activeProfile,
  readConfigQuietly,
} from "./actions/plugins/settings.ts";
import {
  type CommandSpec,
  commandAt,
  commandPath,
  GLOBAL_OPTIONS,
  pluginExecOf,
  registerCommand,
} from "./cli/command.ts";
import { CommandLifecycle } from "./cli/lifecycle.ts";
import { routePluginArgs, startupArgs } from "./cli/plugin-args.ts";
import { runWithLifecycle } from "./cli/run.ts";
import { pluginGroup } from "./commands/plugin/index.ts";
import { jsCommands } from "./commands/plugin/js.ts";
import { pluginCommands } from "./commands/plugin/tool.ts";
import { rootCommand } from "./commands/root.ts";
import { commandUsageError, reasonOf } from "./errors.ts";
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
    const value = outputFlagValue(a, args[i + 1]);
    if (value === "json" || value === "human") format = value;
  }
  return format;
}

/** The value `arg` gives `-o`, in any spelling: `-o json`, `-ojson`, `--output json`, `--output=json`. */
function outputFlagValue(
  arg: string,
  next: string | undefined,
): string | undefined {
  if (arg === "-o" || arg === "--output") return next;
  if (arg.startsWith("--output=")) return arg.slice("--output=".length);
  if (arg.startsWith("-o") && arg.length > 2) return arg.slice(2);
  return undefined;
}

/**
 * The command tree `build` makes, plus `dh plugin` and each plugin's nouns,
 * from what startup discovers (the local bundle and the caches; no network).
 * Never fails: what can't be read is reported by `dh plugin list`.
 */
async function withPlugins(
  build: (found?: Discovery) => CommandSpec<never>,
  args: string[],
): Promise<CommandSpec<never>> {
  const builtins = [...(build().commands ?? []).map((c) => c.name), "plugin"];
  const config = await readConfigQuietly();
  let active: ActiveProfile | undefined;
  try {
    active = activeProfile(config, startupArgs(args).profile);
  } catch {
    // An unknown --profile: the default's plugins, so the command reports it.
    active = activeProfile(config, undefined);
  }
  let found: Discovery;
  try {
    found = await discoverPlugins(config, active, builtins);
  } catch (e) {
    const reason = `can't read plugins: ${reasonOf(e) ?? "unknown error"}`;
    found = {
      sources: [],
      noSource: [{ channel: "shell", reason }],
      plugins: [],
      builtins,
    };
  }
  const spec = build(found);
  return {
    ...spec,
    commands: [
      ...(spec.commands ?? []),
      pluginGroup(found),
      ...found.plugins.flatMap((p) =>
        p.channel === "js" ? jsCommands(p) : pluginCommands(p),
      ),
    ],
  };
}

const root = new Command()
  .name(BIN)
  .version(`${BIN} ${VERSION}`, "-V, --version", "Show the version number.")
  .helpOption("-h, --help", "Show this help.")
  .configureHelp({
    showGlobalOptions: true,
    styleTitle: (s) => styleText("bold", s),
    styleCommandText: (s) => styleText(["bold", "magenta"], s),
    styleOptionTerm: blue,
    styleSubcommandTerm: blue,
    styleArgumentTerm: blue,
    // A plugin's commands take only `-o` of the global flags (and their own
    // -h); over SSH, `--profile`, `--ssh-host` (hidden elsewhere) and
    // `--no-prompt` too.
    visibleGlobalOptions(cmd) {
      const all = Help.prototype.visibleGlobalOptions.call(this, cmd);
      const exec = pluginExecOf(cmd);
      if (!exec) return all;
      const shown =
        exec === "ssh"
          ? ["--output", "--profile", "--ssh-host", "--no-prompt"]
          : ["--output"];
      return shown.flatMap((long) =>
        GLOBAL_OPTIONS.filter((o) => o.long === long),
      );
    },
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
const args = process.argv.slice(2);
const lifecycle = new CommandLifecycle();
registerCommand(root, await withPlugins(rootCommand, args), lifecycle);

setFormat(formatFromArgs(args));
let code = 0;
try {
  await runWithLifecycle(lifecycle, async () => {
    try {
      await root.parseAsync(routePluginArgs(root, args), { from: "user" });
    } catch (error) {
      if (error instanceof CommanderError && error.exitCode !== 0) {
        throw commandUsageError(
          error.message,
          commandPath(commandAt(root, args).cmd),
        );
      }
      throw error;
    }
  });
} catch (e) {
  // --help and --version: answered, with no update check.
  if (e instanceof CommanderError && e.exitCode === 0) process.exit(0);
  code = reportError(e, {
    ...lifecycle.failureReport,
    command:
      lifecycle.failureReport.command ??
      commandPath(commandAt(root, args).cmd, false),
  });
}

// Not after a cancel: the user wants out.
if (code !== 3) {
  try {
    await refreshPlugins(commandAt(root, args).cmd, startupArgs(args).profile);
  } catch (e) {
    if (process.env.DH_DEBUG) err(`${BIN}: plugin check failed: ${e}`);
  }
  try {
    await autoUpdate();
  } catch (e) {
    if (process.env.DH_DEBUG) err(`${BIN}: auto-update failed: ${e}`);
  }
}
// The Deephaven client libraries keep sockets open.
process.exit(code);
