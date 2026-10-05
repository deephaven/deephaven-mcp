import { Command, EnumType, ValidationError } from "@cliffy/command";
import { DhError } from "./auth/errors.ts";
import { Store } from "./auth/store.ts";
import { agents } from "./commands/agents.ts";
import { authCommand } from "./commands/auth/index.ts";
import { err, out, reportError, setFormat } from "./output.ts";
import { autoUpdate } from "./updater.ts";
import { BIN, VERSION } from "./version.ts";

/** Known before parsing, so even argument errors honor `-o json`. */
function formatFromArgs(args: string[]): "human" | "json" {
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    const value = a === "-o" || a === "--output"
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

async function statusLine(): Promise<void> {
  try {
    const { config } = await new Store().read();
    const p = config.defaultProfile && config.profiles[config.defaultProfile];
    out(
      p
        ? `Logged in as ${p.user} @ ${new URL(p.server).host} (default)`
        : `Not logged in. Run \`${BIN} auth login\` to get started.`,
    );
  } catch {
    out(`Run \`${BIN} auth\` to check your profiles.`);
  }
  out(`For agents: \`${BIN} agents\` prints commands and error codes as JSON.`);
}

const root = new Command()
  .name(BIN)
  .version(VERSION)
  .description("Deephaven command line interface.")
  .throwErrors()
  .globalType("format", new EnumType(["human", "json"]))
  .globalOption("--profile <name:string>", "Profile to use (env DH_PROFILE).")
  .globalOption("-o, --output <format:format>", "Output format.", {
    default: "human",
  })
  .globalOption("--no-input", "Never prompt.")
  .action(async function () {
    this.showHelp();
    await statusLine();
  })
  .command("auth", authCommand())
  .command(
    "agents",
    "Print the command tree, flags and error codes as JSON.",
  )
  .action(function () {
    agents(this.getMainCommand());
  })
  .reset();

setFormat(formatFromArgs(Deno.args));
let code = 0;
try {
  await root.parse(Deno.args);
} catch (e) {
  code = reportError(
    e instanceof ValidationError
      ? new DhError("usage", e.message, `Run \`${BIN} --help\` for usage.`)
      : e,
  );
}

try {
  await autoUpdate();
} catch (e) {
  if (Deno.env.get("DH_DEBUG")) err(`${BIN}: auto-update failed: ${e}`);
}
// The Deephaven client libraries keep sockets open.
Deno.exit(code);
