import { styleText } from "node:util";
import type { Profile } from "../actions/auth/store.ts";
import type { Discovery } from "../actions/plugins/discover.ts";
import { type CommandSpec, defineCommand } from "../cli/command.ts";
import { BIN, VERSION } from "../version.ts";
import { agentsCommand } from "./agents.ts";
import { authCommand } from "./auth/index.ts";
import { consoleCommand } from "./console/index.ts";

/**
 * `dh` alone: help and who's logged in. Every command is listed under
 * `commands`. `found`: the plugins startup discovered, for `dh agents`.
 */
export function rootCommand(found?: Discovery): CommandSpec<never> {
  return defineCommand({
    name: BIN,
    description: "Deephaven command line interface.",
    commands: [authCommand(), consoleCommand, agentsCommand(found)],

    async run(ctx) {
      let p: Profile | undefined;
      let readable = true;
      try {
        const { config } = await ctx.store.read();
        p = config.defaultProfile
          ? config.profiles[config.defaultProfile]
          : undefined;
      } catch {
        readable = false;
      }
      return {
        human: [
          ctx.cli.helpInformation().trimEnd(),
          "",
          loginLine(readable, p),
          styleText(
            "dim",
            `For agents: \`${BIN} agents\` summarizes commands and error codes (\`-o json\` for everything).`,
          ),
        ],
        json: {
          version: VERSION,
          default: p
            ? { profile: p.name, server: p.server, user: p.user }
            : null,
        },
      };
    },
  });
}

/** Who's logged in by default, or what to run next. `readable`: whether the config could be read. Colored only on a terminal. */
function loginLine(readable: boolean, p: Profile | undefined): string {
  if (!readable) return `! Run \`${BIN} auth\` to check your profiles.`;
  if (!p) {
    return `${styleText(["yellow", "bold"], "Not logged in.")} Run \`${BIN} auth login\` to get started.`;
  }
  return `${styleText("green", "●")} Logged in as ${styleText("bold", p.name)}`;
}
