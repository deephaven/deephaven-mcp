import type { Profile } from "../auth/store.ts";
import { defineCommand } from "../cli/command.ts";
import { BIN, VERSION } from "../version.ts";
import { agents } from "./agents.ts";
import { auth } from "./auth/index.ts";

/** `dh` alone: help and who's logged in. Every command is listed under `commands`. */
export const rootCommand = defineCommand({
  name: BIN,
  description: "Deephaven command line interface.",
  commands: [auth, agents],

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
        loginLine(readable, p),
        `For agents: \`${BIN} agents\` summarizes commands and error codes (\`-o json\` for everything).`,
      ],
      json: {
        version: VERSION,
        default: p ? { profile: p.name, server: p.server, user: p.user } : null,
      },
    };
  },
});

/** Who's logged in by default, or what to run next. `readable`: whether the config could be read. */
function loginLine(readable: boolean, p: Profile | undefined): string {
  if (!readable) return `Run \`${BIN} auth\` to check your profiles.`;
  if (!p) return `Not logged in. Run \`${BIN} auth login\` to get started.`;
  return `Logged in as ${p.user} @ ${new URL(p.server).host} (default)`;
}
