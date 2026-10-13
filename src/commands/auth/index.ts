import { styleText } from "node:util";
import { sortedProfiles } from "../../auth/profile.ts";
import { defineCommand } from "../../cli/command.ts";
import { BIN } from "../../version.ts";
import { login } from "./login.ts";
import { logout } from "./logout.ts";
import { migrate } from "./migrate.ts";
import { rename } from "./rename.ts";
import { status } from "./status.ts";
import { use } from "./use.ts";

const KIND_LABEL = { enterprise: "Enterprise", community: "Community" };

/** `auth` alone lists the profiles. */
export const auth = defineCommand({
  name: "auth",
  description: "Sign in to Deephaven servers and manage profiles.",
  commands: [login, use, rename, logout, status, migrate],

  async run(ctx) {
    const { config } = await ctx.store.read();
    const rows = sortedProfiles(config).map(([id, p]) => ({
      name: p.name,
      server: p.server,
      kind: config.servers[p.server]?.kind ?? "community",
      user: p.user,
      method: p.method,
      default: id === config.defaultProfile,
    }));
    if (rows.length === 0) {
      return {
        human: `No profiles. Run \`${BIN} auth login\` to get started.`,
        json: rows,
      };
    }
    const width = Math.max(...rows.map((r) => r.name.length));
    return {
      human: rows.map((r) =>
        `${r.default ? styleText("green", "●") : " "} ${r.name.padEnd(width)}   ${KIND_LABEL[
          r.kind
        ].padEnd(10)}${r.default ? "   (default)" : ""}`.trimEnd(),
      ),
      json: rows,
    };
  },
});
