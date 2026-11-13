import { styleText } from "node:util";
import { sortedProfiles } from "../../actions/auth/profile.ts";
import {
  hostsDefaultFirst,
  serverSsh,
} from "../../actions/plugins/settings.ts";
import { type CommandSpec, defineCommand } from "../../cli/command.ts";
import { BIN } from "../../version.ts";
import { loginCommand } from "./login.ts";
import { logout } from "./logout.ts";
import { migrate } from "./migrate.ts";
import { rename } from "./rename.ts";
import { status } from "./status.ts";
import { use } from "./use.ts";

const KIND_LABEL = { enterprise: "Enterprise", community: "Community" };

/** `auth` alone lists the profiles. `plugins`: the loaded plugins' specs, for login. */
export function authCommand(): CommandSpec<never> {
  return defineCommand({
    name: "auth",
    description: "Sign in to Deephaven servers and manage profiles.",
    commands: [loginCommand(), use, rename, logout, status, migrate],

    async run(ctx) {
      const { config } = await ctx.store.read();
      const rows = sortedProfiles(config).map(([id, p]) => ({
        name: p.name,
        server: p.server,
        kind: config.servers[p.server]?.kind ?? "community",
        user: p.user ?? null,
        method: p.method,
        default: id === config.defaultProfile,
        ...(p.method === "ssh"
          ? { sshHosts: hostsDefaultFirst(serverSsh(config, p.server)) }
          : {}),
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
          `${r.default ? styleText("green", "●") : " "} ${r.name.padEnd(width)}   ${(r.method === "ssh" ? "SSH only" : KIND_LABEL[r.kind]).padEnd(10)}${r.default ? "   (default)" : ""}`.trimEnd(),
        ),
        json: rows,
      };
    },
  });
}
