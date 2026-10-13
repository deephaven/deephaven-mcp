import { Option } from "commander";
import {
  type Removed,
  removeProfiles,
  trustCasToRevoke,
} from "../../auth/flows.ts";
import { sortedProfiles } from "../../auth/profile.ts";
import { type Context, defineCommand, type Output } from "../../cli/command.ts";
import { note } from "../../output.ts";
import { BIN } from "../../version.ts";
import { askProfilesToRemove } from "./questions.ts";

/** `dh auth logout`'s flags. */
export interface LogoutOptions {
  all?: boolean;
}

export const logout = defineCommand<LogoutOptions>({
  name: "logout",
  description: "Revoke this computer's key and remove a profile.",
  args: [{ name: "[profile]" }],
  options: [new Option("--all", "Remove every profile.")],

  async run(ctx, [name], opts) {
    const state = await ctx.store.read();
    if (sortedProfiles(state.config).length === 0) {
      return { human: "No profiles.", json: { removed: [], default: null } };
    }
    const chosen = await askProfilesToRemove(
      ctx.ui,
      state.config,
      name ?? ctx.profile,
      opts.all,
    );
    const nameOf = (id: string) => state.config.profiles[id]?.name ?? id;

    const { ids, skipped } = await trustCasToRevoke(
      state,
      chosen,
      logoutCommand(ctx, chosen.length === 1 ? chosen.map(nameOf) : undefined),
    );
    for (const id of skipped) {
      note(
        `! Kept ${nameOf(id)}: its server uses a different CA certificate. Run \`${BIN} auth logout ${nameOf(id)}\`.`,
      );
    }

    const done = await removeProfiles(ctx.store, state, ids);
    for (const warning of done.warnings) note(warning);
    for (const resaved of done.resaved) {
      note(
        `! Kept ${resaved}: it was signed in again during logout. Run \`${BIN} auth logout ${resaved}\` again.`,
      );
    }
    return removedOutput(done, skipped.map(nameOf));
  },
});

/**
 * This logout as explicit arguments, for a re-run with a CA. Explicit because
 * it may have started from the login menu; `names` undefined means `--all`.
 */
function logoutCommand(ctx: Context, names: string[] | undefined): string[] {
  return [
    "auth",
    "logout",
    ...(ctx.json ? ["-o", "json"] : []),
    ...(ctx.noPrompt ? ["--no-prompt"] : []),
    // `--`: names may start with `-`.
    ...(names ? ["--", ...names] : ["--all"]),
  ];
}

function removedOutput(done: Removed, skipped: string[]): Output {
  return {
    human: [
      ...done.removed.map((r) =>
        r.revoked
          ? `✔ Revoked key on ${new URL(r.server).host} and removed ${r.profile}`
          : `✔ Removed ${r.profile}`,
      ),
      done.default ? `Default profile: ${done.default}` : "No profiles left.",
    ],
    json: {
      removed: done.removed,
      kept: [...skipped, ...done.resaved],
      default: done.default ?? null,
    },
  };
}
