import { requireAuth } from "../../auth/session.ts";
import { disconnect } from "../../auth/signin.ts";
import { defineCommand } from "../../cli/command.ts";

export const status = defineCommand({
  name: "status",
  description: "Check the profile against its server.",

  async run(ctx) {
    const session = await requireAuth(ctx.profileFlag, ctx.store);
    disconnect(session);
    const operating =
      session.operateAs && session.operateAs !== session.user
        ? ` (operating as ${session.operateAs})`
        : "";
    const profile = session.profileName
      ? ` · profile ${session.profileName}`
      : "";
    return {
      human: `✔ Logged in to ${session.origin} as ${session.user}${operating}${profile}`,
      json: {
        profile: session.profileName ?? null,
        server: session.origin,
        kind: session.kind,
        user: session.user,
        operateAs: session.operateAs ?? null,
      },
    };
  },
});
