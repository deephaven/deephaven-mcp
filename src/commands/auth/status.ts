import { requireAuth } from "../../actions/auth/session.ts";
import { disconnectServer } from "../../actions/auth/signin.ts";
import { readCachedPart } from "../../actions/plugins/cache.ts";
import {
  activeProfile,
  hostsDefaultFirst,
  serverSsh,
} from "../../actions/plugins/settings.ts";
import { type Context, defineCommand, type Output } from "../../cli/command.ts";

export const status = defineCommand({
  name: "status",
  description: "Check the profile against its server.",

  async run(ctx) {
    const sshOnly = await sshOnlyStatus(ctx);
    if (sshOnly) return sshOnly;
    const session = await requireAuth(
      ctx.profileFlag,
      ctx.store,
      ctx.lifecycle,
    );
    disconnectServer(session);
    const operating =
      session.operateAs && session.operateAs !== session.user
        ? [`  Operating as: ${session.operateAs}`]
        : [];
    const profile = session.profileName
      ? [`  Profile: ${session.profileName}`]
      : [];
    return {
      human: [
        `✔ Logged in to ${session.origin} as ${session.user}`,
        ...operating,
        ...profile,
      ],
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

/**
 * An SSH-only active profile's status, without connecting: its server, SSH
 * hosts (default first) and the cluster version from the cache. Undefined
 * for any other profile, or `DH_SERVER` alone.
 */
async function sshOnlyStatus(ctx: Context): Promise<Output | undefined> {
  if (!ctx.profile && process.env.DH_SERVER) return undefined;
  const config = (await ctx.store.read()).config;
  const active = activeProfile(config, ctx.profileFlag);
  if (active?.profile.method !== "ssh") return undefined;
  const { name, server } = active.profile;
  const hosts = hostsDefaultFirst(serverSsh(config, server));
  const version =
    (await readCachedPart(server, "shell"))?.state.clusterVersion ?? null;
  return {
    human: [
      `✔ SSH access to ${server} (no sign-in)`,
      `  Profile: ${name}`,
      `  SSH hosts: ${hosts.join(", ")}`,
      ...(version ? [`  Cluster: ${version}`] : []),
    ],
    json: {
      profile: name,
      server,
      kind: config.servers[server]?.kind ?? "enterprise",
      user: null,
      method: "ssh",
      sshHosts: hosts,
      clusterVersion: version,
    },
  };
}
