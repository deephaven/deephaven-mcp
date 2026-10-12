import { noProfile, setDefault } from "../../auth/flows.ts";
import { findByName, sortedProfiles } from "../../auth/profile.ts";
import type { Config } from "../../auth/store.ts";
import { defineCommand } from "../../cli/command.ts";
import { DhError } from "../../errors.ts";
import { BIN } from "../../version.ts";
import { askDefaultProfile } from "./questions.ts";

export const use = defineCommand({
  name: "use",
  description: "Set the default profile.",
  args: [
    {
      name: "[profile]",
      description:
        "Profile name, or :<user> for that user on the default's server.",
    },
  ],

  async run(ctx, [name]) {
    const { config } = await ctx.store.read();
    if (sortedProfiles(config).length === 0) {
      throw new DhError(
        "auth_required",
        "No profiles.",
        `Run \`${BIN} auth login\` to get started.`,
      );
    }
    const id = name
      ? profileIdFor(config, name)
      : await askDefaultProfile(ctx.ui, config);
    // Another process may have removed it since the read above.
    const chosen = await setDefault(ctx.store, id);
    if (!chosen) throw noProfile(name ?? id);
    return {
      human: `✔ Default profile is now ${chosen}`,
      json: { default: chosen },
    };
  },
});

/** A profile name, or `:<user>`: that user's profile on the default profile's server. */
function profileIdFor(config: Config, name: string): string {
  const found = findByName(config, name);
  if (found) return found[0];
  if (!name.startsWith(":")) throw noProfile(name);
  const user = name.slice(1);
  const current = config.defaultProfile
    ? config.profiles[config.defaultProfile]
    : undefined;
  if (!current) {
    throw new DhError(
      "usage",
      `No default profile to take the server from`,
      `Name the profile instead, e.g. \`${BIN} auth use <host>:${user}\`.`,
    );
  }
  const matches = sortedProfiles(config).filter(
    ([, p]) => p.server === current.server && p.user === user,
  );
  // Operate-as profiles share the user; the plain one wins.
  const plain = matches.find(([, p]) => !p.operateAs || p.operateAs === user);
  const match = plain ?? (matches.length === 1 ? matches[0] : undefined);
  if (match) return match[0];
  if (matches.length > 0) {
    throw new DhError(
      "usage",
      `Several profiles for ${user} on ${current.server}`,
      `Pick one by name: ${matches.map(([, p]) => p.name).join(", ")}`,
    );
  }
  throw new DhError(
    "usage",
    `No profile for ${user} on ${current.server}`,
    `Run \`${BIN} auth login ${current.server}\` and sign in as ${user}.`,
  );
}
