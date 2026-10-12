import { DhError } from "../../auth/errors.ts";
import { closeSession } from "../../auth/flows.ts";
import { findByName, sortedProfiles } from "../../auth/profile.ts";
import { requireAuth } from "../../auth/session.ts";
import { type Config, Store } from "../../auth/store.ts";
import { result } from "../../output.ts";
import { getPrompter } from "../../ui/prompt.ts";
import { BIN } from "../../version.ts";
import { globals, promptsAllowed } from "../common.ts";

const KIND_LABEL = { enterprise: "Enterprise", community: "Community" };

export async function list(): Promise<void> {
  const { config } = await new Store().read();
  const rows = sortedProfiles(config).map(([id, p]) => ({
    name: p.name,
    server: p.server,
    kind: config.servers[p.server]?.kind ?? "community",
    user: p.user,
    method: p.method,
    default: id === config.defaultProfile,
  }));
  result(() => {
    if (rows.length === 0) {
      return `No profiles. Run \`${BIN} auth login\` to get started.`;
    }
    const width = Math.max(...rows.map((r) => r.name.length));
    return rows.map(
      (r) =>
        `${r.default ? "●" : " "} ${r.name.padEnd(width)}   ${KIND_LABEL[
          r.kind
        ].padEnd(10)}${r.default ? "   (default)" : ""}`,
    );
  }, rows);
}

function noProfile(name: string): DhError {
  return new DhError(
    "usage",
    `No profile named "${name}"`,
    `Run \`${BIN} auth\` to list profiles.`,
  );
}

/** `:<user>`: that user's profile on the default profile's server. */
function findByUser(config: Config, user: string): string {
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
  const found = plain ?? (matches.length === 1 ? matches[0] : undefined);
  if (found) return found[0];
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

export async function use(options: unknown, name?: string): Promise<void> {
  const g = globals(options);
  const store = new Store();
  const { config } = await store.read();
  const profiles = sortedProfiles(config);
  if (profiles.length === 0) {
    throw new DhError(
      "auth_required",
      "No profiles.",
      `Run \`${BIN} auth login\` to get started.`,
    );
  }
  const id = name
    ? (findByName(config, name)?.[0] ??
      (name.startsWith(":") ? findByUser(config, name.slice(1)) : undefined))
    : await (async () => {
        const prompt = await getPrompter(promptsAllowed(g));
        return await prompt.select(
          "Which profile should be the default?",
          profiles.map(([id, p]) => ({
            label: p.name,
            value: id,
            hint: id === config.defaultProfile ? "(default)" : undefined,
          })),
          "<profile>",
        );
      })();
  if (!id) throw noProfile(name!);
  const chosen = await store.update((state) => {
    state.config.defaultProfile = id;
    return state.config.profiles[id]?.name;
  });
  result(() => `✔ Default profile is now ${chosen}`, { default: chosen });
}

export async function rename(name: string, newName: string): Promise<void> {
  const store = new Store();
  await store.update(({ config }) => {
    const found = findByName(config, name);
    if (!found) throw noProfile(name);
    if (!newName.trim()) throw new DhError("usage", "The new name is empty");
    if (findByName(config, newName)) {
      throw new DhError("usage", `A profile named "${newName}" already exists`);
    }
    found[1].name = newName;
  });
  result(() => `✔ Renamed ${name} to ${newName}`, { from: name, to: newName });
}

export async function status(options: unknown): Promise<void> {
  const g = globals(options);
  const session = await requireAuth(g.profile);
  try {
    result(
      () =>
        `✔ Logged in to ${session.origin} as ${session.user}${
          session.operateAs && session.operateAs !== session.user
            ? ` (operating as ${session.operateAs})`
            : ""
        }${session.profileName ? ` · profile ${session.profileName}` : ""}`,
      {
        profile: session.profileName ?? null,
        server: session.origin,
        kind: session.kind,
        user: session.user,
        operateAs: session.operateAs ?? null,
      },
    );
  } finally {
    closeSession(session);
  }
}
