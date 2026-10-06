import { describe, DhError } from "../../auth/errors.ts";
import { revokeOwnKey } from "../../auth/flows.ts";
import {
  findByName,
  removeProfile,
  sortedProfiles,
} from "../../auth/profile.ts";
import { ensureCa } from "../../auth/session.ts";
import { Store } from "../../auth/store.ts";
import { note, result } from "../../output.ts";
import { getPrompter } from "../../ui/prompt.ts";
import { BIN } from "../../version.ts";
import { globals, profileName as selectedProfile } from "../common.ts";

export interface LogoutOptions {
  all?: boolean;
}

const ALL = "__all__";

export async function logout(
  options: LogoutOptions,
  profileName?: string,
): Promise<void> {
  const g = globals(options);
  const store = new Store();
  const { config, credentials } = await store.read();
  const profiles = sortedProfiles(config);
  if (profiles.length === 0) {
    result(() => "No profiles.", { removed: [], default: null });
    return;
  }

  let ids: string[];
  let extraArgs: string[] = [];
  const name = profileName ?? selectedProfile(g);
  if (options.all) {
    ids = profiles.map(([id]) => id);
  } else if (name) {
    const found = findByName(config, name);
    if (!found) {
      throw new DhError(
        "usage",
        `No profile named "${name}"`,
        `Run \`${BIN} auth\` to list profiles.`,
      );
    }
    ids = [found[0]];
  } else if (profiles.length === 1) {
    ids = [profiles[0][0]];
  } else {
    const prompt = await getPrompter(g.input !== false);
    const picked = await prompt.select(
      "Which profile do you want to remove?",
      [
        ...profiles.map(([id, p]) => ({ label: p.name, value: id })),
        { label: "All profiles", value: ALL },
      ],
      "<profile> or --all",
    );
    ids = picked === ALL ? profiles.map(([id]) => id) : [picked];
    extraArgs = picked === ALL ? ["--all"] : [config.profiles[picked].name];
  }

  // Revoking needs each server's CA, and a process can trust only one extra CA.
  const caOf = (id: string) => {
    const cred = credentials.credentials[id];
    return cred?.type === "keyPair" && cred.generated
      ? config.servers[config.profiles[id].server]?.caCert
      : undefined;
  };
  const cas = new Set(ids.map(caOf).filter((ca) => ca !== undefined));
  if (cas.size === 1) await ensureCa([...cas][0], extraArgs);
  const trusted = Deno.env.get("DENO_CERT");
  const kept = ids.filter((id) => {
    const ca = caOf(id);
    return ca !== undefined && ca !== trusted;
  });
  for (const id of kept) {
    const keptName = config.profiles[id].name;
    note(
      `! Kept ${keptName}: its server uses a different CA certificate. Run \`${BIN} auth logout ${keptName}\`.`,
    );
  }
  ids = ids.filter((id) => !kept.includes(id));

  const removed: { profile: string; server: string; revoked: boolean }[] = [];
  for (const id of ids) {
    const p = config.profiles[id];
    const cred = credentials.credentials[id];
    let revoked = false;
    if (cred) {
      try {
        revoked = await revokeOwnKey(
          p.server,
          p.user,
          p.operateAs,
          cred,
          store.cacheDir,
        );
      } catch (e) {
        note(
          `! Could not revoke the key on ${p.server}: ${
            describe(e)
          }. Removing the profile anyway.`,
        );
      }
    }
    removed.push({ profile: p.name, server: p.server, revoked });
  }

  const nextDefault = await store.update((state) => {
    let next = state.config.defaultProfile;
    for (const id of ids) {
      next = removeProfile(state.config, id);
      delete state.credentials.credentials[id];
    }
    return next ? state.config.profiles[next]?.name : undefined;
  });

  result(
    () => [
      ...removed.map((r) =>
        r.revoked
          ? `✔ Revoked key on ${
            new URL(r.server).host
          } and removed ${r.profile}`
          : `✔ Removed ${r.profile}`
      ),
      nextDefault ? `Default profile: ${nextDefault}` : "No profiles left.",
    ],
    {
      removed,
      kept: kept.map((id) => config.profiles[id].name),
      default: nextDefault ?? null,
    },
  );
}
