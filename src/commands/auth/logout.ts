import { DhError, describe } from "../../auth/errors.ts";
import { addOrphan, revokeOwnKey } from "../../auth/flows.ts";
import {
  findByName,
  removeProfile,
  sortedProfiles,
} from "../../auth/profile.ts";
import { ensureCa, loadedCa } from "../../auth/session.ts";
import { Store } from "../../auth/store.ts";
import { note, result } from "../../output.ts";
import { getPrompter } from "../../ui/prompt.ts";
import { BIN } from "../../version.ts";
import {
  globals,
  promptsAllowed,
  profileName as selectedProfile,
} from "../common.ts";

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
  const nameOf = (id: string) => config.profiles[id]?.name ?? id;
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
  } else if (profiles.length === 1 && profiles[0]) {
    ids = [profiles[0][0]];
  } else {
    const prompt = await getPrompter(promptsAllowed(g));
    const picked = await prompt.select(
      "Which profile do you want to remove?",
      [
        ...profiles.map(([id, p]) => ({ label: p.name, value: id })),
        { label: "All profiles", value: ALL },
      ],
      "<profile> or --all",
    );
    ids = picked === ALL ? profiles.map(([id]) => id) : [picked];
    extraArgs = picked === ALL ? ["--all"] : [nameOf(picked)];
  }

  // Revoking needs each server's CA, and a process can trust only one extra CA.
  const caOf = (id: string) => {
    const cred = credentials.credentials[id];
    const server = config.profiles[id]?.server ?? "";
    return cred?.type === "keyPair" && cred.generated
      ? config.servers[server]?.caCert
      : undefined;
  };
  const cas = new Set(ids.map(caOf).filter((ca) => ca !== undefined));
  if (cas.size === 1) await ensureCa([...cas][0], extraArgs);
  const trusted = loadedCa();
  const kept = ids.filter((id) => {
    const ca = caOf(id);
    return ca !== undefined && ca !== trusted;
  });
  for (const id of kept) {
    const keptName = nameOf(id);
    note(
      `! Kept ${keptName}: its server uses a different CA certificate. Run \`${BIN} auth logout ${keptName}\`.`,
    );
  }
  ids = ids.filter((id) => !kept.includes(id));

  const removed: { profile: string; server: string; revoked: boolean }[] = [];
  const unrevoked: string[] = [];
  for (const id of ids) {
    const p = config.profiles[id];
    if (!p) continue;
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
        unrevoked.push(id);
        note(
          `! Could not delete the key on ${p.server}: ${describe(
            e,
          )}. Removing the profile anyway; the key is deleted the next time you sign in there as ${p.user}.`,
        );
      }
    }
    removed.push({ profile: p.name, server: p.server, revoked });
  }

  // Revoking can be slow; a profile signed in again meanwhile has a new key to keep.
  const resaved: string[] = [];
  const nextDefault = await store.update((state) => {
    let next = state.config.defaultProfile;
    for (const id of ids) {
      const now = state.credentials.credentials[id];
      if (JSON.stringify(now) !== JSON.stringify(credentials.credentials[id])) {
        resaved.push(id);
        continue;
      }
      next = removeProfile(state.config, id);
      delete state.credentials.credentials[id];
      const p = config.profiles[id];
      if (p && unrevoked.includes(id)) {
        addOrphan(state.credentials, p.server, p.user, now);
      }
    }
    return next ? state.config.profiles[next]?.name : undefined;
  });
  for (const id of resaved) {
    const name = nameOf(id);
    note(
      `! Kept ${name}: it was signed in again during logout. Run \`${BIN} auth logout ${name}\` again.`,
    );
  }
  const gone = removed.filter(
    (r) => !resaved.some((id) => nameOf(id) === r.profile),
  );

  result(
    () => [
      ...gone.map((r) =>
        r.revoked
          ? `✔ Revoked key on ${
              new URL(r.server).host
            } and removed ${r.profile}`
          : `✔ Removed ${r.profile}`,
      ),
      nextDefault ? `Default profile: ${nextDefault}` : "No profiles left.",
    ],
    {
      removed: gone,
      kept: [...kept, ...resaved].map(nameOf),
      default: nextDefault ?? null,
    },
  );
}
