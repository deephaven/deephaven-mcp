/**
 * Migrating legacy dhcli / Deephaven MCP entries: each becomes a sign-in
 * recipe, then goes through the same connect → signIn → commitSignIn as login.
 */
import { BIN } from "../version.ts";
import { type CommunityLogin, communityUser } from "./community.ts";
import { DhError, describe } from "./errors.ts";
import {
  type Committed,
  commitSignIn,
  saveProfile,
  type ToSave,
} from "./flows.ts";
import { assertTrusted } from "./jsapi.ts";
import { parseKeyFile, readKeyFile } from "./keyfile.ts";
import {
  describePlan,
  type LegacyConfig,
  type LegacyItem,
  resolveSecret,
  type SecretRef,
} from "./legacy.ts";
import { loadedCa } from "./session.ts";
import { connect, disconnect, type SignIn, signIn } from "./signin.ts";
import type { Config, Credential, Store } from "./store.ts";

export interface Migrated {
  item: LegacyItem;
  profile?: string;
  detail: string;
  ok: boolean;
  warnings: string[];
}

/** Which config file and entry a profile came from; `#` separates the two. */
export function importedFrom(path: string, item: LegacyItem): string {
  return `dhcli:${path}#${item.source}`;
}

/** Entries an earlier migration already saved, and the rest. */
export function splitMigrated(
  legacy: LegacyConfig,
  config: Config,
): { done: LegacyItem[]; pending: LegacyItem[] } {
  const done = legacy.items.filter((i) =>
    Object.values(config.profiles).some(
      (p) =>
        p.server === i.origin &&
        [p.importedFrom ?? []].flat().includes(importedFrom(legacy.path, i)),
    ),
  );
  return { done, pending: legacy.items.filter((i) => !done.includes(i)) };
}

interface Recipe {
  how: SignIn;
  save: ToSave;
  /** False when the secret's variable isn't set: saved without signing in. */
  verify: boolean;
  /** What signing in waits on, for the progress line. */
  waiting: string;
  detail: string;
}

function secretCredential(ref: SecretRef, value: string): Credential {
  return "env" in ref
    ? { type: "envRef", env: ref.env }
    : { type: "secret", value };
}

/** How to sign in as `item`, and what to save; reads secrets, not the network. */
async function recipeFor(item: LegacyItem): Promise<Recipe> {
  const host = new URL(item.origin).host;
  const plan = item.plan;

  if (plan.method === "password") {
    const password = await resolveSecret(plan.password);
    if (!password) {
      throw new DhError(
        "credential_unavailable",
        "env" in plan.password
          ? `needs $${plan.password.env} set`
          : "no saved password",
      );
    }
    return {
      how: {
        method: "password",
        username: plan.username,
        password,
        operateAs: plan.operateAs,
      },
      save: { method: "password", credential: "new-key" },
      verify: true,
      waiting: `signing in to ${host} as ${plan.username}, then authorizing this computer`,
      detail: "authorized this computer",
    };
  }

  if (plan.method === "private-key") {
    const key = plan.keyPath
      ? await readKeyFile(plan.keyPath)
      : parseKeyFile(plan.keyText ?? "", item.source);
    if (!key.user) throw new DhError("usage", "key file has no user line");
    return {
      how: {
        method: "private-key",
        username: key.user,
        keyPair: key.keyPair,
        operateAs: key.operateAs,
      },
      save: {
        method: "private-key",
        credential: plan.keyPath
          ? { type: "keyFile", path: plan.keyPath }
          : {
              type: "keyPair",
              keyType: key.keyPair.type,
              publicKey: key.keyPair.publicKey,
              privateKey: key.keyPair.privateKey,
              generated: false,
            },
      },
      verify: true,
      waiting: `signing in to ${host} as ${key.user} with the key`,
      detail: plan.keyPath ? `uses ${plan.keyPath}` : "key copied",
    };
  }

  // Community.
  let username: string | undefined;
  let secret: string | undefined;
  let credential: Credential = { type: "none" };
  let ref: SecretRef | undefined;
  if (plan.method !== "anonymous") {
    ref = plan.secret;
    secret = await resolveSecret(ref);
  }
  if (plan.method === "basic") {
    username = plan.username;
    if (!username) {
      // v1 Basic stores "user:password" in one token.
      const at = secret?.indexOf(":") ?? -1;
      if (!secret || at < 0) {
        throw new DhError(
          "credential_unavailable",
          "needs a user:password token",
        );
      }
      username = secret.slice(0, at);
      secret = secret.slice(at + 1);
      // An env var keeps holding user:password; the password is split off at use.
      if (!("env" in ref!)) ref = { literal: secret };
    }
  }
  if (ref) {
    if (!secret && !("env" in ref)) {
      throw new DhError("credential_unavailable", "no saved secret");
    }
    credential = secretCredential(ref, secret ?? "");
    if (
      credential.type === "envRef" &&
      plan.method === "basic" &&
      !plan.username
    ) {
      credential.basicToken = true;
    }
  }
  const handler = plan.method === "custom" ? plan.handler : undefined;
  const verify = secret !== undefined || plan.method === "anonymous";
  return {
    how: { method: plan.method, handler, username, secret },
    save: { method: plan.method, credential, handler },
    verify,
    waiting: `signing in to ${host} (${describePlan(item)})`,
    detail: verify
      ? describePlan(item)
      : `${describePlan(item)} (not verified; variable not set)`,
  };
}

/** Signs in as `item` and saves it. Never throws: failures are outcomes. */
export async function migrateItem(
  store: Store,
  legacyPath: string,
  item: LegacyItem,
  waiting: (what: string) => void,
): Promise<Migrated> {
  try {
    if (item.caCert && loadedCa() !== item.caCert) {
      throw new DhError(
        "usage",
        `uses a custom CA; run \`${BIN} auth login ${item.origin} --ca-cert ${item.caCert}\``,
      );
    }
    const recipe = await recipeFor(item);
    const save = {
      ...recipe.save,
      caCert: item.caCert,
      importedFrom: importedFrom(legacyPath, item),
    };
    let saved: Pick<Committed, "name" | "warnings">;
    if (recipe.verify) {
      waiting(recipe.waiting);
      saved = await signInAndSave(store, item, recipe.how, save);
    } else {
      // Not signing in now, but later use would still refuse an untrusted origin.
      assertTrusted(new URL(item.origin));
      const credential = save.credential as Credential;
      saved = {
        ...(await saveProfile(store, {
          ...save,
          origin: item.origin,
          kind: item.kind,
          user: communityUser(recipe.how as CommunityLogin),
          credential,
        })),
        warnings: [],
      };
    }
    return {
      item,
      profile: saved.name,
      detail: recipe.detail,
      ok: true,
      warnings: saved.warnings,
    };
  } catch (e) {
    return { item, detail: reason(item, e), ok: false, warnings: [] };
  }
}

async function signInAndSave(
  store: Store,
  item: LegacyItem,
  how: SignIn,
  save: ToSave,
): Promise<Committed> {
  const server = await connect(item.origin, item.kind, store.cacheDir);
  try {
    const session = await signIn(server, how);
    try {
      return await commitSignIn(store, session, save);
    } finally {
      disconnect(session);
    }
  } finally {
    disconnect(server);
  }
}

function reason(item: LegacyItem, e: unknown): string {
  if (e instanceof DhError && e.code === "server_unreachable") {
    return `can't reach ${new URL(item.origin).host} (${e.hint ?? e.message})`;
  }
  return e instanceof DhError && e.hint
    ? `${e.message} (${e.hint})`
    : describe(e);
}

/**
 * Remembers the migration so login stops offering it (unless every entry
 * failed), and makes the legacy default the default.
 */
export async function recordMigration(
  store: Store,
  legacy: LegacyConfig,
  chosen: LegacyItem[],
  outcomes: Migrated[],
): Promise<void> {
  const anyOk = outcomes.some((o) => o.ok);
  await store.update(({ config }) => {
    if (chosen.length === 0) config.legacyImport = "declined";
    else if (anyOk) config.legacyImport = "done";
    const legacyDefault = outcomes.find(
      (o) =>
        o.ok &&
        o.item.name === legacy.defaultItem?.name &&
        o.item.kind === legacy.defaultItem.kind,
    );
    if (legacyDefault) {
      const id = Object.entries(config.profiles).find(
        ([, p]) => p.name === legacyDefault.profile,
      )?.[0];
      if (id) config.defaultProfile = id;
    }
  });
}
