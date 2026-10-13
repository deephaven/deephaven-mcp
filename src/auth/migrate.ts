/**
 * Migrating legacy dhcli / Deephaven MCP entries: each becomes a sign-in request,
 * then goes through the same connectServer → signIn → commitSignIn as login.
 */

import { DhError, describe } from "../errors.ts";
import { BIN } from "../version.ts";
import { type CommunityLogin, communityUser } from "./community.ts";
import {
  commitSignIn,
  type ProfileToSave,
  type SavedSignIn,
  saveProfile,
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
import {
  connectServer,
  disconnectServer,
  type SignInRequest,
  signIn,
} from "./signin.ts";
import type { Config, Credential, Store } from "./store.ts";

/** The outcome for one legacy entry. */
export interface Migrated {
  item: LegacyItem;
  /** The profile's name, if it was saved. */
  profile?: string;
  /** What was done, or why it failed. */
  detail: string;
  ok: boolean;
  /** Old keys that couldn't be deleted. */
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

/** How to sign in as a legacy entry and save it, worked out without the network. */
interface LegacySignIn {
  request: SignInRequest;
  toSave: ProfileToSave;
  /** False when the secret's variable isn't set: saved without signing in. */
  verify: boolean;
  /** What signing in waits on, for the progress line. */
  waiting: string;
  /** What migrating it does, for the result line. */
  detail: string;
}

/** A secret from an env var is saved as the variable's name, otherwise as its value. */
function secretCredential(ref: SecretRef, value: string): Credential {
  return "env" in ref
    ? { type: "envRef", env: ref.env }
    : { type: "secret", value };
}

/** How to sign in as `item`, and what to save; reads secrets, not the network. */
async function legacySignIn(item: LegacyItem): Promise<LegacySignIn> {
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
      request: {
        method: "password",
        username: plan.username,
        password,
        operateAs: plan.operateAs,
      },
      toSave: { method: "password", credential: "new-key" },
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
      request: {
        method: "private-key",
        username: key.user,
        keyPair: key.keyPair,
        operateAs: key.operateAs,
      },
      toSave: {
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
      if (ref && !("env" in ref)) ref = { literal: secret };
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
    request: { method: plan.method, handler, username, secret },
    toSave: { method: plan.method, credential, handler },
    verify,
    waiting: `signing in to ${host} (${describePlan(item)})`,
    detail: verify
      ? describePlan(item)
      : `${describePlan(item)} (not verified; variable not set)`,
  };
}

/**
 * Signs in as `item` (from the config at `legacyPath`) and saves it. `waiting`
 * is told what each server call is about to wait on. Never throws: failures
 * are outcomes.
 */
export async function migrateLegacyEntry(
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
    const entry = await legacySignIn(item);
    const toSave = {
      ...entry.toSave,
      caCert: item.caCert,
      importedFrom: importedFrom(legacyPath, item),
    };
    let saved: Pick<SavedSignIn, "name" | "warnings">;
    if (entry.verify) {
      waiting(entry.waiting);
      saved = await signInAndSave(store, item, entry.request, toSave);
    } else {
      // Not signing in now, but later use would still refuse an untrusted origin.
      assertTrusted(new URL(item.origin));
      const credential = toSave.credential as Credential;
      saved = {
        ...(await saveProfile(store, {
          ...toSave,
          origin: item.origin,
          kind: item.kind,
          user: communityUser(entry.request as CommunityLogin),
          credential,
        })),
        warnings: [],
      };
    }
    return {
      item,
      profile: saved.name,
      detail: entry.detail,
      ok: true,
      warnings: saved.warnings,
    };
  } catch (e) {
    return { item, detail: migrationFailure(item, e), ok: false, warnings: [] };
  }
}

/** Login's actions for one entry: connect, sign in, save. */
async function signInAndSave(
  store: Store,
  item: LegacyItem,
  request: SignInRequest,
  toSave: ProfileToSave,
): Promise<SavedSignIn> {
  const server = await connectServer(item.origin, item.kind, store.cacheDir);
  try {
    const session = await signIn(server, request);
    try {
      return await commitSignIn(store, session, toSave);
    } finally {
      disconnectServer(session);
    }
  } finally {
    disconnectServer(server);
  }
}

/** One line on why `item` wasn't migrated. */
function migrationFailure(item: LegacyItem, e: unknown): string {
  if (e instanceof DhError && e.code === "server_unreachable") {
    return `can't reach ${new URL(item.origin).host} (${e.hint ?? e.message})`;
  }
  return e instanceof DhError && e.hint
    ? `${e.message} (${e.hint})`
    : describe(e);
}

/**
 * Records the result so login stops offering to migrate: `declined` if none were
 * `chosen`, `done` if any succeeded, unset if all failed. Makes the legacy
 * default the default.
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
