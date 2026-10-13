import { DhError, describe, reasonOf, timeoutMs } from "../errors.ts";
import { BIN } from "../version.ts";
import {
  connectEnterprise,
  createKey,
  deleteKey,
  disconnect as disconnectEnterprise,
  type EnterpriseConnection,
  loginKey,
} from "./enterprise.ts";
import type { KeyPair } from "./keyfile.ts";
import {
  defaultName,
  findByIdentity,
  findByName,
  newProfileId,
  removeProfile,
} from "./profile.ts";
import { ensureCa, loadedCa } from "./session.ts";
import type { Session } from "./signin.ts";
import type {
  Credential,
  Credentials,
  Kind,
  Method,
  State,
  Store,
} from "./store.ts";

/** Everything {@link saveProfile} needs; the profile's id and name are chosen there. */
export interface NewProfile {
  origin: string;
  kind: Kind;
  /** CA certificate to remember for the server. */
  caCert?: string;
  /** Legacy config entry, when migrating. */
  importedFrom?: string;
  user: string;
  operateAs?: string;
  method: Method;
  /** Community custom auth handler class. */
  handler?: string;
  credential: Credential;
}

/** The profile a save wrote. */
export interface Saved {
  id: string;
  name: string;
  /** Whether it's the default after the save. */
  isDefault: boolean;
  /** Credential this save replaced (re-login of the same identity). */
  previous?: Credential;
}

/** How to save a signed-in session; `"new-key"` authorizes this computer with a new Enterprise key. */
export interface ProfileToSave {
  method: Method;
  credential: Credential | "new-key";
  handler?: string;
  caCert?: string;
  importedFrom?: string;
}

/** The profile {@link commitSignIn} saved, and what it couldn't clean up. */
export interface SavedSignIn extends Saved {
  /** The new key's comment, if one was made. */
  key?: string;
  /** Old keys that couldn't be deleted (they're retried later). */
  warnings: string[];
}

/**
 * Saves `session` as a profile: authorizes this computer if asked, saves,
 * then deletes keys the save made obsolete. A failed save deletes the new key.
 * `makeDefault` undefined → only if there's no default yet.
 */
export async function commitSignIn(
  store: Store,
  session: Session,
  save: ProfileToSave,
  makeDefault?: boolean,
): Promise<SavedSignIn> {
  let credential: Credential;
  let key: string | undefined;
  if (save.credential === "new-key") {
    const made = await authorizeComputer(store, session);
    credential = made.credential;
    key = made.comment;
  } else {
    credential = save.credential;
  }

  let saved: Saved;
  try {
    saved = await saveProfile(
      store,
      {
        origin: session.origin,
        kind: session.kind,
        user: session.user,
        operateAs: session.operateAs,
        method: save.method,
        handler: save.handler,
        caCert: save.caCert,
        importedFrom: save.importedFrom,
        credential,
      },
      makeDefault,
    );
  } catch (e) {
    // Nothing references a new key until it's saved.
    if (key) await revokeWith(session, credential).catch(() => {});
    throw e;
  }

  const warnings: string[] = [];
  const orphans = await revokeOrphans(store, session).catch((e) => ({
    deleted: 0,
    kept: 1,
    failed: reasonOf(e),
  }));
  const orphanLine = orphansNote(orphans);
  if (orphanLine) warnings.push(orphanLine);
  try {
    await revokeWith(session, saved.previous);
  } catch (e) {
    await store.update(({ credentials }) =>
      addOrphan(credentials, session.origin, session.user, saved.previous),
    );
    warnings.push(
      `! Could not delete the previous key: ${describe(e)}. It's retried at your next sign-in.`,
    );
  }
  return { ...saved, key, warnings };
}

/**
 * Revoking a key needs its server's CA, and a process trusts only one extra
 * CA, so this re-runs as `rerun` if the profiles `ids` share one. Returns the
 * profiles this run can revoke, and those it can't.
 */
export async function trustCasToRevoke(
  state: State,
  ids: string[],
  rerun: string[],
): Promise<{ ids: string[]; skipped: string[] }> {
  const { config, credentials } = state;
  const caOf = (id: string) => {
    const cred = credentials.credentials[id];
    const server = config.profiles[id]?.server ?? "";
    return cred?.type === "keyPair" && cred.generated
      ? config.servers[server]?.caCert
      : undefined;
  };
  const cas = new Set(ids.map(caOf).filter((ca) => ca !== undefined));
  if (cas.size === 1) await ensureCa([...cas][0], [], rerun);
  const trusted = loadedCa();
  const skipped = ids.filter((id) => {
    const ca = caOf(id);
    return ca !== undefined && ca !== trusted;
  });
  return { ids: ids.filter((id) => !skipped.includes(id)), skipped };
}

/** What {@link removeProfiles} did. */
export interface Removed {
  /** `revoked`: its key was deleted on the server. */
  removed: { profile: string; server: string; revoked: boolean }[];
  /** Names of profiles signed in again while this ran, so kept. */
  resaved: string[];
  /** The default profile's name afterward. */
  default?: string;
  warnings: string[];
}

/**
 * Deletes each profile's generated key on its server, then the profiles. A key
 * that can't be deleted is remembered and retried at the next sign-in there.
 * `state` is what was read before; a profile signed in again since is kept.
 */
export async function removeProfiles(
  store: Store,
  state: State,
  ids: string[],
): Promise<Removed> {
  const { config, credentials } = state;
  const removed: Removed["removed"] = [];
  const warnings: string[] = [];
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
        warnings.push(
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
  const next = await store.update((now) => {
    let next = now.config.defaultProfile;
    for (const id of ids) {
      const cred = now.credentials.credentials[id];
      if (
        JSON.stringify(cred) !== JSON.stringify(credentials.credentials[id])
      ) {
        resaved.push(config.profiles[id]?.name ?? id);
        continue;
      }
      next = removeProfile(now.config, id);
      delete now.credentials.credentials[id];
      const p = config.profiles[id];
      if (p && unrevoked.includes(id)) {
        addOrphan(now.credentials, p.server, p.user, cred);
      }
    }
    return next ? now.config.profiles[next]?.name : undefined;
  });
  return {
    removed: removed.filter((r) => !resaved.includes(r.profile)),
    resaved,
    default: next,
    warnings,
  };
}

/** Makes `id` the default. Undefined if another process removed it meanwhile. */
export async function setDefault(
  store: Store,
  id: string,
): Promise<string | undefined> {
  return await store.update(({ config }) => {
    const profile = config.profiles[id];
    if (!profile) return undefined;
    config.defaultProfile = id;
    return profile.name;
  });
}

/** Renames the profile `name` to `newName`; fails if it's missing or the new name is empty or taken. */
export async function renameProfile(
  store: Store,
  name: string,
  newName: string,
): Promise<void> {
  await store.update(({ config }) => {
    const found = findByName(config, name);
    if (!found) throw noProfile(name);
    if (!newName.trim()) throw new DhError("usage", "The new name is empty");
    if (findByName(config, newName)) {
      throw new DhError("usage", `A profile named "${newName}" already exists`);
    }
    found[1].name = newName;
  });
}

/** The `usage` error for a profile name that doesn't exist. */
export function noProfile(name: string): DhError {
  return new DhError(
    "usage",
    `No profile named "${name}"`,
    `Run \`${BIN} auth\` to list profiles.`,
  );
}

/**
 * Saves `p`, replacing the profile with the same identity if there is one.
 * `makeDefault` undefined → only if there's no default yet.
 */
export async function saveProfile(
  store: Store,
  p: NewProfile,
  makeDefault?: boolean,
): Promise<Saved> {
  return await store.update(({ config, credentials }) => {
    const existing = findByIdentity(config, p.origin, p.user, p.operateAs);
    const id =
      existing?.[0] ??
      newProfileId(
        (id) => id in config.profiles || id in credentials.credentials,
      );
    const name =
      existing?.[1].name ?? defaultName(config, p.origin, p.user, p.operateAs);
    // Several legacy entries can be the same identity; remember them all.
    const importedFrom = [
      ...new Set(
        [existing?.[1].importedFrom ?? [], p.importedFrom ?? []].flat(),
      ),
    ];
    const previous = credentials.credentials[id];
    const server = config.servers[p.origin];
    config.servers[p.origin] = {
      ...server,
      kind: p.kind,
      ...(p.caCert ? { caCert: p.caCert } : {}),
      ...(p.importedFrom && !server ? { importedFrom: p.importedFrom } : {}),
    };
    config.profiles[id] = {
      name,
      server: p.origin,
      user: p.user,
      method: p.method,
      operateAs: p.operateAs,
      handler: p.handler,
      ...(importedFrom.length ? { importedFrom } : {}),
      createdAt: existing?.[1].createdAt ?? new Date().toISOString(),
    };
    credentials.credentials[id] = p.credential;
    const hasDefault =
      config.defaultProfile !== undefined &&
      config.defaultProfile in config.profiles &&
      config.defaultProfile !== id;
    if (makeDefault === true || (makeDefault === undefined && !hasDefault)) {
      config.defaultProfile = id;
    }
    return {
      id,
      name,
      isDefault: config.defaultProfile === id,
      previous,
    };
  });
}

/**
 * Uploads a new key for the signed-in Enterprise user, and checks it logs in
 * on a new connection before returning it.
 */
export async function authorizeComputer(
  store: Store,
  session: Session,
): Promise<{ credential: Credential & { type: "keyPair" }; comment: string }> {
  const conn = session.enterprise;
  if (!conn)
    throw new DhError("internal", `${session.origin} is not Enterprise`);
  const key = await createKey(conn, session.user);
  const credential = {
    type: "keyPair" as const,
    keyType: key.type,
    publicKey: key.publicKey,
    privateKey: key.privateKey,
    generated: true,
  };
  const timeout = timeoutMs();
  let check: EnterpriseConnection | undefined;
  try {
    check = await connectEnterprise(session.origin, session.cacheRoot, timeout);
    await loginKey(
      check,
      session.user,
      key,
      session.operateAs,
      timeout,
      "auth_failed",
    );
  } catch (e) {
    await deleteKey(conn, session.user, key).catch(() =>
      store.update(({ credentials }) =>
        addOrphan(credentials, session.origin, session.user, credential),
      ),
    );
    if (!check) throw e;
    throw new DhError(
      "key_upload_failed",
      `${session.origin} accepted the key upload but not a login with it`,
      undefined,
      e instanceof DhError ? e.detail : reasonOf(e),
    );
  } finally {
    if (check) disconnectEnterprise(check);
  }
  return { credential, comment: key.comment };
}

/** Deletes an old generated key with an already signed-in session. */
export async function revokeWith(
  session: Session,
  previous: Credential | undefined,
): Promise<boolean> {
  if (previous?.type !== "keyPair" || !previous.generated) return false;
  if (!session.enterprise) return false;
  await deleteKey(session.enterprise, session.user, keyPairOf(previous));
  return true;
}

/** Signs in to `origin` with `cred` and deletes that key. False, doing nothing, unless dh generated it. */
export async function revokeOwnKey(
  origin: string,
  user: string,
  operateAs: string | undefined,
  cred: Credential,
  cacheRoot: string,
): Promise<boolean> {
  if (cred.type !== "keyPair" || !cred.generated) return false;
  const timeout = timeoutMs();
  const conn = await connectEnterprise(origin, cacheRoot, timeout);
  try {
    const keyPair = keyPairOf(cred);
    await loginKey(conn, user, keyPair, operateAs, timeout, "auth_expired");
    await deleteKey(conn, user, keyPair);
    return true;
  } finally {
    disconnectEnterprise(conn);
  }
}

/** Remembers `cred`, a generated key of `user` on `server` that couldn't be deleted, for {@link revokeOrphans}. */
export function addOrphan(
  credentials: Credentials,
  server: string,
  user: string,
  cred: Credential | undefined,
): void {
  if (cred?.type !== "keyPair" || !cred.generated) return;
  credentials.orphans ??= [];
  const orphans = credentials.orphans;
  if (orphans.some((o) => o.publicKey === cred.publicKey)) return;
  orphans.push({
    server,
    user,
    keyType: cred.keyType,
    publicKey: cred.publicKey,
  });
}

/** Deletes remembered keys of the signed-in Enterprise user; `failed` keeps the first reason. */
export async function revokeOrphans(
  store: Store,
  session: Session,
): Promise<{ deleted: number; kept: number; failed?: string }> {
  const conn = session.enterprise;
  if (!conn) return { deleted: 0, kept: 0 };
  const { credentials } = await store.read();
  const mine = (credentials.orphans ?? []).filter(
    (o) => o.server === session.origin && o.user === session.user,
  );
  const deleted = new Set<string>();
  let failed: string | undefined;
  for (const o of mine) {
    const key = { type: o.keyType, publicKey: o.publicKey, privateKey: "" };
    try {
      await deleteKey(conn, session.user, key);
      deleted.add(o.publicKey);
    } catch (e) {
      failed ??= reasonOf(e) ?? "no reason given";
    }
  }
  const kept = mine.length - deleted.size;
  if (deleted.size === 0) return { deleted: 0, kept, failed };
  await store.update(({ credentials }) => {
    const left = (credentials.orphans ?? []).filter(
      (o) => !deleted.has(o.publicKey),
    );
    if (left.length) credentials.orphans = left;
    else delete credentials.orphans;
  });
  return { deleted: deleted.size, kept, failed };
}

/** One line on what {@link revokeOrphans} did, or undefined if there was nothing to do. */
export function orphansNote(result: {
  deleted: number;
  kept: number;
  failed?: string;
}): string | undefined {
  const keys = (n: number) => `${n} old key${n === 1 ? "" : "s"}`;
  if (result.kept) {
    return `! Could not delete ${keys(result.kept)} an earlier logout left; retried next sign-in: ${result.failed}`;
  }
  if (result.deleted) {
    return `✔ Deleted ${keys(result.deleted)} that an earlier logout couldn't`;
  }
  return undefined;
}

/** The key pair stored in a credential, in the shape the Enterprise client takes. */
export function keyPairOf(cred: Credential & { type: "keyPair" }): KeyPair {
  return {
    type: cred.keyType,
    publicKey: cred.publicKey,
    privateKey: cred.privateKey,
  };
}
