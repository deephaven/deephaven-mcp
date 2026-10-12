import { disconnect as disconnectCommunity } from "./community.ts";
import {
  connectEnterprise,
  createKey,
  deleteKey,
  disconnect as disconnectEnterprise,
  type EnterpriseConnection,
  loginKey,
} from "./enterprise.ts";
import { DhError, reasonOf, timeoutMs } from "./errors.ts";
import type { KeyPair } from "./keyfile.ts";
import { defaultName, findByIdentity, newProfileId } from "./profile.ts";
import type { Session } from "./session.ts";
import type { Credential, Credentials, Kind, Method, Store } from "./store.ts";

export interface NewProfile {
  origin: string;
  kind: Kind;
  caCert?: string;
  importedFrom?: string;
  user: string;
  operateAs?: string;
  method: Method;
  handler?: string;
  credential: Credential;
}

export interface Saved {
  id: string;
  name: string;
  isDefault: boolean;
  /** Credential this save replaced (re-login of the same identity). */
  previous?: Credential;
}

/** `makeDefault` undefined → only if there's no default yet. */
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
  cacheRoot: string,
): Promise<{ credential: Credential & { type: "keyPair" }; comment: string }> {
  const conn = session.enterprise!;
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
    check = await connectEnterprise(session.origin, cacheRoot, timeout);
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

/** Logs in with a generated key and deletes it on the server. */
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

/** Remembers a generated key that couldn't be deleted, for {@link revokeOrphans}. */
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

export function keyPairOf(cred: Credential & { type: "keyPair" }): KeyPair {
  return {
    type: cred.keyType,
    publicKey: cred.publicKey,
    privateKey: cred.privateKey,
  };
}

export function closeSession(session: Session): void {
  if (session.enterprise) disconnectEnterprise(session.enterprise);
  if (session.community) disconnectCommunity(session.community.client);
}
