import {
  connectEnterprise,
  createKey,
  deleteKey,
  disconnect as disconnectEnterprise,
  loginKey,
} from "./enterprise.ts";
import { disconnect as disconnectCommunity } from "./community.ts";
import { DhError, timeoutMs } from "./errors.ts";
import type { KeyPair } from "./keyfile.ts";
import { defaultName, findByIdentity, newProfileId } from "./profile.ts";
import type { Session } from "./session.ts";
import type { Credential, Kind, Method, Store } from "./store.ts";

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
    const id = existing?.[0] ?? newProfileId();
    const name = existing?.[1].name ??
      defaultName(config, p.origin, p.user, p.operateAs);
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
      createdAt: existing?.[1].createdAt ?? new Date().toISOString(),
    };
    credentials.credentials[id] = p.credential;
    const hasDefault = config.defaultProfile !== undefined &&
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
  session: Session,
  cacheRoot: string,
): Promise<{ credential: Credential & { type: "keyPair" }; comment: string }> {
  const conn = session.enterprise!;
  const key = await createKey(conn, session.user);
  const timeout = timeoutMs();
  const check = await connectEnterprise(session.origin, cacheRoot, timeout);
  try {
    await loginKey(
      check,
      session.user,
      key,
      session.operateAs,
      timeout,
      "auth_failed",
    );
  } catch (e) {
    await deleteKey(conn, session.user, key).catch(() => {});
    throw new DhError(
      "key_upload_failed",
      `${session.origin} accepted the key upload but not a login with it`,
      e instanceof Error ? e.message : undefined,
    );
  } finally {
    disconnectEnterprise(check);
  }
  return {
    credential: {
      type: "keyPair",
      keyType: key.type,
      publicKey: key.publicKey,
      privateKey: key.privateKey,
      generated: true,
    },
    comment: key.comment,
  };
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
