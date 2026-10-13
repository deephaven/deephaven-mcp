import { DhError, describe, reasonOf, withTimeout } from "../errors.ts";
import { type Api, loadCommunityApi } from "./jsapi.ts";
import type { Method } from "./store.ts";

export const ANONYMOUS_HANDLER =
  "io.deephaven.auth.AnonymousAuthenticationHandler";
export const PSK_HANDLER =
  "io.deephaven.authentication.psk.PskAuthenticationHandler";

/** A connected, not yet signed-in Community client. */
export interface CommunityConnection {
  origin: string;
  /** The server's jsapi module. */
  dh: Api;
  /** Its `CoreClient`. */
  client: Api;
  /** Auth handler classes the server offers. */
  handlers: string[];
}

/** The {@link Method}s a Community server can offer. */
export type CommunityMethod = Extract<
  Method,
  "anonymous" | "psk" | "basic" | "custom"
>;

/** One Community sign-in: the method, plus the handler class, user and secret it needs. */
export interface CommunityLogin {
  method: CommunityMethod;
  handler?: string;
  username?: string;
  secret?: string;
}

/** `cause`: why it failed; without one, the error says it timed out. */
function unreachable(origin: string, cause?: unknown): DhError {
  return new DhError(
    "server_unreachable",
    `Could not reach ${origin}`,
    cause === undefined ? "Timed out" : describe(cause),
  );
}

/**
 * Loads `origin`'s jsapi (cached under `cacheRoot`), connects, and reads which
 * auth handlers the server offers. Not signed in yet.
 */
export async function connectCommunity(
  origin: string,
  cacheRoot: string,
  timeout: number,
): Promise<CommunityConnection> {
  const dh = await loadCommunityApi(new URL(origin), cacheRoot, timeout);
  const client = new dh.CoreClient(origin);
  let values: [string, string][];
  try {
    values = await withTimeout(client.getAuthConfigValues(), timeout, () =>
      unreachable(origin),
    );
  } catch (e) {
    disconnect(client);
    throw e instanceof DhError ? e : unreachable(origin, e);
  }
  const handlers = values
    .filter(([key]) => key === "AuthHandlers")
    .flatMap(([, value]) => value.split(","))
    .map((h) => h.trim())
    .filter(Boolean);
  return { origin, dh, client, handlers };
}

/** Basic-auth handlers vary by server build; recognize them by name. */
export function methodFor(handler: string): CommunityMethod {
  if (handler === ANONYMOUS_HANDLER) return "anonymous";
  if (handler === PSK_HANDLER) return "psk";
  if (/basic/i.test(handler)) return "basic";
  return "custom";
}

/** Login options the server offers, with anonymous last. */
export function communityMethods(
  handlers: string[],
): { method: CommunityMethod; handler: string }[] {
  return handlers
    .map((handler) => ({ method: methodFor(handler), handler }))
    .sort(
      (a, b) =>
        Number(a.method === "anonymous") - Number(b.method === "anonymous"),
    );
}

/** The user name a profile records for `login`: the basic-auth user, the handler's class name, or the method. */
export function communityUser(login: CommunityLogin): string {
  switch (login.method) {
    case "basic":
      return login.username ?? login.method;
    case "custom":
      return login.handler?.split(".").pop() ?? login.method;
    default:
      return login.method;
  }
}

/**
 * Signs in on `conn`. A rejection throws `rejected` (`auth_failed` at login,
 * `auth_expired` for a saved profile); a timeout throws `server_unreachable`.
 * The connection is closed on failure.
 */
export async function loginCommunity(
  conn: CommunityConnection,
  login: CommunityLogin,
  timeout: number,
  rejected: "auth_failed" | "auth_expired",
): Promise<void> {
  const { client, origin } = conn;
  try {
    await withTimeout(
      client.login(loginCredentials(conn.dh, login)),
      timeout,
      () => unreachable(origin),
    );
  } catch (e) {
    disconnect(client);
    if (e instanceof DhError) throw e;
    throw new DhError(
      rejected,
      `${origin} rejected the credentials`,
      undefined,
      reasonOf(e),
    );
  }
}

/** `login` in the shape the jsapi's `CoreClient.login` takes; `dh` is the server's jsapi module. */
function loginCredentials(dh: Api, login: CommunityLogin): object {
  switch (login.method) {
    case "anonymous":
      return { type: dh.CoreClient.LOGIN_TYPE_ANONYMOUS };
    case "psk":
      return { type: PSK_HANDLER, token: login.secret };
    case "basic":
      return {
        type: dh.CoreClient.LOGIN_TYPE_PASSWORD,
        username: login.username,
        token: login.secret,
      };
    case "custom":
      return { type: login.handler, token: login.secret };
  }
}

/** The jsapi's `disconnect()` throws after a failed login. */
export function disconnect(client: Api): void {
  try {
    client.disconnect();
  } catch {
    // Already unusable.
  }
}
