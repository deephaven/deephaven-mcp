import type { dh } from "@deephaven/jsapi-types";
import {
  DhError,
  reasonOf,
  serverUnreachable as unreachable,
  withTimeout,
} from "../../errors.ts";
import { type CoreApi, loadCommunityApi } from "./jsapi.ts";
import type { Method } from "./store.ts";

export const ANONYMOUS_HANDLER =
  "io.deephaven.auth.AnonymousAuthenticationHandler";
export const PSK_HANDLER =
  "io.deephaven.authentication.psk.PskAuthenticationHandler";

/** A connected, not yet signed-in Community client. */
export interface CommunityConnection {
  origin: string;
  /** The server's jsapi module. */
  dh: CoreApi;
  /** Its `CoreClient`. */
  client: dh.CoreClient;
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
  // CoreClient can reject with an empty disconnect event before requestfailed
  // supplies the reason on its underlying connection in the same event turn.
  const events = await client.getAsIdeConnection();
  let failureReason: string | undefined;
  const onFailure = (event: { detail?: unknown }) => {
    if (event.detail != null) failureReason ??= reasonOf(event.detail);
  };
  const removeListener = events.addEventListener("requestfailed", onFailure);
  try {
    await withTimeout(
      client.login(loginCredentials(conn.dh, login)),
      timeout,
      () => unreachable(origin),
    );
  } catch (e) {
    const detail = failureReason ?? communityLoginReason(e);
    disconnect(client);
    if (e instanceof DhError) throw e;
    throw new DhError(
      rejected,
      `${origin} rejected the credentials`,
      undefined,
      detail,
    );
  } finally {
    removeListener();
  }
}

function communityLoginReason(reason: unknown): string | undefined {
  if (typeof reason === "object" && reason !== null && "type" in reason) {
    if (reason.type === "disconnect") {
      if ("detail" in reason && reason.detail != null) {
        const detail = reasonOf(reason.detail);
        if (detail) return detail;
      }
      return "Connection closed during sign-in; the client supplied no failure reason.";
    }
  }
  return reasonOf(reason);
}

/** `login` in the shape the jsapi's `CoreClient.login` takes; `dh` is the server's jsapi module. */
function loginCredentials(dh: CoreApi, login: CommunityLogin): object {
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
export function disconnect(client: dh.CoreClient): void {
  try {
    client.disconnect();
  } catch {
    // Already unusable.
  }
}
