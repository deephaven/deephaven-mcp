import { describe, DhError, withTimeout } from "./errors.ts";
import { type Api, loadCommunityApi } from "./jsapi.ts";
import type { Method } from "./store.ts";

export const ANONYMOUS_HANDLER =
  "io.deephaven.auth.AnonymousAuthenticationHandler";
export const PSK_HANDLER =
  "io.deephaven.authentication.psk.PskAuthenticationHandler";

export interface CommunityConnection {
  origin: string;
  dh: Api;
  client: Api;
  handlers: string[];
}

export interface CommunityLogin {
  method: Method;
  handler?: string;
  username?: string;
  secret?: string;
}

function unreachable(origin: string, cause?: unknown): DhError {
  return new DhError(
    "server_unreachable",
    `Could not reach ${origin}`,
    cause === undefined ? "Timed out" : describe(cause),
  );
}

export async function connectCommunity(
  origin: string,
  cacheRoot: string,
  timeout: number,
): Promise<CommunityConnection> {
  const dh = await loadCommunityApi(new URL(origin), cacheRoot, timeout);
  const client = new dh.CoreClient(origin);
  let values: string[][];
  try {
    values = await withTimeout(
      client.getAuthConfigValues(),
      timeout,
      () => unreachable(origin),
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
export function methodFor(handler: string): Method {
  if (handler === ANONYMOUS_HANDLER) return "anonymous";
  if (handler === PSK_HANDLER) return "psk";
  if (/basic/i.test(handler)) return "basic";
  return "custom";
}

/** Login options the server offers, with anonymous last. */
export function communityMethods(
  handlers: string[],
): { method: Method; handler: string }[] {
  return handlers
    .map((handler) => ({ method: methodFor(handler), handler }))
    .sort((a, b) =>
      Number(a.method === "anonymous") - Number(b.method === "anonymous")
    );
}

export function communityUser(login: CommunityLogin): string {
  switch (login.method) {
    case "basic":
      return login.username!;
    case "custom":
      return login.handler!.split(".").pop()!;
    default:
      return login.method;
  }
}

export async function loginCommunity(
  conn: CommunityConnection,
  login: CommunityLogin,
  timeout: number,
  rejected: "auth_failed" | "auth_expired",
): Promise<void> {
  const { dh, client, origin } = conn;
  const credentials = login.method === "anonymous"
    ? { type: dh.CoreClient.LOGIN_TYPE_ANONYMOUS }
    : login.method === "psk"
    ? { type: PSK_HANDLER, token: login.secret }
    : login.method === "basic"
    ? {
      type: dh.CoreClient.LOGIN_TYPE_PASSWORD,
      username: login.username,
      token: login.secret,
    }
    : { type: login.handler, token: login.secret };
  try {
    await withTimeout(
      client.login(credentials),
      timeout,
      () => unreachable(origin),
    );
  } catch (e) {
    disconnect(client);
    if (e instanceof DhError) throw e;
    throw new DhError(rejected, `${origin} rejected the credentials`);
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
