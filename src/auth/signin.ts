/**
 * Connecting and signing in: the steps every command shares, whatever the
 * server kind or method. Nothing here asks the user anything.
 */

import { DhError, timeoutMs } from "../errors.ts";
import { note } from "../output.ts";
import {
  type CommunityConnection,
  type CommunityLogin,
  communityMethods,
  communityUser,
  connectCommunity,
  disconnect as disconnectCommunity,
  loginCommunity,
} from "./community.ts";
import {
  connectEnterprise,
  disconnect as disconnectEnterprise,
  type EnterpriseAuthConfig,
  type EnterpriseConnection,
  enterpriseAuthConfig,
  loginKey,
  loginPassword,
  loginSaml,
  userName,
} from "./enterprise.ts";
import type { KeyPair } from "./keyfile.ts";
import { openBrowser, samlNonce, signInUrl } from "./saml.ts";
import type { Kind, Method } from "./store.ts";

/** An Enterprise server's SSO settings. */
export type Saml = NonNullable<EnterpriseAuthConfig["saml"]>;

/** A connected server, not yet signed in. Exactly one of `enterprise` and `community` is set, matching `kind`. */
export interface Server {
  origin: string;
  kind: Kind;
  /** Where the server's client library is cached. */
  cacheRoot: string;
  enterprise?: EnterpriseConnection;
  community?: CommunityConnection;
}

/** A signed-in server. */
export interface Session extends Server {
  user: string;
  /** Enterprise user being acted as, if not `user`. */
  operateAs?: string;
  /** The saved profile it came from, if any. */
  profileName?: string;
}

/** A sign-in method the server advertises. */
export type OfferedMethod =
  | { method: "saml"; label: string; saml: Saml; handler?: undefined }
  | {
      method: Exclude<Method, "saml">;
      label: string;
      /** Community: the handler class. */
      handler?: string;
    };

/** Everything one sign-in needs; `method` says which server kind it's for. */
export type SignInRequest =
  | {
      method: "saml";
      saml: Saml;
      waitMs: number;
      browser: boolean;
      operateAs?: string;
    }
  | {
      method: "password";
      username: string;
      password: string;
      operateAs?: string;
    }
  | {
      method: "private-key";
      username: string;
      keyPair: KeyPair;
      operateAs?: string;
    }
  | CommunityLogin;

/** Loads the server's client code (cached under `cacheRoot`) and connects. Not signed in yet. */
export async function connectServer(
  origin: string,
  kind: Kind,
  cacheRoot: string,
): Promise<Server> {
  const timeout = timeoutMs();
  return kind === "enterprise"
    ? {
        origin,
        kind,
        cacheRoot,
        enterprise: await connectEnterprise(origin, cacheRoot, timeout),
      }
    : {
        origin,
        kind,
        cacheRoot,
        community: await connectCommunity(origin, cacheRoot, timeout),
      };
}

const COMMUNITY_LABELS: Partial<Record<Method, string>> = {
  anonymous: "Anonymous",
  psk: "Pre-shared key",
  basic: "Username and password",
};

/** What the server advertises. Enterprise key login always works but isn't advertised. */
export async function offeredMethods(server: Server): Promise<OfferedMethod[]> {
  if (server.community) {
    return communityMethods(server.community.handlers).map((o) => ({
      method: o.method,
      handler: o.handler,
      label: COMMUNITY_LABELS[o.method] ?? o.handler,
    }));
  }
  const auth = await enterpriseAuthConfig(
    enterpriseConnection(server),
    timeoutMs(),
  );
  return [
    ...(auth.saml
      ? [
          {
            method: "saml" as const,
            label: `${auth.saml.providerName} (SSO)`,
            saml: auth.saml,
          },
        ]
      : []),
    ...(auth.passwordsEnabled
      ? [{ method: "password" as const, label: "Username and password" }]
      : []),
  ];
}

/**
 * Signs in to `server` as `request` says. A rejection throws `rejected`
 * (`auth_failed` at login, `auth_expired` for a saved profile). SSO opens the
 * browser and waits; it may replace the connection, so use the returned
 * session's.
 */
export async function signIn(
  server: Server,
  request: SignInRequest,
  rejected: "auth_failed" | "auth_expired" = "auth_failed",
): Promise<Session> {
  const timeout = timeoutMs();
  if (server.community) {
    const login = request as CommunityLogin;
    await loginCommunity(server.community, login, timeout, rejected);
    return { ...server, user: communityUser(login) };
  }
  const conn = enterpriseConnection(server);
  switch (request.method) {
    case "private-key":
      await loginKey(
        conn,
        request.username,
        request.keyPair,
        request.operateAs,
        timeout,
        rejected,
      );
      return {
        ...server,
        user: request.username,
        operateAs: request.operateAs,
      };
    case "password":
      await loginPassword(
        conn,
        request.username,
        request.password,
        request.operateAs,
        timeout,
      );
      return {
        ...server,
        user: request.username,
        operateAs: request.operateAs,
      };
    case "saml": {
      const signedIn = await waitForSaml(server, conn, request);
      // SSO signs in as the user; operate-as applies to the key made next, whose login checks it.
      return {
        ...server,
        enterprise: signedIn,
        user: await userName(signedIn),
        operateAs: request.operateAs,
      };
    }
    default:
      throw new DhError(
        "internal",
        `${request.method} is not an Enterprise method`,
      );
  }
}

/** Opens the SSO page and waits for the browser sign-in; returns the signed-in connection. */
async function waitForSaml(
  server: Server,
  conn: EnterpriseConnection,
  request: SignInRequest & { method: "saml" },
): Promise<EnterpriseConnection> {
  const nonce = samlNonce();
  const url = signInUrl(
    request.saml.confirmUrl ?? request.saml.loginUrl,
    nonce,
  );
  if (request.browser) {
    note(
      `Opening your browser to sign in. If it doesn't open, go to:\n  ${url}`,
    );
    await openBrowser(url);
  } else {
    note(`Sign in at:\n  ${url}`);
  }
  note(
    `Waiting up to ${Math.round(request.waitMs / 1000)}s for you to sign in in the browser… (--timeout to change, Ctrl-C to cancel)`,
  );
  return await loginSaml(
    conn,
    () => connectEnterprise(server.origin, server.cacheRoot, timeoutMs()),
    nonce,
    request.waitMs,
  );
}

/** `server`'s Enterprise connection; an `internal` error for a Community server. */
function enterpriseConnection(server: Server): EnterpriseConnection {
  if (!server.enterprise) {
    throw new DhError("internal", `${server.origin} is not Enterprise`);
  }
  return server.enterprise;
}

/** Safe to call more than once, and on a server whose sign-in failed. */
export function disconnectServer(server: Server): void {
  if (server.enterprise) disconnectEnterprise(server.enterprise);
  if (server.community) disconnectCommunity(server.community.client);
}
