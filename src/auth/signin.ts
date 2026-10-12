/**
 * Connecting and signing in: the steps every command shares, whatever the
 * server kind or method. Nothing here asks the user anything.
 */

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
import { DhError, timeoutMs } from "./errors.ts";
import type { KeyPair } from "./keyfile.ts";
import { openBrowser, samlNonce, signInUrl } from "./saml.ts";
import type { Kind, Method } from "./store.ts";

export type Saml = NonNullable<EnterpriseAuthConfig["saml"]>;

/** A connected server, not yet signed in. */
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
  operateAs?: string;
  profileName?: string;
}

/** A sign-in method the server advertises. */
export type Offer =
  | { method: "saml"; label: string; saml: Saml; handler?: undefined }
  | {
      method: Exclude<Method, "saml">;
      label: string;
      /** Community: the handler class. */
      handler?: string;
    };

/** Everything one sign-in needs; `method` says which server kind it's for. */
export type SignIn =
  | { method: "saml"; saml: Saml; waitMs: number; browser: boolean }
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
  | (CommunityLogin & { method: "psk" | "basic" | "custom" | "anonymous" });

export async function connect(
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
export async function offers(server: Server): Promise<Offer[]> {
  if (server.community) {
    return communityMethods(server.community.handlers).map((o) => ({
      method: o.method as Exclude<Method, "saml">,
      handler: o.handler,
      label: COMMUNITY_LABELS[o.method] ?? o.handler,
    }));
  }
  const auth = await enterpriseAuthConfig(enterpriseOf(server), timeoutMs());
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
 * Signs in to `server`. SSO opens the browser and waits; it may replace the
 * connection, so use the returned session's.
 */
export async function signIn(
  server: Server,
  how: SignIn,
  rejected: "auth_failed" | "auth_expired" = "auth_failed",
): Promise<Session> {
  const timeout = timeoutMs();
  if (server.community) {
    const login = how as CommunityLogin;
    await loginCommunity(server.community, login, timeout, rejected);
    return { ...server, user: communityUser(login) };
  }
  const conn = enterpriseOf(server);
  switch (how.method) {
    case "private-key":
      await loginKey(
        conn,
        how.username,
        how.keyPair,
        how.operateAs,
        timeout,
        rejected,
      );
      return { ...server, user: how.username, operateAs: how.operateAs };
    case "password":
      await loginPassword(
        conn,
        how.username,
        how.password,
        how.operateAs,
        timeout,
      );
      return { ...server, user: how.username, operateAs: how.operateAs };
    case "saml": {
      const signedIn = await waitForSaml(server, conn, how);
      return {
        ...server,
        enterprise: signedIn,
        user: await userName(signedIn),
      };
    }
    default:
      throw new DhError(
        "internal",
        `${how.method} is not an Enterprise method`,
      );
  }
}

async function waitForSaml(
  server: Server,
  conn: EnterpriseConnection,
  how: SignIn & { method: "saml" },
): Promise<EnterpriseConnection> {
  const nonce = samlNonce();
  const url = signInUrl(how.saml.confirmUrl ?? how.saml.loginUrl, nonce);
  if (how.browser) {
    note(
      `Opening your browser to sign in. If it doesn't open, go to:\n  ${url}`,
    );
    await openBrowser(url);
  } else {
    note(`Sign in at:\n  ${url}`);
  }
  note("Waiting for sign-in… (Ctrl-C to cancel)");
  return await loginSaml(
    conn,
    () => connectEnterprise(server.origin, server.cacheRoot, timeoutMs()),
    nonce,
    how.waitMs,
  );
}

function enterpriseOf(server: Server): EnterpriseConnection {
  if (!server.enterprise) {
    throw new DhError("internal", `${server.origin} is not Enterprise`);
  }
  return server.enterprise;
}

/** Safe to call more than once, and on a server whose sign-in failed. */
export function disconnect(server: Server): void {
  if (server.enterprise) disconnectEnterprise(server.enterprise);
  if (server.community) disconnectCommunity(server.community.client);
}
