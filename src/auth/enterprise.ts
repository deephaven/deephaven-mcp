import { hostname } from "node:os";
import { NodeHttp2gRPCTransport } from "@deephaven/jsapi-nodejs";
import {
  createPasswordCredentials,
  deletePublicKeys,
  generateBase64KeyPair,
  loginClientWithKeyPair,
  uploadPublicKey,
} from "@deephaven-enterprise/auth-nodejs";
import {
  DhError,
  describe,
  reasonOf,
  timeoutMs,
  withTimeout,
} from "../errors.ts";
import { err } from "../output.ts";
import { BIN } from "../version.ts";
import { type Api, loadEnterpriseApi } from "./jsapi.ts";
import type { KeyPair } from "./keyfile.ts";

const SAML_CLASS = "authentication.client.customlogin.class.SAMLAuth";
const SAML_URL = "authentication.client.samlauth.login.url";
const SAML_NAME = "authentication.client.samlauth.provider.name";
const PASSWORDS = "authentication.passwordsEnabled";

/** A connected Enterprise client; signed in once a login call succeeds. */
export interface EnterpriseConnection {
  origin: string;
  /** The server's irisapi module. */
  dhe: Api;
  /** Its `Client`. */
  client: Api;
}

/** The sign-in options an Enterprise server advertises. */
export interface EnterpriseAuthConfig {
  passwordsEnabled: boolean;
  saml?: { loginUrl: string; providerName: string };
}

/** The error code for rejected credentials: `auth_failed` at login, `auth_expired` for a saved profile. */
type Rejected = "auth_failed" | "auth_expired";

/** `cause`: why it failed; without one, the error says it timed out. */
function unreachable(origin: string, cause?: unknown): DhError {
  return new DhError(
    "server_unreachable",
    `Could not reach ${origin}`,
    cause === undefined ? "Timed out" : describe(cause),
  );
}

/** Loads `origin`'s irisapi (cached under `cacheRoot`) and waits for the gRPC connection. Not signed in yet. */
export async function connectEnterprise(
  origin: string,
  cacheRoot: string,
  timeout: number,
): Promise<EnterpriseConnection> {
  const dhe = await loadEnterpriseApi(new URL(origin), cacheRoot, timeout);
  const client = new dhe.Client(new URL(origin).href, {
    transportFactory: NodeHttp2gRPCTransport.factory,
  });
  const connected = Promise.withResolvers<void>();
  const off = client.addEventListener(dhe.Client.EVENT_CONNECT, () =>
    connected.resolve(),
  );
  try {
    await withTimeout(connected.promise, timeout, () => unreachable(origin));
  } catch (e) {
    disconnect({ origin, dhe, client });
    throw e;
  } finally {
    off();
  }
  return { origin, dhe, client };
}

/** Safe to call more than once. */
export function disconnect(conn: EnterpriseConnection): void {
  try {
    conn.client.disconnect();
  } catch {
    // Already unusable.
  }
}

/** Whether the server allows passwords, and its SSO settings if it has SSO. */
export async function enterpriseAuthConfig(
  conn: EnterpriseConnection,
  timeout: number,
): Promise<EnterpriseAuthConfig> {
  const values: string[][] = await withTimeout(
    conn.client.getAuthConfigValues(),
    timeout,
    () => unreachable(conn.origin),
  );
  const c = Object.fromEntries(values);
  return {
    passwordsEnabled: c[PASSWORDS] !== "false",
    saml: samlConfig(c, conn.origin),
  };
}

/** SSO settings from the server's auth config `c`, if SSO is set up; relative URLs resolve against `origin`. */
function samlConfig(
  c: Record<string, string>,
  origin: string,
): EnterpriseAuthConfig["saml"] {
  if (!c[SAML_CLASS] || !c[SAML_URL]) return undefined;
  return {
    loginUrl: new URL(c[SAML_URL], origin).href,
    providerName: c[SAML_NAME] || "SSO",
  };
}

/**
 * Runs `login` with a timeout. A rejection throws `rejected`; a timeout throws
 * `server_unreachable`. Closes `conn` on failure.
 */
async function guardedLogin(
  conn: EnterpriseConnection,
  login: () => Promise<unknown>,
  timeout: number,
  rejected: Rejected,
): Promise<void> {
  try {
    await withTimeout(login(), timeout, () => unreachable(conn.origin));
  } catch (e) {
    disconnect(conn);
    if (e instanceof DhError) throw e;
    throw new DhError(
      rejected,
      `${conn.origin} rejected the credentials`,
      undefined,
      reasonOf(e),
    );
  }
}

/** `operateAs` defaults to `username`. A rejection is `auth_failed`. */
export function loginPassword(
  conn: EnterpriseConnection,
  username: string,
  password: string,
  operateAs: string | undefined,
  timeout: number,
): Promise<void> {
  const credentials = {
    ...createPasswordCredentials(username, password),
    operateAs: operateAs ?? username,
  };
  return guardedLogin(
    conn,
    () => conn.client.login(credentials),
    timeout,
    "auth_failed",
  );
}

/** Signs in with a key pair. A rejection throws `rejected` (`auth_failed` or `auth_expired`). */
export function loginKey(
  conn: EnterpriseConnection,
  username: string,
  keyPair: KeyPair,
  operateAs: string | undefined,
  timeout: number,
  rejected: Rejected,
): Promise<void> {
  return guardedLogin(
    conn,
    () =>
      loginClientWithKeyPair(
        conn.client,
        // biome-ignore lint/suspicious/noExplicitAny: the library's branded types.
        { type: "keyPair", username, keyPair, operateAs } as any,
      ),
    timeout,
    rejected,
  );
}

/**
 * Waits for the browser sign-in for `nonce`. The server holds each `login()`
 * only up to its SAML wait limit; a sign-in that lands later is picked up by
 * calling again on a new connection.
 */
export async function loginSaml(
  first: EnterpriseConnection,
  reconnect: () => Promise<EnterpriseConnection>,
  nonce: string,
  timeout: number,
): Promise<EnterpriseConnection> {
  const deadline = Date.now() + timeout;
  let conn = first;
  for (;;) {
    const remaining = deadline - Date.now();
    try {
      await withTimeout(
        conn.client.login({ type: "saml", token: nonce }),
        Math.max(remaining, 0),
        () =>
          new DhError(
            "auth_failed",
            "Timed out waiting for sign-in",
            "Run the command again, or raise --timeout.",
          ),
      );
      return conn;
    } catch (e) {
      disconnect(conn);
      if (process.env.DH_DEBUG) {
        err(`${BIN}: SSO login call ended (${describe(e)}); asking again`);
      }
      if (e instanceof DhError || Date.now() >= deadline) {
        throw e instanceof DhError
          ? e
          : new DhError(
              "auth_failed",
              "Sign-in failed",
              undefined,
              reasonOf(e),
            );
      }
    }
    conn = await reconnect();
  }
}

/** Who `conn` is signed in as; how SSO learns the user name. Closes `conn` on failure. */
export async function userName(
  conn: EnterpriseConnection,
  timeout: number,
): Promise<string> {
  try {
    const info = await withTimeout<{ username: string }>(
      conn.client.getUserInfo(),
      timeout,
      () => unreachable(conn.origin),
    );
    return info.username;
  } catch (e) {
    disconnect(conn);
    throw e instanceof DhError ? e : unreachable(conn.origin, e);
  }
}

/** A key {@link createKey} uploaded; `comment` is how it's labeled on the server. */
export interface UploadedKey extends KeyPair {
  comment: string;
}

/** Where key uploads go; reported when they fail. */
async function aclWriter(conn: EnterpriseConnection): Promise<string> {
  try {
    const { dbAclWriterHost, dbAclWriterPort } = await withTimeout<{
      dbAclWriterHost: string;
      dbAclWriterPort: number;
    }>(
      conn.client.getServerConfigValues(),
      timeoutMs(),
      () => new DhError("server_unreachable", "Timed out"),
    );
    return `${dbAclWriterHost}:${dbAclWriterPort}`;
  } catch {
    return "its ACL write server";
  }
}

/** What to tell the user about a failed key upload, from its HTTP `status` and response `body`. */
function uploadHint(status: number, body: string, user: string): string {
  if (status === 401) {
    return "The ACL write server didn't accept this session's token.\nThis is a server configuration problem; tell an administrator.";
  }
  if (status === 403) {
    return `The ACL write server didn't allow adding a key for ${user}.`;
  }
  return body || "The server gave no reason.";
}

/** Generates a key pair and uploads its public key for `username`. */
export async function createKey(
  conn: EnterpriseConnection,
  username: string,
): Promise<UploadedKey> {
  const keyPair = generateBase64KeyPair();
  const date = new Date().toISOString().slice(0, 10);
  // The server rejects comments that aren't printable ASCII.
  const comment = `${BIN} CLI - ${hostname()} - ${date}`.replace(
    /[^\x20-\x7e]/g,
    "?",
  );
  let res: Response;
  try {
    res = await withTimeout(
      uploadPublicKey({
        dheClient: conn.client,
        // biome-ignore lint/suspicious/noExplicitAny: the library's branded types.
        userName: username as any,
        publicKey: keyPair.publicKey,
        comment,
        type: keyPair.type,
      }),
      timeoutMs(),
      () => new DhError("server_unreachable", "Timed out"),
    );
  } catch (e) {
    throw new DhError(
      "key_upload_failed",
      `Could not reach ${await aclWriter(conn)} to upload this computer's key`,
      "That address must be reachable from this computer.",
      describe(e),
    );
  }
  if (!res.ok) {
    // The reason is a nicety; a stalled body mustn't hold up the error.
    const body = await withTimeout(res.text(), timeoutMs(), () =>
      unreachable(conn.origin),
    ).catch(() => "");
    throw new DhError(
      "key_upload_failed",
      `${conn.origin} rejected this computer's key (HTTP ${res.status})`,
      uploadHint(res.status, body.trim().slice(0, 300), username),
    );
  }
  res.body?.cancel().catch(() => {});
  return { ...keyPair, comment };
}

/** Short: a key that isn't deleted now is remembered and deleted at the next sign-in. */
export const DELETE_KEY_TIMEOUT_MS = 5_000;

/** Deletes exactly this public key for `username`; other keys are untouched. */
export async function deleteKey(
  conn: EnterpriseConnection,
  username: string,
  keyPair: KeyPair,
): Promise<void> {
  const deleted = deletePublicKeys({
    dheClient: conn.client,
    // biome-ignore lint/suspicious/noExplicitAny: the library's branded types.
    userName: username as any,
    // biome-ignore lint/suspicious/noExplicitAny: the library's branded types.
    publicKeys: [keyPair.publicKey as any],
    // biome-ignore lint/suspicious/noExplicitAny: the library's branded types.
    type: keyPair.type as any,
  });
  await withTimeout(
    deleted,
    DELETE_KEY_TIMEOUT_MS,
    () =>
      new DhError(
        "server_unreachable",
        `Could not reach ${conn.origin}'s ACL write server to delete the key`,
        "Timed out",
      ),
  );
}
