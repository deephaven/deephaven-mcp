import { hostname } from "node:os";
import { NodeHttp2gRPCTransport } from "@deephaven/jsapi-nodejs";
import {
  type AuthenticatedEnterpriseClient,
  type Base64PublicKey,
  createPasswordCredentials,
  deletePublicKeys,
  type EnterpriseClient,
  generateBase64KeyPair,
  type KeyPairCredentials,
  type KeyPairType,
  loginClientWithKeyPair,
  loginClientWithPassword,
  type OperateAsUsername,
  type UnauthenticatedEnterpriseClient,
  type Username,
  uploadPublicKey,
} from "@deephaven-enterprise/jsapi-nodejs";
import {
  DhError,
  describe,
  reasonOf,
  timeoutMs,
  serverUnreachable as unreachable,
  withTimeout,
} from "../../errors.ts";
import { err, note } from "../../output.ts";
import { BIN } from "../../version.ts";
import { type EnterpriseApi, loadEnterpriseApi } from "./jsapi.ts";
import type { KeyPair } from "./keyfile.ts";

const SAML_CLASS = "authentication.client.customlogin.class.SAMLAuth";
const SAML_URL = "authentication.client.samlauth.login.url";
const SAML_NAME = "authentication.client.samlauth.provider.name";
const PASSWORDS = "authentication.passwordsEnabled";

/**
 * A connected Enterprise client. `Client` records whether it's signed in:
 * the login functions take an {@link UnauthenticatedEnterpriseConnection} and
 * return an {@link AuthenticatedEnterpriseConnection}.
 */
export interface EnterpriseConnection<
  Client extends EnterpriseClient = EnterpriseClient,
> {
  origin: string;
  /** The server's irisapi module. */
  dhe: EnterpriseApi;
  /** Its `Client`. */
  client: Client;
}

/** Connected, not signed in yet. */
export type UnauthenticatedEnterpriseConnection =
  EnterpriseConnection<UnauthenticatedEnterpriseClient>;

/** Signed in. */
export type AuthenticatedEnterpriseConnection =
  EnterpriseConnection<AuthenticatedEnterpriseClient>;

/** The sign-in options an Enterprise server advertises. */
export interface EnterpriseAuthConfig {
  passwordsEnabled: boolean;
  saml?: { loginUrl: string; providerName: string };
}

/** The error code for rejected credentials: `auth_failed` at login, `auth_expired` for a saved profile. */
type Rejected = "auth_failed" | "auth_expired";

/**
 * Loads `origin`'s irisapi (cached under `cacheRoot`) and waits for the gRPC
 * connection. Not signed in yet. `downloadTimeout` limits the irisapi download
 * (several MB); `timeout` the connection.
 */
export async function connectEnterprise(
  origin: string,
  cacheRoot: string,
  timeout: number,
  downloadTimeout = timeout,
): Promise<UnauthenticatedEnterpriseConnection> {
  const dhe = await loadEnterpriseApi(
    new URL(origin),
    cacheRoot,
    downloadTimeout,
  );
  // TODO: DH-24026 will add a signal to createEnterpriseClient; use it and drop
  // this cast and the hand-rolled connect wait.
  const client = new dhe.Client(new URL(origin).href, {
    transportFactory: NodeHttp2gRPCTransport.factory,
  }) as UnauthenticatedEnterpriseClient;
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
 * Runs `login` with a timeout and returns the signed-in connection. A
 * rejection throws `rejected`; a timeout throws `server_unreachable`. Closes
 * `conn` on failure.
 */
async function guardedLogin(
  conn: UnauthenticatedEnterpriseConnection,
  login: () => Promise<AuthenticatedEnterpriseClient>,
  timeout: number,
  rejected: Rejected,
): Promise<AuthenticatedEnterpriseConnection> {
  try {
    const client = await withTimeout(login(), timeout, () =>
      unreachable(conn.origin),
    );
    return { ...conn, client };
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

/** `operateAs` defaults to `username`. A rejection throws `rejected` (`auth_failed` or `auth_expired`). */
export function loginPassword(
  conn: UnauthenticatedEnterpriseConnection,
  username: string,
  password: string,
  operateAs: string | undefined,
  timeout: number,
  rejected: Rejected,
): Promise<AuthenticatedEnterpriseConnection> {
  // TODO: DH-24026 will add operateAs to createPasswordCredentials; use it and
  // drop this cast.
  const credentials = {
    ...createPasswordCredentials(username, password),
    operateAs: (operateAs ?? username) as OperateAsUsername,
  };
  return guardedLogin(
    conn,
    () => loginClientWithPassword(conn.client, credentials),
    timeout,
    rejected,
  );
}

/** Signs in with a key pair. A rejection throws `rejected` (`auth_failed` or `auth_expired`). */
export function loginKey(
  conn: UnauthenticatedEnterpriseConnection,
  username: string,
  keyPair: KeyPair,
  operateAs: string | undefined,
  timeout: number,
  rejected: Rejected,
): Promise<AuthenticatedEnterpriseConnection> {
  // TODO: DH-24026 will add createKeyPairCredentials; use it and drop the
  // KeyPairCredentials cast.
  return guardedLogin(
    conn,
    () =>
      loginClientWithKeyPair(conn.client, {
        type: "keyPair",
        username,
        keyPair,
        operateAs,
      } as KeyPairCredentials),
    timeout,
    rejected,
  );
}

/** Heuristic cutoff below the server's ~60s SSO wait limit; it is not a status code. */
const SAML_MIN_HOLD_MS = 5_000;

/**
 * Waits for the browser sign-in for `nonce` without an overall timeout. The
 * server doesn't distinguish a wait-limit rejection from cancellation or other
 * failures. Treat a non-DhError rejection after at least 5s as a possible
 * wait-limit expiry and retry with the same nonce. This timing heuristic can
 * also retry other slow failures.
 */
export async function loginSaml(
  first: UnauthenticatedEnterpriseConnection,
  reconnect: () => Promise<UnauthenticatedEnterpriseConnection>,
  nonce: string,
): Promise<AuthenticatedEnterpriseConnection> {
  // TODO: DH-24026 will add loginClientWithExternal, which does this retry loop
  // (onRetry for the notice); use it and drop the cast below.
  let conn = first;
  let showedRetryNotice = false;
  for (;;) {
    const started = Date.now();
    try {
      await conn.client.login({ type: "saml", token: nonce });
      return {
        ...conn,
        client: conn.client as unknown as AuthenticatedEnterpriseClient,
      };
    } catch (e) {
      disconnect(conn);
      if (e instanceof DhError || Date.now() - started < SAML_MIN_HOLD_MS) {
        throw e instanceof DhError
          ? e
          : new DhError(
              "auth_failed",
              "Sign-in failed",
              undefined,
              reasonOf(e),
            );
      }
      if (!showedRetryNotice) {
        note("Browser error? Interrupt and retry.");
        showedRetryNotice = true;
      }
      if (process.env.DH_DEBUG) {
        err(`${BIN}: SSO login call ended (${describe(e)}); asking again`);
      }
    }
    conn = await reconnect();
  }
}

/** Who `conn` is signed in as; how SSO learns the user name. Closes `conn` on failure. */
export async function userName(
  conn: AuthenticatedEnterpriseConnection,
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

/** A key pair from {@link newKey}; `comment` is how it's labeled on the server. */
export interface LabeledKey extends KeyPair {
  comment: string;
}

/** Where key uploads go; reported when they fail. */
async function aclWriter(
  conn: AuthenticatedEnterpriseConnection,
): Promise<string> {
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

/** A new key pair labeled for this computer; nothing is sent until {@link uploadKey}. */
export function newKey(): LabeledKey {
  const date = new Date().toISOString().slice(0, 10);
  // The server rejects comments that aren't printable ASCII.
  const comment = `${BIN} CLI - ${hostname()} - ${date}`.replace(
    /[^\x20-\x7e]/g,
    "?",
  );
  return { ...generateBase64KeyPair(), comment };
}

/** Uploads `keyPair`'s public key for `username`. */
export async function uploadKey(
  conn: AuthenticatedEnterpriseConnection,
  username: string,
  keyPair: LabeledKey,
): Promise<void> {
  let res: Response;
  try {
    res = await withTimeout(
      uploadPublicKey({
        dheClient: conn.client,
        userName: username as Username,
        publicKey: keyPair.publicKey as Base64PublicKey,
        comment: keyPair.comment,
        type: keyPair.type as KeyPairType,
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
}

/** Short: a key that isn't deleted now is remembered and deleted at the next sign-in. */
export const DELETE_KEY_TIMEOUT_MS = 5_000;

/** Deletes exactly this public key for `username`; other keys are untouched. */
export async function deleteKey(
  conn: AuthenticatedEnterpriseConnection,
  username: string,
  keyPair: KeyPair,
): Promise<void> {
  const deleted = deletePublicKeys({
    dheClient: conn.client,
    userName: username as Username,
    publicKeys: [keyPair.publicKey as Base64PublicKey],
    type: keyPair.type as KeyPairType,
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
