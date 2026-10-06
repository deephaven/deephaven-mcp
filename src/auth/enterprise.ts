import { NodeHttp2gRPCTransport } from "@deephaven/jsapi-nodejs";
import {
  createPasswordCredentials,
  deletePublicKeys,
  generateBase64KeyPair,
  loginClientWithKeyPair,
  uploadPublicKey,
} from "@deephaven-enterprise/auth-nodejs";
import { BIN } from "../version.ts";
import { describe, DhError, withTimeout } from "./errors.ts";
import { type Api, loadEnterpriseApi } from "./jsapi.ts";
import type { KeyPair } from "./keyfile.ts";

const SAML_CLASS = "authentication.client.customlogin.class.SAMLAuth";
const SAML_URL = "authentication.client.samlauth.login.url";
const SAML_NAME = "authentication.client.samlauth.provider.name";
const SAML_CONFIRM_URL = "authentication.client.samlauth.confirm.url";
const PASSWORDS = "authentication.passwordsEnabled";

export interface EnterpriseConnection {
  origin: string;
  dhe: Api;
  client: Api;
}

export interface EnterpriseAuthConfig {
  passwordsEnabled: boolean;
  saml?: { loginUrl: string; providerName: string; confirmUrl?: string };
}

type Rejected = "auth_failed" | "auth_expired";

function unreachable(origin: string, cause?: unknown): DhError {
  return new DhError(
    "server_unreachable",
    `Could not reach ${origin}`,
    cause === undefined ? "Timed out" : describe(cause),
  );
}

export async function connectEnterprise(
  origin: string,
  cacheRoot: string,
  timeout: number,
): Promise<EnterpriseConnection> {
  const dhe = await loadEnterpriseApi(new URL(origin), cacheRoot, timeout);
  const client = new dhe.Client(new URL(origin).href, {
    transportFactory: NodeHttp2gRPCTransport.factory,
  });
  await withTimeout(
    new Promise<void>((resolve) => {
      const off = client.addEventListener(dhe.Client.EVENT_CONNECT, () => {
        off();
        resolve();
      });
    }),
    timeout,
    () => unreachable(origin),
  );
  return { origin, dhe, client };
}

export function disconnect(conn: EnterpriseConnection): void {
  try {
    conn.client.disconnect();
  } catch {
    // Already unusable.
  }
}

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
  const resolve = (url: string) => new URL(url, conn.origin).href;
  return {
    passwordsEnabled: c[PASSWORDS] !== "false",
    saml: c[SAML_CLASS] && c[SAML_URL]
      ? {
        loginUrl: resolve(c[SAML_URL]),
        providerName: c[SAML_NAME] || "SSO",
        confirmUrl: c[SAML_CONFIRM_URL]
          ? resolve(c[SAML_CONFIRM_URL])
          : undefined,
      }
      : undefined,
  };
}

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
    throw new DhError(rejected, `${conn.origin} rejected the credentials`);
  }
}

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
        // deno-lint-ignore no-explicit-any
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
      if (e instanceof DhError || Date.now() >= deadline) {
        throw e instanceof DhError
          ? e
          : new DhError("auth_failed", "Sign-in failed");
      }
    }
    conn = await reconnect();
  }
}

export async function userName(conn: EnterpriseConnection): Promise<string> {
  const info = await conn.client.getUserInfo();
  return info.username;
}

export interface UploadedKey extends KeyPair {
  comment: string;
}

/** Where key uploads go; reported when they fail. */
async function aclWriter(conn: EnterpriseConnection): Promise<string> {
  try {
    const { dbAclWriterHost, dbAclWriterPort } = await conn.client
      .getServerConfigValues();
    return `${dbAclWriterHost}:${dbAclWriterPort}`;
  } catch {
    return "its ACL write server";
  }
}

function uploadHint(status: number, body: string, user: string): string {
  if (status === 401) {
    return "The ACL write server didn't accept this session's token. This is a server configuration problem; tell an administrator.";
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
  const comment = `${BIN} CLI - ${Deno.hostname()} - ${date}`
    .replace(/[^\x20-\x7e]/g, "?");
  let res: Response;
  try {
    res = await uploadPublicKey({
      dheClient: conn.client,
      // deno-lint-ignore no-explicit-any
      userName: username as any,
      publicKey: keyPair.publicKey,
      comment,
      type: keyPair.type,
    });
  } catch (e) {
    throw new DhError(
      "key_upload_failed",
      `Could not reach ${await aclWriter(conn)} to upload this computer's key`,
      `${describe(e)}. That address must be reachable from this computer.`,
    );
  }
  const body = (await res.text().catch(() => "")).trim().slice(0, 300);
  if (!res.ok) {
    throw new DhError(
      "key_upload_failed",
      `${conn.origin} rejected this computer's key (HTTP ${res.status})`,
      uploadHint(res.status, body, username),
    );
  }
  return { ...keyPair, comment };
}

export async function deleteKey(
  conn: EnterpriseConnection,
  username: string,
  keyPair: KeyPair,
): Promise<void> {
  await deletePublicKeys({
    dheClient: conn.client,
    // deno-lint-ignore no-explicit-any
    userName: username as any,
    // deno-lint-ignore no-explicit-any
    publicKeys: [keyPair.publicKey as any],
    // deno-lint-ignore no-explicit-any
    type: keyPair.type as any,
  });
}
