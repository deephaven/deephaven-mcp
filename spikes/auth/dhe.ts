// Enterprise: load irisapi, read the auth config, log in, and round-trip a key.
// Usage:
//   deno task dhe probe <url>
//     No login. Load irisapi, read the auth config, and check that a SAML
//     login for a nonce nobody has signed in with waits rather than failing.
//   deno task dhe saml <url> [--keep-key]
//     Browser SSO (one waiting login() call), then a key round trip.
//   read -s P; printf %s "$P" | DH_USER=<user> deno task dhe password <url> [--keep-key]
//     Password login, then a key round trip.
// Key round trip: upload a key, log in with it on a new connection, delete it
// (unless --keep-key), and check the deleted key is refused.
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Buffer } from "node:buffer";
import { createJsApiFactories } from "@deephaven-enterprise/jsapi-nodejs";
import {
  createPasswordCredentials,
  deletePublicKeys,
  generateBase64KeyPair,
  loginClientWithKeyPair,
  loginClientWithPassword,
  uploadPublicKey,
} from "@deephaven-enterprise/auth-nodejs";
import { check, done, runtime } from "./report.ts";

const SAML_CLASS = "authentication.client.customlogin.class.SAMLAuth";
const SAML_URL = "authentication.client.samlauth.login.url";
const SAML_NAME = "authentication.client.samlauth.provider.name";
const PASSWORDS = "authentication.passwordsEnabled";
const PROBE_WAIT_MS = 20_000;
const SIGN_IN_TIMEOUT_MS = 5 * 60_000;

const [mode = "probe", rawUrl] = Deno.args.filter((a) => !a.startsWith("--"));
if (!rawUrl || !["probe", "saml", "password"].includes(mode)) {
  console.error("usage: dhe <probe|saml|password> <serverUrl> [--keep-key]");
  Deno.exit(2);
}
const serverUrl = new URL(rawUrl);
const keepKey = Deno.args.includes("--keep-key");

// deno-lint-ignore no-explicit-any
type Any = any;

// The jsapi rejects promises nobody awaits after failed logins; record, don't crash.
const stray: string[] = [];
globalThis.addEventListener("unhandledrejection", (e) => {
  e.preventDefault();
  stray.push(String(e.reason?.message ?? e.reason));
});

console.log(`dhe (${runtime}) ${mode} ${serverUrl}`);

const factories = createJsApiFactories({
  storageDir: join(tmpdir(), "dh-auth-check"),
});
const dhe: Any = await check(
  "loadEnterpriseApi (download + import irisapi)",
  () => factories.loadEnterpriseApi(serverUrl),
  (api: Any) => `Client=${typeof api?.Client}`,
);
if (!dhe?.Client) done();

const connect = (): Promise<Any> =>
  factories.createEnterpriseClient(dhe, serverUrl);

const first: Any = await check("connect", connect, () => "connected");
if (!first) done();

const authConfig = await check(
  "getAuthConfigValues",
  async () =>
    Object.fromEntries(await first.getAuthConfigValues()) as Record<
      string,
      string
    >,
  (c) => ({
    saml: Boolean(c[SAML_CLASS] && c[SAML_URL]),
    samlProvider: c[SAML_NAME],
    samlLoginUrl: c[SAML_URL],
    passwordsEnabled: c[PASSWORDS] !== "false",
  }),
);

function samlNonce(): string {
  return Buffer.from(crypto.getRandomValues(new Uint8Array(96)))
    .toString("base64");
}

async function tryLogin(
  client: Any,
  credentials: Any,
  timeoutMs: number,
): Promise<string> {
  const start = performance.now();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<string>((resolve) => {
    timer = setTimeout(
      () => resolve(`still waiting after ${timeoutMs} ms`),
      timeoutMs,
    );
  });
  const attempt = (async () => {
    try {
      await client.login(credentials);
      return "ACCEPTED";
    } catch (e) {
      const ms = Math.round(performance.now() - start);
      return `rejected in ${ms} ms: ${
        e instanceof Error ? e.message : String(e)
      }`;
    }
  })();
  try {
    return await Promise.race([attempt, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

async function keyPairRoundTrip(authed: Any, username: string): Promise<void> {
  const keyPair = generateBase64KeyPair();
  const comment = `dh auth check ${Deno.hostname()} ${
    new Date().toISOString()
  }`;

  await check(
    "getServerConfigValues (ACL writer location)",
    () => authed.getServerConfigValues(),
    (c: Any) => `${c.dbAclWriterHost}:${c.dbAclWriterPort}`,
  );

  const uploaded = await check("uploadPublicKey", async () => {
    const res = await uploadPublicKey({
      dheClient: authed,
      userName: username as Any,
      publicKey: keyPair.publicKey,
      comment,
      type: keyPair.type,
    });
    // auth-nodejs returns the raw Response; it does not check status itself.
    const body = await res.text();
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${body.slice(0, 300)}`);
    return `HTTP ${res.status}`;
  });
  if (!uploaded) return;

  const keyClient: Any = await check(
    "loginClientWithKeyPair (new connection)",
    async () => {
      const c = await connect();
      await loginClientWithKeyPair(c, {
        type: "keyPair",
        username,
        keyPair,
      } as Any);
      return c;
    },
    () => "ok",
  );
  if (keyClient) {
    await check(
      "getUserInfo via key-pair login",
      () => keyClient.getUserInfo(),
      (u: Any) => u.username,
    );
  }

  if (keepKey) {
    console.log(`NOTE kept key "${comment}" on the server`);
    return;
  }
  await check("deletePublicKeys", () =>
    deletePublicKeys({
      dheClient: keyClient ?? authed,
      userName: username as Any,
      publicKeys: [keyPair.publicKey],
      type: keyPair.type,
    }));
  await check("deleted key is refused", async () => {
    const c = await connect();
    try {
      await loginClientWithKeyPair(c, {
        type: "keyPair",
        username,
        keyPair,
      } as Any);
    } catch (e) {
      return `refused: ${e instanceof Error ? e.message : String(e)}`;
    }
    throw new Error("deleted key still logs in");
  });
}

if (mode === "probe") {
  await check(
    "SAML login with unbound nonce",
    async () => {
      const result = await tryLogin(
        first,
        { type: "saml", token: samlNonce() },
        PROBE_WAIT_MS,
      );
      if (!result.startsWith("still waiting")) throw new Error(result);
      return `${result} (server holds the request until sign-in)`;
    },
    undefined,
    PROBE_WAIT_MS + 5_000,
  );
} else if (mode === "saml") {
  const loginUrl = authConfig?.[SAML_URL];
  if (!loginUrl) {
    console.log("FAIL server does not advertise SAML");
    done();
  }
  const nonce = samlNonce();
  const url = new URL(loginUrl!, serverUrl);
  url.searchParams.set("key", nonce);
  console.log(`\nSign in here (opening browser):\n  ${url}\n`);
  const opener = Deno.build.os === "darwin"
    ? ["open", url.href]
    : Deno.build.os === "windows"
    ? ["cmd", "/c", "start", "", url.href]
    : ["xdg-open", url.href];
  await new Deno.Command(opener[0], { args: opener.slice(1) }).output()
    .catch(() => console.log("NOTE could not open a browser"));

  const authed: Any = await check(
    "wait for SAML sign-in (single login call)",
    async () => {
      const result = await tryLogin(
        first,
        { type: "saml", token: nonce },
        SIGN_IN_TIMEOUT_MS,
      );
      if (result !== "ACCEPTED") throw new Error(result);
      return first;
    },
    () => "signed in",
    SIGN_IN_TIMEOUT_MS + 5_000,
  );
  if (authed) {
    const user: Any = await check(
      "getUserInfo",
      () => authed.getUserInfo(),
      (u: Any) => u.username,
    );
    if (user?.username) await keyPairRoundTrip(authed, user.username);
  }
} else if (mode === "password") {
  const username = Deno.env.get("DH_USER");
  const password = (await new Response(Deno.stdin.readable).text()).trim();
  if (!username || !password) {
    console.log("FAIL set DH_USER and pipe the password on stdin");
    done();
  }
  const authed: Any = await check("loginClientWithPassword", async () => {
    await loginClientWithPassword(
      first,
      createPasswordCredentials(username!, password),
    );
    return first;
  }, () => "ok");
  if (authed) await keyPairRoundTrip(authed, username!);
}

if (stray.length) {
  console.log(
    `NOTE ${stray.length} unhandled jsapi rejections: ${
      JSON.stringify([...new Set(stray)])
    }`,
  );
}
done();
