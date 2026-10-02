// Community: load the server's jsapi, log in with the advertised handler, run code.
// Usage: [DH_PSK=<psk>] deno task dhc <url> [--groovy] [--http2] [--cjs]
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadDhModules, NodeHttp2gRPCTransport } from "@deephaven/jsapi-nodejs";
import { check, done, runtime } from "./report.ts";

const PSK_HANDLER = "io.deephaven.authentication.psk.PskAuthenticationHandler";
const ANONYMOUS_HANDLER = "io.deephaven.auth.AnonymousAuthenticationHandler";

const rawUrl = Deno.args.find((a) => !a.startsWith("--"));
if (!rawUrl) {
  console.error("usage: dhc <serverUrl> [--groovy] [--http2] [--cjs]");
  Deno.exit(2);
}
const serverUrl = new URL(rawUrl);
// CJS is how vscode-deephaven loads the jsapi; under Deno only ESM works.
const moduleType = Deno.args.includes("--cjs") ? "cjs" : "esm";
const http2 = Deno.args.includes("--http2");
const groovy = Deno.args.includes("--groovy");
const psk = Deno.env.get("DH_PSK");

console.log(
  `dhc (${runtime}) ${serverUrl} module=${moduleType} transport=${
    http2 ? "http2" : "default"
  } session=${groovy ? "groovy" : "python"}`,
);

// deno-lint-ignore no-explicit-any
const g = globalThis as any;
g.self = globalThis;

// The jsapi rejects promises nobody awaits after failed logins; record, don't crash.
const stray: string[] = [];
globalThis.addEventListener("unhandledrejection", (e) => {
  e.preventDefault();
  stray.push(String(e.reason?.message ?? e.reason));
});

// deno-lint-ignore no-explicit-any
const dh: any = await check(
  "loadDhModules (download + import server jsapi)",
  () =>
    loadDhModules({
      serverUrl,
      storageDir: join(tmpdir(), "dh-auth-check"),
      targetModuleType: moduleType,
    }),
  (m) => `CoreClient=${typeof m?.CoreClient}`,
);
if (!dh?.CoreClient) done();

const client = new dh.CoreClient(
  serverUrl.toString(),
  http2 ? { transportFactory: NodeHttp2gRPCTransport.factory } : undefined,
);

const handlers = await check(
  "getAuthConfigValues",
  async () =>
    // deno-lint-ignore no-explicit-any
    ((await client.getAuthConfigValues()) as any[])
      .filter(([k]) => k === "AuthHandlers")
      .flatMap(([, v]) => String(v).split(",")),
  (h) => h.join(","),
);
if (!handlers) done();

if (handlers.includes(PSK_HANDLER)) {
  await check("login (PSK, wrong token) is rejected", async () => {
    const bad = new dh.CoreClient(serverUrl.toString());
    try {
      // The jsapi needs the auth config loaded before login().
      await bad.getAuthConfigValues();
      await bad.login({ type: PSK_HANDLER, token: "definitely-wrong" });
    } catch (e) {
      return `rejected: ${e instanceof Error ? e.message : String(e)}`;
    } finally {
      // jsapi quirk: disconnect() throws after a failed login.
      try {
        bad.disconnect();
      } catch { /* ignored */ }
    }
    throw new Error("wrong PSK was accepted");
  });
}

if (handlers.includes(PSK_HANDLER)) {
  if (!psk) {
    console.log("FAIL server uses PSK; set DH_PSK");
    done();
  }
  await check(
    "login (PSK)",
    () => client.login({ type: PSK_HANDLER, token: psk }),
  );
} else if (handlers.includes(ANONYMOUS_HANDLER)) {
  await check(
    "login (anonymous)",
    () => client.login({ type: dh.CoreClient.LOGIN_TYPE_ANONYMOUS }),
  );
} else {
  console.log(`FAIL no supported handler in: ${handlers.join(",")}`);
  done();
}

await check("authenticated call: start session, run code", async () => {
  const cn = await client.getAsIdeConnection();
  const session = await cn.startSession(groovy ? "groovy" : "python");
  const result = await session.runCode(
    groovy
      ? "throw new RuntimeException('auth-check-ran')"
      : "raise ValueError('auth-check-ran')",
  );
  if (!String(result.error).includes("auth-check-ran")) {
    throw new Error(`code did not run: ${JSON.stringify(result.error)}`);
  }
  return "server executed code";
});

await check("disconnect", () => client.disconnect());
if (stray.length) {
  console.log(
    `NOTE ${stray.length} unhandled jsapi rejections: ${
      JSON.stringify([...new Set(stray)])
    }`,
  );
}
done();
