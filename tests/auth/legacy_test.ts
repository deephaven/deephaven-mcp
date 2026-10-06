import { assertEquals } from "@std/assert";
import { fromFileUrl } from "@std/path";
import { findLegacy, parseV1, parseV2 } from "../../src/auth/legacy.ts";

const fixture = (path: string) =>
  fromFileUrl(new URL(`../fixtures/legacy/${path}`, import.meta.url));

Deno.test("parseV1: maps every session and system", async () => {
  const path = fixture("v1_config.json");
  const legacy = parseV1(await Deno.readTextFile(path), path);
  const byName = Object.fromEntries(legacy.items.map((i) => [i.name, i]));

  assertEquals(byName.local_dev.origin, "http://localhost:10000");
  assertEquals(byName.local_dev.plan, {
    method: "psk",
    secret: { literal: "Deephaven123" },
  });
  assertEquals(byName.anon.plan, { method: "anonymous" });
  assertEquals(byName.psk_env.origin, "https://groovy.example.com:10002");
  assertEquals(byName.psk_env.plan, {
    method: "psk",
    secret: { env: "DH_COMMUNITY_PSK" },
  });
  assertEquals(byName.basic.plan, {
    method: "basic",
    secret: { literal: "user:pass" },
  });
  assertEquals(byName.custom.plan, {
    method: "custom",
    handler: "com.example.auth.TokenHandler",
    secret: { literal: "tok" },
  });
  assertEquals(byName.prod.origin, "https://prod.example.com:8123");
  assertEquals(byName.prod.plan, {
    method: "password",
    username: "alice",
    password: { env: "DH_PROD_PASSWORD" },
    operateAs: "svc",
  });
  assertEquals(byName.keyed.plan, {
    method: "private-key",
    keyPath: "/etc/deephaven/priv-keyed.base64.txt",
  });
  assertEquals(legacy.notImported.length, 2);
});

Deno.test("parseV2: maps the tree and reads the default system", async () => {
  const legacy = await parseV2(fixture("ai/config"));
  const byName = Object.fromEntries(legacy.items.map((i) => [i.name, i]));
  assertEquals(legacy.defaultName, "prod");
  assertEquals(byName.local_dev.plan, {
    method: "psk",
    secret: { env: "DH_LOCAL_DEV_PSK" },
  });
  assertEquals(byName.prod.origin, "https://deephaven-prod.example.com:8123");
  assertEquals(byName.prod.plan.method, "password");
  assertEquals(byName.staging.caCert, "/etc/deephaven/ca.pem");
  assertEquals(byName.staging.plan, {
    method: "private-key",
    keyPath: "/etc/deephaven/priv-staging.base64.txt",
  });
  assertEquals(legacy.notImported, [
    "enterprise/systems/prod session creation",
  ]);
});

Deno.test("findLegacy: DH_MCP_CONFIG_FILE, then the v2 tree under DH_AI_DATA_DIR", async () => {
  const saved = {
    file: Deno.env.get("DH_MCP_CONFIG_FILE"),
    root: Deno.env.get("DH_AI_DATA_DIR"),
  };
  try {
    Deno.env.delete("DH_MCP_CONFIG_FILE");
    Deno.env.set("DH_AI_DATA_DIR", fixture("ai"));
    assertEquals((await findLegacy())?.defaultName, "prod");
    Deno.env.set("DH_MCP_CONFIG_FILE", fixture("v1_config.json"));
    assertEquals((await findLegacy())?.items.length, 7);
    Deno.env.set("DH_AI_DATA_DIR", fixture("missing"));
    Deno.env.delete("DH_MCP_CONFIG_FILE");
    assertEquals(await findLegacy(), undefined);
  } finally {
    for (
      const [k, v] of [["DH_MCP_CONFIG_FILE", saved.file], [
        "DH_AI_DATA_DIR",
        saved.root,
      ]]
    ) {
      if (v === undefined) Deno.env.delete(k!);
      else Deno.env.set(k!, v);
    }
  }
});
