import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  findLegacy,
  parseV1,
  parseV2,
  resolveSecret,
} from "../../src/auth/legacy.ts";

const fixture = (path: string) =>
  resolve(import.meta.dir, "../fixtures/legacy", path);

test("parseV1: maps every session and system", async () => {
  const path = fixture("v1_config.json");
  const legacy = parseV1(await readFile(path, "utf8"), path);
  const byName = Object.fromEntries(legacy.items.map((i) => [i.name, i]));

  expect(byName.local_dev?.origin).toBe("http://localhost:10000");
  expect(byName.local_dev?.plan).toEqual({
    method: "psk",
    secret: { literal: "Deephaven123" },
  });
  expect(byName.anon?.plan).toEqual({ method: "anonymous" });
  expect(byName.psk_env?.origin).toBe("https://groovy.example.com:10002");
  expect(byName.psk_env?.plan).toEqual({
    method: "psk",
    secret: { env: "DH_COMMUNITY_PSK" },
  });
  expect(byName.basic?.plan).toEqual({
    method: "basic",
    secret: { literal: "user:pass" },
  });
  expect(byName.custom?.plan).toEqual({
    method: "custom",
    handler: "com.example.auth.TokenHandler",
    secret: { literal: "tok" },
  });
  expect(byName.prod?.origin).toBe("https://prod.example.com:8123");
  expect(byName.prod?.plan).toEqual({
    method: "password",
    username: "alice",
    password: { env: "DH_PROD_PASSWORD" },
    operateAs: "svc",
  });
  expect(byName.keyed?.plan).toEqual({
    method: "private-key",
    keyPath: "/etc/deephaven/priv-keyed.base64.txt",
  });
  expect(legacy.notImported.length).toBe(2);
});

test("parseV2: maps the tree and reads the default system", async () => {
  const legacy = await parseV2(fixture("ai/config"));
  const byName = Object.fromEntries(legacy.items.map((i) => [i.name, i]));
  expect(legacy.defaultItem).toEqual({ name: "prod", kind: "enterprise" });
  expect(byName.local_dev?.plan).toEqual({
    method: "psk",
    secret: { env: "DH_LOCAL_DEV_PSK" },
  });
  expect(byName.prod?.origin).toBe("https://deephaven-prod.example.com:8123");
  expect(byName.prod?.plan.method).toBe("password");
  expect(byName.staging?.caCert).toBe("/etc/deephaven/ca.pem");
  expect(byName.staging?.plan).toEqual({
    method: "private-key",
    keyPath: "/etc/deephaven/priv-staging.base64.txt",
  });
  expect(legacy.notImported).toEqual([
    "enterprise/systems/prod session creation",
  ]);
});

test("findLegacy: DH_MCP_CONFIG_FILE, then the v2 tree under DH_AI_DATA_DIR", async () => {
  const saved = {
    DH_MCP_CONFIG_FILE: process.env.DH_MCP_CONFIG_FILE,
    DH_AI_DATA_DIR: process.env.DH_AI_DATA_DIR,
  };
  try {
    delete process.env.DH_MCP_CONFIG_FILE;
    process.env.DH_AI_DATA_DIR = fixture("ai");
    expect((await findLegacy())?.defaultItem?.name).toBe("prod");
    process.env.DH_MCP_CONFIG_FILE = fixture("v1_config.json");
    expect((await findLegacy())?.items.length).toBe(7);
    process.env.DH_AI_DATA_DIR = fixture("missing");
    delete process.env.DH_MCP_CONFIG_FILE;
    expect(await findLegacy()).toBeUndefined();
  } finally {
    for (const [k, v] of Object.entries(saved)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
});

test("unknown auth types are unsupported, not anonymous or password", async () => {
  const v1 = parseV1(
    JSON.stringify({
      enterprise: {
        systems: {
          sso: {
            connection_json_url: "https://x.example.com",
            auth_type: "saml",
          },
        },
      },
    }),
    "v1.json",
  );
  expect(v1.items[0]?.plan).toEqual({ method: "unsupported", type: "saml" });

  const dir = await mkdtemp(join(tmpdir(), "dh-legacy-"));
  await mkdir(join(dir, "community", "sessions"), { recursive: true });
  await writeFile(
    join(dir, "community", "sessions", "x.json"),
    JSON.stringify({ auth: { credentials: { type: "oauth" } } }),
  );
  const v2 = await parseV2(dir);
  expect(v2.items[0]?.plan).toEqual({ method: "unsupported", type: "oauth" });
});

test("resolveSecret: a file keeps its spaces, loses one line ending, and empty is unset", async () => {
  const dir = await mkdtemp(join(tmpdir(), "dh-legacy-"));
  const file = join(dir, "secret");
  await writeFile(file, " s e c \n");
  expect(await resolveSecret({ file })).toBe(" s e c ");
  await writeFile(file, "");
  expect(await resolveSecret({ file })).toBeUndefined();
});
