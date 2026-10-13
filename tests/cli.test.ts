import { expect, test } from "bun:test";
import { chmod, mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { BIN } from "../src/version.ts";
import {
  CLI,
  http2OnlyEnterprise,
  run,
  TEST_CA,
  tempDir,
  WINDOWS,
} from "./helpers.ts";

async function setup() {
  const tmp = await tempDir("dh-cli-test-");
  return {
    tmp,
    configDir: join(tmp, "cli"),
    run: (args: string[], env: Record<string, string> = {}, stdin?: string) =>
      run(
        [...CLI, ...args],
        {
          DH_CONFIG_DIR: join(tmp, "cli"),
          DH_AI_DATA_DIR: join(tmp, "legacy"),
          DH_AUTO_UPDATE: "off",
          DH_PROFILE: "",
          DH_SERVER: "",
          ...env,
        },
        stdin,
      ),
  };
}

interface Extra {
  servers?: Record<string, unknown>;
  profiles?: Record<string, unknown>;
  credentials?: Record<string, unknown>;
}

/** Writes a config with two Community profiles that need no network, plus `extra`. */
async function seed(configDir: string, extra: Extra = {}) {
  await mkdir(configDir, { recursive: true });
  await writeFile(
    join(configDir, "config.json"),
    JSON.stringify({
      version: 1,
      defaultProfile: "p_a",
      servers: {
        "http://localhost:10000": { kind: "community" },
        "http://localhost:10001": { kind: "community" },
        ...extra.servers,
      },
      profiles: {
        p_a: {
          name: "a",
          server: "http://localhost:10000",
          user: "psk",
          method: "psk",
          createdAt: "",
        },
        p_b: {
          name: "b",
          server: "http://localhost:10001",
          user: "anonymous",
          method: "anonymous",
          createdAt: "",
        },
        ...extra.profiles,
      },
    }),
  );
  await writeFile(
    join(configDir, "credentials.json"),
    JSON.stringify({
      version: 1,
      credentials: {
        p_a: { type: "envRef", env: "UNSET_TEST_PSK" },
        ...extra.credentials,
      },
    }),
    { mode: 0o600 },
  );
  if (!WINDOWS) await chmod(join(configDir, "credentials.json"), 0o600);
}

test("cli: no profiles", async () => {
  const { run } = await setup();
  const list = await run(["auth", "-o", "json"]);
  expect(list.code).toBe(0);
  expect(JSON.parse(list.stdout)).toEqual([]);

  const status = await run(["auth", "status", "-o", "json"]);
  expect(status.code).toBe(4);
  expect(JSON.parse(status.stderr).code).toBe("auth_required");

  const bare = await run([]);
  expect(bare.code).toBe(0);
  expect(bare.stdout).toContain(`Run \`${BIN} auth login\``);
});

test("cli: usage errors exit 2, with JSON on stderr", async () => {
  const { run } = await setup();
  const login = await run(["auth", "login", "--no-prompt", "-o", "json"]);
  expect(login.code).toBe(2);
  const error = JSON.parse(login.stderr);
  expect(error.code).toBe("usage");
  expect(error.error).toContain("<server>");
  expect(login.stdout).toBe("");

  const unknown = await run(["auth", "bogus"]);
  expect(unknown.code).toBe(2);
});

test("cli: -o follows commander: the last one wins, and nothing after --", async () => {
  const { run } = await setup();
  const last = await run(["auth", "-o", "json", "-o", "human"]);
  expect(last.stdout).toContain("No profiles.");
  const literal = await run(["auth", "use", "--", "--output=json"]);
  expect(literal.code).toBe(4);
  expect(literal.stderr).toStartWith("✖");
});

test("cli: bare dh -o json prints one JSON value", async () => {
  const { run, configDir } = await setup();
  const none = await run(["-o", "json"]);
  expect(none.code).toBe(0);
  expect(JSON.parse(none.stdout).default).toBeNull();

  await seed(configDir);
  const some = JSON.parse((await run(["-o", "json"])).stdout);
  expect(some.default).toEqual({
    profile: "a",
    server: "http://localhost:10000",
    user: "psk",
  });
});

test("cli: a profile named like a flag, after --", async () => {
  const { run, configDir } = await setup();
  await seed(configDir);
  expect((await run(["auth", "rename", "--", "b", "--all"])).code).toBe(0);
  const out = await run(["auth", "logout", "-o", "json", "--", "--all"]);
  expect(out.code, out.stderr).toBe(0);
  expect(JSON.parse(out.stdout).removed).toEqual([
    { profile: "--all", server: "http://localhost:10001", revoked: false },
  ]);
});

test("cli: migrate refuses a remote plain-HTTP server even when it can't sign in yet", async () => {
  const { run, tmp } = await setup();
  const legacy = join(tmp, "v1.json");
  await writeFile(
    legacy,
    JSON.stringify({
      community: {
        sessions: {
          remote: {
            host: "dhc.example.com",
            port: 10000,
            auth_type: "PSK",
            auth_token_env_var: "UNSET_TEST_PSK",
          },
        },
      },
    }),
  );
  const res = await run([
    "auth",
    "migrate",
    "--from",
    legacy,
    "--yes",
    "-o",
    "json",
  ]);
  expect(JSON.parse(res.stdout)).toEqual({ migrated: 0, failed: 1 });
  expect(res.stderr).toContain("Refusing to load client code over plain HTTP");

  // Login's first-run offer reports the failed migration instead of moving on.
  const login = await run(
    ["auth", "login", "--yes", "--no-prompt", "-o", "json"],
    { DH_MCP_CONFIG_FILE: legacy },
  );
  expect(login.code, login.stderr).toBe(0);
  expect(JSON.parse(login.stdout)).toEqual({ migrated: 0, failed: 1 });
});

test("cli: agents summarizes, and lists every command and error code as JSON", async () => {
  const { run } = await setup();
  const summary = await run(["agents"]);
  expect(summary.code).toBe(0);
  expect(summary.stdout).toContain(`${BIN} auth login [server]`);
  expect(summary.stdout).toContain("auth_required");

  const res = await run(["agents", "-o", "json"]);
  expect(res.code).toBe(0);
  const manifest = JSON.parse(res.stdout);
  const auth = manifest.commands.find(
    (c: { command: string }) => c.command === `${BIN} auth`,
  );
  expect(auth.commands.map((c: { command: string }) => c.command)).toContain(
    `${BIN} auth login`,
  );
  const codes = manifest.errors.map((e: { code: string }) => e.code);
  for (const code of [
    "auth_required",
    "auth_failed",
    "auth_expired",
    "credential_unavailable",
    "key_upload_failed",
    "server_unreachable",
  ]) {
    expect(codes).toContain(code);
  }
});

test("cli: use, rename, and logout without a server", async () => {
  const { run, configDir } = await setup();
  await seed(configDir);

  expect((await run(["auth", "use", "b"])).code).toBe(0);
  expect((await run(["auth", "rename", "b", "anon"])).code).toBe(0);
  const list = JSON.parse((await run(["auth", "-o", "json"])).stdout);
  expect(list.find((p: { default: boolean }) => p.default).name).toBe("anon");

  const missing = await run(["auth", "use", "nope"]);
  expect(missing.code).toBe(2);

  const out = await run(["auth", "logout", "anon", "-o", "json"]);
  expect(out.code, out.stderr).toBe(0);
  expect(JSON.parse(out.stdout).default).toBe("a");
  const creds = JSON.parse(
    await readFile(join(configDir, "credentials.json"), "utf8"),
  );
  expect(Object.keys(creds.credentials)).toEqual(["p_a"]);

  // One profile left: no picker needed. --no-input is the hidden older name.
  const many = await run(["auth", "logout", "--no-input"]);
  expect(many.code).toBe(0);
});

test("cli: use :<user> picks that user on the default's server", async () => {
  const { run, configDir } = await setup();
  await seed(configDir, {
    profiles: {
      p_c: {
        name: "c",
        server: "http://localhost:10000",
        user: "alice",
        method: "basic",
        createdAt: "",
      },
    },
  });
  const use = await run(["auth", "use", ":alice", "-o", "json"]);
  expect(use.code, use.stderr).toBe(0);
  expect(JSON.parse(use.stdout).default).toBe("c");
  // b is anonymous, but on another server.
  const other = await run(["auth", "use", ":anonymous", "-o", "json"]);
  expect(other.code).toBe(2);
  expect(JSON.parse(other.stderr).error).toContain("No profile for anonymous");
});

test("cli: a key logout can't delete is remembered for the next sign-in", async () => {
  const { run, configDir } = await setup();
  const closed = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: { data() {} },
  });
  const server = `http://127.0.0.1:${closed.port}`;
  closed.stop(true);
  await seed(configDir, {
    servers: { [server]: { kind: "enterprise" } },
    profiles: {
      p_e: {
        name: "e",
        server,
        user: "alice",
        method: "saml",
        createdAt: "",
      },
    },
    credentials: {
      p_e: {
        type: "keyPair",
        keyType: "ec",
        publicKey: "PUB",
        privateKey: "PRIV",
        generated: true,
      },
    },
  });
  const out = await run(["auth", "logout", "e", "-o", "json"], {
    DH_TIMEOUT: "2",
  });
  expect(out.code, out.stderr).toBe(0);
  expect(out.stderr).toContain("deleted the next time you sign in");
  const creds = JSON.parse(
    await readFile(join(configDir, "credentials.json"), "utf8"),
  );
  expect(creds.credentials.p_e).toBeUndefined();
  expect(creds.orphans).toEqual([
    { server, user: "alice", keyType: "ec", publicKey: "PUB" },
  ]);
});

test("cli: a profile whose env var is unset", async () => {
  const { run, configDir } = await setup();
  await seed(configDir);
  const res = await run(["auth", "status", "-o", "json"]);
  expect(res.code).toBe(4);
  const error = JSON.parse(res.stderr);
  expect(error.code).toBe("credential_unavailable");
  expect(error.error).toContain("UNSET_TEST_PSK");
});

test("cli: environment-only auth", async () => {
  const { run, configDir } = await setup();
  const both = await run(["auth", "status"], {
    DH_SERVER: "localhost:10000",
    DH_PSK: "a",
    DH_PASSWORD: "b",
  });
  expect(both.code).toBe(2);

  const closed = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: { data() {} },
  });
  const port = closed.port;
  closed.stop(true);
  const unreachable = await run(["auth", "status", "-o", "json"], {
    DH_SERVER: `127.0.0.1:${port}`,
  });
  expect(unreachable.code).toBe(5);
  expect(JSON.parse(unreachable.stderr).code).toBe("server_unreachable");
  // Environment-only auth must not touch the config directory.
  expect(await stat(configDir).catch(() => null)).toBeNull();
});

test("cli: reaches an HTTP/2-only server, also after the --ca-cert re-run", async () => {
  const { run } = await setup();
  const server = await http2OnlyEnterprise();
  try {
    const res = await run([
      "auth",
      "login",
      `https://localhost:${server.port}`,
      "--ca-cert",
      TEST_CA,
      "--no-prompt",
      "-o",
      "json",
    ]);
    expect(res.stderr).toContain("(Deephaven Enterprise)");
    // The fake has no client library to load, so login stops there.
    const error = JSON.parse(res.stderr.trim().split("\n").pop() ?? "");
    expect(error.code).toBe("server_unsupported");
  } finally {
    await server.stop(true);
  }
});
