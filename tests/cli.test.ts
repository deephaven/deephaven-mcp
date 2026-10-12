import { expect, test } from "bun:test";
import { chmod, mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { BIN } from "../src/version.ts";
import { MAIN, run, tempDir, WINDOWS } from "./helpers.ts";

async function setup() {
  const tmp = await tempDir("dh-cli-test-");
  return {
    tmp,
    configDir: join(tmp, "cli"),
    run: (args: string[], env: Record<string, string> = {}, stdin?: string) =>
      run(
        [process.execPath, MAIN, ...args],
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

/** Writes a config with two Community profiles that need no network. */
async function seed(configDir: string) {
  await mkdir(configDir, { recursive: true });
  await writeFile(
    join(configDir, "config.json"),
    JSON.stringify({
      version: 1,
      defaultProfile: "p_a",
      servers: {
        "http://localhost:10000": { kind: "community" },
        "http://localhost:10001": { kind: "community" },
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
      },
    }),
  );
  await writeFile(
    join(configDir, "credentials.json"),
    JSON.stringify({
      version: 1,
      credentials: { p_a: { type: "envRef", env: "UNSET_TEST_PSK" } },
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
  const login = await run(["auth", "login", "--no-input", "-o", "json"]);
  expect(login.code).toBe(2);
  const error = JSON.parse(login.stderr);
  expect(error.code).toBe("usage");
  expect(error.error).toContain("<server>");
  expect(login.stdout).toBe("");

  const unknown = await run(["auth", "bogus"]);
  expect(unknown.code).toBe(2);
});

test("cli: agents lists commands and every error code", async () => {
  const { run } = await setup();
  const res = await run(["agents"]);
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

  // One profile left: no picker needed.
  const many = await run(["auth", "logout", "--no-input"]);
  expect(many.code).toBe(0);
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
