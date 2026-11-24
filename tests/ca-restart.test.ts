import { expect, test } from "bun:test";
import { writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { RUNTIME_FLAGS } from "../scripts/build.ts";
import { CLI, run, TEST_CA, tempDir } from "./helpers.ts";

const fixture = resolve(import.meta.dir, "fixtures/ca-restart.ts");

test("CA restart replays consumed stdin and preserves replacement output format", async () => {
  for (const [mode, input] of [
    ["text", "first line\nsecond line\n"],
    ["text", ""],
    ["secret", "secret value\n"],
    ["untouched", "not read until the child\n"],
  ]) {
    const output = await run(
      [process.execPath, ...RUNTIME_FLAGS, fixture, "original"],
      {
        TEST_RESTART_CA: TEST_CA,
        TEST_RESTART_READ: mode,
        TEST_RESTART_REPLACE: "1",
        DH_LOADED_CA_CERT: undefined,
        NODE_EXTRA_CA_CERTS: undefined,
      },
      input,
    );
    expect(output.code).toBe(0);
    expect(output.stderr).toBe("");
    expect(JSON.parse(output.stdout)).toEqual({
      args: ["replacement", "--output", "silent", "--", "-target"],
      input,
    });
  }
});

test("CA restart keeps original arguments when not replacing the command", async () => {
  const output = await run(
    [process.execPath, ...RUNTIME_FLAGS, fixture, "original", "-o", "json"],
    {
      TEST_RESTART_CA: TEST_CA,
      TEST_RESTART_READ: "text",
      TEST_RESTART_REPLACE: undefined,
      DH_LOADED_CA_CERT: undefined,
      NODE_EXTRA_CA_CERTS: undefined,
    },
    "print('hello')\n",
  );
  expect(output.code).toBe(0);
  expect(JSON.parse(output.stdout)).toEqual({
    args: ["original", "-o", "json"],
    input: "print('hello')\n",
  });
});

test("CLI exec retains piped source across a saved-CA restart", async () => {
  const dir = await tempDir("dh-restart-exec-");
  await seedProfile(dir, "community");
  const output = await run(
    [...CLI, "exec", "-c", "-o", "json", "--no-prompt"],
    cliEnv(dir),
    "first_line = 1\nsecond_line = 2\n",
  );
  expect(output.code).not.toBe(0);
  expect(output.stdout).toBe("");
  const error = JSON.parse(output.stderr);
  expect(error.code).not.toBe("usage");
  expect(error.error).not.toContain("stdin");
});

test("CLI logout retains silent mode across a replacement CA restart", async () => {
  const dir = await tempDir("dh-restart-logout-");
  await seedProfile(dir, "enterprise");
  const output = await run(
    [...CLI, "auth", "logout", "test", "--no-prompt", "-o", "silent"],
    cliEnv(dir),
  );
  expect(output.code).toBe(0);
  expect(output.stdout).toBe("");
  expect(output.stderr).toContain("key");
  const config = await Bun.file(join(dir, "config.json")).json();
  expect(config.profiles).toEqual({});
});

/** Save a profile whose CA requires the real CLI to restart before connecting. */
async function seedProfile(dir: string, kind: "community" | "enterprise") {
  const server = "http://127.0.0.1:1";
  await writeFile(
    join(dir, "config.json"),
    JSON.stringify({
      version: 1,
      defaultProfile: "test",
      servers: { [server]: { kind, caCert: TEST_CA } },
      profiles: {
        test: {
          name: "test",
          server,
          user: "test",
          method: kind === "community" ? "anonymous" : "private-key",
          createdAt: "",
        },
      },
    }),
  );
  const credential =
    kind === "community"
      ? { type: "none" }
      : {
          type: "keyPair",
          keyType: "RSA",
          publicKey: "test-public-key",
          privateKey: "test-private-key",
          generated: true,
        };
  await writeFile(
    join(dir, "credentials.json"),
    JSON.stringify({ version: 1, credentials: { test: credential } }),
    { mode: 0o600 },
  );
}

/** Isolate subprocesses from the caller's auth and TLS environment. */
function cliEnv(dir: string): Record<string, string | undefined> {
  return {
    DH_CONFIG_DIR: dir,
    DH_AUTO_UPDATE: "off",
    DH_TIMEOUT: "1",
    DH_PROFILE: undefined,
    DH_SERVER: undefined,
    DH_LOADED_CA_CERT: undefined,
    NODE_EXTRA_CA_CERTS: undefined,
  };
}
