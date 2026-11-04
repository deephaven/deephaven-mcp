import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { CLI, run as spawn, tempDir } from "./helpers.ts";

// Against real Community servers, e.g.
//   deephaven server --port 10123 --jvm-args "-Dauthentication.psk=<psk>"
//   deephaven server --port 10124 --jvm-args "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"
// DH_TEST_DHC_PSK_URL=localhost:10123 DH_TEST_DHC_PSK=<psk> DH_TEST_DHC_ANON_URL=localhost:10124 bun test
const PSK_URL = process.env.DH_TEST_DHC_PSK_URL ?? "";
const PSK = process.env.DH_TEST_DHC_PSK ?? "";
const ANON_URL = process.env.DH_TEST_DHC_ANON_URL ?? "";

async function run(
  dir: string,
  args: string[],
  env: Record<string, string> = {},
) {
  const out = await spawn([...CLI, ...args, "-o", "json", "--no-prompt"], {
    DH_CONFIG_DIR: dir,
    DH_AI_DATA_DIR: join(dir, "none"),
    DH_AUTO_UPDATE: "off",
    DH_PROFILE: undefined,
    DH_SERVER: undefined,
    ...env,
  });
  return {
    code: out.code,
    json: out.stdout ? JSON.parse(out.stdout) : null,
    stderr: out.stderr,
  };
}

test.skipIf(!PSK_URL || !PSK)(
  "community: PSK login, status, wrong PSK, logout",
  async () => {
    const dir = join(await tempDir("dh-dhc-"), "cli");
    const env = { TEST_PSK: PSK };
    const login = await run(
      dir,
      ["auth", "login", PSK_URL, "--psk-env", "TEST_PSK"],
      env,
    );
    expect(login.code, login.stderr).toBe(0);
    expect(login.json.default).toBe(true);

    const status = await run(dir, ["auth", "status"], env);
    expect(status.code, status.stderr).toBe(0);
    expect(status.json.kind).toBe("community");

    const creds = await readFile(join(dir, "credentials.json"), "utf8");
    // The env var's name is saved, not the PSK.
    expect(creds).not.toContain(PSK);

    const wrong = await run(dir, ["auth", "status"], {
      TEST_PSK: `${PSK}-wrong`,
    });
    expect(wrong.code).toBe(4);
    expect(JSON.parse(wrong.stderr).code).toBe("auth_expired");

    const env2 = await run(dir, ["auth", "status"], {
      DH_SERVER: PSK_URL,
      DH_PSK: PSK,
    });
    expect(env2.code, env2.stderr).toBe(0);
    expect(env2.json.profile).toBeNull();

    const logout = await run(dir, ["auth", "logout", "--all"]);
    expect(logout.code, logout.stderr).toBe(0);
    expect(logout.json.default).toBeNull();
  },
  60_000,
);

test.skipIf(!ANON_URL)(
  "community: an anonymous-only server logs in without flags",
  async () => {
    const dir = join(await tempDir("dh-dhc-"), "cli");
    const login = await run(dir, ["auth", "login", ANON_URL]);
    expect(login.code, login.stderr).toBe(0);
    expect(login.json.user).toBe("anonymous");
  },
  60_000,
);
