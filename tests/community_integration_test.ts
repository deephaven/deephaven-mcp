import { assert, assertEquals } from "@std/assert";
import { fromFileUrl, join } from "@std/path";

// Against real Community servers, e.g.
//   deephaven server --port 10123 --jvm-args "-Dauthentication.psk=<psk>"
//   deephaven server --port 10124 --jvm-args "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"
// DH_TEST_DHC_PSK_URL=localhost:10123 DH_TEST_DHC_PSK=<psk> DH_TEST_DHC_ANON_URL=localhost:10124 deno task test
const PSK_URL = Deno.env.get("DH_TEST_DHC_PSK_URL");
const PSK = Deno.env.get("DH_TEST_DHC_PSK");
const ANON_URL = Deno.env.get("DH_TEST_DHC_ANON_URL");
const MAIN = fromFileUrl(new URL("../src/main.ts", import.meta.url));

async function run(
  dir: string,
  args: string[],
  env: Record<string, string> = {},
) {
  const out = await new Deno.Command(Deno.execPath(), {
    args: ["run", "-A", MAIN, ...args, "-o", "json", "--no-input"],
    env: {
      DH_CONFIG_DIR: dir,
      DH_AI_DATA_DIR: join(dir, "none"),
      DH_AUTO_UPDATE: "off",
      ...env,
    },
    stdin: "null",
  }).output();
  const text = (b: Uint8Array) => new TextDecoder().decode(b);
  return {
    code: out.code,
    json: text(out.stdout) ? JSON.parse(text(out.stdout)) : null,
    stderr: text(out.stderr),
  };
}

Deno.test({
  name: "community: PSK login, status, wrong PSK, logout",
  ignore: !PSK_URL || !PSK,
  async fn() {
    const dir = join(await Deno.makeTempDir(), "cli");
    const env = { TEST_PSK: PSK! };
    const login = await run(dir, [
      "auth",
      "login",
      PSK_URL!,
      "--psk-env",
      "TEST_PSK",
    ], env);
    assertEquals(login.code, 0, login.stderr);
    assertEquals(login.json.default, true);

    const status = await run(dir, ["auth", "status"], env);
    assertEquals(status.code, 0, status.stderr);
    assertEquals(status.json.kind, "community");

    const creds = await Deno.readTextFile(join(dir, "credentials.json"));
    assert(!creds.includes(PSK!), "the PSK was saved instead of its env var");

    const wrong = await run(dir, ["auth", "status"], {
      TEST_PSK: `${PSK}-wrong`,
    });
    assertEquals(wrong.code, 4);
    assertEquals(JSON.parse(wrong.stderr).code, "auth_expired");

    const env2 = await run(dir, ["auth", "status"], {
      DH_SERVER: PSK_URL!,
      DH_PSK: PSK!,
    });
    assertEquals(env2.code, 0, env2.stderr);
    assertEquals(env2.json.profile, null);

    const logout = await run(dir, ["auth", "logout", "--all"]);
    assertEquals(logout.code, 0, logout.stderr);
    assertEquals(logout.json.default, null);
  },
});

Deno.test({
  name: "community: an anonymous-only server logs in without flags",
  ignore: !ANON_URL,
  async fn() {
    const dir = join(await Deno.makeTempDir(), "cli");
    const login = await run(dir, ["auth", "login", ANON_URL!]);
    assertEquals(login.code, 0, login.stderr);
    assertEquals(login.json.user, "anonymous");
  },
});
