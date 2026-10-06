import { assert, assertEquals, assertStringIncludes } from "@std/assert";
import { fromFileUrl, join } from "@std/path";
import { BIN } from "../src/version.ts";

const MAIN = fromFileUrl(new URL("../src/main.ts", import.meta.url));

interface Run {
  code: number;
  stdout: string;
  stderr: string;
}

async function setup() {
  const tmp = await Deno.makeTempDir({ prefix: "dh-cli-test-" });
  return {
    tmp,
    configDir: join(tmp, "cli"),
    run: async (
      args: string[],
      env: Record<string, string> = {},
      stdin?: string,
    ): Promise<Run> => {
      const child = new Deno.Command(Deno.execPath(), {
        args: ["run", "-A", MAIN, ...args],
        env: {
          DH_CONFIG_DIR: join(tmp, "cli"),
          DH_AI_DATA_DIR: join(tmp, "legacy"),
          DH_AUTO_UPDATE: "off",
          DH_PROFILE: "",
          DH_SERVER: "",
          ...env,
        },
        stdin: stdin === undefined ? "null" : "piped",
        stdout: "piped",
        stderr: "piped",
      }).spawn();
      if (stdin !== undefined) {
        const w = child.stdin.getWriter();
        await w.write(new TextEncoder().encode(stdin));
        await w.close();
      }
      const out = await child.output();
      const decode = (b: Uint8Array) => new TextDecoder().decode(b);
      return {
        code: out.code,
        stdout: decode(out.stdout),
        stderr: decode(out.stderr),
      };
    },
  };
}

/** Writes a config with two Community profiles that need no network. */
async function seed(configDir: string) {
  await Deno.mkdir(configDir, { recursive: true });
  await Deno.writeTextFile(
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
  await Deno.writeTextFile(
    join(configDir, "credentials.json"),
    JSON.stringify({
      version: 1,
      credentials: { p_a: { type: "envRef", env: "UNSET_TEST_PSK" } },
    }),
    { mode: 0o600 },
  );
  if (Deno.build.os !== "windows") {
    await Deno.chmod(join(configDir, "credentials.json"), 0o600);
  }
}

Deno.test("cli: no profiles", async () => {
  const { run } = await setup();
  const list = await run(["auth", "-o", "json"]);
  assertEquals(list.code, 0);
  assertEquals(JSON.parse(list.stdout), []);

  const status = await run(["auth", "status", "-o", "json"]);
  assertEquals(status.code, 4);
  assertEquals(JSON.parse(status.stderr).code, "auth_required");

  const bare = await run([]);
  assertEquals(bare.code, 0);
  assertStringIncludes(bare.stdout, `Run \`${BIN} auth login\``);
});

Deno.test("cli: usage errors exit 2, with JSON on stderr", async () => {
  const { run } = await setup();
  const login = await run(["auth", "login", "--no-input", "-o", "json"]);
  assertEquals(login.code, 2);
  const error = JSON.parse(login.stderr);
  assertEquals(error.code, "usage");
  assertStringIncludes(error.error, "<server>");
  assertEquals(login.stdout, "");

  const unknown = await run(["auth", "bogus"]);
  assertEquals(unknown.code, 2);
});

Deno.test("cli: agents lists commands and every error code", async () => {
  const { run } = await setup();
  const res = await run(["agents"]);
  assertEquals(res.code, 0);
  const manifest = JSON.parse(res.stdout);
  const auth = manifest.commands.find((c: { command: string }) =>
    c.command === `${BIN} auth`
  );
  assert(
    auth.commands.some((c: { command: string }) =>
      c.command === `${BIN} auth login`
    ),
  );
  const codes = manifest.errors.map((e: { code: string }) => e.code);
  for (
    const code of [
      "auth_required",
      "auth_failed",
      "auth_expired",
      "credential_unavailable",
      "key_upload_failed",
      "server_unreachable",
    ]
  ) {
    assert(codes.includes(code), code);
  }
});

Deno.test("cli: use, rename, and logout without a server", async () => {
  const { run, configDir } = await setup();
  await seed(configDir);

  assertEquals((await run(["auth", "use", "b"])).code, 0);
  assertEquals((await run(["auth", "rename", "b", "anon"])).code, 0);
  const list = JSON.parse((await run(["auth", "-o", "json"])).stdout);
  assertEquals(list.find((p: { default: boolean }) => p.default).name, "anon");

  const missing = await run(["auth", "use", "nope"]);
  assertEquals(missing.code, 2);

  const out = await run(["auth", "logout", "anon", "-o", "json"]);
  assertEquals(out.code, 0, out.stderr);
  assertEquals(JSON.parse(out.stdout).default, "a");
  const creds = JSON.parse(
    await Deno.readTextFile(join(configDir, "credentials.json")),
  );
  assertEquals(Object.keys(creds.credentials), ["p_a"]);

  const many = await run(["auth", "logout", "--no-input"]);
  assertEquals(many.code, 0, "one profile left: no picker needed");
});

Deno.test("cli: a profile whose env var is unset", async () => {
  const { run, configDir } = await setup();
  await seed(configDir);
  const res = await run(["auth", "status", "-o", "json"]);
  assertEquals(res.code, 4);
  const error = JSON.parse(res.stderr);
  assertEquals(error.code, "credential_unavailable");
  assertStringIncludes(error.error, "UNSET_TEST_PSK");
});

Deno.test("cli: environment-only auth", async () => {
  const { run, configDir } = await setup();
  const both = await run(["auth", "status"], {
    DH_SERVER: "localhost:10000",
    DH_PSK: "a",
    DH_PASSWORD: "b",
  });
  assertEquals(both.code, 2);

  const closed = Deno.listen({ hostname: "127.0.0.1", port: 0 });
  const port = (closed.addr as Deno.NetAddr).port;
  closed.close();
  const unreachable = await run(["auth", "status", "-o", "json"], {
    DH_SERVER: `127.0.0.1:${port}`,
  });
  assertEquals(unreachable.code, 5);
  assertEquals(JSON.parse(unreachable.stderr).code, "server_unreachable");
  const exists = await Deno.stat(configDir).then(() => true, () => false);
  assert(!exists, "environment-only auth touched the config directory");
});
