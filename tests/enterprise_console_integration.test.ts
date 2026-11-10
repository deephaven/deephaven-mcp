import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { CLI, run as spawn, tempDir } from "./helpers.ts";

// Opt in with an Enterprise system that allows password login and Python consoles:
// DH_TEST_DHE_URL, DH_TEST_DHE_USERNAME, and DH_TEST_DHE_PASSWORD.
const SERVER = process.env.DH_TEST_DHE_URL ?? "";
const USER = process.env.DH_TEST_DHE_USERNAME ?? "";
const PASSWORD = process.env.DH_TEST_DHE_PASSWORD ?? "";

async function run(
  dir: string,
  args: string[],
  env: Record<string, string> = {},
) {
  const result = await spawn([...CLI, ...args, "-o", "json", "--no-prompt"], {
    DH_CONFIG_DIR: dir,
    DH_AUTO_UPDATE: "off",
    DH_PROFILE: undefined,
    DH_SERVER: undefined,
    ...env,
  });
  return {
    code: result.code,
    json: result.stdout ? JSON.parse(result.stdout) : null,
    stderr: result.stderr,
  };
}

test.skipIf(!SERVER || !USER || !PASSWORD)(
  "enterprise: create, select, inspect, and delete CLI consoles",
  async () => {
    const dir = join(await tempDir("dh-dhe-console-"), "cli");
    const env = { DH_TEST_PASSWORD: PASSWORD };
    const login = await run(
      dir,
      [
        "auth",
        "login",
        SERVER,
        "--method",
        "password",
        "--username",
        USER,
        "--password-env",
        "DH_TEST_PASSWORD",
        "--default",
      ],
      env,
    );
    expect(login.code, login.stderr).toBe(0);

    let first: { id: string; shortcode: string } | undefined;
    let second: { id: string; shortcode: string } | undefined;
    try {
      first = await createConsole(dir, env);
      expect(first.id).toBeTruthy();
      expect(first.shortcode).toMatch(/^[a-z0-9]{6}$/);

      const selected = await run(dir, ["console", "use", first.shortcode], env);
      expect(selected.code, selected.stderr).toBe(0);
      const before = JSON.parse(
        await readFile(join(dir, "consoles.json"), "utf8"),
      ).selected;

      second = await createConsole(dir, env);
      const after = JSON.parse(
        await readFile(join(dir, "consoles.json"), "utf8"),
      ).selected;
      expect(after).toEqual(before);

      const list = await run(dir, ["console"], env);
      expect(list.code, list.stderr).toBe(0);
      expect(list.json.map((console: { id: string }) => console.id)).toEqual(
        expect.arrayContaining([first.id, second.id]),
      );

      const detail = await run(
        dir,
        ["console", "detail", second.shortcode],
        env,
      );
      expect(detail.code, detail.stderr).toBe(0);
      expect(detail.json).toMatchObject({
        id: second.id,
        shortcode: second.shortcode,
        state: "Running",
      });
    } finally {
      for (const console of [second, first]) {
        if (!console) continue;
        const deletion = await run(
          dir,
          ["console", "delete", console.shortcode, "--force"],
          env,
        );
        expect(deletion.code, deletion.stderr).toBe(0);
      }
    }
  },
  240_000,
);

async function createConsole(
  dir: string,
  env: Record<string, string>,
): Promise<{ id: string; shortcode: string }> {
  const result = await run(dir, ["console", "create", "--no-use"], env);
  expect(result.code, result.stderr).toBe(0);
  expect(result.json.selected).toBe(false);
  return result.json;
}
