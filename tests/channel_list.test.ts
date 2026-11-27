import { afterAll, describe, expect, test } from "bun:test";
import { rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { run, tempDir } from "./helpers.ts";

const SRC = resolve(import.meta.dir, "..", "src");

/** A channel manifest's stubbed answer: a version, a 404, or a network failure. */
type Answer = string | 404 | "fail";

/**
 * Runs `dh <args>` from source in a child process as an installed `channel`
 * build at a temp executable path, with every `cli-channel-<name>` manifest
 * fetch answered from `answers` (unlisted channels 404). Any other URL, such
 * as `DH_UPDATE_URL`, answers version 9.9.9.
 */
async function dh(
  args: string[],
  {
    build = "release",
    followed,
    answers = {},
    env = {},
  }: {
    build?: string;
    followed?: string;
    answers?: Record<string, Answer>;
    env?: Record<string, string>;
  } = {},
) {
  const tmp = await tempDir("dh-channel-list-");
  dirs.push(tmp);
  const exe = join(tmp, "dh");
  if (followed) {
    await writeFile(`${exe}.meta`, JSON.stringify({ channel: followed }));
  }
  const probe = `
    import { mock } from "bun:test";
    Object.defineProperty(process, "execPath", { value: process.env.DH_TEST_EXEC_PATH });
    mock.module(${JSON.stringify(join(SRC, "version.ts"))}, () => ({
      BIN: "dh",
      CHANNEL: ${JSON.stringify(build)},
      REPOSITORY: "example/repo",
      STANDALONE: true,
      VERSION: "3.1.0",
    }));
    const answers = ${JSON.stringify(answers)};
    globalThis.fetch = async (url) => {
      const name = /cli-channel-([^/]+)\\/manifest\\.json$/.exec(String(url))?.[1];
      const answer = name === undefined ? "9.9.9" : (answers[name] ?? 404);
      if (answer === "fail") throw new TypeError("fetch failed");
      if (answer === 404) return new Response(null, { status: 404 });
      return Response.json({ version: answer, binaries: {} });
    };
    await import(${JSON.stringify(join(SRC, "main.ts"))});
  `;
  // A file, not \`bun -e\`: commander reads arguments differently under -e.
  const script = join(tmp, "probe.ts");
  await writeFile(script, probe);
  return run([process.execPath, "--no-env-file", script, ...args], {
    DH_TEST_EXEC_PATH: exe,
    DH_CONFIG_DIR: join(tmp, "config"),
    DH_AUTO_UPDATE: "",
    DH_UPDATE_INTERVAL: "0",
    DH_UPDATE_URL: undefined,
    ...env,
  });
}

const dirs: string[] = [];
afterAll(async () => {
  await Promise.all(dirs.map((d) => rm(d, { recursive: true, force: true })));
});

const PUBLISHED = { release: "3.1.1", rc: "3.2.0-rc.3" };

describe("[channels-cli-2] dh channel", () => {
  test("lists release and rc, marking the followed one", async () => {
    const r = await dh(["channel"], { answers: PUBLISHED });
    expect(r.code, r.stderr).toBe(0);
    expect(r.stdout).toBe("● release   3.1.1\n  rc        3.2.0-rc.3\n");

    const rc = await dh(["channel"], {
      build: "rc",
      followed: "rc",
      answers: PUBLISHED,
    });
    expect(rc.stdout).toBe("  release   3.1.1\n● rc        3.2.0-rc.3\n");
  });

  test("adds the followed channel only when it is neither release nor rc", async () => {
    const r = await dh(["channel"], {
      build: "alpha-bar",
      followed: "alpha-bar",
      answers: {
        ...PUBLISHED,
        "alpha-bar": "3.2.1-alpha-bar.0",
        other: "1.0.0",
      },
    });
    expect(r.code, r.stderr).toBe(0);
    expect(r.stdout).toBe(
      "  release     3.1.1\n  rc          3.2.0-rc.3\n● alpha-bar   3.2.1-alpha-bar.0\n",
    );
  });

  test("not published, unreachable, and still exits 0", async () => {
    const r = await dh(["channel"], { answers: { release: "fail" } });
    expect(r.code, r.stderr).toBe(0);
    expect(r.stdout).toBe(
      "● release   ?\n  rc        not published\n! Couldn't check release.\n",
    );
    const json = await dh(["channel", "-o", "json"], {
      answers: { release: "fail" },
    });
    expect(JSON.parse(json.stdout).channels).toEqual([
      { channel: "release", version: null, following: true },
      { channel: "rc", version: null, following: false },
    ]);
  });

  test("DH_UPDATE_URL doesn't change the listed versions", async () => {
    const r = await dh(["channel"], {
      answers: PUBLISHED,
      env: { DH_UPDATE_URL: "https://updates.example.invalid/manifest.json" },
    });
    expect(r.stdout).toBe("● release   3.1.1\n  rc        3.2.0-rc.3\n");
  });

  test("JSON lists the rows and the running build", async () => {
    const r = await dh(["channel", "-o", "json"], {
      followed: "rc",
      answers: PUBLISHED,
    });
    expect(JSON.parse(r.stdout)).toEqual({
      channels: [
        { channel: "release", version: "3.1.1", following: false },
        { channel: "rc", version: "3.2.0-rc.3", following: true },
      ],
      build: "release",
      version: "3.1.0",
    });
  });

  test("warns when the running build is from another channel", async () => {
    const r = await dh(["channel"], { followed: "rc", answers: PUBLISHED });
    expect(r.stdout).toBe(
      "  release   3.1.1\n● rc        3.2.0-rc.3\n! Running 3.1.0 from the release channel, not rc.\n  Run `dh channel use rc` to install rc's build now.\n",
    );
    const off = await dh(["channel"], {
      followed: "rc",
      answers: PUBLISHED,
      env: { DH_AUTO_UPDATE: "off" },
    });
    expect(off.stdout).toContain("not rc; auto-update is off.");
  });
});
