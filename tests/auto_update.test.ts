import { expect, test } from "bun:test";
import {
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import { binaryName, build } from "../scripts/build.ts";
import { PLATFORM } from "../src/updater.ts";
import { BIN, VERSION } from "../src/version.ts";
import { http2OnlyEnterprise, TEST_CA } from "./helpers.ts";

const NEXT = "9.9.9";
// Every asset of this release redirects to plain HTTP (never resolves: .invalid).
const INSECURE_TAG = "cli-v0.0.0-insecure";
const WINDOWS = process.platform === "win32";
const installer = (name: string) => resolve(import.meta.dir, "..", name);
const INSTALL = WINDOWS
  ? {
      cmd: "powershell",
      args: [
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        installer("install.ps1"),
      ],
    }
  : { cmd: "sh", args: [installer("install.sh")] };

interface Result {
  success: boolean;
  stdout: string;
  stderr: string;
}

async function run(
  cmd: string,
  args: string[],
  env: Record<string, string | undefined>,
  cwd?: string,
): Promise<Result> {
  // Bun.spawn replaces the environment rather than extending it; undefined unsets.
  const merged = Object.entries({ ...process.env, ...env }).filter(
    (entry): entry is [string, string] => entry[1] !== undefined,
  );
  const proc = Bun.spawn([cmd, ...args], {
    env: Object.fromEntries(merged),
    cwd,
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { success: code === 0, stdout, stderr };
}

function ok(result: Result): Result {
  if (!result.success) throw new Error(result.stderr || result.stdout);
  return result;
}

const ENV_KEY =
  "$k = [Microsoft.Win32.Registry]::CurrentUser.CreateSubKey('Environment')";

/** The registry user PATH, unexpanded, with its value kind ("" if unset). */
async function userPath(): Promise<{ value: string; kind: string }> {
  const out = ok(
    await run(
      "powershell",
      [
        "-NoProfile",
        "-Command",
        `${ENV_KEY}
    $kind = if ($null -ne $k.GetValue('Path')) { "$($k.GetValueKind('Path'))" } else { '' }
    $value = [string]$k.GetValue('Path', '', 'DoNotExpandEnvironmentNames')
    @{ value = $value; kind = $kind } | ConvertTo-Json -Compress`,
      ],
      {},
    ),
  );
  return JSON.parse(out.stdout);
}

async function setUserPath(path: { value: string; kind: string }) {
  ok(
    await run(
      "powershell",
      [
        "-NoProfile",
        "-Command",
        `${ENV_KEY}
    if ($env:DH_TEST_PATH_KIND) {
      $k.SetValue('Path', [string]$env:DH_TEST_PATH_VALUE, $env:DH_TEST_PATH_KIND)
    } else { $k.DeleteValue('Path', $false) }`,
      ],
      { DH_TEST_PATH_VALUE: path.value, DH_TEST_PATH_KIND: path.kind },
    ),
  );
}

/** Serves `root` as GitHub release downloads; `requests` counts every request. */
function fakeGitHub(root: string, requests = { count: 0 }) {
  return Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      requests.count++;
      const url = new URL(req.url);
      const path = decodeURIComponent(url.pathname);
      if (path.startsWith(`/releases/download/${INSECURE_TAG}/`)) {
        return Response.redirect(`http://example.invalid${path}`);
      }
      const file = join(root, path);
      if (!file.startsWith(root + sep))
        return new Response(null, { status: 404 });
      const body = Bun.file(file);
      return (await body.exists())
        ? new Response(body)
        : new Response(null, { status: 404 });
    },
  });
}

test("auto-update uses the default URL when DH_UPDATE_URL is unset or empty", async () => {
  const probe = `
    import { mock } from "bun:test";
    Object.defineProperty(process, "execPath", {
      value: process.env.DH_TEST_EXEC_PATH,
    });
    mock.module("./src/version.ts", () => ({
      BIN: "dh",
      REPOSITORY: "example/repo",
      STANDALONE: true,
      VERSION: "3.0.0",
    }));
    globalThis.fetch = async (url) => {
      console.log(String(url));
      return Response.json({ version: "3.0.0", binaries: {} });
    };
    const { autoUpdate } = await import("./src/updater.ts");
    await autoUpdate();
  `;
  const tmp = await mkdtemp(join(tmpdir(), "dh-update-probe-"));
  const cwd = resolve(import.meta.dir, "..");
  const env = {
    DH_AUTO_UPDATE: "",
    DH_TEST_EXEC_PATH: join(tmp, "dh"),
    DH_UPDATE_INTERVAL: "0",
  };
  const defaultUrl =
    "https://github.com/example/repo/releases/latest/download/manifest.json";
  const overrideUrl = "https://updates.example.invalid/manifest.json";

  const cases: [string | undefined, string][] = [
    [undefined, defaultUrl],
    ["", defaultUrl],
    [overrideUrl, overrideUrl],
  ];
  try {
    for (const [value, expected] of cases) {
      const result = await run(
        process.execPath,
        ["--no-env-file", "-e", probe],
        { ...env, DH_UPDATE_URL: value },
        cwd,
      );
      expect(result.success, result.stderr).toBe(true);
      expect(result.stdout.trim()).toBe(expected);
    }

    const invalid = await run(
      process.execPath,
      ["--no-env-file", "-e", probe],
      { ...env, DH_UPDATE_URL: "   " },
      cwd,
    );
    expect(invalid.success).toBe(false);
    expect(invalid.stderr).toContain("Invalid URL");
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
});

/**
 * A fake GitHub (see {@link fakeGitHub}) in a temp dir, with helpers to build
 * releases into it and point channel releases at them, as publish.ts does.
 */
async function releaseServer() {
  const tmp = await mkdtemp(join(tmpdir(), "dh-update-test-"));
  const requests = { count: 0 };
  const server = fakeGitHub(tmp, requests);
  const repo = `http://127.0.0.1:${server.port}`;
  const release = (tag: string) => join(tmp, "releases", "download", tag);
  const baseUrl = (tag: string) => `${repo}/releases/download/${tag}/`;
  return {
    tmp,
    requests,
    repo,
    release,
    baseUrl,
    buildRelease: (tag: string, version: string, channel = "release") =>
      build(release(tag), { version, channel, baseUrl: baseUrl(tag) }),
    /** Points `channel`'s channel release at `tag`. */
    publishChannel: async (tag: string, channel = "release") => {
      await mkdir(release(`cli-channel-${channel}`), { recursive: true });
      await copyFile(
        join(release(tag), "manifest.json"),
        join(release(`cli-channel-${channel}`), "manifest.json"),
      );
    },
    /** The install metadata file the install scripts wrote into `dir`. */
    metaOf: async (dir: string) =>
      JSON.parse(
        await readFile(
          join(dir, WINDOWS ? `${BIN}.exe.meta` : `${BIN}.meta`),
          "utf8",
        ),
      ),
    stop: async () => {
      await server.stop(true);
      await rm(tmp, { recursive: true, force: true });
    },
  };
}

test("[channels-pub-6] the install scripts install on a channel", async () => {
  const gh = await releaseServer();
  const { tmp, requests, repo, buildRelease, publishChannel, metaOf } = gh;
  const tag = `cli-v${VERSION}`;
  const installEnv = {
    DH_INSTALL_NO_MODIFY_PATH: "1",
    DH_INSTALL_REPO_URL: repo,
  };
  try {
    await buildRelease(tag, VERSION);
    await publishChannel(tag);
    await buildRelease(`cli-v${NEXT}`, NEXT);

    // release by default, saved in the metadata file.
    const bin = join(tmp, "bin");
    const install = ok(
      await run(INSTALL.cmd, INSTALL.args, {
        ...installEnv,
        DH_INSTALL_DIR: bin,
      }),
    );
    expect(install.stdout).toContain(`Installed ${BIN} v${VERSION}`);
    expect(await metaOf(bin)).toEqual({ channel: "release" });

    // Another channel: its own manifest, saved in dh.meta.
    const rcVersion = `${VERSION}-rc.0`;
    await buildRelease(`cli-v${rcVersion}`, rcVersion, "rc");
    await publishChannel(`cli-v${rcVersion}`, "rc");
    const rcBin = join(tmp, "bin-rc");
    const rcInstall = ok(
      await run(INSTALL.cmd, INSTALL.args, {
        ...installEnv,
        DH_INSTALL_DIR: rcBin,
        DH_CHANNEL: "rc",
      }),
    );
    expect(rcInstall.stdout).toContain(rcVersion);
    expect(await metaOf(rcBin)).toEqual({ channel: "rc" });

    const unpublished = await run(INSTALL.cmd, INSTALL.args, {
      ...installEnv,
      DH_INSTALL_DIR: join(tmp, "bin-nope"),
      DH_CHANNEL: "nope",
    });
    expect(unpublished.success).toBe(false);
    expect(unpublished.stderr).toContain("no channel nope");

    for (const channel of ["dev", "Rc"]) {
      const before = requests.count;
      const rejected = await run(INSTALL.cmd, INSTALL.args, {
        ...installEnv,
        DH_INSTALL_DIR: join(tmp, "bin-bad"),
        DH_CHANNEL: channel,
      });
      expect(rejected.success).toBe(false);
      expect(rejected.stderr.toLowerCase()).toMatch(/reserved|invalid channel/);
      expect(requests.count).toBe(before);
    }

    // A pinned version, in any spelling, pre-releases included, regardless
    // of the channel.
    for (const [pinned, version] of [
      [NEXT, NEXT],
      [`v${NEXT}`, NEXT],
      [`cli-v${NEXT}`, NEXT],
      [`cli-v${rcVersion}`, rcVersion],
    ]) {
      const dir = join(tmp, `bin-pinned-${pinned}`);
      const out = ok(
        await run(INSTALL.cmd, INSTALL.args, {
          ...installEnv,
          DH_INSTALL_DIR: dir,
          DH_INSTALL_VERSION: pinned,
        }),
      );
      expect(out.stdout).toContain(`Installed ${BIN} v${version}`);
      expect(await metaOf(dir)).toEqual({ channel: "release" });
    }
  } finally {
    await gh.stop();
  }
}, 180_000);

test("dh installed with the install script updates itself from GitHub releases", async () => {
  const gh = await releaseServer();
  const { tmp, repo, release, buildRelease, publishChannel } = gh;
  let tag = `cli-v${VERSION}`;
  const channelManifest = join(release("cli-channel-release"), "manifest.json");
  try {
    await buildRelease(tag, VERSION);
    await publishChannel(tag);

    const bin = join(tmp, "bin");
    const installEnv = {
      DH_INSTALL_DIR: bin,
      DH_INSTALL_NO_MODIFY_PATH: "1",
    };
    const insecure = await run(INSTALL.cmd, INSTALL.args, {
      ...installEnv,
      DH_INSTALL_REPO_URL: "http://example.com/dh",
    });
    expect(insecure.success).toBe(false);
    expect(insecure.stderr).toContain("insecure URL");

    const redirected = await run(INSTALL.cmd, INSTALL.args, {
      ...installEnv,
      DH_INSTALL_REPO_URL: repo,
      DH_INSTALL_VERSION: INSECURE_TAG,
    });
    expect(redirected.success).toBe(false);
    // curl's wording varies by version; both start with this.
    expect(redirected.stderr).toContain(
      WINDOWS ? "insecure URL" : 'Protocol "http"',
    );

    const install = ok(
      await run(INSTALL.cmd, INSTALL.args, {
        ...installEnv,
        DH_INSTALL_REPO_URL: repo,
      }),
    );
    expect(install.stdout).toContain(`Installed ${BIN} v${VERSION}`);

    if (!WINDOWS) {
      // A temp HOME/ZDOTDIR keeps this away from the real shell config.
      const home = join(tmp, "home");
      await mkdir(home);
      const pathEnv = {
        ...installEnv,
        DH_INSTALL_REPO_URL: repo,
        DH_INSTALL_NO_MODIFY_PATH: "",
        HOME: home,
        ZDOTDIR: home,
        SHELL: "/bin/zsh",
      };
      for (let i = 0; i < 2; i++) {
        ok(await run(INSTALL.cmd, INSTALL.args, pathEnv));
      }
      const rc = await readFile(join(home, ".zshrc"), "utf8");
      const line = `export PATH="${bin}:$PATH"`;
      expect(rc.split(line).length - 1).toBe(1);

      // ':' separates PATH entries, so such a directory gets the manual hint instead.
      const colonHome = join(tmp, "colon-home");
      await mkdir(colonHome);
      const colon = ok(
        await run(INSTALL.cmd, INSTALL.args, {
          ...pathEnv,
          DH_INSTALL_DIR: join(tmp, "with:colon"),
          HOME: colonHome,
          ZDOTDIR: colonHome,
        }),
      );
      expect(colon.stdout).toContain("to your PATH to run dh");
      expect(
        await stat(join(colonHome, ".zshrc")).catch(() => null),
      ).toBeNull();
    } else if (process.env.CI) {
      // This edits the real user PATH, so it only runs on throwaway CI runners.
      const original = await userPath();
      const seeded = [original.value, "%USERPROFILE%\\dh-test"]
        .filter(Boolean)
        .join(";");
      try {
        // Both registry kinds must survive: rewriting REG_SZ as REG_EXPAND_SZ changes %NAME% text.
        for (const kind of ["ExpandString", "String"]) {
          await setUserPath({ value: seeded, kind });
          for (let i = 0; i < 2; i++) {
            ok(
              await run(INSTALL.cmd, INSTALL.args, {
                ...installEnv,
                DH_INSTALL_REPO_URL: repo,
                DH_INSTALL_NO_MODIFY_PATH: "",
              }),
            );
          }
          expect(await userPath()).toEqual({ value: `${seeded};${bin}`, kind });
        }
      } finally {
        await setUserPath(original);
      }
    }

    const exe = join(bin, WINDOWS ? `${BIN}.exe` : BIN);
    const env = {
      DH_UPDATE_URL: `${repo}/releases/download/cli-channel-release/manifest.json`,
      DH_DEBUG: "1",
      DH_CONFIG_DIR: join(tmp, "config"),
    };
    const now = { ...env, DH_UPDATE_INTERVAL: "0" };
    const version = async () => (await run(exe, ["--version"], env)).stdout;

    expect(await version()).toContain(VERSION);

    // The compiled binary must offer HTTP/2: Enterprise's key service needs it.
    const h2 = await http2OnlyEnterprise();
    try {
      const found = await run(
        exe,
        ["auth", "login", `https://localhost:${h2.port}`, "--no-prompt"],
        { ...env, DH_AUTO_UPDATE: "off", NODE_EXTRA_CA_CERTS: TEST_CA },
      );
      expect(found.stdout).toContain("(Deephaven Enterprise)");
    } finally {
      await h2.stop(true);
    }

    // The manifest is briefly missing while publish.ts replaces it: the check
    // fails, but still starts the interval.
    await rm(channelManifest);
    const early = await run(exe, [], env);
    expect(early.stderr).toContain("auto-update failed");

    tag = `cli-v${NEXT}`;
    await buildRelease(tag, NEXT);
    await publishChannel(tag);

    const throttled = await run(exe, [], env);
    expect(throttled.stderr).not.toContain(`Updated ${BIN}`);
    expect(await version()).toContain(VERSION);

    await run(exe, [], { ...now, DH_AUTO_UPDATE: "off" });
    expect(await version()).toContain(VERSION);

    const asset = join(release(tag), binaryName(PLATFORM));
    const good = await readFile(asset);
    const corrupt = Buffer.from(good);
    corrupt.writeUInt8(corrupt.readUInt8(0) ^ 0xff, 0);
    await writeFile(asset, corrupt);
    const tampered = await run(exe, [], now);
    expect(tampered.stderr).toContain("Checksum mismatch");
    expect(await version()).toContain(VERSION);
    await writeFile(asset, good);

    const first = await run(exe, [], now);
    expect(first.stderr).toContain(`Updated ${BIN} ${VERSION} -> ${NEXT}`);
    expect(await version()).toContain(NEXT);

    const second = await run(exe, [], now);
    expect(second.stderr).not.toContain(`Updated ${BIN}`);
    if (WINDOWS) {
      // The previous binary is cleaned up on the next run.
      expect(await stat(`${exe}.old`).catch(() => null)).toBeNull();
    }

    const redirect = await run(exe, [], {
      ...now,
      DH_UPDATE_URL: `${repo}/releases/download/${INSECURE_TAG}/manifest.json`,
    });
    expect(redirect.stderr).toContain("insecure URL");
    expect(await version()).toContain(NEXT);

    // A repo dh runs inside must not configure it (Bun binaries autoload these by default).
    const hostile = join(tmp, "hostile-cwd");
    await mkdir(hostile);
    await writeFile(join(hostile, ".env"), "DH_DEBUG=1\n");
    await writeFile(join(hostile, "preload.ts"), 'console.error("PRELOADED");');
    await writeFile(
      join(hostile, "bunfig.toml"),
      'preload = ["./preload.ts"]\n',
    );
    // The update fails (insecure redirect); only a loaded .env would print that.
    const inRepo = await run(
      exe,
      [],
      {
        ...now,
        DH_UPDATE_URL: `${repo}/releases/download/${INSECURE_TAG}/manifest.json`,
        DH_DEBUG: undefined,
      },
      hostile,
    );
    expect(inRepo.stderr).not.toContain("PRELOADED");
    expect(inRepo.stderr).not.toContain("auto-update failed");
  } finally {
    await gh.stop();
  }
}, 180_000);
