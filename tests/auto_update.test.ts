import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
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
import { withFileLock } from "../src/actions/auth/lock.ts";
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

/**
 * Runs autoUpdate() in a child process as a compiled `channel` build at
 * `execPath`, printing each URL it fetches. By default every fetch returns a
 * manifest for this version, so nothing installs; `fetchBody` replaces the
 * fetch function's body (it gets `url`).
 */
async function probeUpdate(
  execPath: string,
  env: Record<string, string | undefined>,
  {
    channel = "release",
    version = "3.0.0",
    fetchBody = `return Response.json({ version: "3.0.0", binaries: {} });`,
  } = {},
): Promise<Result> {
  const probe = `
    import { mock } from "bun:test";
    Object.defineProperty(process, "execPath", {
      value: process.env.DH_TEST_EXEC_PATH,
    });
    mock.module("./src/version.ts", () => ({
      BIN: "dh",
      CHANNEL: ${JSON.stringify(channel)},
      REPOSITORY: "example/repo",
      STANDALONE: true,
      VERSION: ${JSON.stringify(version)},
    }));
    globalThis.fetch = async (url) => {
      console.log(String(url));
      ${fetchBody}
    };
    const { autoUpdate } = await import("./src/updater.ts");
    await autoUpdate();
  `;
  return run(
    process.execPath,
    ["--no-env-file", "-e", probe],
    {
      DH_AUTO_UPDATE: "",
      DH_UPDATE_INTERVAL: "0",
      DH_UPDATE_URL: undefined,
      ...env,
      DH_TEST_EXEC_PATH: execPath,
    },
    resolve(import.meta.dir, ".."),
  );
}

const channelUrl = (channel: string) =>
  `https://github.com/example/repo/releases/download/cli-channel-${channel}/manifest.json`;

test("[channels-cli-1] auto-update fetches the followed channel's manifest, or DH_UPDATE_URL", async () => {
  const tmp = await mkdtemp(join(tmpdir(), "dh-update-probe-"));
  const exe = join(tmp, "dh");
  const meta = `${exe}.meta`;
  const overrideUrl = "https://updates.example.invalid/manifest.json";
  try {
    const cases: [string | undefined, string][] = [
      [undefined, channelUrl("release")],
      ["", channelUrl("release")],
      [overrideUrl, overrideUrl],
    ];
    for (const [value, expected] of cases) {
      const result = await probeUpdate(exe, { DH_UPDATE_URL: value });
      expect(result.success, result.stderr).toBe(true);
      expect(result.stdout.trim()).toBe(expected);
    }

    const invalid = await probeUpdate(exe, { DH_UPDATE_URL: "   " });
    expect(invalid.success).toBe(false);
    expect(invalid.stderr).toContain("Invalid URL");

    await writeFile(meta, JSON.stringify({ channel: "rc", extra: 1 }));
    const rc = await probeUpdate(exe, {});
    expect(rc.stdout.trim()).toBe(channelUrl("rc"));
    const kept = JSON.parse(await readFile(meta, "utf8"));
    expect(kept.channel).toBe("rc");
    expect(kept.extra).toBe(1);
    expect(Date.parse(kept.lastUpdateCheck)).toBeGreaterThan(0);

    // Throttled by lastUpdateCheck, unless the interval is 0.
    const throttled = await probeUpdate(exe, { DH_UPDATE_INTERVAL: "1" });
    expect(throttled.stdout.trim()).toBe("");

    for (const bad of ["{not json", "[]", '{"channel":"Bad Name"}']) {
      await writeFile(meta, bad);
      const result = await probeUpdate(exe, {});
      expect(result.success, result.stderr).toBe(true);
      expect(result.stdout.trim()).toBe(channelUrl("release"));
      const rewritten = JSON.parse(await readFile(meta, "utf8"));
      expect(rewritten.channel).toBeUndefined();
      expect(typeof rewritten.lastUpdateCheck).toBe("string");
    }

    await rm(meta);
    const dev = await probeUpdate(exe, {}, { channel: "dev" });
    expect(dev.success, dev.stderr).toBe(true);
    expect(dev.stdout.trim()).toBe("");
    expect(await stat(meta).catch(() => null)).toBeNull();
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
});

/**
 * An update to a new build whose download runs `duringDownload` (probe code)
 * before returning the binary. Returns whether the executable was replaced.
 */
async function updateWith(
  exe: string,
  duringDownload = "",
  env: Record<string, string> = {},
): Promise<{ replaced: boolean; result: Result }> {
  const NEW = "new build";
  await writeFile(exe, "old build");
  const sha256 = createHash("sha256").update(NEW).digest("hex");
  const result = await probeUpdate(
    exe,
    { DH_DEBUG: "1", ...env },
    {
      fetchBody: `
      if (String(url).endsWith("manifest.json")) {
        return Response.json({
          version: "3.0.1",
          binaries: { ${JSON.stringify(PLATFORM)}: { url: "dh-bin", sha256: ${JSON.stringify(sha256)} } },
        });
      }
      ${duringDownload}
      return new Response(${JSON.stringify(NEW)});`,
    },
  );
  expect(result.success, result.stderr).toBe(true);
  return { replaced: (await readFile(exe, "utf8")) === NEW, result };
}

test("[channels-cli-1] auto-update re-checks the channel under the dh.meta lock before replacing the executable", async () => {
  const tmp = await mkdtemp(join(tmpdir(), "dh-update-race-"));
  const exe = join(tmp, "dh");
  const meta = `${exe}.meta`;
  try {
    // Control: an uncontended update replaces it.
    expect((await updateWith(exe)).replaced).toBe(true);

    // dh channel use rc lands during a release update's download.
    await rm(meta);
    const switched = await updateWith(
      exe,
      `await Bun.write(${JSON.stringify(meta)}, '{"channel":"rc"}');`,
    );
    expect(switched.replaced).toBe(false);
    expect(switched.result.stderr).toContain("channel changed to rc");
    expect(JSON.parse(await readFile(meta, "utf8")).channel).toBe("rc");

    // Another dh holds the lock: the swap is skipped, not waited on.
    await rm(meta);
    const busy = await withFileLock(`${meta}.lock`, 1_000, () =>
      updateWith(exe),
    );
    expect(busy.replaced).toBe(false);
    expect(busy.result.stderr).toContain("is busy; skipping");
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

    // A pinned version, in either spelling, regardless of the channel.
    for (const pinned of [NEXT, `cli-v${NEXT}`]) {
      const dir = join(tmp, `bin-pinned-${pinned}`);
      const out = ok(
        await run(INSTALL.cmd, INSTALL.args, {
          ...installEnv,
          DH_INSTALL_DIR: dir,
          DH_INSTALL_VERSION: pinned,
        }),
      );
      expect(out.stdout).toContain(NEXT);
    }
  } finally {
    await gh.stop();
  }
}, 180_000);

test("[channels-cli-1] [channels-cli-2] dh installed with the install script updates itself from GitHub releases", async () => {
  const gh = await releaseServer();
  const { tmp, repo, release, baseUrl, buildRelease, publishChannel } = gh;
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
    // A failed download in dh channel use saves nothing.
    const metaBeforeUse = await readFile(`${exe}.meta`, "utf8");
    const failedUse = await run(exe, ["channel", "use", "rc", "-o", "json"], {
      ...env,
      DH_AUTO_UPDATE: "off",
    });
    expect(failedUse.success).toBe(false);
    expect(failedUse.stderr).toContain('"code":"server_unreachable"');
    expect(await readFile(`${exe}.meta`, "utf8")).toBe(metaBeforeUse);
    expect(await version()).toContain(VERSION);
    await writeFile(asset, good);

    const first = await run(exe, [], now);
    expect(first.stderr).toContain(`Updated ${BIN} ${VERSION} → ${NEXT}`);
    expect(await version()).toContain(NEXT);

    const second = await run(exe, [], now);
    expect(second.stderr).not.toContain(`Updated ${BIN}`);
    if (WINDOWS) {
      // The previous binary is cleaned up on the next run.
      expect(await stat(`${exe}.old`).catch(() => null)).toBeNull();
    }

    // A local build is a dev build: dh channel use refuses to replace it.
    const devDir = join(tmp, "dev-build");
    await build(devDir, { baseUrl: baseUrl("unused") });
    const devExe = join(devDir, binaryName(PLATFORM));
    const devBefore = await readFile(devExe);
    const devUse = await run(devExe, ["channel", "use", "rc"], env);
    expect(devUse.success).toBe(false);
    expect(devUse.stderr).toContain("needs an installed");
    expect(await readFile(devExe)).toEqual(devBefore);
    expect(await stat(`${devExe}.meta`).catch(() => null)).toBeNull();

    // Not on the channel: dh channel use fails and saves nothing.
    const meta = `${exe}.meta`;
    const metaBefore = await readFile(meta, "utf8");
    const missing = await run(exe, ["channel", "use", "nope", "-o", "json"], {
      ...env,
      DH_UPDATE_URL: `${repo}/releases/download/cli-channel-nope/manifest.json`,
    });
    expect(missing.success).toBe(false);
    expect(missing.stderr).toContain('"code":"channel_not_found"');
    expect(await readFile(meta, "utf8")).toBe(metaBefore);

    // Versions only need to differ: the channel moving back installs the older build.
    await publishChannel(`cli-v${VERSION}`);
    const switched = ok(
      await run(exe, ["channel", "use", "rc", "-o", "json"], {
        ...env,
        DH_AUTO_UPDATE: "off",
      }),
    );
    expect(JSON.parse(switched.stdout)).toEqual({
      channel: "rc",
      from: NEXT,
      to: VERSION,
    });
    expect(JSON.parse(await readFile(meta, "utf8")).channel).toBe("rc");
    expect(await version()).toContain(VERSION);
    // dh channel's output (and its check of the real channel manifests, which
    // DH_UPDATE_URL doesn't redirect) is covered in channel_list.test.ts.
    const again = ok(
      await run(exe, ["channel", "use", "rc"], {
        ...env,
        DH_AUTO_UPDATE: "off",
      }),
    ).stdout;
    expect(again).toBe(`✔ Following rc\n  Already on ${VERSION}\n`);

    await publishChannel(tag);
    const forward = await run(exe, [], now);
    expect(forward.stderr).toContain(`Updated ${BIN} ${VERSION} → ${NEXT}`);

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
