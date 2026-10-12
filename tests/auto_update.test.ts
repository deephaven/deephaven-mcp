import { expect, test } from "bun:test";
import {
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
import { VERSION } from "../src/version.ts";

const NEXT = "9.9.9";
// Every asset of this release redirects to plain HTTP (never resolves: .invalid).
const INSECURE_TAG = "v0.0.0-insecure";
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

/** Serves `root` with GitHub's `releases/latest` redirects pointing at `latest.tag`. */
function fakeGitHub(root: string, latest: { tag: string }) {
  return Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      const path = decodeURIComponent(url.pathname);
      if (path === "/releases/latest") {
        return Response.redirect(
          new URL(`/releases/tag/${latest.tag}`, url).href,
        );
      }
      const asset = path.match(/^\/releases\/latest\/download\/(.+)$/);
      if (asset) {
        const to = `/releases/download/${latest.tag}/${asset[1]}`;
        return Response.redirect(new URL(to, url).href);
      }
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

test("dh installed with the install script updates itself from GitHub releases", async () => {
  const tmp = await mkdtemp(join(tmpdir(), "dh-update-test-"));
  const latest = { tag: `v${VERSION}` };
  const server = fakeGitHub(tmp, latest);
  const repo = `http://127.0.0.1:${server.port}`;
  const release = (tag: string) => join(tmp, "releases", "download", tag);
  const baseUrl = (tag: string) => `${repo}/releases/download/${tag}/`;
  try {
    await build(release(latest.tag), { baseUrl: baseUrl(latest.tag) });

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
    expect(install.stdout).toContain(`Installed dh v${VERSION}`);

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
    } else if (process.env.CI) {
      // This edits the real user PATH, so it only runs on throwaway CI runners.
      const original = await userPath();
      const seeded = [original.value, "%USERPROFILE%\\dh-test"]
        .filter(Boolean)
        .join(";");
      try {
        await setUserPath({ value: seeded, kind: "ExpandString" });
        for (let i = 0; i < 2; i++) {
          ok(
            await run(INSTALL.cmd, INSTALL.args, {
              ...installEnv,
              DH_INSTALL_REPO_URL: repo,
              DH_INSTALL_NO_MODIFY_PATH: "",
            }),
          );
        }
        expect(await userPath()).toEqual({
          value: `${seeded};${bin}`,
          kind: "ExpandString",
        });
      } finally {
        await setUserPath(original);
      }
    }

    const exe = join(bin, WINDOWS ? "dh.exe" : "dh");
    const env = {
      DH_UPDATE_URL: `${repo}/releases/latest/download/manifest.json`,
      DH_DEBUG: "1",
    };
    const now = { ...env, DH_UPDATE_INTERVAL: "0" };
    const version = async () => (await run(exe, ["--version"], env)).stdout;

    expect(await version()).toContain(VERSION);

    // Tag exists but its assets aren't uploaded: the check fails, but still starts the interval.
    latest.tag = `v${NEXT}`;
    const early = await run(exe, [], env);
    expect(early.stderr).toContain("auto-update failed");

    await build(release(latest.tag), {
      version: NEXT,
      baseUrl: baseUrl(latest.tag),
    });

    const throttled = await run(exe, [], env);
    expect(throttled.stderr).not.toContain("Updated dh");
    expect(await version()).toContain(VERSION);

    await run(exe, [], { ...now, DH_AUTO_UPDATE: "off" });
    expect(await version()).toContain(VERSION);

    const asset = join(release(latest.tag), binaryName(PLATFORM));
    const good = await readFile(asset);
    const corrupt = Buffer.from(good);
    corrupt.writeUInt8(corrupt.readUInt8(0) ^ 0xff, 0);
    await writeFile(asset, corrupt);
    const tampered = await run(exe, [], now);
    expect(tampered.stderr).toContain("Checksum mismatch");
    expect(await version()).toContain(VERSION);
    await writeFile(asset, good);

    const first = await run(exe, [], now);
    expect(first.stderr).toContain(`Updated dh ${VERSION} -> ${NEXT}`);
    expect(await version()).toContain(NEXT);

    const second = await run(exe, [], now);
    expect(second.stderr).not.toContain("Updated dh");
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
    await server.stop(true);
    await rm(tmp, { recursive: true, force: true });
  }
}, 180_000);
