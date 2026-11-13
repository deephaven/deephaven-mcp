import { describe, expect, setDefaultTimeout, test } from "bun:test";
import {
  chmod,
  copyFile,
  mkdir,
  readdir,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { join, resolve } from "node:path";
import { updateShellSpec, writeShellPart } from "./bundle-helpers.ts";
import { CLI, run, tempDir, WINDOWS } from "./helpers.ts";

const FIXTURES = resolve(import.meta.dir, "fixtures/plugins");

// Each test starts several dh processes, and setup copies the bundle first.
setDefaultTimeout(30_000);
const ORIGIN1 = "https://dhe1.example.com:8123";
const ORIGIN2 = "https://dhe2.example.com:8123";

/** A stand-in tool: prints its arguments one per line, echoes stdin with `--stdin`, exits `$TOOL_EXIT`. */
const TOOL = `#!/bin/sh
for a in "$@"; do printf '[%s]\\n' "$a"; done
[ "$1" = "--stdin" ] && cat
exit "\${TOOL_EXIT:-0}"
`;

const spec = (path: string, extra: Record<string, unknown> = {}) => ({
  schema: 1,
  name: "ctl",
  description: "Wraps a tool.",
  tool: { path },
  commands: [
    {
      name: "intraday",
      description: "Group.",
      commands: [
        { name: "rescan", description: "Leaf.", argv: ["intraday", "rescan"] },
      ],
    },
  ],
  ...extra,
});

const profile = (name: string, server: string) => ({
  name,
  server,
  user: "u",
  method: "password",
  createdAt: "2026-01-01T00:00:00.000Z",
});

/** Two profiles on two servers, `p1` the default, both with SSH enabled for the bundle at `remote`. */
const profiles = (remote: string, ssh: Record<string, unknown> = {}) => ({
  version: 1,
  defaultProfile: "p_1",
  servers: {
    [ORIGIN1]: {
      kind: "enterprise",
      ssh: { enabled: true, bundle: remote, ...ssh },
    },
    [ORIGIN2]: {
      kind: "enterprise",
      ssh: { enabled: true, bundle: remote, hosts: ["infra-2"] },
    },
  },
  profiles: { p_1: profile("p1", ORIGIN1), p_2: profile("p2", ORIGIN2) },
});

interface Options {
  /** `config.json`, given the remote bundle's path; default {@link profiles}. */
  config?: (remote: string) => unknown;
  /** The remote `shell/` specs, given the stand-in tool's path. */
  specs?: (tool: string) => Record<string, unknown>[];
  /** The config directory, under the temp dir. */
  cliDir?: string;
  /** Copy the bundle for each profile first (default true); the ssh log starts empty either way. */
  sync?: boolean;
}

/**
 * A config dir, the stand-in tool, the fake `ssh` first on `PATH` (it runs
 * the remote command here), and a "remote" bundle at `<tmp>/remote/dh-cli`.
 */
async function setup(opts: Options = {}) {
  const tmp = await tempDir("dh-plugin-ssh-test-");
  const tool = join(tmp, "tool");
  await writeFile(tool, TOOL);
  await chmod(tool, 0o755);
  const bin = join(tmp, "bin");
  await mkdir(bin);
  for (const tool of ["ssh", "sudo", "id"]) {
    await copyFile(join(FIXTURES, `fake-${tool}`), join(bin, tool));
    await chmod(join(bin, tool), 0o755);
  }
  const remote = join(tmp, "remote", "dh-cli");
  await writeShellPart(remote, (opts.specs ?? ((t) => [spec(t)]))(tool));
  const dir = join(tmp, opts.cliDir ?? "cli");
  await mkdir(dir, { recursive: true });
  const config = (opts.config ?? profiles)(remote);
  if (config) await writeFile(join(dir, "config.json"), JSON.stringify(config));
  const log = join(tmp, "ssh.log");
  const dh = (
    args: string[],
    env: Record<string, string | undefined> = {},
    stdin?: string,
  ) =>
    run(
      [...CLI, ...args],
      {
        DH_CONFIG_DIR: dir,
        DH_AUTO_UPDATE: "off",
        DH_PLUGIN_BUNDLE: undefined,
        DH_PLUGIN_REFRESH: undefined,
        DH_PROFILE: undefined,
        DH_SERVER: undefined,
        DH_TIMEOUT: undefined,
        FAKE_SSH_LOG: log,
        FAKE_SUDO_LOG: join(tmp, "sudo.log"),
        PATH: `${bin}:${process.env.PATH}`,
        ...env,
      },
      stdin,
    );
  if (opts.sync !== false && config) {
    for (const p of ["p1", "p2"]) {
      const c = config as { profiles: Record<string, { name: string }> };
      if (!Object.values(c.profiles).some((x) => x.name === p)) continue;
      // Copies shell/ only: these servers aren't real, so there's no js/ to fetch.
      const r = await dh(["plugin", "ssh", "enable", "--profile", p]);
      if (r.code !== 0) throw new Error(`enable ${p}: ${r.stderr}`);
    }
  }
  await rm(log, { force: true });
  return {
    tmp,
    dir,
    tool,
    remote,
    dh,
    /** Each ssh run's argv (but `ssh -G`). */
    async logged(): Promise<string[][]> {
      const text = await readFile(log, "utf8").catch(() => "");
      return text
        .split("\n")
        .filter(Boolean)
        .map((l) => JSON.parse(l));
    },
    async clearLog() {
      await rm(log, { force: true });
    },
    async sudoed(): Promise<string[]> {
      const text = await readFile(join(tmp, "sudo.log"), "utf8").catch(
        () => "",
      );
      return text.split("\n").filter(Boolean);
    },
    async config() {
      return JSON.parse(await readFile(join(dir, "config.json"), "utf8"));
    },
    cache: join(dir, "plugins", "cache"),
  };
}

/** The `-o json` error: the last line of stderr, after any stderr passed through. */
function jsonError(stderr: string) {
  return JSON.parse(stderr.trim().split("\n").at(-1) ?? "");
}

const quote = (w: string) => `'${w.replaceAll("'", `'\\''`)}'`;

/** Whether an ssh run's remote command is the bundle copy. */
const isCopy = (argv: string[]) => (argv.at(-1) ?? "").includes("tar -cf");

describe.skipIf(WINDOWS)("ssh plugins: running", () => {
  test("every argument reaches the tool unchanged", async () => {
    const { dh } = await setup();
    const args = [
      "it's",
      "a b",
      "$HOME",
      "`id`",
      "; rm -rf /",
      "*",
      "",
      '--x="y"',
      "\t",
      "a\nb",
      "ünïcode",
    ];
    const r = await dh(["ctl", "intraday", "rescan", ...args]);
    expect(r.stderr).toBe("");
    expect(r.stdout).toBe(
      ["intraday", "rescan", ...args].map((a) => `[${a}]\n`).join(""),
    );
    expect(r.code).toBe(0);
  });

  test("argv without a terminal, DH_TIMEOUT as ConnectTimeout, and the revision check first", async () => {
    const { dh, dir, tool, remote, logged } = await setup();
    await dh(["ctl", "intraday", "rescan", "x"]);
    await dh(["ctl", "intraday", "rescan"], { DH_TIMEOUT: "7" });
    const [first, second] = await logged();
    expect(first?.slice(0, -1)).toEqual([
      "-T",
      "-o",
      "BatchMode=yes",
      "-o",
      "ConnectTimeout=30",
      "-o",
      "ControlMaster=auto",
      "-o",
      `ControlPath=${dir}/ssh/%C`,
      "-o",
      "ControlPersist=60",
      "--",
      "dhe1.example.com",
    ]);
    const revision = (
      await readFile(join(remote, "shell", "revision"), "utf8")
    ).trim();
    const script = `[ "$(cat ${quote(`${remote}/shell/revision`)} 2>/dev/null)" = ${quote(revision)} ] || { echo "dh-ssh:revision" >&2; exit 254; }; exec ${[tool, "intraday", "rescan", "x"].map(quote).join(" ")}`;
    expect(first?.at(-1)).toBe(["sh", "-c", script].map(quote).join(" "));
    expect(second).toContain("ConnectTimeout=7");
  });

  test("no multiplexing when ssh config sets it, or the socket path is too long", async () => {
    const a = await setup();
    await a.dh(["ctl", "intraday", "rescan"], {
      FAKE_SSH_CONTROLMASTER: "auto",
    });
    const b = await setup({ cliDir: join("x".repeat(80), "cli") });
    await b.dh(["ctl", "intraday", "rescan"]);
    for (const argv of [...(await a.logged()), ...(await b.logged())]) {
      expect(argv.join(" ")).not.toContain("Control");
      expect(argv).toContain("BatchMode=yes");
    }
  });

  test("host: the server's ssh.hosts, else the URL hostname; --profile picks the server and isn't passed on", async () => {
    const { dh, logged } = await setup();
    const r1 = await dh(["ctl", "intraday", "rescan"]);
    const r2 = await dh(["ctl", "intraday", "rescan", "--profile", "p2"]);
    const r3 = await dh(["ctl", "intraday", "rescan", "--profile=p2"]);
    const r4 = await dh(["--profile", "p2", "ctl", "intraday", "rescan"]);
    const r5 = await dh(["ctl", "intraday", "rescan"], { DH_PROFILE: "p2" });
    for (const r of [r1, r2, r3, r4, r5]) {
      expect(r.stdout).toBe("[intraday]\n[rescan]\n");
    }
    const hosts = (await logged()).map((argv) => argv.at(-2));
    expect(hosts).toEqual([
      "dhe1.example.com",
      "infra-2",
      "infra-2",
      "infra-2",
      "infra-2",
    ]);
    const bad = await dh([
      "-o",
      "json",
      "ctl",
      "intraday",
      "rescan",
      "--profile",
      "nosuch",
    ]);
    expect(bad.code).toBe(2);
    expect(jsonError(bad.stderr).error).toBe('No profile named "nosuch"');
  });

  test("an IPv6 server's hostname loses its brackets", async () => {
    const origin = "https://[::1]:8123";
    const { dh, logged } = await setup({
      config: (remote) => ({
        version: 1,
        defaultProfile: "p_1",
        servers: {
          [origin]: {
            kind: "enterprise",
            ssh: { enabled: true, bundle: remote },
          },
        },
        profiles: { p_1: profile("p1", origin) },
      }),
    });
    await dh(["ctl", "intraday", "rescan"]);
    expect((await logged())[0]?.at(-2)).toBe("::1");
  });

  test("--no-prompt is dh's, and -o json captures stdout over SSH", async () => {
    const { dh } = await setup();
    const r = await dh([
      "ctl",
      "intraday",
      "rescan",
      "--no-prompt",
      "-o",
      "json",
    ]);
    expect(JSON.parse(r.stdout)).toEqual({ stdout: "[intraday]\n[rescan]\n" });
  });

  test("stdin piped to dh reaches the tool", async () => {
    const { dh } = await setup({
      specs: (t) => [spec(t, { commands: undefined, argv: [] })],
    });
    const r = await dh(["ctl", "--stdin"], {}, "piped\n");
    expect(r.stdout).toBe("[--stdin]\npiped\n");
  });

  test("SSH failures are classified, with a hint and the host", async () => {
    const { dh } = await setup();
    const cases = [
      ["auth", "ssh_auth_failed", 4, "ssh-copy-id"],
      ["hostkey", "ssh_host_unknown", 5, "Connect once"],
      ["dns", "remote_unreachable", 5, "plugin set ctl --ssh-host"],
    ] as const;
    for (const [fail, code, exit, hint] of cases) {
      const r = await dh(["-o", "json", "ctl", "intraday", "rescan"], {
        FAKE_SSH_FAIL: fail,
      });
      expect(r.code).toBe(exit);
      const e = jsonError(r.stderr);
      expect(e).toMatchObject({ code, exit, sshHost: "dhe1.example.com" });
      expect(e.hint).toContain(hint);
    }
    const human = await dh(["ctl", "intraday", "rescan"], {
      FAKE_SSH_FAIL: "auth",
    });
    // Shown once, passed through from ssh; not repeated as the error's detail.
    expect(human.stderr.split("Permission denied").length).toBe(2);
    expect(human.stderr).toContain("SSH login to dhe1.example.com failed");
  });

  test("remote exit 127 is tool_not_found; other exits are tool_failed", async () => {
    const missing = await setup({ specs: () => [spec("/nowhere/tool")] });
    const r1 = await missing.dh(["-o", "json", "ctl", "intraday", "rescan"]);
    expect(r1.code).toBe(6);
    expect(jsonError(r1.stderr)).toMatchObject({
      code: "tool_not_found",
      error: "Tool not found on dhe1.example.com: /nowhere/tool",
      sshHost: "dhe1.example.com",
    });
    const { dh } = await setup();
    const r2 = await dh(["-o", "json", "ctl", "intraday", "rescan"], {
      TOOL_EXIT: "3",
    });
    expect(r2.code).toBe(6);
    expect(jsonError(r2.stderr)).toMatchObject({
      code: "tool_failed",
      toolExit: 3,
      sshHost: "dhe1.example.com",
    });
  });

  test("without a profile there are no SSH plugins, and nothing runs", async () => {
    const { dh, logged } = await setup({
      config: (remote) => ({ ...profiles(remote), defaultProfile: undefined }),
      sync: false,
    });
    const r = await dh(["-o", "json", "ctl", "intraday", "rescan"]);
    expect(r.code).toBe(2);
    const list = JSON.parse(
      (await dh(["plugin", "list", "-o", "json"])).stdout,
    );
    expect(shellOf(list)).toMatchObject({
      channel: "shell",
      reason: "no profile",
    });
    expect(await logged()).toEqual([]);
  });
});

describe.skipIf(WINDOWS)("ssh plugins: the shell channel", () => {
  test("with empty caches and no local bundle, --help registers no plugins and runs no ssh", async () => {
    const { dh, logged } = await setup({ sync: false });
    const help = await dh(["--help"]);
    expect(help.code).toBe(0);
    expect(help.stdout).not.toContain("ctl");
    expect(await logged()).toEqual([]);
  });

  test("plugin ssh enable checks the connection, copies once, and registers the plugins", async () => {
    const { dh, logged, config } = await setup({
      config: (remote) => {
        const c = profiles(remote);
        c.servers[ORIGIN1].ssh = { bundle: remote } as never;
        return c;
      },
      sync: false,
    });
    expect((await dh(["ctl"])).code).toBe(2);
    const r = await dh(["plugin", "ssh", "enable", "--ssh-host", "infra-1"]);
    expect(r.code).toBe(0);
    expect(r.stdout).toContain(`✔ SSH plugins enabled for ${ORIGIN1}`);
    expect(r.stdout).toContain("shell: updated from infra-1: added ctl");
    expect(r.stdout).toContain("✔ connect  infra-1 in");
    const runs = await logged();
    expect(runs.filter(isCopy)).toHaveLength(1);
    expect(runs.map((a) => a.at(-2))).toEqual(["infra-1", "infra-1"]);
    expect((await config()).servers[ORIGIN1].ssh).toMatchObject({
      enabled: true,
      hosts: ["infra-1"],
    });
    const agents = JSON.parse((await dh(["agents", "-o", "json"])).stdout);
    const ctl = agents.commands.find(
      (c: { command: string }) => c.command === "dh ctl",
    );
    expect(ctl).toMatchObject({ plugin: "ctl", pluginExec: "ssh" });
    const off = await dh(["plugin", "ssh", "disable"]);
    expect(off.stdout).toBe(`✔ SSH plugins disabled for ${ORIGIN1}\n`);
    expect((await config()).servers[ORIGIN1].ssh.enabled).toBe(false);
    expect((await dh(["ctl"])).code).toBe(2);
    const list = JSON.parse(
      (await dh(["plugin", "list", "-o", "json"])).stdout,
    );
    expect(shellOf(list).reason).toBe("SSH not enabled");
  });

  test("a failing connection check saves nothing", async () => {
    const { dh, config } = await setup({
      config: (remote) => {
        const c = profiles(remote);
        c.servers[ORIGIN1].ssh = { bundle: remote } as never;
        return c;
      },
      sync: false,
    });
    const r = await dh(["-o", "json", "plugin", "ssh", "enable"], {
      FAKE_SSH_FAIL: "auth",
    });
    expect(r.code).toBe(4);
    expect(jsonError(r.stderr).code).toBe("ssh_auth_failed");
    expect((await config()).servers[ORIGIN1].ssh.enabled).toBeUndefined();
  });

  test("an unchanged bundle is reported unchanged, and only fetchedAt changes", async () => {
    const { dh, cache, logged } = await setup();
    const part = join(cache, encodeURIComponent(ORIGIN1), "shell");
    const before = await snapshot(part);
    const r = await dh(["plugin", "ssh", "enable"]);
    expect(r.stdout).toContain("shell: unchanged\n");
    const sync = await dh(["plugin", "sync"]);
    expect(sync.stdout).toContain("shell: unchanged\n");
    expect(sync.stdout).toContain("js: ✖ Could not fetch");
    expect(sync.code).toBe(5);
    expect((await logged()).filter(isCopy)).toHaveLength(2);
    const after = await snapshot(part);
    const state = (s: Record<string, string>) => {
      const { fetchedAt, ...rest } = JSON.parse(s["state.json"] ?? "{}");
      return { fetchedAt, rest };
    };
    expect(state(after).rest).toEqual(state(before).rest);
    expect(state(after).fetchedAt).not.toBe(state(before).fetchedAt);
    const { "state.json": _, ...others } = after;
    const { "state.json": __, ...othersBefore } = before;
    expect(others).toEqual(othersBefore);
  });

  test("a command with an unchanged bundle makes exactly one ssh run", async () => {
    const { dh, logged } = await setup();
    await dh(["ctl", "intraday", "rescan"]);
    expect(await logged()).toHaveLength(1);
  });

  test("a changed bundle stops the run (254), is copied, and dh runs again with the new spec", async () => {
    const { dh, tool, remote, logged } = await setup();
    await updateShellSpec(
      remote,
      spec(tool, {
        commands: [
          {
            name: "intraday",
            description: "Group.",
            commands: [
              {
                name: "rescan",
                description: "Leaf.",
                argv: ["intraday", "rescan", "--new"],
              },
            ],
          },
        ],
      }),
    );
    const r = await dh(["ctl", "intraday", "rescan", "x"]);
    expect(r.stderr).toBe("");
    expect(r.stdout).toBe("[intraday]\n[rescan]\n[--new]\n[x]\n");
    expect(r.code).toBe(0);
    const runs = await logged();
    expect(runs.map((a) => (isCopy(a) ? "copy" : "run"))).toEqual([
      "run",
      "copy",
      "run",
    ]);
  });

  test("a plugin removed from the bundle is plugin_unavailable", async () => {
    const { dh, remote, tool } = await setup({
      specs: (t) => [spec(t), spec(t, { name: "other" })],
    });
    await writeShellPart(remote, [spec(tool, { name: "other" })]);
    await rm(join(remote, "shell", "ctl.json"));
    const r = await dh(["-o", "json", "ctl", "intraday", "rescan"]);
    expect(r.code).toBe(2);
    expect(jsonError(r.stderr)).toMatchObject({ code: "plugin_unavailable" });
    expect((await dh(["ctl"])).code).toBe(2);
    expect((await dh(["other", "intraday", "rescan"])).code).toBe(0);
  });

  test("files that don't match their digest, or a symlink in the bundle, reject the copy", async () => {
    const { dh, remote, tool } = await setup();
    const listed = async () =>
      JSON.parse(
        (await dh(["plugin", "list", "-o", "json"])).stdout,
      ).channels.find((c: { channel: string }) => c.channel === "shell")
        .revision;
    const before = await listed();
    await updateShellSpec(remote, spec(tool, { description: "Changed." }));
    await writeFile(
      join(remote, "shell", "ctl.json"),
      JSON.stringify(spec(tool, { description: "Not what the digest says." })),
    );
    const bad = await dh(["-o", "json", "plugin", "ssh", "enable"]);
    expect(bad.code).toBe(2);
    expect(jsonError(bad.stderr).code).toBe("plugin_invalid");
    expect(await listed()).toBe(before);

    await updateShellSpec(remote, spec(tool, { description: "Changed." }));
    await symlink("/etc/passwd", join(remote, "shell", "link"));
    const link = await dh(["-o", "json", "plugin", "ssh", "enable"]);
    expect(link.code).toBe(2);
    expect(jsonError(link.stderr).error).toContain("rejected");
    expect(await listed()).toBe(before);
  });

  test("no bundle on the host: plugin list says so, and other commands work", async () => {
    const { dh } = await setup({
      config: (remote) => profiles(`${remote}-missing`),
    });
    const list = await dh(["plugin", "list"]);
    expect(list.stdout).toMatch(
      /no dh-cli bundle at \S+-missing on dhe1\.example\.com/,
    );
    expect((await dh(["auth"])).code).toBe(0);
    expect((await dh(["ctl"])).code).toBe(2);
  });

  test("the check after other commands runs when due, and is silent when it fails", async () => {
    const { dh, logged } = await setup();
    await dh(["auth"]);
    expect(await logged()).toEqual([]);
    const due = await dh(["auth"], { DH_PLUGIN_REFRESH: "0" });
    expect(due.code).toBe(0);
    const runs = await logged();
    expect(runs).toHaveLength(1);
    expect(isCopy(runs[0] ?? [])).toBe(true);
    expect(runs[0]).toContain("BatchMode=yes");
    expect(runs[0]).toContain("ConnectTimeout=5");
    const failing = await dh(["auth"], {
      DH_PLUGIN_REFRESH: "0",
      FAKE_SSH_FAIL: "dns",
    });
    expect(failing.code).toBe(0);
    expect(failing.stderr).toBe("");
  });

  test("DH_PLUGIN_BUNDLE without a profile runs plugins locally, with no ssh", async () => {
    const { dh, logged } = await setup({
      config: () => undefined,
      sync: false,
    });
    const examples = resolve(import.meta.dir, "../examples/dh-cli");
    const r = await dh(["-o", "json", "ctl", "metadata", "list"], {
      DH_PLUGIN_BUNDLE: examples,
    });
    expect(r.code).toBe(6);
    expect(jsonError(r.stderr).error).toBe(
      "Tool not found: /usr/illumon/latest/bin/dhctl",
    );
    expect(await logged()).toEqual([]);
  });

  test("--exec and --prefix are gone; exec and prefix left in config.json are ignored and dropped", async () => {
    const { dh, config } = await setup({
      config: (remote) => {
        const c = profiles(remote) as Record<string, unknown>;
        c.plugins = { ctl: { exec: "local", prefix: ["false"], path: "/x" } };
        (c.servers as Record<string, Record<string, unknown>>)[ORIGIN1] = {
          kind: "enterprise",
          ssh: { enabled: true, bundle: remote },
          plugins: {
            ctl: { prefix: ["false"], version: "1", sshHosts: ["h"] },
          },
        };
        c.sshOfferDeclined = ["ctl"];
        return c;
      },
    });
    for (const flag of ["--exec", "--prefix"]) {
      const r = await dh(["-o", "json", "plugin", "set", "ctl", flag, "ssh"]);
      expect(r.code).toBe(2);
      expect(jsonError(r.stderr).code).toBe("usage");
    }
    const run = await dh(["ctl", "intraday", "rescan"]);
    expect(run.stdout).toBe("[intraday]\n[rescan]\n");
    await dh(["plugin", "ssh", "enable", "--ssh-host", "h"]);
    const c = await config();
    expect(c.plugins).toEqual({ ctl: { path: "/x" } });
    expect(c.servers[ORIGIN1].plugins).toEqual({ ctl: { sshHosts: ["h"] } });
    expect(c.sshOfferDeclined).toBeUndefined();
  });
});

describe.skipIf(WINDOWS)("ssh plugins: help and dh agents", () => {
  test("help lists --profile, --ssh-host and --no-prompt", async () => {
    const { dh } = await setup();
    for (const args of [
      ["ctl", "--help"],
      ["ctl", "intraday", "rescan", "--help"],
    ]) {
      const help = (await dh(args)).stdout;
      const globals = help.slice(help.indexOf("Global Options:"));
      const order = ["--output", "--profile", "--ssh-host", "--no-prompt"];
      const at = order.map((f) => globals.indexOf(f));
      expect(at.every((i) => i >= 0)).toBe(true);
      expect([...at].sort((a, b) => a - b)).toEqual(at);
    }
    expect((await dh(["--help"])).stdout).not.toContain("--ssh-host");
  });

  test("dh agents: pluginFlags by exec, the plugin on its noun, and the Over SSH sentence", async () => {
    const { dh } = await setup();
    const m = JSON.parse((await dh(["agents", "-o", "json"])).stdout);
    expect(m.pluginFlags).toEqual({
      local: ["-o, --output <format>", "-h, --help"],
      ssh: [
        "-o, --output <format>",
        "-h, --help",
        "--profile <name>",
        "--ssh-host <host>",
        "--no-prompt",
      ],
      rest: "passed to the plugin's tool",
    });
    const ctl = m.commands.find(
      (c: { command: string }) => c.command === "dh ctl",
    );
    expect(ctl).toMatchObject({
      plugin: "ctl",
      pluginTool: "tool",
      pluginExec: "ssh",
    });
    expect(JSON.stringify(ctl.commands)).not.toContain("pluginExec");
    const human = (await dh(["agents"])).stdout;
    expect(human).toContain(
      "Over SSH (`dh ctl`), they also take `--profile`, `--ssh-host` and `--no-prompt`.",
    );
  });
});

describe.skipIf(WINDOWS)("ssh plugins: hosts", () => {
  /** The server's hosts are `infra-1`; ctl's own are `own`, if given. */
  const withHosts = (own?: Record<string, unknown>) => (remote: string) => {
    const c = profiles(remote, { hosts: ["infra-1"] });
    if (own) {
      (c.servers[ORIGIN1] as Record<string, unknown>).plugins = { ctl: own };
    }
    return c;
  };
  const hostsOf = async (logged: () => Promise<string[][]>) =>
    (await logged()).map((a) => a.at(-2));

  test('set --ssh-host: checked, repeatable, duplicates dropped; "" goes back to the server\'s', async () => {
    const { dh, config, logged } = await setup({ config: withHosts() });
    const r = await dh([
      "plugin",
      "set",
      "ctl",
      "--ssh-host",
      "dis-1",
      "--ssh-host",
      "dis-2",
      "--ssh-host",
      "dis-1",
    ]);
    expect(r.code).toBe(0);
    expect(r.stdout).toContain(
      `  ssh hosts: dis-1 (default), dis-2 (${ORIGIN1})`,
    );
    expect(r.stdout).toContain("✔ tool     dis-1");
    expect(await hostsOf(logged)).toEqual(["dis-1", "dis-1", "dis-2", "dis-2"]);
    expect((await config()).servers[ORIGIN1].plugins.ctl).toEqual({
      sshHosts: ["dis-1", "dis-2"],
    });
    const cleared = await dh([
      "plugin",
      "set",
      "ctl",
      "--ssh-host",
      "",
      "-o",
      "json",
    ]);
    expect(JSON.parse(cleared.stdout)).toMatchObject({
      sshHosts: ["infra-1"],
      defaultSshHost: "infra-1",
      ownSshHosts: false,
    });
    expect((await config()).servers[ORIGIN1].plugins).toBeUndefined();
  });

  test("set: a tool missing on the host is tool_not_found and nothing is saved; --no-check skips the check", async () => {
    const { dh, config, logged } = await setup({
      config: withHosts(),
      specs: () => [spec("/nowhere/tool")],
    });
    const r = await dh([
      "-o",
      "json",
      "plugin",
      "set",
      "ctl",
      "--ssh-host",
      "x",
    ]);
    expect(r.code).toBe(6);
    expect(jsonError(r.stderr).code).toBe("tool_not_found");
    expect((await config()).servers[ORIGIN1].plugins).toBeUndefined();
    const before = (await logged()).length;
    const unchecked = await dh([
      "plugin",
      "set",
      "ctl",
      "--ssh-host",
      "x",
      "--no-check",
    ]);
    expect(unchecked.code).toBe(0);
    expect((await logged()).length).toBe(before);
  });

  test("runs go to the plugin's first host, then to the one `plugin use` picks", async () => {
    const { dh, config, logged } = await setup({
      config: withHosts({ sshHosts: ["dis-1", "dis-2"] }),
    });
    await dh(["ctl", "intraday", "rescan"]);
    const used = await dh(["plugin", "use", "ctl", "--ssh-host", "dis-2"]);
    expect(used.stdout).toBe(`✔ ctl runs on dis-2 by default on ${ORIGIN1}\n`);
    expect((await config()).servers[ORIGIN1].plugins.ctl).toEqual({
      sshHosts: ["dis-1", "dis-2"],
      defaultSshHost: "dis-2",
    });
    await dh(["ctl", "intraday", "rescan"]);
    expect(await hostsOf(logged)).toEqual(["dis-1", "dis-2"]);
  });

  test("--ssh-host picks the host for one run; one that isn't listed is usage", async () => {
    const { dh, logged } = await setup({
      config: withHosts({
        sshHosts: ["dis-1", "dis-2"],
        defaultSshHost: "dis-2",
      }),
    });
    const r = await dh([
      "ctl",
      "intraday",
      "rescan",
      "x",
      "--ssh-host",
      "dis-1",
    ]);
    expect(r.stdout).toBe("[intraday]\n[rescan]\n[x]\n");
    expect(await hostsOf(logged)).toEqual(["dis-1"]);
    const bad = await dh([
      "-o",
      "json",
      "ctl",
      "intraday",
      "rescan",
      "--ssh-host",
      "nope",
    ]);
    expect(bad.code).toBe(2);
    expect(jsonError(bad.stderr).hint).toContain(
      "dh plugin set ctl --ssh-host dis-1 --ssh-host dis-2 --ssh-host nope",
    );
    const server = await setup({ config: withHosts() });
    const unlisted = await server.dh([
      "-o",
      "json",
      "ctl",
      "intraday",
      "rescan",
      "--ssh-host",
      "nope",
    ]);
    expect(jsonError(unlisted.stderr).hint).toContain(
      "dh plugin ssh enable --ssh-host infra-1 --ssh-host nope",
    );
  });

  test("an old `host` is read as the plugin's one host", async () => {
    const { dh, logged } = await setup({
      config: withHosts({ host: "old-1" }),
    });
    await dh(["ctl", "intraday", "rescan"]);
    expect(await hostsOf(logged)).toEqual(["old-1"]);
  });

  test("plugin use: picker needs a terminal; unknown hosts and plugins are usage", async () => {
    const { dh } = await setup({
      config: withHosts({ sshHosts: ["dis-1", "dis-2"] }),
    });
    const cases: [string[], string][] = [
      [["ctl"], "Input needed: pass --ssh-host"],
      [["ctl", "--ssh-host", "nope"], "nope isn't one of ctl's SSH hosts"],
      [["nosuch"], 'No plugin named "nosuch"'],
    ];
    for (const [args, message] of cases) {
      const r = await dh(["-o", "json", "plugin", "use", ...args]);
      expect(r.code).toBe(2);
      expect(jsonError(r.stderr).error).toContain(message);
    }
  });

  test("plugin list shows the source, and the hosts default first and starred", async () => {
    const { dh } = await setup({
      config: withHosts({
        sshHosts: ["dis-1", "dis-2"],
        defaultSshHost: "dis-2",
      }),
    });
    const text = (await dh(["plugin", "list"])).stdout;
    expect(text).toContain(`shell: over SSH from infra-1 (${ORIGIN1})`);
    expect(text).toContain("cluster 2026.01.1g0");
    expect(text).toContain("  ctl   ssh: dis-2*, dis-1 ");
    const out = JSON.parse((await dh(["plugin", "list", "-o", "json"])).stdout);
    expect(shellOf(out)).toMatchObject({
      channel: "shell",
      source: "ssh",
      server: ORIGIN1,
      sshHost: "infra-1",
      clusterVersion: "2026.01.1g0",
    });
    expect(out.plugins[0]).toMatchObject({
      sshHosts: ["dis-2", "dis-1"],
      defaultSshHost: "dis-2",
    });
  });

  test("plugin check: each host's steps, default first", async () => {
    const { dh } = await setup({
      config: withHosts({
        sshHosts: ["dis-1", "dis-2"],
        defaultSshHost: "dis-2",
      }),
    });
    const r = await dh(["plugin", "check", "ctl", "-o", "json"]);
    expect(r.code).toBe(0);
    const out = JSON.parse(r.stdout);
    expect(out.sshHosts).toEqual(["dis-2", "dis-1"]);
    const steps = ["host", "connect", "tool"];
    expect(
      out.checks.map((c: { step: string; sshHost: string }) => [
        c.step,
        c.sshHost,
      ]),
    ).toEqual([
      ...steps.map((s) => [s, "dis-2"]),
      ...steps.map((s) => [s, "dis-1"]),
    ]);
    const human = await dh(["plugin", "check", "ctl"]);
    expect(human.stdout).toMatch(/✔ connect {2}dis-1 {2}in \d+ ms/);
    expect(human.stdout).toContain("✔ host     dis-2  dis-2 as tester");
    const failing = await dh(["plugin", "check", "ctl", "-o", "json"], {
      FAKE_SSH_FAIL: "auth",
    });
    expect(failing.code).toBe(4);
    const e = jsonError(failing.stderr);
    expect(e.sshHost).toBe("dis-2");
    expect(
      e.checks.map((c: { step: string; ok: boolean }) => [c.step, c.ok]),
    ).toEqual([
      ["host", true],
      ["connect", false],
    ]);
  });
});

describe.skipIf(WINDOWS)("ssh plugins: runAs", () => {
  const RUN_AS = {
    account: "admin",
    unlessFlags: ["-k", "--key", "-user", "--user"],
  };
  const withRunAs = (t: string) => [
    spec(t, { tool: { path: t, runAs: RUN_AS } }),
  ];

  test("runs through sudo as the admin account, unless the user brings their own credentials", async () => {
    const { dh, tool, sudoed } = await setup({ specs: withRunAs });
    const r = await dh(["ctl", "intraday", "rescan", "-t", "A.B"]);
    expect(r.stdout).toBe("[intraday]\n[rescan]\n[-t]\n[A.B]\n");
    expect(await sudoed()).toEqual([
      `-n -u irisadmin ${tool} intraday rescan -t A.B`,
    ]);
    for (const own of [
      ["--user", "jdoe", "--pwfile", "p"],
      ["-kmy.key"],
      ["--key=my.key"],
      ["-user", "jdoe"],
    ]) {
      expect((await dh(["ctl", "intraday", "rescan", ...own])).code).toBe(0);
    }
    expect(await sudoed()).toHaveLength(1);
    // After a `--` the tool sees, --key is an argument, not a flag.
    const after = await dh(["ctl", "intraday", "rescan", "--", "--", "--key"]);
    expect(after.stdout).toBe("[intraday]\n[rescan]\n[--]\n[--key]\n");
    expect(await sudoed()).toHaveLength(2);
  });

  test("the account comes from cluster.cnf on the host, else the default; help then names it", async () => {
    const { dh, tmp, sudoed } = await setup({ specs: withRunAs });
    const etc = join(tmp, "node", "etc", "sysconfig", "deephaven");
    await mkdir(etc, { recursive: true });
    await writeFile(
      join(etc, "cluster.cnf"),
      'DH_MERGE_USER=m\nDH_ADMIN_USER="opsadmin"\nDH_ADMIN_USER=second\n',
    );
    const root = { FAKE_SSH_ROOT: join(tmp, "node") };
    expect((await dh(["ctl", "--help"])).stdout).toContain(
      "Runs as the cluster's admin account (irisadmin), or as you with --key or --user.",
    );
    await dh(["ctl", "intraday", "rescan"], root);
    expect((await sudoed()).at(-1)).toStartWith("-n -u opsadmin ");
    expect((await dh(["ctl", "intraday", "--help"])).stdout).toContain(
      "Runs as the cluster's admin account (opsadmin)",
    );
    await chmod(join(etc, "cluster.cnf"), 0o000);
    await dh(["ctl", "intraday", "rescan"], root);
    expect((await sudoed()).at(-1)).toStartWith("-n -u irisadmin ");
  });

  test("logged in over SSH as the account itself: no sudo", async () => {
    const { dh, sudoed } = await setup({ specs: withRunAs });
    const r = await dh(["ctl", "intraday", "rescan"], {
      FAKE_ID_USER: "irisadmin",
    });
    expect(r.stdout).toBe("[intraday]\n[rescan]\n");
    expect(await sudoed()).toEqual([]);
  });

  test("sudo refused is tool_failed naming the account and the flags; the tool doesn't run as the user", async () => {
    const { dh } = await setup({ specs: withRunAs });
    const r = await dh(["-o", "json", "ctl", "intraday", "rescan"], {
      FAKE_SUDO: "refuse",
    });
    expect(r.code).toBe(6);
    expect(r.stdout).toBe("");
    expect(jsonError(r.stderr)).toMatchObject({
      code: "tool_failed",
      hint: "You can't run tool as irisadmin; pass your own credentials: -k --key -user --user",
      sshHost: "dhe1.example.com",
    });
    const password = await dh(["-o", "json", "ctl", "intraday", "rescan"], {
      FAKE_SUDO: "password",
    });
    expect(jsonError(password.stderr).hint).toBe(
      "Run it in a terminal to enter sudo's password.",
    );
  });

  test("dh agents, plugin list and plugin check show it", async () => {
    const { dh } = await setup({ specs: withRunAs });
    const m = JSON.parse((await dh(["agents", "-o", "json"])).stdout);
    const ctl = m.commands.find(
      (c: { command: string }) => c.command === "dh ctl",
    );
    expect(ctl.runAs).toEqual(RUN_AS);
    expect((await dh(["plugin", "list"])).stdout).toContain(", runs as admin");
    const check = JSON.parse(
      (await dh(["plugin", "check", "ctl", "-o", "json"])).stdout,
    );
    expect(check.checks.map((c: { step: string }) => c.step)).toEqual([
      "host",
      "connect",
      "tool",
      "runAs",
    ]);
    expect(check.checks.at(-1).detail).toBe("irisadmin (sudo)");
    const refused = await dh(["-o", "json", "plugin", "check", "ctl"], {
      FAKE_SUDO: "refuse",
    });
    expect(refused.code).toBe(6);
    expect(jsonError(refused.stderr).hint).toContain(
      "You can't run tool as irisadmin",
    );
  });
});

/** Every file under `dir`, by relative path, with its text. */
async function snapshot(dir: string): Promise<Record<string, string>> {
  const out: Record<string, string> = {};
  const entries = await readdir(dir, { recursive: true, withFileTypes: true });
  for (const e of entries) {
    if (!e.isFile()) continue;
    const path = join(e.parentPath, e.name);
    out[path.slice(dir.length + 1)] = await readFile(path, "utf8");
  }
  return out;
}

/** `dh plugin list -o json`'s shell channel. */
// biome-ignore lint/suspicious/noExplicitAny: parsed JSON.
function shellOf(list: { channels: any[] }) {
  return list.channels.find((c) => c.channel === "shell");
}
