import { describe, expect, setDefaultTimeout, test } from "bun:test";
import { chmod, copyFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { requireAuth } from "../src/actions/auth/session.ts";
import { Store } from "../src/actions/auth/store.ts";
import { DhError } from "../src/errors.ts";
import { writeShellPart } from "./bundle-helpers.ts";
import { CLI, run, tempDir, WINDOWS } from "./helpers.ts";

setDefaultTimeout(30_000);

const FIXTURES = resolve(import.meta.dir, "fixtures/plugins");
const ORIGIN = "https://dhe.example.com:8000";

/** A stand-in tool: prints its arguments one per line. */
const TOOL = `#!/bin/sh
for a in "$@"; do printf '[%s]\\n' "$a"; done
`;

/**
 * An empty config dir, the fake ssh on PATH, a "node" whose
 * /etc/deephaven/FRONTEND_* say `frontend` (unless null), and a bundle with
 * `ctl` at `<tmp>/node/dh-cli`.
 */
async function setup(
  frontend: { host: string; port: string } | null = {
    host: "dhe.example.com",
    port: "8000",
  },
) {
  const tmp = await tempDir("dh-ssh-profile-test-");
  const tool = join(tmp, "tool");
  await writeFile(tool, TOOL);
  await chmod(tool, 0o755);
  const bin = join(tmp, "bin");
  await mkdir(bin);
  await copyFile(join(FIXTURES, "fake-ssh"), join(bin, "ssh"));
  await chmod(join(bin, "ssh"), 0o755);
  const node = join(tmp, "node");
  const etc = join(node, "etc", "deephaven");
  await mkdir(etc, { recursive: true });
  if (frontend) {
    await writeFile(join(etc, "FRONTEND_HOST"), `${frontend.host}\n`);
    await writeFile(join(etc, "FRONTEND_PORT"), `${frontend.port}\n`);
  }
  const bundle = join(node, "dh-cli");
  await writeShellPart(bundle, [
    {
      schema: 1,
      name: "ctl",
      description: "Wraps a tool.",
      tool: { path: tool },
      argv: [],
    },
  ]);
  const dir = join(tmp, "cli");
  await mkdir(dir, { recursive: true });
  const log = join(tmp, "ssh.log");
  const dh = (args: string[], env: Record<string, string | undefined> = {}) =>
    run([...CLI, ...args], {
      DH_CONFIG_DIR: dir,
      DH_AUTO_UPDATE: "off",
      DH_PLUGIN_BUNDLE: undefined,
      DH_PROFILE: undefined,
      DH_SERVER: undefined,
      FAKE_SSH_LOG: log,
      FAKE_SSH_ROOT: node,
      PATH: `${bin}:${process.env.PATH}`,
      ...env,
    });
  return {
    tmp,
    dir,
    bundle,
    dh,
    login: (...args: string[]) =>
      dh([
        "auth",
        "login",
        ...args,
        "--method",
        "ssh",
        "--ssh-bundle",
        bundle,
        "--no-prompt",
      ]),
    async config() {
      return JSON.parse(
        await readFile(join(dir, "config.json"), "utf8").catch(() => "{}"),
      );
    },
  };
}

const jsonError = (stderr: string) =>
  JSON.parse(stderr.trim().split("\n").at(-1) ?? "");

describe.skipIf(WINDOWS)("SSH-only profiles", () => {
  test("login --method ssh saves a profile keyed by the node's FRONTEND_* address; plugins then run over SSH", async () => {
    const s = await setup();
    const r = await s.login("dhe.example.com", "--ssh-host", "infra-1");
    expect(r.code).toBe(0);
    expect(r.stdout).toContain(
      `✔ Cluster ${ORIGIN} (Deephaven Enterprise 2026.01.1g0)`,
    );
    expect(r.stdout).toContain("✔ Plugins: ctl");
    expect(r.stdout).toContain(
      '✔ Saved profile "dhe.example.com:ssh" (default)',
    );
    const c = await s.config();
    expect(c.servers[ORIGIN]).toEqual({
      kind: "enterprise",
      ssh: { enabled: true, hosts: ["infra-1"], bundle: s.bundle },
    });
    expect(Object.values(c.profiles)).toEqual([
      {
        name: "dhe.example.com:ssh",
        server: ORIGIN,
        method: "ssh",
        createdAt: expect.any(String),
      },
    ]);
    const ran = await s.dh(["ctl", "x"]);
    expect(ran.stdout).toBe("[x]\n");
    const listed = JSON.parse((await s.dh(["auth", "-o", "json"])).stdout);
    expect(listed).toEqual([
      {
        name: "dhe.example.com:ssh",
        server: ORIGIN,
        kind: "enterprise",
        user: null,
        method: "ssh",
        default: true,
        sshHosts: ["infra-1"],
      },
    ]);
    const status = JSON.parse(
      (await s.dh(["auth", "status", "-o", "json"])).stdout,
    );
    expect(status).toEqual({
      profile: "dhe.example.com:ssh",
      server: ORIGIN,
      kind: "enterprise",
      user: null,
      method: "ssh",
      sshHosts: ["infra-1"],
      clusterVersion: "2026.01.1g0",
    });
  });

  test("without the FRONTEND_* files, only a typed address with a port will do", async () => {
    const s = await setup(null);
    const bare = await s.dh([
      "-o",
      "json",
      "auth",
      "login",
      "dhe.example.com",
      "--method",
      "ssh",
      "--ssh-bundle",
      s.bundle,
      "--no-prompt",
    ]);
    expect(bare.code).toBe(2);
    expect(jsonError(bare.stderr).error).toBe(
      "Can't tell which port dhe.example.com's web server uses; give the full URL, e.g. https://dhe.example.com:8000",
    );
    expect((await s.config()).profiles ?? {}).toEqual({});
    const full = await s.login(ORIGIN);
    expect(full.code).toBe(0);
    expect(Object.keys((await s.config()).servers)).toEqual([ORIGIN]);
  });

  test("no bundle on the node is server_unsupported, and nothing is saved", async () => {
    const s = await setup();
    const r = await s.dh([
      "-o",
      "json",
      "auth",
      "login",
      "dhe.example.com",
      "--method",
      "ssh",
      "--no-prompt",
    ]);
    expect(r.code).toBe(5);
    expect(jsonError(r.stderr)).toMatchObject({
      code: "server_unsupported",
      error: "No dh plugins at /usr/illumon/latest/dh-cli on dhe.example.com",
    });
    expect(await s.config()).toEqual({});
  });

  test("an unreachable web server without a terminal is server_unreachable, with the SSH hint", async () => {
    const s = await setup();
    const r = await s.dh([
      "-o",
      "json",
      "auth",
      "login",
      "nowhere.invalid",
      "--no-prompt",
    ]);
    expect(r.code).toBe(5);
    expect(jsonError(r.stderr).hint).toBe(
      "Or set up SSH access: `dh auth login nowhere.invalid --method ssh`",
    );
  });

  test("commands that sign in say the profile has SSH access only", async () => {
    const s = await setup();
    await s.login("dhe.example.com");
    let error: unknown;
    try {
      await requireAuth(undefined, new Store(s.dir));
    } catch (e) {
      error = e;
    }
    expect(error).toBeInstanceOf(DhError);
    expect(error).toMatchObject({
      code: "auth_required",
      headline: 'Profile "dhe.example.com:ssh" has SSH access only',
      hint: "To sign in: `dh auth login --profile dhe.example.com:ssh`",
    });
  });

  test("logout removes the profile and keeps the server's ssh settings", async () => {
    const s = await setup();
    await s.login("dhe.example.com", "--ssh-host", "infra-1");
    const r = await s.dh(["auth", "logout", "dhe.example.com:ssh"]);
    expect(r.code).toBe(0);
    const c = await s.config();
    expect(c.profiles).toEqual({});
    expect(c.servers[ORIGIN].ssh).toEqual({
      enabled: true,
      hosts: ["infra-1"],
      bundle: s.bundle,
    });
  });
});
