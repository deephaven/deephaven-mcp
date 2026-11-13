import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import {
  chmod,
  copyFile,
  cp,
  mkdir,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import { join, resolve } from "node:path";
import { newKey } from "../src/actions/auth/enterprise.ts";
import type { Config } from "../src/actions/auth/store.ts";
import { digestOf } from "../src/actions/plugins/bundle.ts";
import { askSshSetup } from "../src/commands/auth/questions.ts";
import type { Prompter } from "../src/ui/prompt.ts";
import { writeManifest } from "./bundle-helpers.ts";
import { CLI, run, tempDir, WINDOWS } from "./helpers.ts";

setDefaultTimeout(30_000);

const EXAMPLE = resolve(import.meta.dir, "../examples/dh-cli");
const FIXTURES = resolve(import.meta.dir, "fixtures/plugins");

const SERVERS = [
  { name: "q1", host: "h1", port: 1, type: "DISPATCHER", serverClass: "Query" },
  { name: "m1", host: "h2", port: 2, type: "DISPATCHER", serverClass: "Merge" },
  { name: "g", host: "Group", port: 0, type: "GROUP", serverClass: "Query" },
];

/** The server's `irisapi.nocache.js`: an `iris` global whose client answers what the tests need. */
const irisapi = (groups: string[]) => `
globalThis.iris = {
  Client: class {
    static EVENT_CONNECT = "connect";
    constructor() {}
    addEventListener(_e, fn) { setTimeout(fn, 0); return () => {}; }
    async login(c) { if (JSON.stringify(c).includes("old")) throw new Error("bad password"); }
    async getUserInfo() { return { username: "u" }; }
    async getDbServers() { return ${JSON.stringify(SERVERS)}; }
    async getGroupsForUser() { return ${JSON.stringify(groups)}; }
    async getAuthConfigValues() { return []; }
    async getChallengeNonce() { return { nonce: "n", ipAddress: "127.0.0.1" }; }
    async challengeResponse() {}
    disconnect() {}
  },
};
`;

interface Request {
  path: string;
  ifNoneMatch: string | null;
}

const servers: { stop(force?: boolean): void }[] = [];
afterEach(() => {
  for (const s of servers.splice(0)) s.stop(true);
});

/**
 * A fake Enterprise server on localhost serving `<tmp>/served/js/` at
 * `/iriside/cli-plugins/` (with ETags) and a stub irisapi; a profile `p1`
 * on it (password `secret`, or `password`), its server's SSH enabled for a
 * copy of the example bundle, and the fake ssh on PATH.
 */
async function setup(
  opts: {
    groups?: string[];
    password?: string;
    kind?: string;
    /** No profile yet; the server entry only says where its bundle is. */
    loggedOut?: boolean;
    /** No profile and no server entry. */
    empty?: boolean;
  } = {},
) {
  const tmp = await tempDir("dh-plugin-js-test-");
  const served = join(tmp, "served");
  await cp(EXAMPLE, served, { recursive: true });
  const requests: Request[] = [];
  let groups = opts.groups ?? [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname;
      requests.push({ path, ifNoneMatch: req.headers.get("if-none-match") });
      if (path === "/iris/connection.json") return new Response("{}");
      if (path === "/irisapi/irisapi.nocache.js") {
        return new Response(irisapi(groups), {
          headers: { "content-type": "text/javascript" },
        });
      }
      const prefix = "/iriside/cli-plugins/";
      if (!path.startsWith(prefix)) return new Response(null, { status: 404 });
      const file = Bun.file(join(served, "js", path.slice(prefix.length)));
      if (!(await file.exists())) return new Response(null, { status: 404 });
      const bytes = await file.bytes();
      const etag = `"${digestOf([bytes])}"`;
      if (req.headers.get("if-none-match") === etag) {
        return new Response(null, { status: 304, headers: { etag } });
      }
      return new Response(bytes, { headers: { etag } });
    },
  });
  servers.push(server);
  const origin = `http://127.0.0.1:${server.port}`;
  const dir = join(tmp, "cli");
  await mkdir(dir, { recursive: true });
  await writeFile(
    join(dir, "config.json"),
    JSON.stringify({
      version: 1,
      servers: opts.empty
        ? {}
        : {
            [origin]: {
              kind: opts.kind ?? "enterprise",
              ssh: opts.loggedOut
                ? { bundle: served }
                : { enabled: true, bundle: served },
            },
          },
      ...(opts.loggedOut || opts.empty
        ? { profiles: {} }
        : {
            defaultProfile: "p_1",
            profiles: {
              p_1: {
                name: "p1",
                server: origin,
                user: "u",
                method: "password",
                createdAt: "2026-01-01T00:00:00.000Z",
              },
            },
          }),
    }),
  );
  await writeFile(
    join(dir, "credentials.json"),
    JSON.stringify({
      version: 1,
      credentials: {
        p_1: { type: "secret", value: opts.password ?? "secret" },
      },
    }),
    { mode: 0o600 },
  );
  const bin = join(tmp, "bin");
  await mkdir(bin);
  await copyFile(join(FIXTURES, "fake-ssh"), join(bin, "ssh"));
  await chmod(join(bin, "ssh"), 0o755);
  const sshLog = join(tmp, "ssh.log");
  return {
    tmp,
    served,
    origin,
    requests,
    setGroups(g: string[]) {
      groups = g;
    },
    /** Plugin requests (no irisapi) since the last call. */
    take(): Request[] {
      const out = requests.filter((r) => r.path.startsWith("/iriside/"));
      requests.length = 0;
      return out;
    },
    async sshRuns(): Promise<number> {
      const text = await readFile(sshLog, "utf8").catch(() => "");
      return text.split("\n").filter(Boolean).length;
    },
    dh: (args: string[], env: Record<string, string | undefined> = {}) =>
      run([...CLI, ...args], {
        DH_CONFIG_DIR: dir,
        DH_AUTO_UPDATE: "off",
        DH_PLUGIN_BUNDLE: undefined,
        DH_PLUGIN_REFRESH: undefined,
        DH_PROFILE: undefined,
        DH_SERVER: undefined,
        FAKE_SSH_LOG: sshLog,
        PATH: `${bin}:${process.env.PATH}`,
        ...env,
      }),
  };
}

/** Rewrites `<served>/js/dbserver/index.js` and the js part's manifest, as a rebuild would. */
async function rebuildDbserver(
  served: string,
  module: (old: string) => string,
) {
  const js = join(served, "js");
  const file = join(js, "dbserver", "index.js");
  await writeFile(file, module(await readFile(file, "utf8")));
  const manifest = JSON.parse(
    await readFile(join(js, "manifest.json"), "utf8"),
  );
  const [entry] = manifest.plugins;
  entry.digest = digestOf([
    await readFile(join(js, "dbserver", "plugin.json")),
    await readFile(file),
  ]);
  await writeManifest(js, "js", manifest.plugins, { shell: manifest.shell });
}

const jsonError = (stderr: string) =>
  JSON.parse(stderr.trim().split("\n").at(-1) ?? "");

describe.skipIf(WINDOWS)("js plugins", () => {
  test("plugin sync fetches both parts; unchanged, it says so for each", async () => {
    const { dh, take, origin } = await setup();
    const before = JSON.parse(
      (await dh(["plugin", "list", "-o", "json"])).stdout,
    );
    expect(before.plugins).toEqual([]);
    const first = await dh(["plugin", "sync"]);
    expect(first.code).toBe(0);
    expect(first.stdout).toBe(
      `js: updated from ${origin}: added dbserver\nshell: updated from 127.0.0.1: added ctl, deephaven\n`,
    );
    take();
    const again = await dh(["plugin", "sync"]);
    expect(again.stdout).toBe("js: unchanged\nshell: unchanged\n");
    expect(take().map((r) => r.path)).toEqual([
      "/iriside/cli-plugins/revision",
    ]);
    const agents = JSON.parse((await dh(["agents", "-o", "json"])).stdout);
    const db = agents.commands.find(
      (c: { command: string }) => c.command === "dh dbserver",
    );
    expect(db).toMatchObject({ plugin: "dbserver", pluginKind: "js" });
    expect(db.commands[0].output.kind).toBe("rows");
  });

  test("dbserver list: rows as JSON, a table for people, --class, and help without connecting", async () => {
    const { dh, take, requests } = await setup();
    await dh(["plugin", "sync"]);
    take();
    const json = await dh(["dbserver", "list", "-o", "json"]);
    expect(json.code).toBe(0);
    expect(JSON.parse(json.stdout)).toEqual(
      SERVERS.map((s) => ({
        name: s.name,
        host: s.host,
        port: s.port,
        type: s.type,
        class: s.serverClass,
      })),
    );
    // Discovery cost: one conditional GET revision.
    expect(take()).toEqual([
      {
        path: "/iriside/cli-plugins/revision",
        ifNoneMatch: expect.stringMatching(/^"sha256:/),
      },
    ]);
    const human = await dh(["dbserver", "list"]);
    expect(human.stdout.split("\n")[0]).toBe(
      "NAME  HOST   PORT  TYPE        CLASS",
    );
    expect(human.stdout).toContain("m1    h2     2     DISPATCHER  Merge");
    const merge = await dh([
      "dbserver",
      "list",
      "--class",
      "merge",
      "-o",
      "json",
    ]);
    expect(
      JSON.parse(merge.stdout).map((s: { name: string }) => s.name),
    ).toEqual(["m1"]);
    requests.length = 0;
    const help = await dh(["dbserver", "list", "--help"]);
    expect(help.stdout).toContain("--class <value>");
    expect(help.stdout).toContain(
      "Output (-o json): a list of rows, each with:",
    );
    expect(help.stdout).toContain("class (string): Server class.");
    expect(requests).toEqual([]);
  });

  test("a changed module is fetched alone, then dh runs again and imports it", async () => {
    const { dh, take, served } = await setup();
    await dh(["plugin", "sync"]);
    await rebuildDbserver(served, (old) =>
      old.replace("class: s.serverClass", 'class: s.serverClass + "!"'),
    );
    take();
    const r = await dh(["dbserver", "list", "-o", "json"]);
    expect(r.code).toBe(0);
    expect(JSON.parse(r.stdout)[0].class).toBe("Query!");
    expect(take().map((x) => x.path)).toEqual([
      "/iriside/cli-plugins/revision",
      "/iriside/cli-plugins/revision",
      "/iriside/cli-plugins/manifest.json",
      "/iriside/cli-plugins/dbserver/plugin.json",
      "/iriside/cli-plugins/dbserver/index.js",
      "/iriside/cli-plugins/revision",
    ]);
  });

  test("a plugin removed from the server is plugin_unavailable; no bundle at all says so", async () => {
    const { dh, served } = await setup();
    await dh(["plugin", "sync"]);
    const js = join(served, "js");
    const manifest = JSON.parse(
      await readFile(join(js, "manifest.json"), "utf8"),
    );
    await writeManifest(js, "js", [], { shell: manifest.shell });
    const r = await dh(["-o", "json", "dbserver", "list"]);
    expect(r.code).toBe(2);
    expect(jsonError(r.stderr).code).toBe("plugin_unavailable");
    await rm(js, { recursive: true });
    await dh(["plugin", "sync"]);
    expect((await dh(["plugin", "list"])).stdout).toContain(
      "this server provides no dh plugins",
    );
    expect((await dh(["auth"])).code).toBe(0);
  });

  test("groups: registered for a user in one, absent for one who isn't", async () => {
    const { dh, served, setGroups } = await setup({
      groups: ["iris-superusers"],
    });
    const js = join(served, "js");
    const manifest = JSON.parse(
      await readFile(join(js, "manifest.json"), "utf8"),
    );
    manifest.plugins[0].groups = ["iris-superusers"];
    await writeManifest(js, "js", manifest.plugins, { shell: manifest.shell });
    await dh(["plugin", "sync"]);
    expect((await dh(["dbserver", "list", "-o", "json"])).code).toBe(0);
    setGroups(["users"]);
    await dh(["plugin", "sync"]);
    expect((await dh(["dbserver", "list"])).code).toBe(2);
    const listed = JSON.parse(
      (await dh(["plugin", "list", "-o", "json"])).stdout,
    );
    expect(listed.plugins.map((p: { name: string }) => p.name)).not.toContain(
      "dbserver",
    );
    const agents = (await dh(["agents"])).stdout;
    expect(agents).not.toContain("dbserver");
  });

  test("the profile's rejected credentials give the same error as any command that signs in", async () => {
    const { dh } = await setup({ password: "old" });
    await dh(["plugin", "sync"]);
    const r = await dh(["-o", "json", "dbserver", "list"]);
    const status = await dh(["-o", "json", "auth", "status"]);
    expect(r.code).toBe(status.code);
    // Each names its own command.
    const { command: _a, ...plugin } = jsonError(r.stderr);
    const { command: _b, ...builtin } = jsonError(status.stderr);
    expect(plugin).toEqual(builtin);
  });

  test("a Community profile gets usage for an Enterprise plugin", async () => {
    const { dh, tmp } = await setup({ kind: "community" });
    const r = await dh(["-o", "json", "dbserver", "list"], {
      DH_PLUGIN_BUNDLE: EXAMPLE,
    });
    expect(r.code).toBe(2);
    expect(jsonError(r.stderr).error).toBe(
      '"dbserver" needs an Enterprise profile',
    );
    expect(tmp).toBeTruthy();
  });

  test("DH_PLUGIN_BUNDLE runs the example's dbserver against the profile's server, with no fetch", async () => {
    const { dh, take } = await setup();
    const r = await dh(["dbserver", "list", "-o", "json"], {
      DH_PLUGIN_BUNDLE: EXAMPLE,
    });
    expect(r.code).toBe(0);
    expect(JSON.parse(r.stdout)).toHaveLength(3);
    expect(take()).toEqual([]);
  });

  test("a module that throws is plugin_failed with its message; DH_DEBUG adds the stack", async () => {
    const { dh, served } = await setup();
    await rebuildDbserver(served, (old) =>
      old.replace(
        "const { client } = await ctx.enterprise();",
        'throw new Error("boom");',
      ),
    );
    const bundle = served;
    const r = await dh(["-o", "json", "dbserver", "list"], {
      DH_PLUGIN_BUNDLE: bundle,
    });
    expect(r.code).toBe(6);
    expect(jsonError(r.stderr)).toMatchObject({
      code: "plugin_failed",
      error: "boom",
    });
    expect(r.stderr).not.toContain("at ");
    const debug = await dh(["dbserver", "list"], {
      DH_PLUGIN_BUNDLE: bundle,
      DH_DEBUG: "1",
    });
    expect(debug.stderr).toContain("Error: boom");
    expect(debug.stderr).toContain("index.js");
  });

  test("a module whose files don't match the digest isn't imported", async () => {
    const { dh, served } = await setup();
    await writeFile(
      join(served, "js", "dbserver", "index.js"),
      "export default {};\n",
    );
    const r = await dh(["-o", "json", "dbserver", "list"], {
      DH_PLUGIN_BUNDLE: served,
    });
    expect(r.code).toBe(2);
    expect(jsonError(r.stderr)).toMatchObject({ code: "plugin_invalid" });
  });

  test("Ctrl-C aborts ctx.signal, and dh exits once run settles", async () => {
    const { served, tmp } = await setup();
    await rebuildDbserver(served, (old) =>
      old.replace(
        "const { client } = await ctx.enterprise();",
        `ctx.progress("waiting");
        await new Promise((r) => ctx.signal.addEventListener("abort", r));
        ctx.progress("aborted");
        return [];
        const { client } = await ctx.enterprise();`,
      ),
    );
    const proc = Bun.spawn([...CLI, "dbserver", "list"], {
      env: {
        ...process.env,
        DH_CONFIG_DIR: join(tmp, "cli"),
        DH_AUTO_UPDATE: "off",
        DH_PLUGIN_BUNDLE: served,
      },
      stdout: "pipe",
      stderr: "pipe",
    });
    const reader = proc.stderr.getReader();
    let stderr = "";
    while (!stderr.includes("waiting")) {
      const { value, done } = await reader.read();
      if (done) break;
      stderr += new TextDecoder().decode(value);
    }
    proc.kill("SIGINT");
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      stderr += new TextDecoder().decode(value);
    }
    expect(await proc.exited).toBe(3);
    expect(stderr).toContain("aborted");
  });

  test("plugin check: sign-in, revision and module", async () => {
    const { dh } = await setup();
    await dh(["plugin", "sync"]);
    const r = await dh(["plugin", "check", "dbserver", "-o", "json"]);
    expect(r.code).toBe(0);
    expect(
      JSON.parse(r.stdout).checks.map((c: { step: string }) => c.step),
    ).toEqual(["sign-in", "revision", "module"]);
  });
});

/** A key file for `user`, as Enterprise writes them. */
async function keyFile(dir: string, user: string): Promise<string> {
  const key = newKey();
  const ec = (b64: string) =>
    Buffer.concat([Buffer.from("EC:"), Buffer.from(b64, "base64")]).toString(
      "base64",
    );
  const file = join(dir, "key.txt");
  await writeFile(
    file,
    `user ${user}\npublic ${ec(key.publicKey)}\nprivate ${ec(key.privateKey)}\n`,
  );
  return file;
}

describe.skipIf(WINDOWS)("dh auth login and plugins", () => {
  const login = async (
    s: Awaited<ReturnType<typeof setup>>,
    ...extra: string[]
  ) =>
    await s.dh([
      "auth",
      "login",
      s.origin,
      "--method",
      "private-key",
      "--private-key-file",
      await keyFile(s.tmp, "u"),
      "--no-prompt",
      ...extra,
    ]);

  test("login fetches the js plugins; the shell ones are named, not registered", async () => {
    const s = await setup({ loggedOut: true });
    const r = await login(s);
    expect(r.code).toBe(0);
    expect(r.stdout).toContain("✔ Plugins: dbserver");
    const agents = JSON.parse((await s.dh(["agents", "-o", "json"])).stdout);
    const nouns = agents.commands.map((c: { command: string }) => c.command);
    expect(nouns).toContain("dh dbserver");
    expect(nouns).not.toContain("dh ctl");
    expect(agents.pluginChannels).toEqual([
      expect.objectContaining({
        channel: "js",
        source: "web",
        server: s.origin,
      }),
      expect.objectContaining({ channel: "shell", reason: "SSH not enabled" }),
    ]);
    const list = (await s.dh(["plugin", "list"])).stdout.trimEnd().split("\n");
    expect(list.at(-1)).toBe(
      "Over SSH, this server also has: ctl, deephaven. `dh plugin ssh enable` to use them.",
    );
    expect(await s.sshRuns()).toBe(0);
  });

  test("--plugins ssh checks the connection and copies; --plugins none records the decline", async () => {
    const yes = await setup({ loggedOut: true });
    const r = await login(yes, "--plugins", "ssh");
    expect(r.code).toBe(0);
    expect(r.stdout).toContain("✔ connect  127.0.0.1 in");
    expect(r.stdout).toContain("✔ SSH plugins: ctl, deephaven");
    const json = JSON.parse((await yes.dh(["agents", "-o", "json"])).stdout);
    expect(json.commands.map((c: { command: string }) => c.command)).toContain(
      "dh ctl",
    );

    const no = await setup({ loggedOut: true });
    const declined = await login(no, "--plugins", "none", "-o", "json");
    expect(declined.code).toBe(0);
    expect(JSON.parse(declined.stdout).plugins).toEqual({
      js: ["dbserver"],
      ssh: "declined",
    });
    const config = JSON.parse(
      await readFile(join(no.tmp, "cli", "config.json"), "utf8"),
    );
    expect(config.servers[no.origin].ssh.enabled).toBe(false);
    expect(await no.sshRuns()).toBe(0);
  });

  test("a server without the js part logs in fine, with nothing to ask", async () => {
    const s = await setup({ loggedOut: true });
    await rm(join(s.served, "js"), { recursive: true });
    const r = await login(s, "-o", "json");
    expect(r.code).toBe(0);
    expect(JSON.parse(r.stdout).plugins).toEqual({ js: null, ssh: null });
  });
});

describe("the login SSH question", () => {
  const ui = (answer: boolean) => {
    const asked: string[] = [];
    return {
      asked,
      ui: {
        interactive: true,
        confirm: async (m: string) => {
          asked.push(m);
          return answer;
        },
      } as unknown as Prompter,
    };
  };
  const origin = "https://dhe.example.com:8123";
  const config = (ssh?: Record<string, unknown>) =>
    ({
      version: 1,
      servers: { [origin]: { kind: "enterprise", ...(ssh ? { ssh } : {}) } },
      profiles: {},
    }) as Config;
  const shell = [{ name: "ctl" }, { name: "cluster" }, { name: "iriscat" }];

  test("asks once, naming the shell plugins and the host", async () => {
    const a = ui(true);
    expect(await askSshSetup(a.ui, config(), origin, shell, undefined)).toBe(
      "ssh",
    );
    expect(a.asked).toEqual([
      "This server also has admin tools over SSH (ctl, cluster, iriscat). Set up SSH to dhe.example.com?",
    ]);
    const b = ui(false);
    expect(await askSshSetup(b.ui, config(), origin, shell, undefined)).toBe(
      "none",
    );
  });

  test("isn't asked with no shell plugins, once answered, or with the flag", async () => {
    const a = ui(true);
    expect(await askSshSetup(a.ui, config(), origin, [], undefined)).toBe(
      undefined,
    );
    expect(
      await askSshSetup(
        a.ui,
        config({ enabled: false }),
        origin,
        shell,
        undefined,
      ),
    ).toBe(undefined);
    expect(await askSshSetup(a.ui, config(), origin, shell, "none")).toBe(
      "none",
    );
    expect(a.asked).toEqual([]);
  });
});

describe.skipIf(WINDOWS)("adding a sign-in to an SSH-only profile", () => {
  const sshLogin = (
    s: Awaited<ReturnType<typeof setup>>,
    address: string,
    env: Record<string, string> = {},
  ) =>
    s.dh(
      [
        "auth",
        "login",
        address,
        "--method",
        "ssh",
        "--ssh-bundle",
        s.served,
        "--no-prompt",
      ],
      env,
    );
  const keyLogin = async (
    s: Awaited<ReturnType<typeof setup>>,
    ...args: string[]
  ) =>
    await s.dh([
      "auth",
      "login",
      ...args,
      "--method",
      "private-key",
      "--private-key-file",
      await keyFile(s.tmp, "u"),
      "--no-prompt",
    ]);
  const config = async (s: Awaited<ReturnType<typeof setup>>) =>
    JSON.parse(await readFile(join(s.tmp, "cli", "config.json"), "utf8"));
  const nouns = async (s: Awaited<ReturnType<typeof setup>>) =>
    JSON.parse((await s.dh(["agents", "-o", "json"])).stdout).commands.map(
      (c: { command: string }) => c.command,
    );

  test("--profile adds the sign-in: same name, ssh settings kept, js plugins registered", async () => {
    const s = await setup({ loggedOut: true });
    expect((await sshLogin(s, s.origin)).code).toBe(0);
    expect(await nouns(s)).toContain("dh ctl");
    expect(await nouns(s)).not.toContain("dh dbserver");
    await s.dh(["plugin", "sync"]);
    expect((await s.dh(["plugin", "list"])).stdout).toContain(
      "Sign in to use this server's other plugins (dbserver): `dh auth login --profile 127.0.0.1:ssh`",
    );
    const sshBefore = (await config(s)).servers[s.origin].ssh;
    const r = await keyLogin(s, "--profile", "127.0.0.1:ssh");
    expect(r.code).toBe(0);
    const c = await config(s);
    expect(Object.values(c.profiles)).toEqual([
      expect.objectContaining({
        name: "127.0.0.1:ssh",
        server: s.origin,
        user: "u",
        method: "private-key",
      }),
    ]);
    expect(c.servers[s.origin].ssh).toEqual(sshBefore);
    expect(await nouns(s)).toContain("dh dbserver");
    expect(await nouns(s)).toContain("dh ctl");
  });

  test("a sign-in through another address re-keys the server entry and its caches", async () => {
    const s = await setup({ empty: true });
    const node = join(s.tmp, "node");
    await mkdir(join(node, "etc", "deephaven"), { recursive: true });
    await writeFile(
      join(node, "etc", "deephaven", "FRONTEND_HOST"),
      "dhe.example.com\n",
    );
    await writeFile(join(node, "etc", "deephaven", "FRONTEND_PORT"), "8000\n");
    const before = "https://dhe.example.com:8000";
    expect(
      (await sshLogin(s, "dhe.example.com", { FAKE_SSH_ROOT: node })).code,
    ).toBe(0);
    expect(Object.keys((await config(s)).servers)).toEqual([before]);
    const r = await keyLogin(s, "--profile", "dhe.example.com:ssh", s.origin);
    expect(r.code).toBe(0);
    const c = await config(s);
    expect(Object.keys(c.servers)).toEqual([s.origin]);
    expect(c.servers[s.origin].ssh).toEqual({
      enabled: true,
      bundle: s.served,
    });
    expect(Object.values(c.profiles)).toEqual([
      expect.objectContaining({
        name: "dhe.example.com:ssh",
        server: s.origin,
        user: "u",
      }),
    ]);
    const cache = join(s.tmp, "cli", "plugins", "cache");
    expect(
      await Bun.file(
        join(cache, encodeURIComponent(s.origin), "shell", "state.json"),
      ).exists(),
    ).toBe(true);
    expect(
      await Bun.file(
        join(cache, encodeURIComponent(before), "shell", "state.json"),
      ).exists(),
    ).toBe(false);
    expect(await nouns(s)).toContain("dh ctl");
  });

  test("re-keying onto an origin that's already saved is usage", async () => {
    const s = await setup({ loggedOut: true });
    const node = join(s.tmp, "node");
    await mkdir(join(node, "etc", "deephaven"), { recursive: true });
    await writeFile(
      join(node, "etc", "deephaven", "FRONTEND_HOST"),
      "dhe.example.com\n",
    );
    await writeFile(join(node, "etc", "deephaven", "FRONTEND_PORT"), "8000\n");
    await sshLogin(s, "dhe.example.com", { FAKE_SSH_ROOT: node });
    const r = await keyLogin(
      s,
      "-o",
      "json",
      "--profile",
      "dhe.example.com:ssh",
      s.origin,
    );
    expect(r.code).toBe(2);
    expect(jsonError(r.stderr).error).toBe(
      `Both https://dhe.example.com:8000 and ${s.origin} are saved; log out of one first`,
    );
  });
});
