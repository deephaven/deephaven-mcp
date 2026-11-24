import { expect, test } from "bun:test";
import {
  chmod,
  mkdir,
  readdir,
  readFile,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { join } from "node:path";
import { isErrno } from "../src/errors.ts";
import { BIN, VERSION } from "../src/version.ts";
import {
  CLI,
  http2OnlyEnterprise,
  run,
  TEST_CA,
  tempDir,
  WINDOWS,
} from "./helpers.ts";

async function setup() {
  const tmp = await tempDir("dh-cli-test-");
  return {
    tmp,
    configDir: join(tmp, "cli"),
    run: (args: string[], env: Record<string, string> = {}, stdin?: string) =>
      run(
        [...CLI, ...args],
        {
          DH_CONFIG_DIR: join(tmp, "cli"),
          DH_AI_DATA_DIR: join(tmp, "legacy"),
          DH_AUTO_UPDATE: "off",
          DH_PROFILE: "",
          DH_SERVER: "",
          ...env,
        },
        stdin,
      ),
  };
}

async function within<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Timed out in test")), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function waitForSso(stream: ReadableStream<Uint8Array>) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let text = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) throw new Error(`CLI exited before SSO: ${text}`);
      text += decoder.decode(value, { stream: true });
      const pid = /SSO_TEST_PID=(\d+)\r?\n/.exec(text)?.[1];
      if (pid) {
        return {
          pid: Number(pid),
          output: text,
          async readRemaining() {
            try {
              for (;;) {
                const next = await reader.read();
                if (next.done) return text;
                text += decoder.decode(next.value, { stream: true });
              }
            } finally {
              reader.releaseLock();
            }
          },
        };
      }
    }
  } catch (error) {
    reader.releaseLock();
    throw error;
  }
}

function isRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    if (isErrno(e, "ESRCH")) return false;
    throw e;
  }
}

function stopProcess(pid: number): void {
  try {
    process.kill(pid, "SIGKILL");
  } catch (e) {
    if (!isErrno(e, "ESRCH")) throw e;
  }
}

interface Extra {
  servers?: Record<string, unknown>;
  profiles?: Record<string, unknown>;
  credentials?: Record<string, unknown>;
}

/** Writes a config with two Community profiles that need no network, plus `extra`. */
async function seed(configDir: string, extra: Extra = {}) {
  await mkdir(configDir, { recursive: true });
  await writeFile(
    join(configDir, "config.json"),
    JSON.stringify({
      version: 1,
      defaultProfile: "p_a",
      servers: {
        "http://localhost:10000": { kind: "community" },
        "http://localhost:10001": { kind: "community" },
        ...extra.servers,
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
        ...extra.profiles,
      },
    }),
  );
  await writeFile(
    join(configDir, "credentials.json"),
    JSON.stringify({
      version: 1,
      credentials: {
        p_a: { type: "envRef", env: "UNSET_TEST_PSK" },
        ...extra.credentials,
      },
    }),
    { mode: 0o600 },
  );
  if (!WINDOWS) await chmod(join(configDir, "credentials.json"), 0o600);
}

test("cli: no profiles", async () => {
  const { run } = await setup();
  const list = await run(["auth", "-o", "json"]);
  expect(list.code).toBe(0);
  expect(JSON.parse(list.stdout)).toEqual([]);

  const status = await run(["auth", "status", "-o", "json"]);
  expect(status.code).toBe(4);
  expect(JSON.parse(status.stderr)).toMatchObject({
    type: "error",
    code: "auth_required",
    command: "auth status",
  });

  const bare = await run([]);
  expect(bare.code).toBe(0);
  expect(bare.stdout).toContain(`Run \`${BIN} auth login\``);
});

test("cli: console commands and errors are discoverable to agents", async () => {
  const { run } = await setup();
  const help = await run(["console", "--help"]);
  expect(help.code).toBe(0);
  expect(help.stdout).toContain("create");
  expect(help.stdout).toContain("default");
  expect(help.stdout).toContain("delete");

  const agents = await run(["agents", "-o", "json"]);
  expect(agents.code).toBe(0);
  const manifest = JSON.parse(agents.stdout);
  const console = manifest.commands.find(
    (command: { command: string }) => command.command === "dh console",
  );
  expect(
    console.commands.map((command: { command: string }) => command.command),
  ).toEqual([
    "dh console detail",
    "dh console use",
    "dh console create",
    "dh console default",
    "dh console delete",
  ]);
  expect(manifest.errors.map((error: { code: string }) => error.code)).toEqual(
    expect.arrayContaining([
      "console_required",
      "console_not_found",
      "console_forbidden",
      "console_not_ready",
      "console_unsupported",
      "console_failed",
    ]),
  );
});

test("cli: usage errors exit 2, with JSON on stderr", async () => {
  const { run } = await setup();
  const login = await run(["auth", "login", "--no-prompt", "-o", "json"]);
  expect(login.code).toBe(2);
  const error = JSON.parse(login.stderr);
  expect(error.code).toBe("usage");
  expect(error.error).toContain("<server>");
  expect(login.stdout).toBe("");

  const unknown = await run(["auth", "bogus"]);
  expect(unknown.code).toBe(2);
  expect(unknown.stderr).toContain('Unknown command or extra argument "bogus"');
  expect(unknown.stderr).toContain("dh auth --help");

  const extra = await run(["auth", "status", "extra"]);
  expect(extra.code).toBe(2);
  expect(extra.stderr).toContain('Unknown command or extra argument "extra"');
  expect(extra.stderr).toContain("dh auth status --help");
});

test("cli: SSO no longer has a timeout option", async () => {
  const { run } = await setup();
  const help = await run(["auth", "login", "--help"]);
  expect(help.code).toBe(0);
  expect(help.stdout).not.toContain("--timeout");

  const removed = await run(["auth", "login", "--timeout", "1", "-o", "json"]);
  expect(removed.code).toBe(2);
  expect(JSON.parse(removed.stderr).code).toBe("usage");
  expect(removed.stderr).toContain("--timeout");
});

test.skipIf(WINDOWS)(
  "cli: SSO waits beyond DH_TIMEOUT and responds to interrupts, including CA re-runs",
  async () => {
    const { tmp, configDir } = await setup();
    const api = `
      class Client {
        static EVENT_CONNECT = "connect";
        addEventListener(_type, listener) {
          queueMicrotask(listener);
          return () => {};
        }
        async getAuthConfigValues() {
          return [
            ["authentication.client.customlogin.class.SAMLAuth", "enabled"],
            ["authentication.client.samlauth.login.url", "/saml"],
            ["authentication.passwordsEnabled", "false"]
          ];
        }
        login() {
          process.stderr.write("SSO_TEST_PID=" + process.pid + "\\n");
          setInterval(() => {}, 1000);
          return new Promise(() => {});
        }
        disconnect() {}
      }
      globalThis.iris = { Client };
    `;
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch(req) {
        const path = new URL(req.url).pathname;
        if (path === "/iris/connection.json") return new Response("{}");
        if (path === "/irisapi/irisapi.nocache.js") return new Response(api);
        return new Response(null, { status: 404 });
      },
    });

    try {
      for (const signal of ["SIGINT", "SIGTERM"] as const) {
        for (const ca of [false, true]) {
          for (const format of ["json", "human"]) {
            const proc = Bun.spawn(
              [
                ...CLI,
                "auth",
                "login",
                `http://127.0.0.1:${server.port}`,
                "--method",
                "saml",
                "--no-browser",
                "--no-prompt",
                "-o",
                format,
                ...(ca ? ["--ca-cert", TEST_CA] : []),
              ],
              {
                env: {
                  ...process.env,
                  DH_CONFIG_DIR: configDir,
                  DH_AI_DATA_DIR: join(tmp, "legacy"),
                  DH_AUTO_UPDATE: "off",
                  DH_PROFILE: "",
                  DH_SERVER: "",
                  DH_TIMEOUT: "1",
                  DH_LOADED_CA_CERT: "",
                  NODE_EXTRA_CA_CERTS: ca ? TEST_CA : "",
                  TMPDIR: tmp,
                },
                stdin: "ignore",
                stdout: "ignore",
                stderr: "pipe",
              },
            );
            let ssoPid: number | undefined;
            try {
              const progress = await within(waitForSso(proc.stderr), 5_000);
              ssoPid = progress.pid;
              if (format === "json") {
                expect(progress.output).toContain("Waiting for sign-in");
                expect(progress.output).toContain("Ctrl-C to cancel");
                expect(progress.output).toContain("key=");
              }
              expect(progress.output).not.toContain("--timeout");
              expect(ssoPid === proc.pid).toBe(!ca);
              if (signal === "SIGINT" && !ca) await Bun.sleep(1_200);
              expect(proc.exitCode).toBeNull();
              expect(isRunning(ssoPid)).toBe(true);
              if (ca) {
                expect(
                  (await readdir(tmp)).filter((name) =>
                    name.startsWith(`${BIN}-ca-`),
                  ),
                ).toHaveLength(1);
              }

              const interruptedAt = Date.now();
              proc.kill(signal);
              const exit = await within(proc.exited, 3_000);
              expect(Date.now() - interruptedAt).toBeLessThan(2_000);
              expect(exit).toBe(3);
              const reportText = await progress.readRemaining();
              if (format === "human") {
                expect(reportText).toBe(
                  `SSO_TEST_PID=${ssoPid}\n✖ Cancelled while waiting for SSO sign-in at http://127.0.0.1:${server.port}.\n`,
                );
              } else {
                const reportLine = reportText
                  .split("\n")
                  .find((line) => line.startsWith('{"type":"error"'));
                expect(reportLine).toBeDefined();
                const signInLink = progress.output.match(
                  /Sign in at:\s+(https?:\/\/\S+)/,
                )?.[1];
                expect(signInLink).toBeDefined();
                const nonce = new URL(signInLink ?? "").searchParams.get("key");
                expect(nonce).toBeTruthy();
                expect(reportLine).not.toContain(nonce ?? "");
                expect(reportLine).not.toContain(
                  encodeURIComponent(nonce ?? ""),
                );
                expect(JSON.parse(reportLine ?? "")).toMatchObject({
                  type: "error",
                  error: "Interrupted",
                  code: "cancelled",
                  exit: 3,
                  command: "auth login",
                  detail: `Interrupted by ${signal}`,
                  operation: "waiting for SSO sign-in",
                  phase: "signing_in",
                  target: `http://127.0.0.1:${server.port}`,
                });
              }
              expect(isRunning(ssoPid)).toBe(false);
              expect(
                await Bun.file(join(configDir, "config.json")).exists(),
              ).toBe(false);
              expect(
                await Bun.file(join(configDir, "credentials.json")).exists(),
              ).toBe(false);
              expect(
                (await readdir(tmp)).filter((name) =>
                  name.startsWith(`${BIN}-ca-`),
                ),
              ).toHaveLength(0);
            } finally {
              if (proc.exitCode === null) proc.kill("SIGKILL");
              await proc.exited;
              if (
                ssoPid !== undefined &&
                ssoPid !== proc.pid &&
                isRunning(ssoPid)
              ) {
                stopProcess(ssoPid);
              }
            }
          }
        }
      }
      if (process.platform === "darwin" && Bun.which("python3")) {
        const terminal = `
import os, pty, select, sys
pid, fd = pty.fork()
if pid == 0:
    os.execvp(sys.argv[1], sys.argv[1:])
while True:
    ready, _, _ = select.select([fd, 0], [], [])
    if fd in ready:
        try:
            data = os.read(fd, 4096)
        except OSError:
            break
        if not data:
            break
        os.write(1, data)
    if 0 in ready:
        data = os.read(0, 4096)
        if data:
            os.write(fd, data)
_, status = os.waitpid(pid, 0)
sys.exit(os.waitstatus_to_exitcode(status))
`;
        const proc = Bun.spawn(
          [
            "python3",
            "-c",
            terminal,
            ...CLI,
            "auth",
            "login",
            `http://127.0.0.1:${server.port}`,
            "--method",
            "saml",
            "--no-browser",
            "--no-prompt",
            "-o",
            "json",
            "--ca-cert",
            TEST_CA,
          ],
          {
            env: {
              ...process.env,
              DH_CONFIG_DIR: configDir,
              DH_AI_DATA_DIR: join(tmp, "legacy"),
              DH_AUTO_UPDATE: "off",
              DH_PROFILE: "",
              DH_SERVER: "",
              DH_LOADED_CA_CERT: "",
              NODE_EXTRA_CA_CERTS: TEST_CA,
              TMPDIR: tmp,
            },
            stdin: "pipe",
            stdout: "pipe",
            stderr: "pipe",
          },
        );
        let ssoPid: number | undefined;
        let parentPid: number | undefined;
        try {
          const progress = await within(waitForSso(proc.stdout), 5_000);
          ssoPid = progress.pid;
          parentPid = Number(
            Bun.spawnSync(["ps", "-o", "ppid=", "-p", String(ssoPid)])
              .stdout.toString()
              .trim(),
          );
          expect(parentPid).toBeGreaterThan(1);
          const stopped = (pid: number) =>
            Bun.spawnSync(["ps", "-o", "state=", "-p", String(pid)])
              .stdout.toString()
              .trim()
              .startsWith("T");
          const waitForState = async (wantStopped: boolean) => {
            const deadline = Date.now() + 3_000;
            while (
              stopped(parentPid ?? 0) !== wantStopped ||
              stopped(ssoPid ?? 0) !== wantStopped
            ) {
              if (Date.now() >= deadline)
                throw new Error(
                  `Timed out waiting for job-control state: parent=${stopped(parentPid ?? 0)}, child=${stopped(ssoPid ?? 0)}, expected=${wantStopped}`,
                );
              await Bun.sleep(10);
            }
          };
          process.kill(parentPid, "SIGTSTP");
          await waitForState(true);
          process.kill(parentPid, "SIGCONT");
          await waitForState(false);
          proc.stdin.write("\x03");
          proc.stdin.flush();
          expect(await within(proc.exited, 3_000)).toBe(3);
          const reportText = await progress.readRemaining();
          const reportLine = reportText
            .split(/\r?\n/)
            .find((line) => line.includes('{"type":"error"'));
          expect(reportLine).toBeDefined();
          expect(
            JSON.parse((reportLine ?? "").slice(reportLine?.indexOf("{"))),
          ).toMatchObject({
            code: "cancelled",
            detail: "Interrupted by SIGINT",
            operation: "waiting for SSO sign-in",
          });
          expect(isRunning(ssoPid)).toBe(false);
          expect(
            (await readdir(tmp)).filter((name) =>
              name.startsWith(`${BIN}-ca-`),
            ),
          ).toHaveLength(0);
        } finally {
          if (proc.exitCode === null) proc.kill("SIGKILL");
          await proc.exited;
          if (parentPid !== undefined && isRunning(parentPid))
            stopProcess(parentPid);
          if (ssoPid !== undefined && isRunning(ssoPid)) stopProcess(ssoPid);
        }
      }
    } finally {
      await server.stop(true);
      await rm(tmp, { recursive: true, force: true });
    }
  },
  15_000,
);

test.skipIf(WINDOWS)(
  "cli: interrupted migration reports earlier saved profiles",
  async () => {
    const { tmp, configDir } = await setup();
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch(req) {
        if (new URL(req.url).pathname === "/irisapi/irisapi.nocache.js") {
          return new Response(`
            globalThis.iris = { Client: class {
              static EVENT_CONNECT = "connect";
              addEventListener() {
                process.stderr.write("SSO_TEST_PID=" + process.pid + "\\n");
                return () => {};
              }
              disconnect() {}
            }};
          `);
        }
        return new Response(null, { status: 404 });
      },
    });
    const legacy = join(tmp, "legacy.json");
    await writeFile(
      legacy,
      JSON.stringify({
        community: {
          sessions: {
            first: {
              host: "localhost",
              auth_type: "PSK",
              auth_token_env_var: "DH_TEST_MIGRATION_MISSING",
            },
          },
        },
        enterprise: {
          systems: {
            second: {
              connection_json_url: `http://127.0.0.1:${server.port}`,
              auth_type: "password",
              username: "alice",
              password: "test",
            },
          },
        },
      }),
    );
    const proc = Bun.spawn(
      [...CLI, "auth", "migrate", "--from", legacy, "--yes", "-o", "json"],
      {
        env: {
          ...process.env,
          DH_CONFIG_DIR: configDir,
          DH_AUTO_UPDATE: "off",
          DH_PROFILE: "",
          DH_SERVER: "",
          DH_TEST_MIGRATION_MISSING: "",
        },
        stdin: "ignore",
        stdout: "ignore",
        stderr: "pipe",
      },
    );
    try {
      const progress = await within(waitForSso(proc.stderr), 5_000);
      proc.kill("SIGINT");
      expect(await within(proc.exited, 3_000)).toBe(3);
      const reportText = await progress.readRemaining();
      const reportLine = reportText
        .split("\n")
        .find((line) => line.startsWith('{"type":"error"'));
      expect(reportLine).toBeDefined();
      expect(JSON.parse(reportLine ?? "")).toMatchObject({
        code: "cancelled",
        operation: "connecting",
        progress: "Migrated 1 profile.",
        recoveryCommand: [BIN, "auth"],
      });
      const config = JSON.parse(
        await readFile(join(configDir, "config.json"), "utf8"),
      );
      expect(Object.keys(config.profiles)).toHaveLength(1);
    } finally {
      if (proc.exitCode === null) proc.kill("SIGKILL");
      await proc.exited;
      await server.stop(true);
      await rm(tmp, { recursive: true, force: true });
    }
  },
  10_000,
);

test("cli: -o follows commander: the last one wins, and nothing after --", async () => {
  const { run } = await setup();
  const last = await run(["auth", "-o", "json", "-o", "human"]);
  expect(last.stdout).toContain("No profiles.");
  const literal = await run(["auth", "use", "--", "--output=json"]);
  expect(literal.code).toBe(4);
  expect(literal.stderr).toStartWith("✖");
});

test("cli: bare dh -o json prints one JSON value", async () => {
  const { run, configDir } = await setup();
  const none = await run(["-o", "json"]);
  expect(none.code).toBe(0);
  expect(JSON.parse(none.stdout).default).toBeNull();

  await seed(configDir);
  const some = JSON.parse((await run(["-o", "json"])).stdout);
  expect(some.default).toEqual({
    profile: "a",
    server: "http://localhost:10000",
    user: "psk",
  });
});

test("cli: a profile named like a flag, after --", async () => {
  const { run, configDir } = await setup();
  await seed(configDir);
  expect((await run(["auth", "rename", "--", "b", "--all"])).code).toBe(0);
  const out = await run(["auth", "logout", "-o", "json", "--", "--all"]);
  expect(out.code, out.stderr).toBe(0);
  expect(JSON.parse(out.stdout).removed).toEqual([
    { profile: "--all", server: "http://localhost:10001", revoked: false },
  ]);
});

test("cli: migrate refuses a remote plain-HTTP server even when it can't sign in yet", async () => {
  const { run, tmp } = await setup();
  const legacy = join(tmp, "v1.json");
  await writeFile(
    legacy,
    JSON.stringify({
      community: {
        sessions: {
          remote: {
            host: "dhc.example.com",
            port: 10000,
            auth_type: "PSK",
            auth_token_env_var: "UNSET_TEST_PSK",
          },
        },
      },
    }),
  );
  const res = await run([
    "auth",
    "migrate",
    "--from",
    legacy,
    "--yes",
    "-o",
    "json",
  ]);
  expect(JSON.parse(res.stdout)).toEqual({ migrated: 0, failed: 1 });
  expect(res.stderr).toContain("Refusing to load client code over plain HTTP");
  expect(res.code).toBe(4);
  expect(JSON.parse(res.stderr.trim().split("\n").at(-1) ?? "").code).toBe(
    "migration_failed",
  );

  // Login's first-run offer: with nothing migrated, it goes on to a normal sign-in.
  const login = await run(
    ["auth", "login", "--yes", "--no-prompt", "-o", "json"],
    { DH_MCP_CONFIG_FILE: legacy },
  );
  expect(login.stderr).toContain(
    "Refusing to load client code over plain HTTP",
  );
  expect(login.stdout).toBe("");
  expect(login.code, login.stderr).toBe(2);
  expect(login.stderr).toContain("<server>");
});

test("[channels-cli-4] cli: agents summarizes, and lists every command and error code as JSON", async () => {
  const { run } = await setup();
  const summary = await run(["agents"]);
  expect(summary.code).toBe(0);
  expect(summary.stdout).toContain(`${BIN} auth login [server]`);
  expect(summary.stdout).toContain("auth_required");
  expect(summary.stdout).toContain("Required: type, error, code, exit");
  expect(summary.stdout).toContain(
    "Optional: command, detail, hint, cleanupDetail",
  );
  expect(summary.stdout).toContain("observation, recoveryCommand");

  const res = await run(["agents", "-o", "json"]);
  expect(res.code).toBe(0);
  const manifest = JSON.parse(res.stdout);
  const requiredFields = ["type", "error", "code", "exit"];
  const optionalFields = [
    "command",
    "detail",
    "hint",
    "cleanupDetail",
    "target",
    "profile",
    "operation",
    "phase",
    "elapsedSeconds",
    "progress",
    "outcome",
    "continuation",
    "retryWarning",
    "observation",
    "recoveryCommand",
  ];
  expect(manifest.errorFormat).toEqual({
    stream: "stderr",
    when: "-o json",
    fields: [...requiredFields, ...optionalFields],
    requiredFields,
    optionalFields,
  });
  const auth = manifest.commands.find(
    (c: { command: string }) => c.command === `${BIN} auth`,
  );
  expect(auth.commands.map((c: { command: string }) => c.command)).toContain(
    `${BIN} auth login`,
  );
  const channel = manifest.commands.find(
    (c: { command: string }) => c.command === `${BIN} channel`,
  );
  expect(channel.commands.map((c: { command: string }) => c.command)).toEqual([
    `${BIN} channel use`,
  ]);
  const login = auth.commands.find(
    (c: { command: string }) => c.command === `${BIN} auth login`,
  );
  expect(login.options.map((o: { flags: string }) => o.flags)).not.toContain(
    "--timeout <seconds>",
  );
  const codes = manifest.errors.map((e: { code: string }) => e.code);
  for (const code of [
    "auth_required",
    "auth_failed",
    "auth_expired",
    "credential_unavailable",
    "key_upload_failed",
    "server_unreachable",
    "channel_not_found",
  ]) {
    expect(codes).toContain(code);
  }
});

test("[channels-cli-2] [channels-cli-4] cli: from source, the build channel is dev and channels can't be switched", async () => {
  const { run } = await setup();
  const help = await run(["--help"]);
  expect(help.stdout).toContain("Channel: dev (config ");

  const root = JSON.parse((await run(["-o", "json"])).stdout);
  expect(root.channel).toBe("dev");

  const shown = await run(["channel"]);
  expect(shown.code).toBe(0);
  expect(shown.stdout).toBe("dev (never updates)\n");
  expect(JSON.parse((await run(["channel", "-o", "json"])).stdout)).toEqual({
    channels: [],
    build: "dev",
    version: VERSION,
  });

  // Alpha channels may be described, but none is ever named to people or agents.
  const helpTexts = await Promise.all(
    [
      ["channel", "--help"],
      ["channel", "use", "--help"],
      ["agents", "-o", "json"],
    ].map(async (args) => (await run(args)).stdout),
  );
  for (const text of helpTexts) expect(text).not.toMatch(/alpha-[a-z0-9]/);

  const installed = await run(["channel", "use", "rc"]);
  expect(installed.code).toBe(2);
  expect(installed.stderr).toContain("needs an installed");
  for (const name of ["dev", "Rc"]) {
    const bad = await run(["channel", "use", name, "-o", "json"]);
    expect(bad.code).toBe(2);
    expect(bad.stderr).toContain('"code":"usage"');
  }
});

test("cli: use, rename, and logout without a server", async () => {
  const { run, configDir } = await setup();
  await seed(configDir);

  expect((await run(["auth", "use", "b"])).code).toBe(0);
  expect((await run(["auth", "rename", "b", "anon"])).code).toBe(0);
  const list = JSON.parse((await run(["auth", "-o", "json"])).stdout);
  expect(list.find((p: { default: boolean }) => p.default).name).toBe("anon");

  const missing = await run(["auth", "use", "nope"]);
  expect(missing.code).toBe(2);

  const out = await run(["auth", "logout", "anon", "-o", "json"]);
  expect(out.code, out.stderr).toBe(0);
  expect(JSON.parse(out.stdout).default).toBe("a");
  const creds = JSON.parse(
    await readFile(join(configDir, "credentials.json"), "utf8"),
  );
  expect(Object.keys(creds.credentials)).toEqual(["p_a"]);

  // One profile left: no picker needed. --no-input is the hidden older name.
  const many = await run(["auth", "logout", "--no-input"]);
  expect(many.code).toBe(0);
});

test("cli: use :<user> picks that user on the default's server", async () => {
  const { run, configDir } = await setup();
  await seed(configDir, {
    profiles: {
      p_c: {
        name: "c",
        server: "http://localhost:10000",
        user: "alice",
        method: "basic",
        createdAt: "",
      },
    },
  });
  const use = await run(["auth", "use", ":alice", "-o", "json"]);
  expect(use.code, use.stderr).toBe(0);
  expect(JSON.parse(use.stdout).default).toBe("c");
  // b is anonymous, but on another server.
  const other = await run(["auth", "use", ":anonymous", "-o", "json"]);
  expect(other.code).toBe(2);
  expect(JSON.parse(other.stderr).error).toContain("No profile for anonymous");
});

test("cli: a key logout can't delete is remembered for the next sign-in", async () => {
  const { run, configDir } = await setup();
  const closed = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: { data() {} },
  });
  const server = `http://127.0.0.1:${closed.port}`;
  closed.stop(true);
  await seed(configDir, {
    servers: { [server]: { kind: "enterprise" } },
    profiles: {
      p_e: {
        name: "e",
        server,
        user: "alice",
        method: "saml",
        createdAt: "",
      },
    },
    credentials: {
      p_e: {
        type: "keyPair",
        keyType: "ec",
        publicKey: "PUB",
        privateKey: "PRIV",
        generated: true,
      },
    },
  });
  const out = await run(["auth", "logout", "e", "-o", "json"], {
    DH_TIMEOUT: "2",
  });
  expect(out.code, out.stderr).toBe(0);
  expect(out.stderr).toContain("deleted the next time you sign in");
  const creds = JSON.parse(
    await readFile(join(configDir, "credentials.json"), "utf8"),
  );
  expect(creds.credentials.p_e).toBeUndefined();
  expect(creds.orphans).toEqual([
    { server, user: "alice", keyType: "ec", publicKey: "PUB" },
  ]);
});

test("cli: a profile whose env var is unset", async () => {
  const { run, configDir } = await setup();
  await seed(configDir);
  const res = await run(["auth", "status", "-o", "json"]);
  expect(res.code).toBe(4);
  const error = JSON.parse(res.stderr);
  expect(error.code).toBe("credential_unavailable");
  expect(error.error).toContain("UNSET_TEST_PSK");
});

test("cli: environment-only auth", async () => {
  const { run, configDir } = await setup();
  const both = await run(["auth", "status"], {
    DH_SERVER: "localhost:10000",
    DH_PSK: "a",
    DH_PASSWORD: "b",
  });
  expect(both.code).toBe(2);

  const closed = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: { data() {} },
  });
  const port = closed.port;
  closed.stop(true);
  const unreachable = await run(["auth", "status", "-o", "json"], {
    DH_SERVER: `127.0.0.1:${port}`,
  });
  expect(unreachable.code).toBe(5);
  expect(JSON.parse(unreachable.stderr).code).toBe("server_unreachable");
  // Environment-only auth must not touch the config directory.
  expect(await stat(configDir).catch(() => null)).toBeNull();
});

test("cli: server diagnostics omit credential-bearing address components", async () => {
  const { run, configDir } = await setup();
  const address =
    "https://alice:password@example.com:8123/path?token=secret#fragment";
  for (const format of ["human", "json"]) {
    const args = format === "json" ? ["-o", "json"] : [];
    const conflict = await run(["auth", "status", ...args], {
      DH_SERVER: address,
      DH_PSK: "one",
      DH_PASSWORD: "two",
    });
    expect(conflict.code).toBe(2);
    expect(conflict.stderr).not.toMatch(
      /alice|password@example|token=secret|fragment|\/path/,
    );
    if (format === "json") {
      expect(JSON.parse(conflict.stderr).target).toBe(
        "https://example.com:8123",
      );
    }
    const invalid = await run([
      "auth",
      "login",
      "https://alice:password@[broken/?token=secret#fragment",
      "--no-prompt",
      ...args,
    ]);
    expect(invalid.code).toBe(2);
    expect(invalid.stderr).toContain("Not a server address");
    expect(invalid.stderr).not.toMatch(
      /alice|password|token|secret|fragment|broken/,
    );
  }
  await seed(configDir, {
    profiles: {
      p_a: {
        name: "a",
        server: address,
        user: "psk",
        method: "psk",
        createdAt: "",
      },
    },
  });
  const profile = await run(["auth", "status", "-o", "json"]);
  expect(profile.code).toBe(4);
  expect(JSON.parse(profile.stderr).target).toBe("https://example.com:8123");
  expect(profile.stderr).not.toMatch(
    /alice|password|token|secret|fragment|\/path/,
  );
});

test("cli: reaches an HTTP/2-only server, also after the --ca-cert re-run", async () => {
  const { run } = await setup();
  const server = await http2OnlyEnterprise();
  try {
    const res = await run([
      "auth",
      "login",
      `https://localhost:${server.port}`,
      "--ca-cert",
      TEST_CA,
      "--no-prompt",
      "-o",
      "json",
    ]);
    expect(res.stderr).toContain("(Deephaven Enterprise)");
    // The fake has no client library to load, so login stops there.
    const error = JSON.parse(res.stderr.trim().split("\n").pop() ?? "");
    expect(error.code).toBe("server_unsupported");
  } finally {
    await server.stop(true);
  }
});
