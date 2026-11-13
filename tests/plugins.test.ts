import { describe, expect, test } from "bun:test";
import { chmod, copyFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { parseSpec } from "../src/actions/plugins/spec.ts";
import { DhError } from "../src/errors.ts";
import {
  type EntryOverrides,
  writeManifest,
  writeShellPart,
} from "./bundle-helpers.ts";
import { CLI, run, tempDir, WINDOWS } from "./helpers.ts";

/**
 * A stand-in tool: prints its arguments one per line, echoes stdin with
 * `--stdin`, sleeps `$TOOL_WAIT` seconds, exits `$TOOL_EXIT`.
 */
const TOOL = `#!/bin/sh
for a in "$@"; do printf '[%s]\\n' "$a"; done
[ "$1" = "--stdin" ] && cat
[ -n "$TOOL_WAIT" ] && sleep "$TOOL_WAIT"
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

/**
 * A config dir, the stand-in tool, and a bundle from `specs` in
 * `DH_PLUGIN_BUNDLE`; `entries` overrides manifest fields per plugin.
 */
async function setup(
  specs: (tool: string) => Record<string, unknown>[],
  config?: unknown,
  entries: EntryOverrides = {},
) {
  const tmp = await tempDir("dh-plugin-test-");
  const tool = join(tmp, "tool");
  await writeFile(tool, TOOL);
  await chmod(tool, 0o755);
  const dir = join(tmp, "cli");
  await mkdir(dir, { recursive: true });
  const bundle = join(tmp, "dh-cli");
  await writeShellPart(bundle, specs(tool), entries);
  if (config) await writeFile(join(dir, "config.json"), JSON.stringify(config));
  const env = {
    ...process.env,
    DH_CONFIG_DIR: dir,
    DH_AUTO_UPDATE: "off",
    DH_PLUGIN_BUNDLE: bundle,
    DH_PROFILE: "",
  };
  return {
    tool,
    tmp,
    bundle,
    /** `dh args` still running, its stdout piped. */
    spawn: (args: string[], extra: Record<string, string>) =>
      Bun.spawn([...CLI, ...args], {
        env: { ...env, ...extra },
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
      }),
    dh: (args: string[], env: Record<string, string> = {}, stdin?: string) =>
      run(
        [...CLI, ...args],
        {
          DH_CONFIG_DIR: dir,
          DH_AUTO_UPDATE: "off",
          DH_PLUGIN_BUNDLE: bundle,
          DH_PROFILE: undefined,
          ...env,
        },
        stdin,
      ),
  };
}

describe.skipIf(WINDOWS)("plugin commands", () => {
  test("pass every argument through in order, with the spec's argv first", async () => {
    const { dh } = await setup((tool) => [spec(tool)]);
    const r = await dh([
      "ctl",
      "intraday",
      "rescan",
      "--table",
      "a b",
      "-x",
      "it's",
      "--k=v",
    ]);
    expect(r.stdout).toBe(
      "[intraday]\n[rescan]\n[--table]\n[a b]\n[-x]\n[it's]\n[--k=v]\n",
    );
    expect(r.code).toBe(0);
  });

  test("keep dh's global flags, and pass them to the tool after --", async () => {
    const { dh } = await setup((tool) => [spec(tool)]);
    const r = await dh([
      "ctl",
      "intraday",
      "rescan",
      "-o",
      "json",
      "--",
      "-o",
      "x",
    ]);
    expect(JSON.parse(r.stdout)).toEqual({
      stdout: "[intraday]\n[rescan]\n[-o]\n[x]\n",
    });
  });

  test("take only -o and -h of the global flags; the rest go to the tool", async () => {
    const { dh } = await setup((tool) => [spec(tool)]);
    const r = await dh([
      "ctl",
      "intraday",
      "rescan",
      "--profile",
      "p",
      "--no-prompt",
      "-V",
      "--output=json",
      "-o",
      "out.csv",
    ]);
    expect(JSON.parse(r.stdout)).toEqual({
      stdout:
        "[intraday]\n[rescan]\n[--profile]\n[p]\n[--no-prompt]\n[-V]\n[-o]\n[out.csv]\n",
    });
  });

  test("--profile or --no-prompt before a plugin command is a usage error", async () => {
    const { dh } = await setup((tool) => [spec(tool)]);
    for (const flags of [
      ["--profile", "p"],
      ["--no-prompt"],
      ["--profile=p"],
    ]) {
      const r = await dh(["-o", "json", ...flags, "ctl", "intraday", "rescan"]);
      expect(r.code).toBe(2);
      expect(JSON.parse(r.stderr).error).toContain("doesn't apply");
    }
  });

  test("help lists only the global flags plugin commands take", async () => {
    const { dh } = await setup((tool) => [spec(tool)]);
    for (const args of [
      ["ctl", "--help"],
      ["ctl", "intraday", "rescan", "-h"],
    ]) {
      const help = (await dh(args)).stdout;
      expect(help).toContain("--output");
      expect(help).not.toContain("--profile");
      expect(help).not.toContain("--no-prompt");
      expect(help).not.toContain("--version");
    }
    expect((await dh(["auth", "status", "--help"])).stdout).toContain(
      "--profile",
    );
  });

  test("dh agents states the plugin flag rule once and marks plugin nouns", async () => {
    const { dh } = await setup((tool) => [spec(tool)]);
    const m = JSON.parse((await dh(["agents", "-o", "json"])).stdout);
    expect(m.pluginFlags.local).toEqual([
      "-o, --output <format>",
      "-h, --help",
    ]);
    const ctl = m.commands.find(
      (c: { command: string }) => c.command === "dh ctl",
    );
    expect(ctl.plugin).toBe("ctl");
    expect(ctl.pluginTool).toBe("tool");
    expect(ctl.pluginExec).toBe("local");
    expect(JSON.stringify(ctl.commands)).not.toContain('"plugin"');
    expect(
      m.commands.find((c: { command: string }) => c.command === "dh auth")
        .plugin,
    ).toBeUndefined();
  });

  test("pass stdin through", async () => {
    const { dh } = await setup((tool) => [
      spec(tool, { commands: undefined, argv: [] }),
    ]);
    const r = await dh(["ctl", "--stdin"], {}, "piped\n");
    expect(r.stdout).toBe("[--stdin]\npiped\n");
  });

  test("a signal to dh while the tool runs is passed to the tool, and dh waits for it", async () => {
    const { spawn } = await setup((tool) => [
      spec(tool, { commands: undefined, argv: [] }),
    ]);
    // (Ctrl-C reaches the tool from the terminal, so dh leaves that one to it;
    // the test runner keeps SIGINT from its children, so SIGTERM stands in.)
    const proc = spawn(["ctl", "x"], { TOOL_WAIT: "5" });
    const reader = proc.stdout.getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe("[x]\n");
    proc.kill("SIGTERM");
    // The tool stopped by the signal: tool_failed, not dh cancelled (3).
    expect(await proc.exited).toBe(6);
  });

  test("a non-zero exit is tool_failed, exit 6, with toolExit in JSON", async () => {
    const { dh } = await setup((tool) => [spec(tool)]);
    const r = await dh(["-o", "json", "ctl", "intraday", "rescan"], {
      TOOL_EXIT: "3",
    });
    expect(r.code).toBe(6);
    expect(JSON.parse(r.stderr)).toMatchObject({
      code: "tool_failed",
      exit: 6,
      toolExit: 3,
    });
  });

  test("the machine's path replaces the spec's", async () => {
    const { dh } = await setup(() => [spec("/nowhere/tool")], {
      version: 1,
      servers: {},
      profiles: {},
      plugins: { ctl: { path: "/bin/echo" } },
    });
    const r = await dh(["ctl", "intraday", "rescan", "x"]);
    expect(r.stdout).toBe("intraday rescan x\n");
  });

  test("runAs: through sudo as the account here, unless the user brings their own credentials", async () => {
    const { dh, tool, tmp } = await setup((tool) => [
      spec(tool, {
        tool: {
          path: tool,
          runAs: { account: "admin", unlessFlags: ["--key"] },
        },
      }),
    ]);
    const bin = join(tmp, "bin");
    await mkdir(bin);
    await copyFile(
      join(import.meta.dir, "fixtures/plugins/fake-sudo"),
      join(bin, "sudo"),
    );
    await chmod(join(bin, "sudo"), 0o755);
    const log = join(tmp, "sudo.log");
    const env = { PATH: `${bin}:${process.env.PATH}`, FAKE_SUDO_LOG: log };
    const r = await dh(["ctl", "intraday", "rescan", "x"], env);
    expect(r.stdout).toBe("[intraday]\n[rescan]\n[x]\n");
    expect(await readFile(log, "utf8")).toBe(
      `-n -u irisadmin ${tool} intraday rescan x\n`,
    );
    await dh(["ctl", "intraday", "rescan", "--key", "k"], env);
    expect((await readFile(log, "utf8")).split("\n")).toHaveLength(2);
    const refused = await dh(["-o", "json", "ctl", "intraday", "rescan"], {
      ...env,
      FAKE_SUDO: "refuse",
    });
    expect(refused.code).toBe(6);
    expect(
      JSON.parse(refused.stderr.trim().split("\n").at(-1) ?? "").hint,
    ).toBe("You can't run tool as irisadmin; pass your own credentials: --key");
  });

  test("a missing tool is tool_not_found, exit 6", async () => {
    const { dh } = await setup(() => [spec("/nowhere/tool")]);
    const r = await dh(["ctl", "intraday", "rescan"]);
    expect(r.code).toBe(6);
    expect(r.stderr).toContain("Tool not found: /nowhere/tool");
  });

  test("plugin list names the bundle, and each plugin and how it runs", async () => {
    const { dh, bundle, tool } = await setup((tool) => [spec(tool)]);
    const text = (await dh(["plugin", "list"])).stdout;
    expect(text).toContain(`shell: DH_PLUGIN_BUNDLE (${bundle}/shell)`);
    expect(text).toContain("cluster 2026.01.1g0");
    expect(text).toContain(`  ctl   local: ${tool}\n`);
    const json = JSON.parse(
      (await dh(["plugin", "list", "-o", "json"])).stdout,
    );
    expect(shellOf(json)).toMatchObject({
      channel: "shell",
      source: "env",
      dir: join(bundle, "shell"),
      clusterVersion: "2026.01.1g0",
    });
    expect(json.plugins).toEqual([
      {
        name: "ctl",
        kind: "shell",
        nouns: ["ctl"],
        exec: "local",
        tool,
        sshHosts: null,
        defaultSshHost: null,
        runAs: null,
        state: "ready",
        reason: null,
      },
    ]);
  });

  test("bad entries and taken nouns reject their plugin only, and are listed", async () => {
    const { dh } = await setup(
      (tool) => [
        spec(tool, { name: "auth" }),
        spec(tool, { name: "config" }),
        spec(tool, {
          name: "one",
          commands: undefined,
          nouns: [{ name: "dup", description: "D.", argv: [] }],
        }),
        spec(tool, {
          name: "two",
          commands: undefined,
          nouns: [{ name: "dup", description: "D.", argv: [] }],
        }),
        spec(tool, { name: "bad", tool: { path: "relative" } }),
        spec(tool, { name: "far" }),
        spec(tool, { name: "future" }),
        spec(tool),
      ],
      undefined,
      { far: { spec: "../far.json" }, future: { dh: ">=99" } },
    );
    const out = JSON.parse((await dh(["plugin", "list", "-o", "json"])).stdout);
    const state = Object.fromEntries(
      out.plugins.map((p: { name: string; state: string; reason: string }) => [
        p.name,
        `${p.state}: ${p.reason}`,
      ]),
    );
    expect(state.ctl).toBe("ready: null");
    expect(state.auth).toContain('invalid: "auth" is a built-in command');
    expect(state.config).toContain('invalid: "config" is reserved');
    expect(state.one).toContain('invalid: "dup" is registered by one and two');
    expect(state.two).toContain('invalid: "dup" is registered by one and two');
    expect(state.bad).toContain("invalid:");
    expect(state.bad).toContain("absolute path");
    expect(state["?"] ?? state.far).toContain('"spec" must be a path inside');
    expect(state.future).toBe("unavailable: needs dh >=99");
    expect((await dh(["plugin", "list"])).stdout).toContain(
      "  future   local  (needs dh >=99)\n",
    );
    expect((await dh(["auth", "--help"])).stdout).toContain("Sign in");
    const help = (await dh(["--help"])).stdout;
    expect(help).not.toContain("dup");
    expect(help).not.toContain("future");
    expect((await dh(["dup"])).code).toBe(2);
    const future = await dh(["-o", "json", "future", "x"]);
    expect(future.code).toBe(2);
    expect(JSON.parse(future.stderr)).toMatchObject({
      code: "plugin_unavailable",
    });
    expect(JSON.parse(future.stderr).error).toContain("needs dh >=99");
    const agents = JSON.parse((await dh(["agents", "-o", "json"])).stdout);
    const nouns = agents.commands.map((c: { command: string }) => c.command);
    expect(nouns).toContain("dh ctl");
    expect(nouns).not.toContain("dh future");
  });

  test("a manifest for another channel, or of an unknown schema, is ignored", async () => {
    const { dh, bundle } = await setup((tool) => [spec(tool)]);
    const shell = join(bundle, "shell");
    const manifest = JSON.parse(
      await readFile(join(shell, "manifest.json"), "utf8"),
    );
    await writeManifest(shell, "js", manifest.plugins);
    const out = JSON.parse((await dh(["plugin", "list", "-o", "json"])).stdout);
    expect(out.plugins).toEqual([]);
    expect(shellOf(out).reason).toContain('"channel" is "js"');
    await writeManifest(shell, "shell", manifest.plugins, { schema: 2 });
    const later = JSON.parse(
      (await dh(["plugin", "list", "-o", "json"])).stdout,
    );
    expect(shellOf(later).reason).toContain("unsupported");
    expect((await dh(["ctl"])).code).toBe(2);
  });

  test("a plugin with nouns registers each, and its name isn't a command", async () => {
    const example = await Bun.file(
      join(import.meta.dir, "../examples/dh-cli-src/shell/deephaven.json"),
    ).json();
    const { dh } = await setup((tool) => [
      { ...example, tool: { path: tool } },
    ]);
    expect((await dh(["etcd", "status", "x"])).stdout).toBe(
      "[etcd]\n[status]\n[x]\n",
    );
    expect((await dh(["node", "join"])).stdout).toBe("[join]\n");
    expect((await dh(["tls", "--out", "d"])).stdout).toBe(
      "[tls]\n[--out]\n[d]\n",
    );
    expect((await dh(["deephaven"])).code).toBe(2);
    const m = JSON.parse((await dh(["agents", "-o", "json"])).stdout);
    const marked = m.commands
      .filter((c: { plugin?: string }) => c.plugin === "deephaven")
      .map((c: { command: string }) => c.command);
    expect(marked).toEqual(
      ["service", "node", "etcd", "keys", "units", "coreplus", "tls"].map(
        (n) => `dh ${n}`,
      ),
    );
  });

  test("declared options show as dh options, and still pass through", async () => {
    const { dh } = await setup((tool) => [
      spec(tool, {
        commands: [
          {
            name: "list",
            description: "Leaf.",
            argv: ["list"],
            options: [
              {
                flags: "-t, --table-name <arg>",
                description: "Table.",
                required: true,
              },
              { flags: "-pf, --pwfile <arg>", description: "Password file." },
              { flags: "-o <file>", description: "The tool's own -o." },
            ],
            source: { help: "usage: tool list RAW-HELP-TEXT" },
          },
        ],
      }),
    ]);
    const help = (await dh(["ctl", "list", "--help"])).stdout;
    expect(help).toContain("-t, --table-name <arg>");
    expect(help).toContain("Table. (required)");
    // Commander can't spell -pf; the long form is shown.
    expect(help).toContain("--pwfile <arg>");
    expect(help).not.toContain("-pf");
    // A tool flag spelled like dh's own isn't shown; source.help never is.
    expect(help).not.toContain("The tool's own -o");
    expect(help).not.toContain("RAW-HELP-TEXT");
    const r = await dh(["ctl", "list", "--table-name", "T", "-pf", "p"]);
    expect(r.stdout).toBe("[list]\n[--table-name]\n[T]\n[-pf]\n[p]\n");
  });

  test("plugin commands are listed by dh agents", async () => {
    const { dh } = await setup((tool) => [spec(tool)]);
    const r = await dh(["agents"]);
    expect(r.stdout).toContain("dh ctl intraday rescan [args...]");
  });
});

describe("parseSpec", () => {
  const invalid = (value: unknown) => {
    try {
      parseSpec(value, "x.json");
    } catch (e) {
      expect(e).toBeInstanceOf(DhError);
      expect((e as DhError).code).toBe("plugin_invalid");
      return (e as DhError).message;
    }
    throw new Error("expected plugin_invalid");
  };

  test("accepts the example dhctl spec", async () => {
    const example = await Bun.file(
      join(import.meta.dir, "../examples/dh-cli-src/shell/ctl.json"),
    ).json();
    expect(parseSpec(example, "ctl.json").name).toBe("ctl");
  });

  test("rejects bad specs, naming the problem", () => {
    expect(invalid(spec("relative/tool"))).toContain("absolute path");
    expect(invalid(spec("/t", { name: "Bad Name" }))).toContain('"name"');
    expect(invalid(spec("/t", { argv: [] }))).toContain("both");
    expect(invalid(spec("/t", { commands: [] }))).toContain("non-empty");
    expect(
      invalid(
        spec("/t", {
          commands: [
            { name: "a", description: "", argv: [] },
            { name: "a", description: "", argv: [] },
          ],
        }),
      ),
    ).toContain("duplicate");
    const leaf = (options: unknown) =>
      spec("/t", { commands: undefined, argv: [], options });
    expect(invalid(leaf([{ flags: "x", description: "" }]))).toContain(
      "starting with",
    );
    expect(invalid(leaf([{ flags: "-x" }]))).toContain("description");
    expect(invalid(spec("/t", { options: [] }))).toContain("group");
    const noun = { name: "n", description: "N.", argv: [] };
    expect(invalid(spec("/t", { nouns: [noun] }))).toContain(
      'has both "nouns" and "commands"',
    );
    expect(
      invalid(spec("/t", { commands: undefined, nouns: [noun, noun] })),
    ).toContain('duplicate noun "n"');
    expect(
      invalid(spec("/t", { tool: { path: "/t", runAs: { account: "root" } } })),
    ).toContain('"tool.runAs"');
  });
});

/** `dh plugin list -o json`'s shell channel. */
// biome-ignore lint/suspicious/noExplicitAny: parsed JSON.
function shellOf(list: { channels: any[] }) {
  return list.channels.find((c) => c.channel === "shell");
}
