import { describe, expect, test } from "bun:test";
import { copyFile, mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { kit } from "./kit.ts";

const FIXTURES = resolve(import.meta.dir, "fixtures");
const WINDOWS = process.platform === "win32";

/** A fixture tool copied in as `<name>`; `ran()` lists the tool's invocations. */
async function setup(name = "dhctl", fixture = "fake-dhctl") {
  const tmp = await mkdtemp(join(tmpdir(), "cli-plugin-kit-scaffold-"));
  const tool = join(tmp, name);
  await copyFile(join(FIXTURES, fixture), tool);
  const log = join(tmp, "invocations.log");
  return {
    tool,
    ran: async () =>
      (await readFile(log, "utf8").catch(() => "")).split("\n").filter(Boolean),
    scaffold: async (args: string[]) => {
      const r = await kit(["scaffold", ...args], { FAKE_LOG: log });
      return { ...r, spec: r.code === 0 ? JSON.parse(r.stdout) : undefined };
    },
  };
}

/** A spec's command tree as nested names. */
const tree = (cmds: { name: string; commands?: unknown[] }[]): unknown =>
  cmds.map((c) =>
    c.commands ? { [c.name]: tree(c.commands as { name: string }[]) } : c.name,
  );

describe.skipIf(WINDOWS)("cli-plugin-kit scaffold", () => {
  test("prints a spec from the tool's --help, with each command's help kept", async () => {
    const { scaffold, tool } = await setup();
    const r = await scaffold([tool, "--recurse"]);
    expect(r.code).toBe(0);
    expect(r.stderr).toContain("✔ Read 12 help runs");
    expect(r.stderr).toContain("ctl intraday rescan");
    const { spec } = r;
    expect(spec.name).toBe("ctl");
    expect(spec.tool.path).toBe(tool);
    expect(spec.tool.version).toBeUndefined();
    expect(spec.source.helpDigest).toMatch(/^sha256:[0-9a-f]{64}$/);
    expect(tree(spec.commands)).toEqual([
      { intraday: ["truncate", "delete", "rescan"] },
      { metadata: ["list", "validate", "update"] },
      { logger: ["generate"] },
    ]);
    const list = spec.commands[1].commands[0];
    expect(list.options).toContainEqual({
      flags: "-t, --table-name <arg>",
      description:
        "The table or tables to act upon in Namespace.TableName or Namespace format.",
      required: true,
    });
    expect(list.source.help).toStartWith("usage: dhctl metadata list");
  });

  test("proposes runAs from options whose help mentions authentication", async () => {
    const { scaffold, tool } = await setup();
    const r = await scaffold([tool, "--recurse"]);
    expect(r.spec.tool.runAs).toEqual({
      account: "admin",
      unlessFlags: ["-k", "--key", "-user", "--user"],
    });
    expect(r.stderr).toContain('Proposed "tool.runAs"');
    const flat = await scaffold([tool, "--no-recurse"]);
    expect(flat.spec.tool.runAs).toBeUndefined();
  });

  test("--no-recurse runs only <tool> --help, and lists its subcommands", async () => {
    const { scaffold, tool, ran } = await setup();
    const r = await scaffold([tool, "--no-recurse"]);
    expect(r.code).toBe(0);
    expect(await ran()).toEqual(["--help"]);
    expect(tree(r.spec.commands)).toEqual(["intraday", "metadata", "logger"]);
    expect(r.spec.commands[0].argv).toEqual(["intraday"]);
  });

  test("without --recurse or --no-recurse, no terminal means a usage error", async () => {
    const { scaffold, tool, ran } = await setup();
    const r = await scaffold([tool]);
    expect(r.code).toBe(2);
    expect(r.stderr).toContain("--recurse or --no-recurse");
    expect(await ran()).toEqual(["--help"]);
  });

  test("Cobra-style: `help <sub>` where offered, so a careless subcommand never runs", async () => {
    const { scaffold, tool, ran } = await setup("app", "fake-cobra");
    const r = await scaffold([tool, "--recurse"]);
    expect(r.code).toBe(0);
    const invocations = await ran();
    expect(invocations).toContain("help wipe");
    expect(invocations.filter((a) => a.startsWith("wipe"))).toEqual([]);
    expect(tree(r.spec.commands)).toEqual([
      "completion",
      { db: ["migrate", "reset"] },
      "serve",
      "wipe",
    ]);
  });

  test("--nouns makes each top-level group a noun, and gathers the loose verbs", async () => {
    const { scaffold, tool } = await setup("app", "fake-cobra");
    const r = await scaffold([tool, "--recurse", "--nouns"]);
    expect(r.code).toBe(0);
    expect(r.spec.commands).toBeUndefined();
    expect(tree(r.spec.nouns)).toEqual([
      { db: ["migrate", "reset"] },
      { app: ["completion", "serve", "wipe"] },
    ]);
    expect(r.stderr).toContain(
      'under the noun "app"; place them by hand: completion, serve, wipe',
    );
  });

  test("warns when --help prints no usage, as from a tool without --help", async () => {
    const { scaffold, tool, ran } = await setup("crcat", "fake-crcat");
    const r = await scaffold([tool]);
    expect(r.code).toBe(0);
    expect(r.stderr).toContain(
      "! `crcat --help` printed no usage line, so crcat may not take --help",
    );
    expect(await ran()).toEqual(["--help"]);
    expect(r.spec.argv).toEqual([]);
    expect(r.spec.source.help).toContain("Failed to read checkpoint record");
  });

  test("the same run gives the same helpDigest", async () => {
    const { scaffold, tool } = await setup();
    const first = await scaffold([tool, "--recurse"]);
    const second = await scaffold([tool, "--recurse"]);
    expect(second.spec.source.helpDigest).toBe(first.spec.source.helpDigest);
  });

  test("names: --name, else the tool's without dh; the tool must be an absolute path", async () => {
    const { scaffold, tool } = await setup("dhconfig");
    expect((await scaffold([tool, "--no-recurse"])).spec.name).toBe("config");
    const named = await scaffold([tool, "--name", "cluster", "--no-recurse"]);
    expect(named.spec.name).toBe("cluster");
    expect((await scaffold([tool, "--name", "Bad"])).code).toBe(2);
    expect((await scaffold(["dhctl"])).code).toBe(2);
    const missing = await scaffold(["/nowhere/dhctl"]);
    expect(missing.code).toBe(1);
    expect(missing.stderr).toContain("Tool not found: /nowhere/dhctl");
  });
});
