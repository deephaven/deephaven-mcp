import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, relative, resolve } from "node:path";
import { build, type Cluster } from "../src/build.ts";
import { kit } from "./kit.ts";

const EXAMPLES = resolve(import.meta.dir, "../../../examples");
const CLUSTER: Cluster = {
  kind: "enterprise",
  version: "2026.01.261102164822g2f6a8ebf11",
};

const temp = (prefix: string) => mkdtemp(join(tmpdir(), prefix));

/** Every file under `dir`, relative, sorted. */
async function files(dir: string): Promise<string[]> {
  const entries = await readdir(dir, { recursive: true, withFileTypes: true });
  return entries
    .filter((e) => e.isFile())
    .map((e) => relative(dir, join(e.parentPath, e.name)))
    .sort();
}

/** A `<src>` with these shell specs and js plugin modules (path → source). */
async function source(
  shell: Record<string, unknown>[],
  js: Record<string, string> = {},
): Promise<string> {
  const src = await temp("cli-plugin-kit-src-");
  await mkdir(join(src, "shell"));
  for (const spec of shell) {
    await writeFile(
      join(src, "shell", `${spec.name}.json`),
      JSON.stringify(spec),
    );
  }
  for (const [path, text] of Object.entries(js)) {
    await mkdir(join(src, "js", path, ".."), { recursive: true });
    await writeFile(join(src, "js", path), text);
  }
  return src;
}

const spec = (name: string, extra: Record<string, unknown> = {}) => ({
  schema: 1,
  name,
  description: `The ${name} tool.`,
  tool: { path: `/usr/bin/${name}` },
  argv: [],
  ...extra,
});

/** A js plugin module, without the SDK: what `definePlugin` returns. */
const plugin = (def: string) => `export default { api: 1, ${def} };\n`;
const LIST = `list: { description: "List.", output: { kind: "none" }, run() {} }`;

describe("cli-plugin-kit build", () => {
  test("rebuilding examples/dh-cli-src reproduces examples/dh-cli", async () => {
    const out = await temp("cli-plugin-kit-out-");
    const r = await kit([
      "build",
      join(EXAMPLES, "dh-cli-src"),
      out,
      "--cluster-version",
      CLUSTER.version,
    ]);
    expect(r.code).toBe(0);
    const expected = join(EXAMPLES, "dh-cli");
    expect(await files(out)).toEqual(await files(expected));
    for (const f of await files(expected)) {
      expect({ f, text: await readFile(join(out, f), "utf8") }).toEqual({
        f,
        text: await readFile(join(expected, f), "utf8"),
      });
    }
  });

  test("manifests: digests over the files' bytes, revisions over the digests and the shell list", async () => {
    const out = await temp("cli-plugin-kit-out-");
    const { js, shell } = await build(
      join(EXAMPLES, "dh-cli-src"),
      out,
      CLUSTER,
    );
    const sha = (...texts: string[]) => {
      const h = createHash("sha256");
      for (const t of texts) h.update(t);
      return `sha256:${h.digest("hex")}`;
    };
    const read = (p: string) => readFile(join(out, p), "utf8");
    const ctl = shell.plugins.find((p) => p.name === "ctl");
    expect(ctl?.digest).toBe(sha(await read("shell/ctl.json")));
    const db = js.plugins[0];
    expect(db).toMatchObject({
      name: "dbserver",
      kind: "js",
      nouns: ["dbserver"],
      spec: "dbserver/plugin.json",
      module: "dbserver/index.js",
      api: 1,
    });
    expect(db?.digest).toBe(
      sha(
        await read("js/dbserver/plugin.json"),
        await read("js/dbserver/index.js"),
      ),
    );
    expect(shell.revision).toBe(
      sha(
        shell.plugins
          .map((p) => `${p.name}:${p.digest}\n`)
          .sort()
          .join(""),
      ),
    );
    expect(js.shell).toEqual(
      shell.plugins.map((p) => ({
        name: p.name,
        description: p.description,
        nouns: p.nouns,
      })),
    );
    expect(js.revision).toBe(
      sha(
        [
          `dbserver:${db?.digest}\n`,
          "shell:ctl:ctl\n",
          "shell:deephaven:service,node,etcd,keys,units,coreplus,tls\n",
        ]
          .sort()
          .join(""),
      ),
    );
    expect(await read("js/revision")).toBe(`${js.revision}\n`);
    expect(shell.shell).toBeUndefined();
  });

  test("shell specs lose their source fields", async () => {
    const out = await temp("cli-plugin-kit-out-");
    await build(join(EXAMPLES, "dh-cli-src"), out, CLUSTER);
    const original = await readFile(
      join(EXAMPLES, "dh-cli-src/shell/ctl.json"),
      "utf8",
    );
    expect(original).toContain('"source"');
    const built = await readFile(join(out, "shell/ctl.json"), "utf8");
    expect(built).not.toContain('"source"');
    expect(JSON.parse(built).tool.runAs.account).toBe("admin");
  });

  test("plugin.json: everything but functions, options as flags", async () => {
    const out = await temp("cli-plugin-kit-out-");
    const src = await source([], {
      "ops/index.ts": plugin(`
        name: "ops", description: "Ops.", requires: "any",
        groups: ["iris-superusers"],
        nouns: {
          server: { description: "Servers.", commands: {
            restart: {
              description: "Restart one.",
              args: { serverName: { description: "Which.", required: true } },
              options: {
                dryRun: { type: "boolean", description: "Don't." },
                tag: { type: "string", description: "Tags.", multiple: true },
                mode: { type: "string", description: "Mode.", choices: ["a", "b"], default: "a" },
              },
              output: { kind: "text" },
              run() {}, human: (r) => r,
            },
          } },
          health: { description: "Health.", output: { kind: "object", fields: { ok: { type: "boolean", description: "OK." } } }, run() {} },
        },`),
    });
    const { js } = await build(src, out, CLUSTER);
    expect(js.plugins[0]).toMatchObject({
      name: "ops",
      nouns: ["server", "health"],
      groups: ["iris-superusers"],
    });
    const json = JSON.parse(
      await readFile(join(out, "js/ops/plugin.json"), "utf8"),
    );
    expect(json).toEqual({
      schema: 1,
      api: 1,
      name: "ops",
      description: "Ops.",
      requires: "any",
      groups: ["iris-superusers"],
      nouns: [
        {
          name: "server",
          description: "Servers.",
          commands: [
            {
              name: "restart",
              description: "Restart one.",
              args: [
                { name: "serverName", description: "Which.", required: true },
              ],
              options: [
                {
                  key: "dryRun",
                  flags: "--dry-run",
                  type: "boolean",
                  description: "Don't.",
                },
                {
                  key: "tag",
                  flags: "--tag <value...>",
                  type: "string",
                  description: "Tags.",
                  multiple: true,
                },
                {
                  key: "mode",
                  flags: "--mode <value>",
                  type: "string",
                  description: "Mode.",
                  choices: ["a", "b"],
                  default: "a",
                },
              ],
              output: { kind: "text" },
            },
          ],
        },
        {
          name: "health",
          description: "Health.",
          output: {
            kind: "object",
            fields: { ok: { type: "boolean", description: "OK." } },
          },
        },
      ],
    });
  });

  test("the module is bundled with its imports into one file", async () => {
    const out = await temp("cli-plugin-kit-out-");
    const src = await source([], {
      "x/src/index.ts": `import { describe } from "./describe.ts";\n${plugin(`name: "x", description: describe("x"), requires: "any", commands: { ${LIST} },`)}`,
      "x/src/describe.ts": `export const describe = (n: string): string => \`The \${n} plugin.\`;\n`,
    });
    const { js } = await build(src, out, CLUSTER);
    expect(js.plugins[0]?.description).toBe("The x plugin.");
    const module = await readFile(join(out, "js/x/index.js"), "utf8");
    expect(module).not.toContain("import ");
    expect(module).toContain(" plugin.`");
  });

  test("a noun or plugin name used twice fails the build", async () => {
    const out = await temp("cli-plugin-kit-out-");
    const nounTwice = await source([
      spec("deephaven", {
        argv: undefined,
        nouns: [{ name: "ctl", description: "C.", argv: [] }],
      }),
      spec("ctl"),
    ]);
    await expect(build(nounTwice, out, CLUSTER)).rejects.toThrow(
      'Noun "ctl" is registered twice',
    );
    const nameTwice = await source([spec("ctl")], {
      "ctl/index.ts": plugin(
        `name: "ctl", description: "C.", requires: "any", nouns: { other: { description: "O.", commands: { ${LIST} } } },`,
      ),
    });
    await expect(build(nameTwice, out, CLUSTER)).rejects.toThrow(
      'Plugin name "ctl" is used twice',
    );
  });

  test("invalid specs and plugins fail the build", async () => {
    const out = await temp("cli-plugin-kit-out-");
    const cases: [Parameters<typeof source>, string][] = [
      [[[spec("a", { tool: { path: "/a", version: "1" } })]], "tool.version"],
      [
        [[spec("a", { nouns: [{ name: "b", description: "B.", argv: [] }] })]],
        'has both "nouns" and "argv"',
      ],
      [
        [[spec("a", { tool: { path: "/a", runAs: { account: "root" } } })]],
        '"tool.runAs"',
      ],
      [[[], { "p/index.ts": "export default { name: 'p' };" }], 'no "api"'],
      [
        [
          [],
          {
            "p/index.ts": plugin(
              `name: "p", description: "P.", requires: "any", commands: { go: { description: "Go.", options: { sshHost: { type: "string", description: "H." } }, output: { kind: "none" }, run() {} } },`,
            ),
          },
        ],
        "--ssh-host is one of dh's flags",
      ],
      [
        [
          [],
          {
            "p/index.ts": plugin(
              `name: "p", description: "P.", requires: "any", commands: { go: { description: "Go.", output: { kind: "none" } } },`,
            ),
          },
        ],
        'needs "commands" or a "run" function',
      ],
      [
        [
          [],
          {
            "p/index.ts": plugin(
              `name: "p", description: "P.", requires: "any", commands: { ${LIST} }, nouns: { ${LIST} },`,
            ),
          },
        ],
        'needs "commands" or "nouns", not both',
      ],
    ];
    for (const [args, message] of cases) {
      await expect(build(await source(...args), out, CLUSTER)).rejects.toThrow(
        message,
      );
    }
  });

  test("--cluster-version is required", async () => {
    const r = await kit(["build", join(EXAMPLES, "dh-cli-src"), "/nowhere"]);
    expect(r.code).toBe(2);
    expect(r.stderr).toContain("--cluster-version is required");
  });
});
