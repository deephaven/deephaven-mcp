#!/usr/bin/env node
/**
 * `cli-plugin-kit`: builds a cluster's `dh` plugin bundle, and scaffolds
 * `shell` specs from a tool's `--help`.
 */
import { basename, isAbsolute } from "node:path";
import { createInterface } from "node:readline/promises";
import { parseArgs } from "node:util";
import { build } from "./build.ts";
import {
  localHelpRunner,
  readToolHelp,
  type Scaffold,
  scaffoldPlugin,
  sshHelpRunner,
  type ToolHelp,
} from "./scaffold.ts";

const USAGE = `Usage:
  cli-plugin-kit build <src> <out> --cluster-version <version> [--cluster-kind enterprise|community]
      Builds <src>/shell/*.json and <src>/js/<plugin>/ into <out>/shell/ and <out>/js/.
  cli-plugin-kit scaffold <tool> [--ssh <host>] [--name <noun>] [--nouns] [--recurse | --no-recurse]
      Prints a shell spec for <tool>, read from its --help, on stdout.`;

const NAME = /^[a-z][a-z0-9-]*$/;

async function main(argv: string[]): Promise<void> {
  const [command, ...rest] = argv;
  if (command === "build") return await buildCommand(rest);
  if (command === "scaffold") return await scaffoldCommand(rest);
  if (command === "--help" || command === "-h") {
    console.log(USAGE);
    return;
  }
  throw new UsageError(command ? `Unknown command: ${command}` : "No command");
}

async function buildCommand(args: string[]): Promise<void> {
  const { values, positionals } = parseArgs({
    args,
    allowPositionals: true,
    options: {
      "cluster-version": { type: "string" },
      "cluster-kind": { type: "string", default: "enterprise" },
    },
  });
  const [src, out, ...extra] = positionals;
  if (!src || !out || extra.length > 0) {
    throw new UsageError("build takes <src> and <out>");
  }
  const version = values["cluster-version"];
  if (!version) throw new UsageError("--cluster-version is required");
  const kind = values["cluster-kind"];
  if (kind !== "enterprise" && kind !== "community") {
    throw new UsageError("--cluster-kind must be enterprise or community");
  }
  const { js, shell } = await build(src, out, { kind, version });
  for (const m of [shell, js]) {
    const names = m.plugins.map((p) => p.name).join(", ") || "no plugins";
    console.log(`${m.channel}: ${names} (${m.revision})`);
  }
}

async function scaffoldCommand(args: string[]): Promise<void> {
  const { values, positionals } = parseArgs({
    args,
    allowPositionals: true,
    allowNegative: true,
    options: {
      ssh: { type: "string" },
      name: { type: "string" },
      nouns: { type: "boolean" },
      recurse: { type: "boolean" },
    },
  });
  const [tool, ...extra] = positionals;
  if (!tool || extra.length > 0) throw new UsageError("scaffold takes <tool>");
  if (!isAbsolute(tool)) {
    throw new UsageError(`The tool must be an absolute path: ${tool}`);
  }
  const name = values.name ?? defaultName(tool);
  if (!NAME.test(name)) {
    throw new UsageError(
      `Can't name the plugin "${name}": use lowercase letters, digits and dashes (--name)`,
    );
  }
  const runner = values.ssh
    ? sshHelpRunner(tool, values.ssh)
    : localHelpRunner(tool);
  const top = await readToolHelp(tool, runner);
  const recurse = await confirmRecurse(values.recurse, top);
  if (recurse) console.error("… Reading each subcommand's help");
  const scaffold = await scaffoldPlugin(top, name, recurse, values.nouns);
  process.stdout.write(`${JSON.stringify(scaffold.spec, null, 2)}\n`);
  for (const line of notes(scaffold, runner.host)) console.error(line);
}

/**
 * Whether to run each subcommand's help, recursively: `--[no-]recurse`, else
 * asked in a terminal. A careless tool might run a subcommand instead of
 * showing its help, so it's the user's call.
 */
async function confirmRecurse(
  flag: boolean | undefined,
  top: ToolHelp,
): Promise<boolean> {
  if (top.subcommands.length === 0) return false;
  if (flag !== undefined) return flag;
  const tool = basename(top.path);
  if (!process.stdin.isTTY || !process.stderr.isTTY) {
    throw new UsageError(
      `${tool} --help lists subcommands: pass --recurse or --no-recurse`,
    );
  }
  const shown = top.subcommands.slice(0, 12).join(", ");
  const more = top.subcommands.length > 12 ? ", …" : "";
  console.error(
    `${tool} --help lists ${top.subcommands.length}: ${shown}${more}`,
  );
  const rl = createInterface({ input: process.stdin, output: process.stderr });
  try {
    const answer = await rl.question(
      `Run each one's help too (\`${tool} <sub> --help\`, recursively)? Only safe if ${tool} handles --help on every subcommand. [Y/n] `,
    );
    return !/^n/i.test(answer.trim());
  } finally {
    rl.close();
  }
}

/** `dhctl` → `ctl`, `iriscat` → `iriscat`; lowercase, other characters as `-`. */
function defaultName(tool: string): string {
  const base = basename(tool)
    .toLowerCase()
    .replace(/\.[a-z]+$/, "");
  return base.replace(/^dh(?=[a-z])/, "").replace(/[^a-z0-9-]+/g, "-");
}

/** What was read, then what to edit. */
function notes(s: Scaffold, host: string | undefined): string[] {
  const tool = basename(s.spec.tool.path);
  const over = host === undefined ? "" : ` over SSH: ${host}`;
  const lines = [`✔ Read ${s.runs} help runs${over}`];
  if (s.spec.tool.runAs) {
    lines.push(
      `! Proposed "tool.runAs" from options whose help mentions authentication: account "${s.spec.tool.runAs.account}", unlessFlags ${s.spec.tool.runAs.unlessFlags?.join(" ")}. Confirm both.`,
    );
  }
  if (s.loose.length > 0) {
    lines.push(
      `! ${tool}'s top-level commands that aren't groups are under the noun "${s.spec.name}"; place them by hand: ${s.loose.join(", ")}`,
    );
  }
  if (s.noUsage) {
    lines.push(
      `! \`${tool} --help\` printed no usage line, so ${tool} may not take --help; the spec is one command that passes everything through. Write its "description" and any "options" by hand ("source.help" has what ${tool} printed).`,
    );
  }
  if (s.needsDescription.length > 0) {
    lines.push(
      `! ${s.needsDescription.length} commands have a placeholder "description" (each command's "source.help" has the tool's help):`,
      ...s.needsDescription.map((c) => `  ${c}`),
    );
  }
  if (s.noOptions.length > 0) {
    lines.push(
      `! ${s.noOptions.length} commands have no "options" read from their help; add any they take:`,
      ...s.noOptions.map((c) => `  ${c}`),
    );
  }
  if (s.truncated.length > 0) {
    lines.push(
      "! Subcommands not followed (too deep, or too many help runs):",
      ...s.truncated.map((c) => `  ${c}`),
    );
  }
  return lines;
}

class UsageError extends Error {}

main(process.argv.slice(2)).catch((e: unknown) => {
  const message = e instanceof Error ? e.message : String(e);
  console.error(`cli-plugin-kit: ${message}`);
  if (e instanceof UsageError) console.error(`\n${USAGE}`);
  process.exitCode = e instanceof UsageError ? 2 : 1;
});
