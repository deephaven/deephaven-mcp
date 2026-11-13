/**
 * `cli-plugin-kit scaffold`: a `shell` spec from a tool's `--help`. Moved
 * from `dh plugin create`, without version pins or a prefix.
 */
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { stat } from "node:fs/promises";
import { basename } from "node:path";
import {
  type PluginSpec,
  parseSpec,
  type SpecCommand,
} from "../../../src/actions/plugins/spec.ts";
import { hasUsage, isUsageFor, type ParsedHelp, parseHelp } from "./help.ts";

/** How long one help run may take. */
const TIMEOUT_MS = 60_000;

/** How deep to follow subcommands, and how many help runs to allow in all. */
const MAX_DEPTH = 4;
const MAX_RUNS = 200;
/** Help runs at once; the DHE tools are JVMs, so each takes a second or more. */
const PARALLEL = 4;

/**
 * Where the tool's help is read: `local`, or over SSH on `host`. `run` gives
 * `<tool> <args>`'s stdout and stderr together, whatever it exits with.
 */
export interface HelpRunner {
  host?: string;
  run(args: string[]): Promise<string>;
}

/** `<tool> --help`, read before asking whether to read its subcommands' help too. */
export interface ToolHelp {
  path: string;
  runner: HelpRunner;
  text: string;
  /** What the help lists, `help` excluded; a one-item list's item is included unconfirmed. */
  subcommands: string[];
}

/** A spec built from a tool's help, and what still needs a person. */
export interface Scaffold {
  spec: PluginSpec;
  /** Commands (as typed, e.g. `ctl intraday rescan`) whose description is a placeholder. */
  needsDescription: string[];
  /** Commands that run the tool, but whose options couldn't be read from its help. */
  noOptions: string[];
  /** Commands not followed further because of the depth or run limit. */
  truncated: string[];
  /** `<tool> --help` printed no usage line: the tool may not take `--help`, so the spec is a guess. */
  noUsage: boolean;
  /** With `nouns`: the tool's top-level commands that aren't groups, gathered under a noun named after the plugin. */
  loose: string[];
  runs: number;
}

/**
 * Runs `<tool> --help`, the one run that's as safe as the user running it.
 */
export async function readToolHelp(
  path: string,
  runner: HelpRunner,
): Promise<ToolHelp> {
  if (runner.host === undefined) await assertToolExists(path);
  const text = await runner.run(["--help"]);
  const help = parseHelp(text, [basename(path)]);
  const candidate = help.candidate ? [help.candidate] : [];
  return {
    path,
    runner,
    text,
    subcommands: help.subcommands.length > 0 ? help.subcommands : candidate,
  };
}

/**
 * Builds a spec for a tool from `top`, its `--help`. With `recurse`, reads
 * each listed subcommand's help too (`<tool> help <sub>` where the tool has a
 * `help` command, else `<tool> <sub> --help`), recursively; without, the
 * listed subcommands become commands without running anything else. Each
 * command's options are read from its help where possible, and the help
 * itself is kept as `source.help`, for whoever edits the spec. Exit codes are
 * ignored, since some tools "fail" on `--help` but still print usage. With
 * `nouns`, each top-level group is its own noun. Options whose help mentions
 * authentication are proposed as `tool.runAs.unlessFlags`.
 */
export async function scaffoldPlugin(
  top: ToolHelp,
  name: string,
  recurse: boolean,
  nouns = false,
): Promise<Scaffold> {
  const tool = basename(top.path);
  const walk: Walk = {
    runner: top.runner,
    tool,
    plugin: name,
    recurse,
    needsDescription: [],
    noOptions: [],
    truncated: [],
    runs: 1,
    texts: [],
  };
  const root = await scaffoldNode(name, [], walk, top.text);
  const split = nouns && root.commands ? splitNouns(name, root) : undefined;
  const credentials = credentialFlags(root);
  const spec = {
    schema: 1,
    name,
    // The tool's own description, from its top-level help, else a placeholder.
    description:
      parseHelp(top.text, [tool]).description ?? `Commands from ${tool}.`,
    tool: {
      path: top.path,
      ...(credentials.length > 0
        ? { runAs: { account: "admin", unlessFlags: credentials } }
        : {}),
    },
    ...(split
      ? { nouns: split.nouns }
      : { commands: root.commands, argv: root.argv, options: root.options }),
    source: {
      createdFrom: `${tool} --help`,
      // Sorted: runs finish in any order, and the digest must not change with it.
      helpDigest: `sha256:${createHash("sha256")
        .update(walk.texts.sort().join("\0"))
        .digest("hex")}`,
      help: root.source?.help,
    },
  };
  const noUsage = !hasUsage(top.text);
  return {
    spec: parseSpec(spec, `${name}.json`),
    noUsage,
    loose: split?.loose ?? [],
    // The plugin's own description is filled in above.
    needsDescription: walk.needsDescription.filter((c) => c !== name).sort(),
    // Without usage, the warning about it says more than "no options" would.
    noOptions: walk.noOptions.filter((c) => !noUsage || c !== name).sort(),
    truncated: walk.truncated.sort(),
    runs: walk.runs,
  };
}

/**
 * `root`'s groups as nouns; its leaves (the tool's loose top-level verbs)
 * under one more noun named `name`, to be placed by hand.
 */
function splitNouns(
  name: string,
  root: SpecCommand,
): { nouns: SpecCommand[]; loose: string[] } {
  const commands = root.commands ?? [];
  const groups = commands.filter((c) => c.commands);
  const leaves = commands.filter((c) => !c.commands);
  if (leaves.length === 0) return { nouns: groups, loose: [] };
  const rest = {
    name,
    description: root.description,
    commands: leaves,
    ...(root.source ? { source: root.source } : {}),
  };
  return { nouns: [...groups, rest], loose: leaves.map((c) => c.name) };
}

/**
 * Every spelling of the options, anywhere in the tree, whose help mentions
 * authentication (`-k, --key <arg>` → `-k`, `--key`), in order of appearance.
 */
function credentialFlags(node: SpecCommand): string[] {
  const found: string[] = [];
  const visit = (n: SpecCommand) => {
    for (const o of n.options ?? []) {
      if (!/authenticat/i.test(o.description)) continue;
      for (const flag of o.flags.split(/[\s,]+/)) {
        if (flag.startsWith("-") && !found.includes(flag)) found.push(flag);
      }
    }
    for (const c of n.commands ?? []) visit(c);
  };
  visit(node);
  return found;
}

/** The walk over a tool's commands: what it runs, and what it has found so far. */
interface Walk {
  runner: HelpRunner;
  tool: string;
  plugin: string;
  recurse: boolean;
  needsDescription: string[];
  noOptions: string[];
  truncated: string[];
  runs: number;
  /** Every help text, after the command it's for, for `helpDigest`. */
  texts: string[];
}

/**
 * The command at `argv`, from its help `text`: a group if the help lists
 * subcommands (and the walk may follow them), else a leaf named `name`.
 * `listed`: its description from its parent's command listing.
 */
async function scaffoldNode(
  name: string,
  argv: string[],
  walk: Walk,
  text: string,
  listed?: string,
): Promise<SpecCommand> {
  const label = [walk.tool, ...argv].join(" ");
  const typed = [walk.plugin, ...argv.map(nounOf)].join(" ");
  walk.texts.push(`${label}\n${text}`);
  const help = parseHelp(text, [walk.tool, ...argv]);
  const { subs, texts } = await confirmedSubcommands(argv, help, walk);
  const canFollow =
    argv.length < MAX_DEPTH && walk.runs + subs.length <= MAX_RUNS;
  if (subs.length > 0 && (canFollow || !walk.recurse)) {
    const commands = walk.recurse
      ? await followSubcommands(argv, subs, help, walk, texts)
      : subs.map((sub) => listedOnly(argv, sub, help, walk));
    if (!listed) walk.needsDescription.push(typed);
    return {
      name,
      description: listed ?? label,
      commands,
      source: { help: text.trim() },
    };
  }
  if (subs.length > 0) walk.truncated.push(typed);
  const description = listed ?? help.description;
  if (!description) walk.needsDescription.push(typed);
  if (help.options.length === 0) walk.noOptions.push(typed);
  return {
    name,
    description: description ?? label,
    argv,
    ...(help.options.length > 0 ? { options: help.options } : {}),
    source: { help: text.trim() },
  };
}

/**
 * `help`'s subcommands; a one-item list's item only if its own help names it
 * (and the walk may run it). `texts`: their help, if reading it was how they
 * were confirmed.
 */
async function confirmedSubcommands(
  argv: string[],
  help: ParsedHelp,
  walk: Walk,
): Promise<{ subs: string[]; texts?: string[] }> {
  if (help.subcommands.length > 0) return { subs: help.subcommands };
  const sub = help.candidate;
  if (!sub || !walk.recurse || argv.length >= MAX_DEPTH) return { subs: [] };
  const texts = await readSubcommandHelps(argv, [sub], help, walk);
  const confirmed = isUsageFor(texts[0] ?? "", [walk.tool, ...argv, sub]);
  return confirmed ? { subs: [sub], texts } : { subs: [] };
}

/** Each subcommand of `argv`, from its own help; `known`: that help, if already read. */
async function followSubcommands(
  argv: string[],
  subs: string[],
  help: ParsedHelp,
  walk: Walk,
  known?: string[],
): Promise<SpecCommand[]> {
  const texts = known ?? (await readSubcommandHelps(argv, subs, help, walk));
  return await mapLimited(subs, PARALLEL, (sub, i) =>
    scaffoldNode(
      nounOf(sub),
      [...argv, sub],
      walk,
      texts[i] ?? "",
      help.descriptions[sub],
    ),
  );
}

/** A subcommand known only from its parent's listing; nothing is run. */
function listedOnly(
  argv: string[],
  sub: string,
  help: ParsedHelp,
  walk: Walk,
): SpecCommand {
  const description = help.descriptions[sub];
  const typed = [walk.plugin, ...argv.map(nounOf), nounOf(sub)].join(" ");
  if (!description) walk.needsDescription.push(typed);
  walk.noOptions.push(typed);
  return {
    name: nounOf(sub),
    description: description ?? [walk.tool, ...argv, sub].join(" "),
    argv: [...argv, sub],
  };
}

/**
 * The help of each of `subs` under `argv`, in order. Where the tool lists a
 * `help` command, `<tool> … help <sub>` shows it without running the
 * subcommand; if that doesn't print the first one's usage, `<sub> --help` is
 * used for all of them.
 */
async function readSubcommandHelps(
  argv: string[],
  subs: string[],
  help: ParsedHelp,
  walk: Walk,
): Promise<string[]> {
  const [first = ""] = subs;
  if (help.hasHelpCommand) {
    const text = await readHelp(walk, [...argv, "help", first]);
    if (isUsageFor(text, [walk.tool, ...argv, first])) {
      const rest = await mapLimited(subs.slice(1), PARALLEL, (sub) =>
        readHelp(walk, [...argv, "help", sub]),
      );
      return [text, ...rest];
    }
  }
  return await mapLimited(subs, PARALLEL, (sub) =>
    readHelp(walk, [...argv, sub, "--help"]),
  );
}

/** One help run, counted against {@link MAX_RUNS}. */
async function readHelp(walk: Walk, args: string[]): Promise<string> {
  walk.runs++;
  return await walk.runner.run(args);
}

/** A subcommand's name as a `dh` noun or verb: lowercase, `_` as `-`. */
function nounOf(sub: string): string {
  return sub.toLowerCase().replaceAll("_", "-");
}

/** Reads the help of the tool at `path` on this machine. */
export function localHelpRunner(path: string): HelpRunner {
  return {
    run: async (args) => {
      const r = await capture([path, ...args]);
      return joinOutput(r.stdout, r.stderr);
    },
  };
}

/**
 * Reads the help of the tool at `path` on `host`, over SSH without prompts.
 * An SSH failure (exit 255) fails the command; so does, on the first run,
 * exit 127 (the tool isn't there) and exit 126 (it can't be run).
 */
export function sshHelpRunner(path: string, host: string): HelpRunner {
  let first = true;
  return {
    host,
    async run(args) {
      const isFirst = first;
      first = false;
      const command = [path, ...args].map(shellQuote).join(" ");
      const r = await capture(["ssh", "-o", "BatchMode=yes", host, command]);
      if (r.code === 255) {
        throw new Error(`SSH to ${host} failed: ${lastLine(r.stderr)}`);
      }
      if (isFirst && r.code === 127) {
        throw new Error(`Tool not found on ${host}: ${path}`);
      }
      if (isFirst && r.code === 126) {
        throw new Error(`Can't run ${path} on ${host}: permission denied`);
      }
      return joinOutput(r.stdout, r.stderr);
    },
  };
}

/** `argv`'s stdout, stderr and exit code, whatever it exits with. */
function capture(
  argv: string[],
): Promise<{ code: number; stdout: string; stderr: string }> {
  const [command = "", ...args] = argv;
  return new Promise((resolve, reject) => {
    const proc = spawn(command, args, {
      stdio: ["ignore", "pipe", "pipe"],
      timeout: TIMEOUT_MS,
    });
    let stdout = "";
    let stderr = "";
    proc.stdout.setEncoding("utf8").on("data", (d) => (stdout += d));
    proc.stderr.setEncoding("utf8").on("data", (d) => (stderr += d));
    proc.on("error", reject);
    proc.on("close", (code, signal) => {
      if (signal === "SIGTERM" && code === null) {
        reject(
          new Error(`${argv.join(" ")} didn't finish in ${TIMEOUT_MS / 1000}s`),
        );
      } else {
        resolve({ code: code ?? 1, stdout, stderr });
      }
    });
  });
}

/** `word` for a POSIX shell: in `'…'`, each `'` written `'\''`. */
function shellQuote(word: string): string {
  return `'${word.replaceAll("'", `'\\''`)}'`;
}

function lastLine(text: string): string {
  return text.trim().split("\n").at(-1) ?? "";
}

function joinOutput(stdout: string, stderr: string): string {
  return [stdout.trimEnd(), stderr.trimEnd()].filter(Boolean).join("\n");
}

async function assertToolExists(path: string): Promise<void> {
  try {
    await stat(path);
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
    throw new Error(`Tool not found: ${path}`);
  }
}

/** `fn` over `items`, at most `limit` at a time, results in order. */
async function mapLimited<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let next = 0;
  const worker = async () => {
    while (next < items.length) {
      const i = next++;
      results[i] = await fn(items[i] as T, i);
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, worker),
  );
  return results;
}
