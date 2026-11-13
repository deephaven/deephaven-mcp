import { type Command, Option } from "commander";
import { Store } from "../actions/auth/store.ts";
import type { JsOutput } from "../actions/plugins/plugin-json.ts";
import type { Exec } from "../actions/plugins/settings.ts";
import type { RunAs } from "../actions/plugins/spec.ts";
import { commandUsageError, DhError } from "../errors.ts";
import { result, setFormat } from "../output.ts";
import { lazyPrompter, type Prompter } from "../ui/prompt.ts";
import type { CommandLifecycle } from "./lifecycle.ts";

/** What every command gets, built once from the global flags. */
export interface Context {
  /** Cancellation and cleanup for this command invocation. */
  lifecycle: CommandLifecycle;
  /** The command's cancellation signal. */
  signal: AbortSignal;
  /** The config directory: profiles and credentials. */
  store: Store;
  /** Questions for the user; without a terminal or with --no-prompt, they fail naming the flag. */
  ui: Prompter;
  /** True with `-o json`. */
  json: boolean;
  /** `--profile`, then `DH_PROFILE`. */
  profile?: string;
  /** `--profile` alone, without the `DH_PROFILE` fallback. */
  profileFlag?: string;
  /** True with --no-prompt (or the hidden --no-input). */
  noPrompt: boolean;
  /** `--ssh-host`, the last one given; only an `ssh` plugin's commands get one. */
  sshHost?: string;
  /** The whole command tree, for help and `dh agents`. */
  cli: Command;
}

/** A command's result: what to print for people, and the `-o json` value. */
export interface Output {
  /** Printed one line per entry, without `-o json`. */
  human: string | string[];
  /** Printed as one JSON value with `-o json`. */
  json: unknown;
  /** The command succeeded without a result to print. */
  empty?: boolean;
  /** A partial failure: reported after the result, and sets the exit status. */
  error?: DhError;
}

/** One positional argument. */
export interface ArgSpec {
  /** Commander syntax: `<required>` or `[optional]`. */
  name: string;
  description?: string;
}

/**
 * One command. `run` reads as its script, alternating questions (prompts via
 * `ctx.ui`, returning plain answers) and actions (network and disk, never
 * prompting), and returns what to print.
 */
export interface CommandSpec<Opts = Record<string, never>> {
  name: string;
  /** One line, shown in help and `dh agents`. */
  description: string;
  args?: ArgSpec[];
  /** The command's own flags; global ones come from {@link GLOBAL_OPTIONS}. */
  options?: Option[];
  /**
   * Accept any arguments and flags, and hand them to `run` as typed, in order.
   * For a plugin's commands, which run its tool: of the global flags, only
   * {@link PLUGIN_GLOBAL_FLAGS} are `dh`'s (see `cli/plugin-args.ts`).
   */
  passthrough?: boolean;
  /** Set on a plugin's noun: the plugin, the tool its commands run, and where. Listed by `dh agents`. */
  plugin?: PluginMarker;
  /** Left out of help and `dh agents`. */
  hidden?: boolean;
  /** A line shown after the command's help. */
  helpText?: string;
  /** A `js` plugin leaf's result, documented in help and `dh agents`. */
  output?: JsOutput;
  /** `args` in declared order, or every argument with `passthrough`; `opts` are the parsed `options`. */
  run(ctx: Context, args: (string | undefined)[], opts: Opts): Promise<Output>;
  /** Subcommands; they inherit the global flags. */
  commands?: CommandSpec<never>[];
}

/** Only checks the shape; `opts` is typed as `Opts`. */
export function defineCommand<Opts = Record<string, never>>(
  spec: CommandSpec<Opts>,
): CommandSpec<Opts> {
  return spec;
}

/** The flags every command accepts. */
export const GLOBAL_OPTIONS = [
  new Option("--profile <name>", "Profile to use (env DH_PROFILE)."),
  new Option("-o, --output <format>", "Output format.")
    .choices(["human", "json"])
    .default("human"),
  new Option(
    "--no-prompt",
    "Never prompt; fail with the flag to pass instead.",
  ),
  // The older name; agents trained on other CLIs may still reach for it.
  new Option("--no-input").hideHelp(),
  // Shown only in an `ssh` plugin's help. Collected, since commander parses it
  // here even for commands that declare their own (see `ownGlobals`).
  new Option(
    "--ssh-host <host>",
    "Which of the plugin's SSH hosts to run on (default: its default host).",
  )
    .argParser(collect)
    .hideHelp(),
];

/** `-o json` output: rows of {name: string — Server name., …}. */
function outputHelp(output: JsOutput): string {
  const what = {
    rows: "a list of rows",
    object: "an object",
    text: '{"text": …}',
    none: "null",
  }[output.kind];
  const fields = Object.entries(output.fields ?? {}).map(
    ([name, f]) => `  ${name} (${f.type}): ${f.description}`,
  );
  return [
    `Output (-o json): ${what}${fields.length > 0 ? ", each with:" : ""}`,
    ...fields,
  ].join("\n");
}

/** A repeatable option's values, in order. */
export function collect(value: string, previous: string[] | undefined) {
  return [...(previous ?? []), value];
}

/** A plugin's noun: the plugin it belongs to, and for a `shell` plugin the tool its commands run, and where. */
export type PluginMarker =
  | {
      /** The plugin's name (its identity); a plugin may register several nouns. */
      name: string;
      kind: "shell";
      tool: string;
      exec: Exec;
      /** The spec's `tool.runAs`, if any. */
      runAs?: RunAs;
    }
  | { name: string; kind: "js" };

/** The global flags a plugin's commands take, by `exec`; every other flag goes to the tool. */
export const PLUGIN_GLOBAL_FLAGS: Record<Exec, string[]> = {
  local: ["-o, --output <format>", "-h, --help"],
  ssh: [
    "-o, --output <format>",
    "-h, --help",
    "--profile <name>",
    "--ssh-host <host>",
    "--no-prompt",
  ],
};

/** Commands registered with `passthrough`. */
const passthroughCommands = new WeakSet<Command>();
/** Commands registered with `hidden`. */
const hiddenCommands = new WeakSet<Command>();

/** Commands' documented results, for `dh agents`. */
const outputs = new WeakMap<Command, JsOutput>();

/** A `js` plugin leaf's documented result. */
export function outputOf(cmd: Command): JsOutput | undefined {
  return outputs.get(cmd);
}

/** Whether `cmd` is left out of help and `dh agents`. */
export function isHidden(cmd: Command): boolean {
  return hiddenCommands.has(cmd);
}
/** Plugin nouns, with the tool they wrap. */
const pluginTools = new WeakMap<Command, PluginMarker>();

/** Whether `cmd` hands every argument to a tool. */
export function isPassthrough(cmd: Command): boolean {
  return passthroughCommands.has(cmd);
}

/** The tool `cmd` wraps and where, if `cmd` is a plugin's noun. */
export function pluginOf(cmd: Command): PluginMarker | undefined {
  return pluginTools.get(cmd);
}

/** Where the plugin runs, if `cmd` is a plugin's noun or one of its subcommands. */
export function pluginExecOf(cmd: Command): Exec | undefined {
  for (let c: Command | null = cmd; c; c = c.parent) {
    const marker = pluginTools.get(c);
    if (marker) return marker.kind === "shell" ? marker.exec : undefined;
  }
  return undefined;
}

/** {@link GLOBAL_OPTIONS} as commander parses them. `prompt`/`input` are false with `--no-prompt`/`--no-input`. */
interface GlobalOptions {
  profile?: string;
  output?: "human" | "json";
  prompt?: boolean;
  input?: boolean;
  sshHost?: string[];
}

/** Builds the {@link Context} from `cmd`'s parsed global flags, and sets the output format. */
function contextFromFlags(cmd: Command, lifecycle: CommandLifecycle): Context {
  const g = cmd.optsWithGlobals() as GlobalOptions;
  const noPrompt = g.prompt === false || g.input === false;
  setFormat(g.output ?? "human");
  let cli = cmd;
  while (cli.parent) cli = cli.parent;
  lifecycle.setFailureReport({
    command: commandPath(cmd, false),
  });
  return {
    lifecycle,
    signal: lifecycle.signal,
    store: new Store(),
    ui: lazyPrompter(!noPrompt),
    json: g.output === "json",
    profile: g.profile ?? (process.env.DH_PROFILE || undefined),
    profileFlag: g.profile,
    noPrompt,
    sshHost: g.sshHost?.at(-1),
    cli,
  };
}

/**
 * `cmd`'s options, with the global flags it also declares itself (`dh plugin
 * set --ssh-host`) taken from the root, which commander gives them to. A
 * global `--ssh-host` on any other command but an `ssh` plugin's is `usage`.
 */
function ownGlobals(cmd: Command, spec: CommandSpec<never>) {
  const opts = { ...cmd.opts() };
  const globals = cmd.optsWithGlobals();
  const own = new Set((spec.options ?? []).map((o) => o.attributeName()));
  for (const key of own) {
    if (opts[key] === undefined && globals[key] !== undefined) {
      opts[key] = globals[key];
    }
  }
  if (
    globals.sshHost !== undefined &&
    !own.has("sshHost") &&
    pluginExecOf(cmd) !== "ssh"
  ) {
    throw new DhError(
      "usage",
      "--ssh-host only applies to commands of plugins that run over SSH",
      `Run \`${commandPath(cmd)} --help\` for usage.`,
    );
  }
  return opts;
}

/** Wires `spec` onto `cmd`: arguments, options, the shared handler, then subcommands. */
export function registerCommand(
  cmd: Command,
  spec: CommandSpec<never>,
  lifecycle: CommandLifecycle,
): Command {
  cmd.description(spec.description);
  for (const a of spec.args ?? []) cmd.argument(a.name, a.description);
  for (const o of spec.options ?? []) cmd.addOption(o);
  if (spec.plugin) pluginTools.set(cmd, spec.plugin);
  if (spec.hidden) hiddenCommands.add(cmd);
  if (spec.helpText) cmd.addHelpText("after", `\n${spec.helpText}`);
  if (spec.output) {
    outputs.set(cmd, spec.output);
    cmd.addHelpText("after", `\n${outputHelp(spec.output)}`);
  }
  if (spec.passthrough) {
    passthroughCommands.add(cmd);
    // Unknown flags land in `args` in order, with what follows `--`.
    cmd.allowUnknownOption();
  }
  // Checked below, so a typo'd subcommand and an extra argument read the same.
  cmd.allowExcessArguments();
  cmd.action(async (...params: unknown[]) => {
    const self = params.at(-1) as Command;
    const opts = ownGlobals(self, spec);
    const extra = self.args[spec.args?.length ?? 0];
    if (extra !== undefined && !spec.passthrough) {
      throw commandUsageError(
        `Unknown command or extra argument "${extra}"`,
        commandPath(self),
      );
    }
    const out = await spec.run(
      contextFromFlags(self, lifecycle),
      spec.passthrough
        ? self.args
        : (self.processedArgs as (string | undefined)[]),
      opts as never,
    );
    result(() => out.human, out.json, out.empty);
    if (out.error) throw out.error;
  });
  // `command()` (not addCommand) so subcommands inherit the root's settings.
  for (const child of spec.commands ?? []) {
    registerCommand(
      cmd.command(child.name, { hidden: child.hidden === true }),
      child,
      lifecycle,
    );
  }
  return cmd;
}

/**
 * The deepest command `args` name under `root`, e.g. `auth status` in
 * `dh auth status extra`, and the index just past its name in `args`.
 */
export function commandAt(
  root: Command,
  args: string[],
): { cmd: Command; end: number } {
  let cmd = root;
  let end = 0;
  for (let i = 0; i < args.length; i++) {
    const a = args[i] ?? "";
    if (a === "--") break;
    // The global flags that take a value; the value isn't a command.
    if (
      a === "--profile" ||
      a === "--ssh-host" ||
      a === "-o" ||
      a === "--output"
    ) {
      i++;
    }
    if (a.startsWith("-")) continue;
    const sub = cmd.commands.find((c) => c.name() === a);
    if (!sub) break;
    cmd = sub;
    end = i + 1;
  }
  return { cmd, end };
}

/** `cmd` as typed, e.g. `dh auth status`. */
export function commandPath(cmd: Command, includeRoot = true): string {
  const names: string[] = [];
  for (let c: Command | null = cmd; c; c = c.parent) names.unshift(c.name());
  return names.slice(includeRoot ? 0 : 1).join(" ");
}
