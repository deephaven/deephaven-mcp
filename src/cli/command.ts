import { type Command, Option } from "commander";
import { Store } from "../auth/store.ts";
import { result, setFormat } from "../output.ts";
import { lazyPrompter, type Prompter } from "../ui/prompt.ts";

/** What every command gets, built once from the global flags. */
export interface Context {
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
  /** The whole command tree, for help and `dh agents`. */
  cli: Command;
}

/** A command's result: what to print for people, and the `-o json` value. */
export interface Output {
  /** Printed one line per entry, without `-o json`. */
  human: string | string[];
  /** Printed as one JSON value with `-o json`. */
  json: unknown;
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
  /** `args` in declared order; `opts` are the parsed `options`. */
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
];

/** {@link GLOBAL_OPTIONS} as commander parses them. `prompt`/`input` are false with `--no-prompt`/`--no-input`. */
interface GlobalOptions {
  profile?: string;
  output?: "human" | "json";
  prompt?: boolean;
  input?: boolean;
}

/** Builds the {@link Context} from `cmd`'s parsed global flags, and sets the output format. */
function contextFromFlags(cmd: Command): Context {
  const g = cmd.optsWithGlobals() as GlobalOptions;
  const noPrompt = g.prompt === false || g.input === false;
  setFormat(g.output ?? "human");
  let cli = cmd;
  while (cli.parent) cli = cli.parent;
  return {
    store: new Store(),
    ui: lazyPrompter(!noPrompt),
    json: g.output === "json",
    profile: g.profile ?? (process.env.DH_PROFILE || undefined),
    profileFlag: g.profile,
    noPrompt,
    cli,
  };
}

/** Wires `spec` onto `cmd`: arguments, options, the shared handler, then subcommands. */
export function registerCommand(
  cmd: Command,
  spec: CommandSpec<never>,
): Command {
  cmd.description(spec.description);
  for (const a of spec.args ?? []) cmd.argument(a.name, a.description);
  for (const o of spec.options ?? []) cmd.addOption(o);
  cmd.action(async (...params: unknown[]) => {
    const self = params.at(-1) as Command;
    const out = await spec.run(
      contextFromFlags(self),
      self.processedArgs as (string | undefined)[],
      self.opts() as never,
    );
    result(() => out.human, out.json);
  });
  // `command()` (not addCommand) so subcommands inherit the root's settings.
  for (const child of spec.commands ?? []) {
    registerCommand(cmd.command(child.name), child);
  }
  return cmd;
}
