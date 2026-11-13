import type { Command } from "commander";
import { DhError } from "../errors.ts";
import {
  commandAt,
  commandPath,
  isPassthrough,
  pluginExecOf,
} from "./command.ts";

/** Global flags with no effect on a `local` plugin's commands, and `dh`'s on an `ssh` plugin's. */
const SSH_ONLY = ["--profile", "--ssh-host", "--no-prompt", "--no-input"];

/**
 * `args` as commander should parse them. For a plugin command, every flag
 * after the command goes behind `--`, so commander hands it to the tool,
 * except `dh`'s `-o json`/`-o human` and `-h`/`--help`, and for an `ssh`
 * plugin also `--profile`, `--ssh-host`, `--no-prompt` and `--no-input`. A `--` the user
 * typed is folded in, so the tool never sees an extra one. Any other `args`
 * are returned as given.
 *
 * `-o` is `dh`'s only with a value `dh` accepts, so a tool's own `-o out.csv`
 * reaches it without `--`. On an unavailable plugin's bare noun, `-h` and
 * `--help` go behind `--` too, so they give its error instead of help.
 */
export function routePluginArgs(root: Command, args: string[]): string[] {
  const { cmd, end } = commandAt(root, args);
  if (!isPassthrough(cmd)) return args;
  const exec = pluginExecOf(cmd);
  const ssh = exec === "ssh";
  // An unavailable plugin's noun: hidden, unmarked, and every use is its error.
  const bare = exec === undefined;
  if (exec === "local") assertNoInapplicableFlags(cmd, args.slice(0, end));
  const dh: string[] = [];
  const tool: string[] = [];
  const rest = args.slice(end);
  for (let i = 0; i < rest.length; i++) {
    const a = rest[i] ?? "";
    const next = rest[i + 1];
    if (a === "--") {
      tool.push(...rest.slice(i + 1));
      break;
    }
    if ((a === "-o" || a === "--output") && isFormat(next)) {
      dh.push(a, next ?? "");
      i++;
    } else if (
      ssh &&
      (a === "--profile" || a === "--ssh-host") &&
      next !== undefined &&
      next !== "--"
    ) {
      dh.push(a, next);
      i++;
    } else if ((isDhFlag(a) && !(bare && isHelp(a))) || (ssh && isSshFlag(a)))
      dh.push(a);
    else tool.push(a);
  }
  return [...args.slice(0, end), ...dh, "--", ...tool];
}

/** `--profile`, `--ssh-host` and `--no-prompt` before a plugin command were meant for `dh`, which can't use them there. */
function assertNoInapplicableFlags(cmd: Command, before: string[]): void {
  const flag = before.find((a) =>
    SSH_ONLY.some((f) => a === f || a.startsWith(`${f}=`)),
  );
  if (!flag) return;
  const name = flag.split("=")[0];
  throw new DhError(
    "usage",
    `${name} doesn't apply to \`${commandPath(cmd)}\`; it runs a tool on this machine`,
    `To pass ${name} to the tool, put it after the command.`,
  );
}

function isHelp(a: string): boolean {
  return a === "-h" || a === "--help";
}

/** What startup needs from the command line before parsing it. */
export interface StartupArgs {
  /** `--profile <v>` or `--profile=<v>`, the last before `--`. */
  profile?: string;
  /** `--ssh-host <v>` or `--ssh-host=<v>`, the last before `--`. */
  sshHost?: string;
  /** The first argument before `--` that isn't a flag or a global flag's value: the noun. */
  noun?: string;
}

/** {@link StartupArgs} from `args`, read as `routePluginArgs` keeps `--profile`: anywhere before `--`. */
export function startupArgs(args: string[]): StartupArgs {
  const found: StartupArgs = {};
  for (let i = 0; i < args.length; i++) {
    const a = args[i] ?? "";
    if (a === "--") break;
    if (a === "--profile" || a === "--ssh-host") {
      const value = args[i + 1];
      if (value !== undefined && value !== "--") {
        if (a === "--profile") found.profile = value;
        else found.sshHost = value;
      }
      i++;
    } else if (a.startsWith("--profile=")) {
      found.profile = a.slice("--profile=".length);
    } else if (a.startsWith("--ssh-host=")) {
      found.sshHost = a.slice("--ssh-host=".length);
    } else if (a === "-o" || a === "--output") {
      i++;
    } else if (!a.startsWith("-") && found.noun === undefined) {
      found.noun = a;
    }
  }
  return found;
}

/** One-token spellings of `dh`'s flags: `-ojson`, `--output=json`, `-h`, `--help`. */
function isDhFlag(a: string): boolean {
  if (a === "-h" || a === "--help") return true;
  if (a.startsWith("--output=")) return isFormat(a.slice("--output=".length));
  return a === "-ojson" || a === "-ohuman";
}

/** `--profile=<name>`, `--ssh-host=<host>`, `--no-prompt`, `--no-input`: `dh`'s on an `ssh` plugin. A bare trailing `--profile` or `--ssh-host` too, so commander reports its missing value. */
function isSshFlag(a: string): boolean {
  return (
    a === "--profile" ||
    a.startsWith("--profile=") ||
    a === "--ssh-host" ||
    a.startsWith("--ssh-host=") ||
    a === "--no-prompt" ||
    a === "--no-input"
  );
}

function isFormat(value: string | undefined): boolean {
  return value === "json" || value === "human";
}
