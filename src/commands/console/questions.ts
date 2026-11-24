import { readFile } from "node:fs/promises";
import type { ExecSource } from "../../actions/console/exec.ts";
import {
  type ConsoleSettings,
  parseConsoleSettings,
} from "../../actions/console/settings.ts";
import { DhError, isErrno, reasonOf } from "../../errors.ts";
import type { Prompter } from "../../ui/prompt.ts";

export interface ConsoleSettingOptions {
  language?: string;
  heapSizeGb?: string;
  server?: string;
  engine?: string;
  json?: string;
}

/** Read and validate one exec code source before remote or config work starts. */
export async function askExecSource(
  file: string | undefined,
  code: string | boolean | undefined,
): Promise<ExecSource> {
  if (file && code !== undefined) {
    throw new DhError(
      "usage",
      "Use one code source: a file or -c",
      "Pass a file or -c '<code>', not both.",
    );
  }
  if (file) return await readExecFile(file);
  if (typeof code === "string") return { code };
  if (code === true) return await readExecStdin();
  throw new DhError(
    "usage",
    "A code source is required",
    "Pass `dh exec <file>` or `dh exec -c '<code>'`.",
  );
}

/** Read a script file selected by the command argument. */
async function readExecFile(file: string): Promise<ExecSource> {
  try {
    return { code: await readFile(file, "utf8") };
  } catch (e) {
    throw isErrno(e, "ENOENT")
      ? new DhError("usage", `Script file not found: ${file}`)
      : new DhError(
          "usage",
          `Can't read script file: ${file}`,
          undefined,
          reasonOf(e),
        );
  }
}

/** Read nonempty code when `-c` has no inline argument. */
async function readExecStdin(): Promise<ExecSource> {
  if (process.stdin.isTTY) {
    throw new DhError(
      "usage",
      "No code received from stdin",
      "Pipe or redirect a script with `dh exec -c`, or pass `-c '<code>'.",
    );
  }
  const stdin = await Bun.stdin.text();
  if (stdin.length === 0) {
    throw new DhError(
      "usage",
      "No code received from stdin",
      "Pipe or redirect a script with `dh exec -c`, or pass `-c '<code>'.",
    );
  }
  return { code: stdin };
}

/** Read JSON settings and apply explicit CLI flags over matching fields. */
export function askConsoleSettings(
  positionalJson: string | undefined,
  options: ConsoleSettingOptions,
): ConsoleSettings {
  const settings = parseConsoleSettings(options.json ?? positionalJson);
  if (options.language !== undefined) {
    if (options.language !== "python" && options.language !== "groovy") {
      throw new DhError("usage", "Console language must be python or groovy");
    }
    settings.language = options.language;
  }
  if (options.heapSizeGb !== undefined) {
    const heapSizeGb = Number(options.heapSizeGb);
    if (!Number.isFinite(heapSizeGb) || heapSizeGb <= 0) {
      throw new DhError("usage", "Console heap size must be a positive number");
    }
    settings.heapSizeGb = heapSizeGb;
  }
  if (options.server !== undefined) settings.server = options.server;
  if (options.engine !== undefined) settings.engine = options.engine;
  return settings;
}

export type ConsoleDefaultsRequest =
  | { action: "show" }
  | { action: "reset" }
  | { action: "update"; settings: ConsoleSettings };

/** Distinguish viewing defaults from an explicit update, including empty JSON. */
export function askConsoleDefaults(
  positionalJson: string | undefined,
  options: ConsoleSettingOptions & { reset?: boolean },
): ConsoleDefaultsRequest {
  const settings = askConsoleSettings(positionalJson, options);
  const hasSettings =
    positionalJson !== undefined ||
    options.json !== undefined ||
    options.language !== undefined ||
    options.heapSizeGb !== undefined ||
    options.server !== undefined ||
    options.engine !== undefined;
  if (options.reset && hasSettings) {
    throw new DhError(
      "usage",
      "--reset cannot be combined with console settings",
    );
  }
  if (options.reset) return { action: "reset" };
  if (hasSettings) return { action: "update", settings };
  return { action: "show" };
}

/** Confirm deletion unless the caller supplied --force. */
export async function confirmDeleteConsole(
  ui: Prompter,
  target: string | undefined,
  force: boolean,
): Promise<boolean> {
  if (force) return true;
  return await ui.confirm(
    `Delete console${target ? ` ${target}` : ""}?`,
    false,
    "--force",
  );
}
