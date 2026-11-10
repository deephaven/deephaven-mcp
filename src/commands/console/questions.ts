import {
  type ConsoleSettings,
  parseConsoleSettings,
} from "../../actions/console/settings.ts";
import { DhError } from "../../errors.ts";
import type { Prompter } from "../../ui/prompt.ts";

export interface ConsoleSettingOptions {
  language?: string;
  heapSizeGb?: string;
  server?: string;
  engine?: string;
  json?: string;
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
