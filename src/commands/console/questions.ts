import type { ConsoleSettings } from "../../actions/console/store.ts";
import { DhError } from "../../errors.ts";
import type { Prompter } from "../../ui/prompt.ts";

const SETTING_KEYS = new Set([
  "language",
  "heapSizeGb",
  "server",
  "engine",
  "jvmArgs",
  "environment",
  "autoDeleteTimeoutSeconds",
]);

export interface ConsoleSettingOptions {
  language?: string;
  heapSizeGb?: string;
  server?: string;
  engine?: string;
  json?: string;
}

export function askConsoleSettings(
  positionalJson: string | undefined,
  options: ConsoleSettingOptions,
): ConsoleSettings {
  const settings = parseSettings(options.json ?? positionalJson);
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

function parseSettings(text: string | undefined): ConsoleSettings {
  if (text === undefined) return {};
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch (error) {
    throw new DhError(
      "usage",
      "Console settings must be valid JSON",
      undefined,
      error instanceof Error ? error.message : String(error),
    );
  }
  if (!isRecord(value)) {
    throw new DhError("usage", "Console settings must be a JSON object");
  }
  for (const key of Object.keys(value)) {
    if (!SETTING_KEYS.has(key)) {
      throw new DhError("usage", `Unknown console setting "${key}"`);
    }
  }
  const settings: ConsoleSettings = {};
  const language = value.language;
  if (language === null || language === "python" || language === "groovy") {
    settings.language = language;
  } else if (language !== undefined) {
    throw new DhError(
      "usage",
      "Console language must be python, groovy, or null",
    );
  }
  const heapSizeGb = value.heapSizeGb;
  if (heapSizeGb === null || isPositiveNumber(heapSizeGb)) {
    settings.heapSizeGb = heapSizeGb;
  } else if (heapSizeGb !== undefined) {
    throw new DhError(
      "usage",
      "Console heapSizeGb must be a positive number or null",
    );
  }
  for (const key of ["server", "engine"] as const) {
    const field = value[key];
    if (field === null || typeof field === "string") {
      settings[key] = field;
    } else if (field !== undefined) {
      throw new DhError("usage", `Console ${key} must be a string or null`);
    }
  }
  const jvmArgs = value.jvmArgs;
  if (
    jvmArgs === null ||
    (Array.isArray(jvmArgs) && jvmArgs.every((arg) => typeof arg === "string"))
  ) {
    settings.jvmArgs = jvmArgs;
  } else if (jvmArgs !== undefined) {
    throw new DhError(
      "usage",
      "Console jvmArgs must be an array of strings or null",
    );
  }
  const environment = value.environment;
  if (environment === null || environment === undefined) {
    if (environment === null) settings.environment = null;
  } else if (isStringRecord(environment)) {
    settings.environment = environment;
  } else {
    throw new DhError(
      "usage",
      "Console environment must be an object of string values or null",
    );
  }
  const timeout = value.autoDeleteTimeoutSeconds;
  if (timeout === null || isPositiveInteger(timeout)) {
    settings.autoDeleteTimeoutSeconds = timeout;
  } else if (timeout !== undefined) {
    throw new DhError(
      "usage",
      "Console autoDeleteTimeoutSeconds must be a positive integer or null",
    );
  }
  return settings;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isStringRecord(value: unknown): value is Record<string, string> {
  return (
    isRecord(value) &&
    Object.values(value).every((item) => typeof item === "string")
  );
}

function isPositiveNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

function isPositiveInteger(value: unknown): value is number {
  return isPositiveNumber(value) && Number.isInteger(value);
}

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
