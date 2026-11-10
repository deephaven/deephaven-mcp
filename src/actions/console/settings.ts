import { DhError } from "../../errors.ts";

/** Creation overrides; null restores a field to the current server default. */
export interface ConsoleSettings {
  language?: "python" | "groovy" | null;
  heapSizeGb?: number | null;
  server?: string | null;
  engine?: string | null;
  jvmArgs?: string[] | null;
  environment?: Record<string, string> | null;
  autoDeleteTimeoutSeconds?: number | null;
}

/** Fully resolved settings ready for server capability validation. */
export interface EffectiveConsoleSettings {
  language: "python" | "groovy";
  heapSizeGb: number;
  server: string;
  engine: string;
  jvmArgs: string[];
  environment: Record<string, string>;
  autoDeleteTimeoutSeconds: number;
}

const SETTING_KEYS = new Set([
  "language",
  "heapSizeGb",
  "server",
  "engine",
  "jvmArgs",
  "environment",
  "autoDeleteTimeoutSeconds",
]);

/** Decode a JSON settings object without resolving server-dependent defaults. */
export function parseConsoleSettings(
  text: string | undefined,
): ConsoleSettings {
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
  return {
    ...readSetting(value, "language", isLanguage, "python, groovy, or null"),
    ...readSetting(
      value,
      "heapSizeGb",
      isPositiveNumber,
      "a positive number or null",
    ),
    ...readSetting(value, "server", isString, "a string or null"),
    ...readSetting(value, "engine", isString, "a string or null"),
    ...readSetting(
      value,
      "jvmArgs",
      isStringArray,
      "an array of strings or null",
    ),
    ...readSetting(
      value,
      "environment",
      isStringRecord,
      "an object of string values or null",
    ),
    ...readSetting(
      value,
      "autoDeleteTimeoutSeconds",
      isPositiveInteger,
      "a positive integer or null",
    ),
  };
}

/** Validate one optional field while preserving explicit null resets. */
function readSetting<K extends keyof ConsoleSettings>(
  value: Record<string, unknown>,
  key: K,
  accepts: (field: unknown) => field is NonNullable<ConsoleSettings[K]>,
  expected: string,
): ConsoleSettings {
  const field = value[key];
  const setting: ConsoleSettings = {};
  if (field === undefined) return setting;
  if (field !== null && !accepts(field)) {
    throw new DhError("usage", `Console ${key} must be ${expected}`);
  }
  setting[key] = field;
  return setting;
}

/** Apply global defaults, then command overrides; null always restores the base. */
export function mergeConsoleSettings(
  base: EffectiveConsoleSettings,
  defaults: ConsoleSettings,
  patch: ConsoleSettings,
): EffectiveConsoleSettings {
  const merged = structuredClone(base);
  for (const source of [defaults, patch]) {
    applyConsoleSettings(merged, base, source);
  }
  return merged;
}

/** Validate persisted or direct-action overrides before applying them. */
function applyConsoleSettings(
  merged: EffectiveConsoleSettings,
  base: EffectiveConsoleSettings,
  source: ConsoleSettings,
): void {
  for (const [key, value] of Object.entries(source)) {
    if (value === undefined) continue;
    switch (key) {
      case "language":
        merged.language = resolvedSetting(
          value,
          base.language,
          isLanguage,
          "Console language must be python or groovy",
        );
        break;
      case "heapSizeGb":
        merged.heapSizeGb = resolvedSetting(
          value,
          base.heapSizeGb,
          isPositiveNumber,
          "Console heap size must be a positive number",
        );
        break;
      case "server":
        merged.server = resolvedSetting(
          value,
          base.server,
          isNonEmptyString,
          "Console server must be a non-empty name",
        );
        break;
      case "engine":
        merged.engine = resolvedSetting(
          value,
          base.engine,
          isNonEmptyString,
          "Console engine must be a non-empty name",
        );
        break;
      case "jvmArgs":
        merged.jvmArgs = resolvedSetting(
          value,
          base.jvmArgs,
          isStringArray,
          "Console JVM arguments must be strings",
        );
        break;
      case "environment":
        merged.environment = resolvedSetting(
          value,
          base.environment,
          isStringRecord,
          "Console environment values must be strings",
        );
        break;
      case "autoDeleteTimeoutSeconds":
        merged.autoDeleteTimeoutSeconds = resolvedSetting(
          value,
          base.autoDeleteTimeoutSeconds,
          isPositiveInteger,
          "Console auto-delete timeout must be a positive integer",
        );
        break;
    }
  }
}

/** Resolve a reset or validate an override, keeping effective-setting errors distinct. */
function resolvedSetting<T>(
  value: unknown,
  base: T,
  accepts: (field: unknown) => field is T,
  message: string,
): T {
  if (value === null) return base;
  if (!accepts(value)) throw new DhError("usage", message);
  return value;
}

/** Recognize the two script languages supported by CLI console creation. */
function isLanguage(value: unknown): value is "python" | "groovy" {
  return value === "python" || value === "groovy";
}

/** Guard JSON objects, excluding null and arrays. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Guard string settings before resolving server-specific names. */
function isString(value: unknown): value is string {
  return typeof value === "string";
}

/** Reject empty server or engine names when resolving creation settings. */
function isNonEmptyString(value: unknown): value is string {
  return isString(value) && value.length > 0;
}

/** Guard the JVM argument list without coercing its values. */
function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every(isString);
}

/** Guard environment variables without coercing their values. */
function isStringRecord(value: unknown): value is Record<string, string> {
  return isRecord(value) && Object.values(value).every(isString);
}

/** Accept finite, positive heap sizes, including fractional GB values. */
function isPositiveNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

/** Require whole seconds for auto-delete timeouts. */
function isPositiveInteger(value: unknown): value is number {
  return isPositiveNumber(value) && Number.isInteger(value);
}
