/**
 * Typed helpers for reading process environment variables.
 *
 * Centralizes the parse-with-default pattern that otherwise gets duplicated
 * across the codebase as `parseInt(process.env[NAME] ?? "60", 10)` /
 * `parseFloat(process.env[NAME] ?? "1.0")` / ad-hoc truthy checks. Every
 * helper:
 *
 * - Reads the variable lazily at call time (so tests can stub freely;
 *   nothing is captured at import time).
 * - Returns the documented default when the variable is unset.
 * - The parsing helpers ({@link envInt}, {@link envFloat}) throw
 *   `ValueError` whose message names the offending environment variable
 *   so operators can fix the misconfiguration without digging through stack frames.
 *
 * The helpers preserve the externally visible semantics of the inline
 * expressions they replace:
 *
 * - `envStr(name, default)` mirrors `process.env[name]` (a set variable returns
 *   its value, even when empty).
 * - `envInt` / `envFloat` cast via parseInt / parseFloat but throw ValueError
 *   with an actionable message.
 * - `envBool` follows the uvicorn convention: case-insensitive and
 *   whitespace-trimmed match against `{"1", "true", "yes"}`; everything
 *   else is falsy.
 * - `envRequired` throws `RuntimeError` when the variable is missing or empty,
 *   matching how the few existing required-env-var callers behave today.
 */

/**
 * Case-insensitive truthy values for boolean environment variables.
 *
 * Matches the convention used by uvicorn and most Python services: an env
 * var is "true" iff it is set to one of these tokens (after stripping
 * surrounding whitespace and lowercasing). Anything else (including the
 * common typos `on`/`y`/`t`) is falsy. Using a small explicit set
 * avoids accidentally treating arbitrary non-empty strings as truthy.
 */
export const _TRUTHY_ENV_VALUES: ReadonlySet<string> = new Set(["1", "true", "yes"]);

/**
 * Error raised when an environment variable cannot be parsed as the expected type.
 */
export class ValueError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = "ValueError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Error raised when a required environment variable is missing or empty.
 */
export class RuntimeError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = "RuntimeError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Return the environment variable `name` as a string.
 *
 * A set variable returns its value even when that value is the empty string.
 * Callers that want "empty -> default" semantics should write
 * `envStr(name) ?? fallback`.
 *
 * @param name - The environment variable name.
 * @param defaultValue - Value to return when `name` is unset. Defaults to `undefined`.
 * @returns The variable's value, or `defaultValue` if unset.
 */
export function envStr(name: string): string | undefined;
export function envStr(name: string, defaultValue: string): string;
export function envStr(name: string, defaultValue: undefined): string | undefined;
export function envStr(name: string, defaultValue?: string): string | undefined {
  const val = process.env[name];
  if (val === undefined) {
    return defaultValue;
  }
  return val;
}

/**
 * Return the environment variable `name` parsed as an integer.
 *
 * @param name - The environment variable name.
 * @param defaultValue - Value to return when `name` is unset.
 * @returns The parsed integer, or `defaultValue` if unset.
 * @throws {@link ValueError} If the variable is set to a value that cannot be parsed.
 *   The message names the offending variable and includes the underlying error.
 */
export function envInt(name: string, defaultValue: number): number {
  const raw = process.env[name];
  if (raw === undefined) {
    return defaultValue;
  }
  const parsed = parseInt(raw, 10);
  if (isNaN(parsed) || String(parsed) !== raw.trim()) {
    throw new ValueError(
      `Environment variable ${name}='${raw}' is not a valid integer`
    );
  }
  return parsed;
}

/**
 * Return the environment variable `name` parsed as a float.
 *
 * @param name - The environment variable name.
 * @param defaultValue - Value to return when `name` is unset.
 * @returns The parsed float, or `defaultValue` if unset.
 * @throws {@link ValueError} If the variable is set to a value that cannot be parsed.
 *   The message names the offending variable and includes the underlying error.
 */
export function envFloat(name: string, defaultValue: number): number {
  const raw = process.env[name];
  if (raw === undefined) {
    return defaultValue;
  }
  const parsed = parseFloat(raw);
  if (isNaN(parsed)) {
    throw new ValueError(
      `Environment variable ${name}='${raw}' is not a valid float`
    );
  }
  return parsed;
}

/**
 * Return the environment variable `name` as a boolean.
 *
 * @param name - The environment variable name.
 * @param defaultValue - Value to return when `name` is unset. Defaults to `false` (fail-closed).
 * @returns `true` if the variable is set to a value in {@link _TRUTHY_ENV_VALUES}
 *   (case-insensitive, whitespace trimmed); `false` if set to anything else;
 *   `defaultValue` if unset.
 */
export function envBool(name: string, defaultValue: boolean = false): boolean {
  const raw = process.env[name];
  if (raw === undefined) {
    return defaultValue;
  }
  return _TRUTHY_ENV_VALUES.has(raw.trim().toLowerCase());
}

/**
 * Options for {@link envRequired}.
 */
export interface EnvRequiredOptions {
  /** Custom message for the thrown error. When `undefined`, the default message is used. */
  errorMsg?: string;
}

/**
 * Return the environment variable `name` or throw.
 *
 * A variable is considered "missing" if it is unset OR set to the empty string;
 * both cases throw. This is intentionally stricter than `process.env[name]`'s
 * notion of "set" so an empty value is treated as misconfiguration rather than
 * silently accepted.
 *
 * @param name - The environment variable name.
 * @param options - Optional options including custom error message.
 * @returns The variable's non-empty value.
 * @throws {@link RuntimeError} When the variable is unset or empty.
 */
export function envRequired(name: string, options?: EnvRequiredOptions): string {
  const value = process.env[name];
  if (!value) {
    throw new RuntimeError(
      options?.errorMsg ?? `Environment variable ${name} is not set.`
    );
  }
  return value;
}
