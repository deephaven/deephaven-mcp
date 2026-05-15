/**
 * Shared field validation utilities for Deephaven MCP configuration.
 *
 * These helpers contain the core validation logic shared between the community
 * and enterprise config modules. Every function throws
 * {@link ConfigurationError} on failure.
 */

import pino from "pino";
import { ConfigurationError } from "../exceptions.js";

const _logger = pino({ name: "deephaven-mcp:config/validators" });

/**
 * Validate that a configuration field has the correct type.
 *
 * Supports single types and union types (tuples). Produces a consistent error
 * message that includes the context (e.g., the containing system/session name
 * or path), the field name, and the expected vs actual type.
 *
 * @param context - Identifier of the containing section, used in error messages.
 * @param fieldName - Name of the field being validated.
 * @param value - The value to type-check.
 * @param expectedTypes - The expected JavaScript type string(s) (e.g., `"string"`, `["string", "number"]`).
 * @param options - Optional options.
 * @param options.isOptional - If `true`, error messages use `"Optional field"` instead of `"Field"`.
 * @throws {ConfigurationError} If `value` does not match `expectedTypes`.
 */
export function validateFieldType(
  context: string,
  fieldName: string,
  value: unknown,
  expectedTypes: string | string[],
  options?: { isOptional?: boolean },
): void {
  const prefix = options?.isOptional ? "Optional field" : "Field";
  const types = Array.isArray(expectedTypes) ? expectedTypes : [expectedTypes];
  const actualType = value === null ? "null" : typeof value;

  // Special handling: check JavaScript types
  if (!types.some((t) => _matchesType(value, t))) {
    let msg: string;
    if (types.length === 1) {
      msg = `${prefix} '${fieldName}' for ${context} must be of type ${types[0]}, but got ${actualType}.`;
    } else {
      msg = `${prefix} '${fieldName}' for ${context} must be one of types (${types.join(", ")}), but got ${actualType}.`;
    }
    _logger.error(`[_validators:validateFieldType] ${msg}`);
    throw new ConfigurationError(msg);
  }
}

function _matchesType(value: unknown, expectedType: string): boolean {
  if (expectedType === "null") return value === null;
  if (expectedType === "array") return Array.isArray(value);
  if (expectedType === "object") return typeof value === "object" && value !== null && !Array.isArray(value);
  return typeof value === expectedType;
}

/**
 * Validate that `data` contains only `allowed` keys with correct types.
 *
 * For each key/value pair in `data`:
 * - If the key is in `allowed`, type-check the value via {@link validateFieldType}.
 * - If the key is not in `allowed`:
 *   - `rejectUnknown=true` (default) → throw {@link ConfigurationError}.
 *   - `rejectUnknown=false` → log a warning and skip the value.
 *
 * @param context - Identifier of the containing section, used in error messages.
 * @param data - The dictionary to validate.
 * @param allowed - Map of allowed field name to expected type(s).
 * @param options - Optional options.
 * @param options.rejectUnknown - Policy for unknown fields. Defaults to `true`.
 * @throws {ConfigurationError} If an unknown key is present and `rejectUnknown` is `true`,
 *   or if any known field has an incorrect type.
 */
export function validateAllowedFields(
  context: string,
  data: Record<string, unknown>,
  allowed: Record<string, string | string[]>,
  options?: { rejectUnknown?: boolean },
): void {
  const rejectUnknown = options?.rejectUnknown ?? true;
  for (const [fieldName, value] of Object.entries(data)) {
    if (!(fieldName in allowed)) {
      if (rejectUnknown) {
        const msg = `Unknown field '${fieldName}' for ${context}. Allowed fields: ${JSON.stringify(Object.keys(allowed).sort())}.`;
        _logger.error(`[_validators:validateAllowedFields] ${msg}`);
        throw new ConfigurationError(msg);
      }
      _logger.warn(`[_validators:validateAllowedFields] Unknown field '${fieldName}' for ${context} will be ignored.`);
      continue;
    }
    validateFieldType(context, fieldName, value, allowed[fieldName]!);
  }
}

/**
 * Throw if both `fieldA` and `fieldB` are present in `data`.
 *
 * @param context - Identifier of the containing section, used in error messages.
 * @param data - The dictionary to inspect.
 * @param fieldA - First field name.
 * @param fieldB - Second field name.
 * @throws {ConfigurationError} If both fields are present.
 */
export function validateMutuallyExclusive(
  context: string,
  data: Record<string, unknown>,
  fieldA: string,
  fieldB: string,
): void {
  if (fieldA in data && fieldB in data) {
    const msg = `For ${context}, '${fieldA}' and '${fieldB}' are mutually exclusive; specify only one.`;
    _logger.error(`[_validators:validateMutuallyExclusive] ${msg}`);
    throw new ConfigurationError(msg);
  }
}

/**
 * Validate that value is a non-negative integer (not boolean).
 *
 * Booleans are explicitly rejected even though `typeof true === "boolean"` differs
 * from `"number"` in TypeScript, but we also reject `true`/`false` numerically.
 *
 * @param fieldName - Name of the field, used in error messages.
 * @param value - The value to validate.
 * @throws {ConfigurationError} If value is a boolean, not an integer, or negative.
 */
export function validateNonNegativeInt(fieldName: string, value: unknown): void {
  if (typeof value === "boolean") {
    const msg = `'${fieldName}' must be an integer, but got boolean.`;
    _logger.error(`[_validators:validateNonNegativeInt] ${msg}`);
    throw new ConfigurationError(msg);
  }
  if (typeof value !== "number" || !Number.isInteger(value)) {
    const actualType = typeof value;
    const msg = `'${fieldName}' must be an integer, but got ${actualType}.`;
    _logger.error(`[_validators:validateNonNegativeInt] ${msg}`);
    throw new ConfigurationError(msg);
  }
  if (value < 0) {
    const msg = `'${fieldName}' must be non-negative, but got ${value}.`;
    _logger.error(`[_validators:validateNonNegativeInt] ${msg}`);
    throw new ConfigurationError(msg);
  }
}

/**
 * Validate that value is a positive number (int or float, not boolean).
 *
 * @param fieldName - Name of the field, used in error messages.
 * @param value - The value to validate.
 * @throws {ConfigurationError} If value is a boolean, not a number, or not positive.
 */
export function validatePositiveNumber(fieldName: string, value: unknown): void {
  if (typeof value === "boolean") {
    const msg = `'${fieldName}' must be a number (int or float), but got boolean.`;
    _logger.error(`[_validators:validatePositiveNumber] ${msg}`);
    throw new ConfigurationError(msg);
  }
  if (typeof value !== "number") {
    const msg = `'${fieldName}' must be a number (int or float), but got ${typeof value}.`;
    _logger.error(`[_validators:validatePositiveNumber] ${msg}`);
    throw new ConfigurationError(msg);
  }
  if (value <= 0) {
    const msg = `'${fieldName}' must be positive, but got ${value}.`;
    _logger.error(`[_validators:validatePositiveNumber] ${msg}`);
    throw new ConfigurationError(msg);
  }
}

/**
 * Validate that `config[fieldName]` is a positive number if present and non-null/undefined.
 *
 * Silently passes when the field is absent or its value is null/undefined.
 *
 * @param config - The configuration dictionary to inspect.
 * @param fieldName - Key to look up in `config`.
 * @throws {ConfigurationError} If the field is present with a non-null/undefined value that
 *   is a boolean, not a number, or not positive.
 */
export function validateOptionalPositiveNumber(config: Record<string, unknown>, fieldName: string): void {
  if (!(fieldName in config)) return;
  const value = config[fieldName];
  if (value === null || value === undefined) return;
  validatePositiveNumber(fieldName, value);
}

/**
 * Validate that a list contains only string items.
 *
 * @param fieldName - Name of the list field, used in error messages.
 * @param items - The list to validate.
 * @throws {ConfigurationError} If any item is not a string.
 */
export function validateStringList(fieldName: string, items: unknown[]): void {
  for (let i = 0; i < items.length; i++) {
    if (typeof items[i] !== "string") {
      const msg = `'${fieldName}[${i}]' must be a string, got ${typeof items[i]}.`;
      _logger.error(`[_validators:validateStringList] ${msg}`);
      throw new ConfigurationError(msg);
    }
  }
}

/**
 * Validate that `config[fieldName]` is a list of strings if present.
 *
 * @param config - The configuration dictionary to inspect.
 * @param fieldName - Key to look up in `config`.
 * @throws {ConfigurationError} If the field is present and contains a non-string item.
 */
export function validateOptionalStringList(config: Record<string, unknown>, fieldName: string): void {
  if (!(fieldName in config)) return;
  validateStringList(fieldName, config[fieldName] as unknown[]);
}

/**
 * Validate that a dict has string keys and string values.
 *
 * @param fieldName - Name of the dict field, used in error messages.
 * @param mapping - The dict to validate.
 * @throws {ConfigurationError} If any key or value is not a string.
 */
export function validateStringDict(fieldName: string, mapping: Record<string, unknown>): void {
  for (const [key, value] of Object.entries(mapping)) {
    if (typeof key !== "string") {
      const msg = `'${fieldName}' key must be a string, got ${typeof key}.`;
      _logger.error(`[_validators:validateStringDict] ${msg}`);
      throw new ConfigurationError(msg);
    }
    if (typeof value !== "string") {
      const msg = `'${fieldName}[${key}]' value must be a string, got ${typeof value}.`;
      _logger.error(`[_validators:validateStringDict] ${msg}`);
      throw new ConfigurationError(msg);
    }
  }
}

/**
 * Validate that `config[fieldName]` is a string→string dict if present.
 *
 * @param config - The configuration dictionary to inspect.
 * @param fieldName - Key to look up in `config`.
 * @throws {ConfigurationError} If the field is present and has a non-string key or value.
 */
export function validateOptionalStringDict(config: Record<string, unknown>, fieldName: string): void {
  if (!(fieldName in config)) return;
  validateStringDict(fieldName, config[fieldName] as Record<string, unknown>);
}

/**
 * Read a required secret from a named environment variable.
 *
 * Used to resolve a config-file `<field>_env_var` indirection: the config field
 * stores the *name* of an environment variable, and the actual secret is read
 * from that variable at use time.
 *
 * @param options - Options object.
 * @param options.fieldName - The config field that named the env var.
 * @param options.envVarName - The actual environment variable name.
 * @param options.context - Human-readable description of the surrounding config.
 * @returns The non-empty value of the environment variable.
 * @throws {ConfigurationError} If the variable is unset or empty.
 */
export function resolveRequiredEnvVar(options: {
  fieldName: string;
  envVarName: string;
  context: string;
}): string {
  const { fieldName, envVarName, context } = options;
  const value = process.env[envVarName];
  if (!value) {
    const msg = `${context}: '${fieldName}' refers to environment variable '${envVarName}' which is unset or empty.`;
    _logger.error(`[_validators:resolveRequiredEnvVar] ${msg}`);
    throw new ConfigurationError(msg);
  }
  return value;
}

/**
 * Resolve a secret from an inline value or a `<field>_env_var` indirection.
 *
 * Honors the project-wide schema convention enforced by
 * {@link validateMutuallyExclusive}: at most one of the two fields is populated
 * in a validated config. Looks at the inline field first; falls back to the
 * env-var field via {@link resolveRequiredEnvVar}. Returns `undefined` when
 * neither field is set.
 *
 * @param options - Options object.
 * @param options.config - The config dict to read from.
 * @param options.inlineField - Name of the inline value field (e.g. `"psk"`).
 * @param options.envVarField - Name of the env-var indirection field (e.g. `"psk_env_var"`).
 * @param options.context - Human-readable surrounding-config description for error messages.
 * @returns Resolved secret string, or `undefined` if neither field is set.
 * @throws {ConfigurationError} If `envVarField` names an env var that is unset or empty.
 */
export function resolveSecretField(options: {
  config: Record<string, unknown>;
  inlineField: string;
  envVarField: string;
  context: string;
}): string | undefined {
  const { config, inlineField, envVarField, context } = options;
  const inline = config[inlineField];
  if (typeof inline === "string" && inline) return inline;
  const envVar = config[envVarField];
  if (typeof envVar === "string" && envVar) {
    return resolveRequiredEnvVar({
      fieldName: envVarField,
      envVarName: envVar,
      context,
    });
  }
  return undefined;
}
