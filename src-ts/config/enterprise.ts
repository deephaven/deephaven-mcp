/**
 * Configuration handling for the Deephaven MCP enterprise server.
 *
 * Validates and redacts flat enterprise config files and provides the
 * {@link EnterpriseServerConfigManager} used by `dh-mcp-enterprise-server`.
 *
 * Enterprise config file format (flat — all fields at top level):
 * ```json
 * {
 *   "system_name": "prod",
 *   "connection_json_url": "https://dhe.example.com/iris/connection.json",
 *   "auth": {
 *     "backends": ["password", "private_key"],
 *     "allow_effective_user": false
 *   },
 *   "session_creation": {
 *     "max_concurrent_sessions": 5,
 *     "defaults": {"heap_size_gb": 4, "programming_language": "Python"}
 *   }
 * }
 * ```
 *
 * Top-level schema:
 * - **Required**: `system_name`, `connection_json_url`, `auth`.
 * - **Optional**: `session_creation`, `connection_timeout`,
 *   `mcp_session_idle_timeout_seconds`.
 */

import pino from "pino";
import { ConfigurationError } from "../exceptions.js";
import {
  ConfigManager,
  _getConfigPath,
  _loadAndValidateConfig,
  _logConfigSummary,
} from "./base.js";
import {
  validateAllowedFields,
  validateFieldType,
  validateNonNegativeInt,
  validateOptionalPositiveNumber,
} from "./validators.js";

const _logger = pino({ name: "deephaven-mcp:config/enterprise" });

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/**
 * Default timeout in seconds for establishing connections to enterprise systems.
 */
export const DEFAULT_CONNECTION_TIMEOUT_SECONDS = 10.0;

/**
 * The set of values allowed in `auth.backends`.
 */
export const SUPPORTED_AUTH_BACKENDS: ReadonlySet<string> = new Set([
  "password",
  "private_key",
]);

const _REQUIRED_TOP_LEVEL_FIELDS: Record<string, string | string[]> = {
  system_name: "string",
  connection_json_url: "string",
  auth: "object",
};

const _OPTIONAL_TOP_LEVEL_FIELDS: Record<string, string | string[]> = {
  session_creation: "object",
  connection_timeout: "number",
  mcp_session_idle_timeout_seconds: "number",
};

const _ALLOWED_TOP_LEVEL_FIELDS: Record<string, string | string[]> = {
  ..._REQUIRED_TOP_LEVEL_FIELDS,
  ..._OPTIONAL_TOP_LEVEL_FIELDS,
};

const _ALLOWED_AUTH_FIELDS: Record<string, string | string[]> = {
  backends: "array",
  allow_effective_user: "boolean",
};

const _ALLOWED_SESSION_CREATION_FIELDS: Record<string, string | string[]> = {
  max_concurrent_sessions: "number",
  defaults: "object",
};

const _ALLOWED_SESSION_CREATION_DEFAULTS: Record<string, string | string[]> = {
  heap_size_gb: "number",
  auto_delete_timeout: "number",
  server: "string",
  engine: "string",
  extra_jvm_args: "array",
  extra_environment_vars: "array",
  admin_groups: "array",
  viewer_groups: "array",
  timeout_seconds: "number",
  session_arguments: "object",
  programming_language: "string",
};

// ---------------------------------------------------------------------------
// Redaction
// ---------------------------------------------------------------------------

/**
 * Return a shallow copy of `systemConfig` safe for logging.
 *
 * The current enterprise config schema carries no secret material —
 * credentials are delivered per-request via HTTP headers, never via
 * the config file — so this function returns a plain shallow copy.
 *
 * @param systemConfig - The enterprise system configuration dictionary.
 * @returns A shallow copy of `systemConfig`.
 */
export function redactEnterpriseConfig(
  systemConfig: Record<string, unknown>,
): Record<string, unknown> {
  return { ...systemConfig };
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

function _validateTopLevelFields(
  systemName: string,
  config: Record<string, unknown>,
): void {
  for (const [fieldName, expectedType] of Object.entries(_REQUIRED_TOP_LEVEL_FIELDS)) {
    if (!(fieldName in config)) {
      const msg = `Required field '${fieldName}' missing in enterprise system '${systemName}'.`;
      _logger.error(`[config:_validateTopLevelFields] ${msg}`);
      throw new ConfigurationError(msg);
    }
    validateFieldType(
      `enterprise system '${systemName}'`,
      fieldName,
      config[fieldName],
      expectedType,
    );
  }

  validateAllowedFields(
    `enterprise system '${systemName}'`,
    config,
    _ALLOWED_TOP_LEVEL_FIELDS,
  );
}

function _validateAuthBlock(
  systemName: string,
  auth: Record<string, unknown>,
): void {
  const context = `auth for enterprise system '${systemName}'`;
  validateAllowedFields(context, auth, _ALLOWED_AUTH_FIELDS);

  if (!("backends" in auth)) {
    const msg =
      `Required field 'backends' missing in ${context}. Provide a ` +
      `non-empty list whose elements are drawn from ` +
      `${[...SUPPORTED_AUTH_BACKENDS].sort()}.`;
    _logger.error(`[config:_validateAuthBlock] ${msg}`);
    throw new ConfigurationError(msg);
  }

  const backends = auth["backends"];
  validateFieldType(context, "backends", backends, "array");
  const backendsArr = backends as unknown[];

  if (backendsArr.length === 0) {
    const msg = `'backends' for ${context} must be a non-empty list.`;
    _logger.error(`[config:_validateAuthBlock] ${msg}`);
    throw new ConfigurationError(msg);
  }

  const uniqueBackends = new Set(backendsArr);
  if (uniqueBackends.size !== backendsArr.length) {
    const msg = `'backends' for ${context} must not contain duplicate entries.`;
    _logger.error(`[config:_validateAuthBlock] ${msg}`);
    throw new ConfigurationError(msg);
  }

  for (const entry of backendsArr) {
    if (typeof entry !== "string") {
      const msg =
        `'backends' for ${context} must contain only strings; ` +
        `got element of type ${typeof entry}.`;
      _logger.error(`[config:_validateAuthBlock] ${msg}`);
      throw new ConfigurationError(msg);
    }
    if (!SUPPORTED_AUTH_BACKENDS.has(entry)) {
      const msg =
        `'backends' for ${context} contains unsupported entry ` +
        `'${entry}'; allowed values are ` +
        `${[...SUPPORTED_AUTH_BACKENDS].sort()}.`;
      _logger.error(`[config:_validateAuthBlock] ${msg}`);
      throw new ConfigurationError(msg);
    }
  }

  const allowEffectiveUser = "allow_effective_user" in auth
    ? auth["allow_effective_user"]
    : false;
  if ("allow_effective_user" in auth) {
    validateFieldType(context, "allow_effective_user", allowEffectiveUser, "boolean");
  }
  if (allowEffectiveUser && !backendsArr.includes("password")) {
    const msg =
      `'allow_effective_user' for ${context} can only be true when ` +
      `'password' is included in 'backends'.`;
    _logger.error(`[config:_validateAuthBlock] ${msg}`);
    throw new ConfigurationError(msg);
  }
}

function _validateSessionCreation(
  systemName: string,
  config: Record<string, unknown>,
): void {
  const sessionCreation = config["session_creation"];
  if (sessionCreation === undefined || sessionCreation === null) {
    return;
  }

  const context = `session_creation for enterprise system '${systemName}'`;
  validateAllowedFields(
    context,
    sessionCreation as Record<string, unknown>,
    _ALLOWED_SESSION_CREATION_FIELDS,
  );

  const sc = sessionCreation as Record<string, unknown>;
  if ("max_concurrent_sessions" in sc) {
    validateNonNegativeInt("max_concurrent_sessions", sc["max_concurrent_sessions"]);
  }

  const defaults = sc["defaults"];
  if (defaults === undefined || defaults === null) {
    const msg =
      `'session_creation.defaults' is required for enterprise system ` +
      `'${systemName}' but is missing.`;
    _logger.error(`[config:_validateSessionCreation] ${msg}`);
    throw new ConfigurationError(msg);
  }

  const defaultsContext = `session_creation.defaults for enterprise system '${systemName}'`;
  validateAllowedFields(
    defaultsContext,
    defaults as Record<string, unknown>,
    _ALLOWED_SESSION_CREATION_DEFAULTS,
  );

  if (!("heap_size_gb" in (defaults as Record<string, unknown>))) {
    const msg =
      `'session_creation.defaults.heap_size_gb' is required for ` +
      `enterprise system '${systemName}' but is missing.`;
    _logger.error(`[config:_validateSessionCreation] ${msg}`);
    throw new ConfigurationError(msg);
  }
}

// ---------------------------------------------------------------------------
// Top-level enterprise config validation
// ---------------------------------------------------------------------------

/**
 * Validate a flat enterprise server configuration.
 *
 * Required fields:
 * - `system_name` (string)
 * - `connection_json_url` (string)
 * - `auth` (object): see auth block validation.
 *
 * Optional fields:
 * - `connection_timeout` (number > 0)
 * - `mcp_session_idle_timeout_seconds` (number > 0)
 * - `session_creation` (object): when present, `defaults.heap_size_gb` is required.
 *
 * @param config - The configuration to validate; must be an object.
 * @returns The same `config` object, unchanged, after successful validation.
 * @throws {ConfigurationError} For any validation failure.
 */
export function validateEnterpriseConfig(
  config: unknown,
): Record<string, unknown> {
  _logger.debug("[config:validateEnterpriseConfig] Validating enterprise server config");

  if (typeof config !== "object" || config === null || Array.isArray(config)) {
    const typeName = config === null ? "null" : Array.isArray(config) ? "array" : typeof config;
    const msg = `Enterprise system configuration must be a dictionary, but got ${typeName}.`;
    _logger.error(`[config:validateEnterpriseConfig] ${msg}`);
    throw new ConfigurationError(msg);
  }

  const cfg = config as Record<string, unknown>;
  const systemNameRaw = cfg["system_name"];
  const systemName =
    typeof systemNameRaw === "string" ? systemNameRaw : "<unset>";

  _validateTopLevelFields(systemName, cfg);
  _validateAuthBlock(systemName, cfg["auth"] as Record<string, unknown>);

  validateOptionalPositiveNumber(cfg, "connection_timeout");
  validateOptionalPositiveNumber(cfg, "mcp_session_idle_timeout_seconds");

  _validateSessionCreation(systemName, cfg);

  _logger.debug(
    `[config:validateEnterpriseConfig] Enterprise system '${systemName}' validation passed`,
  );
  return cfg;
}

async function _loadAndValidateEnterpriseConfig(
  configPath: string,
): Promise<Record<string, unknown>> {
  return _loadAndValidateConfig(
    configPath,
    validateEnterpriseConfig as (data: Record<string, unknown>) => Record<string, unknown>,
    "_loadAndValidateEnterpriseConfig",
  );
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

/**
 * ConfigManager for the DHE MCP server (`dh-mcp-enterprise-server`).
 *
 * Reads a *flat* enterprise config file where the system fields sit at
 * the top level (no system-name nesting). Validates the config as a
 * single enterprise system and returns it directly.
 */
export class EnterpriseServerConfigManager extends ConfigManager {
  private _loadingPromise: Promise<Record<string, unknown>> | undefined;

  /**
   * Load and validate the flat enterprise config file (async-safe with caching).
   *
   * @returns The flat enterprise system config object (fields at top level).
   * @throws {Error} If no config path is provided and `DH_MCP_CONFIG_FILE` is unset.
   * @throws {ConfigurationError} If the file cannot be read or fails validation.
   */
  async getConfig(): Promise<Record<string, unknown>> {
    _logger.debug(
      "[EnterpriseServerConfigManager:getConfig] Loading enterprise " +
      "server configuration...",
    );

    if (this._cache !== undefined) {
      _logger.debug(
        "[EnterpriseServerConfigManager:getConfig] Using cached configuration.",
      );
      return this._cache;
    }

    if (this._loadingPromise !== undefined) {
      return this._loadingPromise;
    }

    this._loadingPromise = (async () => {
      try {
        const resolvedPath =
          this._configPath !== undefined
            ? this._configPath
            : _getConfigPath();
        const flatConfig = await _loadAndValidateEnterpriseConfig(resolvedPath);
        this._cache = flatConfig;
        _logConfigSummary(
          flatConfig,
          "EnterpriseServerConfigManager:getConfig",
          redactEnterpriseConfig,
        );
        _logger.info(
          "[EnterpriseServerConfigManager:getConfig] Enterprise " +
          "configuration loaded successfully.",
        );
        return this._cache;
      } finally {
        this._loadingPromise = undefined;
      }
    })();

    return this._loadingPromise;
  }

  /**
   * PRIVATE: Inject a configuration dictionary into the cache (for testing).
   *
   * @param config - A raw configuration dictionary to validate and cache.
   * @throws {ConfigurationError} If `config` fails enterprise schema validation.
   */
  async _setConfigCache(config: Record<string, unknown>): Promise<void> {
    this._cache = validateEnterpriseConfig(config);
  }
}

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

/**
 * Return the configured `auth.backends` list.
 *
 * @param config - A validated enterprise configuration dictionary.
 * @returns The configured backends, in declaration order.
 */
export function getEnterpriseAuthBackends(config: Record<string, unknown>): string[] {
  return [...((config["auth"] as Record<string, unknown>)["backends"] as string[])];
}

/**
 * Return the configured `auth.allow_effective_user` flag.
 *
 * @param config - A validated enterprise configuration dictionary.
 * @returns `true` if `auth.allow_effective_user` is set to `true`; `false` otherwise.
 */
export function getEnterpriseAllowEffectiveUser(
  config: Record<string, unknown>,
): boolean {
  return Boolean(
    ((config["auth"] as Record<string, unknown>)["allow_effective_user"] ?? false),
  );
}
