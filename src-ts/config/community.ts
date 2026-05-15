/**
 * Configuration handling for the Deephaven MCP community server.
 *
 * Validates and redacts community config files and provides the
 * {@link CommunityServerConfigManager} used by `dh-mcp-community-server`.
 *
 * Community config file format (flat — all keys at top level):
 * ```json
 * {
 *   "auth": {
 *     "enabled": true,
 *     "psk_env_var": "DH_MCP_COMMUNITY_PSK"
 *   },
 *   "security": {"credential_retrieval_mode": "dynamic_only"},
 *   "sessions": {
 *     "local": {"host": "localhost", "port": 10000, "auth_type": "PSK", "auth_token": "..."}
 *   },
 *   "session_creation": {"defaults": {"launch_method": "python"}},
 *   "mcp_session_idle_timeout_seconds": 3600
 * }
 * ```
 *
 * Valid top-level keys: `auth`, `security`, `sessions`,
 * `session_creation`, `mcp_session_idle_timeout_seconds`. Unknown keys
 * at any level are rejected.
 */

import pino from "pino";
import { ConfigurationError } from "../exceptions.js";
import { REDACTED } from "../redaction.js";
import {
  ConfigManager,
  _getConfigPath,
  _loadAndValidateConfig,
  _logConfigSummary,
} from "./base.js";
import {
  validateAllowedFields,
  validateMutuallyExclusive,
  validateNonNegativeInt,
  validateOptionalPositiveNumber,
  validateOptionalStringDict,
  validateOptionalStringList,
  validatePositiveNumber,
} from "./validators.js";

const _logger = pino({ name: "deephaven-mcp:config/community" });

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const _KNOWN_AUTH_TYPES: ReadonlySet<string> = new Set([
  "PSK",
  "Anonymous",
  "Basic",
  "io.deephaven.authentication.psk.PskAuthenticationHandler",
]);

const _ALLOWED_COMMUNITY_SESSION_FIELDS: Record<string, string | string[]> = {
  host: "string",
  port: "number",
  auth_type: "string",
  auth_token: "string",
  auth_token_env_var: "string",
  never_timeout: "boolean",
  session_type: "string",
  use_tls: "boolean",
  tls_root_certs: ["string", "null"],
  client_cert_chain: ["string", "null"],
  client_private_key: ["string", "null"],
};

const _VALID_CREDENTIAL_RETRIEVAL_MODES: ReadonlySet<string> = new Set([
  "none",
  "dynamic_only",
  "static_only",
  "all",
]);

const _ALLOWED_SECURITY_FIELDS: Record<string, string | string[]> = {
  credential_retrieval_mode: "string",
};

const _ALLOWED_LAUNCH_METHODS: ReadonlySet<string> = new Set(["docker", "python"]);

const _ALLOWED_SESSION_CREATION_FIELDS: Record<string, string | string[]> = {
  max_concurrent_sessions: "number",
  defaults: "object",
};

const _ALLOWED_SESSION_CREATION_DEFAULTS: Record<string, string | string[]> = {
  launch_method: "string",
  auth_type: "string",
  auth_token: ["string", "null"],
  auth_token_env_var: ["string", "null"],
  programming_language: "string",
  docker_image: "string",
  docker_memory_limit_gb: ["number", "null"],
  docker_cpu_limit: ["number", "null"],
  docker_volumes: "array",
  python_venv_path: ["string", "null"],
  heap_size_gb: "number",
  extra_jvm_args: "array",
  environment_vars: "object",
  startup_timeout_seconds: "number",
  startup_check_interval_seconds: "number",
  startup_retries: "number",
};

const _ALLOWED_TOP_LEVEL_FIELDS: Record<string, string | string[]> = {
  auth: "object",
  security: "object",
  sessions: "object",
  session_creation: "object",
  mcp_session_idle_timeout_seconds: "number",
};

const _ALLOWED_AUTH_FIELDS: Record<string, string | string[]> = {
  enabled: "boolean",
  psk: "string",
  psk_env_var: "string",
};

// ---------------------------------------------------------------------------
// Redaction
// ---------------------------------------------------------------------------

/**
 * Return a copy of `sessionConfig` with sensitive fields redacted.
 *
 * - `auth_token` is redacted when truthy.
 * - `tls_root_certs`, `client_cert_chain`, `client_private_key` are
 *   redacted only when the value is a `Buffer`/`Uint8Array` and
 *   `redactBinaryValues` is `true`. String values (e.g., filesystem paths)
 *   are preserved as-is.
 *
 * @param sessionConfig - The per-session configuration dictionary.
 * @param redactBinaryValues - If `false`, skip binary TLS field redaction. Defaults to `true`.
 * @returns A new dictionary with sensitive fields replaced with `"[REDACTED]"`.
 */
export function redactCommunitySessionConfig(
  sessionConfig: Record<string, unknown>,
  redactBinaryValues = true,
): Record<string, unknown> {
  const out = { ...sessionConfig };
  if (out["auth_token"]) {
    out["auth_token"] = REDACTED;
  }
  if (redactBinaryValues) {
    for (const key of ["tls_root_certs", "client_cert_chain", "client_private_key"]) {
      const value = out[key];
      if (value && (value instanceof Buffer || value instanceof Uint8Array)) {
        out[key] = REDACTED;
      }
    }
  }
  return out;
}

function _redactSessionCreationConfig(
  sessionCreationConfig: Record<string, unknown>,
): Record<string, unknown> {
  const out = structuredClone(sessionCreationConfig) as Record<string, unknown>;
  const defaults = out["defaults"];
  if (typeof defaults === "object" && defaults !== null && "auth_token" in (defaults as Record<string, unknown>)) {
    (defaults as Record<string, unknown>)["auth_token"] = REDACTED;
  }
  return out;
}

/**
 * Return a deep copy of `config` with every sensitive field redacted.
 *
 * @param config - The full community configuration dictionary.
 * @returns A deep copy of `config` safe to include in log output.
 */
export function redactCommunityConfig(
  config: Record<string, unknown>,
): Record<string, unknown> {
  const out = structuredClone(config) as Record<string, unknown>;
  const auth = out["auth"];
  if (typeof auth === "object" && auth !== null && "psk" in (auth as Record<string, unknown>)) {
    (auth as Record<string, unknown>)["psk"] = REDACTED;
  }
  const sessions = out["sessions"];
  if (typeof sessions === "object" && sessions !== null) {
    const redacted: Record<string, unknown> = {};
    for (const [name, cfg] of Object.entries(sessions as Record<string, unknown>)) {
      redacted[name] = typeof cfg === "object" && cfg !== null
        ? redactCommunitySessionConfig(cfg as Record<string, unknown>)
        : cfg;
    }
    out["sessions"] = redacted;
  }
  const sessionCreation = out["session_creation"];
  if (typeof sessionCreation === "object" && sessionCreation !== null) {
    out["session_creation"] = _redactSessionCreationConfig(
      sessionCreation as Record<string, unknown>,
    );
  }
  return out;
}

// ---------------------------------------------------------------------------
// Validation — auth
// ---------------------------------------------------------------------------

function _validateAuthConfig(authConfig: Record<string, unknown>): void {
  const context = "'auth' section";
  validateAllowedFields(context, authConfig, _ALLOWED_AUTH_FIELDS);
  validateMutuallyExclusive(context, authConfig, "psk", "psk_env_var");

  const enabled = "enabled" in authConfig ? authConfig["enabled"] : true;
  const hasUsableSecret =
    Boolean(authConfig["psk"]) || Boolean(authConfig["psk_env_var"]);
  const hasSecretField = "psk" in authConfig || "psk_env_var" in authConfig;

  if (enabled && !hasUsableSecret) {
    const msg =
      `${context} has 'enabled: true' but neither 'psk' nor ` +
      "'psk_env_var' was provided. When authentication is enabled, " +
      "the config must specify exactly one:\n" +
      "\n" +
      '    "psk": "<your-secret-here>"           ' +
      "(secret stored directly in config)\n" +
      "\n" +
      '    "psk_env_var": "<ENV_VAR_NAME>"       ' +
      "(config names an env var holding the secret)\n" +
      "\n" +
      "To run without authentication (local development only — " +
      "server will refuse to start unless bound to 127.0.0.1 / " +
      "localhost), set 'enabled: false' instead.";
    _logger.error(`[config:_validateAuthConfig] ${msg}`);
    throw new ConfigurationError(msg);
  }

  if (!enabled && hasSecretField) {
    const msg =
      `${context} has 'enabled: false' but also specifies a PSK ` +
      "('psk' or 'psk_env_var'). When authentication is disabled " +
      "the server runs without a secret. Either:\n" +
      "  - remove the 'psk' / 'psk_env_var' field, or\n" +
      "  - set 'enabled: true' to require the PSK on every request.";
    _logger.error(`[config:_validateAuthConfig] ${msg}`);
    throw new ConfigurationError(msg);
  }
}

// ---------------------------------------------------------------------------
// Validation — security
// ---------------------------------------------------------------------------

function _validateSecurityConfig(securityConfig: Record<string, unknown>): void {
  validateAllowedFields("'security' section", securityConfig, _ALLOWED_SECURITY_FIELDS);

  const mode = securityConfig["credential_retrieval_mode"];
  if (mode !== undefined && !_VALID_CREDENTIAL_RETRIEVAL_MODES.has(mode as string)) {
    const valid = [..._VALID_CREDENTIAL_RETRIEVAL_MODES].sort().join(", ");
    const msg = `'security.credential_retrieval_mode' must be one of [${valid}], got '${mode}'.`;
    _logger.error(`[config:_validateSecurityConfig] ${msg}`);
    throw new ConfigurationError(msg);
  }
}

// ---------------------------------------------------------------------------
// Validation — sessions
// ---------------------------------------------------------------------------

/**
 * Validate a single community session's configuration dictionary.
 *
 * @param sessionName - The session name (used in error messages).
 * @param configItem - The session configuration to validate.
 * @throws {ConfigurationError} If the configuration is invalid.
 */
export function validateCommunitySessionConfig(
  sessionName: string,
  configItem: unknown,
): void {
  const context = `session '${sessionName}'`;
  if (typeof configItem !== "object" || configItem === null || Array.isArray(configItem)) {
    const typeName = configItem === null ? "null" : Array.isArray(configItem) ? "array" : typeof configItem;
    const msg = `${context} must be a dictionary, got ${typeName}.`;
    _logger.error(`[config:validateCommunitySessionConfig] ${msg}`);
    throw new ConfigurationError(msg);
  }
  const config = configItem as Record<string, unknown>;
  validateAllowedFields(context, config, _ALLOWED_COMMUNITY_SESSION_FIELDS);
  validateMutuallyExclusive(context, config, "auth_token", "auth_token_env_var");

  const authType = config["auth_type"];
  if (authType !== undefined && !_KNOWN_AUTH_TYPES.has(authType as string)) {
    _logger.warn(
      `[config:validateCommunitySessionConfig] ${context} ` +
      `uses auth_type='${authType}' which is not a commonly known ` +
      `value. Known values: ${[..._KNOWN_AUTH_TYPES].sort()}. Custom ` +
      `authenticators are also valid.`,
    );
  }
}

function _validateSessionsConfig(sessionsMap: unknown): void {
  if (typeof sessionsMap !== "object" || sessionsMap === null || Array.isArray(sessionsMap)) {
    const typeName = sessionsMap === null ? "null" : Array.isArray(sessionsMap) ? "array" : typeof sessionsMap;
    const msg = `'sessions' must be a dictionary, got ${typeName}.`;
    _logger.error(`[config:_validateSessionsConfig] ${msg}`);
    throw new ConfigurationError(msg);
  }
  for (const [sessionName, sessionCfg] of Object.entries(sessionsMap as Record<string, unknown>)) {
    validateCommunitySessionConfig(sessionName, sessionCfg);
  }
}

// ---------------------------------------------------------------------------
// Validation — session_creation
// ---------------------------------------------------------------------------

function _validateSessionCreationDefaults(defaults: Record<string, unknown>): void {
  const context = "'session_creation.defaults'";
  validateAllowedFields(context, defaults, _ALLOWED_SESSION_CREATION_DEFAULTS);
  validateMutuallyExclusive(context, defaults, "auth_token", "auth_token_env_var");

  const launchMethod = defaults["launch_method"];
  if (launchMethod !== undefined && !_ALLOWED_LAUNCH_METHODS.has(launchMethod as string)) {
    const msg = `'session_creation.defaults.launch_method' must be one of ${[..._ALLOWED_LAUNCH_METHODS].sort()}, got '${launchMethod}'.`;
    _logger.error(`[config:_validateSessionCreationDefaults] ${msg}`);
    throw new ConfigurationError(msg);
  }

  const authType = defaults["auth_type"];
  if (authType !== undefined && !_KNOWN_AUTH_TYPES.has(authType as string)) {
    _logger.warn(
      `[config:_validateSessionCreationDefaults] ` +
      `session_creation.defaults uses auth_type='${authType}' which ` +
      `is not a commonly known value. Known values: ` +
      `${[..._KNOWN_AUTH_TYPES].sort()}. Custom authenticators are also valid.`,
    );
  }

  validateOptionalPositiveNumber(defaults, "heap_size_gb");
  validateOptionalPositiveNumber(defaults, "docker_memory_limit_gb");
  validateOptionalPositiveNumber(defaults, "docker_cpu_limit");
  validateOptionalPositiveNumber(defaults, "startup_timeout_seconds");
  validateOptionalPositiveNumber(defaults, "startup_check_interval_seconds");
  validateOptionalStringList(defaults, "docker_volumes");
  validateOptionalStringList(defaults, "extra_jvm_args");
  validateOptionalStringDict(defaults, "environment_vars");
  if ("startup_retries" in defaults) {
    validateNonNegativeInt("startup_retries", defaults["startup_retries"]);
  }
}

function _validateSessionCreationConfig(sessionCreationConfig: unknown): void {
  if (typeof sessionCreationConfig !== "object" || sessionCreationConfig === null || Array.isArray(sessionCreationConfig)) {
    const typeName = sessionCreationConfig === null ? "null" : Array.isArray(sessionCreationConfig) ? "array" : typeof sessionCreationConfig;
    const msg = `'session_creation' must be a dictionary, got ${typeName}.`;
    _logger.error(`[config:_validateSessionCreationConfig] ${msg}`);
    throw new ConfigurationError(msg);
  }
  const config = sessionCreationConfig as Record<string, unknown>;
  validateAllowedFields("'session_creation' section", config, _ALLOWED_SESSION_CREATION_FIELDS);

  if ("max_concurrent_sessions" in config) {
    validateNonNegativeInt("max_concurrent_sessions", config["max_concurrent_sessions"]);
  }

  if ("defaults" in config) {
    _validateSessionCreationDefaults(config["defaults"] as Record<string, unknown>);
  }
}

// ---------------------------------------------------------------------------
// Top-level community config validation
// ---------------------------------------------------------------------------

/**
 * Validate a community configuration dictionary.
 *
 * @param config - The parsed configuration; must be an object for
 *   validation to succeed.
 * @returns The same `config` object, unchanged, after successful validation.
 * @throws {ConfigurationError} If `config` is not an object, an unknown top-level
 *   key is present, or any section fails its schema.
 */
export function validateCommunityConfig(
  config: unknown,
): Record<string, unknown> {
  if (typeof config !== "object" || config === null || Array.isArray(config)) {
    const typeName = config === null ? "null" : Array.isArray(config) ? "array" : typeof config;
    const msg = `Community configuration must be a dictionary, got ${typeName}.`;
    _logger.error(`[config:validateCommunityConfig] ${msg}`);
    throw new ConfigurationError(msg);
  }
  const cfg = config as Record<string, unknown>;

  validateAllowedFields("community configuration", cfg, _ALLOWED_TOP_LEVEL_FIELDS);

  if (!("auth" in cfg)) {
    const msg =
      "Required field 'auth' missing in community configuration. " +
      "Authentication is enabled by default and must be configured " +
      "explicitly. Provide one of:\n" +
      "\n" +
      '    "auth": { "psk_env_var": "DH_MCP_COMMUNITY_PSK" }   ' +
      "(env-var indirection, recommended)\n" +
      "\n" +
      '    "auth": { "psk": "<your-secret-here>" }             ' +
      "(secret stored directly in config)\n" +
      "\n" +
      '    "auth": { "enabled": false }                        ' +
      "(no auth — only valid on loopback binds)";
    _logger.error(`[config:validateCommunityConfig] ${msg}`);
    throw new ConfigurationError(msg);
  }
  _validateAuthConfig(cfg["auth"] as Record<string, unknown>);

  if ("security" in cfg) {
    _validateSecurityConfig(cfg["security"] as Record<string, unknown>);
  }
  if ("sessions" in cfg) {
    _validateSessionsConfig(cfg["sessions"]);
  }
  if ("session_creation" in cfg) {
    _validateSessionCreationConfig(cfg["session_creation"]);
  }
  if ("mcp_session_idle_timeout_seconds" in cfg) {
    validatePositiveNumber("mcp_session_idle_timeout_seconds", cfg["mcp_session_idle_timeout_seconds"]);
  }

  _logger.info("[config:validateCommunityConfig] Configuration validation passed.");
  return cfg;
}

async function _loadAndValidateCommunityConfig(
  configPath: string,
): Promise<Record<string, unknown>> {
  return _loadAndValidateConfig(
    configPath,
    validateCommunityConfig as (data: Record<string, unknown>) => Record<string, unknown>,
    "_loadAndValidateCommunityConfig",
  );
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

/**
 * ConfigManager for the DHC MCP server (`dh-mcp-community-server`).
 *
 * Reads a community config file. The format uses `sessions`,
 * `session_creation`, `security`, and `mcp_session_idle_timeout_seconds`
 * as optional top-level keys; validation enforces the community schema.
 */
export class CommunityServerConfigManager extends ConfigManager {
  private _loadingPromise: Promise<Record<string, unknown>> | undefined;

  /**
   * Load and validate the community config file (async-safe with caching).
   *
   * @returns The validated community configuration dictionary.
   * @throws {Error} If no config path is provided and `DH_MCP_CONFIG_FILE` is unset.
   * @throws {ConfigurationError} If the file cannot be read or fails validation.
   */
  async getConfig(): Promise<Record<string, unknown>> {
    _logger.debug(
      "[CommunityServerConfigManager:getConfig] Loading Deephaven MCP " +
      "application configuration...",
    );

    if (this._cache !== undefined) {
      _logger.debug(
        "[CommunityServerConfigManager:getConfig] Using cached configuration.",
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
        const validated = await _loadAndValidateCommunityConfig(resolvedPath);
        this._cache = validated;
        _logConfigSummary(
          validated,
          "CommunityServerConfigManager:getConfig",
          redactCommunityConfig,
        );
        _logger.info(
          "[CommunityServerConfigManager:getConfig] Community " +
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
   * @throws {ConfigurationError} If `config` fails community schema validation.
   */
  async _setConfigCache(config: Record<string, unknown>): Promise<void> {
    this._cache = validateCommunityConfig(config);
  }
}
