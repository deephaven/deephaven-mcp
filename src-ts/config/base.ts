/**
 * Shared base infrastructure for Deephaven MCP configuration management.
 *
 * This module provides the abstract {@link ConfigManager} base class and shared
 * file-loading utilities used by both the community and enterprise server config managers.
 *
 * Concrete subclasses:
 * - {@link CommunityServerConfigManager} in `community.ts`
 * - {@link EnterpriseServerConfigManager} in `enterprise.ts`
 *
 * Private module-level helpers:
 * - {@link _getConfigPath}: resolve path from `DH_MCP_CONFIG_FILE`.
 * - {@link _loadAndValidateConfig}: combined load + validate with error wrapping.
 * - {@link _logConfigSummary}: pretty-prints the (optionally redacted) config to the log.
 * - {@link _loadConfigFromFile}: async JSON/JSON5 read with error wrapping.
 */

import { readFile } from "node:fs/promises";
import pino from "pino";
import JSON5 from "json5";
import { envRequired } from "../env.js";
import { ConfigurationError } from "../exceptions.js";

const _logger = pino({ name: "deephaven-mcp:config/base" });

export const CONFIG_ENV_VAR = "DH_MCP_CONFIG_FILE";
/**
 * Name of the environment variable specifying the path to the Deephaven MCP config file.
 */

export const DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS: number = 3600.0;
/**
 * Default MCP session idle timeout in seconds (1 hour).
 *
 * After this many seconds of inactivity from an MCP client, its per-session
 * Deephaven registry is closed by the TTL sweeper. Overridable per-server via the
 * `mcp_session_idle_timeout_seconds` config file key.
 */

// ---------------------------------------------------------------------------
// ConfigManager
// ---------------------------------------------------------------------------

/**
 * Abstract base class for Deephaven MCP configuration managers.
 *
 * Provides the common interface and shared infrastructure for async, cached
 * configuration loading. Concrete subclasses implement config-format-specific
 * loading and validation logic.
 *
 * Common features:
 * - **Async-safe**: Uses a mutex-like pattern to prevent concurrent loads.
 * - **Caching**: Loads configuration once; subsequent calls return the cached value.
 * - **Cache control**: {@link clearConfigCache} forces reload on next access.
 */
export abstract class ConfigManager {
  protected _configPath: string | undefined;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- config is an arbitrary object
  protected _cache: Record<string, any> | undefined = undefined;
  private _loadPromise: Promise<Record<string, unknown>> | undefined = undefined;

  /**
   * @param configPath - Optional explicit path to the configuration file. If provided,
   *   this takes precedence over the `DH_MCP_CONFIG_FILE` environment variable.
   *   If `undefined` (default), the environment variable is used.
   */
  constructor(configPath?: string) {
    this._configPath = configPath;
  }

  /**
   * Clear the cached Deephaven configuration.
   *
   * Forces the next configuration access to reload from disk.
   */
  async clearConfigCache(): Promise<void> {
    _logger.debug("[ConfigManager:clearConfigCache] Clearing Deephaven configuration cache...");
    this._cache = undefined;
    this._loadPromise = undefined;
    _logger.debug("[ConfigManager:clearConfigCache] Configuration cache cleared.");
  }

  /**
   * Load and return the validated configuration.
   *
   * Subclasses must implement format-specific loading and validation.
   *
   * @returns The validated configuration dictionary.
   * @throws {Error} If no config path was provided and `DH_MCP_CONFIG_FILE` is unset.
   * @throws {ConfigurationError} If the file cannot be read, parsed, or fails schema validation.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- config is an arbitrary object
  abstract getConfig(): Promise<Record<string, any>>;

  /**
   * Return the MCP session idle timeout in seconds.
   *
   * Reads the optional `mcp_session_idle_timeout_seconds` key from the loaded
   * configuration and returns it as a number. If the key is absent,
   * {@link DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS} is returned.
   *
   * @returns Idle timeout in seconds. Always positive.
   * @throws {Error} Propagated from {@link getConfig} when the config path is unresolvable.
   * @throws {ConfigurationError} Propagated from {@link getConfig}.
   */
  async getMcpSessionIdleTimeoutSeconds(): Promise<number> {
    const config = await this.getConfig();
    const value = config["mcp_session_idle_timeout_seconds"];
    return typeof value === "number" ? value : DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS;
  }

  /**
   * PRIVATE: Inject a configuration dictionary into the cache (for testing).
   *
   * @param config - A raw configuration dictionary to validate and cache.
   * @throws {ConfigurationError} If `config` fails the subclass's schema validation.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- config is an arbitrary object
  abstract _setConfigCache(config: Record<string, any>): Promise<void>;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Load and parse a Deephaven MCP configuration file (JSON or JSON5) using async I/O.
 *
 * @param configPath - The absolute or relative path to the configuration JSON/JSON5 file.
 * @returns The parsed configuration cast as a dictionary.
 * @throws {ConfigurationError} Wraps any of the following underlying failures:
 *   - File not found
 *   - Permission denied
 *   - Invalid JSON/JSON5 syntax
 *   - Any other unexpected error during file read or parsing
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- config is an arbitrary object
export async function _loadConfigFromFile(configPath: string): Promise<Record<string, any>> {
  try {
    const content = await readFile(configPath, "utf-8");
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- config is an arbitrary object
    return JSON5.parse(content) as Record<string, any>;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") {
      _logger.error(`[_loadConfigFromFile] Configuration file not found: ${configPath}`);
      throw new ConfigurationError(`Configuration file not found: ${configPath}`);
    }
    if ((err as NodeJS.ErrnoException).code === "EACCES" || (err as NodeJS.ErrnoException).code === "EPERM") {
      _logger.error(`[_loadConfigFromFile] Permission denied when trying to read configuration file: ${configPath}`);
      throw new ConfigurationError(`Permission denied when trying to read configuration file: ${configPath}`);
    }
    if (err instanceof SyntaxError) {
      _logger.error(`[_loadConfigFromFile] Invalid JSON/JSON5 in configuration file ${configPath}: ${err.message}`);
      throw new ConfigurationError(`Invalid JSON/JSON5 in configuration file ${configPath}: ${err.message}`);
    }
    if (err instanceof ConfigurationError) {
      throw err;
    }
    const msg = err instanceof Error ? err.message : String(err);
    _logger.error(`[_loadConfigFromFile] Unexpected error reading configuration file ${configPath}: ${msg}`);
    throw new ConfigurationError(`Unexpected error loading or parsing config file ${configPath}: ${msg}`);
  }
}

/**
 * Retrieve the configuration file path from the `DH_MCP_CONFIG_FILE` environment variable.
 *
 * @returns The raw value of `DH_MCP_CONFIG_FILE`.
 * @throws {Error} If the `DH_MCP_CONFIG_FILE` environment variable is not set.
 */
export function _getConfigPath(): string {
  let configPath: string;
  try {
    configPath = envRequired(CONFIG_ENV_VAR);
  } catch (err) {
    _logger.error(`[_getConfigPath] Environment variable ${CONFIG_ENV_VAR} is not set.`);
    throw err;
  }
  _logger.info(`[_getConfigPath] Environment variable ${CONFIG_ENV_VAR} is set to: ${configPath}`);
  return configPath;
}

/**
 * Load a config file and run a validator; wrap any error as {@link ConfigurationError}.
 *
 * @param configPath - Path to the JSON/JSON5 config file.
 * @param validator - Function that validates and returns the parsed dict.
 * @param caller - Caller label used in error log messages.
 * @returns The fully validated configuration dictionary.
 * @throws {ConfigurationError} For any failure during loading or validation.
 */
export async function _loadAndValidateConfig(
  configPath: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- config is an arbitrary object
  validator: (data: Record<string, any>) => Record<string, any>,
  caller: string,
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- config is an arbitrary object
): Promise<Record<string, any>> {
  try {
    const data = await _loadConfigFromFile(configPath);
    return validator(data);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    _logger.error(`[${caller}] Error loading configuration file ${configPath}: ${msg}`);
    throw new ConfigurationError(`Error loading configuration file: ${msg}`);
  }
}

/**
 * Log a summary of the loaded Deephaven MCP configuration.
 *
 * @param config - The loaded and validated configuration dictionary.
 * @param label - Log prefix label identifying the caller.
 * @param redactor - Optional function to redact sensitive fields before logging.
 */
export function _logConfigSummary(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- config is an arbitrary object
  config: Record<string, any>,
  label: string = "ConfigManager:getConfig",
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- config is an arbitrary object
  redactor?: (config: Record<string, any>) => Record<string, any>,
): void {
  _logger.info(`[${label}] Configuration summary:`);
  const redactedConfig = redactor ? redactor(config) : config;
  try {
    const formatted = JSON.stringify(redactedConfig, Object.keys(redactedConfig).sort(), 2);
    _logger.info(`[${label}] Loaded configuration:\n${formatted}`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    _logger.warn(`[${label}] Failed to format config as JSON: ${msg}`);
    _logger.info(`[${label}] Loaded configuration: ${JSON.stringify(redactedConfig)}`);
  }
}
