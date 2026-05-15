/**
 * Async Deephaven MCP configuration management.
 *
 * Public surface for loading, validating, and managing Deephaven MCP
 * configuration from a JSON or JSON5 file.
 */

export {
  ConfigManager,
  CONFIG_ENV_VAR,
  DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS,
} from "./base.js";

export {
  validateCommunityConfig,
  validateCommunitySessionConfig,
  redactCommunityConfig,
  redactCommunitySessionConfig,
  CommunityServerConfigManager,
} from "./community.js";

export {
  DEFAULT_CONNECTION_TIMEOUT_SECONDS,
  SUPPORTED_AUTH_BACKENDS,
  validateEnterpriseConfig,
  redactEnterpriseConfig,
  EnterpriseServerConfigManager,
  getEnterpriseAuthBackends,
  getEnterpriseAllowEffectiveUser,
} from "./enterprise.js";

export {
  resolveRequiredEnvVar,
  resolveSecretField,
  validateFieldType,
  validateAllowedFields,
  validateMutuallyExclusive,
  validateNonNegativeInt,
  validatePositiveNumber,
  validateOptionalPositiveNumber,
  validateStringList,
  validateOptionalStringList,
  validateStringDict,
  validateOptionalStringDict,
} from "./validators.js";

export { ConfigurationError } from "../exceptions.js";
