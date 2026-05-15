/**
 * Smoke tests for config public re-export surface.
 */
import { it, expect } from "vitest";
import {
  ConfigManager,
  CONFIG_ENV_VAR,
  DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS,
  CommunityServerConfigManager,
  EnterpriseServerConfigManager,
  DEFAULT_CONNECTION_TIMEOUT_SECONDS,
  SUPPORTED_AUTH_BACKENDS,
  validateCommunityConfig,
  validateCommunitySessionConfig,
  validateEnterpriseConfig,
  redactCommunityConfig,
  redactCommunitySessionConfig,
  redactEnterpriseConfig,
  resolveRequiredEnvVar,
  resolveSecretField,
  ConfigurationError,
} from "./index.js";

it("all_surface_importable", () => {
  expect(ConfigManager).toBeDefined();
  expect(CONFIG_ENV_VAR).toBeDefined();
  expect(DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS).toBeDefined();
  expect(CommunityServerConfigManager).toBeDefined();
  expect(EnterpriseServerConfigManager).toBeDefined();
  expect(DEFAULT_CONNECTION_TIMEOUT_SECONDS).toBeDefined();
  expect(SUPPORTED_AUTH_BACKENDS).toBeDefined();
  expect(validateCommunityConfig).toBeDefined();
  expect(validateCommunitySessionConfig).toBeDefined();
  expect(validateEnterpriseConfig).toBeDefined();
  expect(redactCommunityConfig).toBeDefined();
  expect(redactCommunitySessionConfig).toBeDefined();
  expect(redactEnterpriseConfig).toBeDefined();
  expect(resolveRequiredEnvVar).toBeDefined();
  expect(resolveSecretField).toBeDefined();
  expect(ConfigurationError).toBeDefined();
});

it("config_env_var_value", () => {
  expect(CONFIG_ENV_VAR).toBe("DH_MCP_CONFIG_FILE");
});

it("default_idle_timeout_is_3600", () => {
  expect(DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS).toBe(3600.0);
});

it("default_connection_timeout_is_10", () => {
  expect(DEFAULT_CONNECTION_TIMEOUT_SECONDS).toBe(10.0);
});
