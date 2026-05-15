/**
 * Tests for config/enterprise module.
 */
import { describe, it, expect, afterEach, beforeEach } from "vitest";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  DEFAULT_CONNECTION_TIMEOUT_SECONDS,
  SUPPORTED_AUTH_BACKENDS,
  redactEnterpriseConfig,
  validateEnterpriseConfig,
  EnterpriseServerConfigManager,
  getEnterpriseAuthBackends,
  getEnterpriseAllowEffectiveUser,
} from "./enterprise.js";
import { ConfigurationError } from "../exceptions.js";
import { CONFIG_ENV_VAR } from "./base.js";

function minimalConfig(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    system_name: "prod",
    connection_json_url: "https://x/iris/connection.json",
    auth: { backends: ["password"] },
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

it("default_timeout_constant", () => {
  expect(DEFAULT_CONNECTION_TIMEOUT_SECONDS).toBe(10.0);
});

it("supported_backends_constant", () => {
  expect(SUPPORTED_AUTH_BACKENDS.has("password")).toBe(true);
  expect(SUPPORTED_AUTH_BACKENDS.has("private_key")).toBe(true);
  expect(SUPPORTED_AUTH_BACKENDS.size).toBe(2);
});

// ---------------------------------------------------------------------------
// redactEnterpriseConfig
// ---------------------------------------------------------------------------

describe("redactEnterpriseConfig", () => {
  it("returns_shallow_copy", () => {
    const cfg = minimalConfig();
    const out = redactEnterpriseConfig(cfg);
    expect(out).toEqual(cfg);
    expect(out).not.toBe(cfg);
    out["system_name"] = "other";
    expect(cfg["system_name"]).toBe("prod");
  });
});

// ---------------------------------------------------------------------------
// validateEnterpriseConfig — top-level fields
// ---------------------------------------------------------------------------

describe("validateEnterpriseConfig top-level", () => {
  it("not_dict_raises", () => {
    expect(() => validateEnterpriseConfig("nope")).toThrow(/must be a dictionary/);
    expect(() => validateEnterpriseConfig("nope")).toThrow(ConfigurationError);
  });

  it("system_name_not_str_raises", () => {
    expect(() =>
      validateEnterpriseConfig({
        system_name: 1,
        connection_json_url: "u",
        auth: { backends: ["password"] },
      })
    ).toThrow(/system_name/);
  });

  it("system_name_missing_raises", () => {
    expect(() =>
      validateEnterpriseConfig({ connection_json_url: "u", auth: { backends: ["password"] } })
    ).toThrow(/system_name/);
  });

  it("connection_json_url_missing_raises", () => {
    expect(() =>
      validateEnterpriseConfig({ system_name: "x", auth: { backends: ["password"] } })
    ).toThrow(/connection_json_url/);
  });

  it("connection_json_url_wrong_type_raises", () => {
    expect(() =>
      validateEnterpriseConfig({
        system_name: "x",
        connection_json_url: 1,
        auth: { backends: ["password"] },
      })
    ).toThrow(/connection_json_url/);
  });

  it("auth_wrong_type_raises", () => {
    expect(() =>
      validateEnterpriseConfig({
        system_name: "x",
        connection_json_url: "u",
        auth: "not-a-dict",
      })
    ).toThrow(/auth/);
  });

  it("unknown_top_level_field_raises", () => {
    expect(() =>
      validateEnterpriseConfig(minimalConfig({ surprise: 1 }))
    ).toThrow(/Unknown field 'surprise'/);
  });

  it("minimal_ok", () => {
    expect(validateEnterpriseConfig(minimalConfig())).toBeTruthy();
  });

  it("bad_connection_timeout_raises", () => {
    expect(() => validateEnterpriseConfig(minimalConfig({ connection_timeout: -1 }))).toThrow(/connection_timeout/);
  });

  it("bad_idle_timeout_raises", () => {
    expect(() =>
      validateEnterpriseConfig(minimalConfig({ mcp_session_idle_timeout_seconds: 0 }))
    ).toThrow(/mcp_session_idle_timeout_seconds/);
  });

  it("legacy_auth_type_field_raises", () => {
    expect(() =>
      validateEnterpriseConfig({
        system_name: "prod",
        connection_json_url: "u",
        auth: { backends: ["password"] },
        auth_type: "password",
      })
    ).toThrow(/Unknown field 'auth_type'/);
  });

  it("legacy_password_field_raises", () => {
    expect(() =>
      validateEnterpriseConfig(minimalConfig({ password: "secret" }))
    ).toThrow(/Unknown field 'password'/);
  });
});

// ---------------------------------------------------------------------------
// validateEnterpriseConfig — auth block
// ---------------------------------------------------------------------------

describe("validateEnterpriseConfig auth block", () => {
  it("unknown_field_raises", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ auth: { backends: ["password"], extra: 1 } })
      )
    ).toThrow(/Unknown field 'extra'/);
  });

  it("missing_backends_raises", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ auth: {} })
      )
    ).toThrow(/'backends' missing/);
  });

  it("backends_wrong_type_raises", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ auth: { backends: "password" } })
      )
    ).toThrow(/backends/);
  });

  it("backends_empty_raises", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ auth: { backends: [] } })
      )
    ).toThrow(/non-empty/);
  });

  it("backends_duplicates_raises", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ auth: { backends: ["password", "password"] } })
      )
    ).toThrow(/duplicate/);
  });

  it("backends_non_string_raises", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ auth: { backends: [1] } })
      )
    ).toThrow(/only strings/);
  });

  it("backends_unsupported_value_raises", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ auth: { backends: ["kerberos"] } })
      )
    ).toThrow(/unsupported entry 'kerberos'/);
  });

  it("backends_password_ok", () => {
    expect(() =>
      validateEnterpriseConfig(minimalConfig({ auth: { backends: ["password"] } }))
    ).not.toThrow();
  });

  it("backends_private_key_ok", () => {
    expect(() =>
      validateEnterpriseConfig(minimalConfig({ auth: { backends: ["private_key"] } }))
    ).not.toThrow();
  });

  it("backends_both_ok", () => {
    expect(() =>
      validateEnterpriseConfig(minimalConfig({ auth: { backends: ["password", "private_key"] } }))
    ).not.toThrow();
  });

  it("allow_effective_user_wrong_type_raises", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ auth: { backends: ["password"], allow_effective_user: "yes" } })
      )
    ).toThrow(/allow_effective_user/);
  });

  it("allow_effective_user_true_without_password_raises", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ auth: { backends: ["private_key"], allow_effective_user: true } })
      )
    ).toThrow(/'password' is included/);
  });

  it("allow_effective_user_false_without_password_ok", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ auth: { backends: ["private_key"], allow_effective_user: false } })
      )
    ).not.toThrow();
  });

  it("allow_effective_user_true_with_password_ok", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({
          auth: { backends: ["password", "private_key"], allow_effective_user: true },
        })
      )
    ).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// validateEnterpriseConfig — session_creation
// ---------------------------------------------------------------------------

describe("validateEnterpriseConfig session_creation", () => {
  it("absent_ok", () => {
    expect(() => validateEnterpriseConfig(minimalConfig())).not.toThrow();
  });

  it("unknown_top_level_raises", () => {
    expect(() =>
      validateEnterpriseConfig(minimalConfig({ session_creation: { bogus: 1 } }))
    ).toThrow(/Unknown field 'bogus'/);
  });

  it("bad_max_concurrent_raises", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ session_creation: { max_concurrent_sessions: -1, defaults: { heap_size_gb: 1 } } })
      )
    ).toThrow(/max_concurrent_sessions/);
  });

  it("missing_defaults_raises", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ session_creation: { max_concurrent_sessions: 1 } })
      )
    ).toThrow(/defaults.*required/);
  });

  it("defaults_unknown_field_raises", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ session_creation: { defaults: { heap_size_gb: 1, bogus: 2 } } })
      )
    ).toThrow(/Unknown field 'bogus'/);
  });

  it("defaults_missing_heap_size_raises", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ session_creation: { defaults: { server: "s" } } })
      )
    ).toThrow(/heap_size_gb.*required/);
  });

  it("full_session_creation_ok", () => {
    const cfg = minimalConfig({
      session_creation: {
        max_concurrent_sessions: 5,
        defaults: {
          heap_size_gb: 2,
          auto_delete_timeout: 60,
          server: "s",
          engine: "e",
          extra_jvm_args: [],
          extra_environment_vars: [],
          admin_groups: [],
          viewer_groups: [],
          timeout_seconds: 30,
          session_arguments: {},
          programming_language: "Python",
        },
      },
    });
    expect(() => validateEnterpriseConfig(cfg)).not.toThrow();
  });

  it("with_session_creation_ok", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({ session_creation: { defaults: { heap_size_gb: 1 } } })
      )
    ).not.toThrow();
  });

  it("full_auth_block_ok", () => {
    expect(() =>
      validateEnterpriseConfig(
        minimalConfig({
          auth: { backends: ["password", "private_key"], allow_effective_user: true },
        })
      )
    ).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

describe("getEnterpriseAuthBackends", () => {
  it("returns_list_copy", () => {
    const cfg = minimalConfig({ auth: { backends: ["password", "private_key"] } });
    const out = getEnterpriseAuthBackends(cfg);
    expect(out).toEqual(["password", "private_key"]);
    out.push("kerberos");
    expect(((cfg["auth"] as Record<string, unknown>)["backends"] as string[])).toEqual([
      "password",
      "private_key",
    ]);
  });
});

describe("getEnterpriseAllowEffectiveUser", () => {
  it("default_false", () => {
    expect(getEnterpriseAllowEffectiveUser(minimalConfig())).toBe(false);
  });

  it("explicit_true", () => {
    const cfg = minimalConfig({ auth: { backends: ["password"], allow_effective_user: true } });
    expect(getEnterpriseAllowEffectiveUser(cfg)).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// EnterpriseServerConfigManager
// ---------------------------------------------------------------------------

describe("EnterpriseServerConfigManager", () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = join(tmpdir(), `test-enterprise-${Date.now()}`);
    await mkdir(tmpDir, { recursive: true });
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
    delete process.env[CONFIG_ENV_VAR];
  });

  it("loads_from_explicit_path", async () => {
    const cfgFile = join(tmpDir, "cfg.json");
    await writeFile(cfgFile, JSON.stringify(minimalConfig()));
    const mgr = new EnterpriseServerConfigManager(cfgFile);
    const result = await mgr.getConfig();
    expect(result["system_name"]).toBe("prod");
    expect((result["auth"] as Record<string, unknown>)["backends"]).toEqual(["password"]);
  });

  it("caches_result", async () => {
    const cfgFile = join(tmpDir, "cfg.json");
    await writeFile(cfgFile, JSON.stringify(minimalConfig()));
    const mgr = new EnterpriseServerConfigManager(cfgFile);
    const r1 = await mgr.getConfig();
    const r2 = await mgr.getConfig();
    expect(r1).toBe(r2);
  });

  it("uses_env_var", async () => {
    const cfgFile = join(tmpDir, "cfg.json");
    await writeFile(cfgFile, JSON.stringify(minimalConfig()));
    process.env[CONFIG_ENV_VAR] = cfgFile;
    const mgr = new EnterpriseServerConfigManager();
    const result = await mgr.getConfig();
    expect(result["system_name"]).toBe("prod");
  });

  it("set_cache_validates_and_caches", async () => {
    const mgr = new EnterpriseServerConfigManager("/nonexistent");
    await mgr._setConfigCache(minimalConfig());
    expect((await mgr.getConfig())["system_name"]).toBe("prod");
  });

  it("set_cache_invalid_raises", async () => {
    const mgr = new EnterpriseServerConfigManager("/nonexistent");
    await expect(mgr._setConfigCache({ bogus: 1 })).rejects.toThrow(ConfigurationError);
  });

  it("clear_cache_resets", async () => {
    const cfgFile = join(tmpDir, "cfg.json");
    await writeFile(cfgFile, JSON.stringify(minimalConfig()));
    const mgr = new EnterpriseServerConfigManager(cfgFile);
    await mgr.getConfig();
    await mgr.clearConfigCache();
    expect(mgr["_cache"]).toBeUndefined();
  });

  it("invalid_file_raises", async () => {
    const cfgFile = join(tmpDir, "bad.json");
    await writeFile(cfgFile, "not json");
    const mgr = new EnterpriseServerConfigManager(cfgFile);
    await expect(mgr.getConfig()).rejects.toThrow(ConfigurationError);
  });
});
