/**
 * Tests for config/community module.
 */
import { describe, it, expect, afterEach } from "vitest";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  redactCommunitySessionConfig,
  redactCommunityConfig,
  validateCommunitySessionConfig,
  validateCommunityConfig,
  CommunityServerConfigManager,
} from "./community.js";
import { ConfigurationError } from "../exceptions.js";
import { CONFIG_ENV_VAR } from "./base.js";

// ---------------------------------------------------------------------------
// redactCommunitySessionConfig
// ---------------------------------------------------------------------------

describe("redactCommunitySessionConfig", () => {
  it("redacts_auth_token", () => {
    const cfg = { host: "h", auth_token: "secret" };
    const out = redactCommunitySessionConfig(cfg);
    expect(out["auth_token"]).toBe("[REDACTED]");
    expect(out["host"]).toBe("h");
    // original untouched
    expect(cfg["auth_token"]).toBe("secret");
  });

  it("redacts_binary_tls_fields", () => {
    const cfg = {
      tls_root_certs: Buffer.from("bytes"),
      client_cert_chain: Buffer.from("c"),
      client_private_key: Buffer.from("k"),
    };
    const out = redactCommunitySessionConfig(cfg);
    expect(out["tls_root_certs"]).toBe("[REDACTED]");
    expect(out["client_cert_chain"]).toBe("[REDACTED]");
    expect(out["client_private_key"]).toBe("[REDACTED]");
  });

  it("auth_token_empty_not_redacted", () => {
    const out = redactCommunitySessionConfig({ auth_token: "" });
    expect(out["auth_token"]).toBe("");
  });

  it("string_tls_preserved", () => {
    const cfg = { tls_root_certs: "/path/to/ca.pem" };
    const out = redactCommunitySessionConfig(cfg);
    expect(out["tls_root_certs"]).toBe("/path/to/ca.pem");
  });

  it("binary_skipped_when_flag_false", () => {
    const cfg = { auth_token: "t", tls_root_certs: Buffer.from("b") };
    const out = redactCommunitySessionConfig(cfg, false);
    expect(out["auth_token"]).toBe("[REDACTED]");
    expect(out["tls_root_certs"] instanceof Buffer).toBe(true);
  });

  it("empty_config_passes", () => {
    expect(redactCommunitySessionConfig({})).toEqual({});
  });
});

// ---------------------------------------------------------------------------
// redactCommunityConfig
// ---------------------------------------------------------------------------

describe("redactCommunityConfig", () => {
  it("redacts_psk", () => {
    const cfg = { auth: { psk: "s3cret" } };
    const out = redactCommunityConfig(cfg);
    expect((out["auth"] as Record<string, unknown>)["psk"]).toBe("[REDACTED]");
    // original untouched
    expect((cfg["auth"] as Record<string, unknown>)["psk"]).toBe("s3cret");
  });

  it("preserves_env_var_name", () => {
    const cfg = { auth: { psk_env_var: "DH_PSK" } };
    const out = redactCommunityConfig(cfg);
    expect((out["auth"] as Record<string, unknown>)["psk_env_var"]).toBe("DH_PSK");
  });

  it("redacts_session_auth_token", () => {
    const cfg = {
      sessions: { a: { auth_token: "s" }, b: "not-a-dict" },
      session_creation: { defaults: { auth_token: "t" } },
    };
    const out = redactCommunityConfig(cfg);
    expect(((out["sessions"] as Record<string, unknown>)["a"] as Record<string, unknown>)["auth_token"]).toBe("[REDACTED]");
    expect((out["sessions"] as Record<string, unknown>)["b"]).toBe("not-a-dict");
    expect(((out["session_creation"] as Record<string, unknown>)["defaults"] as Record<string, unknown>)["auth_token"]).toBe("[REDACTED]");
    // original untouched
    expect(((cfg["sessions"] as Record<string, unknown>)["a"] as Record<string, unknown>)["auth_token"]).toBe("s");
  });

  it("empty_config_passes", () => {
    expect(redactCommunityConfig({})).toEqual({});
  });
});

// ---------------------------------------------------------------------------
// validateCommunitySessionConfig
// ---------------------------------------------------------------------------

describe("validateCommunitySessionConfig", () => {
  it("not_dict_raises", () => {
    expect(() => validateCommunitySessionConfig("s", "bad")).toThrow(/must be a dictionary/);
    expect(() => validateCommunitySessionConfig("s", "bad")).toThrow(ConfigurationError);
  });

  it("empty_passes", () => {
    expect(() => validateCommunitySessionConfig("s", {})).not.toThrow();
  });

  it("full_config_passes", () => {
    expect(() =>
      validateCommunitySessionConfig("s", {
        host: "h",
        port: 10000,
        auth_type: "PSK",
        auth_token: "t",
        never_timeout: true,
        session_type: "python",
        use_tls: false,
        tls_root_certs: null,
        client_cert_chain: "/x",
        client_private_key: null,
      })
    ).not.toThrow();
  });

  it("unknown_field_rejected", () => {
    expect(() => validateCommunitySessionConfig("s", { bogus: 1 })).toThrow(/Unknown field 'bogus'/);
  });

  it("wrong_type_raises", () => {
    expect(() => validateCommunitySessionConfig("s", { port: "str" })).toThrow(/port/);
  });

  it("mutually_exclusive_raises", () => {
    expect(() =>
      validateCommunitySessionConfig("s", { auth_token: "a", auth_token_env_var: "B" })
    ).toThrow(/mutually exclusive/);
  });

  it("unknown_auth_type_does_not_raise", () => {
    // Should warn but not throw
    expect(() => validateCommunitySessionConfig("s", { auth_type: "CustomThing" })).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// validateCommunityConfig
// ---------------------------------------------------------------------------

describe("validateCommunityConfig", () => {
  it("not_dict_raises", () => {
    expect(() => validateCommunityConfig([1, 2])).toThrow(/must be a dictionary/);
    expect(() => validateCommunityConfig([1, 2])).toThrow(ConfigurationError);
  });

  it("unknown_top_level_raises", () => {
    expect(() => validateCommunityConfig({ extra: 1 })).toThrow(/Unknown field 'extra'/);
  });

  it("missing_auth_raises", () => {
    expect(() => validateCommunityConfig({})).toThrow(/'auth' missing in community configuration/);
  });

  it("minimal_anonymous_ok", () => {
    const cfg = { auth: { enabled: false } };
    expect(validateCommunityConfig(cfg)).toBe(cfg);
  });

  it("all_sections_ok", () => {
    const cfg = {
      auth: { enabled: false },
      security: { credential_retrieval_mode: "none" },
      sessions: { a: { host: "h" } },
      session_creation: { defaults: { launch_method: "python" } },
      mcp_session_idle_timeout_seconds: 60,
    };
    expect(validateCommunityConfig(cfg)).toBe(cfg);
  });

  it("bad_idle_timeout_raises", () => {
    expect(() =>
      validateCommunityConfig({ auth: { enabled: false }, mcp_session_idle_timeout_seconds: 0 })
    ).toThrow(/mcp_session_idle_timeout_seconds/);
  });

  it("idle_timeout_wrong_type_raises", () => {
    expect(() =>
      validateCommunityConfig({ auth: { enabled: false }, mcp_session_idle_timeout_seconds: "x" })
    ).toThrow(/mcp_session_idle_timeout_seconds/);
  });

  it("auth_block_inline_psk_ok", () => {
    expect(() => validateCommunityConfig({ auth: { psk: "s" } })).not.toThrow();
  });

  it("auth_block_env_var_ok", () => {
    expect(() => validateCommunityConfig({ auth: { psk_env_var: "DH_MCP_PSK" } })).not.toThrow();
  });

  it("auth_block_both_psk_and_env_var_raises", () => {
    expect(() =>
      validateCommunityConfig({ auth: { psk: "s", psk_env_var: "X" } })
    ).toThrow(/mutually exclusive/);
  });

  it("auth_empty_block_raises", () => {
    expect(() => validateCommunityConfig({ auth: {} })).toThrow(/enabled: true/);
  });

  it("auth_empty_psk_raises", () => {
    expect(() => validateCommunityConfig({ auth: { psk: "" } })).toThrow(/enabled: true/);
  });

  it("auth_empty_psk_env_var_raises", () => {
    expect(() => validateCommunityConfig({ auth: { psk_env_var: "" } })).toThrow(/enabled: true/);
  });

  it("auth_enabled_false_with_psk_raises", () => {
    expect(() => validateCommunityConfig({ auth: { enabled: false, psk: "s" } })).toThrow(/enabled: false/);
  });

  it("auth_enabled_false_with_empty_psk_raises", () => {
    expect(() => validateCommunityConfig({ auth: { enabled: false, psk: "" } })).toThrow(/enabled: false/);
  });
});

// ---------------------------------------------------------------------------
// CommunityServerConfigManager
// ---------------------------------------------------------------------------

const _MINIMAL_CFG_JSON = '{"auth": {"enabled": false}, "sessions": {}}';
const _MINIMAL_CFG_DICT = { auth: { enabled: false }, sessions: {} };

describe("CommunityServerConfigManager", () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = join(tmpdir(), `test-community-${Date.now()}`);
    await mkdir(tmpDir, { recursive: true });
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
    delete process.env[CONFIG_ENV_VAR];
  });

  it("loads_from_explicit_path", async () => {
    const cfgFile = join(tmpDir, "config.json");
    await writeFile(cfgFile, _MINIMAL_CFG_JSON);
    const mgr = new CommunityServerConfigManager(cfgFile);
    const result = await mgr.getConfig();
    expect(result).toEqual(_MINIMAL_CFG_DICT);
  });

  it("caches_result", async () => {
    const cfgFile = join(tmpDir, "config.json");
    await writeFile(cfgFile, _MINIMAL_CFG_JSON);
    const mgr = new CommunityServerConfigManager(cfgFile);
    const r1 = await mgr.getConfig();
    const r2 = await mgr.getConfig();
    expect(r1).toBe(r2);
  });

  it("uses_env_var", async () => {
    const cfgFile = join(tmpDir, "config.json");
    await writeFile(cfgFile, _MINIMAL_CFG_JSON);
    process.env[CONFIG_ENV_VAR] = cfgFile;
    const mgr = new CommunityServerConfigManager();
    const result = await mgr.getConfig();
    expect(result).toEqual(_MINIMAL_CFG_DICT);
  });

  it("set_cache_validates_and_caches", async () => {
    const mgr = new CommunityServerConfigManager("/nonexistent");
    await mgr._setConfigCache({ ...(_MINIMAL_CFG_DICT) });
    const result = await mgr.getConfig();
    expect(result).toEqual(_MINIMAL_CFG_DICT);
  });

  it("set_cache_invalid_raises", async () => {
    const mgr = new CommunityServerConfigManager("/nonexistent");
    await expect(mgr._setConfigCache({ bogus: 1 })).rejects.toThrow(ConfigurationError);
  });

  it("clear_cache_resets_cache", async () => {
    const cfgFile = join(tmpDir, "config.json");
    await writeFile(cfgFile, _MINIMAL_CFG_JSON);
    const mgr = new CommunityServerConfigManager(cfgFile);
    await mgr.getConfig();
    await mgr.clearConfigCache();
    expect(mgr["_cache"]).toBeUndefined();
  });

  it("invalid_file_raises", async () => {
    const cfgFile = join(tmpDir, "bad.json");
    await writeFile(cfgFile, "not json");
    const mgr = new CommunityServerConfigManager(cfgFile);
    await expect(mgr.getConfig()).rejects.toThrow(ConfigurationError);
  });
});

// needed import for beforeEach
import { beforeEach } from "vitest";
