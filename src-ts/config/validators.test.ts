/**
 * Tests for config/validators module.
 */
import { describe, it, expect, vi, afterEach } from "vitest";
import {
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
  resolveRequiredEnvVar,
  resolveSecretField,
} from "./validators.js";
import { ConfigurationError } from "../exceptions.js";

// ---------------------------------------------------------------------------
// validateFieldType
// ---------------------------------------------------------------------------

describe("validateFieldType", () => {
  it("single_type_valid", () => {
    expect(() => validateFieldType("ctx", "port", 8080, "number")).not.toThrow();
  });

  it("single_type_invalid", () => {
    expect(() => validateFieldType("ctx", "port", "8080", "number")).toThrow(ConfigurationError);
    expect(() => validateFieldType("ctx", "port", "8080", "number")).toThrow(/Field 'port' for ctx must be of type number/);
  });

  it("tuple_valid", () => {
    expect(() => validateFieldType("ctx", "timeout", 1.5, ["number"])).not.toThrow();
  });

  it("tuple_invalid", () => {
    expect(() => validateFieldType("ctx", "timeout", "x", ["number", "string"])).not.toThrow(); // "x" is a string, passes
    expect(() => validateFieldType("ctx", "timeout", {}, ["number", "string"])).toThrow(ConfigurationError);
    expect(() => validateFieldType("ctx", "timeout", {}, ["number", "string"])).toThrow(/must be one of types/);
  });

  it("is_optional_prefix", () => {
    expect(() => validateFieldType("ctx", "x", 1, "string", { isOptional: true })).toThrow(ConfigurationError);
    expect(() => validateFieldType("ctx", "x", 1, "string", { isOptional: true })).toThrow(/Optional field 'x'/);
  });
});

// ---------------------------------------------------------------------------
// validateAllowedFields
// ---------------------------------------------------------------------------

describe("validateAllowedFields", () => {
  it("valid", () => {
    expect(() => validateAllowedFields("ctx", { a: 1, b: "x" }, { a: "number", b: "string" })).not.toThrow();
  });

  it("unknown_rejects_by_default", () => {
    expect(() => validateAllowedFields("ctx", { a: 1, c: 9 }, { a: "number" })).toThrow(ConfigurationError);
    expect(() => validateAllowedFields("ctx", { a: 1, c: 9 }, { a: "number" })).toThrow(/Unknown field 'c' for ctx/);
  });

  it("unknown_warns_when_not_strict", () => {
    expect(() => validateAllowedFields("ctx", { a: 1, c: 9 }, { a: "number" }, { rejectUnknown: false })).not.toThrow();
  });

  it("bad_type", () => {
    expect(() => validateAllowedFields("ctx", { a: "nope" }, { a: "number" })).toThrow(ConfigurationError);
    expect(() => validateAllowedFields("ctx", { a: "nope" }, { a: "number" })).toThrow(/Field 'a' for ctx must be of type number/);
  });
});

// ---------------------------------------------------------------------------
// validateMutuallyExclusive
// ---------------------------------------------------------------------------

describe("validateMutuallyExclusive", () => {
  it("neither", () => {
    expect(() => validateMutuallyExclusive("ctx", {}, "a", "b")).not.toThrow();
  });

  it("only_a", () => {
    expect(() => validateMutuallyExclusive("ctx", { a: 1 }, "a", "b")).not.toThrow();
  });

  it("only_b", () => {
    expect(() => validateMutuallyExclusive("ctx", { b: 2 }, "a", "b")).not.toThrow();
  });

  it("both_raises", () => {
    expect(() => validateMutuallyExclusive("ctx", { a: 1, b: 2 }, "a", "b")).toThrow(ConfigurationError);
    expect(() => validateMutuallyExclusive("ctx", { a: 1, b: 2 }, "a", "b")).toThrow(/'a' and 'b' are mutually exclusive/);
  });
});

// ---------------------------------------------------------------------------
// validateNonNegativeInt
// ---------------------------------------------------------------------------

describe("validateNonNegativeInt", () => {
  it("zero", () => {
    expect(() => validateNonNegativeInt("retries", 0)).not.toThrow();
  });

  it("positive", () => {
    expect(() => validateNonNegativeInt("retries", 3)).not.toThrow();
  });

  it("negative_raises", () => {
    expect(() => validateNonNegativeInt("retries", -1)).toThrow(ConfigurationError);
    expect(() => validateNonNegativeInt("retries", -1)).toThrow(/'retries'.*must be non-negative/);
  });

  it("bool_raises", () => {
    expect(() => validateNonNegativeInt("retries", true)).toThrow(ConfigurationError);
    expect(() => validateNonNegativeInt("retries", true)).toThrow(/'retries'.*must be an integer/);
  });

  it("float_raises", () => {
    expect(() => validateNonNegativeInt("retries", 1.5)).toThrow(ConfigurationError);
    expect(() => validateNonNegativeInt("retries", 1.5)).toThrow(/'retries'.*must be an integer/);
  });
});

// ---------------------------------------------------------------------------
// validatePositiveNumber
// ---------------------------------------------------------------------------

describe("validatePositiveNumber", () => {
  it("valid_int", () => {
    expect(() => validatePositiveNumber("timeout_seconds", 5)).not.toThrow();
  });

  it("valid_float", () => {
    expect(() => validatePositiveNumber("timeout_seconds", 1.5)).not.toThrow();
  });

  it("zero_invalid", () => {
    expect(() => validatePositiveNumber("timeout_seconds", 0)).toThrow(ConfigurationError);
    expect(() => validatePositiveNumber("timeout_seconds", 0)).toThrow(/'timeout_seconds'.*must be positive/);
  });

  it("negative_invalid", () => {
    expect(() => validatePositiveNumber("timeout_seconds", -1)).toThrow(ConfigurationError);
    expect(() => validatePositiveNumber("timeout_seconds", -1)).toThrow(/'timeout_seconds'.*must be positive/);
  });

  it("bool_invalid", () => {
    expect(() => validatePositiveNumber("timeout_seconds", true)).toThrow(ConfigurationError);
    expect(() => validatePositiveNumber("timeout_seconds", true)).toThrow(/'timeout_seconds'.*must be a number/);
  });

  it("string_invalid", () => {
    expect(() => validatePositiveNumber("timeout_seconds", "x")).toThrow(ConfigurationError);
    expect(() => validatePositiveNumber("timeout_seconds", "x")).toThrow(/'timeout_seconds'.*must be a number/);
  });
});

// ---------------------------------------------------------------------------
// validateOptionalPositiveNumber
// ---------------------------------------------------------------------------

describe("validateOptionalPositiveNumber", () => {
  it("absent", () => {
    expect(() => validateOptionalPositiveNumber({}, "timeout_seconds")).not.toThrow();
  });

  it("null_value", () => {
    expect(() => validateOptionalPositiveNumber({ timeout_seconds: null }, "timeout_seconds")).not.toThrow();
  });

  it("valid_int", () => {
    expect(() => validateOptionalPositiveNumber({ timeout_seconds: 5 }, "timeout_seconds")).not.toThrow();
  });

  it("valid_float", () => {
    expect(() => validateOptionalPositiveNumber({ timeout_seconds: 1.5 }, "timeout_seconds")).not.toThrow();
  });

  it("zero", () => {
    expect(() => validateOptionalPositiveNumber({ timeout_seconds: 0 }, "timeout_seconds")).toThrow(ConfigurationError);
    expect(() => validateOptionalPositiveNumber({ timeout_seconds: 0 }, "timeout_seconds")).toThrow(/'timeout_seconds'.*must be positive/);
  });

  it("negative", () => {
    expect(() => validateOptionalPositiveNumber({ timeout_seconds: -1 }, "timeout_seconds")).toThrow(ConfigurationError);
    expect(() => validateOptionalPositiveNumber({ timeout_seconds: -1 }, "timeout_seconds")).toThrow(/'timeout_seconds'.*must be positive/);
  });

  it("bool", () => {
    expect(() => validateOptionalPositiveNumber({ timeout_seconds: true }, "timeout_seconds")).toThrow(ConfigurationError);
    expect(() => validateOptionalPositiveNumber({ timeout_seconds: true }, "timeout_seconds")).toThrow(/'timeout_seconds'.*must be a number/);
  });
});

// ---------------------------------------------------------------------------
// validateStringList
// ---------------------------------------------------------------------------

describe("validateStringList", () => {
  it("empty", () => {
    expect(() => validateStringList("volumes", [])).not.toThrow();
  });

  it("all_strings", () => {
    expect(() => validateStringList("volumes", ["a", "b", "c"])).not.toThrow();
  });

  it("int_item", () => {
    expect(() => validateStringList("volumes", ["a", 123 as unknown as string])).toThrow(ConfigurationError);
    expect(() => validateStringList("volumes", ["a", 123 as unknown as string])).toThrow(/'volumes\[1\]'.*must be a string/);
  });

  it("null_item", () => {
    expect(() => validateStringList("volumes", [null as unknown as string])).toThrow(ConfigurationError);
    expect(() => validateStringList("volumes", [null as unknown as string])).toThrow(/'volumes\[0\]'.*must be a string/);
  });
});

// ---------------------------------------------------------------------------
// validateOptionalStringList
// ---------------------------------------------------------------------------

describe("validateOptionalStringList", () => {
  it("absent", () => {
    expect(() => validateOptionalStringList({}, "volumes")).not.toThrow();
  });

  it("empty", () => {
    expect(() => validateOptionalStringList({ volumes: [] }, "volumes")).not.toThrow();
  });

  it("valid", () => {
    expect(() => validateOptionalStringList({ volumes: ["a", "b"] }, "volumes")).not.toThrow();
  });

  it("int_item", () => {
    expect(() => validateOptionalStringList({ volumes: ["a", 123] }, "volumes")).toThrow(ConfigurationError);
    expect(() => validateOptionalStringList({ volumes: ["a", 123] }, "volumes")).toThrow(/'volumes\[1\]'.*must be a string/);
  });
});

// ---------------------------------------------------------------------------
// validateStringDict
// ---------------------------------------------------------------------------

describe("validateStringDict", () => {
  it("empty", () => {
    expect(() => validateStringDict("env_vars", {})).not.toThrow();
  });

  it("valid", () => {
    expect(() => validateStringDict("env_vars", { KEY: "value", OTHER: "val" })).not.toThrow();
  });

  it("int_value", () => {
    expect(() => validateStringDict("env_vars", { KEY: 123 as unknown as string })).toThrow(ConfigurationError);
    expect(() => validateStringDict("env_vars", { KEY: 123 as unknown as string })).toThrow(/'env_vars\[KEY\]' value must be a string/);
  });
});

// ---------------------------------------------------------------------------
// validateOptionalStringDict
// ---------------------------------------------------------------------------

describe("validateOptionalStringDict", () => {
  it("absent", () => {
    expect(() => validateOptionalStringDict({}, "env_vars")).not.toThrow();
  });

  it("valid", () => {
    expect(() => validateOptionalStringDict({ env_vars: { K: "v" } }, "env_vars")).not.toThrow();
  });

  it("int_value", () => {
    expect(() => validateOptionalStringDict({ env_vars: { K: 99 } }, "env_vars")).toThrow(ConfigurationError);
    expect(() => validateOptionalStringDict({ env_vars: { K: 99 } }, "env_vars")).toThrow(/'env_vars\[K\]' value must be a string/);
  });
});

// ---------------------------------------------------------------------------
// resolveRequiredEnvVar
// ---------------------------------------------------------------------------

describe("resolveRequiredEnvVar", () => {
  afterEach(() => {
    delete process.env["DH_TEST_VAR"];
  });

  it("returns_value", () => {
    vi.stubEnv("DH_TEST_VAR", "secret-value");
    expect(resolveRequiredEnvVar({
      fieldName: "auth.psk_env_var",
      envVarName: "DH_TEST_VAR",
      context: "test ctx",
    })).toBe("secret-value");
  });

  it("unset_raises", () => {
    delete process.env["DH_TEST_VAR"];
    expect(() => resolveRequiredEnvVar({
      fieldName: "auth.psk_env_var",
      envVarName: "DH_TEST_VAR",
      context: "test ctx",
    })).toThrow(ConfigurationError);
    expect(() => resolveRequiredEnvVar({
      fieldName: "auth.psk_env_var",
      envVarName: "DH_TEST_VAR",
      context: "test ctx",
    })).toThrow(/test ctx: 'auth.psk_env_var' refers to environment variable 'DH_TEST_VAR'/);
  });

  it("empty_raises", () => {
    vi.stubEnv("DH_TEST_VAR", "");
    expect(() => resolveRequiredEnvVar({
      fieldName: "auth.psk_env_var",
      envVarName: "DH_TEST_VAR",
      context: "test ctx",
    })).toThrow(ConfigurationError);
    expect(() => resolveRequiredEnvVar({
      fieldName: "auth.psk_env_var",
      envVarName: "DH_TEST_VAR",
      context: "test ctx",
    })).toThrow(/unset or empty/);
  });
});

// ---------------------------------------------------------------------------
// resolveSecretField
// ---------------------------------------------------------------------------

describe("resolveSecretField", () => {
  afterEach(() => {
    delete process.env["DH_TEST_PSK"];
  });

  it("inline_returns_value", () => {
    const config = { psk: "inline-secret" };
    expect(resolveSecretField({
      config,
      inlineField: "psk",
      envVarField: "psk_env_var",
      context: "ctx",
    })).toBe("inline-secret");
  });

  it("env_var_returns_value", () => {
    vi.stubEnv("DH_TEST_PSK", "env-secret");
    const config = { psk_env_var: "DH_TEST_PSK" };
    expect(resolveSecretField({
      config,
      inlineField: "psk",
      envVarField: "psk_env_var",
      context: "ctx",
    })).toBe("env-secret");
  });

  it("neither_returns_undefined", () => {
    expect(resolveSecretField({
      config: {},
      inlineField: "psk",
      envVarField: "psk_env_var",
      context: "ctx",
    })).toBeUndefined();
  });

  it("env_var_unset_raises", () => {
    delete process.env["DH_TEST_PSK"];
    expect(() => resolveSecretField({
      config: { psk_env_var: "DH_TEST_PSK" },
      inlineField: "psk",
      envVarField: "psk_env_var",
      context: "ctx",
    })).toThrow(ConfigurationError);
    expect(() => resolveSecretField({
      config: { psk_env_var: "DH_TEST_PSK" },
      inlineField: "psk",
      envVarField: "psk_env_var",
      context: "ctx",
    })).toThrow(/DH_TEST_PSK/);
  });

  it("inline_takes_precedence", () => {
    delete process.env["DH_TEST_PSK"];
    const config = { psk: "inline-secret", psk_env_var: "DH_TEST_PSK" };
    expect(resolveSecretField({
      config,
      inlineField: "psk",
      envVarField: "psk_env_var",
      context: "ctx",
    })).toBe("inline-secret");
  });

  it("inline_empty_falls_through_to_env", () => {
    vi.stubEnv("DH_TEST_PSK", "env-secret");
    const config = { psk: "", psk_env_var: "DH_TEST_PSK" };
    expect(resolveSecretField({
      config,
      inlineField: "psk",
      envVarField: "psk_env_var",
      context: "ctx",
    })).toBe("env-secret");
  });

  it("inline_non_string_falls_through", () => {
    vi.stubEnv("DH_TEST_PSK", "env-secret");
    const config = { psk: null, psk_env_var: "DH_TEST_PSK" };
    expect(resolveSecretField({
      config,
      inlineField: "psk",
      envVarField: "psk_env_var",
      context: "ctx",
    })).toBe("env-secret");
  });

  it("env_var_non_string_returns_undefined", () => {
    const config = { psk_env_var: null };
    expect(resolveSecretField({
      config,
      inlineField: "psk",
      envVarField: "psk_env_var",
      context: "ctx",
    })).toBeUndefined();
  });
});
