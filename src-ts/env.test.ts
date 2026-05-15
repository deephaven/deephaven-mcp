/**
 * Unit tests for env module.
 */
import { describe, it, expect, afterEach } from "vitest";
import { vi } from "vitest";
import {
  _TRUTHY_ENV_VALUES,
  envBool,
  envFloat,
  envInt,
  envRequired,
  envStr,
  ValueError,
  RuntimeError,
} from "./env.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

// ---------------------------------------------------------------------------
// envStr
// ---------------------------------------------------------------------------

describe("envStr", () => {
  it("unset_returns_default", () => {
    vi.stubEnv("DH_TEST_X", undefined as unknown as string);
    expect(envStr("DH_TEST_X")).toBeUndefined();
    expect(envStr("DH_TEST_X", "fallback")).toBe("fallback");
  });

  it("set_returns_value", () => {
    vi.stubEnv("DH_TEST_X", "hello");
    expect(envStr("DH_TEST_X")).toBe("hello");
    expect(envStr("DH_TEST_X", "fallback")).toBe("hello");
  });

  it("empty_returns_empty", () => {
    vi.stubEnv("DH_TEST_X", "");
    expect(envStr("DH_TEST_X", "fallback")).toBe("");
  });
});

// ---------------------------------------------------------------------------
// envInt
// ---------------------------------------------------------------------------

describe("envInt", () => {
  it("unset_returns_default", () => {
    vi.stubEnv("DH_TEST_X", undefined as unknown as string);
    expect(envInt("DH_TEST_X", 42)).toBe(42);
  });

  it("set_returns_parsed", () => {
    vi.stubEnv("DH_TEST_X", "7");
    expect(envInt("DH_TEST_X", 42)).toBe(7);
  });

  it("negative_value", () => {
    vi.stubEnv("DH_TEST_X", "-3");
    expect(envInt("DH_TEST_X", 0)).toBe(-3);
  });

  it("invalid_raises_with_var_name", () => {
    vi.stubEnv("DH_TEST_X", "abc");
    expect(() => envInt("DH_TEST_X", 0)).toThrow(ValueError);
    expect(() => envInt("DH_TEST_X", 0)).toThrow(/DH_TEST_X='abc'/);
  });

  it("empty_raises", () => {
    vi.stubEnv("DH_TEST_X", "");
    expect(() => envInt("DH_TEST_X", 0)).toThrow(ValueError);
    expect(() => envInt("DH_TEST_X", 0)).toThrow(/DH_TEST_X=''/);
  });
});

// ---------------------------------------------------------------------------
// envFloat
// ---------------------------------------------------------------------------

describe("envFloat", () => {
  it("unset_returns_default", () => {
    vi.stubEnv("DH_TEST_X", undefined as unknown as string);
    expect(envFloat("DH_TEST_X", 1.5)).toBe(1.5);
  });

  it("set_returns_parsed", () => {
    vi.stubEnv("DH_TEST_X", "2.75");
    expect(envFloat("DH_TEST_X", 0.0)).toBe(2.75);
  });

  it("integer_string_parses", () => {
    vi.stubEnv("DH_TEST_X", "3");
    expect(envFloat("DH_TEST_X", 0.0)).toBe(3.0);
  });

  it("invalid_raises_with_var_name", () => {
    vi.stubEnv("DH_TEST_X", "not-a-float");
    expect(() => envFloat("DH_TEST_X", 0.0)).toThrow(ValueError);
    expect(() => envFloat("DH_TEST_X", 0.0)).toThrow(/DH_TEST_X='not-a-float'/);
  });
});

// ---------------------------------------------------------------------------
// envBool
// ---------------------------------------------------------------------------

describe("envBool", () => {
  it("unset_returns_default", () => {
    vi.stubEnv("DH_TEST_X", undefined as unknown as string);
    expect(envBool("DH_TEST_X")).toBe(false);
    expect(envBool("DH_TEST_X", true)).toBe(true);
  });

  it.each(["1", "true", "TRUE", "True", "yes", "YES", "Yes"])(
    "truthy_values %s",
    (raw) => {
      vi.stubEnv("DH_TEST_X", raw);
      expect(envBool("DH_TEST_X")).toBe(true);
      vi.unstubAllEnvs();
    }
  );

  it("truthy_with_whitespace", () => {
    vi.stubEnv("DH_TEST_X", "  Yes  ");
    expect(envBool("DH_TEST_X")).toBe(true);
  });

  it.each(["0", "false", "no", "off", "on", "y", "t", "", "garbage"])(
    "falsy_values %s",
    (raw) => {
      vi.stubEnv("DH_TEST_X", raw);
      expect(envBool("DH_TEST_X")).toBe(false);
      expect(envBool("DH_TEST_X", true)).toBe(false);
      vi.unstubAllEnvs();
    }
  );

  it("truthy_set_contents", () => {
    expect(_TRUTHY_ENV_VALUES).toEqual(new Set(["1", "true", "yes"]));
  });
});

// ---------------------------------------------------------------------------
// envRequired
// ---------------------------------------------------------------------------

describe("envRequired", () => {
  it("set_returns_value", () => {
    vi.stubEnv("DH_TEST_X", "/path/to/config");
    expect(envRequired("DH_TEST_X")).toBe("/path/to/config");
  });

  it("unset_raises_default_msg", () => {
    vi.stubEnv("DH_TEST_X", undefined as unknown as string);
    expect(() => envRequired("DH_TEST_X")).toThrow(RuntimeError);
    expect(() => envRequired("DH_TEST_X")).toThrow(
      /Environment variable DH_TEST_X is not set\./
    );
  });

  it("empty_raises", () => {
    vi.stubEnv("DH_TEST_X", "");
    expect(() => envRequired("DH_TEST_X")).toThrow(RuntimeError);
    expect(() => envRequired("DH_TEST_X")).toThrow(/DH_TEST_X is not set/);
  });

  it("unset_raises_custom_msg", () => {
    vi.stubEnv("DH_TEST_X", undefined as unknown as string);
    expect(() =>
      envRequired("DH_TEST_X", { errorMsg: "please configure DH_TEST_X first" })
    ).toThrow(RuntimeError);
    expect(() =>
      envRequired("DH_TEST_X", { errorMsg: "please configure DH_TEST_X first" })
    ).toThrow(/please configure DH_TEST_X first/);
  });
});

