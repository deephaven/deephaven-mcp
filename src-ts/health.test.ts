/**
 * Tests for health module.
 */
import { describe, it, expect } from "vitest";
import { HEALTH_PATH } from "./health.js";

describe("health", () => {
  it("health_path_value", () => {
    expect(HEALTH_PATH).toBe("/health");
  });

  it("health_path_is_string", () => {
    expect(typeof HEALTH_PATH).toBe("string");
  });

  it("health_path_shape_contract", () => {
    expect(HEALTH_PATH.startsWith("/")).toBe(true);
    expect(!HEALTH_PATH.endsWith("/") || HEALTH_PATH === "/").toBe(true);
    expect(HEALTH_PATH).toBe(HEALTH_PATH.trim());
    expect(HEALTH_PATH).not.toContain(" ");
  });
});
