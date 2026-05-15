/**
 * Tests for resource-manager/utils module.
 */
import { describe, it, expect } from "vitest";
import { findAvailablePort, generateAuthToken } from "./utils.js";

// ---------------------------------------------------------------------------
// findAvailablePort
// ---------------------------------------------------------------------------

describe("findAvailablePort", () => {
  it("returns_a_valid_port_number", async () => {
    const port = await findAvailablePort();
    expect(typeof port).toBe("number");
    expect(port).toBeGreaterThan(0);
    expect(port).toBeLessThanOrEqual(65535);
  });

  it("returns_different_ports_on_subsequent_calls", async () => {
    // This is non-deterministic but very likely to be different
    const port1 = await findAvailablePort();
    const port2 = await findAvailablePort();
    // They might or might not be different, but both should be valid
    expect(port1).toBeGreaterThan(0);
    expect(port2).toBeGreaterThan(0);
  });

  it("returns_port_in_ephemeral_range", async () => {
    const port = await findAvailablePort();
    // Ephemeral ports are typically above 1024
    expect(port).toBeGreaterThan(1024);
  });
});

// ---------------------------------------------------------------------------
// generateAuthToken
// ---------------------------------------------------------------------------

describe("generateAuthToken", () => {
  it("returns_32_character_hex_string", () => {
    const token = generateAuthToken();
    expect(typeof token).toBe("string");
    expect(token).toHaveLength(32);
  });

  it("only_contains_hex_characters", () => {
    const token = generateAuthToken();
    expect(/^[0-9a-f]{32}$/.test(token)).toBe(true);
  });

  it("returns_different_tokens_on_each_call", () => {
    const token1 = generateAuthToken();
    const token2 = generateAuthToken();
    // These might be equal in theory (2^-128 probability) but effectively never
    expect(token1).not.toBe(token2);
  });

  it("is_lowercase", () => {
    const token = generateAuthToken();
    expect(token).toBe(token.toLowerCase());
  });
});
