/**
 * Tests for auth/credentials/principal module.
 */
import { it, expect } from "vitest";
import { Principal } from "./principal.js";

it("minimal_fields", () => {
  const p = new Principal("alice", "Alice");
  expect(p.subject).toBe("alice");
  expect(p.displayName).toBe("Alice");
  expect(p.raw).toEqual({});
});

it("with_raw_claims", () => {
  const p = new Principal("alice", "Alice", { backend: "psk" });
  expect(p.raw).toEqual({ backend: "psk" });
});

it("is_frozen", () => {
  const p = new Principal("alice", "Alice");
  expect(() => {
    (p as unknown as Record<string, string>)["subject"] = "bob";
  }).toThrow();
});

it("default_raw_is_independent_between_instances", () => {
  const p1 = new Principal("a", "A");
  const p2 = new Principal("b", "B");
  // Default empty object; each instance gets its own via default param
  expect(p1.raw).not.toBe(p2.raw);
});
