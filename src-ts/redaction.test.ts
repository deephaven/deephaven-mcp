/**
 * Tests for redaction module.
 */
import { it, expect } from "vitest";
import { REDACTED } from "./redaction.js";

it("redacted_has_canonical_value", () => {
  // Pin down the canonical redaction marker.
  // Every call site in the codebase substitutes this exact string for
  // sensitive values. Changing it is almost certainly a mistake (log
  // parsers, monitoring rules, and documentation all assume this
  // literal), so this test fails loudly if the value is ever flipped.
  expect(REDACTED).toBe("[REDACTED]");
});
